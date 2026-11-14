"""Commit protocol, crash recovery, reconciler and sweeper (design §8, TC-1…TC-14).

Crashes are injected at named hook points (as in P8); recovery runs the real
reconciler against real Scylla/Redis/MinIO.
"""

from __future__ import annotations

import hashlib
import threading

import numpy as np
from antipiracy_contracts.events.fingerprinting import EncodeFailure
import pytest

from fingerprinter import artifact
from fingerprinter.commit import Artifact
from fingerprinter.coord import uuid7
from fingerprinter.db import LwtUnknownError
from fingerprinter.errors import EncodeError
from fingerprinter.objects import rep_key
from fingerprinter.publish import READY
from tests.v2.conftest import ensure_group, integration
from tests.v2.helpers import request_env

pytestmark = integration


class Crash(Exception):
    pass


def crash_at(name):
    def hook(point, **ctx):
        if point == name:
            raise Crash(point)
    return hook


def _art(seed=0, n=3) -> Artifact:
    rng = np.random.default_rng(seed)
    v = rng.normal(size=(n, 768)).astype(np.float32)
    v /= np.linalg.norm(v, axis=1, keepdims=True)
    seg = artifact.Segments(np.arange(n), np.arange(n, dtype=np.int64) * 5000, (1, 1000),
                            np.arange(n) * 5.0, v, 5.0)
    data, sha = artifact.build(seg, {"seed": seed})
    return Artifact(data, sha, n, n * 5.0, "{}", "cpu", "float32")


def _claimed(stack, data=b"c"):
    env = request_env(data, "https://a.example/v.mp4")
    stack.requests.upsert_event(env)
    cid = str(env.payload.content.content_id)
    return stack.requests.claim(cid, "default", uuid7())


def _objects(stack):
    return sorted(o.key for o in stack.objects.list(stack.objects.cfg.representations_bucket, "rep/"))


def _ready_ids(stack):
    return [f[b"event_id"].decode() for _, f in stack.coord.client.xrange(stack.coord.stream(READY))]


def _settled(stack, row):
    rep = stack.store.representation(row["content_id"], str(stack.spec.digest))
    assert rep is not None
    assert rep["f_index"] and rep["f_request"] and rep["f_targets"] and rep["ready_event_published_at"]
    assert stack.requests.get(row["content_id"], "default")["state"] == "READY"
    assert list(stack.store.intents()) == []
    assert len(stack.db.rows("SELECT * FROM {ks}.representations")) == 1
    return rep


def _expire(stack):
    stack.clock.advance(seconds=stack.settings.encoder.attempt_lease_s + 1)
    stack.requests.schedule()


def test_commit_happy_path(stack):
    ensure_group(stack, READY)
    row = _claimed(stack)
    assert stack.committer.commit(row, stack.spec, _art()) == "won"
    rep = _settled(stack, row)
    assert _objects(stack) == [rep_key(stack.spec.hex, row["content_id"], row["attempt_id"])]
    assert _ready_ids(stack) == [rep["ready_event_id"]]


def test_tc1_crash_before_upload_requeues(stack):
    row = _claimed(stack)
    _expire(stack)
    r = stack.requests.get(row["content_id"], "default")
    assert r["state"] == "PENDING" and _objects(stack) == []


@pytest.mark.parametrize("point", ["after_upload"])
def test_tc2_tc3_object_without_intent_is_orphan_after_grace(stack, point):
    row = _claimed(stack)
    stack.committer.hook = crash_at(point)
    with pytest.raises(Crash):
        stack.committer.commit(row, stack.spec, _art())
    assert stack.reconciler.sweep(grace_s=0)["skipped_active"] == 1  # attempt still leased
    _expire(stack)
    assert stack.reconciler.sweep(grace_s=3600)["deleted"] == 0  # younger than the grace
    assert stack.reconciler.sweep(grace_s=0)["deleted"] == 1
    assert _objects(stack) == []


def test_tc4_intent_without_row_dead_attempt(stack):
    row = _claimed(stack)
    stack.committer.hook = crash_at("after_intent")
    with pytest.raises(Crash):
        stack.committer.commit(row, stack.spec, _art())
    rep = stack.reconciler.intent_pass(grace_s=0)
    assert rep.skipped == 1  # attempt alive: reconciliation waits
    _expire(stack)
    rep = stack.reconciler.intent_pass(grace_s=0)
    assert rep.repairs == {"rep_dead_attempt": 1}
    assert list(stack.store.intents()) == []
    assert stack.store.representation(row["content_id"], str(stack.spec.digest)) is None
    assert len(_objects(stack)) == 1  # left to the sweeper's grace
    assert stack.reconciler.sweep(grace_s=0)["deleted"] == 1
    assert stack.requests.get(row["content_id"], "default")["state"] == "PENDING"


@pytest.mark.parametrize("point", ["after_row", "after_index"])
def test_tc5_tc6_row_written_followups_completed_by_reconciler(stack, point):
    ensure_group(stack, READY)
    row = _claimed(stack)
    stack.committer.hook = crash_at(point)
    with pytest.raises(Crash):
        stack.committer.commit(row, stack.spec, _art())
    stack.committer.hook = lambda p, **c: None
    rep = stack.reconciler.intent_pass(grace_s=0)
    assert rep.completed == 1
    _settled(stack, row)
    assert len(_ready_ids(stack)) == 1


def test_tc7_crash_after_xadd_before_ack_republishes_same_event_id(stack):
    ensure_group(stack, READY)
    row = _claimed(stack)
    stack.committer.hook = crash_at("after_xadd")
    with pytest.raises(Crash):
        stack.committer.commit(row, stack.spec, _art())
    stack.committer.hook = lambda p, **c: None
    stack.reconciler.intent_pass(grace_s=0)
    rep = _settled(stack, row)
    ids = _ready_ids(stack)
    assert ids == [rep["ready_event_id"], rep["ready_event_id"]]  # duplicate allowed, same id


def test_tc8_crash_after_ack_before_intent_delete(stack):
    ensure_group(stack, READY)
    row = _claimed(stack)
    stack.committer.hook = crash_at("before_intent_delete")
    with pytest.raises(Crash):
        stack.committer.commit(row, stack.spec, _art())
    stack.committer.hook = lambda p, **c: None
    stack.reconciler.intent_pass(grace_s=0)
    _settled(stack, row)
    assert len(_ready_ids(stack)) == 1


def test_tc9_lwt_unknown_outcome_resolved_by_serial_read(stack, monkeypatch):
    ensure_group(stack, READY)
    row = _claimed(stack)
    real = stack.db.lwt
    calls = {"n": 0}

    def flaky(cql, *values):
        res = real(cql, *values)
        if "INSERT INTO {ks}.representations" in cql and calls["n"] == 0:
            calls["n"] += 1
            raise LwtUnknownError("timeout after apply")
        return res

    monkeypatch.setattr(stack.db, "lwt", flaky)
    assert stack.committer.commit(row, stack.spec, _art()) == "won"
    _settled(stack, row)
    assert len(_objects(stack)) == 1  # nothing deleted prematurely


def test_tc10_two_workers_commit_simultaneously_one_winner(stack):
    ensure_group(stack, READY)
    a = _claimed(stack)
    _expire(stack)
    b = stack.requests.claim(a["content_id"], "default", uuid7())
    other = stack.fresh()
    barrier = threading.Barrier(2)
    results = {}

    def go(name, committer, row, seed):
        barrier.wait()
        results[name] = committer.commit(row, stack.spec, _art(seed))

    t1 = threading.Thread(target=go, args=("a", stack.committer, a, 1))
    t2 = threading.Thread(target=go, args=("b", other.committer, b, 2))
    t1.start(); t2.start(); t1.join(); t2.join()  # noqa: E702
    assert sorted(results.values()) == ["lost", "won"]
    rep = _settled(stack, a)
    winner = a if results["a"] == "won" else b
    assert rep["artifact_attempt_id"] == winner["attempt_id"]
    assert _objects(stack) == [rep_key(stack.spec.hex, a["content_id"], winner["attempt_id"])]


def test_tc11_stale_worker_wins_only_uncontested_insert_and_cannot_move_request(stack):
    ensure_group(stack, READY)
    a = _claimed(stack)
    _expire(stack)
    b = stack.requests.claim(a["content_id"], "default", uuid7())
    assert stack.requests.fail(a, EncodeError(EncodeFailure.TIMEOUT, "x", retryable=True)) == "superseded"
    assert stack.committer.commit(a, stack.spec, _art(1)) == "won"  # uncontested insert
    rep = _settled(stack, a)
    assert rep["artifact_attempt_id"] == a["attempt_id"]
    assert stack.fresh().committer.commit(b, stack.spec, _art(2)) == "lost"
    assert _objects(stack) == [rep_key(stack.spec.hex, a["content_id"], a["attempt_id"])]
    after = stack.store.representation(a["content_id"], str(stack.spec.digest))
    assert after["artifact_attempt_id"] == a["attempt_id"]  # never replaced


def test_tc12_sweeper_keeps_referenced_active_and_intent_held(stack):
    ensure_group(stack, READY)
    bucket = stack.objects.cfg.representations_bucket
    done = _claimed(stack, b"done")
    stack.committer.commit(done, stack.spec, _art())
    active = _claimed(stack, b"active")
    stack.objects.put_bytes(bucket, rep_key(stack.spec.hex, active["content_id"], active["attempt_id"]), b"x",
                            hashlib.sha256(b"x").hexdigest())
    held = _claimed(stack, b"held")
    stack.committer.hook = crash_at("after_intent")
    with pytest.raises(Crash):
        stack.committer.commit(held, stack.spec, _art())
    stack.committer.hook = lambda p, **c: None
    abandoned = rep_key(stack.spec.hex, done["content_id"], uuid7())
    stack.objects.put_bytes(bucket, abandoned, b"y", hashlib.sha256(b"y").hexdigest())
    counts = stack.reconciler.sweep(grace_s=0)
    assert counts["kept"] == 1 and counts["skipped_active"] >= 1 and counts["skipped_intent"] == 1
    assert counts["deleted"] == 1
    assert abandoned not in _objects(stack)
    assert len(_objects(stack)) == 3


def test_tc13_missing_index_never_makes_referenced_object_deletable(stack):
    ensure_group(stack, READY)
    row = _claimed(stack)
    stack.committer.commit(row, stack.spec, _art())
    digest = str(stack.spec.digest)
    stack.db.run("DELETE FROM {ks}.representations_by_spec WHERE spec_digest = ? AND bucket = ?",
                 digest, stack.store.bucket_of(row["content_id"]))
    stack.db.lwt("UPDATE {ks}.representations SET f_index = false WHERE content_id = ? AND spec_digest = ? IF EXISTS",
                 row["content_id"], digest)
    assert stack.reconciler.sweep(grace_s=0)["kept"] == 1
    assert len(_objects(stack)) == 1
    stack.reconciler.full_pass()
    stack.reconciler.intent_pass(grace_s=0)
    assert stack.db.one("SELECT * FROM {ks}.representations_by_spec WHERE spec_digest = ? AND bucket = ?",
                        digest, stack.store.bucket_of(row["content_id"]))


def test_tc14_full_pass_repairs_after_intent_removed_by_hand(stack):
    ensure_group(stack, READY)
    row = _claimed(stack)
    stack.committer.hook = crash_at("after_row")
    with pytest.raises(Crash):
        stack.committer.commit(row, stack.spec, _art())
    stack.committer.hook = lambda p, **c: None
    for intent in list(stack.store.intents()):
        stack.store.intent_del(intent)
    report = stack.reconciler.full_pass()
    assert report.repairs.get("rep_intent_recreated") == 1
    stack.reconciler.intent_pass(grace_s=0)
    _settled(stack, row)


def test_no_false_success_partial_commit_is_not_ready(stack):
    """An intent and an object do not make a representation: no row, no READY."""
    row = _claimed(stack)
    stack.committer.hook = crash_at("after_intent")
    with pytest.raises(Crash):
        stack.committer.commit(row, stack.spec, _art())
    assert stack.store.representation(row["content_id"], str(stack.spec.digest)) is None
    assert stack.requests.get(row["content_id"], "default")["state"] == "IN_PROGRESS"
    assert _ready_ids(stack) == []
