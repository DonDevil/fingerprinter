"""Event publication, holds, consumers and retention (design §9, TE-1…TE-7)."""

from __future__ import annotations

import pytest

from fingerprinter.publish import READY, TARGET_REGISTERED, Outcome
from tests.v2.conftest import ensure_group, integration
from tests.v2.test_commit import _art, _claimed

pytestmark = integration


def _stream_len(stack, name):
    return int(stack.coord.client.xlen(stack.coord.stream(name)))


def test_te5_media_registry_group_missing_holds_then_releases(stack):
    row = _claimed(stack)
    assert stack.committer.commit(row, stack.spec, _art()) == "won"
    held, reason = stack.pub.held(READY)
    assert held and "media-registry" in reason
    assert _stream_len(stack, READY) == 0
    rep = stack.store.representation(row["content_id"], str(stack.spec.digest))
    assert rep["ready_event_published_at"] is None
    assert len(list(stack.store.intents())) == 1  # durable, no TTL
    assert stack.reconciler.intent_pass(grace_s=0).held == 1
    ensure_group(stack, READY)  # crawler2 starts later
    stack.reconciler.intent_pass(grace_s=0)
    assert _stream_len(stack, READY) == 1 and list(stack.store.intents()) == []


def test_te1_publisher_down_more_than_14_days_nothing_expires(stack):
    row = _claimed(stack)
    stack.committer.commit(row, stack.spec, _art())
    stack.clock.advance(days=15)
    assert len(list(stack.store.intents())) == 1
    ensure_group(stack, READY)
    stack.reconciler.intent_pass()  # default grace; intent is 15 days old
    assert _stream_len(stack, READY) == 1
    rep = stack.store.representation(row["content_id"], str(stack.spec.digest))
    assert rep["ready_event_published_at"] is not None


def test_te6_backpressure_never_trims_unacknowledged(stack):
    small = stack.fresh(publisher=stack.settings.publisher.model_copy(update={"stream_max_bytes": 6000}))
    ensure_group(stack, READY)
    rows = [_claimed(small, f"bp{i}".encode()) for i in range(6)]
    for i, row in enumerate(rows):
        small.committer.commit(row, small.spec, _art(i))
    published = _stream_len(stack, READY)
    assert 0 < published < 6
    assert len(list(small.store.intents())) == 6 - published  # rest waits durably
    client, key = stack.coord.client, stack.coord.stream(READY)
    small.reconciler.intent_pass(grace_s=0)
    assert _stream_len(stack, READY) == published  # unacknowledged entries are never trimmed
    entries = client.xreadgroup("media-registry", "c1", {key: ">"}, count=100)
    ids = [i for _, es in entries for i, _ in es]
    client.xack(key, "media-registry", *ids)
    for _ in range(6):
        small.reconciler.intent_pass(grace_s=0)
        for _, es in client.xreadgroup("media-registry", "c1", {key: ">"}, count=100) or []:
            client.xack(key, "media-registry", *[i for i, _ in es])
    assert list(small.store.intents()) == []
    reps = small.db.rows("SELECT ready_event_published_at FROM {ks}.representations")
    assert all(r["ready_event_published_at"] for r in reps) and len(reps) == 6


def test_te7_xadd_is_not_processing(stack):
    ensure_group(stack, READY)
    row = _claimed(stack)
    stack.committer.commit(row, stack.spec, _art())
    # acknowledged as "appended" although no consumer has read it
    info = stack.coord.client.xinfo_groups(stack.coord.stream(READY))[0]
    assert int(info["pending"]) == 0 and info["last-delivered-id"] in (b"0-0", "0-0")
    assert stack.store.representation(row["content_id"], str(stack.spec.digest))["ready_event_published_at"]


def test_te3_te4_target_events_held_until_registered_then_released_and_republished(stack, tmp_path):
    ensure_group(stack, READY)
    media = tmp_path / "t.bin"
    media.write_bytes(b"target-bytes")
    reg = stack.targets.register(media, title="T", kind="movie", actor="op")
    # bind READY by committing a representation of the target content
    row = stack.requests.claim(reg.content_id, "default", __import__("fingerprinter.coord", fromlist=["uuid7"]).uuid7())
    stack.committer.commit(row, stack.spec, _art())
    t = stack.targets.target(reg.target_id, reg.version)
    assert t["state"] == "READY" and t["registered_envelope"]
    assert not stack.coord.client.exists(stack.coord.stream(TARGET_REGISTERED))  # nothing written
    assert stack.pub.held(TARGET_REGISTERED)[0]
    with pytest.raises(ValueError):
        stack.pub.register_consumer(TARGET_REGISTERED, "matcher", actor="op")  # group must exist first
    ensure_group(stack, TARGET_REGISTERED, "matcher")
    stack.pub.register_consumer(TARGET_REGISTERED, "matcher", actor="op")
    stack.reconciler.intent_pass(grace_s=0)
    assert _stream_len(stack, TARGET_REGISTERED) == 1
    assert stack.targets.target(reg.target_id, reg.version)["registered_event_published_at"]
    assert stack.targets.republish(TARGET_REGISTERED) == 1  # bootstrap a late consumer
    ids = [f[b"event_id"] for _, f in stack.coord.client.xrange(stack.coord.stream(TARGET_REGISTERED))]
    assert len(ids) == 2 and ids[0] == ids[1]  # same event_id: consumers dedupe


def test_publisher_status_reports_holds(stack):
    status = {s["stream"]: s for s in stack.pub.status()}
    assert status[READY]["held"] and status[TARGET_REGISTERED]["held"]
    ensure_group(stack, READY)
    assert stack.pub.publish(READY, "evt_x", "representation.ready", "{}") is Outcome.PUBLISHED
