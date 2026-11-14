"""Request lifecycle, accounting, retries, fencing and the due index (design §6.10–§7.4).

TC-11, TD-1…TD-8, TE-8, TX-10 (accounting core), §7.4 failure mapping. Real
Scylla (LWT) and Redis; the clock is Redis TIME plus an injectable offset.
"""

from __future__ import annotations

from datetime import timedelta

from antipiracy_contracts.events import decode_event_as
from antipiracy_contracts.events.fingerprinting import EncodeFailed, EncodeFailure

from fingerprinter.coord import utc, uuid7
from fingerprinter.errors import EncodeError
from fingerprinter.publish import FAILED
from fingerprinter.requests import internal_source
from tests.v2.conftest import ensure_group, integration
from tests.v2.helpers import request_env

pytestmark = integration


def _new(stack, data=b"content", url="https://a.example/v.mp4", **kw):
    env = request_env(data, url, **kw)
    stack.requests.upsert_event(env)
    cid = str(env.payload.content.content_id)
    return cid, env


def _claim(stack, cid, key="default"):
    return stack.requests.claim(cid, key, uuid7())


def _failed_events(stack):
    return [decode_event_as(f[b"envelope"], EncodeFailed)
            for _, f in stack.coord.client.xrange(stack.coord.stream(FAILED))]


def _retryable(detail="net"):
    return EncodeError(EncodeFailure.SOURCE_UNREACHABLE, detail, retryable=True, scope="source")


def test_claim_is_fenced_and_counted(stack):
    cid, _ = _new(stack)
    row = _claim(stack, cid)
    assert row["state"] == "IN_PROGRESS" and row["claims"] == 1
    assert _claim(stack, cid) is None  # not PENDING any more
    stale = dict(row, attempt_id=uuid7())
    assert not stack.requests.release(stale, reason="x")
    assert not stack.requests.mark_ready(stale, "rep_x")
    assert stack.requests.fail(stale, _retryable()) == "superseded"
    fresh = stack.requests.get(cid, "default")
    assert fresh["state"] == "IN_PROGRESS" and fresh["attempt_id"] == row["attempt_id"]


def test_release_and_expiry_do_not_count_failures(stack):
    cid, _ = _new(stack)
    row = _claim(stack, cid)
    assert stack.requests.release(row, reason="shutdown")
    row = _claim(stack, cid)
    stack.clock.advance(seconds=stack.settings.encoder.attempt_lease_s + 1)
    counts = stack.requests.schedule()
    assert counts["expired"] == 1
    r = stack.requests.get(cid, "default")
    assert r["state"] == "PENDING" and r["failures"] == 0 and r["claims"] == 2


def test_retryable_backoff_then_failed_after_max_attempts_with_one_event(stack):
    ensure_group(stack, FAILED)
    cid, env = _new(stack)
    base = stack.settings.encoder.backoff_base_s
    for i in range(4):
        row = _claim(stack, cid)
        assert row is not None, i
        state = stack.requests.fail(row, _retryable())
        r = stack.requests.get(cid, "default")
        if i < 3:
            assert state == "PENDING" and r["failures"] == i + 1
            assert utc(r["next_attempt_at"]) - stack.store.now() > timedelta(seconds=base * 3**i - 5)
            assert _claim(stack, cid) is None  # backoff not elapsed
            stack.clock.advance(seconds=base * 3**i + 1)
        else:
            assert state == "FAILED"
    (ev,) = _failed_events(stack)
    p = ev.payload
    assert p.request_id == env.event_id and p.attempt == 4 and p.retryable
    assert p.failure is EncodeFailure.SOURCE_UNREACHABLE and p.spec is None
    r = stack.requests.get(cid, "default")
    assert r["failed_event_published_at"] is not None


def test_non_retryable_content_failure_is_terminal_at_once(stack):
    ensure_group(stack, FAILED)
    cid, _ = _new(stack)
    row = _claim(stack, cid)
    err = EncodeError(EncodeFailure.DECODE_ERROR, "zero frames", retryable=False, scope="content")
    assert stack.requests.fail(row, err) == "FAILED"
    (ev,) = _failed_events(stack)
    assert ev.payload.failure is EncodeFailure.DECODE_ERROR and not ev.payload.retryable
    assert ev.payload.attempt == 1


def test_source_failure_moves_to_next_source_without_attributed_failure(stack):
    ensure_group(stack, FAILED)
    data = b"two-sources"
    cid, _ = _new(stack, data, "https://a.example/1.mp4")
    stack.requests.upsert_event(request_env(data, "https://b.example/2.mp4"))
    row = _claim(stack, cid)
    gone = EncodeError(EncodeFailure.SOURCE_UNREACHABLE, "http 404", retryable=False, scope="source")
    keys = sorted(row["sources"])
    assert stack.requests.fail(row, gone, bad={keys[0]: "404"}) == "PENDING"
    r = stack.requests.get(cid, "default")
    assert r["failures"] == 0 and set(r["bad_sources"]) == {keys[0]}
    row = _claim(stack, cid)
    assert stack.requests.fail(row, gone, bad={keys[1]: "404"}) == "FAILED"
    assert _failed_events(stack)[0].payload.attempt == 2


def test_tx10_poison_item_bounded_by_claim_budget(stack):
    ensure_group(stack, FAILED)
    cid, _ = _new(stack)
    max_claims = stack.settings.encoder.max_claims
    for _ in range(max_claims):
        row = _claim(stack, cid)
        assert row is not None
        stack.clock.advance(seconds=stack.settings.encoder.attempt_lease_s + 1)
        stack.requests.schedule()  # kills/stalls: abandoned by lease expiry, never attributed
    assert _claim(stack, cid) is None
    r = stack.requests.get(cid, "default")
    assert r["state"] == "FAILED" and r["failures"] == 0 and r["claims"] == max_claims
    assert r["last_detail"] == "claim_budget_exhausted" and r["last_retryable"]
    (ev,) = _failed_events(stack)
    assert ev.payload.failure is EncodeFailure.INTERNAL_ERROR and ev.payload.attempt == max_claims


def test_te8_reopen_new_episode_rebases_and_attempt_is_monotonic(stack):
    ensure_group(stack, FAILED)
    data = b"reopen"
    cid, env = _new(stack, data, "https://a.example/1.mp4")
    row = _claim(stack, cid)
    gone = EncodeError(EncodeFailure.SOURCE_UNREACHABLE, "http 404", retryable=False, scope="source")
    stack.requests.fail(row, gone, bad={next(iter(row["sources"])): "404"})
    assert stack.requests.get(cid, "default")["state"] == "FAILED"
    # redelivery of the seen event never reopens
    assert stack.requests.upsert_event(env) == "duplicate"
    # a new event with an untried locator reopens a new episode
    assert stack.requests.upsert_event(request_env(data, "https://c.example/3.mp4")) == "reopened"
    r = stack.requests.get(cid, "default")
    assert r["state"] == "PENDING" and r["episode"] == 2 and r["failures"] == 0
    assert r["claims_base"] == r["claims"] == 1
    row = _claim(stack, cid)
    stack.requests.fail(row, gone, bad={k: "404" for k in row["sources"]})
    first, second = _failed_events(stack)
    assert first.payload.request_id == second.payload.request_id == env.event_id
    assert second.payload.attempt > first.payload.attempt  # distinct (request_id, attempt)


def test_failed_new_event_same_locator_republishes_until_backoff_elapsed(stack):
    ensure_group(stack, FAILED)
    data = b"retry-later"
    url = "https://a.example/same.mp4"
    cid, _ = _new(stack, data, url)
    for _ in range(4):
        row = _claim(stack, cid)
        stack.requests.fail(row, _retryable())
        stack.clock.advance(days=2)
    assert stack.requests.get(cid, "default")["state"] == "FAILED"
    stack.clock.offset -= timedelta(days=2)  # right after the last failure
    assert stack.requests.upsert_event(request_env(data, url)) == "failed_republished"
    events = _failed_events(stack)
    assert len(events) == 2 and events[0].event_id == events[1].event_id  # same stored envelope
    stack.clock.advance(days=30)
    assert stack.requests.upsert_event(request_env(data, url)) == "reopened"


def test_te8_target_only_request_publishes_no_encode_failed(stack):
    ensure_group(stack, FAILED)
    sha = "ab" * 32
    src = internal_source(stack.objects.cfg.targets_bucket, f"ref/{sha}", 10, sha, "tgt/1")
    stack.requests.upsert_internal("cnt_" + "0" * 12 + "8" + "0" * 3 + "8" + "0" * 15, "default",
                                   digest="sha256:" + sha, source=src, priority=90, origin="target")
    cid = stack.requests.all()[0]["content_id"]
    row = _claim(stack, cid)
    err = EncodeError(EncodeFailure.INTERNAL_ERROR, "reference_missing", retryable=False, scope="source")
    assert stack.requests.fail(row, err, bad={k: "reference_missing" for k in row["sources"]}) == "FAILED"
    assert _failed_events(stack) == []
    r = stack.requests.get(cid, "default")
    assert r["first_event_id"] is None and r["failed_envelope"] is None
    assert [i for i in stack.store.intents()] == []


def test_operator_retry_reopens(stack):
    cid, _ = _new(stack)
    row = _claim(stack, cid)
    stack.requests.fail(row, EncodeError(EncodeFailure.TOO_LARGE, "x", retryable=False, scope="content"))
    assert stack.requests.retry(cid, "default", actor="op")
    r = stack.requests.get(cid, "default")
    assert r["state"] == "PENDING" and r["episode"] == 2


def test_td7_long_attempt_renewed_no_requeue_one_claim(stack):
    cid, _ = _new(stack)
    row = _claim(stack, cid)
    lease = stack.settings.encoder.attempt_lease_s
    for _ in range(int(31 * 86400 / (lease / 2)) // 200):  # weeks of renewals, sampled
        stack.clock.advance(seconds=lease / 2)
        assert stack.requests.renew(row)
        assert stack.requests.schedule()["expired"] == 0
    stack.clock.advance(days=31)
    assert stack.requests.renew(row)  # still discoverable and owned after > 30 days
    r = stack.requests.get(cid, "default")
    assert r["state"] == "IN_PROGRESS" and r["claims"] == 1
    assert stack.store.due_exists(cid, "default", utc(r["lease_until"]))


def test_td8_renewal_fails_after_supersede(stack):
    cid, _ = _new(stack)
    a = _claim(stack, cid)
    stack.clock.advance(seconds=stack.settings.encoder.attempt_lease_s + 1)
    stack.requests.schedule()
    b = _claim(stack, cid)
    assert b is not None
    assert not stack.requests.renew(a)
    assert stack.requests.fail(a, _retryable()) == "superseded"
    assert stack.requests.get(cid, "default")["attempt_id"] == b["attempt_id"]


def test_td1_td2_pending_beyond_30_days_still_scheduled(stack):
    long = stack.fresh(encoder=stack.settings.encoder.model_copy(update={"backoff_base_s": 40 * 86400}))
    cid, _ = _new(long)
    row = long.requests.claim(cid, "default", uuid7())
    long.requests.fail(row, _retryable())
    stack.clock.advance(days=39)
    assert long.requests.schedule()["enqueued"] == 0
    stack.clock.advance(days=2)
    assert long.requests.schedule()["enqueued"] == 1
    assert long.coord.queue_depth() == 1


def test_td3_due_entry_deleted_by_hand_is_recreated_by_full_pass(stack):
    cid, _ = _new(stack)
    r = stack.requests.get(cid, "default")
    stack.store.due_del(cid, "default", utc(r["next_attempt_at"]))
    assert stack.requests.schedule()["enqueued"] == 0
    report = stack.reconciler.full_pass()
    assert report.repairs.get("due_entry_recreated") == 1
    assert stack.requests.schedule()["enqueued"] == 1


def test_td4_crash_between_entry_write_and_transition_leaves_extra_entry(stack):
    cid, _ = _new(stack)
    extra = stack.store.now()
    stack.store.due_put(cid, "default", extra - timedelta(minutes=10))  # orphan "new" entry
    stack.clock.advance(seconds=1)
    counts = stack.requests.schedule()
    assert counts["stale"] == 1 and counts["enqueued"] == 1


def test_td5_terminal_cleanup_only_terminal_entries(stack):
    ensure_group(stack, FAILED)
    a, _ = _new(stack, b"a", "https://a.example/a")
    b, _ = _new(stack, b"b", "https://a.example/b")
    row = _claim(stack, a)
    stack.requests.fail(row, EncodeError(EncodeFailure.DECODE_ERROR, "x", retryable=False, scope="content"))
    entries = [e for d in stack.store.due_days() for e in stack.store.due_scan(d, stack.store.now() + timedelta(days=1))]
    assert {e["content_id"] for e in entries} == {b}


def test_td6_redis_flush_rebuilds_queue(stack):
    for i in range(3):
        _new(stack, f"q{i}".encode(), f"https://a.example/{i}")
    stack.requests.schedule()
    assert stack.coord.queue_depth() == 3
    stack.coord.client.delete(stack.coord.queue_key)
    assert stack.coord.queue_depth() == 0
    stack.requests.schedule()
    assert stack.coord.queue_depth() == 3


def test_upsert_concurrent_same_content_one_row(stack):
    import threading

    data = b"race"
    envs = [request_env(data, f"https://h{i}.example/v.mp4") for i in range(6)]
    workers = [threading.Thread(target=stack.fresh().requests.upsert_event, args=(e,)) for e in envs]
    for w in workers:
        w.start()
    for w in workers:
        w.join()
    (row,) = stack.requests.all()
    assert len(row["request_event_ids"]) == 6 and len(row["sources"]) == 6
    assert row["first_event_id"] in {str(e.event_id) for e in envs}
