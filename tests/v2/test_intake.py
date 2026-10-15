"""Intake without a consumer group and the gap protocol (design §6, TG-1…TG-14).

Real Redis streams (XADD/XTRIM/XDEL exactly like crawler2's ungrouped publisher)
and real Scylla state. Every test has its own stream prefix and reader name.
"""

from __future__ import annotations

from datetime import timedelta

import pytest

from fingerprinter.intake import (
    CLOSED_ACCEPTED,
    CLOSED_REPLAYED,
    OPEN,
    REPLAY_INCOMPLETE,
    REPLAY_OBSERVING,
    REPLAY_REQUESTED,
    REPLAY_UNAVAILABLE,
    Intake,
)
from fingerprinter.publish import INTAKE
from tests.v2.conftest import integration
from tests.v2.helpers import request_env, xadd_request

pytestmark = integration


def _intake(stack, *, init: str | None = "from-start") -> Intake:
    intake = Intake(stack.store, stack.requests)
    if init:
        intake.init(init, actor="test", reason="test")
    assert intake.acquire()
    return intake


def _drain(intake: Intake) -> None:
    for _ in range(50):
        r = intake.cycle()
        assert not r.halted and not r.lost_lease
        if r.idle:
            return
    raise AssertionError("intake did not drain")


def _stream(stack) -> str:
    return stack.coord.stream(INTAKE)


def _requests(stack) -> list[dict]:
    return stack.requests.all()


def test_tg1_normal_intake_upserts_once_and_advances(stack):
    intake = _intake(stack)
    for i in range(5):
        xadd_request(stack.coord.client, _stream(stack), request_env(f"m{i}".encode(), f"https://ex.com/{i}.mp4"))
    _drain(intake)
    st = intake.state()
    assert st["consumed_ordinal"] == 5
    rows = _requests(stack)
    assert len(rows) == 5
    assert {r["state"] for r in rows} == {"PENDING"}
    assert all(len(r["request_event_ids"]) == 1 and r["first_event_id"] for r in rows)
    assert intake.status()["healthy"]


def test_tg2_duplicate_delivery_same_event_is_noop(stack):
    intake = _intake(stack)
    env = request_env(b"dup", "https://ex.com/a.mp4")
    for _ in range(3):  # P8 physical copies of one event
        xadd_request(stack.coord.client, _stream(stack), env)
    _drain(intake)
    rows = _requests(stack)
    assert len(rows) == 1 and rows[0]["request_event_ids"] == {str(env.event_id)}
    assert intake.state()["consumed_ordinal"] == 3


def test_tg2_new_event_same_content_adds_source(stack):
    intake = _intake(stack)
    xadd_request(stack.coord.client, _stream(stack), request_env(b"same", "https://a.com/x.mp4"))
    second = request_env(b"same", "https://b.com/y.mp4", priority=80)
    xadd_request(stack.coord.client, _stream(stack), second)
    _drain(intake)
    (row,) = _requests(stack)
    assert len(row["sources"]) == 2 and row["priority"] == 80
    assert len(row["request_event_ids"]) == 2
    assert row["first_event_id"] != str(second.event_id)  # first by arrival


def test_tg3_crash_before_cursor_cas_rereads_idempotently(stack):
    intake = _intake(stack)
    for i in range(3):
        xadd_request(stack.coord.client, _stream(stack), request_env(f"c{i}".encode(), f"https://ex.com/{i}"))

    def crash(point, **ctx):
        raise RuntimeError("crash before cursor CAS")

    intake.hook = crash
    with pytest.raises(RuntimeError):
        intake.cycle()
    assert intake.state()["consumed_ordinal"] == 0  # cursor unchanged
    assert len(_requests(stack)) == 3  # upserts done
    intake.hook = lambda point, **ctx: None
    _drain(intake)
    assert len(_requests(stack)) == 3 and intake.state()["consumed_ordinal"] == 3


def test_tg4_lease_takeover_and_stale_cas_rejected(stack):
    first = _intake(stack)
    second = Intake(stack.fresh("intake").store, stack.requests)
    assert not second.acquire()  # lease held
    stack.clock.advance(seconds=stack.settings.intake.lease_ttl_s + 5)
    assert second.acquire()
    xadd_request(stack.coord.client, _stream(stack), request_env(b"t4", "https://ex.com/t4"))
    r = first.cycle()  # stale owner: epoch moved
    assert r.lost_lease
    _drain(second)
    assert second.state()["consumed_ordinal"] == 1
    assert not first.renew()


def test_tg5_trim_of_unread_entries_opens_exact_gap_and_continues(stack):
    intake = _intake(stack)
    client, s = stack.coord.client, _stream(stack)
    for i in range(3):
        xadd_request(client, s, request_env(f"a{i}".encode(), f"https://ex.com/a{i}"))
    _drain(intake)
    for i in range(10):
        xadd_request(client, s, request_env(f"b{i}".encode(), f"https://ex.com/b{i}"))
    client.xtrim(s, maxlen=4, approximate=False)  # 3 read + 6 unread trimmed (9 removed)
    _drain(intake)
    gaps = intake.open_gaps()
    assert len(gaps) == 1
    g = gaps[0]
    assert g["kind"] == "trim" and g["state"] == OPEN and g["missing_count"] == 6
    assert intake.state()["consumed_ordinal"] == 13
    assert len(_requests(stack)) == 3 + 4  # reader continued with retained entries
    status = intake.status()
    assert not status["healthy"] and len(status["open_gaps"]) == 1


def test_tg6_trim_of_read_entries_only_is_not_a_gap(stack):
    intake = _intake(stack)
    client, s = stack.coord.client, _stream(stack)
    for i in range(6):
        xadd_request(client, s, request_env(f"r{i}".encode(), f"https://ex.com/r{i}"))
    _drain(intake)
    client.xtrim(s, maxlen=1, approximate=False)
    # non-contiguous IDs: explicit far-apart ID after the cursor
    last = intake.state()["cursor_id"]
    ms = int(last.split("-")[0]) + 100000
    client.xadd(s, {"event_id": "x", "event_type": "encode.requested",
                    "envelope": request_env(b"far", "https://ex.com/far").model_dump_json()}, id=f"{ms}-7")
    _drain(intake)
    assert intake.open_gaps() == []
    assert intake.state()["consumed_ordinal"] == 7


def test_tg7_empty_and_missing_stream(stack):
    intake = _intake(stack)
    assert intake.cycle().idle  # missing stream with o = 0
    xadd_request(stack.coord.client, _stream(stack), request_env(b"e", "https://ex.com/e"))
    _drain(intake)
    stack.coord.client.delete(_stream(stack))
    r = intake.cycle()
    assert r.halted == "stream_reset"
    assert intake.state()["mode"] == "HALTED"
    assert intake.open_gaps()[0]["kind"] == "stream_reset"
    intake.reinit_after_reset(actor="op", reason="redis lost data")
    assert intake.state()["mode"] == "RUNNING" and intake.state()["consumed_ordinal"] == 0
    assert intake.open_gaps()  # the reset gap stays open for replay/accept


def test_tg8_uninitialized_reader_halts(stack):
    intake = Intake(stack.store, stack.requests)
    assert not intake.acquire()
    assert intake.cycle().halted == "uninitialized"
    assert intake.status()["halt_reason"] == "uninitialized"


def test_init_from_now_records_accepted_bootstrap_gap(stack):
    for i in range(4):
        xadd_request(stack.coord.client, _stream(stack), request_env(f"h{i}".encode(), f"https://ex.com/h{i}"))
    intake = _intake(stack, init="from-now")
    _drain(intake)
    assert _requests(stack) == []
    (g,) = intake.gaps()
    assert g["kind"] == "bootstrap" and g["state"] == CLOSED_ACCEPTED and g["missing_count"] == 4
    with pytest.raises(ValueError):
        intake.init("from-start", actor="test")


def _trim_gap(stack, intake):
    client, s = stack.coord.client, _stream(stack)
    lost = [request_env(f"lost{i}".encode(), f"https://ex.com/lost{i}") for i in range(3)]
    xadd_request(client, s, request_env(b"seen", "https://ex.com/seen"))
    _drain(intake)
    for env in lost:
        xadd_request(client, s, env)
    xadd_request(client, s, request_env(b"kept", "https://ex.com/kept"))
    client.xtrim(s, maxlen=1, approximate=False)
    _drain(intake)
    (g,) = intake.open_gaps()
    return g, lost


def test_tg9_replay_with_new_ids_closes_gap(stack):
    intake = _intake(stack)
    g, lost = _trim_gap(stack, intake)
    now = stack.store.now()
    intake.replay_start(g["gap_id"], now - timedelta(hours=1), now, actor="op")
    assert intake.gap(g["gap_id"])["state"] == REPLAY_REQUESTED
    for env in lost:  # crawler2 relay --replay-from: same events, new IDs at the tail
        xadd_request(stack.coord.client, _stream(stack), env)
    xadd_request(stack.coord.client, _stream(stack), request_env(b"kept", "https://ex.com/kept"))
    intake.replay_done(g["gap_id"], actor="op")
    assert intake.gap(g["gap_id"])["state"] == REPLAY_OBSERVING
    _drain(intake)
    assert intake.gap(g["gap_id"])["state"] == CLOSED_REPLAYED
    assert len(_requests(stack)) == 5  # seen + 3 recovered + kept; duplicate kept = no-op
    assert intake.status()["healthy"]


def test_tg10_replayed_entries_trimmed_before_read_is_incomplete(stack):
    intake = _intake(stack)
    g, lost = _trim_gap(stack, intake)
    now = stack.store.now()
    intake.replay_start(g["gap_id"], now - timedelta(hours=1), now, actor="op")
    for env in lost:
        xadd_request(stack.coord.client, _stream(stack), env)
    intake.replay_done(g["gap_id"], actor="op")
    stack.coord.client.xtrim(_stream(stack), maxlen=1, approximate=False)
    _drain(intake)
    assert intake.gap(g["gap_id"])["state"] == REPLAY_INCOMPLETE
    # second replay (operator-driven, no automatic loop) closes it
    intake.replay_start(g["gap_id"], now - timedelta(hours=1), now, actor="op")
    for env in lost:
        xadd_request(stack.coord.client, _stream(stack), env)
    intake.replay_done(g["gap_id"], actor="op")
    _drain(intake)
    assert intake.gap(g["gap_id"])["state"] == CLOSED_REPLAYED
    new_gaps = [x for x in intake.open_gaps() if x["gap_id"] != g["gap_id"]]
    assert len(new_gaps) == 1  # the replay-trim gap itself is recorded, still needs a decision


def test_tg11_unreplayable_only_accept_closes(stack):
    intake = _intake(stack)
    g, _ = _trim_gap(stack, intake)
    intake.mark_unreplayable(g["gap_id"], actor="op", reason="outside 14 d outbox retention")
    assert intake.gap(g["gap_id"])["state"] == REPLAY_UNAVAILABLE
    with pytest.raises(ValueError):
        intake.replay_start(g["gap_id"], stack.store.now(), stack.store.now(), actor="op")
    intake.accept(g["gap_id"], operator="op", reason="window outside retention")
    assert intake.gap(g["gap_id"])["state"] == CLOSED_ACCEPTED


def test_tg12_accept_gap_is_explicit_audited_and_keeps_cursor(stack):
    intake = _intake(stack)
    g, _ = _trim_gap(stack, intake)
    cursor = intake.state()["cursor_id"]
    with pytest.raises(ValueError):
        intake.accept(g["gap_id"], operator="op", reason="  ")
    detail = intake.accept(g["gap_id"], operator="alice", reason="accepted loss")
    after = intake.gap(g["gap_id"])
    assert after["state"] == CLOSED_ACCEPTED
    assert detail["missing_count"] == 3
    assert any('"accept-gap"' in a and "alice" in a for a in after["audit"])
    assert intake.state()["cursor_id"] == cursor
    with pytest.raises(ValueError):  # never re-opened or "recovered"
        intake.replay_start(g["gap_id"], stack.store.now(), stack.store.now(), actor="op")


def test_tg13_xdel_after_cursor_conservative_gap(stack):
    intake = _intake(stack)
    client, s = stack.coord.client, _stream(stack)
    xadd_request(client, s, request_env(b"x0", "https://ex.com/x0"))
    _drain(intake)
    ids = [xadd_request(client, s, request_env(f"x{i}".encode(), f"https://ex.com/x{i}")) for i in range(1, 4)]
    client.xdel(s, ids[1])
    _drain(intake)
    kinds = {g["kind"] for g in intake.open_gaps()}
    assert "tombstone" in kinds


def test_tg14_status_nonzero_with_open_gap(stack):
    intake = _intake(stack)
    _trim_gap(stack, intake)
    status = intake.status()
    assert status["healthy"] is False
    assert status["open_gaps"] and status["open_gaps"][0]["state"] == OPEN


def test_invalid_entries_recorded_and_skipped(stack):
    intake = _intake(stack)
    client, s = stack.coord.client, _stream(stack)
    client.xadd(s, {"event_id": "bad", "event_type": "encode.requested", "envelope": b"{not json"})
    xadd_request(client, s, request_env(b"ok", "https://ex.com/ok"))
    _drain(intake)
    assert intake.status()["invalid_entries"] == 1
    assert len(_requests(stack)) == 1
