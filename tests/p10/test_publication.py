"""Publication of ``match.found`` 1.0 and liveness (validation §9d K-8, K-10, K-11; design §9.1, §9.3, §4.2).

Production technique 2.1 is UNAVAILABLE, so nothing is ever published by it
(test_flow). To exercise the publication path itself, these tests register a
**test-only** technique version with a CALIBRATED record (a Platt map with
made-up parameters, labelled as such) in the test keyspace; it never exists
in a real keyspace.
"""

from __future__ import annotations

import threading

import pytest
from antipiracy_contracts.events import decode_event_as
from antipiracy_contracts.events.fingerprinting import MatchFound

from fingerprinter.confidence import CALIBRATED
from fingerprinter.matching import MATCH_FOUND
from fingerprinter.p10ctx import media_live, target_live
from tests.p10.conftest import VECTORS
from tests.p10.test_flow import clip, film

pytestmark = pytest.mark.p10_integration
TEST_VERSION = "2.1-test-calibrated"


@pytest.fixture
def calibrated(p10):
    if not (VECTORS / "l-pairs").exists():
        pytest.skip("cached L-PAIRS vectors not present")
    tech = p10.sys.techniques.get("2.1")
    p10.sys.techniques.register(TEST_VERSION, tech.params,
                                calibration={"method": "platt", "model": {"a": -2.0, "b": 6.0},
                                                                       "note": "TEST ONLY - not a fitted calibration"},
                                status=CALIBRATED, calibration_id="cal-test-only", actor="test", evidence="test")
    p10.sys.p.settings = p10.sys.p.settings.model_copy(update={"matching": p10.sys.p.settings.matching.model_copy(
        update={"technique_version": TEST_VERSION})})
    return p10


def test_k8_k10_envelope_once_held_then_released(calibrated) -> None:
    p10 = calibrated
    p10.register_target(*film("sintel"), title="Sintel")
    m = p10.ingest_media(*clip("lp-000-sintel-re-encode.mp4"))
    p10.bootstrap()
    p10.drive()
    rows = [r for r in p10.s.db.rows("SELECT * FROM {ks}.matches") if r["content_id"] == m]
    assert len(rows) == 1
    r = rows[0]
    assert r["technique_version"] == TEST_VERSION and r["calibration_id"] == "cal-test-only"
    assert 0.0 <= r["confidence"] <= 1.0 and r["event_id"] and r["published_at"] is None   # held: no consumer
    env = decode_event_as(r["envelope"], MatchFound)            # the frozen 1.0 contract validates it
    assert str(env.payload.match.match_id) == r["match_id"] and env.event_type == "match.found"
    assert str(env.event_id) == r["event_id"]
    stream = p10.s.coord.stream(MATCH_FOUND)
    assert p10.s.coord.client.xlen(stream) == 0
    # K-10: publish-held twice concurrently while still held -> same envelope, nothing XADDed
    outs: list = []
    ths = [threading.Thread(target=lambda: outs.append(p10.sys.committer.publish_held(TEST_VERSION, actor="t")))
           for _ in range(2)]
    for t in ths:
        t.start()
    for t in ths:
        t.join()
    again = p10.s.db.one("SELECT * FROM {ks}.matches WHERE content_id = ?", m)
    assert again["event_id"] == r["event_id"] and again["envelope"] == r["envelope"]
    # a consumer registers (as P11/P12 would): release; one entry, same event id
    p10.s.coord.client.xgroup_create(stream, "test-consumer", id="0", mkstream=True)
    p10.s.pub.register_consumer(MATCH_FOUND, "test-consumer", actor="test")
    p10.sys.committer.publish_held(TEST_VERSION, actor="t")
    p10.sys.committer.publish_held(TEST_VERSION, actor="t")
    entries = p10.s.coord.client.xrange(stream)
    assert len(entries) == 1
    fields = entries[0][1]
    assert (fields.get(b"event_id") or fields.get("event_id")).decode() == r["event_id"]
    assert p10.s.db.one("SELECT published_at FROM {ks}.matches WHERE content_id = ?", m)["published_at"] is not None


def test_publish_held_refused_for_uncalibrated(p10) -> None:
    with pytest.raises(ValueError, match="UNAVAILABLE"):
        p10.sys.committer.publish_held("2.1", actor="test")


@pytest.mark.parametrize(("binding", "target", "live"), [
    ("READY", "READY", True), ("READY", "REGISTERING", True), ("READY", "BUILD_FAILED", True),
    ("READY", "RETIRED", False), ("READY", "PURGED", False), ("PENDING", "READY", False),
    ("PENDING", "REGISTERING", False), (None, "READY", False), ("READY", None, False),
])
def test_k11_one_liveness_predicate(binding, target, live) -> None:
    b = None if binding is None else {"state": binding}
    t = None if target is None else {"state": target}
    assert target_live(b, t) is live
    assert media_live({"state": "READY"}) and not media_live({"state": "RETIRED"}) and not media_live(None)
