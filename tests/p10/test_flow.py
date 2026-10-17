"""End-to-end P10 flow on the real stack (validation §4, §7; groups B, H).

Real cached ``dinov2-seg/2`` vectors of L-PAIRS (films and clips) enter
through P9's own commit protocol; the index is built from authoritative
state and promoted; discovery (hints + backstop scans), the matcher and the
reconciler run as in production. Asserts both directions, ANN candidate
reduction before verification, held publication (technique 2.1 is
UNAVAILABLE), and that direction B re-encodes nothing.
"""

from __future__ import annotations

import json

import numpy as np
import pytest

from fingerprinter.matching import MATCH_FOUND
from fingerprinter.p10ctx import KIND_A, KIND_B
from tests.p10.conftest import VECTORS

pytestmark = pytest.mark.p10_integration

FILMS = {"sintel": "Sintel.2010.1080p.mkv", "elephants-dream": "elephantsdream-720-h264-st-aac.mov",
         "sprite-fright": "Sprite_Fright_-_Blender_Open_Movie-full_movie.webm"}


def cached(name: str):
    z = np.load(VECTORS / "l-pairs" / f"{name}.npz")
    return z["vectors"].astype(np.float32), z["pts_s"].astype(np.float64)


def film(fid):
    return cached("targets__" + FILMS[fid])


def clip(name):
    return cached("clips__" + name)


@pytest.fixture(autouse=True)
def _need_vectors():
    if not (VECTORS / "l-pairs").exists():
        pytest.skip("cached L-PAIRS vectors not present")


def matches(p10):
    return list(p10.s.db.rows("SELECT * FROM {ks}.matches"))


def test_both_directions_held_and_no_reencode(p10) -> None:
    t1 = p10.register_target(*film("sintel"), title="Sintel")
    t2 = p10.register_target(*film("elephants-dream"), title="ED")
    m1 = p10.ingest_media(*clip("lp-000-sintel-re-encode.mp4"))
    neg = p10.ingest_media(*clip("lp-029-elephants-dream-pillarbox.webm"))   # matches ED, not Sintel
    p10.bootstrap()
    p10.drive()
    rows = {(r["content_id"], r["target_id"]): r for r in matches(p10)}
    assert (m1, t1[0]) in rows and rows[(m1, t1[0])]["verdict"] == "confirmed"
    assert (m1, t2[0]) not in rows and (neg, t1[0]) not in rows
    assert (neg, t2[0]) in rows
    r = rows[(m1, t1[0])]
    assert r["confidence"] is None and r["event_id"] is None and r["published_at"] is None  # held: UNAVAILABLE
    prov = json.loads(r["provenance"])
    assert prov["verifier_params_digest"] and prov["index_generation"] and prov["ann"]["k0"] == 32
    # nothing was XADDed to match.found
    assert p10.s.coord.client.xlen(p10.s.coord.stream(MATCH_FOUND)) == 0

    # direction B: historical media M2 first, then a new target T3 — no re-encode of M2
    m2 = p10.ingest_media(*clip("lp-004-sprite-fright-re-encode.mkv"))
    p10.drive()
    claims_before = {r["content_id"]: r["claims"] for r in p10.s.requests.all()}
    t3 = p10.register_target(*film("sprite-fright"), title="Sprite Fright")
    p10.drive()
    rows = {(r["content_id"], r["target_id"]): r for r in matches(p10)}
    assert (m2, t3[0]) in rows, "a new target matched historical media"
    claims_after = {r["content_id"]: r["claims"] for r in p10.s.requests.all()}
    assert all(claims_after[c] == n for c, n in claims_before.items()), "no encode attempt for historical media"
    tb = p10.sys.tasks.row(p10.target(t3[0], t3[1]).subject_id, p10.spec, KIND_B)
    assert tb["state"] == "DONE" and tb["done_ok_run"] == 1
    stats = json.loads(tb["ann_stats"])
    assert stats["queries"] >= stats["query_segments"] > 0 and stats["stored"] <= 5000
    ta = p10.sys.tasks.row(p10.media(m1).subject_id, p10.spec, KIND_A)
    assert ta["state"] == "DONE"
    st = p10.sys.recovery.settled(p10.media(neg).subject_id, p10.spec, [KIND_A])
    assert st["status"] == "settled", st
