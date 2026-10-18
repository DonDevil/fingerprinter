"""Low-information lifecycle (validation §9c L-*; design §7.3) — ``matching.lowinfo_enabled`` only in these tests.

Synthetic geometry: target T1 and T3 share two identical "studio ident"
segments (each is the other's witness); T2 is unrelated; a media clip whose
segments are closer to the shared ident than to T2's segments.
"""

from __future__ import annotations

import numpy as np
import pytest

from fingerprinter.errors import StorageUnavailableError
from fingerprinter.lowinfo import COMPLETE, PARTIAL
from fingerprinter.p10ctx import KIND_A
from tests.p10 import synth

pytestmark = pytest.mark.p10_integration
DIM = synth.DIM


def unit(v):
    v = np.asarray(v, dtype=np.float64)
    return (v / np.linalg.norm(v)).astype(np.float32)


@pytest.fixture
def li(p10):
    p10.sys.p.settings = p10.sys.p.settings.model_copy(update={"matching": p10.sys.p.settings.matching.model_copy(
        update={"lowinfo_enabled": True})})
    rng = np.random.default_rng(77)
    ident = [unit(rng.standard_normal(DIM)) for _ in range(2)]
    b = [unit(rng.standard_normal(DIM)) for _ in range(2)]
    other = [unit(rng.standard_normal(DIM)) for _ in range(6)]
    t1 = p10.register_target(np.vstack(ident + other[:3]), np.arange(5) * 5.0, title="T1")
    t3 = p10.register_target(np.vstack(ident + other[3:]), np.arange(5) * 5.0, title="T3")
    t2 = p10.register_target(np.vstack(b + [unit(rng.standard_normal(DIM)) for _ in range(3)]), np.arange(5) * 5.0,
                             title="T2")
    # media segment m_i: cos(m_i, ident_i) ~ 0.80 > cos(m_i, b_i) ~ 0.62 >= floor 0.60 (L-17)
    m = [unit(0.80 * ident[i] + 0.62 * b[i] + 0.0 * rng.standard_normal(DIM)) for i in range(2)]
    media_vec = np.vstack(m + [unit(rng.standard_normal(DIM)) for _ in range(2)])
    p10.bootstrap()
    p10.drive()
    for sp in (p10.spec,):
        p10.sys.lowinfo.observe(sp)
        p10.sys.lowinfo.refresh(sp)
    return p10, {"t1": t1, "t2": t2, "t3": t3, "media": media_vec, "ident": ident}


def trep(p10, t):
    return p10.target(t[0], t[1]).subject_id


def test_l1_classification_equals_exhaustive_reference(li) -> None:
    p10, w = li
    r1 = p10.sys.lowinfo.record(p10.spec, trep(p10, w["t1"]))
    r3 = p10.sys.lowinfo.record(p10.spec, trep(p10, w["t3"]))
    r2 = p10.sys.lowinfo.record(p10.spec, trep(p10, w["t2"]))
    assert r1["status"] == COMPLETE and set(r1["flags"]) == {0, 1}
    assert set(r3["flags"]) == {0, 1} and not (r2.get("flags") or set())
    assert all(any(trep(p10, w["t3"]) in x for x in r1["witnesses"][k]) for k in (0, 1))   # artifact-confirmed
    # exhaustive numpy reference over the authoritative set
    vec = {k: p10.sys.p.artifacts.load_subject(p10.target(*w[k][:2])) for k in ("t1", "t2", "t3")}
    ref = {k: {int(i) for i in range(len(v)) if any(float(v.vectors[i] @ vec[o].vectors[j]) >= 0.95
                                                    for o in vec if o != k for j in range(len(vec[o])))}
           for k, v in vec.items()}
    assert ref == {"t1": {0, 1}, "t2": set(), "t3": {0, 1}}


def test_l17_per_hit_exclusion_keeps_the_other_target(li) -> None:
    p10, w = li
    cid = p10.ingest_media(w["media"], np.arange(4) * 5.0)
    p10.drive()
    s = p10.media(cid)
    task = p10.sys.tasks.row(s.subject_id, p10.spec, KIND_A)
    cands = p10.s.db.rows("SELECT other_id FROM {ks}.match_candidates WHERE subject_id = ? AND spec_digest = ? AND "
                          "kind = ? AND generation = ? AND run = ?", s.subject_id, p10.spec, KIND_A,
                          task["searched_generation"], task["searched_run"])
    keys = {c["other_id"] for c in cands}
    assert trep(p10, w["t2"]) in keys                     # T2's lower hits still voted (C-14h)
    assert trep(p10, w["t1"]) not in keys and trep(p10, w["t3"]) not in keys   # the usable-flag hits did not


def test_l4_l12_dead_witness_makes_segment_eligible(li) -> None:
    p10, w = li
    t1 = trep(p10, w["t1"])
    assert p10.sys.lowinfo.usable_flags(p10.spec, t1, {}) == frozenset({0, 1})
    p10.s.targets.retire(w["t3"][0], reason="test", actor="test")      # P9 retirement, refresh NOT run
    assert p10.sys.lowinfo.usable_flags(p10.spec, t1, {}) == frozenset()   # fail-closed: no live witness


def test_l13_partial_record_excludes_nothing_and_l6_stale_writer_rejected(li) -> None:
    p10, w = li
    t1 = trep(p10, w["t1"])
    rec = p10.sys.lowinfo.record(p10.spec, t1)
    p10.s.db.run("UPDATE {ks}.lowinfo_class SET status = ? WHERE spec_digest = ? AND target_representation_id = ?",
                 PARTIAL, p10.spec, t1)
    assert p10.sys.lowinfo.usable_flags(p10.spec, t1, {}) == frozenset()
    newer = int(rec["rev"]) + 5
    t = p10.target(*w["t1"][:2])
    assert p10.sys.lowinfo._write_full(p10.spec, t, newer, {0}, {0: ["x|y|1"]}, COMPLETE)
    assert not p10.sys.lowinfo._write_full(p10.spec, t, newer - 3, {0, 1}, {}, COMPLETE)   # older rev rejected
    assert int(p10.sys.lowinfo.record(p10.spec, t1)["rev"]) == newer


def test_l16_unreadable_witness_retries_never_excludes(li, monkeypatch) -> None:
    p10, w = li
    t1 = trep(p10, w["t1"])
    real = p10.s.db.one

    def flaky(cql, *a, **k):
        if "target_representations WHERE target_id" in cql and "spec_digest = ?" in cql:
            raise StorageUnavailableError("injected")
        return real(cql, *a, **k)

    monkeypatch.setattr(p10.s.db, "one", flaky)
    with pytest.raises(StorageUnavailableError):
        p10.sys.lowinfo.usable_flags(p10.spec, t1, {})
