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
from fingerprinter.p10ctx import KIND_A, KIND_B
from fingerprinter.vindex import IndexUnavailableError
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


def test_l5_duplicate_observation_bumps_revision_once(li) -> None:
    """L-5: repeated observation of an unchanged target set (duplicate registration/retirement
    observations) does not bump ``target_set_rev``; one real change bumps it exactly once."""
    p10, w = li
    r0 = p10.sys.lowinfo.observe(p10.spec)
    assert [p10.sys.lowinfo.observe(p10.spec) for _ in range(3)] == [r0] * 3
    p10.s.targets.retire(w["t2"][0], reason="L-5", actor="test")
    assert p10.sys.lowinfo.observe(p10.spec) == r0 + 1
    assert p10.sys.lowinfo.observe(p10.spec) == r0 + 1


def test_l11_unobserved_registration_adds_no_exclusion(li) -> None:
    """L-11: X (sharing the ident with T1/T3) registered in P9 but not observed by the scan: no flag names X
    as a witness, no new exclusion, and T2's usable flags are unchanged."""
    p10, w = li
    before = {k: p10.sys.lowinfo.usable_flags(p10.spec, trep(p10, w[k]), {}) for k in ("t1", "t2", "t3")}
    rng = np.random.default_rng(78)
    x = p10.register_target(np.vstack(w["ident"] + [unit(rng.standard_normal(DIM)) for _ in range(3)]),
                            np.arange(5) * 5.0, title="X")
    xs = trep(p10, x)
    for r in p10.sys.lowinfo.records(p10.spec):
        assert not any(xs in ref for refs in (r.get("witnesses") or {}).values() for ref in refs)
    assert p10.sys.lowinfo.record(p10.spec, xs) is None
    after = {k: p10.sys.lowinfo.usable_flags(p10.spec, trep(p10, w[k]), {}) for k in ("t1", "t2", "t3")}
    assert after == before


def test_l15_payload_never_decides_an_exclusion(li) -> None:
    """L-15: T's payload says ``lowinfo = true`` but its record has no flag ⇒ eligible (the record decides);
    U's payload says false although its record is flagged ⇒ eligible (payload false: no check, a stale
    payload can only reduce exclusion)."""
    p10, w = li
    t2, t1 = trep(p10, w["t2"]), trep(p10, w["t1"])
    check = p10.sys.lowinfo.hit_excluded(p10.spec, {}, {})
    assert not check({"trep": t2, "k": 0, "lowinfo": True})        # record of T2 has no flag
    assert not check({"trep": t1, "k": 0, "lowinfo": False})       # T1 seg 0 is flagged, payload false
    assert check({"trep": t1, "k": 0, "lowinfo": True})            # both agree: excluded (filter OK)


# --- cases unrun after the exit-gate pass (validation §13.21) ------------------------------------


def reference_flags(p10) -> dict[str, set[int]]:
    """Exhaustive numpy float32 classification of every live target at the authoritative set."""
    vec = {t.subject_id: (t, p10.sys.p.artifacts.load_subject(t)) for t in p10.sys.lowinfo.members(p10.spec)}
    out: dict[str, set[int]] = {}
    for sid, (t, v) in vec.items():
        flags = set()
        for i, k in enumerate(v.segment_index):
            for oid, (o, ov) in vec.items():
                if o.target_id != t.target_id and np.any(ov.vectors.astype(np.float64) @
                                                         v.vectors[i].astype(np.float64) >= 0.95):
                    flags.add(int(k))
                    break
        out[sid] = flags
    return out


def stored_flags(p10) -> dict[str, set[int]]:
    live = {t.subject_id for t in p10.sys.lowinfo.members(p10.spec)}
    return {r["target_representation_id"]: {int(x) for x in (r.get("flags") or ())}
            for r in p10.sys.lowinfo.records(p10.spec) if r["target_representation_id"] in live
            and r.get("status") == COMPLETE}


def refresh_row(p10) -> dict:
    return p10.s.db.one("SELECT * FROM {ks}.lowinfo_refresh WHERE spec_digest = ?", p10.spec)


def with_ident(p10, w, seed: int, title: str, ident=None):
    rng = np.random.default_rng(seed)
    return p10.register_target(np.vstack((ident or w["ident"]) + [unit(rng.standard_normal(DIM)) for _ in range(3)]),
                               np.arange(5) * 5.0, title=title)


def test_l2_retired_witness_found_from_witness_lists(li) -> None:
    """L-2: T1's ident flags have witnesses T3 and X. Retiring T3 (observed) keeps the flags through the
    live witness X (no rerun); retiring X too clears them (no live witness), and T1's target_vs_media is
    re-enqueued (``lowinfo_flag_cleared``); the payload copy follows the new revision."""
    p10, w = li
    x = with_ident(p10, w, 91, "X")
    p10.drive()
    p10.sys.lowinfo.observe(p10.spec)
    p10.sys.lowinfo.refresh(p10.spec)
    t1 = trep(p10, w["t1"])
    rec = p10.sys.lowinfo.record(p10.spec, t1)
    assert set(rec["flags"]) == {0, 1} and all(len(rec["witnesses"][k]) == 2 for k in (0, 1))
    run0 = p10.sys.tasks.row(t1, p10.spec, KIND_B)["run"]
    p10.s.targets.retire(w["t3"][0], reason="L-2", actor="test")
    p10.sys.lowinfo.observe(p10.spec)
    out = p10.sys.lowinfo.refresh(p10.spec)
    rec = p10.sys.lowinfo.record(p10.spec, t1)
    assert out["cleared"] == 0 and set(rec["flags"]) == {0, 1}
    assert all(trep(p10, w["t3"]) not in "".join(rec["witnesses"][k]) for k in (0, 1))   # found from the lists
    assert p10.sys.tasks.row(t1, p10.spec, KIND_B)["run"] == run0
    p10.s.targets.retire(x[0], reason="L-2", actor="test")
    rev = p10.sys.lowinfo.observe(p10.spec)
    out = p10.sys.lowinfo.refresh(p10.spec)
    rec = p10.sys.lowinfo.record(p10.spec, t1)
    assert out["cleared"] >= 1 and not (rec.get("flags") or set()) and int(rec["rev"]) == rev
    assert p10.sys.lowinfo.usable_flags(p10.spec, t1, {}) == frozenset()
    tb = p10.sys.tasks.row(t1, p10.spec, KIND_B)
    assert tb["run"] == run0 + 1 and tb["rerun_reason"] == "lowinfo_flag_cleared"
    e = p10.sys.indexer.entry(p10.spec, "target", p10.target(*w["t1"][:2]).bucket, t1)
    assert e["desired_ver"].endswith(f"+li{rev}")
    assert stored_flags(p10) == reference_flags(p10)


def test_l3_crash_after_the_revision_bump_resumes_and_converges(li, monkeypatch) -> None:
    """L-3: X (sharing the ident) is observed (rev r+1); the refresh dies after its first record. Then
    ``done_rev < requested_rev``, records not yet written leave X's segments eligible (fail-closed); the
    next refresh resumes and the records equal the exhaustive reference."""
    p10, w = li
    x = with_ident(p10, w, 92, "X")
    p10.sys.indexer.ensure(p10.target(x[0], x[1]))     # indexed; the reconciler's own refresh not run
    rev = p10.sys.lowinfo.observe(p10.spec)
    real = p10.sys.lowinfo.classify
    n = {"calls": 0}

    def crash_after_first(spec, t, r):
        n["calls"] += 1
        if n["calls"] == 2:
            raise RuntimeError("kill -9 of the refresh worker")
        return real(spec, t, r)

    monkeypatch.setattr(p10.sys.lowinfo, "classify", crash_after_first)
    with pytest.raises(RuntimeError):
        p10.sys.lowinfo.refresh(p10.spec)
    monkeypatch.setattr(p10.sys.lowinfo, "classify", real)
    row = refresh_row(p10)
    assert int(row["done_rev"] or 0) < int(row["requested_rev"]) == rev
    xs = trep(p10, x)
    rec = p10.sys.lowinfo.record(p10.spec, xs)
    if rec is None or int(rec["rev"]) < rev:
        assert p10.sys.lowinfo.usable_flags(p10.spec, xs, {}) == frozenset()
    p10.sys.lowinfo.refresh(p10.spec)
    assert int(refresh_row(p10)["done_rev"]) == rev
    assert stored_flags(p10) == reference_flags(p10)
    assert stored_flags(p10)[xs] == {0, 1}


def test_l7_corrupted_and_missing_records_detected_and_repaired(li) -> None:
    """L-7: T2's record is corrupted with a flag whose witness is live but not similar, T3's record is
    deleted. The corrupted flag is usable until detected (an alarm case outside the safety model:
    records are written only after an exact cosine check); one full recomputation reports
    ``extra_flag`` and ``missing_record`` and repairs both."""
    p10, w = li
    t2 = p10.target(*w["t2"][:2])
    t1 = p10.target(*w["t1"][:2])
    rec = p10.sys.lowinfo.record(p10.spec, t2.subject_id)
    from fingerprinter.lowinfo import witness_ref

    assert p10.sys.lowinfo._write_full(p10.spec, t2, int(rec["rev"]), {3}, {3: [witness_ref(t1)]}, COMPLETE)
    p10.s.db.run("DELETE FROM {ks}.lowinfo_class WHERE spec_digest = ? AND target_representation_id = ?",
                 p10.spec, trep(p10, w["t3"]))
    assert p10.sys.lowinfo.usable_flags(p10.spec, t2.subject_id, {}) == frozenset({3})   # the alarm window
    diff = p10.sys.lowinfo.full_recompute(p10.spec)
    assert diff["extra_flag"] >= 1 and diff["missing_record"] >= 1
    assert p10.sys.lowinfo.usable_flags(p10.spec, t2.subject_id, {}) == frozenset()
    assert stored_flags(p10) == reference_flags(p10)


def test_l8_registration_during_a_refresh_loops_to_the_latest(li, monkeypatch) -> None:
    """L-8: a refresh for r+1 (X) is running when Y registers and is observed (r+2). ``done_rev`` never
    claims r+1 or r+2 before the records of r+2 are written; the next refresh completes r+2; usable
    flags stay a subset of the reference throughout."""
    p10, w = li
    x = with_ident(p10, w, 93, "X")
    p10.sys.indexer.ensure(p10.target(x[0], x[1]))     # indexed; the reconciler's own refresh not run
    r1 = p10.sys.lowinfo.observe(p10.spec)
    real = p10.sys.lowinfo.classify
    state = {"y": None}

    def register_y_during(spec, t, r):
        if state["y"] is None:
            state["y"] = with_ident(p10, w, 94, "Y")
            p10.sys.indexer.ensure(p10.target(state["y"][0], state["y"][1]))
            state["rev"] = p10.sys.lowinfo.observe(p10.spec)
        return real(spec, t, r)

    monkeypatch.setattr(p10.sys.lowinfo, "classify", register_y_during)
    p10.sys.lowinfo.refresh(p10.spec)
    monkeypatch.setattr(p10.sys.lowinfo, "classify", real)
    r2 = state["rev"]
    assert r2 == r1 + 1
    row = refresh_row(p10)
    assert int(row["requested_rev"]) == r2 and int(row["done_rev"] or 0) < r1   # r+1's LWT lost to r+2
    ref = reference_flags(p10)
    for sid in ref:
        assert p10.sys.lowinfo.usable_flags(p10.spec, sid, {}) <= ref[sid]
    p10.sys.lowinfo.refresh(p10.spec)
    assert int(refresh_row(p10)["done_rev"]) == r2
    assert stored_flags(p10) == reference_flags(p10)
    assert stored_flags(p10)[trep(p10, state["y"])] == {0, 1}


def test_l9_refresh_failing_repeatedly_becomes_blocked_and_visible(li, monkeypatch) -> None:
    """L-9: the refresh worker cannot reach Qdrant (every classification search fails). After the
    configured attempts the refresh is BLOCKED with ``last_error`` and ``attempts``; nothing is excluded
    except through usable flags of the records written before; once Qdrant is back it completes."""
    p10, w = li
    x = with_ident(p10, w, 95, "X")
    p10.sys.indexer.ensure(p10.target(x[0], x[1]))     # indexed; the reconciler's own refresh not run
    rev = p10.sys.lowinfo.observe(p10.spec)
    before = {sid: p10.sys.lowinfo.usable_flags(p10.spec, sid, {}) for sid in reference_flags(p10)}
    real = p10.index.search_batch

    def down(name, reqs, **kw):
        if reqs and reqs[0].exact:
            raise IndexUnavailableError("injected: refresh worker cannot reach Qdrant")
        return real(name, reqs, **kw)

    monkeypatch.setattr(p10.index, "search_batch", down)
    for _ in range(p10.sys.p.settings.matching.lowinfo_max_attempts + 1):
        out = p10.sys.reconcile_once(force=True)
        assert "error" in str(out.get("lowinfo"))
    row = refresh_row(p10)
    assert row["state"] == "BLOCKED" and "injected" in (row["last_error"] or "")
    assert int(row["attempts"]) >= p10.sys.p.settings.matching.lowinfo_max_attempts
    assert int(row["done_rev"] or 0) < rev
    for sid, flags in before.items():
        assert p10.sys.lowinfo.usable_flags(p10.spec, sid, {}) <= flags | reference_flags(p10)[sid]
    monkeypatch.setattr(p10.index, "search_batch", real)
    p10.sys.lowinfo.refresh(p10.spec)
    row = refresh_row(p10)
    assert int(row["done_rev"]) == rev and row["state"] == "DONE" and int(row["attempts"] or 0) == 0
    assert stored_flags(p10) == reference_flags(p10)


def test_l14_truncated_enumeration_is_completed_never_claimed_complete(p10, monkeypatch) -> None:
    """L-14 (lifecycle): one ident segment is shared by more than ``L`` other targets, and the
    highest-ranked copies belong to targets that are retired in P9 but whose points are still in the
    index (their DELETE not yet applied). A query of size ``L`` is truncated: the classification must
    enumerate further (or record PARTIAL and not advance ``done_rev``), never write a COMPLETE record that
    misses a flag a live witness supports. With the page bound exhausted the record is PARTIAL, excludes
    nothing, and ``done_rev``/``full_requested`` show the incomplete step."""
    m = p10.sys.p.settings.matching.model_copy(update={"lowinfo_enabled": True, "lowinfo_limit": 2})
    p10.sys.p.settings = p10.sys.p.settings.model_copy(update={"matching": m})
    rng = np.random.default_rng(140)
    ident = unit(rng.standard_normal(DIM))
    near = unit(ident + 0.18 * unit(rng.standard_normal(DIM)))      # cos(ident, near) ≈ 0.98
    assert 0.95 < float(ident @ near) < 0.995

    def tgt(first, seed, title):
        r = np.random.default_rng(seed)
        return p10.register_target(np.vstack([first] + [unit(r.standard_normal(DIM)) for _ in range(4)]),
                                   np.arange(5) * 5.0, title=title)

    t = tgt(ident, 141, "T")
    live_w = tgt(near, 142, "W")                       # the only live witness, ranked below the dead copies
    dead = [tgt(ident, 143 + i, f"D{i}") for i in range(3)]
    p10.bootstrap()
    p10.drive()
    for d in dead:                                     # retired in P9; DELETE intents not applied yet
        p10.s.targets.retire(d[0], reason="L-14", actor="test")
    ts = p10.target(t[0], t[1]).subject_id
    p10.sys.lowinfo.observe(p10.spec)
    p10.sys.lowinfo.refresh(p10.spec)
    rec = p10.sys.lowinfo.record(p10.spec, ts)
    assert rec["status"] == COMPLETE and 0 in set(rec["flags"]), rec
    assert any(p10.target(live_w[0], live_w[1]).subject_id in x for x in rec["witnesses"][0])
    assert stored_flags(p10) == reference_flags(p10)
    # the page bound exhausted: PARTIAL, nothing excluded, done_rev not advanced, full recomputation requested
    import fingerprinter.lowinfo as lowinfo_mod

    monkeypatch.setattr(lowinfo_mod, "MAX_PAGES", 1)
    rev = p10.sys.lowinfo.observe(p10.spec, members=p10.sys.lowinfo.members(p10.spec)[:-1])  # force a new rev
    p10.sys.lowinfo.refresh(p10.spec)
    rec = p10.sys.lowinfo.record(p10.spec, ts)
    assert rec["status"] == PARTIAL
    assert p10.sys.lowinfo.usable_flags(p10.spec, ts, {}) == frozenset()
    row = refresh_row(p10)
    assert int(row["done_rev"] or 0) < rev and row["full_requested"]
