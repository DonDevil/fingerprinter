"""Matrix cases still unrun after the exit-gate pass (validation §11 P10-T11, §13.21 "still not run") — real stack.

Same conventions as ``test_matrix.py``: a raising hook models a crash at that
boundary; direct Qdrant calls model a stale worker or a non-protocol change;
every case checks the parts of the §9a common pass condition that apply.
"""

from __future__ import annotations

import threading
import time
from datetime import timedelta

import pytest

from fingerprinter.coord import utc
from fingerprinter.indexing import BLOCKED
from fingerprinter.p10ctx import BUCKETS, KIND_A
from fingerprinter.vindex import MEDIA, TARGET, Point, point_id, subject_filter, subject_seq_eq
from tests.p10 import synth
from tests.p10.test_mutation import CrashError, active_collection, entry

pytestmark = pytest.mark.p10_integration


def resident(p10) -> int:
    return sum(p10.index.count(c) for c in p10.index.created if p10.index.collection_exists(c))


def tight_limit(p10, max_points: int, margin: int) -> None:
    p10.sys.p.settings = p10.sys.p.settings.model_copy(update={"index": p10.sys.p.settings.index.model_copy(
        update={"max_points": max_points, "capacity_margin_points": margin})})
    p10.sys.capacity.ix = p10.sys.p.settings.index


# --- §9e CAP-4: no bypass of the ledger by a second generation --------------------------------


@pytest.fixture
def media_world(p10):
    """Both roles ACTIVE; the media role holds the target's reference content (60 points)."""
    t = p10.register_target(*synth.film(41), title="T")
    p10.bootstrap()
    return p10, t


def test_cap4_bulk_load_beyond_its_admission_draws_on_the_ledger(media_world) -> None:
    """CAP-4: the corpus grows after the rebuild admission (P = ⌈1.05 × N⌉) by more than the headroom,
    before the bulk load reaches the new subjects (their own mutators have not run). The bulk load
    and catch-up may write only within P; every point beyond it is reserved in the ledger first. The
    ledger stays ≥ Σ exact counts of every collection at every bucket boundary of the build."""
    p10, _ = media_world
    new: list[str] = []
    samples: list[tuple[int, int]] = []

    def grow(**_):
        for i in range(5):   # 5 x 6 = 30 points > 5 % of the 60 admitted
            new.append(p10.ingest_media(*synth.random_subject(400 + i)))

    p10.hooks.on("builder.after_admission", grow)
    p10.hooks.on("builder.after_bucket", lambda **_: samples.append((p10.sys.capacity.read()["reserved"],
                                                                     resident(p10))))
    res = p10.sys.rebuild(p10.spec, MEDIA, promote=False, wait_boundary=False)
    p10.hooks.clear()
    assert new and samples
    g = res["generation"]
    holder = p10.sys.gens.row(p10.spec, MEDIA, g)["reserved_points"]
    g_count = p10.index.count(p10.sys.gens.row(p10.spec, MEDIA, g)["collection"])
    assert g_count > holder, "the fixture must exceed the admission headroom"
    bad = [(r, n) for r, n in samples if r < n]
    assert not bad, f"ledger below resident at bucket boundaries: {bad[:5]} (holder {holder}, g {g_count})"
    assert res["state"] == "READY", res
    assert p10.sys.capacity.read()["reserved"] >= resident(p10)


def test_cap4_bulk_load_beyond_admission_refused_at_the_limit(media_world) -> None:
    """CAP-4 (refusal): as above, but the limit leaves room for the admission, for the new subjects'
    reservations in the ACTIVE generation (5 x 6) and for 3 more points only — not for their copies in
    the new generation beyond P. Those writes must be refused before any Qdrant call (rows
    ``BLOCKED: capacity``, no points), the build must not crash, the generation must not be READY
    while subjects are missing, and resident points never exceed the limit."""
    p10, _ = media_world
    new: list[str] = []
    led = p10.sys.capacity.read()["reserved"]
    projected = -(-60 * 105 // 100)                       # ⌈1.05 × 60⌉ of the media role at admission
    tight_limit(p10, 250_000, 250_000 - (led + projected + 30 + 3))

    def grow(**_):
        for i in range(5):
            new.append(p10.ingest_media(*synth.random_subject(500 + i)))

    p10.hooks.on("builder.after_admission", grow)
    res = p10.sys.rebuild(p10.spec, MEDIA, promote=False, wait_boundary=False)
    p10.hooks.clear()
    g = res["generation"]
    row = p10.sys.gens.row(p10.spec, MEDIA, g)
    assert row["reserved_points"] == projected
    assert res["state"] != "READY", res
    assert p10.sys.capacity.read()["reserved"] >= resident(p10)
    assert resident(p10) <= p10.sys.capacity.limit
    rows = [e for b in range(BUCKETS) for e in p10.sys.indexer.entries(p10.spec, MEDIA, b)]
    blocked = [e for e in rows if e["state"] == BLOCKED]
    assert blocked, "growth beyond P must be refused"           # which subject depends on the bucket order
    for e in blocked:
        assert e["blocked_reason"] == "capacity" and (e["applied"] or {}).get(g) != e["seq"]
        assert p10.index.count(row["collection"], subject_filter(MEDIA, e["subject_id"])) == 0


# --- §9a R-7 (a): the catch-up pass killed ------------------------------------------------------


def test_r7a_catch_up_pass_killed_restarts_a_full_pass_never_promoted_partial(media_world, monkeypatch) -> None:
    """R-7 (a): subjects whose intents were written after the bulk load (not yet applied anywhere) must be
    applied by the catch-up pass; the pass is killed after its first apply. The generation stays
    CATCHING_UP and cannot be promoted; a new build adopts it, runs a full catch-up pass, validates and
    promotes once; every row is applied to it and its manifest equals the artifact-only digest."""
    p10, _ = media_world
    late: list = []

    def after_last_bucket(**ctx):
        if ctx["bucket"] == BUCKETS - 1 and not late:
            for i in range(4):
                s = p10.media(p10.ingest_media(*synth.random_subject(700 + i)))
                p10.sys.indexer.write_intent(s)          # live intent, its mutator "paused" before applying
                late.append(s)

    p10.hooks.on("builder.after_bucket", after_last_bucket)
    real_apply = p10.sys.indexer.apply
    calls = {"n": 0}

    def apply_then_crash(*a, **k):
        if k.get("covered_gens") and late:           # inside the catch-up pass
            calls["n"] += 1
            if calls["n"] == 2:
                raise CrashError("kill -9 of the catch-up pass")
        return real_apply(*a, **k)

    monkeypatch.setattr(p10.sys.indexer, "apply", apply_then_crash)
    with pytest.raises(CrashError):
        p10.sys.rebuild(p10.spec, MEDIA, promote=True, wait_boundary=False)
    p10.hooks.clear()
    monkeypatch.setattr(p10.sys.indexer, "apply", real_apply)
    _, gens = p10.sys.gens.partition(p10.spec, MEDIA)
    g = max(gens)
    assert gens[g]["state"] == "CATCHING_UP"
    applied = [s for s in late if (entry(p10, s)["applied"] or {}).get(g) == entry(p10, s)["seq"]]
    assert 0 < len(applied) < len(late), "the crash must leave a partial pass"
    assert not p10.sys.gens.promote(p10.spec, MEDIA, g)           # never promoted on a partial pass
    p10.sys.gens.release_lease(p10.spec, MEDIA, gens[g]["build_epoch"])   # the dead builder's lease
    res = p10.sys.rebuild(p10.spec, MEDIA, promote=True, wait_boundary=False)
    assert res["generation"] == g and res.get("promoted"), res
    for s in late:
        e = entry(p10, s)
        assert (e["applied"] or {}).get(g) == e["seq"] and e["state"] == "APPLIED"
    assert p10.sys.gens.row(p10.spec, MEDIA, g)["manifest_digest"] == p10.sys.builder.manifest_digest(p10.spec, MEDIA)
    assert p10.sys.capacity.read()["reserved"] >= resident(p10)


# --- §9a R-10: promotion, a cached pointer and a slow search step --------------------------------


def test_r10_cached_pointer_slow_step_abandoned_and_repeated_on_the_new_pointer(p10, monkeypatch) -> None:
    """R-10: a search step pinned to g1 sends its first round; g2 is promoted while that round is in
    flight (the promoter then 'crashes' — nothing else runs) and the round's response is delayed past the
    step deadline. Readers never see an empty pointer; the old-pointer round succeeds during the grace;
    every query of a step goes to its pinned generation; no query is sent after its step's deadline; the
    slow step is abandoned and repeated on g2; no query reaches g1 after ``drain_horizon_at``; the
    reconciler retires g1 after the grace and a post-horizon pass."""
    m = p10.sys.p.settings.matching.model_copy(update={"k0": 1, "workers": 1})   # adaptive k: several rounds
    p10.sys.p.settings = p10.sys.p.settings.model_copy(update={"matching": m})
    timeout = m.search_timeout_s
    tid, ver, _ = p10.register_target(*synth.film(61), title="T")
    p10.bootstrap()
    p10.drive()
    g1 = p10.sys.gens.active(p10.spec, TARGET, fresh=True)
    res = p10.sys.rebuild(p10.spec, TARGET, promote=False, wait_boundary=False)
    assert res["state"] == "READY"
    g2 = p10.sys.gens.row(p10.spec, TARGET, res["generation"])
    events: list[tuple] = []                       # ("ptr", gen, read_mono) | ("send", mono, wall, collection)
    real_active = p10.sys.searcher.gens.active

    def active(spec, role, **kw):
        ptr = real_active(spec, role, **kw)
        if role == TARGET and ptr is not None:
            events.append(("ptr", ptr.generation, ptr.read_mono))
        return ptr

    monkeypatch.setattr(p10.sys.searcher.gens, "active", active)
    in_flight, promoted = threading.Event(), threading.Event()
    real_search = p10.index.search_batch
    old_round_ok: list[bool] = []

    def search_batch(name, reqs, **kw):
        if name in (g1.collection, g2["collection"]):
            events.append(("send", time.monotonic(), p10.sys.p.now(), name))
        out = real_search(name, reqs, **kw)
        if name == g1.collection and not in_flight.is_set():
            in_flight.set()
            assert promoted.wait(30)
            old_round_ok.append(True)               # the old-pointer query answered during the grace
            time.sleep(timeout + 0.3)                # the response arrives after the step deadline
        return out

    monkeypatch.setattr(p10.index, "search_batch", search_batch)
    cid = p10.ingest_media(*synth.clip_of(61, 40.0, 90.0))
    p10.sys.discovery.desire(p10.media(cid))
    empty: list[int] = []
    stop = threading.Event()

    def sampler():
        while not stop.is_set():
            if real_active(p10.spec, TARGET, fresh=True) is None:
                empty.append(1)

    worker = threading.Thread(target=p10.run_matcher)
    worker.start()
    assert in_flight.wait(60)
    samp = threading.Thread(target=sampler)
    samp.start()
    assert p10.sys.gens.promote(p10.spec, TARGET, g2["generation"])   # the LWT; the promoter then dies
    stop.set()
    samp.join()
    promoted.set()
    worker.join(120)
    task = p10.sys.tasks.row(p10.media(cid).subject_id, p10.spec, KIND_A)
    assert "search_deadline" in (task.get("waiting_reason") or ""), task
    time.sleep(1.0)
    p10.run_matcher()                                 # the repeated step reads a fresh pointer
    task = p10.sys.tasks.row(p10.media(cid).subject_id, p10.spec, KIND_A)
    assert task["state"] == "DONE" and task["searched_generation"] == g2["generation"], task
    assert any(r["content_id"] == cid and r["target_id"] == tid for r in p10.s.db.rows("SELECT * FROM {ks}.matches"))
    assert not empty and old_round_ok
    # per step: one pinned collection, every send before the step deadline
    steps: list[dict] = []
    for ev in events:
        if ev[0] == "ptr":
            steps.append({"gen": ev[1], "deadline": ev[2] + timeout, "sends": []})
        elif steps:
            steps[-1]["sends"].append(ev)
    gname = {g1.generation: g1.collection, g2["generation"]: g2["collection"]}
    used = [s for s in steps if s["sends"]]
    assert len(used) >= 2 and used[0]["gen"] == g1.generation and used[-1]["gen"] == g2["generation"]
    for s in used:
        assert {e[3] for e in s["sends"]} == {gname[s["gen"]]}
        assert all(e[1] <= s["deadline"] for e in s["sends"]), "a query was sent after its step deadline"
    horizon = utc(p10.sys.gens.row(p10.spec, TARGET, g1.generation)["drain_horizon_at"])
    assert all(e[2] <= horizon for e in events if e[0] == "send" and e[3] == g1.collection)
    # retirement after the grace and a clean pass that started after the horizon
    ix = p10.sys.p.settings.index.model_copy(update={"drain_grace_s": 0.5})
    p10.sys.p.settings = p10.sys.p.settings.model_copy(update={"index": ix})
    while p10.sys.p.now() < horizon + timedelta(seconds=0.6):
        time.sleep(0.2)
    p10.sys.generations_pass()
    old = p10.sys.gens.row(p10.spec, TARGET, g1.generation)
    assert old["state"] == "RETIRED" and not p10.index.collection_exists(g1.collection)
    assert utc(old["verify_started_at"]) >= horizon


# --- §9a R-21: the reconciler killed mid verification pass ---------------------------------------


def test_r21_reconciler_killed_mid_verification_pass_converges_exactly_once(media_world) -> None:
    """R-21: R-17 losses (3 subjects' current points deleted) and an R-16 orphan; the verification pass is
    killed right after its first occurrence became durable. The restarted pass and the reconciler repair
    every divergence exactly once in effect (one occurrence per lost subject, closed RERUN_DONE; points
    restored; orphan removed); repeating the pass changes nothing. (The implementation restarts the pass
    from its beginning rather than from a persisted cursor — design §20 N-25.)"""
    p10, _ = media_world
    subjects = [p10.media(p10.ingest_media(*synth.random_subject(800 + i))) for i in range(6)]
    for s in subjects:
        p10.sys.indexer.ensure(s)
    p10.drive()
    g, name = active_collection(p10, MEDIA)
    lost = subjects[:3]
    for s in lost:
        p10.index.delete_by_filter(name, subject_seq_eq(MEDIA, s.subject_id, int(entry(p10, s)["seq"])))
    vec, _ = synth.random_subject(899, 2)
    p10.index.upsert(name, [Point(point_id("rep_orphan_r21", 1, k), vec[k],
                                  {"rep": "rep_orphan_r21", "cid": "cnt_none", "spec": p10.spec, "k": k, "t": 0.0,
                                   "seq": 1}) for k in range(2)])
    opened: list[str] = []

    def kill_after_first(**ctx):
        opened.append(ctx["subject"])
        if len(opened) == 1:
            raise CrashError("kill -9 after the first durable occurrence")

    p10.hooks.on("recovery.after_open", kill_after_first)
    with pytest.raises(CrashError):
        p10.sys.verify_pass()
    p10.hooks.clear()
    first = [r for r in p10.sys.recovery.occurrences(p10.spec, MEDIA)]
    assert len(first) == 1 and first[0]["state"] == "OPEN"          # durable before the crash
    p10.sys.verify_pass()                                            # the restarted pass
    p10.drive()
    occ = p10.sys.recovery.occurrences(p10.spec, MEDIA)
    lost_ids = {s.subject_id for s in lost}
    assert sorted(r["subject_id"] for r in occ) == sorted(lost_ids)  # exactly one occurrence per lost subject
    assert all(r["state"] == "CLOSED" and r["close_reason"] == "RERUN_DONE" for r in occ)
    for s in subjects:
        assert p10.index.count(name, subject_seq_eq(MEDIA, s.subject_id, int(entry(p10, s)["seq"]))) == s.segment_count
    assert p10.index.count(name, subject_filter(MEDIA, "rep_orphan_r21")) == 0
    before = {(r["subject_id"], r["occurrence"]): dict(r) for r in occ}
    again = p10.sys.checker.verify_generation(p10.spec, MEDIA, g)
    assert again["short"] == again["stale"] == again["orphans"] == again["deleted_rows_points"] == 0
    after = {(r["subject_id"], r["occurrence"]): dict(r) for r in p10.sys.recovery.occurrences(p10.spec, MEDIA)}
    assert after == before
    assert p10.sys.recovery.counters(p10.spec) == (0, 0)


# --- §9e CAP-9: release idempotency and audit races -------------------------------------------


def test_cap9_release_idempotency_and_audit_races(media_world, monkeypatch) -> None:
    """CAP-9 (C-14l): (a) a worker killed after its cleanup re-runs and reserves again; (b) the response
    of a reservation CAS is dropped after Scylla applied it; (c) audits run continuously while live
    writes and a rebuild admission proceed; (d) a builder dies after the admission batch, before the
    CREATED row. Pass: ledger ≥ Σ exact counts at every sample; no statement other than the audit lowers
    ``reserved`` (statement log); duplicate reservations are removed by the next audit; the orphan
    holder row is kept until the build lease expired, then removed."""
    from fingerprinter.db import LwtUnknownError

    p10, _ = media_world
    ix = p10.sys.p.settings.index.model_copy(update={"build_lease_s": 2.0})
    p10.sys.p.settings = p10.sys.p.settings.model_copy(update={"index": ix})
    cap = p10.sys.capacity
    in_audit = threading.local()
    lowered_outside: list[tuple] = []
    log: list[tuple] = []
    real_lwt = p10.s.db.lwt
    drop = {"armed": False}

    def lwt(cql, *args):
        is_cas = "index_capacity SET reserved = ?" in cql
        if is_cas and not getattr(in_audit, "on", False):
            new, old = args[0], args[3]       # reserve, admission batch and audit: (new, at, instance, old)
            if new < old:
                lowered_outside.append((cql[:80], new, old))
        res = real_lwt(cql, *args)
        if is_cas:
            log.append((time.monotonic(), "audit" if getattr(in_audit, "on", False) else "reserve", args[0],
                        res.applied))
            if drop["armed"] and res.applied and not getattr(in_audit, "on", False):
                drop["armed"] = False
                raise LwtUnknownError("injected: reservation CAS response dropped after apply")
        return res

    monkeypatch.setattr(p10.s.db, "lwt", lwt)
    real_audit = cap.audit

    def audit():
        in_audit.on = True
        try:
            return real_audit()
        finally:
            in_audit.on = False

    monkeypatch.setattr(cap, "audit", audit)
    violations: list[tuple] = []
    stop = threading.Event()

    def background():
        while not stop.is_set():
            audit()
            before = resident(p10)
            r = cap.read()["reserved"]
            if r < before:
                violations.append((r, before))
            time.sleep(0.05)

    bg = threading.Thread(target=background, daemon=True)
    bg.start()
    try:
        # (a) killed after its cleanup, before bookkeeping; the re-run reserves again
        s1 = p10.media(p10.ingest_media(*synth.random_subject(910)))
        p10.hooks.on("indexer.before_bookkeeping", lambda **_: (_ for _ in ()).throw(CrashError("after cleanup")))
        with pytest.raises(CrashError):
            p10.sys.indexer.ensure(s1)
        p10.hooks.clear()
        p10.sys.indexer.ensure(s1)
        # (b) a dropped reservation response: reserve() retries and over-reserves (safe)
        s2 = p10.media(p10.ingest_media(*synth.random_subject(911)))
        drop["armed"] = True
        p10.sys.indexer.ensure(s2)
        assert not drop["armed"], "the dropped-response path was not exercised"
        # (c) live writes and a rebuild admission while audits run
        for i in range(6):
            p10.sys.indexer.ensure(p10.media(p10.ingest_media(*synth.random_subject(920 + i))))
        res = p10.sys.rebuild(p10.spec, MEDIA, promote=False, wait_boundary=False)
        assert res["state"] == "READY"
    finally:
        stop.set()
        bg.join(10)
    assert not violations, violations[:5]
    assert not lowered_outside, lowered_outside[:5]
    a = audit()
    assert a.applied and cap.read()["reserved"] == a.bound >= resident(p10)
    # (d) dies after the admission batch, before CREATED (kill -9: no lease release)
    monkeypatch.setattr(p10.sys.gens, "release_lease", lambda *a_, **k_: None)
    p10.hooks.on("builder.after_admission", lambda **_: (_ for _ in ()).throw(CrashError("after admission")))
    with pytest.raises(CrashError):
        p10.sys.builder.build(p10.spec, TARGET)
    p10.hooks.clear()
    led = cap.read()
    orphan = [h for h in led["holders"] if h.startswith("gen:") and ":target:" in h
              and p10.sys.gens.row(p10.spec, TARGET, int(h.rsplit(":", 1)[1])) is None]
    assert len(orphan) == 1
    audit()
    assert orphan[0] in cap.read()["holders"]          # kept while the build lease is alive
    time.sleep(2.2)
    audit()
    assert orphan[0] not in cap.read()["holders"]      # removed after the lease expired
    assert cap.read()["reserved"] >= resident(p10)
    assert not lowered_outside


# --- §9d K-4: two bindings of one target version (two specs) ---------------------------------------


def test_k4_two_spec_bindings_of_one_target_version_are_not_collapsed(p10) -> None:
    """K-4: one target version bound under two specs (``dinov2-seg/2`` and ``dinov2-seg/1``, the second
    binding created by P9's own backfill and commit path) and a media clip represented under both: two
    bindings, two target subjects, two index rows in separate spec partitions and collections, and two
    ``matches`` rows with distinct MatchIds — one per spec, never collapsed and never cross-spec."""
    from fingerprinter.backfill import Backfill
    from fingerprinter.coord import uuid7
    from fingerprinter.spec import RUNTIME_PROFILE, dinov2_seg_v1

    st = p10.s
    tid, ver, ref_cid = p10.register_target(*synth.film(71), title="T")
    clip_vec = synth.clip_of(71, 40.0, 90.0)
    m_cid = p10.ingest_media(*clip_vec)
    spec1 = dinov2_seg_v1()
    d1, d2 = str(spec1.digest), p10.spec
    assert d1 != d2
    st.store.add_spec(spec1, status="active", actor="test", origin_profile=RUNTIME_PROFILE)
    st.store.approve_compat(d1, RUNTIME_PROFILE, actor="test", evidence="test")
    bf = Backfill(st.store, st.requests)
    job = bf.create(d2, d1, actor="test")
    bf.run(job, actor="test")
    film_vec = synth.film(71)
    for cid, vec in ((ref_cid, film_vec), (m_cid, clip_vec)):
        req = next(r for r in st.requests.all() if r["content_id"] == cid and d1 in (r.get("resolved_spec"),
                                                                                     r.get("spec_key")))
        row = st.requests.claim(cid, req["spec_key"], uuid7())
        assert st.committer.commit(row, spec1, p10.artifact(*vec)) == "won"
    bindings = {b["spec_digest"]: b for b in st.targets.bindings(tid) if b["version"] == ver}
    assert set(bindings) == {d1, d2} and all(b["state"] == "READY" for b in bindings.values())
    assert bindings[d1]["target_representation_id"] != bindings[d2]["target_representation_id"]
    p10.sys.init_spec(d1, actor="test")
    for d in (d2, d1):
        for role in (TARGET, MEDIA):
            assert p10.sys.rebuild(d, role, promote=True, wait_boundary=False).get("promoted")
    p10.drive()
    subjects = {d: p10.sys.p.target_subject(tid, ver, d) for d in (d1, d2)}
    assert subjects[d1].subject_id != subjects[d2].subject_id
    collections = set()
    for d, s in subjects.items():
        e = p10.sys.indexer.entry(d, TARGET, s.bucket, s.subject_id)
        assert e is not None and e["state"] == "APPLIED"
        ptr = p10.sys.gens.active(d, TARGET, fresh=True)
        collections.add(ptr.collection)
        assert p10.index.count(ptr.collection, subject_seq_eq(TARGET, s.subject_id, int(e["seq"]))) == s.segment_count
        other = subjects[d1 if d == d2 else d2]
        assert p10.index.count(ptr.collection, subject_filter(TARGET, other.subject_id)) == 0
    assert len(collections) == 2
    rows = [r for r in st.db.rows("SELECT * FROM {ks}.matches") if r["content_id"] == m_cid and r["target_id"] == tid]
    assert sorted(r["target_spec_digest"] for r in rows) == sorted([d1, d2]), rows
    assert all(r["media_spec_digest"] == r["target_spec_digest"] for r in rows)   # never cross-spec
    assert len({r["match_id"] for r in rows}) == 2


# --- O-6 (decisions §6): the P9 republish defect does not reach P10 ------------------------------


def test_o6_p9_republish_into_the_rep_ready_stream_is_rejected_by_the_hint_reader(p10) -> None:
    """O-6 is a P9 defect (``publisher republish --stream <other>`` re-emits ``target.retired`` envelopes
    into whichever stream is named; P9 code unchanged here). Reproduced through P9's own
    ``TargetManager.republish``; P10's only stream reader validates every entry as
    ``representation.ready`` and skips the foreign envelopes (counted ``invalid``): no intent, task or
    cursor stall follows from them. P10 never reads the target streams (D5)."""
    from fingerprinter.discovery import READY

    tid, ver, _ = p10.register_target(*synth.film(81), title="T")
    p10.bootstrap()
    p10.drive()
    p10.s.targets.retire(tid, reason="O-6", actor="test")
    stream = p10.s.coord.stream(READY)
    p10.s.coord.client.xgroup_create(stream, "media-registry", id="$", mkstream=True)  # P9's release condition
    p10.s.pub.register_consumer(READY, "media-registry", actor="test")                   # (P8's group, production)
    before = p10.s.coord.client.xlen(stream)
    n = p10.s.targets.republish(READY)                    # what `publisher republish --stream <ready>` does
    added = p10.s.coord.client.xrange(stream, min="-", max="+")[before:]
    assert n >= 1 and len(added) == n
    assert all(b"target.retired" in (f.get(b"envelope") or b"") for _, f in added)   # the P9 defect, observed
    tasks_before = len(p10.sys.tasks.all())
    out = p10.sys.discovery.read_hints()
    assert out["invalid"] == n and out["applied"] == 0
    row = p10.s.db.one("SELECT cursor_id FROM {ks}.p10_readers WHERE reader = 'rep-ready'")
    last = added[-1][0].decode() if isinstance(added[-1][0], bytes) else added[-1][0]
    assert row["cursor_id"] == last                      # the reader moved past them (no stall)
    assert len(p10.sys.tasks.all()) == tasks_before
