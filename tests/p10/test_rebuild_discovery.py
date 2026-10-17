"""Index drop and rebuild (validation §5, R-20) and work discovery (§9b H-*) on the real stack."""

from __future__ import annotations

import json

import pytest

from fingerprinter.discovery import MIN_TOKEN
from fingerprinter.p10ctx import KIND_A, KIND_B
from tests.p10 import synth
from tests.p10.conftest import VECTORS
from tests.p10.test_flow import clip, film

pytestmark = pytest.mark.p10_integration


def decisions(p10) -> dict[str, tuple]:
    rows = p10.s.db.rows("SELECT * FROM {ks}.matches")
    return {r["match_id"]: (r["verdict"], r["score"], r["alignment"]) for r in rows}


@pytest.mark.skipif(not (VECTORS / "l-pairs").exists(), reason="cached L-PAIRS vectors not present")
def test_drop_and_rebuild_equal_manifests_and_identical_decisions(p10) -> None:
    t1 = p10.register_target(*film("sintel"), title="Sintel")
    t2 = p10.register_target(*film("elephants-dream"), title="ED")
    media = [p10.ingest_media(*clip(c)) for c in ("lp-000-sintel-re-encode.mp4",
                                                  "lp-029-elephants-dream-pillarbox.webm",
                                                  "lp-035-sintel-re-encode.webm")]
    p10.bootstrap()
    p10.drive()
    before = decisions(p10)
    assert len(before) >= 2
    manifests = {}
    for role in ("target", "media"):
        ptr = p10.sys.gens.active(p10.spec, role, fresh=True)
        manifests[role] = p10.sys.gens.row(p10.spec, role, ptr.generation)["manifest_digest"]
        assert manifests[role] == p10.sys.builder.manifest_digest(p10.spec, role)  # = the artifact-only digest
        p10.index.delete_collection(ptr.collection)                              # lose the whole index
    # tasks wait, nothing is decided or failed, no fallback runs (R-20)
    p10.sys.tasks.rerun(p10.media(media[0]), KIND_A, reason="probe", actor="test")
    p10.run_matcher()
    probe = p10.sys.tasks.row(p10.media(media[0]).subject_id, p10.spec, KIND_A)
    assert probe["state"] == "WAITING_INDEX", probe
    # rebuild, target role first; lost ACTIVE generations stay ACTIVE until superseded (no ACTIVE->FAILED)
    for role in ("target", "media"):
        res = p10.sys.rebuild(p10.spec, role, promote=True, wait_boundary=False)
        assert res["state"] == "READY" and res["promoted"], res
        ptr = p10.sys.gens.active(p10.spec, role, fresh=True)
        assert p10.sys.gens.row(p10.spec, role, ptr.generation)["manifest_digest"] == manifests[role]
    # the lost DRAINING generations: unauditable fallback occurrences, then RETIRED
    p10.sys.p.settings = p10.sys.p.settings.model_copy(update={"index": p10.sys.p.settings.index.model_copy(
        update={"drain_grace_s": 0.0})})
    import time

    time.sleep(2.5)  # past the recorded search horizon (active_ttl + search_timeout + margin)
    p10.sys.generations_pass()
    for role in ("target", "media"):
        _, gens = p10.sys.gens.partition(p10.spec, role)
        assert {r["state"] for r in gens.values()} == {"ACTIVE", "RETIRED"}
    occ = p10.sys.recovery.occurrences(p10.spec)
    # 2 targets + 5 media subjects (3 clips and the 2 targets' reference contents)
    assert len(occ) == 7 and all(o["detected_by"] == "unauditable" for o in occ)
    for m in media:
        p10.sys.tasks.rerun(p10.media(m), KIND_A, reason="rebuild-check", actor="test")
    for t in (t1, t2):
        p10.sys.tasks.rerun(p10.target(t[0], t[1]), KIND_B, reason="rebuild-check", actor="test")
    p10.drive(8)
    assert decisions(p10) == before                                   # identical decision set
    assert p10.sys.recovery.counters(p10.spec) == (0, 0)
    # every earlier decision was re-proposed by the rebuilt index (skipped as already decided)
    cands = p10.s.db.rows("SELECT * FROM {ks}.match_candidates ALLOW FILTERING")
    reproposed = {c["match_id"] for c in cands if c.get("reason") == "already_decided"}
    assert set(before) <= reproposed


def _consumer(p10) -> None:
    """Register crawler2's media-registry group so P9 publishes representation.ready (otherwise held)."""
    from tests.v2.conftest import ensure_group

    ensure_group(p10.s, "representation.ready.v1")
    p10.s.pub.register_consumer("representation.ready.v1", "media-registry", actor="test")


def test_h1_h2_h3_hint_reader_idempotent_and_cursor_after_work(p10) -> None:
    _consumer(p10)
    p10.bootstrap()
    cids = [p10.ingest_media(*synth.random_subject(20 + i)) for i in range(3)]
    p10.hooks.on("reader.before_cursor_lwt", lambda **_: (_ for _ in ()).throw(RuntimeError("crash")))
    with pytest.raises(RuntimeError):
        p10.sys.discovery.read_hints()
    p10.hooks.clear()
    for c in cids:  # work recorded before the crash (H-2)
        assert p10.sys.tasks.row(p10.media(c).subject_id, p10.spec, KIND_A) is not None
    out = p10.sys.discovery.read_hints()   # re-read the batch: duplicates are no-ops (H-1, H-3)
    assert out["entries"] == 3
    out = p10.sys.discovery.read_hints()
    assert out["entries"] == 0
    rows = [r for r in p10.sys.tasks.all() if r["kind"] == KIND_A]
    assert len(rows) == 3


def test_h4_second_reader_cannot_advance_cursor(p10) -> None:
    _consumer(p10)
    p10.bootstrap()
    p10.ingest_media(*synth.random_subject(30))
    assert p10.sys.discovery.read_hints()["entries"] == 1
    from fingerprinter.discovery import Discovery
    from fingerprinter.p10ctx import P10

    other_p = P10(p10.sys.p.settings, p10.s.db, p10.s.coord, p10.s.objects, p10.s.store.__class__(
        p10.s.db, p10.s.coord, p10.s.settings, role="host2"), p10.index)
    other = Discovery(other_p, p10.sys.indexer, p10.sys.tasks, None)
    assert other.read_hints().get("not_owner") == 1


def test_h5_trimmed_hints_recovered_by_backstop(p10) -> None:
    _consumer(p10)
    p10.bootstrap()
    c1 = p10.ingest_media(*synth.random_subject(40))
    p10.sys.discovery.read_hints()
    c2 = p10.ingest_media(*synth.random_subject(41))
    stream = p10.s.coord.stream("representation.ready.v1")
    p10.s.coord.client.xtrim(stream, maxlen=0, approximate=False)      # entries vanish before we read them
    p10.ingest_media(*synth.random_subject(42))
    out = p10.sys.discovery.read_hints()
    assert out["gaps"] == 1 and p10.sys.discovery.backstop_requested
    assert p10.sys.tasks.row(p10.media(c2).subject_id, p10.spec, KIND_A) is None
    p10.sys.discovery.scan_reps(p10.spec)
    assert p10.sys.tasks.row(p10.media(c2).subject_id, p10.spec, KIND_A) is not None
    assert c1


def test_h9_targets_scan_partition_granular_on_populated_data(p10) -> None:
    """H-9: >= 300 partitions, >= 20 with >= 4 rows (versions x 2 specs), keys page 3, fetch size 1;
    a kill mid-partition and right before the cursor LWT; every row present at cycle start visited."""
    db = p10.s.db
    other_spec = "sha256:" + "0" * 64
    rows = set()
    for i in range(300):
        tid = f"tgt_h9_{i:04d}"
        nver = 2 if i % 15 == 0 else 1
        for v in range(1, nver + 1):
            db.run("INSERT INTO {ks}.targets (target_id, version, state, content_id) VALUES (?,?,'READY',?)",
                   tid, v, f"cnt_h9_{i}_{v}")
            for spec in (p10.spec, other_spec):
                db.run("INSERT INTO {ks}.target_representations (target_id, version, spec_digest, state, content_id) "
                       "VALUES (?,?,?,'PENDING',?)", tid, v, spec, f"cnt_h9_{i}_{v}")
                rows.add((tid, v, spec))
    assert sum(1 for i in range(300) if i % 15 == 0) >= 20
    d = p10.sys.discovery
    seen: list[tuple] = []
    calls = {"n": 0}

    def crash_mid(**ctx):
        calls["n"] += 1
        if calls["n"] == 7:
            raise RuntimeError("kill before cursor LWT")

    p10.hooks.on("scan.after_partition_before_cursor", crash_mid)
    with pytest.raises(RuntimeError):
        d.scan_targets(keys_page=3, fetch_size=1)
    p10.hooks.clear()
    st = db.one("SELECT last_key FROM {ks}.p10_scans WHERE scan = 'targets' AND spec_digest = '*' AND bucket = 0")
    assert st["last_key"] and not st["last_key"].startswith(str(MIN_TOKEN))
    out = d.scan_targets(keys_page=3, fetch_size=1)
    seen.extend(d.last_visited)
    assert out["cycle_complete"] == 1
    # union of the partial run (partitions before the kill) and the resumed run covers every row
    first_part = {(t, v, s) for (t, v, s) in rows if t in {x[0] for x in seen}}
    missing = rows - {tuple(x) for x in seen}
    done_before_kill = rows - first_part
    assert missing <= done_before_kill  # only partitions completed before the kill are not re-read
    st = db.one("SELECT cycle, last_key FROM {ks}.p10_scans WHERE scan = 'targets' AND spec_digest "
                "= '*' AND bucket = 0")
    assert st["cycle"] == 1 and st["last_key"] == f"{MIN_TOKEN}:"
    full = d.scan_targets(keys_page=3, fetch_size=1)
    assert {tuple(x) for x in d.last_visited} == rows and full["cycle_complete"] == 1
    json.dumps(full)
