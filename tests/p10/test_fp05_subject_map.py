"""FP-05 Investigation A: the sidecar subject map (point id -> candidate-search metadata). No services needed."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest

from fingerprinter.candidates import CANDIDATE_PAYLOAD_FIELDS, Policy, Searcher
from fingerprinter.generations import Pointer
from fingerprinter.subjectmap import SubjectMap, SubjectMapError, SubjectMaps
from fingerprinter.verify import Vectors
from fingerprinter.vindex import TARGET, Hit, Point, QdrantIndex, SearchRequest, point_id

SPEC = "sha256:" + "ab" * 32


def target_payload(subject: str, seq: int, k: int, *, low: bool = False, tver: int = 3) -> dict:
    # exactly what indexing.payload_for writes (plus the two keys no search hit is read for)
    return {"rep": "rep:" + subject, "cid": "cnt:" + subject, "spec": SPEC, "k": k, "t": 5.0 * k, "trep": subject,
            "tid": "tgt:" + subject, "tver": tver, "lowinfo": low, "lowinfo_rev": 1, "seq": seq}


def media_payload(subject: str, seq: int, k: int) -> dict:
    return {"rep": subject, "cid": "cnt:" + subject, "spec": SPEC, "k": k, "t": 5.0 * k, "seq": seq}


def consumed(pl: dict) -> dict:
    return {k: v for k, v in pl.items() if k in CANDIDATE_PAYLOAD_FIELDS}


def populate(sm: SubjectMap, subjects: int = 20, segs: int = 6, seq: int = 1, role: str = TARGET) -> dict[str, dict]:
    truth: dict[str, dict] = {}
    field = "trep" if role == TARGET else "rep"
    mk = target_payload if role == TARGET else media_payload
    for s in range(subjects):
        pts = []
        for k in range(segs):
            pl = mk(f"s{s}", seq, k, **({"low": k % 3 == 0} if role == TARGET else {}))
            pid = point_id(f"s{s}", seq, k)
            truth[pid] = consumed(pl)
            pts.append((pid, pl))
        sm.record_many(pts, field)
    return truth


def new_map(collection: str = "fp_c_target_g2", generation: int = 2, role: str = TARGET) -> SubjectMap:
    return SubjectMap(collection, spec=SPEC, role=role, generation=generation)


# --- point-id resolution ------------------------------------------------------------------------------------


@pytest.mark.parametrize("role", ["target", "media"])
def test_resolves_exactly_the_payload_keys_the_matcher_reads(role):
    sm = new_map(role=role)
    truth = populate(sm, role=role)
    ids = list(truth)
    assert sm.get(ids) == [truth[i] for i in ids]
    assert len(sm) == len(truth)


def test_resolution_survives_checkpoint_and_reload_bit_for_bit(tmp_path):
    sm = new_map()
    truth = populate(sm)
    sm.save(tmp_path)
    sm.close()
    for mmap in (False, True):
        back = SubjectMap.load(tmp_path, sm.collection, spec=SPEC, role=TARGET, generation=2, mmap=mmap)
        assert back.get(list(truth)) == [truth[i] for i in truth]
        assert len(back) == len(truth)
        back.close()


def test_missing_identifier_is_none_never_a_guess():
    sm = new_map()
    populate(sm)
    unknown = point_id("nobody", 1, 0)
    assert sm.get([unknown]) == [None]
    sm.save  # noqa: B018 - (attribute access only)


def test_duplicate_identifier_keeps_one_entry_and_the_last_write():
    sm = new_map()
    pid = point_id("s", 1, 0)
    sm.record(pid, target_payload("s", 1, 0, low=False), "trep")
    sm.record(pid, target_payload("s", 1, 0, low=True), "trep")
    assert len(sm) == 1 and sm.get([pid])[0]["lowinfo"] is True


def test_duplicate_between_base_and_overlay_is_one_entry_after_compaction(tmp_path):
    sm = new_map()
    pid = point_id("s", 1, 0)
    sm.record(pid, target_payload("s", 1, 0, low=False), "trep")
    sm.save(tmp_path)                                    # now in the frozen base
    sm.record(pid, target_payload("s", 1, 0, low=True), "trep")
    assert sm.get([pid])[0]["lowinfo"] is True           # the overlay wins
    sm.save(tmp_path)
    assert len(sm) == 1 and sm.get([pid])[0]["lowinfo"] is True


def test_reupsert_cleanup_removes_older_seq_in_overlay_and_in_base(tmp_path):
    sm = new_map()
    old = populate(sm, subjects=3, segs=4, seq=1)
    sm.save(tmp_path)                                    # seq 1 entries are in the frozen base
    new = {}
    for k in range(4):
        pl = target_payload("s1", 2, k)
        new[point_id("s1", 2, k)] = consumed(pl)
        sm.record(point_id("s1", 2, k), pl, "trep")
    assert sm.remove_subject("s1", seq_lt=2) == 4        # s1's seq 1 gone; s0/s2 untouched
    assert all(m is None for m in sm.get([point_id("s1", 1, k) for k in range(4)]))
    assert sm.get([point_id("s1", 2, k) for k in range(4)]) == [new[point_id("s1", 2, k)] for k in range(4)]
    assert sm.get([point_id("s0", 1, 0)])[0] == old[point_id("s0", 1, 0)]
    assert len(sm) == 4 * 2 + 4
    sm.remove_subject("s1", seq_lte=2)                   # a DELETE
    assert sm.get([point_id("s1", 2, 0)]) == [None]
    sm.save(tmp_path)                                    # compaction drops the tombstones
    back = SubjectMap.load(tmp_path, sm.collection, spec=SPEC, role=TARGET, generation=2)
    assert len(back) == 8 and back.get([point_id("s1", 2, 0)]) == [None]


# --- generation identity / staleness -------------------------------------------------------------------------


def test_load_refuses_a_map_of_another_generation_collection_spec_or_role(tmp_path):
    sm = new_map()
    populate(sm)
    sm.save(tmp_path)
    ok = {"spec": SPEC, "role": TARGET, "generation": 2}
    SubjectMap.load(tmp_path, sm.collection, **ok).close()
    for bad in ({"generation": 3}, {"spec": "sha256:" + "cd" * 32}, {"role": "media"}):
        with pytest.raises(SubjectMapError):
            SubjectMap.load(tmp_path, sm.collection, **{**ok, **bad})
    # the same files copied under another collection name (e.g. a stale directory renamed) are refused too
    os.rename(tmp_path / sm.collection, tmp_path / "fp_c_target_g3")
    with pytest.raises(SubjectMapError):
        SubjectMap.load(tmp_path, "fp_c_target_g3", **{**ok, "generation": 3})


def test_load_refuses_corruption_and_incomplete_directories(tmp_path):
    sm = new_map()
    populate(sm)
    d = sm.save(tmp_path)
    kw = {"spec": SPEC, "role": TARGET, "generation": 2}
    raw = (d / "seq.bin").read_bytes()
    (d / "seq.bin").write_bytes(raw[:-8] + b"\x07" * 8)           # bit rot
    with pytest.raises(SubjectMapError, match="checksum"):
        SubjectMap.load(tmp_path, sm.collection, **kw)
    (d / "seq.bin").write_bytes(raw)
    SubjectMap.load(tmp_path, sm.collection, **kw).close()
    (d / "MANIFEST.json").unlink()                                # the manifest is written last: no manifest = incomplete
    with pytest.raises(SubjectMapError):
        SubjectMap.load(tmp_path, sm.collection, **kw)
    with pytest.raises(SubjectMapError):
        SubjectMap.load(tmp_path, "never_built", **kw)


def test_crash_during_checkpoint_leaves_the_previous_complete_map_active(tmp_path, monkeypatch):
    sm = new_map()
    truth = populate(sm)
    sm.save(tmp_path)
    populate(sm, subjects=30)                                      # more points, not yet checkpointed
    real_replace = os.replace
    calls = {"n": 0}

    def crash(src, dst):
        calls["n"] += 1
        raise OSError("simulated crash before activation")

    monkeypatch.setattr(os, "replace", crash)
    with pytest.raises(OSError):
        sm.save(tmp_path)
    monkeypatch.setattr(os, "replace", real_replace)
    back = SubjectMap.load(tmp_path, sm.collection, spec=SPEC, role=TARGET, generation=2)
    # the old complete checkpoint is intact, and the write-ahead journal restores the writes made after it
    assert len(back) == 30 * 6 and back.get(list(truth)) == [truth[i] for i in truth]
    assert any(p.name.startswith(".") for p in tmp_path.iterdir())     # the unfinished temp dir was never activated


def test_journal_replays_live_writes_and_ignores_a_torn_tail(tmp_path):
    sm = new_map()
    populate(sm, subjects=2)
    sm.save(tmp_path)                                              # checkpoint, journal starts
    pl = target_payload("live", 1, 0)
    sm.record(point_id("live", 1, 0), pl, "trep")
    sm.record(point_id("live", 1, 1), target_payload("live", 1, 1), "trep")
    sm.remove_subject("s0", seq_lte=1)
    sm.close()
    j = tmp_path / sm.collection / "journal.log"
    with open(j, "ab") as fh:
        fh.write(b"deadbeef {\"op\":\"+\",\"id\":")                # a torn, CRC-less tail from a crash
    back = SubjectMap.load(tmp_path, sm.collection, spec=SPEC, role=TARGET, generation=2)
    assert back.get([point_id("live", 1, 0)])[0] == consumed(pl)
    assert back.get([point_id("s0", 1, 0)]) == [None]
    assert len(back) == 6 + 2                                      # s1's 6 + the 2 live points


# --- registry: the active collection and its map can never silently disagree --------------------------------


def test_registry_serves_only_a_map_whose_count_equals_qdrants(tmp_path):
    maps = SubjectMaps(tmp_path, verify_ttl_s=0.0)
    sm = maps.create("c_g2", spec=SPEC, role=TARGET, generation=2)
    populate(sm, subjects=4, segs=5)
    sm.save(tmp_path)
    kw = {"spec": SPEC, "role": TARGET, "generation": 2}
    assert maps.resolve("c_g2", count_fn=lambda c: 20, **kw) is sm
    assert maps.resolve("c_g2", count_fn=lambda c: 21, **kw) is None          # Qdrant has a point the map lacks
    assert "count mismatch" in maps.refusals()["c_g2"]
    assert maps.resolve("c_g2", count_fn=lambda c: 19, **kw) is None          # the map has a stale extra
    assert maps.resolve("c_g2", count_fn=lambda c: 20, **kw) is sm            # recovers when consistent again


def test_registry_refuses_a_map_for_the_wrong_generation_and_unknown_collections(tmp_path):
    maps = SubjectMaps(tmp_path, verify_ttl_s=0.0)
    sm = maps.create("c_g2", spec=SPEC, role=TARGET, generation=2)
    populate(sm, subjects=1, segs=2)
    sm.save(tmp_path)
    assert maps.resolve("c_g2", spec=SPEC, role=TARGET, generation=3, count_fn=lambda c: 2) is None
    assert maps.resolve("c_g9", spec=SPEC, role=TARGET, generation=9, count_fn=lambda c: 0) is None
    assert SubjectMaps(None).resolve("c_g2", spec=SPEC, role=TARGET, generation=2, count_fn=lambda c: 2) is None


class FakeScrollIndex:
    """The slice of VectorIndex that seal/rebuild/search use, over a dict of id -> (vector, payload)."""

    def __init__(self, points: dict[str, tuple[np.ndarray, dict]]) -> None:
        self.points = points
        self.sent: list[SearchRequest] = []

    def scroll_all(self, name, flt=None, *, page=256, with_vector=False):
        for pid, (_v, pl) in self.points.items():
            yield Point(pid, None, dict(pl))

    def count(self, name, flt=None):
        return len(self.points)

    def retrieve(self, name, ids, *, with_vector=True):
        return [Point(i, None, dict(self.points[i][1])) for i in ids if i in self.points]


def test_seal_rebuilds_a_missing_or_incomplete_map_and_refuses_what_it_cannot_make_equal(tmp_path):
    pts = {}
    for s in range(5):
        for k in range(4):
            pts[point_id(f"s{s}", 1, k)] = (None, target_payload(f"s{s}", 1, k, low=k == 2))
    ix = FakeScrollIndex(pts)
    maps = SubjectMaps(tmp_path)
    kw = {"spec": SPEC, "role": TARGET, "generation": 2}
    maps.seal(ix, "c_g2", exact=20, **kw)                    # no map at all (e.g. crash during the build)
    back = SubjectMap.load(tmp_path, "c_g2", **kw)
    assert len(back) == 20 and {i: consumed(p) for i, (_v, p) in pts.items()} == dict(back.items())
    with pytest.raises(SubjectMapError):                       # Qdrant says 21, the scroll only finds 20
        maps.seal(ix, "c_g2", exact=21, **kw)


def test_the_map_is_complete_against_qdrants_own_payloads(tmp_path):
    """Mapping completeness: every point Qdrant holds resolves, with identical metadata."""
    pts = {point_id(f"s{s}", 2, k): (None, target_payload(f"s{s}", 2, k, low=k % 2 == 0)) for s in range(7) for k in range(5)}
    ix = FakeScrollIndex(pts)
    maps = SubjectMaps(tmp_path)
    sm = maps.rebuild_from_index(ix, "c_g2", spec=SPEC, role=TARGET, generation=2)
    got = sm.get(list(pts))
    assert None not in got and got == [consumed(p) for _v, p in pts.values()]


# --- the matcher: the map path and the Qdrant-payload path produce the same candidates ----------------------


def fake_p10(index, maps, count_ok=True):
    cfg = SimpleNamespace(matching=SimpleNamespace(search_timeout_s=30.0),
                          index=SimpleNamespace(search_ef=None, oversampling=2.0))
    return SimpleNamespace(settings=cfg, index=index, hook=lambda *a, **k: None, subject_maps=maps)


class SearchIndex(FakeScrollIndex):
    def search_batch(self, name, reqs, **kw):
        self.sent.extend(reqs)
        pool = sorted(self.points, key=lambda i: i)
        out = []
        for r in reqs:
            hits = []
            for rank, pid in enumerate(pool):
                score = 0.99 - 0.004 * rank
                if r.score_threshold is not None and score < r.score_threshold:
                    break
                pl = self.points[pid][1]
                body = {} if r.payload_fields == () else (dict(pl) if r.payload_fields is None else
                                                          {k: pl[k] for k in r.payload_fields if k in pl})
                hits.append(Hit(pid, score, body))
            out.append(hits[: r.limit])
        return out


def run_search(p10, spec=SPEC):
    ptr = Pointer(2, "c_g2", 1, None, time.monotonic())  # type: ignore[arg-type]
    gens = SimpleNamespace(active=lambda *a, **k: ptr)
    q = Vectors(np.array([0, 1]), np.array([0.0, 5.0]), np.zeros((2, 4), np.float32))
    pol = Policy(k0=16, k_max=64, floor=0.80, min_hits=1, cap=100)
    return Searcher(p10, gens).search(spec, "media_vs_targets", q, pol, skip_query=set(), exclude_key="s3",
                                      hit_excluded=lambda pl: bool(pl.get("lowinfo")) and pl.get("k") == 0)


def candidates_view(out):
    return [(g.key, sorted(g.segments), g.best, g.point_id, g.payload) for g in out.candidates], out.truncated, \
        out.saturated, {k: v for k, v in out.stats.items() if k not in ("seconds", "subject_map")}


def build_corpus():
    pts = {point_id(f"s{s}", 1, k): (None, target_payload(f"s{s}", 1, k, low=(s + k) % 4 == 0)) for s in range(12)
           for k in range(6)}
    return pts


def test_matcher_output_is_identical_with_and_without_the_map(tmp_path):
    pts = build_corpus()
    baseline = run_search(fake_p10(SearchIndex(dict(pts)), SubjectMaps(None)))
    ix = SearchIndex(dict(pts))
    maps = SubjectMaps(tmp_path, verify_ttl_s=0.0)
    maps.seal(ix, "c_g2", spec=SPEC, role=TARGET, generation=2, exact=len(pts))
    out = run_search(fake_p10(ix, maps))
    assert all(r.payload_fields == () for r in ix.sent)            # Qdrant was asked for no payload at all
    assert out.stats["subject_map"] == {"misses": 0}
    assert candidates_view(out) == candidates_view(baseline)
    assert len(out.candidates) > 3 and out.stats["lowinfo_excluded_hits"] > 0


def test_a_missing_entry_is_fetched_from_qdrant_and_counted_not_dropped(tmp_path):
    pts = build_corpus()
    baseline = run_search(fake_p10(SearchIndex(dict(pts)), SubjectMaps(None)))
    ix = SearchIndex(dict(pts))
    maps = SubjectMaps(tmp_path, verify_ttl_s=0.0)
    maps.seal(ix, "c_g2", spec=SPEC, role=TARGET, generation=2, exact=len(pts))
    sm = maps.get("c_g2")
    victim = point_id("s5", 1, 2)
    sm._base_dead.add(int(victim.replace("-", ""), 16))                # lose one entry (count now short)
    # count would now disagree -> the registry refuses the map and the baseline path runs
    out = run_search(fake_p10(ix, maps))
    assert "subject_map" not in out.stats and candidates_view(out)[:3] == candidates_view(baseline)[:3]
    # a map that passes the count check but lacks an id Qdrant returns (one extra, one missing): resolved via retrieve
    sm.record(point_id("ghost", 1, 0), target_payload("ghost", 1, 0), "trep")
    out = run_search(fake_p10(ix, maps))
    assert out.stats["subject_map"]["misses"] >= 1
    assert candidates_view(out)[:3] == candidates_view(baseline)[:3]


def test_with_payload_is_false_for_an_empty_field_tuple():
    sent: list[dict] = []
    ix = QdrantIndex("http://unused", None)
    ix._call = lambda m, p, body=None, **kw: (sent.append(body), [[]])[1]  # type: ignore[method-assign]
    ix.search_batch("c", [SearchRequest(vector=np.zeros(4, np.float32), limit=3, payload_fields=())],
                    hnsw_ef=None, oversampling=1.0)
    assert sent[0]["searches"][0]["with_payload"] is False
    assert json.dumps(sent[0])  # serialisable


def test_default_configuration_leaves_the_baseline_path_untouched():
    from fingerprinter.config import IndexConfig

    assert IndexConfig().subject_map_dir is None
    assert SubjectMaps(None).enabled is False
