"""R-24 — Qdrant 1.15.1 filter semantics the §12.2 mutation protocol relies on (validation §9a).

Executed against the pinned dev Qdrant, with collections created with the
production §4.2 parameters (int8 ``always_ram``, on-disk vectors and payload,
m=32) and the §4.3 payload indexes (``rep``/``trep`` keyword, ``seq``
integer). Every operation is ``wait=true`` and followed immediately by exact
counts per (subject, seq).

(i) DELETE boundary ``seq lte d``; (ii) cleanup boundary ``seq lt u``;
(iii) manifest/pre-count ``seq match``; (iv) bigint ``seq`` near 2^40;
(v) the same after the segment is optimized and indexed; (vi) without the
``seq`` integer index (recorded only — not the production configuration);
(vii) stored float32 vectors returned bit-identically.
Also L-14: ``exact`` + ``quantization.ignore`` scores equal the numpy float32
cosine to 1e-5 and ``score_threshold`` excludes a 0.9499 neighbour of 0.95.
"""

from __future__ import annotations

import time

import numpy as np
import pytest

from fingerprinter.vindex import (
    MEDIA,
    TARGET,
    GenerationParams,
    Point,
    SearchRequest,
    collection_name,
    point_id,
    subject_seq_eq,
    subject_seq_lt,
    subject_seq_lte,
)
from tests.p10.conftest import record_evidence, test_spec_digest

pytestmark = pytest.mark.p10_integration

SEGMENTS = 4
RNG = np.random.default_rng(24)


def unit(n: int, dim: int = 768) -> np.ndarray:
    v = RNG.standard_normal((n, dim)).astype(np.float32)
    return (v / np.linalg.norm(v, axis=1, keepdims=True)).astype(np.float32)


def load(index, name: str, role: str, layout: dict[str, list[int]], vectors: dict) -> None:
    field = "rep" if role == MEDIA else "trep"
    pts = []
    for subject, seqs in layout.items():
        for seq in seqs:
            vec = vectors.setdefault((subject, seq), unit(SEGMENTS))
            for k in range(SEGMENTS):
                payload = {field: subject, "seq": seq, "k": k, "t": 5.0 * k, "spec": "test"}
                if role == TARGET:
                    payload["rep"] = "content-rep-" + subject
                pts.append(Point(point_id(subject, seq, k), vec[k], payload))
    index.upsert(name, pts)


def counts(index, name: str, role: str, layout: dict[str, list[int]]) -> dict[str, int]:
    return {f"{s}@{q}": index.count(name, subject_seq_eq(role, s, q)) for s, seqs in layout.items() for q in seqs}


def make(index, role: str, *, seq_index: bool = True, optimize: bool = False) -> str:
    name = collection_name(test_spec_digest(), role, 1)
    if seq_index:
        index.create_collection(name, GenerationParams(), role)
    else:
        p = GenerationParams()
        index.raw("PUT", f"/collections/{name}", {
            "vectors": {"size": 768, "distance": "Dot", "on_disk": True},
            "hnsw_config": {"m": p.hnsw_m, "ef_construct": p.ef_construct},
            "quantization_config": {"scalar": {"type": "int8", "always_ram": True}},
            "on_disk_payload": True})
        index.raw("PUT", f"/collections/{name}/index?wait=true",
                  {"field_name": "rep" if role == MEDIA else "trep", "field_schema": "keyword"})
    if optimize:
        # test-only: index every segment immediately so the filters run on an indexed segment
        index.raw("PATCH", f"/collections/{name}", {"optimizers_config": {"indexing_threshold": 1}})
    return name


def wait_indexed(index, name: str, expect: int) -> dict:
    deadline = time.monotonic() + 300
    while time.monotonic() < deadline:
        info = index.collection_info(name)
        if info.get("status") == "green" and int(info.get("indexed_vectors_count") or 0) >= expect:
            return info
        time.sleep(0.5)
    raise AssertionError(f"collection {name} not indexed: {index.collection_info(name)}")


def run_boundaries(index, role: str, base: int, *, seq_index: bool = True, optimize: bool = False) -> dict:
    s, s2 = f"S-{base}", f"Sp-{base}"
    layout = {s: [base + 10, base + 11, base + 12], s2: [base + 5, base + 11, base + 12]}
    out: dict = {}
    vectors: dict = {}

    def prepared() -> str:
        name = make(index, role, seq_index=seq_index, optimize=optimize)
        load(index, name, role, layout, vectors)
        if optimize:
            out.setdefault("indexed_info", {})[name] = {
                k: v for k, v in wait_indexed(index, name, 6 * SEGMENTS).items()
                if k in ("status", "indexed_vectors_count", "points_count", "segments_count")}
        assert counts(index, name, role, layout) == {k: SEGMENTS for k in counts(index, name, role, layout)}
        return name

    # (i) DELETE boundary: subject = S AND seq <= 11
    name = prepared()
    index.delete_by_filter(name, subject_seq_lte(role, s, base + 11))
    out["i_delete_lte"] = got = counts(index, name, role, layout)
    assert got == {f"{s}@{base + 10}": 0, f"{s}@{base + 11}": 0, f"{s}@{base + 12}": SEGMENTS,
                   f"{s2}@{base + 5}": SEGMENTS, f"{s2}@{base + 11}": SEGMENTS, f"{s2}@{base + 12}": SEGMENTS}
    # (ii) cleanup boundary on a fresh copy: subject = S AND seq < 12 (strict)
    name = prepared()
    index.delete_by_filter(name, subject_seq_lt(role, s, base + 12))
    out["ii_cleanup_lt"] = got = counts(index, name, role, layout)
    assert got[f"{s}@{base + 10}"] == 0 and got[f"{s}@{base + 11}"] == 0 and got[f"{s}@{base + 12}"] == SEGMENTS
    assert all(got[f"{s2}@{q}"] == SEGMENTS for q in layout[s2])
    # (iii) manifest / pre-count: subject = S AND seq = 12 counts the 12-points only
    name = prepared()
    out["iii_count_eq"] = c = index.count(name, subject_seq_eq(role, s, base + 12))
    assert c == SEGMENTS
    out["iii_total"] = index.count(name)
    assert out["iii_total"] == 6 * SEGMENTS
    return out


@pytest.mark.parametrize("role", [MEDIA, TARGET])
def test_r24_filter_boundaries(qdrant, role: str) -> None:
    evidence = {"qdrant_version": qdrant.version(), "role": role, "params": GenerationParams().document()}
    evidence["small_seq"] = run_boundaries(qdrant, role, 0)                      # (i)-(iii)
    evidence["bigint_seq_2_40"] = run_boundaries(qdrant, role, 2**40)            # (iv)
    evidence["indexed_segments"] = run_boundaries(qdrant, role, 7, optimize=True)  # (v)
    evidence["indexed_segments_bigint"] = run_boundaries(qdrant, role, 2**40 + 7, optimize=True)
    assert evidence["qdrant_version"] == "1.15.1"
    record_evidence(f"r24-qdrant-filters-{role}", evidence)


def test_r24_vi_without_seq_index_recorded(qdrant) -> None:
    """(vi) recorded only: the protocol's production configuration has the integer index."""
    outcome: dict = {}
    try:
        outcome["result"] = run_boundaries(qdrant, MEDIA, 3, seq_index=False)
        outcome["same_semantics_without_index"] = True
    except Exception as exc:  # noqa: BLE001 - recorded, not asserted
        outcome["same_semantics_without_index"] = False
        outcome["error"] = f"{type(exc).__name__}: {exc}"
    record_evidence("r24-vi-no-seq-index", outcome)


def test_r24_vii_vectors_bit_identical(qdrant) -> None:
    name = make(qdrant, MEDIA)
    vec = unit(64)
    pts = [Point(point_id("S", 1, k), vec[k], {"rep": "S", "seq": 1, "k": k}) for k in range(64)]
    qdrant.upsert(name, pts)
    got = {p.id: p for p in qdrant.retrieve(name, [p.id for p in pts], with_vector=True)}
    same = [got[p.id].vector.astype(np.float32).tobytes() == np.asarray(p.vector, np.float32).tobytes() for p in pts]
    scrolled = {p.id: p for p in qdrant.scroll_all(name, with_vector=True)}
    same_scroll = [scrolled[p.id].vector.tobytes() == np.asarray(p.vector, np.float32).tobytes() for p in pts]
    # after optimization (int8 quantized index + on-disk originals)
    qdrant.raw("PATCH", f"/collections/{name}", {"optimizers_config": {"indexing_threshold": 1}})
    wait_indexed(qdrant, name, 64)
    got2 = {p.id: p for p in qdrant.retrieve(name, [p.id for p in pts], with_vector=True)}
    same_opt = [got2[p.id].vector.tobytes() == np.asarray(p.vector, np.float32).tobytes() for p in pts]
    record_evidence("r24-vii-bit-identical", {"points": 64, "retrieve": sum(same), "scroll": sum(same_scroll),
                                              "after_indexing": sum(same_opt)})
    assert all(same) and all(same_scroll) and all(same_opt)


def test_l14_exact_ignore_quantization(qdrant) -> None:
    """L-14 (Qdrant part): exact + quantization.ignore + score_threshold behave as documented."""
    name = make(qdrant, TARGET, optimize=True)
    base = unit(200)
    q = base[0].astype(np.float64)
    # a vector at cosine exactly 0.9499 and one at 0.9501 to the query, in float32
    ortho = base[1].astype(np.float64) - (base[1].astype(np.float64) @ q) * q
    ortho /= np.linalg.norm(ortho)
    near = []
    for c in (0.9499, 0.9501, 0.97):
        v = (c * q + np.sqrt(1 - c * c) * ortho).astype(np.float32)
        near.append(v / np.linalg.norm(v.astype(np.float64)).astype(np.float32))
    allv = np.vstack([base[2:], np.vstack(near)]).astype(np.float32)
    pts = [Point(point_id(f"T{i}", 1, 0), allv[i], {"trep": f"T{i}", "seq": 1, "k": 0}) for i in range(len(allv))]
    qdrant.upsert(name, pts)
    wait_indexed(qdrant, name, len(pts))
    req = SearchRequest(vector=base[0], limit=256, exact=True, ignore_quantization=True, score_threshold=0.95)
    hits = qdrant.search_batch(name, [req], hnsw_ef=None, oversampling=2.0)[0]
    exp = allv.astype(np.float32) @ base[0].astype(np.float32)
    by_id = {p.id: i for i, p in enumerate(pts)}
    diffs = [abs(h.score - float(exp[by_id[h.id]])) for h in hits]
    found = sorted(round(float(exp[by_id[h.id]]), 4) for h in hits)
    record_evidence("l14-exact-ignore-quantization", {"hits": len(hits), "max_abs_score_diff": max(diffs),
                                                      "found_cosines": found})
    assert max(diffs) <= 1e-5
    assert all(float(exp[by_id[h.id]]) >= 0.95 for h in hits)
    assert len(hits) == int((exp >= 0.95).sum()) == 2  # 0.9501 and 0.97; 0.9499 excluded


def test_adapter_requests_identity_encoding_unit() -> None:
    """§4 latency defect (validation §13.17): the adapter must not ask Qdrant for compressed bodies."""
    from fingerprinter.vindex import QdrantIndex

    assert QdrantIndex("http://127.0.0.1:1", None)._session.headers["accept-encoding"] == "identity"


def test_small_search_responses_uncompressed_and_fast(qdrant) -> None:
    """Regression: with requests' default ``gzip`` Qdrant 1.15.1 compressed search responses and a
    single limit-10 query took ≈ 42 ms (p50) instead of ≈ 1 ms; through the adapter it must not."""
    import time

    name = make(qdrant, MEDIA, optimize=True)
    vec = unit(2000)
    pts = [Point(point_id(f"S{i}", 1, 0), vec[i], {"rep": f"S{i}", "seq": 1, "k": 0, "spec": "t"})
           for i in range(len(vec))]
    for i in range(0, len(pts), 250):
        qdrant.upsert(name, pts[i:i + 250])
    wait_indexed(qdrant, name, len(vec))
    raw = qdrant._session.post(f"{qdrant.url}/collections/{name}/points/search",
                               json={"vector": vec[0].tolist(), "limit": 10, "with_payload": True})
    assert raw.status_code == 200 and "content-encoding" not in {k.lower() for k in raw.headers}
    lat = []
    for q in vec[:60]:
        t0 = time.perf_counter()
        qdrant.search_batch(name, [SearchRequest(vector=q, limit=10)], hnsw_ef=10, oversampling=2.0)
        lat.append(1000 * (time.perf_counter() - t0))
    assert float(np.median(lat[10:])) < 20.0, lat
