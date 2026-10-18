"""P10 validation §4: ANN recall against the exact oracle, through the real ``QdrantIndex``.

Runs against the ACTIVE media collection of an M3 keyspace (built by
``m3_live.py --soak``: L-PAIRS clips + SOAK-1000 + the 7 reference contents,
≈ 105 k points). Exact = the same collection searched with ``exact=true`` and
``quantization.ignore=true`` (brute force over the stored float32 vectors;
R-24 (vii) showed they are bit-identical to the artifacts), checked against
numpy on a sample of queries. ANN = the production parameters (int8 HNSW,
rescoring, oversampling 2, ``hnsw_ef = max(k, index.search_ef)``).

1. segment recall@k (k = 10, 100): |ANN top-k ∩ exact top-k| / k over the 760
   film segments (direction-B queries) and 500 seeded L-PAIRS clip segments;
2. per-query latency (single-query requests, sequential);
3. media-level candidate recall of the direction-B policy (adaptive k 32→512
   while the k-th score ≥ floor 0.70, ≥ 2 distinct query segments, own content
   excluded) with ANN vs exact search, per film.

    FINGERPRINTER_SCYLLA__KEYSPACE=fingerprinter_soak_m3 FINGERPRINTER_INDEX__COLLECTION_PREFIX=m3s \
        env/bin/python benchmarks/p10-index/ann_recall.py --vectors ~/anti_piracy/datasets/p10/vectors \
        --out benchmarks/p10-index/results/ann-recall-live.json
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import numpy as np

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1]))


def main() -> None:  # noqa: PLR0915
    ap = argparse.ArgumentParser()
    ap.add_argument("--vectors", type=Path, required=True)
    ap.add_argument("--clip-queries", type=int, default=500)
    ap.add_argument("--out", type=Path, required=True)
    a = ap.parse_args()
    from fingerprinter.config import Settings
    from fingerprinter.p10system import P10System
    from fingerprinter.services import Services
    from fingerprinter.spec import dinov2_seg_v2
    from fingerprinter.vindex import SearchRequest

    settings = Settings.from_env()
    svc = Services.connect(settings, role="ann")
    s = P10System.connect(svc)
    digest = str(dinov2_seg_v2().digest)
    ptr = s.gens.active(digest, "media", fresh=True)
    if ptr is None:
        raise SystemExit("no ACTIVE media generation in this keyspace")
    ix, name = s.p.index, ptr.collection
    total = ix.count(name)
    films = {}
    for f in sorted((a.vectors / "l-pairs").glob("targets__*.npz")):
        z = np.load(f)
        films[f.name] = z["vectors"].astype(np.float32)
    rng = np.random.default_rng(10)
    clip_files = sorted((a.vectors / "l-pairs").glob("clips__*.npz"))
    clip_q = []
    for f in rng.choice(clip_files, size=min(len(clip_files), a.clip_queries), replace=True):
        z = np.load(f)
        clip_q.append(z["vectors"][int(rng.integers(len(z["vectors"])))].astype(np.float32))
    queries = {"film_segments": np.vstack(list(films.values())), "clip_segments": np.vstack(clip_q)}
    ef = settings.index.search_ef
    over = settings.index.oversampling

    def search(q, k, exact):
        t0 = time.perf_counter()
        hits = ix.search_batch(name, [SearchRequest(vector=q, limit=k, exact=exact, ignore_quantization=exact)],
                               hnsw_ef=None if exact else max(k, ef or 0), oversampling=over, timeout_s=120)[0]
        return hits, time.perf_counter() - t0

    report = {"benchmark": "P10-ANN-RECALL-LIVE", "keyspace": settings.scylla.keyspace, "collection": name,
              "points": total, "qdrant_version": ix.version(), "params": {"hnsw_ef": "max(k, search_ef)",
                                                                            "search_ef": ef, "oversampling": over,
                                                                            "quantization": "int8", "rescore": True},
              "segment_recall": {}, "latency_ms": {}}
    numpy_check = {"queries": 0, "max_abs_score_diff": 0.0}
    for qname, Q in queries.items():
        for k in (10, 100):
            rec, lat_ann, lat_ex = [], [], []
            for i, q in enumerate(Q):
                ann, ta = search(q, k, False)
                ex, te = search(q, k, True)
                lat_ann.append(ta)
                lat_ex.append(te)
                kth = ex[-1].score if len(ex) >= k else -2.0
                ann_ids = {h.id for h in ann}
                ex_ids = {h.id for h in ex}
                ties = {h.id for h in ann if abs(h.score - kth) <= 1e-6}
                rec.append(len(ann_ids & (ex_ids | ties)) / max(len(ex_ids), 1))
                if k == 100 and qname == "film_segments" and i < 5:
                    pts = ix.retrieve(name, [h.id for h in ex], with_vector=True)
                    sc = np.array([float(np.asarray(p.vector, np.float32) @ q) for p in pts])
                    numpy_check["queries"] += 1
                    numpy_check["max_abs_score_diff"] = max(numpy_check["max_abs_score_diff"],
                                                            float(np.max(np.abs(sc - np.array([h.score for h in ex])))))
            report["segment_recall"][f"{qname}@{k}"] = {"mean": round(float(np.mean(rec)), 4),
                                                        "min": round(float(np.min(rec)), 4), "queries": len(rec)}
            report["latency_ms"][f"{qname}@{k}"] = {
                "ann_p50": round(1000 * float(np.median(lat_ann)), 2),
                "ann_p95": round(1000 * float(np.percentile(lat_ann, 95)), 2),
                "ann_p99": round(1000 * float(np.percentile(lat_ann, 99)), 2),
                "exact_p50": round(1000 * float(np.median(lat_ex)), 2)}
            print(qname, k, report["segment_recall"][f"{qname}@{k}"], report["latency_ms"][f"{qname}@{k}"],
                  flush=True)
    report["exact_oracle_vs_numpy"] = numpy_check

    # media-level candidate recall of the direction-B policy, ANN vs exact
    m = settings.matching
    ref_cids = {}
    for t in s.tasks.all():
        if t["kind"] == "target_vs_media":
            subj = json.loads(t["subject"])
            ref_cids[subj["content_id"]] = subj
    per_film = {}
    for fname, F in films.items():
        sets = {}
        for exact in (False, True):
            hits_by: dict[str, set[int]] = {}
            for i, q in enumerate(F):
                k = m.k0
                while True:
                    hits, _ = search(q, k, exact)
                    if len(hits) >= k and hits[-1].score >= m.floor_b and k < m.k_max_b:
                        k = min(2 * k, m.k_max_b)
                        continue
                    break
                for h in hits:
                    if h.score < m.floor_b:
                        break
                    cid = h.payload.get("cid")
                    if cid in ref_cids:
                        continue  # a reference content (own content of some target)
                    hits_by.setdefault(cid, set()).add(i)
            sets[exact] = {c for c, segs in hits_by.items() if len(segs) >= m.min_hits}
        inter = len(sets[False] & sets[True])
        per_film[fname] = {"exact_candidates": len(sets[True]), "ann_candidates": len(sets[False]),
                           "ann_recall_vs_exact": round(inter / max(len(sets[True]), 1), 4)}
        print(fname, per_film[fname], flush=True)
    vals = [v["ann_recall_vs_exact"] for v in per_film.values()]
    report["direction_b_media_candidate_recall_vs_exact"] = {"per_film": per_film,
                                                            "mean": round(float(np.mean(vals)), 4),
                                                            "min": round(float(np.min(vals)), 4)}
    report["acceptance"] = {
        "segment_recall_at_100_ge_0_97": all(v["mean"] >= 0.97 for k, v in report["segment_recall"].items()
                                             if k.endswith("@100")),
        "media_candidate_recall_ge_exact_minus_0_005": float(np.min(vals)) >= 1.0 - 0.005,
        "query_p95_le_10ms": all(v["ann_p95"] <= 10.0 for v in report["latency_ms"].values()),
    }
    a.out.parent.mkdir(parents=True, exist_ok=True)
    a.out.write_text(json.dumps(report, indent=1, sort_keys=True))
    print(json.dumps(report["acceptance"]))
    svc.close()


if __name__ == "__main__":
    main()
