"""P10 audit: ANN candidates on real dinov2-seg/2 vectors, against an exact (brute-force) oracle.

Corpus M ("historical media"): SOAK-1000 run-5 representations (98,983
vectors, 1,000 items) + the 180 L-PAIRS clips. Queries:

- QB (direction B, new target -> historical media): every segment of the 7
  L-PAIRS target films (the S-SRC originals, not in M);
- QA (segment-level ANN quality): a seeded sample of L-PAIRS clip segments
  searched against the SOAK-1000 part of M.

Ground truth is provenance only: every SOAK-1000 item and L-PAIRS clip
records its reel interval, every film its reel interval, so an item is
*relevant* to a film when the intervals overlap by >= 30 s, *irrelevant* when
they are >= 15 s apart (stream-copy keyframe slop), and excluded otherwise.
Synthetic clips are irrelevant to every film. No model output defines a label.

Measured: exact search (FAISS ``IndexFlatIP``, the offline oracle only),
FAISS HNSW / HNSW-SQ8 / IVF-Flat / IVF-PQ + exact re-rank, and Qdrant
(HNSW, float32 in RAM; HNSW + int8 quantization in RAM with on-disk
originals and rescoring). Segment recall@k vs exact, latency, build time,
memory; media-level recall of the direction-B candidate policy; the
corpus-growth bound; within-item sparsification.

Runs in an isolated venv (faiss-cpu, qdrant-client), never in ``env/``:

    ~/anti_piracy/datasets/p10/venv/bin/python -I benchmarks/p10-index/ann_bench.py \
        --vectors ~/anti_piracy/datasets/p10/vectors --data ~/anti_piracy/datasets/p9 \
        --out benchmarks/p10-index/results/ann-bench.json [--qdrant http://127.0.0.1:16333]
"""
from __future__ import annotations

import argparse
import json
import os
import platform
import subprocess
import time
from collections import defaultdict
from pathlib import Path

import numpy as np

REL_MIN_OVERLAP_S = 30.0
IRREL_MIN_GAP_S = 15.0


def rss_mb() -> float:
    for line in Path("/proc/self/status").read_text().splitlines():
        if line.startswith("VmRSS:"):
            return int(line.split()[1]) / 1024
    return 0.0


def pct(a, p):
    return round(float(np.percentile(np.asarray(a), p)), 3)


def load(vectors: Path, data: Path):
    reel = json.loads((data / "reel/manifest.json").read_text())
    films = {f["id"]: (f["reel_start_s"], f["reel_start_s"] + f["duration_s"]) for f in reel["films"]}
    items, mats = [], []  # items: (name, interval|None, kind)
    soak = json.loads((data / "soak-1000/manifest.json").read_text())
    smeta = {i["file"]: i for i in soak["items"]}
    sidx = json.loads((vectors / "soak-1000/index.json").read_text())["items"]
    for name in sorted(sidx):
        z = np.load(vectors / "soak-1000" / (name.replace("/", "__") + ".npz"))
        it = smeta[name]
        items.append((name, (it["reel_start_s"], it["reel_start_s"] + it["seconds"]), "soak"))
        mats.append(z["vectors"].astype(np.float32))
    lp = json.loads((data / "l-pairs/manifest.json").read_text())
    targets = {}
    for it in lp["items"]:
        z = np.load(vectors / "l-pairs" / (it["file"].replace("/", "__") + ".npz"))
        if it["kind"] == "target":
            targets[it["film"]] = z["vectors"].astype(np.float32)
        else:
            iv = None if it.get("source_film") is None else (it["reel_start_s"], it["reel_start_s"] + it["seconds"])
            items.append((it["file"], iv, "lpairs-clip"))
            mats.append(z["vectors"].astype(np.float32))
    owner = np.concatenate([np.full(len(m), i, np.int64) for i, m in enumerate(mats)])
    return films, targets, items, np.concatenate(mats), owner


def relevance(films, items):
    rel, irrel = {}, {}
    for f, (fs, fe) in films.items():
        r, ir = set(), set()
        for i, (_, iv, _) in enumerate(items):
            if iv is None:
                ir.add(i)
                continue
            ov = min(fe, iv[1]) - max(fs, iv[0])
            if ov >= REL_MIN_OVERLAP_S:
                r.add(i)
            elif ov <= -IRREL_MIN_GAP_S:
                ir.add(i)
        rel[f], irrel[f] = r, ir
    return rel, irrel


def media_policy(I, D, owner, qfilm, rel, irrel, k, floor, h, cap=None, present=None):
    """Direction-B candidates: per query segment top-k with sim >= floor, grouped by item, >= h hits.

    ``present``: the items in the searched index (relevance is restricted to them)."""
    if present is not None:
        rel = {f: r & present for f, r in rel.items()}
        irrel = {f: r & present for f, r in irrel.items()}
    out = {"recall": [], "false_candidates": [], "candidates": [], "truncated_queries": 0}
    for f in sorted(set(qfilm)):
        rows = np.where(qfilm == f)[0]
        hits = defaultdict(set)
        for r in rows:
            for j in range(min(k, I.shape[1])):
                if I[r, j] < 0 or D[r, j] < floor:
                    continue
                hits[int(owner[I[r, j]])].add(int(r))
        cand = sorted((i for i, s in hits.items() if len(s) >= h), key=lambda i: -len(hits[i]))
        if cap is not None and len(cand) > cap:
            out["truncated_queries"] += 1
            cand = cand[:cap]
        cs = set(cand)
        out["recall"].append(len(cs & rel[f]) / max(len(rel[f]), 1))
        out["false_candidates"].append(len(cs & irrel[f]))
        out["candidates"].append(len(cand))
    return {"media_recall_mean": round(float(np.mean(out["recall"])), 4),
            "media_recall_min": round(float(np.min(out["recall"])), 4),
            "candidates_per_target_max": int(max(out["candidates"])),
            "candidates_per_target_mean": round(float(np.mean(out["candidates"])), 1),
            "false_candidates_total": int(sum(out["false_candidates"])),
            "truncated_targets": out["truncated_queries"]}


def adaptive_mask(I, D, k0, kmax, floor):
    """Per query segment: k = k0, doubled while the k-th score is still >= floor, up to kmax.

    Returns (I, D) with entries beyond each row's k masked, and the per-row k used."""
    I, D = I.copy(), D.copy()
    ks = np.empty(len(I), np.int64)
    for r in range(len(I)):
        k = k0
        while k < kmax and k <= D.shape[1] and D[r, k - 1] >= floor:
            k *= 2
        k = min(k, kmax, D.shape[1])
        ks[r] = k
        I[r, k:], D[r, k:] = -1, -1.0
    return I, D, ks


def seg_recall(I_ann, I_exact, k):
    return round(float(np.mean([len(set(a[:k]) & set(b[:k])) / k for a, b in zip(I_ann, I_exact)])), 4)


def timed_search(index, Q, k, threads):
    import faiss
    faiss.omp_set_num_threads(1)
    lat = []
    for i in range(min(len(Q), 400)):
        t = time.perf_counter()
        index.search(Q[i:i + 1], k)
        lat.append((time.perf_counter() - t) * 1000)
    faiss.omp_set_num_threads(threads)
    t = time.perf_counter()
    D, I = index.search(Q, k)
    batch_s = time.perf_counter() - t
    return D, I, {"p50_ms": pct(lat, 50), "p95_ms": pct(lat, 95), "p99_ms": pct(lat, 99),
                  f"batch_qps_{threads}t": round(len(Q) / batch_s, 1)}


def main() -> None:
    import faiss

    ap = argparse.ArgumentParser()
    ap.add_argument("--vectors", type=Path, required=True)
    ap.add_argument("--data", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--qdrant")
    ap.add_argument("--threads", type=int, default=4)
    ap.add_argument("--qa", type=int, default=2000)
    ap.add_argument("--growth-only", action="store_true", help="exact search, policy and growth sections only")
    ap.add_argument("--qdrant-only", help="comma-separated Qdrant config names (skips FAISS variants)")
    a = ap.parse_args()

    films, targets, items, X, owner = load(a.vectors, a.data)
    n, d = X.shape
    rel, irrel = relevance(films, items)
    QB = np.concatenate([targets[f] for f in sorted(targets)])
    qfilm = np.concatenate([np.full(len(targets[f]), f, object) for f in sorted(targets)])
    soak_rows = np.where(np.array([items[o][2] == "soak" for o in owner]))[0]
    clip_rows = np.where(np.array([items[o][2] == "lpairs-clip" for o in owner]))[0]
    rng = np.random.default_rng(10)
    QA = X[rng.choice(clip_rows, size=min(a.qa, len(clip_rows)), replace=False)]
    Xs = X[soak_rows]
    rep: dict = {
        "benchmark": "P10-AUDIT-ANN", "host": {"cpu": platform.processor() or platform.machine(),
                                               "cores": os.cpu_count(), "faiss": faiss.__version__},
        "spec": "dinov2-seg/2 FP32 (sha256:a4df3d19...)", "dim": d, "dtype": "float32",
        "norms": {"min": round(float(np.linalg.norm(X, axis=1).min()), 5),
                  "max": round(float(np.linalg.norm(X, axis=1).max()), 5)},
        "corpus": {"items": len(items), "vectors": n, "soak_vectors": len(soak_rows), "clip_vectors": len(clip_rows),
                   "QB_target_segments": len(QB), "QA_clip_segment_sample": len(QA),
                   "raw_float32_bytes": int(X.nbytes)},
        "relevance": {"rule": f"overlap >= {REL_MIN_OVERLAP_S}s relevant; gap >= {IRREL_MIN_GAP_S}s irrelevant; else excluded",
                      "relevant_items_per_film": {f: len(r) for f, r in rel.items()}},
        "threads_batch": a.threads,
    }

    # exact oracle
    t0 = time.perf_counter()
    flat = faiss.IndexFlatIP(d)
    flat.add(X)
    flat_s = faiss.IndexFlatIP(d)
    flat_s.add(Xs)
    KMAX = 1000
    DB, IB = flat.search(QB, KMAX)
    DA, IA = flat_s.search(QA, 100)
    rep["exact"] = {"build_s": round(time.perf_counter() - t0, 2), "index_bytes": int(X.nbytes)}
    _, _, rep["exact"]["latency_full_corpus_k100"] = timed_search(flat, QB, 100, a.threads)

    # similarity of the k-th neighbour (how saturated top-k is with near-duplicates)
    rep["exact"]["QB_similarity_at_rank"] = {str(r): {"p50": pct(DB[:, r - 1], 50), "p10": pct(DB[:, r - 1], 10)}
                                             for r in (1, 10, 100, 1000)}

    # direction-B candidate policy on exact results (isolates policy from ANN error)
    pol = []
    for k in (10, 32, 100, 256, 1000):
        for floor in (0.5, 0.6, 0.7, 0.8):
            for h in (1, 2, 3):
                pol.append({"k": k, "floor": floor, "min_hits": h,
                            **media_policy(IB, DB, owner, qfilm, rel, irrel, k, floor, h)})
    rep["direction_b_policy_exact"] = pol

    # corpus growth: L-PAIRS clips only vs the full corpus (fixed policy, fixed cap)
    flat_c = faiss.IndexFlatIP(d)
    flat_c.add(X[clip_rows])
    Dc, Ic = flat_c.search(QB, 100)
    owner_c = owner[clip_rows]
    small_items = set(int(i) for i in np.unique(owner_c))
    growth = {"small_vectors": len(clip_rows), "full_vectors": n, "ratio": round(n / len(clip_rows), 1),
              "small_relevant_items_per_film": {f: len(r & small_items) for f, r in rel.items()}, "rows": []}
    for k, floor, h, cap in ((32, 0.6, 2, 200), (100, 0.6, 2, 200), (100, 0.7, 2, 200), (100, 0.6, 2, 500)):
        growth["rows"].append({"k": k, "floor": floor, "min_hits": h, "cap": cap,
                               "small": media_policy(Ic, Dc, owner_c, qfilm, rel, irrel, k, floor, h, cap, present=small_items),
                               "full": media_policy(IB, DB, owner, qfilm, rel, irrel, k, floor, h, cap),
                               "per_segment_bound": k})
    DcK, IcK = flat_c.search(QB, KMAX)
    for k0, kmax, floor, h in ((32, 1000, 0.6, 2), (32, 1000, 0.7, 2), (32, 512, 0.7, 2)):
        Ia_s, Da_s, ks_s = adaptive_mask(IcK, DcK, k0, kmax, floor)
        Ia_f, Da_f, ks_f = adaptive_mask(IB, DB, k0, kmax, floor)
        growth["rows"].append({
            "adaptive_k0": k0, "k_max": kmax, "floor": floor, "min_hits": h, "cap": None,
            "small": {**media_policy(Ia_s, Da_s, owner_c, qfilm, rel, irrel, kmax, floor, h, None, present=small_items),
                      "k_used_mean": round(float(ks_s.mean()), 1), "k_used_max": int(ks_s.max()),
                      "segments_at_k_max": int((ks_s >= kmax).sum())},
            "full": {**media_policy(Ia_f, Da_f, owner, qfilm, rel, irrel, kmax, floor, h),
                     "k_used_mean": round(float(ks_f.mean()), 1), "k_used_max": int(ks_f.max()),
                     "segments_at_k_max": int((ks_f >= kmax).sum())}})
    rep["growth"] = growth

    # within-item sparsification (drop a segment too similar to the last kept one of the same item)
    sp = []
    for tau in (0.90, 0.95, 0.98):
        keep = np.ones(n, bool)
        last = None
        for r in range(n):
            if last is not None and owner[last] == owner[r] and float(X[r] @ X[last]) >= tau:
                keep[r] = False
            else:
                last = r
        kr = np.where(keep)[0]
        fs = faiss.IndexFlatIP(d)
        fs.add(X[kr])
        Ds, Is = fs.search(QB, 100)
        Is = np.where(Is >= 0, kr[np.clip(Is, 0, None)], -1)
        sp.append({"tau": tau, "kept_fraction": round(len(kr) / n, 4),
                   "policy_k100_f0.6_h2": media_policy(Is, Ds, owner, qfilm, rel, irrel, 100, 0.6, 2),
                   "policy_k32_f0.6_h2": media_policy(Is, Ds, owner, qfilm, rel, irrel, 32, 0.6, 2)})
    rep["sparsification"] = sp
    if a.growth_only or a.qdrant_only:
        if a.qdrant_only:
            rep["qdrant"] = qdrant_bench(a.qdrant, X, owner, QB, IB, qfilm, rel, irrel, a.qdrant_only.split(","))
        a.out.parent.mkdir(parents=True, exist_ok=True)
        a.out.write_text(json.dumps(rep, indent=1, sort_keys=True, default=str))
        return

    # FAISS ANN variants
    variants = []

    def add_variant(name, index, train=None, set_params=(), rerank=False):
        r0 = rss_mb()
        t = time.perf_counter()
        if train is not None:
            index.train(train)
        index.add(X)
        build = time.perf_counter() - t
        size = len(faiss.serialize_index(index))
        for label, setter in set_params or [("default", lambda: None)]:
            setter()
            kq = 400 if rerank else 100
            DBa, IBa, latB = timed_search(index, QB, kq, a.threads)
            if rerank:  # exact re-rank of the candidates with the authoritative float32 vectors
                for qi in range(len(QB)):
                    ids = IBa[qi][IBa[qi] >= 0]
                    s = X[ids] @ QB[qi]
                    o = np.argsort(-s)
                    pad = kq - len(ids)
                    IBa[qi] = np.concatenate([ids[o], -np.ones(pad, np.int64)])
                    DBa[qi] = np.concatenate([s[o], -np.ones(pad, np.float32)])
                IBa, DBa = IBa[:, :100], DBa[:, :100]
            variants.append({
                "variant": name, "params": label, "build_s": round(build, 2), "index_bytes": size,
                "bytes_per_vector": round(size / n, 1), "rss_delta_mb": round(rss_mb() - r0, 1),
                "segment_recall_at_10_QB": seg_recall(IBa, IB, 10), "segment_recall_at_100_QB": seg_recall(IBa, IB, 100),
                "latency_QB": latB, "rerank_exact": rerank,
                "direction_b_k100_f0.6_h2": media_policy(IBa, DBa, owner, qfilm, rel, irrel, 100, 0.6, 2),
                "direction_b_k32_f0.6_h2": media_policy(IBa, DBa, owner, qfilm, rel, irrel, 32, 0.6, 2),
            })
        return index

    faiss.omp_set_num_threads(a.threads)
    for m in (16, 32):
        idx = faiss.IndexHNSWFlat(d, m, faiss.METRIC_INNER_PRODUCT)
        idx.hnsw.efConstruction = 200
        add_variant(f"faiss-hnsw-flat-M{m}", idx, set_params=[
            (f"efSearch={ef}", (lambda ef=ef: setattr(idx.hnsw, "efSearch", ef))) for ef in (64, 128, 256)])
        del idx
    idx = faiss.IndexHNSWSQ(d, faiss.ScalarQuantizer.QT_8bit, 32, faiss.METRIC_INNER_PRODUCT)
    idx.hnsw.efConstruction = 200
    add_variant("faiss-hnsw-sq8-M32", idx, train=X, set_params=[
        (f"efSearch={ef}", (lambda ef=ef: setattr(idx.hnsw, "efSearch", ef))) for ef in (128, 256)])
    del idx
    nlist = 1024
    q1 = faiss.IndexFlatIP(d)
    idx = faiss.IndexIVFFlat(q1, d, nlist, faiss.METRIC_INNER_PRODUCT)
    add_variant("faiss-ivf1024-flat", idx, train=X, set_params=[
        (f"nprobe={p}", (lambda p=p: setattr(idx, "nprobe", p))) for p in (16, 64)])
    del idx
    q2 = faiss.IndexFlatIP(d)
    idx = faiss.IndexIVFPQ(q2, d, nlist, 96, 8, faiss.METRIC_INNER_PRODUCT)
    add_variant("faiss-ivf1024-pq96", idx, train=X, rerank=True, set_params=[
        (f"nprobe={p}+rerank400", (lambda p=p: setattr(idx, "nprobe", p))) for p in (32, 64)])
    del idx
    rep["faiss_variants"] = variants

    # segment-level quality on clip queries (QA vs the SOAK part), HNSW M32 only
    idx = faiss.IndexHNSWFlat(d, 32, faiss.METRIC_INNER_PRODUCT)
    idx.hnsw.efConstruction = 200
    idx.add(Xs)
    qa = []
    for ef in (64, 128, 256):
        idx.hnsw.efSearch = ef
        _, Ia = idx.search(QA, 100)
        qa.append({"efSearch": ef, "recall_at_10": seg_recall(Ia, IA, 10), "recall_at_100": seg_recall(Ia, IA, 100)})
    rep["faiss_hnsw_M32_QA_recall"] = qa
    del idx

    if a.qdrant:
        rep["qdrant"] = qdrant_bench(a.qdrant, X, owner, QB, IB, qfilm, rel, irrel)
    rep["peak_rss_mb"] = round(rss_mb(), 1)
    a.out.parent.mkdir(parents=True, exist_ok=True)
    a.out.write_text(json.dumps(rep, indent=1, sort_keys=True, default=str))
    print(json.dumps({"corpus": rep["corpus"], "exact": rep["exact"]}, indent=1))


def docker_mem(name="p10-qdrant"):
    out = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{.MemUsage}}", name],
                         capture_output=True, text=True).stdout.strip()
    return out


def qdrant_bench(url, X, owner, QB, IB, qfilm, rel, irrel, only=None):
    from qdrant_client import QdrantClient, models

    c = QdrantClient(url=url, timeout=600, prefer_grpc=False)
    res = {"server_version": c.info().version if hasattr(c, "info") else None, "runs": []}
    configs = {
        "qdrant-hnsw-m32-f32-ram": dict(on_disk=False, quant=None),
        "qdrant-hnsw-m32-int8-ram-originals-on-disk": dict(on_disk=True, quant=models.ScalarQuantization(
            scalar=models.ScalarQuantizationConfig(type=models.ScalarType.INT8, always_ram=True))),
        "qdrant-hnsw-m32-pq-x16-ram-originals-on-disk": dict(on_disk=True, quant=models.ProductQuantization(
            product=models.ProductQuantizationConfig(compression=models.CompressionRatio.X16, always_ram=True))),
    }
    for name, cfg in configs.items():
        if only and name not in only:
            continue
        if c.collection_exists("p10bench"):
            c.delete_collection("p10bench")
        mem0 = docker_mem()
        c.create_collection("p10bench", vectors_config=models.VectorParams(
            size=X.shape[1], distance=models.Distance.DOT, on_disk=cfg["on_disk"]),
            hnsw_config=models.HnswConfigDiff(m=32, ef_construct=200),
            optimizers_config=models.OptimizersConfigDiff(indexing_threshold=1000),
            quantization_config=cfg["quant"])
        t = time.perf_counter()
        B = 2000
        for s in range(0, len(X), B):
            c.upsert("p10bench", wait=True, points=models.Batch(
                ids=list(range(s, min(s + B, len(X)))), vectors=X[s:s + B].tolist(),
                payloads=[{"item": int(owner[i]), "spec": "a4df3d19"} for i in range(s, min(s + B, len(X)))]))
        upload = time.perf_counter() - t
        while c.get_collection("p10bench").status != models.CollectionStatus.GREEN:
            time.sleep(1)
        ready = time.perf_counter() - t
        mem1 = docker_mem()
        for ef in (64, 128, 256):
            params = models.SearchParams(hnsw_ef=ef, quantization=models.QuantizationSearchParams(
                rescore=True, oversampling=2.0) if cfg["quant"] else None)
            lat = []
            I = np.full((len(QB), 100), -1, np.int64)
            D = np.full((len(QB), 100), -1.0, np.float32)
            for qi in range(len(QB)):
                t1 = time.perf_counter()
                r = c.query_points("p10bench", query=QB[qi].tolist(), limit=100, search_params=params,
                                   with_payload=False).points
                lat.append((time.perf_counter() - t1) * 1000)
                for j, p in enumerate(r):
                    I[qi, j], D[qi, j] = p.id, p.score
            res["runs"].append({"variant": name, "hnsw_ef": ef, "upload_s": round(upload, 1),
                                "upload_plus_index_s": round(ready, 1), "container_mem_before": mem0,
                                "container_mem_after_index": mem1,
                                "latency_ms_http_single": {"p50": pct(lat, 50), "p95": pct(lat, 95), "p99": pct(lat, 99)},
                                "segment_recall_at_10_QB": seg_recall(I, IB, 10),
                                "segment_recall_at_100_QB": seg_recall(I, IB, 100),
                                "direction_b_k100_f0.6_h2": media_policy(I, D, owner, qfilm, rel, irrel, 100, 0.6, 2)})
        res["runs"][-1]["container_mem_after_queries"] = docker_mem()
    c.delete_collection("p10bench")
    return res


if __name__ == "__main__":
    main()
