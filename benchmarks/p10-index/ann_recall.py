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
2. per-query latency (single-query requests, sequential) at k = 10 and 100 (the
   §4 criterion), plus a profile of the larger adaptive-k limits (32…2048);
3. media-level candidate recall (§4 definition) in **both directions** with the
   configured policies (adaptive k from k0 while the k-th score ≥ floor, up to
   ``k_max``; ≥ ``min_hits`` distinct query segments; own/reference content
   excluded): the fraction of provenance-relevant subjects (L-PAIRS + N-SOAK
   labels) returned with ANN vs with exact search on the same population —
   direction B per film, direction A over every labelled media item; plus, as
   a diagnostic only, the strict overlap of the full ANN and exact candidate
   sets (relevant or not) and the saturated segment counts. (Before 2026-10-18
   evening the harness reported only that strict overlap, for direction B,
   under the saturating ``k_max_b`` 512 — validation §13.18.)

    FINGERPRINTER_SCYLLA__KEYSPACE=fingerprinter_soak_m3 FINGERPRINTER_INDEX__COLLECTION_PREFIX=m3s \
        env/bin/python benchmarks/p10-index/ann_recall.py --vectors ~/anti_piracy/datasets/p10/vectors \
        --data ~/anti_piracy/datasets/p9 --out benchmarks/p10-index/results/ann-recall-live.json
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
    ap.add_argument("--data", type=Path, default=Path.home() / "anti_piracy/datasets/p9")
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
    lp_targets = {Path(i["file"]).name: i["film"] for i in json.loads(
        (a.data / "l-pairs" / "manifest.json").read_text())["items"] if i["kind"] == "target"}
    films_by_id = {lp_targets[k.removeprefix("targets__").removesuffix(".npz")]: v for k, v in films.items()}
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
    # latency of the larger adaptive-k rounds (direction B reaches k_max_b on dense neighbourhoods);
    # reported, not part of the §4 per-segment criterion (k = 10 and 100 above)
    prof = {}
    F = queries["film_segments"]
    for k in (32, 64, 128, 256, 512, 1024, 2048):
        lat = [search(F[i % len(F)], k, False)[1] for i in range(200)]
        prof[str(k)] = {"p50": round(1000 * float(np.median(lat)), 2), "p95": round(1000 * float(np.percentile(lat, 95)), 2),
                        "p99": round(1000 * float(np.percentile(lat, 99)), 2)}
        print("latency k", k, prof[str(k)], flush=True)
    report["latency_by_limit_ms"] = prof

    # media-level candidate recall (§4), both directions, with the configured policies (adaptive k, floor,
    # >= min_hits distinct query segments, own content excluded): the fraction of provenance-relevant
    # subjects returned with ANN vs with exact search; plus the strict overlap of the full candidate sets
    # (|ANN ∩ exact| / |exact|, every candidate, relevant or not — a diagnostic, not the §4 metric).
    m = settings.matching
    sys.path.insert(0, str(HERE))
    from corpus import load_lpairs, load_nsoak
    from tests.v2.helpers import request_env

    lp = load_lpairs(a.data)
    ns = load_nsoak(a.data, a.vectors)
    lp_items = {Path(i["file"]).name: i for i in json.loads((a.data / "l-pairs" / "manifest.json").read_text())["items"]}
    soak_man = {i["file"]: i for i in json.loads((a.data / "soak-1000" / "manifest.json").read_text())["items"]}

    def cid(name: str, sha: str) -> str:
        return str(request_env("sha256:" + sha, f"https://fixture.local/{name}").payload.content.content_id)

    item_cid = {n: cid(n, lp_items[n]["sha256"]) for n in lp.items}
    item_cid |= {n: cid(n, soak_man[n]["sha256"]) for n in ns.items}
    present = {h for h in item_cid.values()}
    ref_cids = {}
    for t in s.tasks.all():
        if t["kind"] == "target_vs_media":
            subj = json.loads(t["subject"])
            ref_cids[subj["content_id"]] = subj
    pairs = [p for p in lp.pairs + ns.pairs if p.item in item_cid]
    relevant_b = {f: {item_cid[p.item] for p in pairs if p.label and p.target == f} for f in films_by_id}
    tptr = s.gens.active(digest, "target", fresh=True)

    def policy(Qv, col, k_max, floor, field, own=None, batch=64):
        """{subject: distinct query segments} for both modes, batched like the Searcher."""
        out = {}
        for exact in (False, True):
            hits_by: dict[str, set[int]] = {}
            active, k, res = list(range(len(Qv))), m.k0, {}
            while active:
                hits = []
                for i in range(0, len(active), batch):
                    chunk = active[i:i + batch]
                    hits += ix.search_batch(col, [SearchRequest(vector=Qv[j], limit=k, exact=exact,
                                                                ignore_quantization=exact) for j in chunk],
                                            hnsw_ef=None if exact else max(k, ef or 0), oversampling=over,
                                            timeout_s=600)
                nxt = []
                for j, h in zip(active, hits, strict=True):
                    res[j] = h
                    if len(h) >= k and h[-1].score >= floor and k < k_max:
                        nxt.append(j)
                active, k = nxt, min(2 * k, k_max)
            sat = sum(1 for h in res.values() if len(h) >= k_max and h[-1].score >= floor)
            for j, h in res.items():
                for hit in h:
                    if hit.score < floor:
                        break
                    key = hit.payload.get(field)
                    if key in ref_cids or (own and (key == own or hit.payload.get("cid") == own)):
                        continue
                    hits_by.setdefault(key, set()).add(j)
            out[exact] = ({c for c, sg in hits_by.items() if len(sg) >= m.min_hits}, sat)
        return out

    def recall(found, rel):
        return len(found & rel) / len(rel) if rel else None

    per_film = {}
    for fid, F in films_by_id.items():
        o = policy(F, name, m.k_max_b, m.floor_b, "cid")
        rel = relevant_b[fid] & present
        ra, re_ = recall(o[False][0], rel), recall(o[True][0], rel)
        per_film[fid] = {"relevant": len(rel), "ann_recall_vs_provenance": round(ra, 4),
                         "exact_recall_vs_provenance": round(re_, 4), "difference": round(ra - re_, 4),
                         "ann_candidates": len(o[False][0]), "exact_candidates": len(o[True][0]),
                         "strict_overlap_all_candidates": round(len(o[False][0] & o[True][0])
                                                                / max(len(o[True][0]), 1), 4),
                         "saturated_ann": o[False][1], "saturated_exact": o[True][1]}
        print("B", fid, per_film[fid], flush=True)
    # direction A: every labelled media item's segments against the ACTIVE target generation
    title_of = {s.p.target_subject(r["target_id"], r["version"], digest).subject_id: r["title"]
                for r in svc.db.stream("SELECT target_id, version, title FROM {ks}.targets")}
    rel_a = {}
    for p in pairs:
        if p.label:
            rel_a.setdefault(p.item, set()).add(p.target)
    a_found = {False: 0, True: 0}
    a_rel = 0
    a_sets_equal = 0
    a_sat = {False: 0, True: 0}
    for n in sorted(rel_a):
        f = (a.vectors / "l-pairs" / f"clips__{n}.npz") if n in lp.items else \
            (a.vectors / "soak-1000" / (n.replace("/", "__") + ".npz"))
        Qm = np.load(f)["vectors"].astype(np.float32)
        o = policy(Qm, tptr.collection, m.k_max_a, m.floor_a, "trep", own=item_cid[n])
        films_found = {mode: {title_of.get(k) for k in o[mode][0]} for mode in (False, True)}
        a_rel += len(rel_a[n])
        for mode in (False, True):
            a_found[mode] += len(films_found[mode] & rel_a[n])
            a_sat[mode] += o[mode][1]
        a_sets_equal += int(o[False][0] == o[True][0])
    vals = [v["difference"] for v in per_film.values()]
    report["media_candidate_recall"] = {
        "definition": "validation §4: provenance-relevant subjects returned by the full candidate policy, "
                      "ANN vs exact search (same population, same policy); criterion ANN >= exact - 0.005",
        "policy": {"k0": m.k0, "k_max_a": m.k_max_a, "floor_a": m.floor_a, "k_max_b": m.k_max_b,
                   "floor_b": m.floor_b, "min_hits": m.min_hits},
        "direction_b": {"per_film": per_film, "min_difference": round(float(np.min(vals)), 4),
                        "pooled_ann": round(sum(v["ann_recall_vs_provenance"] * v["relevant"]
                                                for v in per_film.values())
                                            / sum(v["relevant"] for v in per_film.values()), 4),
                        "pooled_exact": round(sum(v["exact_recall_vs_provenance"] * v["relevant"]
                                                  for v in per_film.values())
                                              / sum(v["relevant"] for v in per_film.values()), 4)},
        "direction_a": {"items": len(rel_a), "relevant_pairs": a_rel,
                        "ann_recall_vs_provenance": round(a_found[False] / a_rel, 4),
                        "exact_recall_vs_provenance": round(a_found[True] / a_rel, 4),
                        "items_with_identical_candidate_sets": a_sets_equal,
                        "saturated_segments_ann": a_sat[False], "saturated_segments_exact": a_sat[True]},
    }
    mc = report["media_candidate_recall"]
    print(json.dumps({k: v for k, v in mc.items() if k != "definition"}, default=str)[:2000], flush=True)
    report["acceptance"] = {
        "segment_recall_at_100_ge_0_97": all(v["mean"] >= 0.97 for k, v in report["segment_recall"].items()
                                             if k.endswith("@100")),
        "media_candidate_recall_b_ge_exact_minus_0_005": mc["direction_b"]["min_difference"] >= -0.005,
        "media_candidate_recall_a_ge_exact_minus_0_005":
            mc["direction_a"]["ann_recall_vs_provenance"] >= mc["direction_a"]["exact_recall_vs_provenance"] - 0.005,
        "query_p95_le_10ms": all(v["ann_p95"] <= 10.0 for v in report["latency_ms"].values()),
        "no_saturation": all(v["saturated_ann"] == 0 for v in per_film.values())
                         and mc["direction_a"]["saturated_segments_ann"] == 0,
    }
    a.out.parent.mkdir(parents=True, exist_ok=True)
    a.out.write_text(json.dumps(report, indent=1, sort_keys=True))
    print(json.dumps(report["acceptance"]))
    svc.close()


if __name__ == "__main__":
    main()
