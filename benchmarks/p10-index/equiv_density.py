"""Level-5 equivalence: can denser temporal sampling recover the ``equiv/v2`` misses? (validation §13.27)

A diagnostic, not a tuning run and not a gate: nothing is selected, registered or promoted.

``equivalence()`` links a pair only if **one** run covers ≥ 90 % of *all* segments of both items, and a
segment counts as covered only if it is matched (score ≥ the run's ``s_seg``); bridged gaps do not
count. So for any run parameters with ``s_seg`` = s, coverage of A is at most the fraction of A's
samples that have a partner in B with cosine ≥ s at one common offset (``bound`` below). If that bound
is below 0.90 for a pair, no run parametrisation with that ``s_seg`` can link it at that sampling rate.

For each set (E-TUNE first: the tuning set; then E-SET: held out, diagnosis only):
1. the ``equiv/v2`` misses are recomputed from the existing 5 s vectors (the registered rule);
2. every item of a missed excerpt is re-encoded with a **test spec** that differs from ``dinov2-seg/2``
   only in ``sampling.segment_duration_s`` (its own digest — never mixed with the 5 s vectors, never
   registered in any keyspace), with P9's production decode, preprocessing and model (FP32);
3. per equivalent pair of those excerpts: the bound at 5 s and at the dense rate (best common offset
   within ±10 s), the ``equiv/v2`` decision at 5 s, and storage/encode cost of the dense vectors.

``--grid`` (after ``--encode-all`` of both sets) runs the equivalence protocol of ``equiv_tune.py`` on
the dense vectors: the run structure keeps its time meaning at the dense rate (``min_run`` and
``min_mode_votes`` scaled by 5 s / dense step), the coverage/duration/offset rule is unchanged, the grid
is evaluated on E-TUNE only, selection = maximum E-TUNE recall at zero E-TUNE false links (ties: higher
``s_seg``, smaller ``max_gap``, smaller ``phase_tolerance_s``), then evaluated **once** on E-SET. The
result is evidence for an owner decision (a new P9 spec + backfill); nothing is registered.

    env/bin/python benchmarks/p10-index/equiv_density.py --grid --set ~/anti_piracy/datasets/p10/e-tune \
        --dense-out ~/anti_piracy/datasets/p10/vectors/e-tune-dense1 --heldout ~/anti_piracy/datasets/p10/e-set \
        --heldout-dense ~/anti_piracy/datasets/p10/vectors/e-set-dense1 --dense-s 1.0 \
        --vectors ~/anti_piracy/datasets/p10/vectors/e-tune --out benchmarks/p10-index/results/equiv-density-grid.json

    env/bin/python benchmarks/p10-index/equiv_density.py --set ~/anti_piracy/datasets/p10/e-tune \\
        --vectors ~/anti_piracy/datasets/p10/vectors/e-tune --dense-s 1.0 \\
        --dense-out ~/anti_piracy/datasets/p10/vectors/e-tune-dense1 --out benchmarks/p10-index/results/equiv-density-e-tune.json
"""

from __future__ import annotations

import argparse
import itertools
import json
import sys
import time
from pathlib import Path

import numpy as np

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1]))
sys.path.insert(0, str(HERE))

from equiv_tune import evaluate, load  # noqa: E402

CHUNK, BATCH = 64, 16


def test_spec(seconds: float):
    from fingerprinter.spec import Spec, SpecDocument, dinov2_seg_v2

    doc = dinov2_seg_v2().document.to_json_obj()
    doc["sampling"]["segment_duration_s"] = float(seconds)
    return Spec(SpecDocument.from_json_obj(doc))


def encode(path: Path, spec, emb) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    from fingerprinter.media import decode, preprocess

    ks, ts, vecs, chunk = [], [], [], []
    for f in decode(path, spec):
        ks.append(f.segment_index)
        ts.append(f.pts_s)
        chunk.append(preprocess(f.rgb))
        if len(chunk) >= CHUNK:
            x = np.stack(chunk)
            vecs.append(np.concatenate([emb.embed(x[i:i + BATCH]) for i in range(0, len(x), BATCH)]))
            chunk.clear()
    if chunk:
        x = np.stack(chunk)
        vecs.append(np.concatenate([emb.embed(x[i:i + BATCH]) for i in range(0, len(x), BATCH)]))
    v = np.concatenate(vecs).astype(np.float32) if vecs else np.zeros((0, 768), np.float32)
    return np.asarray(ks, np.int32), np.asarray(ts, np.float64), v


def bound(a_t, a_v, b_t, b_v, s: float, step: float, max_offset: float = 10.0) -> float:
    """Upper bound of one run's coverage of A: best common offset; a sample of A counts if B has a
    sample within ±step/2 of (t + offset) with cosine ≥ s."""
    if not len(a_t) or not len(b_t):
        return 0.0
    sims = a_v.astype(np.float64) @ b_v.astype(np.float64).T
    best = 0.0
    for off in np.arange(-max_offset, max_offset + 1e-9, step / 2):
        d = np.abs(b_t[None, :] - (a_t[:, None] + off))
        ok = np.where(d <= step / 2 + 1e-6, sims, -1.0).max(axis=1) >= s
        best = max(best, float(ok.mean()))
    return best


def encode_all(manifest_dir: Path, out: Path, seconds: float, device: str) -> dict:
    from fingerprinter.model import Embedder

    man = json.loads((manifest_dir / "manifest.json").read_text())
    spec, emb = test_spec(seconds), Embedder(device, "float32")
    out.mkdir(parents=True, exist_ok=True)
    t0, n, done = time.time(), 0, 0
    for it in man["items"]:
        dst = out / (it["file"].replace("/", "__") + ".npz")
        if dst.exists():
            continue
        ks, ts, v = encode(manifest_dir / it["file"], spec, emb)
        np.savez(dst, segment_index=ks, pts_s=ts, vectors=v)
        n += int(len(v))
        done += 1
    (out / "index.json").write_text(json.dumps({"set": str(manifest_dir), "spec_digest": str(spec.digest),
                                                "segment_duration_s": seconds, "device": device}, indent=1))
    return {"encoded": done, "vectors": n, "seconds": round(time.time() - t0, 1)}


DENSE_GRID = {"s_seg": [0.80, 0.75, 0.70, 0.65, 0.60, 0.55], "max_gap_s": [1.0, 2.0, 5.0],
              "phase_tolerance_s": [0.5, 1.0, 2.5]}


def grid(a) -> dict:
    from fingerprinter.verify import EquivParams

    t0 = time.time()
    tune = json.loads((HERE / "results" / "tune-eval.json").read_text())
    scale = int(round(5.0 / a.dense_s))
    base = {**tune["final_parameter_set"]["params"], "segment_duration_s": a.dense_s,
            "min_run": tune["final_parameter_set"]["params"]["min_run"] * scale,
            "min_mode_votes": tune["final_parameter_set"]["params"]["min_mode_votes"] * scale}
    configs = []
    for s_seg, gap_s, phase in itertools.product(*DENSE_GRID.values()):
        configs.append({"s_seg": s_seg, "s_mean": s_seg, "vote_floor": round(s_seg - 0.10, 6),
                        "max_gap": max(1, int(round(gap_s / a.dense_s))), "phase_tolerance_s": phase})
    from concurrent.futures import ProcessPoolExecutor

    with ProcessPoolExecutor(a.workers) as pool:
        res = list(pool.map(evaluate, [(str(a.set), str(a.dense_out), base, c) for c in configs]))
    t_grid = time.time() - t0
    safe = [r for r in res if r["confusion"]["fp"] == 0]
    if not safe:
        raise SystemExit("no dense setting has zero false links on E-TUNE")
    best = max(safe, key=lambda r: (r["confusion"]["recall"], r["override"]["s_seg"], -r["override"]["max_gap"],
                                    -r["override"]["phase_tolerance_s"]))
    t1 = time.time()
    held = evaluate((str(a.heldout), str(a.heldout_dense), base, best["override"]))
    t_held = time.time() - t1
    cand = EquivParams(rule=f"equiv/dense{a.dense_s:g}-candidate", segment=best["override"])
    return {"benchmark": "P10-EQUIV-DENSITY-GRID", "dense_segment_duration_s": a.dense_s,
            "dense_spec_digest": str(test_spec(a.dense_s).digest), "base_params": base, "grid": DENSE_GRID,
            "configs": len(configs), "selection": "max E-TUNE recall at zero E-TUNE false links; ties -> higher "
                                                  "s_seg, smaller max_gap, smaller phase_tolerance_s",
            "tune": [{k: v for k, v in r.items() if k != "misses"} for r in res],
            "selected": best["override"], "selected_tune": {k: v for k, v in best.items()},
            "candidate_rule_digest": cand.digest(), "heldout": held,
            "seconds": {"grid": round(t_grid), "heldout": round(t_held),
                        "heldout_per_pair_ms": round(1000 * t_held / max(held["confusion"]["tp"] + held["confusion"]["fn"]
                                                                          + held["confusion"]["fp"] + held["confusion"]["tn"], 1), 3)}}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--set", type=Path, required=True)
    ap.add_argument("--vectors", type=Path, required=True)
    ap.add_argument("--dense-s", type=float, default=1.0)
    ap.add_argument("--dense-out", type=Path, required=True)
    ap.add_argument("--device", default="cuda")
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--encode-all", action="store_true", help="encode every item of --set into --dense-out")
    ap.add_argument("--grid", action="store_true", help="tune on --set (dense), evaluate once on --heldout")
    ap.add_argument("--heldout", type=Path)
    ap.add_argument("--heldout-dense", type=Path)
    ap.add_argument("--workers", type=int, default=4)
    a = ap.parse_args()
    if a.encode_all:
        print(json.dumps(encode_all(a.set, a.dense_out, a.dense_s, a.device)))
        return
    if a.grid:
        report = grid(a)
        a.out.parent.mkdir(parents=True, exist_ok=True)
        a.out.write_text(json.dumps(report, indent=1, sort_keys=True, default=str))
        print(json.dumps({"selected": report["selected"], "tune": report["selected_tune"]["confusion"],
                          "heldout": report["heldout"]["confusion"], "heldout_neg": report["heldout"]["negatives_by_class"],
                          "closest": report["heldout"]["closest_negative_by_rejecting_rule"],
                          "seconds": report["seconds"]}, indent=1, default=str))
        return
    from fingerprinter.model import Embedder
    from fingerprinter.verify import EQUIV_RULES, VerifierParams, equivalence

    t0 = time.time()
    tune = json.loads((HERE / "results" / "tune-eval.json").read_text())
    params = VerifierParams.from_document(tune["final_parameter_set"]["params"])
    v2 = EQUIV_RULES["equiv/v2"]
    s = float(v2.segment["s_seg"])
    man, vec5 = load(a.set, a.vectors)
    by = {it["file"]: it for it in man["items"]}
    variants = [f for f, it in by.items() if it["role"] == "variant"]
    pos = [(x, y) for x, y in itertools.combinations(sorted(variants), 2) if by[x]["excerpt"] == by[y]["excerpt"]]
    missed = [(x, y) for x, y in pos if not equivalence(vec5[x], vec5[y], params, v2).linked]
    excerpts = sorted({by[x]["excerpt"] for x, _ in missed})
    spec = test_spec(a.dense_s)
    a.dense_out.mkdir(parents=True, exist_ok=True)
    emb = Embedder(a.device, "float32")
    dense, cost = {}, {"items": 0, "seconds": 0.0, "vectors": 0, "bytes": 0, "vectors_5s": 0}
    for f in sorted(f for f in variants if by[f]["excerpt"] in excerpts):
        out = a.dense_out / (f.replace("/", "__") + ".npz")
        t1 = time.time()
        if out.exists():
            z = np.load(out)
            ks, ts, v = z["segment_index"], z["pts_s"], z["vectors"]
        else:
            ks, ts, v = encode(a.set / f, spec, emb)
            np.savez(out, segment_index=ks, pts_s=ts, vectors=v)
            cost["seconds"] += time.time() - t1
        dense[f] = (ts, v)
        cost["items"] += 1
        cost["vectors"] += int(len(v))
        cost["bytes"] += int(v.nbytes)
        cost["vectors_5s"] += int(len(vec5[f]))
    rows = []
    for x, y in pos:
        if by[x]["excerpt"] not in excerpts:
            continue
        a5, b5 = vec5[x], vec5[y]
        r5 = equivalence(a5, b5, params, v2)
        b5x = min(bound(a5.pts_s, a5.vectors, b5.pts_s, b5.vectors, s, 5.0),
                  bound(b5.pts_s, b5.vectors, a5.pts_s, a5.vectors, s, 5.0))
        (ta, va), (tb, vb) = dense[x], dense[y]
        bdx = min(bound(ta, va, tb, vb, s, a.dense_s), bound(tb, vb, ta, va, s, a.dense_s))
        rows.append({"pair": [x, y], "variants": [by[x]["variant"], by[y]["variant"]],
                     "v2_linked_5s": r5.linked, "v2_coverage_5s": [r5.coverage_a, r5.coverage_b],
                     "bound_5s": round(b5x, 4), f"bound_{a.dense_s:g}s": round(bdx, 4)})
    key = f"bound_{a.dense_s:g}s"
    miss_rows = [r for r in rows if not r["v2_linked_5s"]]
    report = {
        "benchmark": "P10-EQUIV-DENSITY", "set": man.get("corpus", str(a.set)), "seconds": round(time.time() - t0),
        "rule": "equiv/v2", "rule_digest": v2.digest(), "s_seg": s, "coverage_required": v2.coverage,
        "base_spec_digest": str(test_spec(5.0).digest), "dense_spec_digest": str(spec.digest),
        "dense_segment_duration_s": a.dense_s,
        "equivalent_pairs": len(pos), "v2_misses_5s": len(missed), "missed_excerpts": excerpts,
        "pairs_of_missed_excerpts": len(rows),
        "misses_with_bound_ge_coverage": {"5s": sum(r["bound_5s"] >= v2.coverage for r in miss_rows),
                                          f"{a.dense_s:g}s": sum(r[key] >= v2.coverage for r in miss_rows)},
        "linked_pairs_with_dense_bound_below_coverage": sum(r[key] < v2.coverage for r in rows if r["v2_linked_5s"]),
        "dense_cost": {**cost, "seconds": round(cost["seconds"], 1),
                       "vectors_ratio": round(cost["vectors"] / max(cost["vectors_5s"], 1), 3)},
        "pairs": rows,
    }
    a.out.parent.mkdir(parents=True, exist_ok=True)
    a.out.write_text(json.dumps(report, indent=1, sort_keys=True, default=str))
    print(json.dumps({k: v for k, v in report.items() if k != "pairs"}, indent=1, default=str))


if __name__ == "__main__":
    main()
