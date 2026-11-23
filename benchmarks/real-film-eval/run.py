#!/usr/bin/env python
"""Single entry point of the real-film evaluation. Stages are resumable: each writes
``local-data/real-film-evaluation/metrics/stage-<name>.done.json`` keyed by a config fingerprint.

    taskset -c 0-9 nice -n 10 env/bin/python benchmarks/real-film-eval/run.py --stages all
"""
from __future__ import annotations

import argparse
import sys
import time
import traceback
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parents[1]))

from common import DATA, d, jload  # noqa: E402

STAGES = ["inventory", "embed_full", "segment", "validate", "excerpts", "clips", "embed_clips", "windows",
          "hardneg", "evaluate", "report"]


def stage_embed_full(emb) -> None:
    import numpy as np

    from common import cfg_fp, jdump, mark_done, stage_done
    from encode import encode_video, validate
    movies = jload(d("manifests", "movies.json"))
    paths = jload(d("manifests", "filename_map.LOCAL.json"))
    fp = cfg_fp({"v": 2, "grid": 1.0, "rates": ["1fps"], "movies": [m["sha256"] for m in movies]})
    if stage_done("embed_full", fp):
        return
    allstats = {}
    for m in movies:
        mid = m["movie_id"]
        out = d("embeddings", "full", f"{mid}.npz")
        sf = d("metrics", "embed_full", f"{mid}.json")
        if out.exists() and sf.exists() and jload(sf).get("source_sha256") == m["sha256"]:
            allstats[mid] = jload(sf)
            continue
        print(time.strftime("%H:%M:%S"), "encoding", mid, flush=True)
        r = encode_video(Path(paths[mid]), emb, ["1fps"], grid_s=1.0)
        np.savez(out, k=r["k"], pts=r["pts"], vec=r["vec"])
        st = {**r["stats"], "movie_id": mid, "source_sha256": m["sha256"], "validation": validate(r["vec"])}
        jdump(st, sf)
        allstats[mid] = st
        print(mid, {k: st[k] for k in ("embedded", "failed_frames", "error", "wall_s", "end_to_end_fps", "embed_throughput_fps")}, flush=True)
    mark_done("embed_full", fp, {"films": len(allstats)})


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--stages", default="all")
    ap.add_argument("--device", default="cuda")
    a = ap.parse_args()
    todo = STAGES if a.stages == "all" else a.stages.split(",")
    emb = None
    for s in todo:
        t = time.time()
        print(time.strftime("%H:%M:%S"), "=== stage", s, flush=True)
        try:
            if s == "inventory":
                import inventory
                inventory.run()
            elif s == "embed_full":
                if emb is None:
                    from fingerprinter.model import Embedder
                    emb = Embedder(a.device, "float32")
                stage_embed_full(emb)
            else:
                if emb is None and s in ("validate", "embed_clips", "windows"):
                    from fingerprinter.model import Embedder
                    emb = Embedder(a.device, "float32")
                import importlib
                importlib.import_module(f"stage_{s}").run(emb)
        except Exception:  # noqa: BLE001
            traceback.print_exc()
            (DATA / "logs" / f"stage-{s}.FAILED").write_text(traceback.format_exc())
            raise
        print(time.strftime("%H:%M:%S"), f"stage {s} done in {time.time() - t:.0f}s", flush=True)


if __name__ == "__main__":
    main()
