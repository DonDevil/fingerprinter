"""Production decode -> production preprocess -> production DINOv2 embedder, with stage timings (FP-08).

Nothing here re-implements fingerprinting: ``fingerprinter.media.decode`` / ``preprocess`` and
``fingerprinter.model.Embedder`` are used unchanged. The only deviation from ``dinov2-seg/2`` is the *sampling grid*:
a test spec identical to the registered spec except ``sampling.segment_duration_s = 0.1`` (same approach as
benchmarks/p10-index/equiv_density.py). Because the production selector picks the first frame at/after each
grid boundary, the coarser grids nest exactly (verified in ``verify_nesting`` for CFR sources):

    0.1 s grid index k:   5 fps -> k % 2 == 0     2 fps -> k % 5 == 0     1 fps -> k % 10 == 0     0.2 fps (production) -> k % 50 == 0
"""
from __future__ import annotations

import queue
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from common import wait_until_cool  # noqa: E402

GRID_S = 0.1
WINDOW: tuple[float, float] | None = None  # (start_s, duration_s) input seek for window decodes
RATE_MOD = {"5fps": (2, 0), "2fps": (5, 0), "1fps": (10, 0), "0.2fps": (50, 0)}
CHUNK, BATCH = 64, 16


def test_spec(seconds: float = GRID_S):
    from fingerprinter.spec import Spec, SpecDocument, dinov2_seg_v2

    doc = dinov2_seg_v2().document.to_json_obj()
    doc["sampling"]["segment_duration_s"] = float(seconds)
    return Spec(SpecDocument.from_json_obj(doc))


def rate_mask(k: np.ndarray, rate: str) -> np.ndarray:
    m, r = RATE_MOD[rate]
    return (k % m) == r


def keep_fn(rates: list[str]):
    mods = [RATE_MOD[r] for r in rates]
    return lambda k: any(k % m == r for m, r in mods)


def use_gpu_decode() -> None:
    """Insert ``-hwaccel cuda`` (NVDEC) in front of the production decode command. Frames were verified byte-identical
    to the software decode (md5 over 1200 frames, movie_001 and movie_003); the filter/convert/preprocess path is unchanged."""
    import fingerprinter.media as M
    if getattr(M.decode_command, "_gpu", False):
        return
    orig = M.decode_command

    def cmd(path, spec):
        c = orig(path, spec)
        i = c.index("-i")
        win = ["-ss", str(WINDOW[0]), "-t", str(WINDOW[1])] if WINDOW else []
        return c[:i] + ["-hwaccel", "cuda"] + win + c[i:]
    cmd._gpu = True
    M.decode_command = cmd


def encode_video(path: Path, emb, rates: list[str], *, spec=None, thermal: bool = True, grid_s: float = GRID_S,
                 window: tuple[float, float] | None = None) -> dict:
    """Decode ``path`` on the 0.1 s grid, embed the frames needed by ``rates``. Returns arrays + stats.
    A decode error is recorded (message, last good pts) and the partial result is returned, never swallowed."""
    from fingerprinter.media import decode, preprocess

    global WINDOW
    WINDOW = window
    use_gpu_decode()
    spec = spec or test_spec(grid_s)
    keep = keep_fn(rates) if grid_s == GRID_S else (lambda k: True)
    q: queue.Queue = queue.Queue(maxsize=3)
    stats = {"decoded_selected": 0, "kept": 0, "preprocess_s": 0.0, "decode_wait_s": 0.0, "failed_frames": 0,
             "error": None, "last_good_pts_s": None}
    t0 = time.time()

    def producer() -> None:
        chunk_k, chunk_t, chunk_x, pool = [], [], [], ThreadPoolExecutor(3)
        pending: list = []

        def flush() -> None:
            if not pending:
                return
            t = time.time()
            xs = []
            for k, ts, f in pending:
                try:
                    xs.append(f.result())
                except Exception:  # noqa: BLE001
                    stats["failed_frames"] += 1
                    xs.append(None)
            ok = [i for i, x in enumerate(xs) if x is not None]
            stats["preprocess_s"] += time.time() - t
            if ok:
                q.put((np.array([pending[i][0] for i in ok], np.int64), np.array([pending[i][1] for i in ok], np.float64),
                       np.stack([xs[i] for i in ok])))
            pending.clear()

        try:
            it = decode(path, spec)
            while True:
                t = time.time()
                try:
                    f = next(it)
                except StopIteration:
                    break
                stats["decode_wait_s"] += time.time() - t
                stats["decoded_selected"] += 1
                if thermal and stats["decoded_selected"] % 16 == 0:
                    # not reading the pipe stalls ffmpeg (64 KiB pipe), so this throttles the decoder too
                    stats["thermal_wait_decode_s"] = stats.get("thermal_wait_decode_s", 0.0) + wait_until_cool()
                stats["last_good_pts_s"] = f.pts_s
                if keep(f.segment_index):
                    stats["kept"] += 1
                    pending.append((f.segment_index, f.pts_s, pool.submit(preprocess, f.rgb.copy())))
                    if len(pending) >= CHUNK:
                        flush()
            flush()
        except Exception as exc:  # noqa: BLE001 - recorded, never silent
            flush()
            stats["error"] = f"{type(exc).__name__}: {exc}"[:500]
        finally:
            pool.shutdown()
            q.put(None)

    th = threading.Thread(target=producer, daemon=True)
    th.start()
    ks, ts, vs = [], [], []
    gpu_s, thermal_wait = 0.0, 0.0
    emb.reset_peak()
    while (item := q.get()) is not None:
        k, t, x = item
        if thermal:
            thermal_wait += wait_until_cool()
        t1 = time.time()
        out = [emb.embed(x[i:i + BATCH]) for i in range(0, len(x), BATCH)]
        emb.sync()
        gpu_s += time.time() - t1
        ks.append(k)
        ts.append(t)
        vs.append(np.concatenate(out))
    th.join()
    k = np.concatenate(ks) if ks else np.zeros(0, np.int64)
    t = np.concatenate(ts) if ts else np.zeros(0)
    v = np.concatenate(vs).astype(np.float32) if vs else np.zeros((0, 768), np.float32)
    wall = time.time() - t0
    stats.update(embedded=int(len(k)), gpu_inference_s=round(gpu_s, 2), thermal_wait_s=round(thermal_wait, 1),
                 wall_s=round(wall, 2), preprocess_s=round(stats["preprocess_s"], 2),
                 decode_wait_s=round(stats["decode_wait_s"], 2), embed_throughput_fps=round(len(k) / max(gpu_s, 1e-9), 1),
                 end_to_end_fps=round(len(k) / max(wall - thermal_wait, 1e-9), 1), vram=emb.vram(), rates=rates,
                 grid_s=grid_s, gpu_decode=True, spec_digest=str(spec.digest))
    return {"k": k, "pts": t, "vec": v, "stats": stats}


def validate(vec: np.ndarray, dim: int = 768) -> dict:
    n = np.linalg.norm(vec.astype(np.float64), axis=1) if len(vec) else np.zeros(0)
    return {"n": int(len(vec)), "dim_ok": vec.ndim == 2 and vec.shape[1] == dim, "dtype": str(vec.dtype),
            "nan": int(np.isnan(vec).sum()), "inf": int(np.isinf(vec).sum()),
            "norm_min": float(n.min()) if len(n) else None, "norm_max": float(n.max()) if len(n) else None,
            "norm_ok": bool(len(n) == 0 or (abs(n - 1) < 1e-4).all())}
