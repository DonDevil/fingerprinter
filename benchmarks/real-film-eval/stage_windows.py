"""Hard-negative windows (top pairs) at the 0.1 s grid for the 2/5 fps comparison, plus contact sheets for visual review."""
from __future__ import annotations

import subprocess
from pathlib import Path

import numpy as np
from PIL import Image, ImageDraw

from common import cfg_fp, d, jload, jdump, mark_done, stage_done
from encode import encode_video, validate

TOP, DUR = 40, 60


def _window(seg: dict, centre: float) -> tuple[int, int]:
    dur = int(min(DUR, seg["duration_s"]))
    start = int(np.clip(round(centre - dur / 2), np.ceil(seg["start_s"]), max(np.ceil(seg["start_s"]), int(seg["end_s"]) - dur)))
    return start, dur


def frame(src: str, t: float, w: int = 320) -> Image.Image | None:
    p = subprocess.run(["ffmpeg", "-nostdin", "-loglevel", "error", "-ss", str(t), "-i", src, "-frames:v", "1", "-vf", f"scale={w}:-2",
                        "-f", "image2pipe", "-vcodec", "png", "-"], capture_output=True)
    if not p.stdout:
        return None
    import io
    return Image.open(io.BytesIO(p.stdout)).convert("RGB")


def run(emb) -> None:
    hn = jload(d("pairs", "hard-negatives", "candidates.json"))
    paths = jload(d("manifests", "filename_map.LOCAL.json"))
    segs = {s["segment_id"]: s for s in jload(d("manifests", "segments.json"))}
    fp = cfg_fp({"v": 1, "top": TOP, "n": len(hn)})
    if stage_done("windows", fp):
        return
    top = sorted(hn, key=lambda h: -h["embedding_similarity_chamfer_1fps"])[:TOP]
    wins = {}
    for h in top:
        sa, sb = segs[h["segment_a"]], segs[h["segment_b"]]
        za = np.load(d("embeddings", "full", f"{sa['movie_id']}.npz")); zb = np.load(d("embeddings", "full", f"{sb['movie_id']}.npz"))
        ca = (sa["start_s"] + sa["end_s"]) / 2
        ia = np.argmin(abs(za["pts"] - ca)); sel = (zb["pts"] >= sb["start_s"]) & (zb["pts"] < sb["end_s"])
        jb = np.where(sel)[0][np.argmax(zb["vec"][sel] @ za["vec"][ia])]
        wins[h["pair_id"]] = {"a": (sa, _window(sa, ca)), "b": (sb, _window(sb, float(zb["pts"][jb])))}
    for pid, w in wins.items():
        for side in "ab":
            seg, (s, dur) = w[side]
            out = d("embeddings", "windows", f"{pid}_{side}.npz")
            if out.exists():
                continue
            r = encode_video(Path(paths[seg["movie_id"]]), emb, ["5fps", "2fps"], window=(s, dur))
            np.savez(out, k=r["k"], pts=r["pts"], vec=r["vec"])
            jdump({**r["stats"], "validation": validate(r["vec"])}, d("metrics", "embed_clips", f"{pid}_{side}.json"))
    jdump({pid: {s: {"segment": w[s][0]["segment_id"], "start_s": w[s][1][0], "dur_s": w[s][1][1]} for s in "ab"} for pid, w in wins.items()},
          d("pairs", "hard-negatives", "windows.json"))
    # contact sheets: 5 pairs per sheet, 3 frames per side
    sheets = d("pairs", "hard-negatives", "sheets", "x")
    ids = list(wins)
    for si in range(0, len(ids), 5):
        rows = []
        for pid in ids[si:si + 5]:
            row = []
            for side in "ab":
                seg, (s, dur) = wins[pid][side]
                for f in (0.15, 0.5, 0.85):
                    im = frame(paths[seg["movie_id"]], s + f * dur)
                    row.append(im or Image.new("RGB", (320, 134)))
            rows.append((pid, row))
        H = max(im.height for _, r in rows for im in r)
        sheet = Image.new("RGB", (6 * 320 + 10, len(rows) * (H + 18)), "white")
        dr = ImageDraw.Draw(sheet)
        for ri, (pid, row) in enumerate(rows):
            dr.text((2, ri * (H + 18) + 2), f"{pid}   A=[1-3]  B=[4-6]", fill="black")
            for ci, im in enumerate(row):
                sheet.paste(im, (ci * 320 + (10 if ci >= 3 else 0), ri * (H + 18) + 16))
        sheet.save(f"{sheets.parent}/sheet_{si // 5:02d}.jpg", quality=80)
    mark_done("windows", fp, {"pairs": len(wins)})
