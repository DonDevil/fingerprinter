"""Stage 1-3: source inventory, integrity (container-level) and ffprobe metadata. Source files are only read."""
from __future__ import annotations

import json
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from fractions import Fraction

from common import EXTS, SRC_DIR, cfg_fp, d, jdump, jload, mark_done, sha256_file, stage_done


def _probe(path) -> dict:
    o = subprocess.run(["ffprobe", "-v", "error", "-show_streams", "-show_format", "-of", "json", str(path)],
                       capture_output=True, text=True, timeout=300)
    return json.loads(o.stdout or "{}") | {"_rc": o.returncode, "_err": o.stderr[-2000:]}


def _packet_scan(path) -> dict:
    """Demux-only scan of video packets: counts, PTS monotonicity, gaps, CFR/VFR evidence, container errors."""
    p = subprocess.run(["ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries", "packet=pts_time,flags",
                        "-of", "csv=p=0", str(path)], capture_output=True, text=True, timeout=1800)
    ts = []
    for ln in p.stdout.splitlines():
        a = ln.split(",")[0]
        try:
            ts.append(float(a))
        except ValueError:
            pass
    ts.sort()
    gaps = [b - a for a, b in zip(ts, ts[1:])]
    if not gaps:
        return {"packets": len(ts), "errors": p.stderr[-2000:]}
    med = sorted(gaps)[len(gaps) // 2]
    big = [(round(ts[i], 3), round(g, 3)) for i, g in enumerate(gaps) if g > max(1.0, 20 * med)]
    uniq = sorted(set(round(g, 4) for g in gaps))
    return {"packets": len(ts), "median_dt": round(med, 5), "n_distinct_dt": len(uniq),
            "vfr_suspect": len(uniq) > 6, "big_gaps": big[:20], "n_big_gaps": len(big),
            "last_pts_s": ts[-1], "errors": p.stderr[-2000:]}


def run(workers: int = 2) -> dict:
    files = sorted(f for f in SRC_DIR.iterdir() if f.is_file() and f.suffix.lower() in EXTS)
    fp = cfg_fp({"files": [(f.name, f.stat().st_size, int(f.stat().st_mtime)) for f in files], "v": 1})
    if stage_done("inventory", fp):
        return jload(d("manifests", "movies.json"))
    t0 = time.time()
    ids = {f.name: f"movie_{i + 1:03d}" for i, f in enumerate(files)}

    def one(f):
        t = time.time()
        sha = sha256_file(f)
        t_sha = time.time() - t
        pr = _probe(f)
        pk = _packet_scan(f)
        v = next((s for s in pr.get("streams", []) if s.get("codec_type") == "video"), {})
        au = [{"codec": s.get("codec_name"), "channels": s.get("channels"), "lang": (s.get("tags") or {}).get("language")}
              for s in pr.get("streams", []) if s.get("codec_type") == "audio"]
        fmt = pr.get("format", {})
        rf, af = v.get("r_frame_rate", "0/1"), v.get("avg_frame_rate", "0/1")
        return {"movie_id": ids[f.name], "source_path": str(f), "size_bytes": f.stat().st_size, "sha256": sha,
                "sha256_seconds": round(t_sha, 1), "duration_s": float(fmt.get("duration") or 0),
                "bit_rate": int(fmt.get("bit_rate") or 0), "container": fmt.get("format_name"),
                "video": {"codec": v.get("codec_name"), "profile": v.get("profile"), "width": v.get("width"),
                          "height": v.get("height"), "pix_fmt": v.get("pix_fmt"), "r_frame_rate": rf, "avg_frame_rate": af,
                          "fps": float(Fraction(af)) if af not in ("0/0", "0/1") else None,
                          "bit_rate": v.get("bit_rate"), "color_space": v.get("color_space"),
                          "field_order": v.get("field_order"), "nb_frames": v.get("nb_frames"),
                          "rotation": next((sd.get("rotation") for sd in v.get("side_data_list", []) if "rotation" in sd), None)},
                "audio": au, "packets": pk, "ffprobe_rc": pr["_rc"], "ffprobe_err": pr["_err"]}

    with ThreadPoolExecutor(workers) as ex:  # bounded: the source is one rotational disk
        recs = list(ex.map(one, files))
    recs.sort(key=lambda r: r["movie_id"])
    # the filename mapping stays local; the shareable manifest has no filenames
    jdump({r["movie_id"]: r["source_path"] for r in recs}, d("manifests", "filename_map.LOCAL.json"))
    pub = [{k: v for k, v in r.items() if k != "source_path"} for r in recs]
    jdump(pub, d("manifests", "movies.json"))
    mark_done("inventory", fp, {"seconds": round(time.time() - t0, 1)})
    return pub


if __name__ == "__main__":
    for r in run():
        v = r["video"]
        print(r["movie_id"], f'{r["size_bytes"]/1e9:.2f}GB', f'{r["duration_s"]/60:.1f}min', v["codec"], f'{v["width"]}x{v["height"]}',
              v["avg_frame_rate"], "VFR?" if r["packets"].get("vfr_suspect") else "cfr", "gaps", r["packets"].get("n_big_gaps"))
