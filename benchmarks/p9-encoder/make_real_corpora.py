"""Derive the gate corpora C-PREC, L-PAIRS and SOAK-1000 from the licensed S-SRC films.

Definitions: data-preparation §2.2 (C-PREC), §2.3 (L-PAIRS), §2.4 (SOAK-1000).
Every output is produced by a fixed, seeded ffmpeg command that is recorded in
the manifest together with the output's SHA-256, size, probe summary and its
provenance (the S-SRC films it contains, with their licences). Media stay
outside git (``~/anti_piracy/datasets/p9/``). Re-running a step skips outputs
already recorded with a matching SHA-256 (resumable on a laptop).

Steps (each needs the previous ones):

    env/bin/python -I benchmarks/p9-encoder/make_real_corpora.py reel
    env/bin/python -I benchmarks/p9-encoder/make_real_corpora.py c-prec
    env/bin/python -I benchmarks/p9-encoder/make_real_corpora.py l-pairs
    env/bin/python -I benchmarks/p9-encoder/make_real_corpora.py soak-1000
    env/bin/python -I benchmarks/p9-encoder/make_real_corpora.py verify-soak   # re-check only

``reel`` normalizes every film once to a 1920×800, 24 fps H.264 mezzanine
(aspect kept, padded) and concatenates them: clips longer than one film are
windows of this reel. Resolution classes are named by the 16:9 width
(1080p = 1920, 720p = 1280, 480p = 854, 360p = 640, 240p = 426); the frame
height follows the 2.4:1 reel (800/534/356/266/178).

L-PAIRS labels are **provenance only** (which film a clip was cut from); no
model output is used to build or check them.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import random
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

DATA = Path.home() / "anti_piracy" / "datasets" / "p9"
GENERATOR = "fingerprinter benchmarks/p9-encoder/make_real_corpora.py"
REEL_W, REEL_H, REEL_FPS = 1920, 800, 24
RES = {"240p": (426, 178), "360p": (640, 266), "480p": (854, 356), "720p": (1280, 534), "1080p": (1920, 800)}
CODEC_ARGS = {
    "h264": ["-c:v", "libx264", "-preset", "veryfast", "-crf", "24", "-pix_fmt", "yuv420p"],
    "hevc": ["-c:v", "libx265", "-preset", "ultrafast", "-crf", "28", "-pix_fmt", "yuv420p",
             "-x265-params", "log-level=error"],
    "vp9": ["-c:v", "libvpx-vp9", "-deadline", "realtime", "-cpu-used", "8", "-row-mt", "1",
            "-b:v", "0", "-crf", "36", "-pix_fmt", "yuv420p"],
    "av1": ["-c:v", "libsvtav1", "-preset", "12", "-crf", "42", "-pix_fmt", "yuv420p"],
}
EXT = {"h264": "mp4", "hevc": "mkv", "vp9": "webm", "av1": "mkv"}
CONTAINERS = {"h264": ["mp4", "mkv"], "hevc": ["mp4", "mkv"], "vp9": ["webm", "mkv"], "av1": ["mkv", "webm", "mp4"]}


# ---------------------------------------------------------------- helpers
def ff(args: list[str]) -> None:
    cmd = ["ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error", "-y", *args]
    res = subprocess.run(cmd, capture_output=True, text=True, check=False)  # noqa: S603
    if res.returncode != 0:
        raise RuntimeError(f"ffmpeg failed ({res.returncode}): {' '.join(cmd)}\n{res.stderr[-2000:]}")


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1 << 22), b""):
            h.update(block)
    return h.hexdigest()


def probe(path: Path) -> dict[str, Any]:
    out = subprocess.run(  # noqa: S603
        ["ffprobe", "-v", "error", "-show_entries",  # noqa: S607
         "stream=codec_type,codec_name,width,height,r_frame_rate,color_space,color_range:format=duration,format_name",
         "-of", "json", str(path)], capture_output=True, text=True, check=False)
    if out.returncode != 0:
        return {"error": out.stderr.strip()[-200:]}
    o = json.loads(out.stdout)
    v = next((s for s in o.get("streams", []) if s.get("codec_type") == "video"), None)
    a = next((s for s in o.get("streams", []) if s.get("codec_type") == "audio"), None)
    res = {"format": o.get("format", {}).get("format_name"), "duration_s": float(o.get("format", {}).get("duration", 0) or 0)}
    if v:
        res.update({"codec": v.get("codec_name"), "width": v.get("width"), "height": v.get("height"),
                    "r_frame_rate": v.get("r_frame_rate"), "color_space": v.get("color_space"),
                    "color_range": v.get("color_range")})
    if a:
        res["audio_codec"] = a.get("codec_name")
    return res


class Store:
    """Resumable record of finished outputs (``progress.jsonl`` next to the corpus)."""

    def __init__(self, root: Path) -> None:
        self.root = root
        root.mkdir(parents=True, exist_ok=True)
        self.path = root / "progress.jsonl"
        self.done: dict[str, dict[str, Any]] = {}
        if self.path.exists():
            for line in self.path.read_text().splitlines():
                rec = json.loads(line)
                self.done[rec["file"]] = rec

    def finished(self, file: str) -> dict[str, Any] | None:
        rec = self.done.get(file)
        p = self.root / file
        if rec and p.exists() and p.stat().st_size == rec["bytes"]:
            return rec
        return None

    def record(self, rec: dict[str, Any]) -> None:
        self.done[rec["file"]] = rec
        with self.path.open("a") as f:
            f.write(json.dumps(rec) + "\n")


def build(store: Store, jobs: list[dict[str, Any]], workers: int, threads: int) -> list[dict[str, Any]]:
    """Run each job's ffmpeg command (unless already recorded) and return the records in job order."""

    def one(job: dict[str, Any]) -> dict[str, Any]:
        rec = store.finished(job["file"])
        if rec:
            return rec
        out = store.root / job["file"]
        out.parent.mkdir(parents=True, exist_ok=True)
        tmp = out.with_name(".part-" + out.name)
        args = [a.replace("{OUT}", str(tmp)) for a in job["args"]]
        ff([*args[:-1], "-threads", str(threads), args[-1]] if threads else args)
        for post in job.get("post", []):
            post(tmp)
        tmp.rename(out)
        rec = {k: v for k, v in job.items() if k not in ("args", "post")}
        rec.update({"command": ["ffmpeg", *[a.replace(str(DATA), "$DATA") for a in job["args"]]],
                    "sha256": sha256(out), "bytes": out.stat().st_size, "probe": probe(out),
                    "built_at": datetime.now(UTC).isoformat()})
        store.record(rec)
        print(f"  built {job['file']} ({rec['bytes'] / 2**20:.1f} MiB)", flush=True)
        return rec

    with ThreadPoolExecutor(max_workers=workers) as pool:
        return list(pool.map(one, jobs))


def s_src() -> dict[str, Any]:
    m = json.loads((DATA / "s-src" / "manifest.json").read_text())
    films = [i for i in m["items"] if i.get("licence_verified") and i.get("status", "").startswith("COMPLETE")]
    if len(films) < 6:
        sys.exit(f"S-SRC has {len(films)} verified films (< 6); refusing to derive gate corpora")
    for f in films:
        if sha256(DATA / "s-src" / f["file"]) != f["sha256"]:
            sys.exit(f"S-SRC checksum mismatch: {f['file']}")
    m["_sha256"] = hashlib.sha256((DATA / "s-src" / "manifest.json").read_bytes()).hexdigest()
    m["films"] = films
    return m


def reel_info() -> dict[str, Any]:
    return json.loads((DATA / "reel" / "manifest.json").read_text())


def films_in(reel: dict[str, Any], start: float, dur: float) -> list[str]:
    end = start + dur
    return [f["id"] for f in reel["films"] if f["reel_start_s"] < end and f["reel_start_s"] + f["duration_s"] > start]


def write_manifest(root: Path, corpus: str, ssrc: dict[str, Any], reel: dict[str, Any] | None,
                   items: list[dict[str, Any]], extra: dict[str, Any]) -> None:
    lic = {f["id"]: {"title": f["title"], "licence": f["licence"], "licence_url": f["licence_url"],
                     "attribution": f["attribution"], "sha256": f["sha256"]} for f in ssrc["films"]}
    shas = [i["sha256"] for i in items]
    if len(set(shas)) != len(shas):
        sys.exit(f"{corpus}: duplicate output bytes (distinct items must have distinct ContentIds)")
    manifest = {
        "corpus": corpus, "synthetic": False, "created_at": datetime.now(UTC).isoformat(),
        "generator": GENERATOR, "ffmpeg": subprocess.run(["ffmpeg", "-version"], capture_output=True, text=True,  # noqa: S603, S607
                                                         check=False).stdout.splitlines()[0],
        "parents": {"s-src_manifest_sha256": ssrc["_sha256"],
                    **({"reel_sha256": reel["sha256"]} if reel else {})},
        "licences": lic,
        "licence_note": "derived clips inherit the licences of the films they contain (item 'films'); "
                        "attribution required; local evaluation copies, not redistributed",
        **extra, "items": items,
    }
    (root / "manifest.json").write_text(json.dumps(manifest, indent=1) + "\n")
    print(json.dumps({"corpus": corpus, "items": len(items), "manifest": str(root / "manifest.json")}))


# ---------------------------------------------------------------- reel
def cmd_reel(a: argparse.Namespace) -> None:
    ssrc = s_src()
    root = DATA / "reel"
    store = Store(root)
    vf = (f"fps={REEL_FPS},scale={REEL_W}:{REEL_H}:force_original_aspect_ratio=decrease:flags=bicubic,"
          f"pad={REEL_W}:{REEL_H}:(ow-iw)/2:(oh-ih)/2,setsar=1,format=yuv420p")
    jobs = [{"file": f"films/{i:02d}-{f['id']}.mkv", "kind": "mezzanine", "film": f["id"],
             "args": ["-i", str(DATA / "s-src" / f["file"]), "-map", "0:v:0", "-an", "-vf", vf,
                      "-c:v", "libx264", "-preset", "veryfast", "-crf", "16", "-g", "48", "-keyint_min", "48",
                      "-sc_threshold", "0", "-colorspace", "bt709", "-color_primaries", "bt709",
                      "-color_trc", "bt709", "{OUT}"]}
            for i, f in enumerate(ssrc["films"])]
    recs = build(store, jobs, a.workers, a.threads)
    lst = root / "concat.txt"
    lst.write_text("".join(f"file '{root / r['file']}'\n" for r in recs))
    reel = root / "reel.mkv"
    if not reel.exists():
        ff(["-f", "concat", "-safe", "0", "-i", str(lst), "-c", "copy", str(reel)])
    t, films = 0.0, []
    for r, f in zip(recs, ssrc["films"], strict=True):
        frames = int(subprocess.run(  # noqa: S603
            ["ffprobe", "-v", "error", "-count_packets", "-select_streams", "v:0", "-show_entries",  # noqa: S607
             "stream=nb_read_packets", "-of", "csv=p=0", str(root / r["file"])],
            capture_output=True, text=True, check=True).stdout.strip())
        dur = frames / REEL_FPS
        films.append({"id": f["id"], "title": f["title"], "mezzanine": r["file"], "frames": frames,
                      "reel_start_s": round(t, 6), "duration_s": round(dur, 6), "film_sha256": f["sha256"],
                      "mezzanine_sha256": r["sha256"]})
        t += dur
    info = {"corpus": "reel", "created_at": datetime.now(UTC).isoformat(), "generator": GENERATOR,
            "s-src_manifest_sha256": ssrc["_sha256"], "file": "reel.mkv", "sha256": sha256(reel),
            "bytes": reel.stat().st_size, "duration_s": round(t, 6), "fps": REEL_FPS,
            "size": f"{REEL_W}x{REEL_H}", "mezzanine_filter": vf, "films": films}
    (root / "manifest.json").write_text(json.dumps(info, indent=1) + "\n")
    print(json.dumps({"reel_s": round(t, 1), "films": len(films)}))


def encode_job(file: str, reel: Path, start: float, dur: float, vf: str, codec: str, extra: list[str] | None = None,
               in_dur: float | None = None, **meta: Any) -> dict[str, Any]:
    out_extra = list(extra or [])
    if file.endswith(".mp4"):
        out_extra += ["-movflags", "+faststart"] + (["-tag:v", "hvc1"] if codec == "hevc" else [])
    return {"file": file, "codec": codec, "reel_start_s": round(start, 3), "seconds": round(dur, 3), **meta,
            "args": ["-ss", f"{start:.3f}", "-t", f"{in_dur or dur:.3f}", "-i", str(reel), "-map", "0:v:0", "-an",
                     "-vf", vf, *CODEC_ARGS[codec], *out_extra, "{OUT}"]}


# ---------------------------------------------------------------- C-PREC (§2.2)
def cmd_cprec(a: argparse.Namespace) -> None:
    ssrc, reel = s_src(), reel_info()
    root = DATA / "c-prec"
    store = Store(root)
    rp = DATA / "reel" / "reel.mkv"
    rng = random.Random(a.seed)
    jobs = []
    clip_s = 900.0
    n = 0
    for _rep in range(2):
        for res in ("360p", "720p", "1080p"):
            for codec in ("h264", "hevc", "vp9"):
                for fps in (24, 30, 60):
                    w, h = RES[res]
                    start = round(rng.uniform(0, reel["duration_s"] - clip_s), 3)
                    vf = f"fps={fps},scale={w}:{h}:flags=bicubic,format=yuv420p"
                    jobs.append(encode_job(f"cprec-{n:02d}-{res}-{codec}-{fps}.{EXT[codec]}", rp, start, clip_s, vf,
                                           codec, kind="clip", category="balanced", res=res, fps=fps,
                                           films=films_in(reel, start, clip_s)))
                    n += 1
    w, h = RES["720p"]
    base = f"scale={w}:{h}:flags=bicubic"
    hard = [
        ("dark", f"{base},eq=brightness=-0.12:gamma=0.45,format=yuv420p", [], None),
        ("flat-colour", f"{base},drawbox=x=0:y=0:w=iw:h=ih:color=0x3c5a78:t=fill:enable='lt(mod(t,20),10)',format=yuv420p", [], None),
        ("high-motion", f"setpts=PTS/4,fps=24,{base},format=yuv420p", [], 1200.0),
        ("letterbox", f"{base},pad=1280:720:0:93,format=yuv420p", [], None),
        ("pillarbox", "crop=ih*4/3:ih,scale=720:540:flags=bicubic,pad=960:540:120:0,format=yuv420p", [], None),
        ("colour-bt601", f"scale={w}:{h}:flags=bicubic:out_color_matrix=bt601,format=yuv420p",
         ["-colorspace", "smpte170m", "-color_primaries", "smpte170m", "-color_trc", "smpte170m"], None),
        ("colour-full-range", f"scale={w}:{h}:flags=bicubic:out_range=full,format=yuv420p", ["-color_range", "pc"], None),
        ("vfr", f"{base},setpts='(N/24+0.02*floor(N/5))/TB',format=yuv420p", ["-fps_mode", "passthrough"], None),
        ("pts-offset", f"{base},format=yuv420p", ["-output_ts_offset", "7.5"], None),
    ]
    for name, vf, extra, in_dur in hard:
        dur = 300.0
        start = round(rng.uniform(0, reel["duration_s"] - (in_dur or dur)), 3)
        ext = "mkv" if name in ("vfr", "pts-offset") else "mp4"
        jobs.append(encode_job(f"cprec-{n:02d}-hard-{name}.{ext}", rp, start, dur, vf, "h264", extra, in_dur,
                               kind="clip", category=f"hard:{name}", res="720p", fps=24,
                               films=films_in(reel, start, in_dur or dur)))
        n += 1
    recs = build(store, jobs, a.workers, a.threads)
    write_manifest(root, "C-PREC", ssrc, reel, recs, {
        "definition": "data-preparation §2.2: 54 balanced clips ({360p,720p,1080p} × {H.264,HEVC,VP9} × {24,30,60} fps, "
                      "2 each, 15 min at seeded fixed reel offsets) + 9 hard/golden cases (5 min, 720p H.264)",
        "seed": a.seed})


# ---------------------------------------------------------------- L-PAIRS (§2.3)
TRANSFORMS = {
    "re-encode": "scale=1280:534:flags=bicubic",
    "downscale-360p": "scale=640:266:flags=bicubic",
    "crop-10pct": "crop=iw*0.9:ih*0.9,scale=1280:534:flags=bicubic",
    "letterbox": "scale=1280:534:flags=bicubic,pad=1280:720:0:93",
    "pillarbox": "crop=ih*4/3:ih,scale=720:540:flags=bicubic,pad=960:540:120:0",
}
SYNTH = ["testsrc2", "mandelbrot", "life=s=640x266:mold=10:ratio=0.1", "cellauto=s=640x266", "smptehdbars",
         "gradients=s=640x266:speed=0.02"]


def cmd_lpairs(a: argparse.Namespace) -> None:
    ssrc, reel = s_src(), reel_info()
    root = DATA / "l-pairs"
    store = Store(root)
    rp = DATA / "reel" / "reel.mkv"
    rng = random.Random(a.seed)
    films = reel["films"]
    tdir = root / "targets"
    tdir.mkdir(parents=True, exist_ok=True)
    targets = []
    by_id = {f["id"]: f for f in ssrc["films"]}
    for f in films:
        src = DATA / "s-src" / by_id[f["id"]]["file"]
        link = tdir / src.name
        if not link.exists():
            link.symlink_to(os.path.relpath(src, tdir))
        targets.append({"file": f"targets/{src.name}", "kind": "target", "film": f["id"],
                        "sha256": by_id[f["id"]]["sha256"], "bytes": src.stat().st_size,
                        "probe": by_id[f["id"]]["probe"], "films": [f["id"]],
                        "note": "the S-SRC original (symlink)"})
    names = list(TRANSFORMS)
    jobs = []
    for i in range(a.positives):
        f = films[i % len(films)]
        dur = min(math.exp(rng.uniform(math.log(30), math.log(600))), f["duration_s"] - 2)
        start = f["reel_start_s"] + rng.uniform(0, f["duration_s"] - dur - 1)
        transform = names[(i // len(films)) % len(names)]
        codec = ("h264", "hevc", "vp9")[i % 3]
        jobs.append(encode_job(f"clips/lp-{i:03d}-{f['id']}-{transform}.{EXT[codec]}", rp, start, dur,
                               TRANSFORMS[transform] + ",format=yuv420p", codec, kind="clip", source_film=f["id"],
                               transform=transform, films=[f["id"]]))
    for j in range(a.synthetic):
        src = SYNTH[j % len(SYNTH)]
        size = "" if "s=" in src else "=size=640x266"
        dur = round(rng.uniform(30, 120), 1)
        jobs.append({"file": f"clips/lp-syn-{j:02d}.mp4", "kind": "clip", "codec": "h264", "source_film": None,
                     "transform": f"synthetic lavfi {src.split('=')[0]}", "films": [], "seconds": dur,
                     "args": ["-f", "lavfi", "-i", f"{src}{size}:rate=24",
                              "-t", str(dur), "-vf", f"hue=h={rng.randint(0, 359)},scale=640:266,format=yuv420p",
                              *CODEC_ARGS["h264"], "-movflags", "+faststart", "{OUT}"]})
    recs = build(store, jobs, a.workers, a.threads)
    pairs = []
    nf = len(films)
    for i, r in enumerate(recs[: a.positives]):
        fi = i % nf
        pairs.append({"target": Path(targets[fi]["file"]).name, "clip": Path(r["file"]).name, "label": "1",
                      "transform": r["transform"], "basis": f"provenance: cut from {films[fi]['id']}"})
        for k in (1 + (i % (nf - 1)), 1 + ((i + 3) % (nf - 1))):
            other = (fi + k) % nf
            pairs.append({"target": Path(targets[other]["file"]).name, "clip": Path(r["file"]).name, "label": "0",
                          "transform": r["transform"],
                          "basis": f"provenance: cut from {films[fi]['id']}, target is {films[other]['id']}"})
    for j, r in enumerate(recs[a.positives:]):
        pairs.append({"target": Path(targets[j % nf]["file"]).name, "clip": Path(r["file"]).name, "label": "0",
                      "transform": r["transform"], "basis": "provenance: synthetic clip, no film content"})
    import csv  # noqa: PLC0415

    with (root / "l-pairs.csv").open("w", newline="") as fh:
        wr = csv.DictWriter(fh, fieldnames=["target", "clip", "label", "transform", "basis"])
        wr.writeheader()
        wr.writerows(pairs)
    pos = sum(p["label"] == "1" for p in pairs)
    write_manifest(root, "L-PAIRS", ssrc, reel, targets + recs, {
        "definition": "data-preparation §2.3: targets = the S-SRC films; positives = excerpts (30 s–10 min, "
                      "log-uniform) of the target with one transform (re-encode, 360p downscale, 10 % crop, "
                      "letterbox, pillarbox); negatives = each positive clip against two other films + synthetic clips",
        "labels": "provenance only (l-pairs.csv); no model output used", "seed": a.seed,
        "pairs": {"total": len(pairs), "positives": pos, "negatives": len(pairs) - pos,
                  "csv_sha256": sha256(root / "l-pairs.csv")}})


# ---------------------------------------------------------------- SOAK-1000 (§2.4)
def _truncate(frac: float):
    def post(p: Path) -> None:
        data = p.read_bytes()
        p.write_bytes(data[: int(len(data) * frac)])
    return post


def _garble(seed: int):
    def post(p: Path) -> None:
        data = bytearray(p.read_bytes())
        r = random.Random(seed)
        data[:4096] = bytes(r.randrange(256) for _ in range(4096))
        p.write_bytes(bytes(data))
    return post


def decoder_outcome(path: Path) -> dict[str, Any]:
    """What the production decoder does with ``path``: probe + full decode under the
    ``dinov2-seg/2`` decoder policy (explicit-indicators/v1) on this host's ffmpeg, no inference."""
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from fingerprinter.errors import EncodeError  # noqa: PLC0415
    from fingerprinter.media import decode  # noqa: PLC0415
    from fingerprinter.media import probe as fp_probe  # noqa: PLC0415
    from fingerprinter.spec import dinov2_seg_v2  # noqa: PLC0415

    frames = 0
    try:
        fp_probe(path)
        for _ in decode(path, dinov2_seg_v2()):
            frames += 1
    except EncodeError as exc:
        return {"outcome": exc.failure.value, "retryable": exc.retryable, "detail": exc.detail, "frames": frames}
    return {"outcome": "READY", "retryable": False, "detail": "", "frames": frames}


def verify_expected_failures(root: Path, recs: list[dict[str, Any]]) -> list[str]:
    """Check every item labelled as an expected failure against the actual decoder.

    An item passes when the decoder ends it with a non-retryable failure in its
    ``expect`` classes; the outcome is recorded on the item (``verified``). Returns
    the files that do not fail as labelled (the corpus is then not accepted).
    """
    bad = []
    for rec in recs:
        if not rec["expect"].startswith("FAILED:"):
            continue
        classes = rec["expect"].removeprefix("FAILED:").split("|")
        out = decoder_outcome(root / rec["file"])
        ok = out["outcome"] in classes and not out["retryable"]
        rec["verified"] = {**out, "expected": classes, "ok": ok, "spec": "dinov2-seg/2",
                           "truncation_policy": "explicit-indicators/v1"}
        print(f"  verify {rec['file']}: {out['outcome']} ({out['detail'][:80]}) {'ok' if ok else 'NOT AS LABELLED'}",
              flush=True)
        if not ok:
            bad.append(rec["file"])
    return bad


def cmd_verify_soak(a: argparse.Namespace) -> None:
    """Re-run the expected-failure verification on the built SOAK-1000 manifest (read-only)."""
    root = DATA / "soak-1000"
    recs = [dict(r) for r in json.loads((root / "manifest.json").read_text())["items"]]
    bad = verify_expected_failures(root, recs)
    print(json.dumps({"checked": sum(r["expect"].startswith("FAILED:") for r in recs), "not_as_labelled": bad}))
    if bad:
        sys.exit(1)


def cmd_soak(a: argparse.Namespace) -> None:  # noqa: PLR0915
    ssrc, reel = s_src(), reel_info()
    root = DATA / "soak-1000"
    store = Store(root)
    rp = DATA / "reel" / "reel.mkv"
    rng = random.Random(a.seed)
    # variant reels (stream-copy sources): every resolution × codec once, fps rotated so each fps appears 5×
    variants = {}
    vjobs = []
    for ri, res in enumerate(RES):
        for ci, codec in enumerate(CODEC_ARGS):
            fps = (24, 25, 30, 60)[(ri + ci) % 4]
            w, h = RES[res]
            gop = 2 * fps
            gopargs = {"h264": ["-g", str(gop), "-keyint_min", str(gop), "-sc_threshold", "0"],
                       "hevc": ["-x265-params", f"log-level=error:keyint={gop}:min-keyint={gop}:scenecut=0"],
                       "vp9": ["-g", str(gop), "-keyint_min", str(gop)],
                       "av1": ["-g", str(gop)]}[codec]
            args = [*CODEC_ARGS[codec]]
            if codec == "hevc":
                args = args[:-2]  # replaced by the GOP x265-params
            name = f"variants/v-{res}-{codec}-{fps}.mkv"
            variants[(res, codec)] = {"file": name, "fps": fps}
            vjobs.append({"file": name, "kind": "variant-reel", "codec": codec, "res": res, "fps": fps,
                          "args": ["-i", str(rp), "-map", "0:v:0", "-an",
                                   "-vf", f"fps={fps},scale={w}:{h}:flags=bicubic,format=yuv420p",
                                   *args, *gopargs, "{OUT}"]})
    print(f"variant reels: {len(vjobs)}", flush=True)
    build(store, vjobs, a.workers, a.threads)
    buckets = [0] * 600 + [1] * 300 + [2] * 90 + [3] * 10
    rng.shuffle(buckets)
    films = reel["films"]
    keys = list(variants)
    jobs, vfr_left, full_i = [], 24, 0
    for i, b in enumerate(buckets):
        res, codec = keys[rng.randrange(len(keys))]
        ext = rng.choice(CONTAINERS[codec])
        if b == 3:
            f = films[full_i % len(films)]
            full_i += 1
            start, dur = f["reel_start_s"], f["duration_s"]
        else:
            lo, hi = ((10, 180), (180, 1200), (1200, 3600))[b]
            dur = round(rng.uniform(lo, hi), 3)
            start = round(rng.uniform(0, reel["duration_s"] - dur), 3)
        file = f"items/s-{i:04d}-{res}-{codec}.{ext}"
        meta = {"kind": "video", "bucket": ["10s-3min", "3-20min", "20-60min", "full-length"][b], "res": res,
                "films": films_in(reel, start, dur), "expect": "READY"}
        if b == 0 and vfr_left:
            vfr_left -= 1
            w, h = RES[res]
            vf = f"scale={w}:{h}:flags=bicubic,setpts='(N/24+0.02*floor(N/5))/TB',format=yuv420p"
            extra = ["-fps_mode", "passthrough"]
            jobs.append(encode_job(file, rp, start, dur, vf, codec, extra, fps="vfr", vfr=True, **meta))
            continue
        v = variants[(res, codec)]
        out_extra = []
        if ext == "mp4":
            out_extra = ["-movflags", "+faststart"] + (["-tag:v", "hvc1"] if codec == "hevc" else [])
        jobs.append({"file": file, "codec": codec, "fps": v["fps"], "vfr": False, "reel_start_s": start, "seconds": dur,
                     "variant": v["file"], **meta,
                     "args": ["-ss", f"{start:.3f}", "-i", str(root / v["file"]), "-t", f"{dur:.3f}",
                              "-map", "0:v:0", "-c", "copy", *out_extra, "{OUT}"]})
    # 20 corrupt/truncated (detectable forms of design §3.2a, or unrecognizable headers) + 10 audio-only
    for j in range(20):
        codec, ext = (("h264", "mp4"), ("vp9", "webm"), ("hevc", "mkv"), ("av1", "webm"))[j % 4]
        res = ("360p", "480p", "720p")[j % 3]
        dur = round(rng.uniform(20, 90), 3)
        start = round(rng.uniform(0, reel["duration_s"] - dur), 3)
        v = variants[(res, codec)]
        out_extra = ["-movflags", "+faststart"] if ext == "mp4" else []
        name = f"corrupt/c-{j:02d}-{codec}.{ext}"
        if j < 12:
            frac = (1 / 3, 0.9)[j % 2]
            post, how = [_truncate(frac)], f"truncated to {frac:.3f} of its bytes"
        else:
            post, how = [_garble(a.seed + j)], "first 4096 bytes replaced by seeded random bytes"
        if j == 4:
            # OF-5 (2026-10-12): the faststart c-04-h264.mp4 cut to 1/3 decoded without an
            # explicit-indicators/v1 report (READY by design §3.2a), so it was no expected
            # failure. Replacement: the same window as a plain (moov-at-end) MP4 cut to 1/3,
            # an interrupted download whose sample index is lost.
            out_extra, name = [], "corrupt/c-04-h264-moovless.mp4"
            how = "truncated to 0.333 of its bytes (moov at end, index lost)"
        jobs.append({"file": name, "kind": "corrupt", "codec": codec, "corruption": how,
                     "reel_start_s": start, "seconds": dur, "films": films_in(reel, start, dur),
                     "expect": "FAILED:decode_error|unsupported_format", "post": post,
                     "args": ["-ss", f"{start:.3f}", "-i", str(root / v["file"]), "-t", f"{dur:.3f}",
                              "-map", "0:v:0", "-c", "copy", *out_extra, "{OUT}"]})
    audio = [("aac", "m4a", ["-c:a", "aac", "-b:a", "128k"]), ("libopus", "webm", ["-c:a", "libopus", "-b:a", "64k"]),
             ("libmp3lame", "mp3", ["-c:a", "libmp3lame", "-b:a", "128k"]), ("libvorbis", "ogg", ["-c:a", "libvorbis"])]
    for k in range(10):
        enc, ext, args = audio[k % len(audio)]
        dur = round(rng.uniform(5, 60), 1)
        jobs.append({"file": f"audio/a-{k:02d}.{ext}", "kind": "audio_only", "codec": enc, "seconds": dur, "films": [],
                     "expect": "FAILED:unsupported_format",
                     "args": ["-f", "lavfi", "-i", f"sine=frequency={220 + 55 * k}:duration={dur}", *args, "{OUT}"]})
    print(f"items: {len(jobs)}", flush=True)
    recs = build(store, jobs, a.workers, a.threads)
    print("verifying expected failures against the decoder", flush=True)
    if bad := verify_expected_failures(root, recs):
        sys.exit(f"SOAK-1000 not accepted: {len(bad)} expected-failure item(s) do not fail as labelled: {bad}")
    videos = [r for r in recs if r["kind"] == "video"]
    hours = sum(r["probe"].get("duration_s", 0) for r in videos) / 3600
    write_manifest(root, "SOAK-1000", ssrc, reel, recs, {
        "definition": "data-preparation §2.4: 1,000 videos (60 % 10 s–3 min, 30 % 3–20 min, 9 % 20–60 min, 1 % full "
                      "length; 240p–1080p; H.264/HEVC/VP9/AV1; 24/25/30/60 fps; 24 VFR; MP4/MKV/WebM) + 20 "
                      "corrupt/truncated + 10 audio-only. Non-VFR videos are keyframe-aligned stream copies of "
                      "20 seeded variant reels (one per resolution × codec, 2 s GOP)",
        "seed": a.seed, "video_hours": round(hours, 2),
        "expected_failures_verified": "every FAILED:* item was decoded by fingerprinter.media (probe + decode, "
                                      "dinov2-seg/2, explicit-indicators/v1) on this ffmpeg and failed as labelled "
                                      "(item 'verified'); corpus not accepted otherwise (OF-5)",
        "counts": {k: sum(r["kind"] == k for r in recs) for k in ("video", "corrupt", "audio_only")},
        "variants": [store.done[v["file"]] for v in variants.values()]})


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("step", choices=["reel", "c-prec", "l-pairs", "soak-1000", "verify-soak"])
    ap.add_argument("--workers", type=int, default=3)
    ap.add_argument("--threads", type=int, default=4)
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--positives", type=int, default=150)
    ap.add_argument("--synthetic", type=int, default=30)
    a = ap.parse_args()
    if a.seed is None:
        a.seed = {"reel": 0, "c-prec": 9, "l-pairs": 150, "soak-1000": 1000, "verify-soak": 1000}[a.step]
    {"reel": cmd_reel, "c-prec": cmd_cprec, "l-pairs": cmd_lpairs, "soak-1000": cmd_soak,
     "verify-soak": cmd_verify_soak}[a.step](a)


if __name__ == "__main__":
    main()
