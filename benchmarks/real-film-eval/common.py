"""Shared paths, config fingerprinting and stage-completion markers for the real-film evaluation."""
from __future__ import annotations

import hashlib
import json
import os
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "local-data" / "real-film-evaluation"
SRC_DIR = Path("/media/dhanush/Media/Movies-Downloaded")
EXTS = {".mp4", ".mkv", ".avi", ".mov", ".webm", ".m4v"}


def d(*p: str) -> Path:
    path = DATA.joinpath(*p)
    path.mkdir(parents=True, exist_ok=True) if not path.suffix else path.parent.mkdir(parents=True, exist_ok=True)
    return path


def jdump(obj, path: Path) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=1, sort_keys=True, default=str))
    os.replace(tmp, path)


def jload(path: Path):
    return json.loads(path.read_text())


def cfg_fp(obj) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True, default=str).encode()).hexdigest()[:16]


def sha256_file(path: Path, bufsize: int = 8 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while b := f.read(bufsize):
            h.update(b)
    return h.hexdigest()


def stage_done(name: str, fp: str) -> bool:
    m = d("metrics", f"stage-{name}.done.json")
    return m.exists() and jload(m).get("cfg") == fp


def mark_done(name: str, fp: str, extra: dict | None = None) -> None:
    jdump({"cfg": fp, "t_end": time.time(), **(extra or {})}, d("metrics", f"stage-{name}.done.json"))


def wait_until_cool(max_wait_s: float = 1800, poll: float = 5.0) -> float:
    """Block (bounded) while the monitor's thermal state says hot. Returns seconds waited; raises if it never cools."""
    st = DATA / "metrics" / "thermal_state.json"
    waited = 0.0
    while waited <= max_wait_s:
        try:
            s = json.loads(st.read_text())
            if time.time() - s.get("t_wall", 0) > 30:  # stale monitor: fail safe = wait
                ok = False
            else:
                ok = s.get("ok", True)
        except (OSError, ValueError):
            ok = False if st.exists() else True
        if ok:
            return waited
        time.sleep(poll)
        waited += poll
    raise RuntimeError("thermal guard: machine did not cool within bound; stopping workload")
