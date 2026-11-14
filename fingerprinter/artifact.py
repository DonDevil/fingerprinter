"""Artifact format ``fp-seg-npz/v1`` (design §3.8).

An uncompressed zip (NumPy ``.npz`` layout) with ``segment_index`` int32 [N],
``pts`` int64 [N], ``time_base`` int64 [2], ``pts_s``/``window_start_s``/
``window_end_s`` float64 [N], ``vectors`` float32 [N,768] (L2-normalized),
``coarse`` float32 [768] (mean then L2), plus ``manifest.json``. Member order
and zip timestamps are fixed, so equal content gives equal bytes. ``np.load``
reads the arrays; the manifest is read with ``zipfile``.
"""

from __future__ import annotations

import hashlib
import io
import json
import zipfile
from dataclasses import dataclass
from typing import Any

import numpy as np

from fingerprinter.model import l2

_EPOCH = (1980, 1, 1, 0, 0, 0)


@dataclass
class Segments:
    segment_index: np.ndarray
    pts: np.ndarray
    time_base: tuple[int, int]
    pts_s: np.ndarray
    vectors: np.ndarray
    segment_duration_s: float

    @property
    def coarse(self) -> np.ndarray:
        return l2(self.vectors.astype(np.float32).mean(axis=0))


def build(seg: Segments, manifest: dict[str, Any]) -> tuple[bytes, str]:
    idx = seg.segment_index.astype(np.int32)
    arrays = {
        "segment_index": idx,
        "pts": seg.pts.astype(np.int64),
        "time_base": np.array(seg.time_base, dtype=np.int64),
        "pts_s": seg.pts_s.astype(np.float64),
        "window_start_s": idx.astype(np.float64) * seg.segment_duration_s,
        "window_end_s": (idx.astype(np.float64) + 1) * seg.segment_duration_s,
        "vectors": seg.vectors.astype(np.float32),
        "coarse": seg.coarse,
    }
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_STORED) as zf:
        for name, arr in arrays.items():
            member = io.BytesIO()
            np.lib.format.write_array(member, np.ascontiguousarray(arr), allow_pickle=False)
            zf.writestr(zipfile.ZipInfo(f"{name}.npy", date_time=_EPOCH), member.getvalue())
        zf.writestr(
            zipfile.ZipInfo("manifest.json", date_time=_EPOCH),
            json.dumps(manifest, sort_keys=True, default=str).encode(),
        )
    data = buf.getvalue()
    return data, hashlib.sha256(data).hexdigest()


def load(data: bytes) -> tuple[dict[str, np.ndarray], dict[str, Any]]:
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        manifest = json.loads(zf.read("manifest.json"))
        arrays = {
            n[: -len(".npy")]: np.lib.format.read_array(io.BytesIO(zf.read(n)), allow_pickle=False)
            for n in zf.namelist()
            if n.endswith(".npy")
        }
    return arrays, manifest
