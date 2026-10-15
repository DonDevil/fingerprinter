"""Runtime profiles and the encoder's actual runtime fingerprint (design §3.5, P9-D26).

A profile has **common** components (ffmpeg build, Python, torch, transformers,
numpy, Pillow, model file hashes) and **device variants** (``cpu``: nothing
more; ``cuda``: CUDA runtime and cuDNN as reported by torch). An encoder matches
a profile iff every common component is equal **and** its device's variant is
listed, equal and admitted. The NVIDIA driver is provenance only.
"""

from __future__ import annotations

import hashlib
import json
import platform
import re
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from fingerprinter.spec import MODEL_ID, MODEL_REVISION, canonical_json


def _run(args: list[str]) -> str:
    return subprocess.run(  # noqa: S603 - fixed argument lists
        args, capture_output=True, text=True, check=True, timeout=30
    ).stdout


def ffmpeg_components() -> dict[str, str]:
    version = _run(["ffmpeg", "-hide_banner", "-version"])
    first = version.splitlines()[0].strip()
    libs = {
        m.group(1): m.group(2).replace(" ", "")
        for m in re.finditer(r"^(lib\w+)\s+([\d. ]+?)\s*/", version, re.MULTILINE)
    }
    buildconf = _run(["ffmpeg", "-hide_banner", "-buildconf"])
    return {
        "ffmpeg": first,
        **{k: v for k, v in sorted(libs.items())},
        "ffmpeg_buildconf_sha256": hashlib.sha256(buildconf.encode()).hexdigest(),
    }


def model_snapshot_dir(cache_dir: Path | None = None) -> Path | None:
    base = cache_dir or Path.home() / ".cache" / "huggingface" / "hub"
    snap = base / ("models--" + MODEL_ID.replace("/", "--")) / "snapshots" / MODEL_REVISION
    return snap if snap.is_dir() else None


def model_file_hashes(cache_dir: Path | None = None) -> dict[str, str]:
    snap = model_snapshot_dir(cache_dir)
    if snap is None:
        return {}
    out: dict[str, str] = {}
    for path in sorted(snap.iterdir()):
        if path.is_file() or path.is_symlink():
            sha = hashlib.sha256()
            with path.resolve().open("rb") as handle:
                for chunk in iter(lambda: handle.read(1 << 20), b""):
                    sha.update(chunk)
            out[path.name] = sha.hexdigest()
    return out


def common_components(cache_dir: Path | None = None) -> dict[str, Any]:
    import numpy
    import PIL
    import torch
    import transformers

    return {
        **ffmpeg_components(),
        "python": platform.python_version(),
        "torch": torch.__version__,
        "transformers": transformers.__version__,
        "numpy": numpy.__version__,
        "pillow": PIL.__version__,
        "model_files": model_file_hashes(cache_dir),
    }


def cuda_variant() -> dict[str, Any] | None:
    import torch

    if torch.version.cuda is None:
        return None
    return {"cuda_runtime": torch.version.cuda, "cudnn": str(torch.backends.cudnn.version())}


def driver_provenance() -> dict[str, str]:
    try:
        out = _run(
            ["nvidia-smi", "--query-gpu=name,driver_version,memory.total", "--format=csv,noheader"]
        ).strip()
    except (OSError, subprocess.SubprocessError) as exc:
        return {"nvidia_smi": f"unavailable: {type(exc).__name__}"}
    return {"nvidia_smi": out}


@dataclass(frozen=True)
class RuntimeFingerprint:
    common: dict[str, Any]
    device: str
    variant: dict[str, Any]
    provenance: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def capture(cls, device: str, cache_dir: Path | None = None) -> RuntimeFingerprint:
        if device not in ("cpu", "cuda"):
            raise ValueError(f"unknown device {device!r}")
        variant = cuda_variant() if device == "cuda" else {}
        prov = driver_provenance() if device == "cuda" else {}
        return cls(common_components(cache_dir), device, variant or {}, prov)

    def to_json(self) -> str:
        return canonical_json(
            {
                "common": self.common,
                "device": self.device,
                "variant": self.variant,
                "provenance": self.provenance,
            }
        ).decode()

    @property
    def common_digest(self) -> str:
        return hashlib.sha256(canonical_json(self.common)).hexdigest()


@dataclass(frozen=True)
class RuntimeProfile:
    name: str
    common: dict[str, Any]
    variants: dict[str, dict[str, Any]]

    def document(self) -> str:
        return canonical_json(
            {"profile": self.name, "common": self.common, "variants": self.variants}
        ).decode()

    @classmethod
    def from_document(cls, text: str) -> RuntimeProfile:
        obj = json.loads(text)
        return cls(obj["profile"], obj["common"], obj["variants"])

    @classmethod
    def from_fingerprints(cls, name: str, *prints: RuntimeFingerprint) -> RuntimeProfile:
        commons = {canonical_json(p.common) for p in prints}
        if len(commons) != 1:
            raise ValueError("fingerprints differ in common components; not one profile")
        return cls(name, prints[0].common, {p.device: p.variant for p in prints})

    def mismatch(self, fp: RuntimeFingerprint, admitted: set[str]) -> str | None:
        """None if ``fp`` may produce under this profile; else the reason."""
        for key in sorted(set(self.common) | set(fp.common)):
            if self.common.get(key) != fp.common.get(key):
                return f"common component {key!r} differs"
        if fp.device not in self.variants:
            return f"device variant {fp.device!r} not in profile {self.name}"
        if self.variants[fp.device] != fp.variant:
            return f"device variant {fp.device!r} components differ"
        if fp.device not in admitted:
            return f"device variant {fp.device!r} not admitted for profile {self.name}"
        return None
