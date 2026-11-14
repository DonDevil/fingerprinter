"""DINOv2 CLS embedding (design §3.2, §12.3–§12.5).

``facebook/dinov2-base`` at the pinned revision, ``AutoModel``, SDPA attention,
loaded from the local cache only (a missing model is a typed startup error).
Output = CLS token of ``last_hidden_state`` (after the final layer norm), cast
to float32 and L2-normalized in float32 on the host.

Failure classification: ``torch.cuda.OutOfMemoryError`` → ``OutOfMemory``
(the caller halves the batch or, at B = 1, raises ``SingleItemOOMError``);
any other CUDA/accelerator runtime error → ``DeviceError`` (class 4). No silent
switch to CPU ever happens here.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import numpy as np

from fingerprinter.errors import DeviceError, FingerprinterError
from fingerprinter.spec import MODEL_ID, MODEL_REVISION


class ModelUnavailableError(FingerprinterError):
    pass


class CudaUnavailableError(FingerprinterError):
    pass


class OutOfMemory(FingerprinterError):  # noqa: N818 - mirrors torch's name
    pass


def l2(v: np.ndarray) -> np.ndarray:
    v = v.astype(np.float32, copy=False)
    norms = np.linalg.norm(v, axis=-1, keepdims=True).astype(np.float32)
    norms[norms == 0] = np.float32(1.0)
    return (v / norms).astype(np.float32)


class Embedder:
    def __init__(
        self,
        device: str,
        compute_dtype: str,
        *,
        threads: int | None = None,
        cache_dir: Path | None = None,
        local_files_only: bool = True,
    ) -> None:
        import torch

        self.torch = torch
        if device == "cuda" and not torch.cuda.is_available():
            raise CudaUnavailableError("CUDA requested but torch.cuda.is_available() is False")
        if compute_dtype == "float16" and device != "cuda":
            raise FingerprinterError("float16 specs are GPU-only (design §3.7)")
        if threads and device == "cpu":
            torch.set_num_threads(threads)
        self.device = device
        self.compute_dtype = compute_dtype
        self.dtype = torch.float16 if compute_dtype == "float16" else torch.float32
        self.fault: Callable[[int], None] | None = None
        """Test hook: called with the batch size before each forward pass."""
        try:
            from transformers import AutoModel

            model = AutoModel.from_pretrained(
                MODEL_ID,
                revision=MODEL_REVISION,
                attn_implementation="sdpa",
                dtype=self.dtype,
                local_files_only=local_files_only,
                cache_dir=str(cache_dir) if cache_dir else None,
            )
        except OSError as exc:
            raise ModelUnavailableError(f"{MODEL_ID}@{MODEL_REVISION}: {exc}") from exc
        self.model = model.to(device).eval()

    def embed(self, batch: np.ndarray) -> np.ndarray:
        """``[B,3,224,224]`` float32 → ``[B,768]`` float32, L2-normalized."""
        torch = self.torch
        try:
            if self.fault is not None:
                self.fault(int(batch.shape[0]))
            with torch.inference_mode():
                x = torch.from_numpy(np.ascontiguousarray(batch)).to(self.device, dtype=self.dtype)
                out = self.model(pixel_values=x).last_hidden_state[:, 0]
                vec = out.float().cpu().numpy()
        except torch.cuda.OutOfMemoryError as exc:
            raise OutOfMemory(str(exc)[:200]) from exc
        except RuntimeError as exc:
            msg = str(exc)
            if "out of memory" in msg.lower():
                raise OutOfMemory(msg[:200]) from exc
            if self.device == "cuda" and ("CUDA" in msg or "cuda" in msg or "device" in msg):
                raise DeviceError(msg[:200]) from exc
            raise
        return l2(vec)

    def free(self) -> None:
        if self.device == "cuda":
            self.torch.cuda.empty_cache()

    def vram(self) -> dict[str, int]:
        if self.device != "cuda":
            return {}
        t = self.torch.cuda
        return {"allocated": int(t.memory_allocated()), "reserved": int(t.memory_reserved()),
                "max_allocated": int(t.max_memory_allocated()), "max_reserved": int(t.max_memory_reserved())}

    def reset_peak(self) -> None:
        if self.device == "cuda":
            self.torch.cuda.reset_peak_memory_stats()

    def sync(self) -> None:
        if self.device == "cuda":
            self.torch.cuda.synchronize()


def golden_inputs(n: int = 4) -> np.ndarray:
    """Fixed synthetic preprocessed inputs for the startup golden check (seeded PCG64)."""
    from fingerprinter.media import preprocess

    rng = np.random.default_rng(20261010)
    frames = []
    for i in range(n):
        base = rng.integers(0, 256, size=(240, 320, 3), dtype=np.uint8)
        ramp = np.linspace(0, 255, 320, dtype=np.float32)[None, :, None]
        img = ((base.astype(np.float32) * 0.5 + ramp * 0.5 + i * 13) % 256).astype(np.uint8)
        frames.append(preprocess(img))
    return np.stack(frames)
