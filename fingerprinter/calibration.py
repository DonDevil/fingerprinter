"""GPU batch calibration by measured throughput (design §12.3, operator decision OF-2).

Method ``throughput/v1``:

1. **Eligibility (memory).** Candidates are B = 1, 2, 4, … ≤ ``max_batch``. Each
   candidate is first run once on the device (warm-up, discarded); an
   ``OutOfMemory`` makes it and every larger B **ineligible** (no larger batch is
   tried). An OOM during a timed repetition also makes that B ineligible.
2. **Measurement.** Each eligible B embeds ``max(frames, B)`` frames (whole
   batches) ``reps`` times; frames/s per repetition with the device synchronized.
   The same fixed synthetic preprocessed inputs are used for every B.
3. **Confirmation.** The two candidates with the highest median are measured
   ``reps`` more times; their decision uses all ``2·reps`` samples. A single
   anomalous repetition therefore cannot decide (median), and a lucky first round
   is re-checked.
4. **Selection.** The highest median wins; candidates whose median is within
   ``tolerance`` of it are treated as equal and the **smallest** of them is
   chosen (less VRAM, more OOM headroom, lower latency). The largest fitting B is
   never chosen merely because it fits.

Every candidate's samples, median, min/max and relative spread, its peak VRAM,
the selection and the reason are returned as a JSON-able report, which the
encoder stores in ``encoder_hosts.calibration`` together with the runtime and
hardware profile. Cost is bounded: at most ``log2(max_batch)+1`` candidates ×
(1 warm-up + ``reps``) passes, plus ``2·reps`` confirmation passes.
"""

from __future__ import annotations

import statistics
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Protocol

import numpy as np

from fingerprinter.errors import FingerprinterError
from fingerprinter.model import OutOfMemory

METHOD = "throughput/v1"


class CalibrationError(FingerprinterError):
    """No batch size can run on the device (B = 1 does not fit)."""


class _Embedder(Protocol):
    def embed(self, batch: np.ndarray) -> np.ndarray: ...

    def free(self) -> None: ...

    def vram(self) -> dict[str, int]: ...


@dataclass
class Candidate:
    batch: int
    eligible: bool = True
    reason: str | None = None
    samples: list[float] = field(default_factory=list)
    peak_vram: int | None = None

    @property
    def median(self) -> float:
        return statistics.median(self.samples) if self.samples else 0.0

    def to_json(self) -> dict[str, Any]:
        out: dict[str, Any] = {"batch": self.batch, "eligible": self.eligible}
        if self.reason:
            out["reason"] = self.reason
        if self.samples:
            med = self.median
            out.update({
                "frames_per_s": [round(s, 3) for s in self.samples],
                "median": round(med, 3),
                "min": round(min(self.samples), 3),
                "max": round(max(self.samples), 3),
                "spread": round((max(self.samples) - min(self.samples)) / med, 4) if med else None,
            })
        if self.peak_vram is not None:
            out["peak_vram_bytes"] = self.peak_vram
        return out


def select(candidates: list[Candidate], tolerance: float) -> tuple[Candidate, str]:
    """The selection rule (step 4) over measured, eligible candidates."""
    measured = [c for c in candidates if c.eligible and c.samples]
    if not measured:
        raise CalibrationError("no eligible batch size")
    best = max(c.median for c in measured)
    near = sorted((c for c in measured if c.median >= (1.0 - tolerance) * best), key=lambda c: c.batch)
    chosen = near[0]
    top = max(measured, key=lambda c: (c.median, -c.batch))
    if chosen is top:
        reason = "highest median throughput"
    else:
        reason = (f"smallest batch within {tolerance:.0%} of the highest median "
                  f"(B={top.batch}, {top.median:.2f} frames/s)")
    return chosen, reason


def calibrate(
    emb: _Embedder,
    inputs: np.ndarray,
    *,
    max_batch: int,
    frames: int,
    reps: int,
    tolerance: float,
    sync: Callable[[], None] = lambda: None,
    timer: Callable[[], float] = time.perf_counter,
    reset_peak: Callable[[], None] = lambda: None,
) -> dict[str, Any]:
    """Run ``throughput/v1``; ``inputs`` is a ``[N,3,224,224]`` pool tiled as needed."""
    candidates: list[Candidate] = []
    b = 1
    while b <= max_batch:
        c = Candidate(b)
        candidates.append(c)
        n = max(frames, b)
        n -= n % b
        x = _tile(inputs, n)
        try:
            reset_peak()
            emb.embed(x[:b])  # warm-up and memory-fit probe (discarded)
            sync()
            c.samples = [_timed(emb, x, b, sync, timer) for _ in range(reps)]
            c.peak_vram = emb.vram().get("max_reserved") or emb.vram().get("max_allocated")
        except OutOfMemory:
            emb.free()
            c.eligible, c.reason, c.samples = False, "out_of_memory", []
            break
        emb.free()
        b *= 2
    if b <= max_batch:  # stopped by an OOM: the larger ones are ineligible without trying
        b *= 2
        while b <= max_batch:
            candidates.append(Candidate(b, eligible=False, reason="larger than an out-of-memory batch"))
            b *= 2
    eligible = [c for c in candidates if c.eligible]
    if not eligible:
        raise CalibrationError("batch 1 does not fit")
    first_round = {c.batch: c.median for c in eligible}
    for c in sorted(eligible, key=lambda c: (-c.median, c.batch))[:2]:
        n = max(frames, c.batch)
        n -= n % c.batch
        try:
            c.samples += [_timed(emb, _tile(inputs, n), c.batch, sync, timer) for _ in range(reps)]
        except OutOfMemory:
            emb.free()
            c.eligible, c.reason = False, "out_of_memory during confirmation"
    emb.free()
    chosen, reason = select(candidates, tolerance)
    return {
        "method": METHOD,
        "selected_batch": chosen.batch,
        "selected_frames_per_s": round(chosen.median, 3),
        "selection_reason": reason,
        "b_max_fitting": max(c.batch for c in candidates if c.eligible),
        "first_round_median": {str(k): round(v, 3) for k, v in first_round.items()},
        "candidates": [c.to_json() for c in candidates],
        "parameters": {"max_batch": max_batch, "frames_per_rep": frames, "reps": reps,
                       "confirmation_reps": reps, "tolerance": tolerance},
    }


def _timed(emb: _Embedder, x: np.ndarray, b: int, sync: Callable[[], None],
           timer: Callable[[], float]) -> float:
    t0 = timer()
    for i in range(0, len(x), b):
        emb.embed(x[i : i + b])
    sync()
    dt = timer() - t0
    return len(x) / dt if dt > 0 else float("inf")


def _tile(inputs: np.ndarray, n: int) -> np.ndarray:
    reps = -(-n // len(inputs))
    return np.concatenate([inputs] * reps)[:n] if reps > 1 else inputs[:n]
