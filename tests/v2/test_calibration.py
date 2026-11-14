"""GPU batch calibration by measured throughput (OF-2, design §12.3) — unit level.

A fake embedder advances a fake clock by ``B / fps(B)`` per batch, so throughput
per batch size, noise, anomalies and out-of-memory limits are exact. The real
GPU path is covered in ``test_encoder_failures.py`` (``@gpu``).
"""

from __future__ import annotations

from collections.abc import Callable

import numpy as np
import pytest

from fingerprinter.calibration import METHOD, CalibrationError, Candidate, calibrate, select
from fingerprinter.model import OutOfMemory

X = np.zeros((8, 3, 224, 224), dtype=np.float32)


class FakeEmbedder:
    def __init__(self, fps: Callable[[int], float], *, fits: int = 10**9,
                 noise: Callable[[int, int], float] | None = None, oom_at_call: dict[int, int] | None = None):
        self.fps, self.fits, self.noise = fps, fits, noise
        self.oom_at_call = oom_at_call or {}
        self.now = 0.0
        self.calls: dict[int, int] = {}
        self.peak = 0

    def timer(self) -> float:
        return self.now

    def embed(self, batch: np.ndarray) -> np.ndarray:
        b = int(batch.shape[0])
        n = self.calls[b] = self.calls.get(b, 0) + 1
        if b > self.fits or self.oom_at_call.get(b) == n:
            raise OutOfMemory(f"fake OOM at B={b}")
        rate = self.fps(b) * (self.noise(b, n) if self.noise else 1.0)
        self.now += b / rate
        self.peak = max(self.peak, b * 1000)
        return np.zeros((b, 768), dtype=np.float32)

    def free(self) -> None:
        pass

    def vram(self) -> dict[str, int]:
        return {"max_reserved": self.peak}


def run(emb: FakeEmbedder, **kw):
    args = {"max_batch": 256, "frames": 64, "reps": 3, "tolerance": 0.05}
    args.update(kw)
    return calibrate(emb, X, timer=emb.timer, **args)


def rtx2050_like(b: int) -> float:
    """The measured shape on the RTX 2050 (validation §3.1): ≈41 frames/s for
    B ≤ 8, ≈27 for B ≥ 16 — a stand-in only; the test asserts the rule, not 8."""
    return {1: 30.0, 2: 37.0, 4: 40.0, 8: 41.0}.get(b, 27.0)


def test_highest_fitting_batch_is_not_automatically_selected():
    rep = run(FakeEmbedder(rtx2050_like, fits=64))
    assert rep["method"] == METHOD
    assert rep["b_max_fitting"] == 64
    assert rep["selected_batch"] != 64 and rep["selected_batch"] != 32  # neither B_max nor B_max/2
    assert rep["selected_batch"] in (4, 8)  # 4 is within 5 % of 8: the smaller wins


def test_smaller_batch_selected_when_measurably_faster():
    rep = run(FakeEmbedder(lambda b: 100.0 if b == 2 else 50.0, fits=128), tolerance=0.0)
    assert rep["selected_batch"] == 2
    assert rep["selected_frames_per_s"] == pytest.approx(100.0)
    assert "highest median" in rep["selection_reason"]


def test_monotone_device_selects_largest_when_it_is_really_fastest():
    rep = run(FakeEmbedder(lambda b: 10.0 * b, fits=32))
    assert rep["selected_batch"] == 32  # chosen by measurement, not by position


def test_selection_follows_measurements_not_a_hardcoded_size():
    rep = run(FakeEmbedder(lambda b: 60.0 if b == 16 else 20.0, fits=64), tolerance=0.0)
    assert rep["selected_batch"] == 16


def test_single_anomalous_repetition_does_not_decide():
    # B=32 is slow except one absurdly fast repetition
    def noise(b: int, n: int) -> float:
        return 50.0 if (b == 32 and n in (2, 3)) else 1.0  # both batches of rep 1

    rep = run(FakeEmbedder(rtx2050_like, fits=64, noise=noise))
    assert rep["selected_batch"] in (4, 8)
    c32 = next(c for c in rep["candidates"] if c["batch"] == 32)
    assert c32["max"] > 10 * c32["median"] and c32["spread"] > 1  # the anomaly is recorded


def test_lucky_first_round_is_rechecked_by_confirmation():
    # B=16 looks best in two of its first three repetitions (warm-up = call 1), then
    # shows its real speed in the confirmation round.
    def noise(b: int, n: int) -> float:
        return 2.0 if (b == 16 and n in (2, 3)) else 1.0

    emb = FakeEmbedder(rtx2050_like, fits=64, noise=noise)
    rep = run(emb, frames=16)
    c16 = next(c for c in rep["candidates"] if c["batch"] == 16)
    assert rep["first_round_median"]["16"] > rep["first_round_median"]["8"]
    assert len(c16["frames_per_s"]) == 6  # confirmation samples added
    assert rep["selected_batch"] in (4, 8)


def test_memory_ineligible_batches_are_rejected():
    emb = FakeEmbedder(lambda b: 10.0 * b, fits=16)
    rep = run(emb)
    by = {c["batch"]: c for c in rep["candidates"]}
    assert rep["b_max_fitting"] == 16 and rep["selected_batch"] == 16
    assert by[32]["eligible"] is False and by[32]["reason"] == "out_of_memory"
    assert all(by[b]["eligible"] is False for b in (64, 128, 256))
    assert 64 not in emb.calls and 128 not in emb.calls  # never tried past the first OOM
    assert by[16]["peak_vram_bytes"] > 0


def test_oom_during_a_timed_repetition_makes_the_batch_ineligible():
    # B=8 survives the warm-up (call 1) but OOMs on call 3 (inside timing)
    rep = run(FakeEmbedder(rtx2050_like, fits=64, oom_at_call={8: 3}))
    by = {c["batch"]: c for c in rep["candidates"]}
    assert by[8]["eligible"] is False and rep["selected_batch"] != 8
    assert rep["b_max_fitting"] == 4


def test_nothing_fits_is_a_calibration_error():
    with pytest.raises(CalibrationError, match="batch 1 does not fit"):
        run(FakeEmbedder(lambda b: 10.0, fits=0))


def test_cost_is_bounded():
    emb = FakeEmbedder(lambda b: 10.0, fits=10**9)
    run(emb, max_batch=256, frames=64, reps=3)
    # 9 candidates × (1 warm-up + 3 reps of max(64, B) frames) + 2 × 3 confirmation reps
    frames = sum(b * n for b, n in emb.calls.items())
    assert len(emb.calls) == 9 and frames <= 9 * 256 * 4 + 2 * 3 * 256


def test_select_rule_ties_go_to_the_smaller_batch():
    a, b = Candidate(8, samples=[40.0, 41.0, 40.5]), Candidate(16, samples=[41.0, 41.5, 41.2])
    chosen, reason = select([a, b], 0.05)
    assert chosen.batch == 8 and "within 5%" in reason
    chosen, _ = select([a, b], 0.0)
    assert chosen.batch == 16
    with pytest.raises(CalibrationError):
        select([Candidate(1, eligible=False)], 0.05)
