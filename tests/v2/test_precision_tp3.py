"""TP-3 (G-3) comparison rule, operator decision OF-4 (benchmarks/p9-encoder/precision.py).

Decisions must be identical for every pair and alignment identical for every
pair classified as a match; a non-match's partial alignment is diagnostic only.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

_BENCH = Path(__file__).resolve().parents[2] / "benchmarks" / "p9-encoder"
sys.path.insert(0, str(_BENCH))  # precision.py imports its sibling ``common``
try:
    _SPEC = importlib.util.spec_from_file_location("p9_precision", _BENCH / "precision.py")
    precision = importlib.util.module_from_spec(_SPEC)
    _SPEC.loader.exec_module(precision)
finally:
    sys.path.remove(str(_BENCH))

# (matched, target_start, target_end, candidate_start, candidate_end, matched_segment_count)
MATCH = (True, 0.0, 60.0, 5.0, 65.0, 12)
NO_MATCH = (False, 10.0, 15.0, 30.0, 35.0, 1)


@pytest.mark.parametrize(("fp32", "fp16", "kind"), [
    (MATCH, MATCH, None),
    (NO_MATCH, NO_MATCH, None),
    (MATCH, (False, *MATCH[1:]), "decision"),                 # decision flip fails
    (NO_MATCH, (True, *NO_MATCH[1:]), "decision"),
    (MATCH, (True, 0.0, 65.0, 5.0, 70.0, 13), "matched_alignment"),  # matched pair: alignment must agree
    (NO_MATCH, (False, 10.0, 20.0, 30.0, 40.0, 2), "non_match_alignment"),  # lp-107 shape: diagnostic
])
def test_tp3_compare_of4(fp32, fp16, kind):
    assert precision.tp3_compare(fp32, fp16) == kind
