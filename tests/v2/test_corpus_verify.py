"""SOAK-1000 builder: expected failures are verified against the real decoder (OF-5).

``make_real_corpora.verify_expected_failures`` must reject an item labelled as an
expected failure that the production decoder (``dinov2-seg/2``, explicit-indicators/v1)
accepts — the c-04 case of SOAK-1000 run 2 — and accept a genuinely failing one.
"""

from __future__ import annotations

import importlib.util
import subprocess
from pathlib import Path

_PATH = Path(__file__).resolve().parents[2] / "benchmarks" / "p9-encoder" / "make_real_corpora.py"
_SPEC = importlib.util.spec_from_file_location("p9_make_real_corpora", _PATH)
corpora = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(corpora)

EXPECT = "FAILED:decode_error|unsupported_format"


def _mkv(path: Path) -> Path:
    subprocess.run(["ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error", "-y", "-f", "lavfi", "-i",  # noqa: S607
                    "testsrc2=size=160x120:rate=10:duration=12", "-c:v", "libx264", "-g", "20", str(path)],
                   check=True)
    return path


def test_mislabelled_expected_failure_is_rejected_and_recorded(tmp_path):
    _mkv(tmp_path / "ok.mkv")  # decodes fine: READY, so not an expected failure
    recs = [{"file": "ok.mkv", "expect": EXPECT}]
    assert corpora.verify_expected_failures(tmp_path, recs) == ["ok.mkv"]
    assert recs[0]["verified"]["outcome"] == "READY" and recs[0]["verified"]["ok"] is False


def test_detectable_truncation_and_garbage_pass_ready_items_untouched(tmp_path):
    data = _mkv(tmp_path / "src.mkv").read_bytes()
    (tmp_path / "cut.mkv").write_bytes(data[: len(data) // 3])
    (tmp_path / "junk.mkv").write_bytes(b"\x00" * 4096 + data[4096:])
    recs = [{"file": "cut.mkv", "expect": EXPECT}, {"file": "junk.mkv", "expect": EXPECT},
            {"file": "src.mkv", "expect": "READY"}]
    assert corpora.verify_expected_failures(tmp_path, recs) == []
    assert recs[0]["verified"]["outcome"] == "decode_error"
    assert "truncated_input" in recs[0]["verified"]["detail"]
    assert recs[1]["verified"]["outcome"] == "unsupported_format"
    assert "verified" not in recs[2]
