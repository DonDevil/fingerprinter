"""Decode, timestamps and colour policy pinned by golden fixtures (TI-6, design §3.3).

Fixtures are generated with fixed ffmpeg commands into lossless FFV1/Matroska
(deterministic inputs). Golden values (selected segment indices, real PTS and
the SHA-256 of every selected frame's RGB) live in ``golden/decode.json``;
``UPDATE_GOLDEN=1`` rewrites them (only when the runtime profile changes, which
needs the runtime-compatibility gate, design §3.5).
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
from pathlib import Path

import pytest

from fingerprinter.errors import EncodeError
from fingerprinter.media import decode, probe
from fingerprinter.spec import dinov2_seg_v1

GOLDEN = Path(__file__).parent / "golden" / "decode.json"
SPEC = dinov2_seg_v1()


def _ff(args: list[str]) -> None:
    subprocess.run(["ffmpeg", "-nostdin", "-loglevel", "error", "-y", *args], check=True)  # noqa: S603


def _timed(path: Path, times_ms: list[int], size: str = "64x48") -> Path:
    """Frames at exact millisecond PTS (time base 1/1000)."""
    expr = "+".join(f"eq(N,{i})*{t}" for i, t in enumerate(times_ms))
    _ff(["-f", "lavfi", "-i", f"testsrc2=size={size}:rate=1:duration={len(times_ms)}",
         "-vf", f"settb=1/1000,setpts='{expr}'", "-fps_mode", "passthrough",
         "-enc_time_base", "1/1000", "-c:v", "ffv1", "-bitexact", str(path)])
    return path


def _colour(path: Path, size: str, *, pix: str = "yuv420p", tags: list[str] | None = None) -> Path:
    _ff(["-f", "lavfi", "-i", f"testsrc2=size={size}:rate=2:duration=6", "-pix_fmt", pix,
         "-c:v", "ffv1", *(tags or []), "-bitexact", str(path)])
    return path


def _rotated(path: Path) -> Path:
    src = path.with_suffix(".src.mp4")
    _ff(["-f", "lavfi", "-i", "testsrc2=size=64x48:rate=2:duration=6", "-c:v", "libx264",
         "-preset", "ultrafast", "-qp", "0", "-threads", "1", "-pix_fmt", "yuv444p", "-bitexact", str(src)])
    _ff(["-display_rotation", "90", "-i", str(src), "-c", "copy", str(path)])
    return path


CASES = {
    "boundaries": lambda d: _timed(d / "b.mkv", [0, 4999, 5000, 5001, 9999, 10000]),
    "vfr_holes": lambda d: _timed(d / "h.mkv", [0, 1000, 2000, 12500, 13000, 30000]),
    "late_start": lambda d: _timed(d / "s.mkv", [1000, 5999, 6000, 6500]),
    "untagged_sd": lambda d: _colour(d / "usd.mkv", "640x480"),
    "untagged_hd": lambda d: _colour(d / "uhd.mkv", "1280x720"),
    "bt601_limited": lambda d: _colour(d / "601.mkv", "640x480", tags=[
        "-colorspace", "smpte170m", "-color_primaries", "smpte170m", "-color_trc", "smpte170m",
        "-color_range", "tv"]),
    "bt709_limited": lambda d: _colour(d / "709.mkv", "1280x720", tags=[
        "-colorspace", "bt709", "-color_primaries", "bt709", "-color_trc", "bt709", "-color_range", "tv"]),
    "full_range": lambda d: _colour(d / "full.mkv", "640x480", pix="yuvj420p", tags=["-color_range", "pc"]),
    "ten_bit": lambda d: _colour(d / "10b.mkv", "640x480", pix="yuv420p10le"),
    "hdr_pq": lambda d: _colour(d / "pq.mkv", "640x480", tags=[
        "-colorspace", "bt2020nc", "-color_primaries", "bt2020", "-color_trc", "smpte2084"]),
    "bt2020_sdr": lambda d: _colour(d / "2020.mkv", "640x480", tags=[
        "-colorspace", "bt2020nc", "-color_primaries", "bt2020", "-color_trc", "bt709"]),
    "rotated_90": lambda d: _rotated(d / "rot.mp4"),
}


def _observe(path: Path) -> dict:
    frames = list(decode(path, SPEC))
    return {
        "segment_index": [f.segment_index for f in frames],
        "pts": [f.pts for f in frames],
        "time_base": list(frames[0].time_base),
        "shape": list(frames[0].rgb.shape),
        "rgb_sha256": [hashlib.sha256(f.rgb.tobytes()).hexdigest() for f in frames],
    }


@pytest.fixture(scope="module")
def observed(tmp_path_factory):
    d = tmp_path_factory.mktemp("golden")
    out = {name: _observe(make(d)) for name, make in CASES.items()}
    if os.environ.get("UPDATE_GOLDEN") == "1":
        GOLDEN.parent.mkdir(exist_ok=True)
        GOLDEN.write_text(json.dumps(out, indent=1, sort_keys=True) + "\n")
    return out


def test_golden_fixtures_bitwise(observed):
    golden = json.loads(GOLDEN.read_text())
    assert set(golden) == set(observed)
    for name in golden:
        assert observed[name] == golden[name], name


def test_window_boundaries_on_and_one_tick_around(observed):
    b = observed["boundaries"]
    assert b["segment_index"] == [0, 1, 2]
    assert b["pts"] == [0, 5000, 10000]  # 4999 and 9999 stay in the earlier window
    assert b["time_base"] == [1, 1000]


def test_vfr_holes_stay_absent(observed):
    h = observed["vfr_holes"]
    assert h["segment_index"] == [0, 2, 6]  # windows 1, 3, 4, 5 are absent, not filled
    assert h["pts"] == [0, 12500, 30000]


def test_origin_is_first_decoded_pts(observed):
    s = observed["late_start"]
    assert s["segment_index"] == [0, 1] and s["pts"] == [1000, 6000]


def test_colour_policy_tags_else_default_and_no_tone_mapping(observed):
    o = observed
    # tags are applied: the same pixels tagged BT.709 decode differently from untagged HD
    assert o["bt709_limited"]["rgb_sha256"] != o["untagged_hd"]["rgb_sha256"]
    # untagged SD uses swscale's default (BT.601) = explicitly tagged BT.601
    assert o["bt601_limited"]["rgb_sha256"] == o["untagged_sd"]["rgb_sha256"]
    # untagged HD is NOT forced to BT.709 (default interpretation, design §3.3)
    assert o["untagged_hd"]["shape"] == [720, 1280, 3]
    # transfer characteristics are ignored: PQ converts exactly like the SDR transfer
    assert o["hdr_pq"]["rgb_sha256"] == o["bt2020_sdr"]["rgb_sha256"]
    assert o["full_range"]["rgb_sha256"] != o["untagged_sd"]["rgb_sha256"]
    assert o["ten_bit"]["shape"] == [480, 640, 3]


def test_autorotate(observed):
    assert observed["rotated_90"]["shape"] == [64, 48, 3]  # 64x48 source displayed rotated


def test_no_video_stream_and_garbage(tmp_path):
    audio = tmp_path / "a.m4a"
    _ff(["-f", "lavfi", "-i", "sine=duration=2", "-c:a", "aac", str(audio)])
    with pytest.raises(EncodeError) as e:
        probe(audio)
    assert e.value.failure.value == "unsupported_format" and not e.value.retryable
    junk = tmp_path / "j.mp4"
    junk.write_bytes(b"\x00" * 4096)
    with pytest.raises(EncodeError) as e:
        probe(junk)
    assert e.value.failure.value == "unsupported_format"


def test_truncated_file_decode_error_or_partial(tmp_path):
    src = _colour(tmp_path / "t.mkv", "64x48")
    data = src.read_bytes()
    cut = tmp_path / "cut.mkv"
    cut.write_bytes(data[:600])
    with pytest.raises(EncodeError) as e:
        list(decode(cut, SPEC))
    assert e.value.failure.value == "decode_error"
