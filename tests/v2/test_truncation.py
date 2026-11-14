"""Truncated media (operator decision OF-1, design §3.2a/§7.4).

``dinov2-seg/2`` turns an explicit demuxer truncation report into
``decode_error`` even when ffmpeg exits 0; ``dinov2-seg/1`` keeps its approved
behaviour (prefix decoded, no error). Benign warnings, metadata text that looks
like an indicator, and non-truncation corruption warnings never fail a decode.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from fingerprinter.errors import EncodeError
from fingerprinter.media import decode, decode_command, probe, truncation_indicator
from fingerprinter.spec import dinov2_seg_v1, dinov2_seg_v2

V1, V2 = dinov2_seg_v1(), dinov2_seg_v2()


def _ff(args: list[str]) -> None:
    subprocess.run(["ffmpeg", "-nostdin", "-loglevel", "error", "-y", *args], check=True)  # noqa: S603


def _src(path: Path, codec: list[str], *extra: str) -> Path:
    _ff(["-f", "lavfi", "-i", "testsrc2=size=320x240:rate=25:duration=30", *codec, "-g", "50",
         *extra, str(path)])
    return path


SOURCES = {
    "mp4": lambda d: _src(d / "a.mp4", ["-c:v", "libx264", "-preset", "ultrafast"], "-movflags", "+faststart"),
    "mkv": lambda d: _src(d / "a.mkv", ["-c:v", "libx264", "-preset", "ultrafast"]),
    "webm": lambda d: _src(d / "a.webm", ["-c:v", "libvpx", "-b:v", "300k", "-deadline", "realtime"]),
}
INDICATOR = {"mp4": "mov_partial_file", "mkv": "matroska_file_ended_prematurely",
             "webm": "matroska_file_ended_prematurely"}


@pytest.fixture(scope="module")
def media(tmp_path_factory):
    d = tmp_path_factory.mktemp("trunc")
    return {k: make(d) for k, make in SOURCES.items()}


def _cut(src: Path, fraction: float) -> Path:
    data = src.read_bytes()
    out = src.with_name(f"cut{int(fraction * 100)}-{src.name}")
    out.write_bytes(data[: int(len(data) * fraction)])
    return out


def _ffmpeg(path: Path) -> tuple[int, list[str]]:
    p = subprocess.run(decode_command(path, V2), capture_output=True)  # noqa: S603
    return p.returncode, p.stderr.decode("utf-8", "replace").splitlines()


@pytest.mark.parametrize("fmt", ["mp4", "mkv", "webm"])
@pytest.mark.parametrize("fraction", [1 / 3, 0.9])
def test_truncated_media_is_decode_error_with_exit_code_zero(media, fmt, fraction):
    cut = _cut(media[fmt], fraction)
    probe(cut)  # the container still opens: this is not an unsupported format
    code, lines = _ffmpeg(cut)
    assert code == 0  # the case OF-1 is about: ffmpeg itself reports success
    assert INDICATOR[fmt] in {truncation_indicator(line) for line in lines}
    with pytest.raises(EncodeError) as e:
        list(decode(cut, V2))
    assert e.value.failure.value == "decode_error"
    assert not e.value.retryable and e.value.scope == "content"
    assert f"truncated_input: {INDICATOR[fmt]}" in str(e.value)


@pytest.mark.parametrize("fmt", ["mp4", "mkv", "webm"])
def test_spec_v1_behaviour_unchanged_prefix_decodes(media, fmt):
    cut = _cut(media[fmt], 1 / 3)
    frames = list(decode(cut, V1))  # approved dinov2-seg/1 semantics: no error
    assert 0 < len(frames) < 6  # a prefix of the 30 s (6 windows) source


@pytest.mark.parametrize("fmt", ["mp4", "mkv", "webm"])
def test_complete_media_identical_under_v1_and_v2(media, fmt):
    a = [(f.segment_index, f.pts, f.rgb.tobytes()) for f in decode(media[fmt], V1)]
    b = [(f.segment_index, f.pts, f.rgb.tobytes()) for f in decode(media[fmt], V2)]
    assert len(a) == 6 and a == b


def test_moov_at_end_mp4_truncated_stays_unsupported_format(tmp_path):
    src = _src(tmp_path / "late.mp4", ["-c:v", "libx264", "-preset", "ultrafast"])
    cut = _cut(src, 1 / 3)
    with pytest.raises(EncodeError) as e:
        probe(cut)
    assert e.value.failure.value == "unsupported_format"  # pre-existing path, unchanged


def test_benign_warning_deprecated_pixel_format_is_accepted(tmp_path):
    path = tmp_path / "j.mkv"
    _ff(["-f", "lavfi", "-i", "testsrc2=size=320x240:rate=10:duration=12", "-c:v", "mjpeg",
         "-pix_fmt", "yuvj420p", str(path)])
    code, lines = _ffmpeg(path)
    assert code == 0 and any("deprecated pixel format used" in line for line in lines)
    assert [f.segment_index for f in decode(path, V2)] == [0, 1, 2]


def test_indicator_text_in_metadata_is_not_truncation(tmp_path):
    path = tmp_path / "m.mkv"
    _ff(["-f", "lavfi", "-i", "testsrc2=size=320x240:rate=10:duration=12", "-c:v", "libx264",
         "-preset", "ultrafast", "-metadata", "title=File ended prematurely", str(path)])
    code, lines = _ffmpeg(path)
    assert code == 0 and any("File ended prematurely" in line for line in lines)  # printed as metadata
    assert [f.segment_index for f in decode(path, V2)] == [0, 1, 2]


def test_mid_stream_corruption_warnings_are_not_truncation(media, tmp_path):
    data = bytearray(media["mkv"].read_bytes())
    mid = len(data) // 2
    data[mid : mid + 2048] = bytes((b ^ 0x5A) for b in data[mid : mid + 2048])
    path = tmp_path / "damaged.mkv"
    path.write_bytes(bytes(data))
    code, lines = _ffmpeg(path)
    noise = [line for line in lines if "@ 0x" in line and "showinfo" not in line and "out#0" not in line]
    assert code == 0 and noise  # the decoder complained ...
    assert not any(truncation_indicator(line) for line in lines)
    assert len(list(decode(path, V2))) == 6  # ... but nothing reported a truncated input


def test_indicator_matching_is_anchored():
    assert truncation_indicator("[matroska,webm @ 0x5d07] File ended prematurely") == (
        "matroska_file_ended_prematurely")
    assert truncation_indicator(
        "[mov,mp4,m4a,3gp,3g2,mj2 @ 0x5cd5] stream 0, offset 0xf28d8: partial file") == "mov_partial_file"
    for benign in (
        "    title           : File ended prematurely",
        "[mov,mp4,m4a,3gp,3g2,mj2 @ 0x5c15] Packet corrupt (stream = 0, dts = 382464).",
        "[in#0/mov,mp4,m4a,3gp,3g2,mj2 @ 0x5c15] corrupt input packet in stream 0",
        "[h264 @ 0x5dd7] concealing 300 DC, 300 AC, 300 MV errors in I frame",
        "[swscaler @ 0x5fc8] deprecated pixel format used, make sure you did set range correctly",
    ):
        assert truncation_indicator(benign) is None, benign


@pytest.mark.parametrize("sig", ["SIGTERM", "SIGKILL"])
def test_decoder_killed_by_signal_is_retryable_not_decode_error(tmp_path, monkeypatch, sig):
    """Found by SOAK-1000 run 1: ffmpeg ended by a signal (service stop, OOM kill) said nothing
    about the media but became a permanent ``decode_error``."""
    import signal

    src = SOURCES["mkv"](tmp_path)
    procs: list[subprocess.Popen] = []
    real_popen = subprocess.Popen

    def popen(*a, **k):
        p = real_popen(*a, **k)
        procs.append(p)
        return p

    monkeypatch.setattr(subprocess, "Popen", popen)
    with pytest.raises(EncodeError) as e:
        for i, _frame in enumerate(decode(src, V2)):
            if i == 0:  # keep reading: ffmpeg acts on SIGTERM only once its pipe write returns
                procs[-1].send_signal(getattr(signal, sig))
    assert e.value.failure.value == "internal_error" and e.value.retryable
    assert e.value.scope == "attempt" and "signal" in e.value.detail


def test_complete_media_still_decodes_after_signal_check(tmp_path):
    """The signal check only runs on a failed read or a non-zero exit."""
    src = SOURCES["mkv"](tmp_path)
    assert len(list(decode(src, V2))) == 6
