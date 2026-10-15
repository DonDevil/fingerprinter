"""Decode, sampling with real PTS, colour policy and ``preprocess/v1`` (design §3.3–§3.4).

Decode: one sequential full decode of the first video stream (no seek),
``-fflags +genpts -copyts -fps_mode passthrough``, autorotate on, YUV→RGB by
swscale with ``sws_flags=bicubic+accurate_rnd+full_chroma_int``; input matrix
and range from the stream's tags, untagged streams get swscale's default as
built in the pinned ``rt-1`` ffmpeg; **no tone mapping** (HDR is converted as-is).
Frames come through a pipe as ``rawvideo rgb24`` (no PNG files).

Sampling: segment ``k`` covers ``[k·5, (k+1)·5)`` seconds relative to the first
decoded frame's PTS; the representative frame is the first decoded frame whose
window index exceeds that of the previously selected one (ffmpeg ``select``).
``showinfo`` reports each selected frame's integer ``pts`` and the input
``time_base``; ``k`` is derived from that real PTS in the same double arithmetic
ffmpeg used. Absent windows stay absent; non-monotonic PTS → ``decode_error``.

Truncation (operator decision OF-1, design §7.4): for a spec with
``decode.truncation = "explicit-indicators/v1"`` (``dinov2-seg/2``) a decode that
ffmpeg finishes with exit 0 is still a ``decode_error`` when the demuxer printed
one of ``TRUNCATION_INDICATORS`` — explicit end-of-input reports, matched as whole
demuxer log lines (``[<demuxer> @ 0x…] …``), never metadata text. Other warnings
(corrupt packets, concealed macroblocks, deprecated pixel formats, …) are not
truncation evidence and do not fail a decode. Truncation without such a report
(a cut on an element/packet boundary, MPEG-TS, FLV, a short last MP4 sample) is
not detectable this way and is accepted as before. ``dinov2-seg/1`` ignores the
indicators (its behaviour and identity are unchanged).
"""

from __future__ import annotations

import json
import math
import queue
import re
import subprocess
import threading
from collections.abc import Iterator
from dataclasses import dataclass
from fractions import Fraction
from pathlib import Path
from typing import Any

import numpy as np
from antipiracy_contracts.events.fingerprinting import EncodeFailure as F
from PIL import Image

from fingerprinter.errors import EncodeError
from fingerprinter.spec import Spec

_TB = re.compile(r"config in time_base: (\d+)/(\d+)")
_FRAME = re.compile(r"\bn:\s*(\d+)\s+pts:\s*(-?\d+)\s+pts_time:\S+.*?\bs:(\d+)x(\d+)")
MEAN = np.array([0.485, 0.456, 0.406], dtype=np.float32).reshape(3, 1, 1)
STD = np.array([0.229, 0.224, 0.225], dtype=np.float32).reshape(3, 1, 1)
RESCALE = np.float32(1.0 / 255.0)
TRUNCATION_INDICATORS: tuple[tuple[str, re.Pattern[str]], ...] = (
    # libavformat matroskadec (MKV/WebM): EOF inside an element
    ("matroska_file_ended_prematurely",
     re.compile(r"^\[matroska,webm @ 0x[0-9a-f]+\] File ended prematurely")),
    # libavformat mov (MP4/MOV/3GP): a sample's bytes lie beyond the end of the input
    ("mov_partial_file",
     re.compile(r"^\[mov,mp4,m4a,3gp,3g2,mj2 @ 0x[0-9a-f]+\] stream \d+, offset 0x[0-9a-f]+: partial file")),
)


def truncation_indicator(line: str) -> str | None:
    """The name of the explicit truncation indicator in one ffmpeg log line, if any."""
    for name, pattern in TRUNCATION_INDICATORS:
        if pattern.match(line):
            return name
    return None


@dataclass(frozen=True)
class Probe:
    width: int
    height: int
    codec: str
    time_base: str
    duration_s: float | None
    color_space: str | None
    color_range: str | None
    color_transfer: str | None
    pix_fmt: str | None
    rotation: int | None

    def to_json(self) -> dict[str, Any]:
        return self.__dict__.copy()


@dataclass(frozen=True)
class Frame:
    segment_index: int
    pts: int
    time_base: tuple[int, int]
    pts_s: float
    rgb: np.ndarray  # H x W x 3 uint8


def probe(path: Path) -> Probe:
    try:
        out = subprocess.run(  # noqa: S603
            [
                "ffprobe", "-v", "error", "-show_streams", "-show_format", "-of", "json", str(path),
            ],
            capture_output=True,
            text=True,
            timeout=120,
        )
    except subprocess.TimeoutExpired as exc:
        raise EncodeError(F.DECODE_ERROR, "ffprobe timeout", retryable=False, scope="content") from exc
    if out.returncode < 0:  # killed by a signal: says nothing about the media
        raise EncodeError(F.INTERNAL_ERROR, f"ffprobe killed by signal {-out.returncode}", retryable=True)
    if out.returncode != 0:
        raise EncodeError(F.UNSUPPORTED_FORMAT, "ffprobe: unrecognized container", retryable=False, scope="content")
    data = json.loads(out.stdout or "{}")
    videos = [s for s in data.get("streams", []) if s.get("codec_type") == "video"
              and not (s.get("disposition") or {}).get("attached_pic")]
    if not videos:
        raise EncodeError(F.UNSUPPORTED_FORMAT, "no video stream", retryable=False, scope="content")
    v = videos[0]
    rotation = None
    for sd in v.get("side_data_list", []) or []:
        if "rotation" in sd:
            rotation = int(sd["rotation"])
    dur = v.get("duration") or (data.get("format") or {}).get("duration")
    return Probe(
        width=int(v.get("width") or 0),
        height=int(v.get("height") or 0),
        codec=str(v.get("codec_name")),
        time_base=str(v.get("time_base")),
        duration_s=float(dur) if dur not in (None, "N/A") else None,
        color_space=v.get("color_space"),
        color_range=v.get("color_range"),
        color_transfer=v.get("color_transfer"),
        pix_fmt=v.get("pix_fmt"),
        rotation=rotation,
    )


def decode_command(path: Path, spec: Spec) -> list[str]:
    d = spec.document.decode
    seg = spec.document.sampling.segment_duration_s
    expr = (
        f"isnan(prev_selected_t)+gte(floor((t-start_t)/{seg!r}),"
        f"floor((prev_selected_t-start_t)/{seg!r})+1)"
    )
    cmd = ["ffmpeg", "-nostdin", "-hide_banner", "-nostats", "-loglevel", "info"]
    if d.genpts:
        cmd += ["-fflags", "+genpts"]
    if d.copyts:
        cmd += ["-copyts"]
    if not d.autorotate:
        cmd += ["-noautorotate"]
    cmd += [
        "-i", str(path),
        "-map", "0:v:0",
        "-vf", f"select='{expr}',showinfo",
        "-fps_mode", d.fps_mode,
        "-sws_flags", d.sws_flags,
        "-f", "rawvideo",
        "-pix_fmt", d.pix_fmt,
        "pipe:1",
    ]
    return cmd


def segment_index(pts: int, pts0: int, tb: tuple[int, int], seg: float) -> int:
    # The same double arithmetic as ffmpeg's select: t = pts * av_q2d(tb).
    q = tb[0] / tb[1]
    return math.floor((pts * q - pts0 * q) / seg)


class _Stderr(threading.Thread):
    def __init__(self, stream: Any) -> None:
        super().__init__(daemon=True)
        self.stream = stream
        self.frames: queue.Queue[tuple[int, int, int] | None] = queue.Queue()
        self.time_base: tuple[int, int] | None = None
        self.tail: list[str] = []
        self.truncation: str | None = None

    def run(self) -> None:
        for raw in iter(self.stream.readline, b""):
            line = raw.decode("utf-8", "replace")
            if "Parsed_showinfo" in line:
                m = _TB.search(line)
                if m and self.time_base is None:
                    self.time_base = (int(m.group(1)), int(m.group(2)))
                m = _FRAME.search(line)
                if m:
                    self.frames.put((int(m.group(2)), int(m.group(3)), int(m.group(4))))
            else:
                self.tail = (self.tail + [line.strip()])[-8:]
                if self.truncation is None:
                    self.truncation = truncation_indicator(line)
        self.frames.put(None)


def _killed_by_signal(proc: subprocess.Popen[bytes], err: _Stderr) -> str | None:
    """Why ffmpeg ended, if a signal (SIGTERM from a service stop, an OOM kill) ended it.

    Such an end says nothing about the media, so it must not become a
    permanent ``decode_error``.
    """
    try:
        code = proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        return None
    err.join(timeout=5)  # the tail must hold ffmpeg's last lines
    if code < 0:
        return f"ffmpeg killed by signal {-code}"
    if any("received signal" in line for line in err.tail):
        return f"ffmpeg exit {code} after a signal"
    return None


def decode(path: Path, spec: Spec, *, cancel: threading.Event | None = None) -> Iterator[Frame]:
    """Yield the selected frames in presentation order with their real PTS."""
    seg = spec.document.sampling.segment_duration_s
    proc = subprocess.Popen(  # noqa: S603
        decode_command(path, spec), stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    assert proc.stdout is not None and proc.stderr is not None
    err = _Stderr(proc.stderr)
    err.start()
    pts0: int | None = None
    last_k = -1
    last_pts: int | None = None
    count = 0
    try:
        while True:
            if cancel is not None and cancel.is_set():
                raise EncodeError(F.TIMEOUT, "cancelled", retryable=True)
            info = err.frames.get()
            if info is None:
                break
            pts, w, h = info
            size = w * h * 3
            buf = proc.stdout.read(size)
            if buf is None or len(buf) != size:
                if (why := _killed_by_signal(proc, err)) is not None:
                    raise EncodeError(F.INTERNAL_ERROR, why, retryable=True)
                raise EncodeError(F.DECODE_ERROR, "short frame read", retryable=False, scope="content")
            tb = err.time_base
            if tb is None or tb[1] == 0:
                raise EncodeError(F.DECODE_ERROR, "no time_base", retryable=False, scope="content")
            if pts0 is None:
                pts0 = pts
            if last_pts is not None and pts <= last_pts:
                raise EncodeError(F.DECODE_ERROR, "non_monotonic_pts", retryable=False, scope="content")
            k = segment_index(pts, pts0, tb, seg)
            if k <= last_k:
                raise EncodeError(F.DECODE_ERROR, "non_monotonic_pts", retryable=False, scope="content")
            last_pts, last_k = pts, k
            count += 1
            rgb = np.frombuffer(buf, dtype=np.uint8).reshape(h, w, 3)
            yield Frame(k, pts, tb, float(Fraction(pts * tb[0], tb[1])), rgb)
        code = proc.wait(timeout=60)
        if code != 0:
            if (why := _killed_by_signal(proc, err)) is not None:
                raise EncodeError(F.INTERNAL_ERROR, why, retryable=True)
            raise EncodeError(
                F.DECODE_ERROR, f"ffmpeg exit {code}: {' | '.join(err.tail)[-150:]}", retryable=False,
                scope="content",
            )
        if spec.document.rejects_truncation and err.truncation is not None:
            raise EncodeError(
                F.DECODE_ERROR, f"truncated_input: {err.truncation}", retryable=False, scope="content"
            )
        if count == 0:
            raise EncodeError(F.DECODE_ERROR, "zero frames", retryable=False, scope="content")
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()
        proc.stdout.close()
        err.join(timeout=5)


def preprocess(rgb: np.ndarray) -> np.ndarray:
    """``fingerprinter/preprocess/v1``: Pillow resize (shortest edge 256, other edge
    round-half-up, BICUBIC) → centre crop 224 (floor offsets) → float32 ×(1/255) →
    (x − mean)/std → CHW float32."""
    h, w = rgb.shape[:2]
    if w <= h:
        nw, nh = 256, (h * 256 * 2 + w) // (2 * w)
    else:
        nh, nw = 256, (w * 256 * 2 + h) // (2 * h)
    img = Image.fromarray(rgb, mode="RGB").resize((nw, nh), resample=Image.BICUBIC)
    top, left = (nh - 224) // 2, (nw - 224) // 2
    img = img.crop((left, top, left + 224, top + 224))
    arr = np.asarray(img, dtype=np.uint8).astype(np.float32).transpose(2, 0, 1)
    arr = arr * RESCALE
    return ((arr - MEAN) / STD).astype(np.float32, copy=False)
