"""Test helpers: crawler2-shaped events, fixture media."""

from __future__ import annotations

import hashlib
import subprocess
from datetime import UTC, datetime
from pathlib import Path

from antipiracy_contracts.digests import ContentDigest
from antipiracy_contracts.events import encode_event, new_event
from antipiracy_contracts.events.fingerprinting import EncodeRequested
from antipiracy_contracts.ids import EventId
from antipiracy_contracts.models.media import ContentKey, Media, MediaKind
from antipiracy_contracts.models.representations import RepresentationSpec
from antipiracy_contracts.models.web import UrlRef
from antipiracy_contracts.ownership import Component, Producer, ServiceName

SCHEME = "full-bytes/v1"
C2 = Producer(
    service=ServiceName.CRAWLER2, component=Component.MEDIA_REGISTRY, instance="it:media_registry:1"
)


def digest_of(data: bytes) -> ContentDigest:
    return ContentDigest("sha256:" + hashlib.sha256(data).hexdigest())


def request_env(
    data_or_digest: bytes | str,
    url: str,
    *,
    referer: str | None = None,
    required: RepresentationSpec | None = None,
    priority: int = 50,
    event_id: EventId | None = None,
):
    digest = (
        ContentDigest(data_or_digest) if isinstance(data_or_digest, str) else digest_of(data_or_digest)
    )
    payload = EncodeRequested(
        content=ContentKey.of(SCHEME, digest),
        source=Media.of(UrlRef.of(url), MediaKind.VIDEO_FILE),
        referer=UrlRef.of(referer) if referer else None,
        required_spec=required,
        priority=priority,
    )
    return new_event(payload, producer=C2, occurred_at=datetime.now(UTC), event_id=event_id)


def xadd_request(client, stream: str, env, *, maxlen: int | None = None) -> str:
    """Append like crawler2's publisher (ungrouped stream: MAXLEN ~)."""
    fields = {"event_id": str(env.event_id), "event_type": env.event_type, "envelope": encode_event(env)}
    if maxlen is None:
        entry = client.xadd(stream, fields)
    else:
        entry = client.xadd(stream, fields, maxlen=maxlen, approximate=False)
    return entry.decode() if isinstance(entry, bytes) else entry


def make_video(path: Path, *, seconds: float = 12, size: str = "160x120", rate: int = 24,
               extra: list[str] | None = None, src: str = "testsrc2", seed: int = 0) -> Path:
    """A small H.264 MP4 from an ffmpeg test source (deterministic per arguments)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    vf = f"{src}=size={size}:rate={rate}:duration={seconds}"
    cmd = ["ffmpeg", "-nostdin", "-loglevel", "error", "-y", "-f", "lavfi", "-i", vf]
    if seed:
        cmd += ["-vf", f"hue=h={seed * 37 % 360}"]
    cmd += (extra or ["-c:v", "libx264", "-preset", "ultrafast", "-pix_fmt", "yuv420p"])
    cmd += ["-bitexact", "-map_metadata", "-1", str(path)]
    subprocess.run(cmd, check=True)  # noqa: S603
    return path
