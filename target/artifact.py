"""Wraps a registered target's on-disk media as a `MediaArtifact` so the
same `DINOv2EmbeddingEngine.embed_video_segments` call path used for
candidates also embeds targets -- one embedding code path, not two.

Relocated here (target-eager-build audit, Part B §B.4.G) from its original
home as a private helper in `worker/matching_handler.py` (Phase 10), so
both the lazy build-on-miss path (`worker/matching_handler.py`) and the
explicit eager build command (`target/build.py`) share one implementation
instead of two independent copies. Only depends on `TargetRecord`/
`MediaArtifact`/`SharedTargetMediaStore` -- nothing job/worker-specific --
so `target/` is the natural home; `worker/matching_handler.py` re-exports
the original private name for its own and any existing caller's benefit.
"""
from __future__ import annotations

import mimetypes
from pathlib import Path
from typing import Optional, Tuple

from acquisition.artifact import MediaArtifact
from target.identity import TargetRecord
from target.shared_storage import SharedTargetMediaStore


def target_media_artifact(
    record: TargetRecord, media_store: Optional[SharedTargetMediaStore] = None
) -> Tuple[MediaArtifact, bool]:
    """`content_type` is guessed from the file extension (targets don't
    carry a stored content_type, unlike an acquired `MediaArtifact`); only
    video targets are supported, so anything that doesn't guess to
    `video/*` will correctly fail `embed_video_segments`'s own check.

    Phase 13D (audit §3.5): `record.media_path` is a path on whichever host
    ran registration, which may not be this host. If it's absent locally
    and a `media_store` was injected, fetch a temp copy from shared storage
    (content-addressed by `record.content_sha256`) instead -- the second
    return value tells the caller whether that temp copy needs cleanup
    (`record.media_path` itself is a persistent, caller-owned file and must
    never be deleted; a fetched temp copy is this call's own responsibility,
    mirroring `MediaArtifact.cleanup()`'s "caller owns lifetime" contract).
    `media_store=None` (the default) leaves this function's behavior
    exactly as before Phase 13D: pass the path straight through and let
    `embed_video_segments` raise `UnsupportedMediaError` on its own
    existence check if it's missing."""
    local_path = Path(record.media_path)
    is_temp = False
    if media_store is not None and not local_path.exists():
        fetched = media_store.fetch_to_temp(record.content_sha256, suffix=local_path.suffix)
        if fetched is not None:
            local_path = fetched
            is_temp = True

    content_type = mimetypes.guess_type(str(local_path))[0] or "video/mp4"
    artifact = MediaArtifact(
        local_path=local_path,
        original_url=f"local-target://{record.target_id}/{record.target_version}",
        final_url=f"local-target://{record.target_id}/{record.target_version}",
        content_type=content_type,
        byte_size=0,
        checksum_sha256=record.content_sha256,
        acquisition_duration_s=0.0,
    )
    return artifact, is_temp
