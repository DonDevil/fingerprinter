"""The typed result of a successful acquisition.

`MediaArtifact` is the sole contract between acquisition and every later
pipeline stage: a validated local file plus just enough context to
fingerprint it and trace it back to its source. A future DINOv2/pHash/audio
handler consumes `artifact.local_path` and never learns whether the bytes
came from HTTP, a local file, or object storage.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping, Optional


@dataclass
class MediaArtifact:
    local_path: Path
    original_url: str
    final_url: str
    content_type: str
    byte_size: int
    checksum_sha256: str
    acquisition_duration_s: float
    media_metadata: Optional[Mapping[str, object]] = None
    _cleaned_up: bool = field(default=False, init=False, repr=False)

    def cleanup(self) -> None:
        """Remove the local temp file. Idempotent, safe to call more than once.

        The caller owns this artifact's lifetime — nothing deletes the file
        automatically. Call this once the fingerprint pipeline is done
        reading `local_path`.
        """
        if self._cleaned_up:
            return
        self.local_path.unlink(missing_ok=True)
        self._cleaned_up = True
