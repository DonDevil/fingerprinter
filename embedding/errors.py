"""Embedding-specific failure classification.

This is a *separate* taxonomy from `acquisition.errors`
(`TransientAcquisitionError`/`PermanentAcquisitionError`) and from
`worker.fingerprint_worker` (`TransientFailure`/`PermanentFailure`) —
per the phase brief, this phase does not build a second retry framework, it
only documents how a future handler should map these onto Phase 3's two
exception types:

| `embedding.errors` exception | Phase 3 mapping | Why |
|---|---|---|
| `UnsupportedMediaError` | `PermanentFailure` | Retrying can't make an unsupported/corrupt file decodable. |
| `DeviceUnavailableError` | `PermanentFailure` (see note) | Not a fact about the job; see note below. |
| `ModelLoadError` | `PermanentFailure` (see note) | Same — environment/config problem, not job-specific. |
| `InferenceError` | `TransientFailure` | May be a transient resource issue (e.g. momentary OOM); the same media might succeed on retry once resources free up. |

Note: `DeviceUnavailableError`/`ModelLoadError` are raised at `__init__` time,
before any job exists to fail — a caller wiring this engine into a worker
handler would most naturally let engine construction happen once at worker
startup (matching "model loading should happen once per engine instance,
not once per frame") and fail fast there, the same way Phase 5 treats a
missing `ffprobe` binary as an uncaught startup problem rather than a
per-job classification.
"""
from __future__ import annotations


class EmbeddingError(Exception):
    """Base class for every embedding-engine failure."""


class UnsupportedMediaError(EmbeddingError):
    """The media artifact is not a supported image/video, or its bytes
    can't be decoded into at least one usable frame."""


class ModelLoadError(EmbeddingError):
    """The requested model/checkpoint could not be loaded (missing local
    weights, invalid model_id, corrupt checkpoint)."""


class DeviceUnavailableError(EmbeddingError):
    """The requested compute device is not available on this host (e.g.
    device="cuda" requested but torch.cuda.is_available() is False)."""


class InferenceError(EmbeddingError):
    """The model failed during a forward pass on otherwise-valid input."""
