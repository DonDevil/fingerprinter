"""`embedding` package surface.

`DINOv2EmbeddingEngine`/`DEFAULT_MODEL_ID`/`DEFAULT_MODEL_REVISION` are
exposed lazily (PEP 562 module `__getattr__`) rather than imported eagerly
at package-import time. `embedding.dinov2_engine` is the only submodule in
this package that imports torch/numpy/transformers/Pillow
(`embedding/dinov2_engine.py`'s own imports); `embedding.config`,
`embedding.errors`, and `embedding.result` do not and stay eager below,
unchanged.

Verified from source before this change: nothing in this repository does
`import embedding` followed by `embedding.DINOv2EmbeddingEngine` (or any of
the other lazy names) -- every caller already imports
`from embedding.dinov2_engine import ...` directly. But merely importing
*any* submodule of this package -- e.g. `target/segment_cache.py`'s
`from embedding.result import SegmentEmbedding` -- runs this file first
(Python always executes a package's `__init__.py` before a submodule), so
the previous eager `from embedding.dinov2_engine import ...` above forced
the full ML stack into every caller of `target.registry`
(`target.registry` -> `target.segment_cache` -> `embedding.result`),
including a pure metadata operation like `target.cli list` that never
touches an embedding. That transitive cost is what this file's laziness
removes; `__getattr__` below preserves the exact same public names for any
caller that *does* do `from embedding import DINOv2EmbeddingEngine` (or
`embedding.DINOv2EmbeddingEngine`), just deferred to first access instead
of paid at package-import time.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from embedding.config import (
    DEFAULT_SEGMENT_DURATION_S,
    IMAGE_SAMPLING_CONFIG,
    PreprocessingConfig,
    SamplingConfig,
    SegmentSamplingConfig,
)
from embedding.errors import (
    DeviceUnavailableError,
    EmbeddingError,
    InferenceError,
    ModelLoadError,
    UnsupportedMediaError,
)
from embedding.result import (
    SEGMENT_EMBEDDING_SCHEMA_VERSION,
    EmbeddingResult,
    SegmentEmbedding,
    VideoSegmentEmbeddingResult,
)

if TYPE_CHECKING:
    # Only for static type checkers / IDEs -- never executed at runtime, so
    # this does not reintroduce the eager import.
    from embedding.dinov2_engine import DEFAULT_MODEL_ID, DEFAULT_MODEL_REVISION, DINOv2EmbeddingEngine

__all__ = [
    "DINOv2EmbeddingEngine",
    "DEFAULT_MODEL_ID",
    "DEFAULT_MODEL_REVISION",
    "PreprocessingConfig",
    "SamplingConfig",
    "IMAGE_SAMPLING_CONFIG",
    "SegmentSamplingConfig",
    "DEFAULT_SEGMENT_DURATION_S",
    "EmbeddingResult",
    "SegmentEmbedding",
    "VideoSegmentEmbeddingResult",
    "SEGMENT_EMBEDDING_SCHEMA_VERSION",
    "EmbeddingError",
    "UnsupportedMediaError",
    "ModelLoadError",
    "DeviceUnavailableError",
    "InferenceError",
]

_LAZY_DINOV2_ATTRS = frozenset({"DINOv2EmbeddingEngine", "DEFAULT_MODEL_ID", "DEFAULT_MODEL_REVISION"})


def __getattr__(name: str):
    if name in _LAZY_DINOV2_ATTRS:
        from embedding import dinov2_engine

        return getattr(dinov2_engine, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
