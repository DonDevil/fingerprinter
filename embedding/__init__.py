from embedding.config import (
    DEFAULT_SEGMENT_DURATION_S,
    IMAGE_SAMPLING_CONFIG,
    PreprocessingConfig,
    SamplingConfig,
    SegmentSamplingConfig,
)
from embedding.dinov2_engine import DEFAULT_MODEL_ID, DEFAULT_MODEL_REVISION, DINOv2EmbeddingEngine
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
