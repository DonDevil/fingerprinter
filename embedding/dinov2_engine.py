"""The DINOv2 embedding engine.

Boundary (per phase brief):

    MediaArtifact -> DINOv2EmbeddingEngine -> EmbeddingResult -> TargetEmbeddingCache

This module knows nothing about Redis, crawler jobs, or URLs — it consumes
Phase 5's `acquisition.MediaArtifact` (a validated local file, already on
disk) and produces Phase 6-compatible `EmbeddingResult`s. It does not store
anything; `TargetEmbeddingCache` (Phase 6) is the caller's concern, driven
off `EmbeddingResult.to_embedding_spec()` and `.vector`.

Model choice: `facebook/dinov2-base` (768-dim ViT-B/14), pinned to a fixed
snapshot revision. See `docs/architecture/history/phase-07-dinov2-embedding.md`
for the full rationale (also confirmed against `old/dinov2/*` experiments —
the old prototype already validated this exact model/config).
"""
from __future__ import annotations

import shutil
import time
from pathlib import Path
from typing import List, Optional

import numpy as np
import torch
from PIL import Image, UnidentifiedImageError
from transformers import AutoImageProcessor, AutoModel

from acquisition.artifact import MediaArtifact
from embedding.config import IMAGE_SAMPLING_CONFIG, PreprocessingConfig, SamplingConfig
from embedding.errors import (
    DeviceUnavailableError,
    InferenceError,
    ModelLoadError,
    UnsupportedMediaError,
)
from embedding.frames import extract_frames, make_frames_dir
from embedding.result import EMBEDDING_SCHEMA_VERSION, EmbeddingResult

# Pinned to the exact snapshot revision already validated by the old
# prototype (see phase-07 docs) — a floating "main" would let the weights
# change out from under a running deployment without any local signal.
DEFAULT_MODEL_ID = "facebook/dinov2-base"
DEFAULT_MODEL_REVISION = "f9e44c814b77203eaa57a6bdbbd535f21ede1415"

_VALID_DEVICES = ("auto", "cpu", "cuda")


class DINOv2EmbeddingEngine:
    """Loads a DINOv2 model once per instance and embeds `MediaArtifact`s.

    Construction does all the expensive work (model + processor load,
    device placement) so repeated `embed()` calls never reload anything —
    see `docs/architecture/history/phase-07-dinov2-embedding.md` /
    "Device management".
    """

    def __init__(
        self,
        model_id: str = DEFAULT_MODEL_ID,
        model_revision: str = DEFAULT_MODEL_REVISION,
        device: str = "auto",
        preprocessing_config: Optional[PreprocessingConfig] = None,
        sampling_config: Optional[SamplingConfig] = None,
        normalize: bool = True,
        local_files_only: bool = True,
    ) -> None:
        if device not in _VALID_DEVICES:
            raise ValueError(f"device must be one of {_VALID_DEVICES}, got {device!r}")

        self.model_id = model_id
        self.model_revision = model_revision
        self.preprocessing_config = preprocessing_config or PreprocessingConfig()
        self.sampling_config = sampling_config or SamplingConfig()
        self.normalize = normalize
        self.model_version = model_revision

        self.device = self._resolve_device(device)

        load_start = time.monotonic()
        try:
            self._processor = AutoImageProcessor.from_pretrained(
                model_id, revision=model_revision, local_files_only=local_files_only
            )
            self._model = AutoModel.from_pretrained(
                model_id, revision=model_revision, local_files_only=local_files_only
            )
        except (OSError, ValueError) as exc:
            hint = (
                " (local_files_only=True and weights are not cached locally — "
                "either pre-download the model or pass local_files_only=False "
                "to allow a network fetch)"
                if local_files_only
                else ""
            )
            raise ModelLoadError(f"failed to load model {model_id!r} @ {model_revision!r}: {exc}{hint}") from exc
        self._model.to(self.device)
        self._model.eval()
        self.model_load_duration_s = time.monotonic() - load_start

        self.embedding_dim = int(self._model.config.hidden_size)

    @staticmethod
    def _resolve_device(requested: str) -> torch.device:
        if requested == "cpu":
            return torch.device("cpu")
        if requested == "cuda":
            if not torch.cuda.is_available():
                raise DeviceUnavailableError("device='cuda' requested but torch.cuda.is_available() is False")
            return torch.device("cuda")
        # "auto"
        return torch.device("cuda" if torch.cuda.is_available() else "cpu")

    def embed(self, artifact: MediaArtifact) -> EmbeddingResult:
        """Embed one media artifact. Dispatches on `artifact.content_type`:
        `image/*` -> single-frame embedding, `video/*` -> deterministic
        frame sampling + aggregation. Raises `UnsupportedMediaError` for
        anything else or for unreadable/corrupt bytes."""
        if not artifact.local_path.exists():
            raise UnsupportedMediaError(f"media file does not exist: {artifact.local_path}")

        content_type = (artifact.content_type or "").lower()
        if content_type.startswith("image/"):
            return self._embed_image(artifact.local_path)
        if content_type.startswith("video/"):
            return self._embed_video(artifact.local_path)
        raise UnsupportedMediaError(f"unsupported content_type for embedding: {artifact.content_type!r}")

    # -- image path ----------------------------------------------------

    def _embed_image(self, path: Path) -> EmbeddingResult:
        start = time.monotonic()
        image = self._load_image(path)
        vector = self._embed_pil_image(image)
        duration = time.monotonic() - start

        return EmbeddingResult(
            vector=tuple(float(x) for x in vector),
            model_id=self.model_id,
            model_version=self.model_version,
            dimensionality=len(vector),
            normalized=self.normalize,
            preprocessing_config=self.preprocessing_config,
            sampling_config=IMAGE_SAMPLING_CONFIG,
            media_type="image",
            frame_count=1,
            aggregation_method="none",
            embedding_schema_version=EMBEDDING_SCHEMA_VERSION,
            frame_vectors=None,
            inference_duration_s=duration,
        )

    @staticmethod
    def _load_image(path: Path) -> Image.Image:
        try:
            with Image.open(path) as img:
                return img.convert("RGB")
        except (UnidentifiedImageError, OSError) as exc:
            raise UnsupportedMediaError(f"could not decode image {path}: {exc}") from exc

    # -- video path ------------------------------------------------------

    def _embed_video(self, path: Path) -> EmbeddingResult:
        start = time.monotonic()
        frames_dir = make_frames_dir()
        try:
            frame_paths = extract_frames(path, self.sampling_config, frames_dir)
            frame_vectors: List[np.ndarray] = []
            for frame_path in frame_paths:
                image = self._load_image(frame_path)
                frame_vectors.append(self._embed_pil_image(image))
                # Bound peak disk/RAM: drop this frame the moment it's
                # embedded, never hold the whole sampled set open at once.
                frame_path.unlink(missing_ok=True)
        finally:
            shutil.rmtree(frames_dir, ignore_errors=True)

        aggregated = self._aggregate(frame_vectors)
        duration = time.monotonic() - start

        return EmbeddingResult(
            vector=tuple(float(x) for x in aggregated),
            model_id=self.model_id,
            model_version=self.model_version,
            dimensionality=len(aggregated),
            normalized=self.normalize,
            preprocessing_config=self.preprocessing_config,
            sampling_config=self.sampling_config,
            media_type="video",
            frame_count=len(frame_vectors),
            aggregation_method=self.sampling_config.aggregation,
            embedding_schema_version=EMBEDDING_SCHEMA_VERSION,
            frame_vectors=tuple(tuple(float(x) for x in v) for v in frame_vectors),
            inference_duration_s=duration,
        )

    def _aggregate(self, frame_vectors: List[np.ndarray]) -> np.ndarray:
        """Mean-pool per-frame vectors, then re-normalize if `normalize` is
        set. Documented explicitly per phase brief: this is *not* temporal
        voting or clip matching, just an unweighted centroid of the sampled
        frames' embeddings, re-projected onto the unit sphere so the
        aggregated vector has the same normalization contract as a
        single-frame (image) embedding."""
        stacked = np.stack(frame_vectors, axis=0)
        mean = stacked.mean(axis=0)
        if self.normalize:
            mean = self._l2_normalize(mean)
        return mean

    # -- shared inference ------------------------------------------------

    def _embed_pil_image(self, image: Image.Image) -> np.ndarray:
        inputs = self._processor(images=image, return_tensors="pt")
        inputs = {k: v.to(self.device) for k, v in inputs.items()}
        try:
            with torch.inference_mode():
                outputs = self._model(**inputs)
        except RuntimeError as exc:
            raise InferenceError(f"DINOv2 forward pass failed: {exc}") from exc

        # CLS token — index 0 of the patch-token sequence — is DINOv2's
        # standard global image representation (matches old/dinov2/*.py's
        # prior experiments, which used the same `last_hidden_state[:, 0]`).
        cls_token = outputs.last_hidden_state[:, 0]
        vector = cls_token.squeeze(0).to("cpu").numpy()
        # Drop CUDA-side references promptly; avoid retaining any
        # computation graph or device tensor beyond this call.
        del outputs, cls_token, inputs
        if self.device.type == "cuda":
            torch.cuda.empty_cache()

        if self.normalize:
            vector = self._l2_normalize(vector)
        return vector

    @staticmethod
    def _l2_normalize(vector: np.ndarray) -> np.ndarray:
        norm = np.linalg.norm(vector)
        if norm == 0.0:
            return vector
        return vector / norm
