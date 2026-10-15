"""Representation specs and identity (design §3.1–§3.2, P9-D2, PC-3).

``RepresentationId = RepresentationId.for_content(content_id, spec.name,
spec.version, spec.config_digest)`` (P1, FROZEN). ``config_digest`` is the
SHA-256 of the **canonical JSON** of the full spec document:

- UTF-8, object keys sorted, no insignificant whitespace (``,`` and ``:``);
- numbers in shortest round-trip form (Python ``repr`` of floats, plain ints);
  NaN/Infinity rejected;
- arrays keep their order; no other normalization.

The document is validated against ``SpecDocument`` first and re-serialized from
the validated model, so equal content in a different key order or with different
whitespace has the same digest; numbers are normalized to the schema's type
(``5`` for a float field becomes ``5.0``; a float or string where the schema says
int is refused); unknown fields are refused; any changed value gives a different
digest.
``media_version``, locator and source type are never inputs (provenance only).

Spec versions (operator decision OF-1, design §3.2a): ``dinov2-seg/1`` accepts a
decode that ffmpeg finishes with exit 0 even when the demuxer reported a
truncated input; ``dinov2-seg/2`` is identical except ``decode.truncation =
"explicit-indicators/v1"``, which turns those reports into ``decode_error``
(media.py). The optional ``decode.truncation`` field is omitted from the
canonical form when unset, so ``dinov2-seg/1`` keeps its digest and identity.
"""

from __future__ import annotations

import hashlib
import json
import math
from typing import Any, Literal

from antipiracy_contracts.digests import ContentDigest
from antipiracy_contracts.ids import ContentId, RepresentationId, TargetId
from antipiracy_contracts.models.representations import RepresentationSpec
from pydantic import BaseModel, ConfigDict, StrictBool, StrictFloat, StrictInt, StrictStr

SPEC_SCHEMA = "fingerprinter/spec/v1"
MODEL_ID = "facebook/dinov2-base"
MODEL_REVISION = "f9e44c814b77203eaa57a6bdbbd535f21ede1415"
RUNTIME_PROFILE = "rt-1"


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ModelPart(_Strict):
    id: StrictStr
    revision: StrictStr
    loader: StrictStr
    attn_implementation: StrictStr
    output: StrictStr
    dim: StrictInt


class DecodePart(_Strict):
    tool: StrictStr
    stream: StrictStr
    seek: StrictStr
    autorotate: StrictBool
    genpts: StrictBool
    fps_mode: StrictStr
    copyts: StrictBool
    pix_fmt: StrictStr
    sws_flags: StrictStr
    colour_policy: StrictStr
    tone_mapping: StrictStr
    scale: StrictStr
    truncation: Literal["explicit-indicators/v1"] | None = None
    """Absent in ``dinov2-seg/1`` (and then left out of the canonical form)."""


class SamplingPart(_Strict):
    segment_duration_s: StrictFloat
    origin: StrictStr
    frame_rule: StrictStr
    empty_segment: StrictStr


class PreprocessPart(_Strict):
    impl: StrictStr
    library: StrictStr
    resize_shortest_edge: StrictInt
    other_edge: StrictStr
    resample: StrictStr
    crop: StrictInt
    crop_offset: StrictStr
    rescale: StrictStr
    mean: tuple[StrictFloat, StrictFloat, StrictFloat]
    std: tuple[StrictFloat, StrictFloat, StrictFloat]
    layout: StrictStr
    dtype: StrictStr


class InferencePart(_Strict):
    compute_dtype: Literal["float32", "float16"]


class PostprocessPart(_Strict):
    normalize: StrictStr
    accumulate_dtype: StrictStr
    coarse: StrictStr


class ArtifactPart(_Strict):
    format: StrictStr
    vector_dtype: StrictStr


class SpecDocument(_Strict):
    schema_: StrictStr
    family: StrictStr
    pipeline_version: StrictInt
    runtime_profile: StrictStr
    model: ModelPart
    decode: DecodePart
    sampling: SamplingPart
    preprocess: PreprocessPart
    inference: InferencePart
    postprocess: PostprocessPart
    artifact: ArtifactPart

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)

    def to_json_obj(self) -> dict[str, Any]:
        obj = self.model_dump(mode="python")
        obj["schema"] = obj.pop("schema_")
        if obj["decode"].get("truncation") is None:
            obj["decode"].pop("truncation", None)
        return _lists(obj)

    @classmethod
    def from_json_obj(cls, obj: dict[str, Any]) -> SpecDocument:
        data = dict(obj)
        if "schema" in data:
            data["schema_"] = data.pop("schema")
        doc = cls.model_validate(data)
        if doc.schema_ != SPEC_SCHEMA:
            raise ValueError(f"unknown spec schema {doc.schema_!r}")
        return doc

    @property
    def compute_dtype(self) -> str:
        return self.inference.compute_dtype

    @property
    def segment_duration_s(self) -> float:
        return self.sampling.segment_duration_s

    @property
    def rejects_truncation(self) -> bool:
        return self.decode.truncation is not None


def _lists(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _lists(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_lists(v) for v in value]
    return value


def canonical_json(obj: Any) -> bytes:
    """The canonical byte form (module docstring)."""
    _check_numbers(obj)
    return json.dumps(
        obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")


def _check_numbers(value: Any) -> None:
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("non-finite number in a spec document")
    if isinstance(value, dict):
        for k, v in value.items():
            if not isinstance(k, str):
                raise ValueError("spec document keys must be strings")
            _check_numbers(v)
    elif isinstance(value, (list, tuple)):
        for v in value:
            _check_numbers(v)


class Spec:
    """A validated spec document with its canonical digest and contract spec."""

    def __init__(self, document: SpecDocument) -> None:
        self.document = document
        self.canonical = canonical_json(document.to_json_obj())
        self.digest = ContentDigest("sha256:" + hashlib.sha256(self.canonical).hexdigest())
        self.name = document.family
        self.version = str(document.pipeline_version)
        self.contract = RepresentationSpec(
            name=self.name, version=self.version, config_digest=self.digest
        )

    @classmethod
    def from_json(cls, text: str | bytes) -> Spec:
        return cls(SpecDocument.from_json_obj(json.loads(text)))

    def representation_id(self, content_id: ContentId) -> RepresentationId:
        return RepresentationId.for_content(content_id, self.name, self.version, self.digest)

    def target_representation_id(self, target_id: TargetId, version: int) -> RepresentationId:
        return RepresentationId.for_target(target_id, version, self.name, self.version, self.digest)

    @property
    def compute_dtype(self) -> str:
        return self.document.compute_dtype

    @property
    def hex(self) -> str:
        return self.digest.hex

    def __repr__(self) -> str:
        return f"Spec({self.name}/{self.version} {self.digest[:19]}… {self.compute_dtype})"


BUILTINS = ("dinov2-seg/1", "dinov2-seg/1-fp16", "dinov2-seg/2", "dinov2-seg/2-fp16")
DEFAULT_BUILTIN = "dinov2-seg/2"


def builtin(name: str) -> Spec:
    """A built-in spec by CLI name (``BUILTINS``)."""
    if name not in BUILTINS:
        raise ValueError(f"unknown builtin spec {name!r}")
    version = int(name.split("/")[1].split("-")[0])
    dtype = "float16" if name.endswith("-fp16") else "float32"
    return dinov2_seg_v2(compute_dtype=dtype) if version == 2 else dinov2_seg_v1(compute_dtype=dtype)


def dinov2_seg_v2(*, compute_dtype: str = "float32") -> Spec:
    """``dinov2-seg/2`` (design §3.2a, OF-1): ``dinov2-seg/1`` plus
    ``decode.truncation = explicit-indicators/v1`` — a decode the demuxer reports
    as truncated is a ``decode_error`` even when ffmpeg exits 0."""
    doc = _dinov2_seg_doc(1, compute_dtype)
    doc["pipeline_version"] = 2
    doc["decode"]["truncation"] = "explicit-indicators/v1"
    return Spec(SpecDocument.from_json_obj(doc))


def dinov2_seg_v1(*, compute_dtype: str = "float32") -> Spec:
    """The approved initial spec ``dinov2-seg/1`` (design §3.2). FP32 is the default;
    ``compute_dtype="float16"`` gives the distinct, gated FP16 spec (§3.7)."""
    return Spec(SpecDocument.from_json_obj(_dinov2_seg_doc(1, compute_dtype)))


def _dinov2_seg_doc(pipeline_version: int, compute_dtype: str) -> dict[str, Any]:
    doc = {
        "schema": SPEC_SCHEMA,
        "family": "dinov2-seg",
        "pipeline_version": pipeline_version,
        "runtime_profile": RUNTIME_PROFILE,
        "model": {
            "id": MODEL_ID,
            "revision": MODEL_REVISION,
            "loader": "transformers.AutoModel",
            "attn_implementation": "sdpa",
            "output": "cls",
            "dim": 768,
        },
        "decode": {
            "tool": "ffmpeg",
            "stream": "first-video",
            "seek": "none",
            "autorotate": True,
            "genpts": True,
            "fps_mode": "passthrough",
            "copyts": True,
            "pix_fmt": "rgb24",
            "sws_flags": "bicubic+accurate_rnd+full_chroma_int",
            "colour_policy": "tags-else-swscale-default/rt-1",
            "tone_mapping": "none",
            "scale": "none",
        },
        "sampling": {
            "segment_duration_s": 5.0,
            "origin": "first_decoded_pts",
            "frame_rule": "first_frame_with_index_gt_previous",
            "empty_segment": "absent",
        },
        "preprocess": {
            "impl": "fingerprinter/preprocess/v1",
            "library": "Pillow",
            "resize_shortest_edge": 256,
            "other_edge": "round_half_up",
            "resample": "BICUBIC",
            "crop": 224,
            "crop_offset": "floor",
            "rescale": "1/255",
            "mean": [0.485, 0.456, 0.406],
            "std": [0.229, 0.224, 0.225],
            "layout": "NCHW",
            "dtype": "float32",
        },
        "inference": {"compute_dtype": compute_dtype},
        "postprocess": {"normalize": "l2", "accumulate_dtype": "float32", "coarse": "mean_then_l2"},
        "artifact": {"format": "fp-seg-npz/v1", "vector_dtype": "float32"},
    }
    return doc
