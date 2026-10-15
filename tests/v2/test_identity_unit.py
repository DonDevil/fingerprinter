"""Identity, canonicalization, runtime profiles, preprocessing (unit; no services).

TI-1/2/3/4, TP-5, TI-8 (matching logic), §3.4 preprocessing determinism.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime

import numpy as np
import pytest
from antipiracy_contracts.digests import ContentDigest
from antipiracy_contracts.ids import ContentId, TargetId
from antipiracy_contracts.models.representations import Representation

from fingerprinter.config import Settings
from fingerprinter.errors import ConfigError
from fingerprinter.media import preprocess, segment_index
from fingerprinter.runtime import RuntimeFingerprint, RuntimeProfile
from fingerprinter.spec import Spec, SpecDocument, canonical_json, dinov2_seg_v1


def cid(data: bytes) -> ContentId:
    return ContentId.of("full-bytes/v1", ContentDigest("sha256:" + hashlib.sha256(data).hexdigest()))


def test_spec_digest_is_sha256_of_canonical_bytes():
    spec = dinov2_seg_v1()
    assert spec.digest == "sha256:" + hashlib.sha256(spec.canonical).hexdigest()
    assert b" " not in spec.canonical.replace(b"ffmpeg version", b"")  # no insignificant whitespace
    assert spec.contract.name == "dinov2-seg" and spec.contract.version == "1"


def test_canonicalization_ignores_key_order_and_whitespace():
    spec = dinov2_seg_v1()
    obj = json.loads(spec.canonical)
    shuffled = json.dumps(dict(reversed(list(obj.items()))), indent=4)
    assert Spec.from_json(shuffled).digest == spec.digest


@pytest.mark.parametrize(
    "path,value",
    [
        (("model", "revision"), "0" * 40),
        (("model", "id"), "facebook/dinov2-small"),
        (("runtime_profile",), "rt-2"),
        (("decode", "sws_flags"), "bilinear"),
        (("decode", "colour_policy"), "force-bt709"),
        (("decode", "tone_mapping"), "hable"),
        (("sampling", "segment_duration_s"), 4.0),
        (("preprocess", "resize_shortest_edge"), 224),
        (("preprocess", "resample"), "BILINEAR"),
        (("preprocess", "mean"), [0.5, 0.5, 0.5]),
        (("inference", "compute_dtype"), "float16"),
        (("postprocess", "normalize"), "none"),
        (("artifact", "vector_dtype"), "float16"),
        (("pipeline_version",), 2),
    ],
)
def test_every_identity_field_changes_the_digest(path, value):
    base = dinov2_seg_v1()
    obj = json.loads(base.canonical)
    node = obj
    for part in path[:-1]:
        node = node[part]
    node[path[-1]] = value
    changed = Spec.from_json(json.dumps(obj))
    assert changed.digest != base.digest
    content = cid(b"x")
    assert changed.representation_id(content) != base.representation_id(content)


def test_numbers_are_normalized_to_the_schema_type():
    obj = json.loads(dinov2_seg_v1().canonical)
    obj["sampling"]["segment_duration_s"] = 5  # int: normalized to the schema's float 5.0
    assert Spec.from_json(json.dumps(obj)).digest == dinov2_seg_v1().digest
    obj["pipeline_version"] = 1.0  # a float where the schema says int: refused
    with pytest.raises(ValueError):
        SpecDocument.from_json_obj(obj)
    obj["pipeline_version"] = "1"
    with pytest.raises(ValueError):
        SpecDocument.from_json_obj(obj)


def test_unknown_fields_and_non_finite_numbers_rejected():
    obj = json.loads(dinov2_seg_v1().canonical)
    obj["extra"] = 1
    with pytest.raises(ValueError):
        SpecDocument.from_json_obj(obj)
    with pytest.raises(ValueError):
        canonical_json({"a": float("nan")})


def test_ti1_ti2_identity_stable_and_distinct():
    spec = dinov2_seg_v1()
    a, b = cid(b"bytes-a"), cid(b"bytes-b")
    assert spec.representation_id(a) == dinov2_seg_v1().representation_id(a)
    assert spec.representation_id(a) != spec.representation_id(b)
    rep = Representation(
        representation_id=spec.representation_id(a), content_id=a, spec=spec.contract,
        created_at=datetime(2026, 10, 10, tzinfo=UTC),
    )
    assert rep.representation_id == Representation.expected_id(a, spec.contract)


def test_ti4_media_version_and_locator_are_not_inputs():
    # The identity function takes only (content_id, spec): same bytes at two locators
    # or under a new media version have the same ContentId and so the same identity.
    spec = dinov2_seg_v1()
    same_bytes = b"identical served bytes"
    assert spec.representation_id(cid(same_bytes)) == spec.representation_id(cid(bytes(same_bytes)))


def test_tp5_fp16_and_fp32_are_distinct_specs_and_identities():
    fp32, fp16 = dinov2_seg_v1(), dinov2_seg_v1(compute_dtype="float16")
    assert fp32.digest != fp16.digest
    content = cid(b"c")
    assert fp32.representation_id(content) != fp16.representation_id(content)
    t = TargetId.new()
    assert fp32.target_representation_id(t, 1) != fp16.target_representation_id(t, 1)


def _fp(device, **common):
    base = {"ffmpeg": "6.1.1", "torch": "2.13.0", "model_files": {"m": "a"}}
    base.update(common)
    variant = {"cuda_runtime": "13.0", "cudnn": "92000"} if device == "cuda" else {}
    return RuntimeFingerprint(base, device, variant)


def test_ti8_runtime_profile_matching():
    profile = RuntimeProfile.from_fingerprints("rt-1", _fp("cpu"), _fp("cuda"))
    assert profile.mismatch(_fp("cpu"), {"cpu"}) is None
    assert profile.mismatch(_fp("cuda"), {"cpu"}) == "device variant 'cuda' not admitted for profile rt-1"
    assert profile.mismatch(_fp("cuda"), {"cpu", "cuda"}) is None
    assert "torch" in profile.mismatch(_fp("cpu", torch="2.14.0"), {"cpu"})
    other = RuntimeFingerprint(_fp("cuda").common, "cuda", {"cuda_runtime": "12.8", "cudnn": "9"})
    assert "components differ" in profile.mismatch(other, {"cpu", "cuda"})
    with pytest.raises(ValueError):
        RuntimeProfile.from_fingerprints("rt-1", _fp("cpu"), _fp("cuda", torch="x"))


def test_orphan_grace_must_exceed_attempt_timeout():
    with pytest.raises(ConfigError):
        Settings.from_env({}, orphan_grace_s=100.0, encoder={"attempt_timeout_s": 200.0})
    s = Settings.from_env({})
    assert s.orphan_grace_s > s.encoder.attempt_timeout_s == 14_400.0


def test_settings_from_env_nested_and_lists():
    s = Settings.from_env({
        "FINGERPRINTER_HOST_ID": "h9",
        "FINGERPRINTER_SCYLLA__CONTACT_POINTS": "10.0.0.1,10.0.0.2",
        "FINGERPRINTER_ENCODER__MAX_CLAIMS": "7",
    })
    assert s.host_id == "h9" and s.scylla.contact_points == ["10.0.0.1", "10.0.0.2"]
    assert s.encoder.max_claims == 7


def test_segment_index_boundaries_use_real_pts():
    tb = (1, 12288)
    five = 5 * 12288
    assert segment_index(five - 1, 0, tb, 5.0) == 0
    assert segment_index(five, 0, tb, 5.0) == 1
    assert segment_index(five + 1, 0, tb, 5.0) == 1
    assert segment_index(1000 + five, 1000, tb, 5.0) == 1  # origin = first decoded PTS
    assert segment_index(3 * five, 0, tb, 5.0) == 3  # holes give non-contiguous k


def test_preprocess_shape_dtype_and_determinism():
    rng = np.random.default_rng(1)
    for h, w in ((120, 160), (800, 1920), (1080, 720), (256, 256), (225, 400)):
        img = rng.integers(0, 256, size=(h, w, 3), dtype=np.uint8)
        a, b = preprocess(img), preprocess(img.copy())
        assert a.shape == (3, 224, 224) and a.dtype == np.float32
        assert hashlib.sha256(a.tobytes()).hexdigest() == hashlib.sha256(b.tobytes()).hexdigest()


def test_preprocess_resize_rule_round_half_up():
    from PIL import Image

    # 1920x800: short edge 800 → 256, long = round_half_up(1920*256/800) = 614 (614.4)
    seen = {}
    orig = Image.Image.resize

    def spy(self, size, *a, **k):
        seen["size"] = size
        return orig(self, size, *a, **k)

    Image.Image.resize = spy
    try:
        preprocess(np.zeros((800, 1920, 3), dtype=np.uint8))
        assert seen["size"] == (614, 256)
        preprocess(np.zeros((3, 5, 3), dtype=np.uint8) + 7)  # 5*256/3 = 426.67 → 427
        assert seen["size"] == (427, 256)
        preprocess(np.zeros((2, 3, 3), dtype=np.uint8))  # 3*256/2 = 384.0 exactly
        assert seen["size"] == (384, 256)
    finally:
        Image.Image.resize = orig


def test_preprocess_vs_hf_bit_image_processor_tolerance():
    """Documented difference (design §3.4): our explicit Pillow pipeline vs the HF
    processor (TorchvisionBackend under transformers 5.15.1) — close, not equal."""
    from transformers import AutoImageProcessor

    from fingerprinter.spec import MODEL_ID, MODEL_REVISION

    proc = AutoImageProcessor.from_pretrained(MODEL_ID, revision=MODEL_REVISION, local_files_only=True)
    rng = np.random.default_rng(7)
    diffs = []
    for h, w in ((240, 320), (720, 1280)):
        img = rng.integers(0, 256, size=(h, w, 3), dtype=np.uint8)
        ours = preprocess(img)
        theirs = proc(images=img, return_tensors="np")["pixel_values"][0]
        assert theirs.shape == ours.shape
        diffs.append(float(np.abs(ours - theirs).mean()))
    assert max(diffs) < 0.25, diffs  # same geometry/normalization; resampler differs


# --- OF-1: dinov2-seg/2 (design §3.2a) ---------------------------------------------

V1_FP32 = "sha256:ddfc71c1ad8b31dc90de056b93500dd8aa4295273a37ebf4d04011c5ccc8098a"
V1_FP16 = "sha256:4dadb2a089e31bb4544d90ed2c0e94e755613f86e82f58dba67019c572913027"


def test_spec_v1_identity_unchanged_by_the_optional_truncation_field():
    # the digests recorded at P9 implementation (validation.md §1) must not move
    assert dinov2_seg_v1().digest == V1_FP32
    assert dinov2_seg_v1(compute_dtype="float16").digest == V1_FP16
    assert b"truncation" not in dinov2_seg_v1().canonical
    assert not dinov2_seg_v1().document.rejects_truncation


def test_spec_v2_is_a_new_version_and_identity():
    from fingerprinter.spec import DEFAULT_BUILTIN, builtin, dinov2_seg_v2

    v1, v2 = dinov2_seg_v1(), dinov2_seg_v2()
    assert v2.contract.name == "dinov2-seg" and v2.contract.version == "2"
    assert v2.document.decode.truncation == "explicit-indicators/v1" and v2.document.rejects_truncation
    assert v2.digest not in (V1_FP32, V1_FP16) and v2.digest == builtin(DEFAULT_BUILTIN).digest
    content = cid(b"same bytes")
    assert v2.representation_id(content) != v1.representation_id(content)
    assert Spec.from_json(v2.canonical).digest == v2.digest
    # v2 differs from v1 only in the version and the truncation policy
    a, b = json.loads(v1.canonical), json.loads(v2.canonical)
    b["decode"].pop("truncation")
    b["pipeline_version"] = 1
    assert a == b
    fp16 = dinov2_seg_v2(compute_dtype="float16")
    assert fp16.digest != v2.digest and fp16.compute_dtype == "float16"


def test_spec_truncation_policy_values_are_closed():
    obj = json.loads(dinov2_seg_v1().canonical)
    obj["decode"]["truncation"] = "any-warning"
    with pytest.raises(ValueError):
        Spec.from_json(json.dumps(obj))
    obj["decode"]["truncation"] = None  # explicit null is the v1 form: same digest
    assert Spec.from_json(json.dumps(obj)).digest == V1_FP32
