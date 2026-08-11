# Phase 7 — DINOv2 Embedding Engine

## Objective

Establish real DINOv2 inference behind a small, model-specific engine:
`MediaArtifact -> DINOv2EmbeddingEngine -> EmbeddingResult ->
TargetEmbeddingCache`. No similarity matching, no FAISS, no thresholds —
this phase proves the embedding contract and produces real vectors, not
the comparison pipeline.

## Package layout

New top-level package `embedding/`, alongside `acquisition/`, `target/`,
`worker/`, `work_queue/`:

- `embedding/config.py` — `PreprocessingConfig`, `SamplingConfig`,
  `IMAGE_SAMPLING_CONFIG`. No torch/transformers import — plain,
  JSON-serializable dataclasses.
- `embedding/errors.py` — the embedding-specific failure taxonomy.
- `embedding/result.py` — `EmbeddingResult`, the typed output, plus
  `to_embedding_spec()` bridging to Phase 6's `EmbeddingSpec`.
- `embedding/frames.py` — deterministic video frame extraction via
  `ffmpeg` subprocess.
- `embedding/dinov2_engine.py` — `DINOv2EmbeddingEngine`, the model
  wrapper.

`embedding` depends on `acquisition` (consumes `MediaArtifact`) but not on
`target`, `worker`, or `work_queue` at import time — see "Redis
independence" below for why that's a real, tested property and not just a
description.

## Model selection

**Chosen: `facebook/dinov2-base`**, pinned to snapshot revision
`f9e44c814b77203eaa57a6bdbbd535f21ede1415`.

| Property | Value |
|---|---|
| Model identifier | `facebook/dinov2-base` |
| Source | Hugging Face Hub (`transformers.AutoModel`/`AutoImageProcessor`) |
| Pinned revision | `f9e44c814b77203eaa57a6bdbbd535f21ede1415` |
| Architecture | ViT-B/14, `Dinov2Model` |
| Embedding dimensionality | 768 (`hidden_size`) |
| Weights on disk | ~331 MB (`model.safetensors`) |
| Params | ~86M |

Why this model, not a larger one:

- **Already validated.** `old/dinov2/dinov2-test.py` and
  `old/dinov2/video_embedding_test.py` both used `facebook/dinov2-base`
  successfully (CLS-token embedding, cosine similarity), and
  `old/config.yaml`'s `dinov2:` section confirms it was the model the
  prior prototype settled on for its whole matching pipeline
  (`cosine_threshold`, `l2_score_threshold`, etc. were all tuned against
  it). This phase doesn't re-derive that choice from scratch — it inherits
  a decision the old prototype already spent effort validating.
- **Fits the hardware.** Dev GPU is an RTX 2050 with **4 GB VRAM**
  (confirmed via `nvidia-smi`). Measured peak CUDA allocation for this
  engine is ~351 MB (see Diagnostics) — comfortable headroom. `dinov2-large`
  (300M params, 1024-dim) or `dinov2-giant` (1.1B params, 1536-dim) would
  eat a much larger fraction of that budget for no benefit this phase
  needs; `dinov2-base` was chosen deliberately over them, not by default.
- **Weights were already cached locally**
  (`~/.cache/huggingface/hub/models--facebook--dinov2-base`, confirmed
  before writing any code), so this phase needed zero additional model
  download — only `torch`/`transformers`/`torchvision`/`Pillow`/`numpy`
  (the Python packages) were installed. `dinov2-small` (384-dim) was
  **not** cached and was not pulled down speculatively, per the phase
  brief's "do not silently perform a large download" instruction — base
  was the practical choice already sitting on disk, not an arbitrary pick.
- **CPU-viable too.** Runs correctly on CPU (`device="cpu"`) at usable
  latency for this phase's scope (see Diagnostics) — no hard GPU
  requirement, matching the brief's device-management requirement.

Model selection is configurable, not hardcoded: `DINOv2EmbeddingEngine.__init__`
accepts `model_id`/`model_revision`, defaulting to the above.

## Engine interface

```python
from acquisition import MediaAcquirer
from embedding import DINOv2EmbeddingEngine

engine = DINOv2EmbeddingEngine(device="auto")       # loads model once
artifact = acquirer.acquire(url)                    # Phase 5
result = engine.embed(artifact)                     # -> EmbeddingResult
spec = result.to_embedding_spec()                   # -> Phase 6 EmbeddingSpec
cache.put(target_id, target_version, content_sha256, spec, result.vector)  # Phase 6
```

`embed(artifact: MediaArtifact) -> EmbeddingResult` is the entire surface.
The engine never downloads anything, never touches Redis, never manages
crawler jobs, and never manages `MediaArtifact`'s temp-file lifecycle
(`artifact.cleanup()` remains the caller's responsibility, per Phase 5).

`EmbeddingResult` fields (`embedding/result.py`): `vector`, `model_id`,
`model_version`, `dimensionality`, `normalized`, `preprocessing_config`,
`sampling_config`, `media_type`, `frame_count`, `aggregation_method`,
`embedding_schema_version`, optional `frame_vectors` (diagnostics only),
`inference_duration_s`. `to_embedding_spec()` maps the first five
model/config fields directly onto Phase 6's `EmbeddingSpec` — the engine
does not duplicate the cache; it only produces what the cache's
compatibility key requires.

## Media input dispatch

`embed()` dispatches purely on `artifact.content_type`:

- `image/*` → single-frame embedding.
- `video/*` → frame sampling + per-frame embedding + aggregation.
- anything else → `UnsupportedMediaError`.

No URL/Redis/job knowledge anywhere in this package — see "Redis
independence" below.

## Video sampling

`SamplingConfig` (`embedding/config.py`):

```python
SamplingConfig(fps=2.0, max_frames=32, frame_selection="uniform_time_from_start",
                aggregation="mean_pool_l2_normalized")
```

`embedding/frames.py:extract_frames()` shells out to:

```
ffmpeg -v error -i <path> -vf fps=<fps> -frames:v <max_frames> frame_%06d.png
```

- **Deterministic**: `fps=` samples at a fixed rate starting from the first
  frame (t=0), in presentation order — same input bytes + same
  fps/max_frames always yields the same sampled sequence
  (`test_video_sampling_is_deterministic` extracts twice into separate
  directories and byte-compares every frame).
- **Bounded without needing duration up front**: `-frames:v <max_frames>`
  caps output directly; ffmpeg stops on its own once satisfied or the
  input ends, whichever comes first.
- **Reuses an existing dependency**: `ffmpeg`/`ffprobe` are already
  required by Phase 5 (`acquisition/validation.py`); this phase adds no
  second video-decoding dependency (no `opencv-python`, no `PyAV`).
- Frames are written to disk (never buffered in memory as a batch) and are
  deleted immediately after being embedded, one at a time
  (`DINOv2EmbeddingEngine._embed_video`) — the whole video is never loaded
  into RAM, and peak extra disk usage is bounded by `max_frames` PNGs at
  once, not the full decoded video.

## Aggregation

For video, each sampled frame is embedded independently (its own CLS-token
vector), then aggregated by **unweighted mean pooling across frames**,
followed by re-L2-normalization if `normalize=True`
(`DINOv2EmbeddingEngine._aggregate`). This is deliberately the simplest
possible aggregation — an unweighted centroid, not temporal voting, not
clip matching, not a learned pooling — exactly matching the brief's "do not
implement sophisticated temporal matching yet." Per-frame vectors are kept
on `EmbeddingResult.frame_vectors` for diagnostics only; `.vector` (the
aggregated one) is what's handed to `TargetEmbeddingCache.put()`, since
Phase 6's cache API stores one vector per `(target, content, spec)`
representation, not a frame list.

## Image preprocessing

`PreprocessingConfig` (`embedding/config.py`) mirrors exactly what
`facebook/dinov2-base`'s shipped `preprocessor_config.json`
(`BitImageProcessor`) specifies — read directly off the cached config
file, not guessed:

| Step | Value |
|---|---|
| Resize | shortest edge → 256px |
| Resample | bicubic |
| Center crop | 224×224 |
| Color conversion | convert to RGB |
| Rescale | `1/255` |
| Normalize (mean) | `[0.485, 0.456, 0.406]` (ImageNet) |
| Normalize (std) | `[0.229, 0.224, 0.225]` (ImageNet) |

Actual preprocessing execution is delegated to Hugging Face's
`AutoImageProcessor` (loaded once per engine instance, alongside the
model) — `PreprocessingConfig` is the explicit, serializable *record* of
what that processor does, which is what feeds
`EmbeddingSpec.preprocessing_config` so a future preprocessing change
(different crop size, different resize policy) invalidates incompatible
cached embeddings rather than silently producing wrong-shape comparisons
later.

## Normalization

**L2-normalized by default** (`normalize=True`). `DINOv2EmbeddingEngine._l2_normalize`
divides the CLS-token vector by its L2 norm; for video, normalization is
applied to the mean-pooled aggregate, not to each frame before pooling
(pooling first, then one final normalization — see Aggregation). This
matches the old prototype's `video_embedding_test.py`, which also
normalized before storing/comparing. `normalize=False` is supported and
recorded on `EmbeddingResult.normalized` for callers that want the raw CLS
vector — no similarity scoring exists yet in this phase either way, so
normalization here is purely a representation choice, not a matching
decision.

## Device management

`device` ∈ `{"auto", "cpu", "cuda"}`:

- `"cpu"` — always available.
- `"cuda"` — raises `DeviceUnavailableError` immediately at construction if
  `torch.cuda.is_available()` is `False`. No silent fallback.
- `"auto"` — CUDA if available, else CPU.

Model + processor load happens exactly once, in `__init__`
(`model_load_duration_s` records how long) — `embed()` never reloads
anything, verified by `test_model_loads_only_once_per_engine_instance`
(counts `AutoModel.from_pretrained`/`AutoImageProcessor.from_pretrained`
calls across two `embed()` invocations on one engine instance).

Weight loading defaults to `local_files_only=True` — per the brief's "if
downloading model weights is required and would be large/slow, do NOT
silently perform a large download," the engine never reaches out to the
network unless a caller explicitly passes `local_files_only=False`. A
missing local cache raises `ModelLoadError` with an explicit hint rather
than triggering a fetch.

## Resource management

- Inference runs under `torch.inference_mode()` (stricter than
  `torch.no_grad()` — no autograd metadata is attached at all, no
  computation graph is ever built).
- Output tensors are moved to CPU/`numpy` and the GPU-side references
  (`outputs`, `cls_token`, `inputs`) are `del`eted immediately after each
  frame; `torch.cuda.empty_cache()` is called after each inference call
  when running on CUDA, so freed device memory doesn't linger allocated to
  this process's fragment pool between frames.
- Video frames are processed and discarded one at a time (see Video
  sampling) — no batch of decoded frames is ever held in memory together.
- No unnecessary CPU↔GPU copies beyond what `transformers`' processor and
  model forward pass require (inputs moved to the target device once,
  output moved back once, per frame).

## Determinism

`test_repeated_inference_gives_numerically_consistent_output` embeds the
same image twice on CPU and asserts the vectors are identical to
`atol=1e-6` — expected, since eval-mode DINOv2 has no dropout/stochastic
layers and CPU floating-point execution order is stable for repeated calls
within one process. No claim is made (or tested) about bit-for-bit
equality *across different hardware/backends* (e.g. CPU vs. CUDA vs. a
different GPU architecture) — floating-point reduction order can legitimately
differ there.

## Error handling

`embedding/errors.py` defines four failure types, distinct from both
`acquisition.errors` and `worker`'s `TransientFailure`/`PermanentFailure`:

| Exception | Raised when |
|---|---|
| `UnsupportedMediaError` | Unsupported/missing content type, or bytes that can't be decoded into at least one frame (corrupt image/video). |
| `ModelLoadError` | Model/processor failed to load (bad `model_id`/`revision`, or weights not cached locally with `local_files_only=True`). |
| `DeviceUnavailableError` | `device="cuda"` requested but no CUDA device is available. |
| `InferenceError` | A forward pass raised at runtime on otherwise-valid input. |

Per the brief, this phase does **not** build a second retry framework —
`embedding/errors.py`'s module docstring documents the intended mapping
onto Phase 3's `TransientFailure`/`PermanentFailure` for a future handler
to apply (`UnsupportedMediaError` → permanent, `InferenceError` →
transient, `ModelLoadError`/`DeviceUnavailableError` → permanent,
treated as startup-time failures the same way Phase 5 treats a missing
`ffprobe` binary).

## Cache compatibility (Phase 6 bridge)

`EmbeddingResult.to_embedding_spec()` constructs a Phase 6 `EmbeddingSpec`
from `model_id`, `model_version` (the pinned revision string),
`embedding_schema_version`, `preprocessing_config.to_dict()`, and
`sampling_config.to_dict()` — exactly the five fields Phase 6 requires for
cache-compatibility comparison. `target.versioning.EmbeddingSpec` is
imported **lazily**, inside `to_embedding_spec()`, not at module level —
see "Redis independence" below for why.

## Redis independence

`target/__init__.py` (Phase 6) eagerly imports `target.registry`, which
imports `redis`. To keep `embedding`'s own modules free of that transitive
dependency, `embedding/result.py` imports `target.versioning.EmbeddingSpec`
lazily inside `to_embedding_spec()` (only under `TYPE_CHECKING` at module
level) rather than at import time. `test_engine_does_not_depend_on_redis`
verifies this concretely: it spawns a fresh subprocess that imports
`embedding` and `DINOv2EmbeddingEngine` only, then asserts `'redis' not in
sys.modules`. The dependency only appears once a caller actually calls
`to_embedding_spec()` to bridge to Phase 6 — which is the correct
boundary, not an accident of import order.

## Tests

`tests/test_embedding.py`, 16 tests, run entirely offline against two tiny
local fixtures generated for this phase (no external downloads):

- `tests/fixtures/tiny_image.png` — 32×32 PNG, `ffmpeg -f lavfi testsrc`.
- `tests/fixtures/tiny_video.mp4` — 32×32, 4s @ 4fps synthetic test
  pattern, `ffmpeg -f lavfi testsrc`.

1. `test_engine_initializes_correctly`
2. `test_model_loads_only_once_per_engine_instance`
3. `test_image_embedding_succeeds`
4. `test_video_sampling_is_deterministic`
5. `test_video_embedding_succeeds`
6. `test_embedding_dimensionality_is_correct`
7. `test_normalization_behavior_is_correct`
8. `test_preprocessing_configuration_is_represented_correctly`
9. `test_sampling_configuration_is_represented_correctly`
10. `test_cpu_execution_works`
11. `test_unavailable_requested_device_fails_clearly`
12. `test_repeated_inference_gives_numerically_consistent_output`
13. `test_malformed_unsupported_input_fails_clearly`
14. `test_engine_does_not_depend_on_redis`
15. `test_embedding_result_can_be_converted_to_embedding_spec`
16. `test_embedding_can_be_stored_and_retrieved_from_phase6_cache`

Run: `.venv/bin/python -m pytest tests/` — all 87 tests pass (71 Phase 1-6
+ 16 Phase 7), stable across repeated runs. (One unrelated pre-existing
Phase 5 test, `test_corrupt_media_rejected`, was observed to flake once
during this phase's work — `tests/media_test_server.py`'s `_CORRUPT_BODY`
is a module-load-time `os.urandom(2048)` constant, and rare byte sequences
can apparently still sniff as a probeable stream to `ffprobe`. Confirmed
unrelated to this phase's changes: reproduced on a clean `git stash` of
this phase's work, and passed again on every subsequent run. Not modified,
per the brief's "do not refactor `old/`... " scope and because it's a
Phase 5 file, not a Phase 7 one.)

## Diagnostic measurements

Collected as a natural side effect of the focused tests above, on this dev
machine (RTX 2050, 4 GB VRAM) — diagnostic only, not a performance
benchmark:

| Measurement | CPU | CUDA |
|---|---|---|
| Model load time (weights already OS-cached) | 0.12 s | 0.30 s |
| Single-image inference (warm) | 0.12 s | 0.03 s |
| Small video (4 sampled frames) inference | 0.52 s | 0.16 s |
| Peak CUDA memory allocated | — | ~351 MiB |
| Peak CUDA memory reserved | — | ~388 MiB |
| Peak process RSS (both engines loaded in one process) | ~1.6 GiB | (same process) |

351 MiB peak CUDA allocation against a 4 GB VRAM budget confirms the model
choice is comfortably practical on this hardware — no optimization
attempted or needed at this phase.

## Limitations

- **No batch inference.** Frames (video) and images are embedded one at a
  time, not batched into a single forward pass — simplest correct
  implementation for this phase; a real throughput pass would batch
  frames per video.
- **Mean-pool aggregation is intentionally naive.** No frame weighting, no
  outlier rejection, no temporal structure — exactly what the brief asked
  for at this phase ("do not implement sophisticated temporal matching
  yet").
- **`model_version` is the pinned Hub revision string, not a semantic
  version.** Sufficient for cache-compatibility (any revision change is a
  different string, hence a cache miss), but not human-friendly.
- **No streaming/incremental video decode beyond ffmpeg's own behavior** —
  `-frames:v` bounds *output* count, but ffmpeg still decodes forward
  through the file up to that point; a pathological file that stalls
  decoding is bounded by the fixed `DEFAULT_FFMPEG_TIMEOUT_S` (60s)
  subprocess timeout, not by finer-grained progress checks.
- **No multi-GPU / device-index selection** — `device="cuda"` always means
  `cuda:0`; not needed for this single-GPU dev host.
- **CUDA memory is not proactively capped** (no `torch.cuda.set_per_process_memory_fraction`)
  — relies on the model's own small footprint (~351 MiB) staying well
  under the 4 GB card; would matter more if this engine ever ran alongside
  other GPU workloads.
- **`InferenceError` retryability is documented, not enforced** — nothing
  in this phase automatically routes an embedding failure into Phase 3's
  retry machinery; that wiring belongs to whichever future phase adds a
  real fingerprint-worker handler (mirrors Phase 5's acquisition-handler
  precedent, which this phase deliberately doesn't repeat since no such
  handler exists yet for embeddings).
- **Peak RSS figure (~1.6 GiB) reflects one process holding *two* fully
  loaded engine instances (CPU + CUDA) at once**, from the diagnostic
  script — a single real deployment would load one engine per process and
  use less.

## Deferred work

Same bucket as Phases 1-6, still deferred: FAISS, nearest-neighbor search,
cosine similarity, match/no-match decisions, thresholds, temporal voting,
clip matching, audio fingerprints, pHash, multi-model fusion, crawler
integration, Redis job wiring beyond proving `EmbeddingResult` fits Phase
6's cache, distributed GPU scheduling, object storage, production
deployment, monitoring, and rewriting the top-level architecture document.
Also newly deferred from this phase: batched frame inference, a real
fingerprint-worker handler that wires `DINOv2EmbeddingEngine` +
`TargetRegistry` together end-to-end (this phase proves the two are
compatible via direct cache tests, not via a handler), and any
throughput/latency optimization beyond the diagnostic measurements above.
