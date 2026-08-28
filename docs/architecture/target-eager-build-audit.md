# Target Eager-Build Audit — `blast/v1` Job-Failure Root Cause + `target.cli build` Feasibility

## 1. Status

**AUDIT AND DIAGNOSIS ONLY. No production code was modified to produce this
document.** All findings below come from direct source inspection, read-only
Redis inspection of pre-existing job-state hashes, `ffprobe`/`sha256sum`
against the registered target file, and one throwaway `ffmpeg` timing test
(written to a scratch directory and deleted afterward, never touching
`target_cache/` or Redis). No dedup marker, job state, target record, or
cache entry was created, deleted, or rebuilt to produce these findings.

Every claim below is labeled, matching the convention already established in
`docs/architecture/target-management-audit.md`:

- **VERIFIED FROM SOURCE** — read directly in the file/line cited, this session.
- **VERIFIED FROM RUNTIME STATE** — read directly from live Redis / filesystem / `ffprobe` output, this session.
- **INFERRED** — a reasonable conclusion from source/state that isn't directly asserted by a single line or record.
- **RECOMMENDATION** — a proposed direction, explicitly not yet implemented.

Scope: `integration/submission.py`, `work_queue/{state,results,keys}.py`,
`target/{cli,service,registry,cache,segment_cache,versioning,lock}.py`,
`worker/{matching_handler,main,fingerprint_worker}.py`,
`embedding/{frames,config,dinov2_engine,result}.py`,
`acquisition/{ssrf_guard,validation}.py`, plus `tests/test_target_build_on_miss.py`
and `tests/test_target_cli.py` as reference for established testing patterns.
`old/` was not read (no relevance). No architecture change, Redis contract
change, or queue-schema change is proposed anywhere in this document.

---

## Part A — Root-cause diagnosis: why the isolated `blast/v1` job failed

### A.1 Reproduction

- Candidate tested: the preview/sample URL, `Blast (2026) HDRip Sample
  (640x360).mp4` (~4.16MB, confirmed live — see A.7).
- Target tested: `target_id=blast`, `target_version=v1` →
  `/home/dhanush/Videos/Blast.mp4`.
- Two prior isolated-submission jobs were found already recorded in Redis
  (`fingerprint:job:{id}:state`), both `ENQUEUED` → `CLAIMED` → `FAILED`,
  attempt 1 of 1, no retry.

### A.2 Failure stage

**VERIFIED FROM SOURCE / VERIFIED FROM RUNTIME STATE.** The failure occurs in
**target segment-embedding resolution** — specifically the build-on-miss
path inside `_resolve_target_segments()`
(`worker/matching_handler.py:217-247`) — which runs *after* candidate
acquisition and candidate embedding have both already succeeded. This is not
a candidate-acquisition, SSRF, HTTP, or candidate-decoding failure.

### A.3 Actual error

Exact `failure_reason` recorded on both failed job-state hashes
(`fingerprint:job:{job_id}:state`):

```
target 'blast' version 'v1' media is unusable: ffmpeg timed out extracting segment frames from /home/dhanush/Videos/Blast.mp4
```

Raised at `worker/matching_handler.py:237`, wrapping
`embedding/frames.py:137`:

```python
raise UnsupportedMediaError(f"ffmpeg timed out extracting segment frames from {media_path}") from exc
```

which fires on `subprocess.TimeoutExpired` inside `extract_segment_frames()`,
timeout = `DEFAULT_SEGMENT_FFMPEG_TIMEOUT_S = 300.0`
(`embedding/frames.py:33`).

Timing recorded on the two failed jobs (`claimed_at` → `failed_at`):
**306.9s** and **313.1s** respectively — both just over the 300s ceiling.

### A.4 Root cause

`extract_segment_frames()` (`embedding/frames.py:96-146`) decodes the target
file **end-to-end with no frame-count cap** — `ffmpeg -vf
fps=1/segment_duration_s`, no `-frames:v` — because it must sample one frame
per 5-second segment across the *entire* video (`SegmentSamplingConfig.
segment_duration_s = 5.0`, `embedding/config.py:119`, unoverridden by the
worker).

The registered target is the **full movie**, not a short reference clip:

| Property | Value | Source |
|---|---|---|
| Duration | 8495.552s (~2h21m) | `ffprobe -show_entries format=duration` |
| Size | 1,816,320,506 bytes (1.7GB) | `ls -la` |
| Video codec | HEVC, 1920×800 | `ffprobe -show_entries stream=codec_name,width,height` |

A controlled timing test (decode + PNG-write, same ffmpeg invocation shape
as production, first 600s of the file only) took **20.0s wall time**,
extrapolating to **~284s** for the full 8495.5s file under *idle* conditions
alone — already within ~6% of the 300s ceiling before any of the real
worker's concurrent load (embedding-model inference, disk I/O for ~1,700
PNG frames, candidate processing in the same process) is added. The two
observed real failures (306.9s, 313.1s) are consistent with that margin
being consumed by real-world load.

**Root cause, precisely stated:** a hardcoded, duration-unaware
300-second subprocess timeout (`DEFAULT_SEGMENT_FFMPEG_TIMEOUT_S`,
`embedding/frames.py:33`) is insufficient for the specific target media
that was registered (a full-length, ~2h21m movie) on this hardware. The
error-handling/classification logic itself (`UnsupportedMediaError` →
`PermanentFailure`, no retry) behaves exactly as intentionally designed
(see the module docstring in `worker/matching_handler.py:22-32`) — this is
not a bug in that logic, only in the fixed timeout constant relative to the
registered target's duration.

### A.5 Why claiming succeeded but processing failed

Claiming only requires the stream/consumer-group machinery to work (already
verified healthy in the earlier bridge investigation). Processing requires
the handler to run to completion, which for a first-time job against an
uncached target must build the target's segment embeddings on the spot
(`registry.get_or_build_segment_embedding`, build-on-miss).
`target_cache/pooled/` and `target_cache/segments/` were both confirmed
**empty** (0 files) — no build has ever succeeded for `blast/v1` — so every
job against this target re-triggers the same 300s-bounded full-file ffmpeg
decode and hits the same ceiling every time. This is independent of, and
downstream from, the earlier bridge/queue investigation.

### A.6 Target status

- Registration: **correct**. `target.cli get blast --version v1` resolves
  cleanly; the registry's `content_sha256`
  (`3151aaf0...c522bb`) exactly matches
  `sha256sum /home/dhanush/Videos/Blast.mp4`.
- Target embedding/cache: **not usable**. `target_cache/pooled/` and
  `target_cache/segments/` contain 0 files — no build has ever completed,
  because every attempt times out as described above.

### A.7 Candidate status

- Reachable: yes — `curl` with a `Range: bytes=0-65535` request against the
  preview URL returned `HTTP/2 206`, `content-type: video/mp4`,
  `content-range: bytes 0-65535/4364548` (a real, ~4.16MB video file with
  working byte-range support).
- Acquisition/SSRF: not implicated — the failure trace never reaches
  `acquisition/`'s error classes; the recorded error text names the
  *target* path (`/home/dhanush/Videos/Blast.mp4`), not the candidate URL,
  independently confirming candidate acquisition and candidate embedding
  already completed before the job reached target resolution.

### A.8 Code status

No functional defect found in acquisition, validation, target-registration,
or job-queue code — all behaved correctly. The one implementation weak
point is `DEFAULT_SEGMENT_FFMPEG_TIMEOUT_S = 300.0`
(`embedding/frames.py:33`), which is not scaled to target duration/size and
is insufficient for a full-length movie on this hardware. Whether this is a
"code defect" (fixed timeout, no duration-awareness) or a "data/config
problem" (a full-length movie registered as the target instead of a shorter
reference clip) is a design call, not resolved here — **Part B below is one
way to address the operational symptom (moving the timeout risk out of the
live job path) without deciding that question.**

### A.9 Next action (not implemented here)

Either (a) re-register `blast/v1` from a shorter reference clip, or (b)
raise `DEFAULT_SEGMENT_FFMPEG_TIMEOUT_S` for full-length targets, or (c) —
what Part B audits — give operators a way to run the (possibly slow) build
step explicitly, ahead of live job processing, so a timeout is caught at a
controlled moment instead of failing a real job. These are not mutually
exclusive.

---

## Part B — Feasibility audit: `python -m target.cli build <target_id> --version <version>`

### B.1 Objective

Determine whether the existing target-management architecture already
contains what's needed to add an explicit, operator-triggered "build this
target's embeddings now" command — so a target's (possibly slow, possibly
timeout-prone) first build happens under operator control, not inside a
live fingerprint job — and if so, exactly what the smallest correct
implementation looks like. **This part proposes a design; it changes
nothing.**

### B.2 Current-state findings

| Component | Finding | Label |
|---|---|---|
| `target/service.py` (`TargetService`) | No embedding/build knowledge at all — deliberately: "never touches Redis... never knows cache paths" (module docstring). No partial build method exists. | VERIFIED FROM SOURCE |
| `target/registry.py` (`TargetRegistry.get_or_build_segment_embedding`, lines 500-572) | Already implements the full cache-first, lock-guarded, build-callback mechanism a build command needs. Takes any `build(record) -> (segments, coarse_vector)` callable — not candidate-specific despite its only current caller being candidate-driven. | VERIFIED FROM SOURCE |
| `target/cache.py` / `target/segment_cache.py` | Define what "built" means: a `SegmentEmbeddingCacheEntry` keyed by `cache_entry_key(target_id, target_version, content_sha256, spec)`. `_load_and_validate()` treats any schema/field mismatch or corruption as a miss, never a crash — this **is** the idempotency/corruption-tolerance guarantee, already built. | VERIFIED FROM SOURCE |
| `worker/matching_handler.py:_resolve_target_segments` (lines 217-247) | The *only* existing caller of `get_or_build_segment_embedding`. Builds `spec` from `candidate.to_embedding_spec()` — but per `embedding/result.py`, every field of that spec (`model_id`, `model_version`, `embedding_schema_version`, `preprocessing_config`, `segment_sampling_config`) comes from the **engine's own fixed configuration**, not from anything candidate-specific. | VERIFIED FROM SOURCE |
| `embedding/frames.py:extract_segment_frames` | No awareness of "eager vs. lazy" caller — behaves identically either way. An eager build will hit the *same* 300s ceiling on `blast/v1` that the lazy path did (see Part A) — this feature does not fix that, it relocates *when* it's discovered. | VERIFIED FROM SOURCE |
| `worker/main.py` | Already exposes every wiring function an eager build needs as free functions: `build_redis_client`, `build_registry` (incl. shared-storage variant), `build_media_store`, `build_engine`. All import `torch`/`transformers` transitively — exactly why `target/cli.py` currently avoids importing `worker.main` at module scope (see its own docstring, lines 20-29). | VERIFIED FROM SOURCE |
| `tests/test_target_build_on_miss.py` | Established test pattern for this exact mechanism: synthetic `build` callback, no real DINOv2/ffmpeg. Covers cache-hit, build-once, unknown-target, missing-segment-cache, build-exception-releases-lock, concurrent-miss-builds-once, lock-wait-timeout. | VERIFIED FROM SOURCE |
| `tests/test_target_cli.py` | Established CLI test pattern: in-process `main(argv)`, `monkeypatch.setenv`, human + `--json` output assertions, exit-code assertions. | VERIFIED FROM SOURCE |

### B.3 Exact existing build path (what a `build` command reuses unchanged)

```python
registry.get_or_build_segment_embedding(
    target_id, target_version, spec,
    build=lambda record: (
        lambda result: (result.segments, result.coarse_vector)
    )(engine.embed_video_segments(_target_artifact(record, media_store)[0])),
)
```

This is verbatim what `_resolve_target_segments` already does today. An
eager build command is *this same call*, invoked proactively from a
CLI/service entry point instead of reactively from a job handler — **no
new registry, cache, or lock logic is required.**

### B.4 Questionnaire

**A. Does the existing code already contain enough functionality to build a
target eagerly, or is a new service-level method required?**
No new low-level functionality is required. `TargetRegistry.
get_or_build_segment_embedding` + `DINOv2EmbeddingEngine.
embed_video_segments` + a `_target_artifact`-equivalent wrapper already do
everything. What's missing is only an **orchestration entry point** — a
service-level method wiring these together outside the job-processing
path — plus the CLI verb to invoke it.

**B. What exact existing function(s) perform the lazy build?**
`worker/matching_handler.py:_resolve_target_segments()`, calling
`TargetRegistry.get_or_build_segment_embedding`, fed by
`DINOv2EmbeddingEngine.embed_video_segments()` and `_target_artifact()`.

**C. What exact cache artifacts must exist for a target to be considered
"built"?**
A `SegmentEmbeddingCacheEntry` in the segment cache for
`(target_id, target_version, content_sha256, spec)`, where `spec` is
derived from the engine's current model/preprocessing/segment-sampling
configuration — i.e. `registry.has_compatible_segment_embedding(target_id,
target_version, spec)` returns `True`. The pooled (non-segment) cache is
not required for the matching path in current use and should not be
conflated with "built."

**D. How should `build` behave in each failure scenario?**

| Scenario | Existing mechanism | Behavior |
|---|---|---|
| target/version doesn't exist | `registry.get_target` → `None` before any lock/build; `get_or_build_segment_embedding` raises `KeyError` | Report as clean not-found error |
| already completely built | First cache check hits; `build()` never called | Report "already built" / print existing entry summary, not an error |
| partially built (pooled xor segment) | Cache lookups are per-representation, independent | No special-casing — segment build proceeds on its own miss regardless of pooled-cache state |
| media missing | `_target_artifact` / `embed_video_segments`'s existence check → `UnsupportedMediaError` | Report as media error |
| media invalid/unusable | Same `UnsupportedMediaError` path (ffprobe/ffmpeg non-zero exit, no frames, non-video content-type) | Report as media error |
| ffmpeg times out | Same `UnsupportedMediaError` (`extract_segment_frames`'s `TimeoutExpired` branch) — **this is exactly Part A's failure** | Surfaced immediately at operator-controlled time instead of failing a live job — this is the point of the feature |
| embedding generation fails (model/inference error) | `InferenceError` propagates un-wrapped from `build()`; worker maps this to `TransientFailure` (retryable) | Treat as retryable/transient, not a hard target failure |
| process crashes partway through build | Lock is `SET NX PX` with TTL, no auto-renewal (`target/lock.py`) — expires after `lock_ttl_ms` (default 600s), next caller rebuilds. Cache writes go through `FilesystemEmbeddingCache._atomic_write`'s `tempfile` + `os.replace` — a crash mid-write never leaves a corrupt file visible under the real name | No new crash-safety work needed; matches guarantees already established in `tests/test_target_crash_safety.py` for the rest of the lifecycle |

**E. Should the build operation be idempotent, and how?**
Yes, for free. Re-running `build` on an already-built target is a pure
cache hit (no lock touched, `build()` never called) — inherent to
`get_or_build_segment_embedding`'s existing "check cache first" structure,
not something the new command needs to implement itself.

**F. Should the CLI call the registry/cache/embedding layers directly, or
should there be a service-level `build_target(...)` operation?**
Service-level. A `build_target(...)`-shaped function belongs alongside
`TargetService`/`TargetRegistry`, not inline in `target/cli.py` — matching
the module's own stated architecture ("CLI is a thin client... all
lifecycle logic lives in TargetService/TargetRegistry," and "CLI is the
first, but not the only intended, client"). Since `TargetService` is
deliberately kept torch/embedding-free, the natural seam is a small new
orchestration function/module that takes an already-constructed
`TargetRegistry` + `DINOv2EmbeddingEngine` and calls
`get_or_build_segment_embedding` — callable from both the CLI and a future
dashboard/API without either duplicating the wiring.

**G. Smallest clean implementation?**
One new function, e.g. `target/build.py:build_target(registry, engine,
target_id, target_version, media_store=None) -> SegmentEmbeddingCacheEntry`,
that:
1. looks up the record (propagates `KeyError`/not-found as-is),
2. constructs `spec` from `engine`'s own config (no candidate needed —
   see B.2/`_resolve_target_segments`),
3. calls `registry.get_or_build_segment_embedding(...)` with a `build`
   closure built the same way `_resolve_target_segments` builds one today.

`_target_artifact` (currently private to `worker/matching_handler.py`)
should move to a shared location both this new module and
`matching_handler.py` can import from without either pulling in the
other's worker-specific machinery — it only depends on `TargetRecord`/
`MediaArtifact`, not on anything job/worker-specific, so `target/` itself
is the natural home.

Then `target/cli.py` gets a `build` subcommand whose handler lazily
imports the engine-construction wiring (`worker.main.build_engine`/
`build_media_store`, or `DINOv2EmbeddingEngine` directly) **inside the
command function**, not at module scope — preserving the module's existing,
explicitly-stated "other subcommands stay torch-free" property.

**H. Concurrency/race considerations if a dashboard builds a target while a
worker is running?**
No new race: eager-build and lazy-build-on-miss converge on the identical
`get_or_build_segment_embedding` call, so the existing per-
`(target_id, target_version, content_sha256, spec)` lock already
serializes them correctly (one winner builds, others poll). The only
latent risk is the lock's own documented, pre-existing limitation — it does
not auto-extend, so a build exceeding `DEFAULT_LOCK_TTL_MS` (600s) could
let a second caller start a redundant build. This is unrelated to, and not
introduced by, this feature.

**I. Should lazy build-on-miss remain as a fallback, or eventually be
disabled?**
**RECOMMENDATION**, not implemented: keep it as the fallback. It's what
makes the system correct-by-default — a target processed without ever
running `build` still works, just slower on first hit — and the mechanism
the eager path reuses is identical, so there's no correctness reason to
remove it. The eager command's value is purely operational (predictable
timing, moving the ffmpeg-timeout risk out of the live job path), not a
replacement for the safety net.

**J. Tests to extend / add?**
- Extend: `tests/test_target_build_on_miss.py`'s synthetic-`build`-callback
  style applies almost directly to testing the new `build_target()`
  orchestration (spec construction, not-found propagation, already-built
  no-op, exception propagation) without real ffmpeg/DINOv2.
- New: a `tests/test_target_build.py` (or extension of the file above)
  covering `build_target()`'s own logic; new CLI tests in
  `tests/test_target_cli.py` for the `build` subcommand (success,
  already-built, not-found, media-error cases) following that file's
  existing `main(argv)` / `monkeypatch.setenv` / `--json` pattern; and an
  import-isolation test (mirroring `tests/test_embedding_lazy_import.py`)
  asserting `target.cli`'s other subcommands still don't pull in `torch`
  at module scope after the `build` subcommand is added.

### B.5 Proposed minimal design (summary)

New `target/build.py` with one function `build_target(registry, engine,
target_id, target_version, media_store=None)`, reusing a relocated
`_target_artifact` and `registry.get_or_build_segment_embedding` verbatim.
New `build` subcommand in `target/cli.py` that lazily imports the
engine-construction wiring only inside its handler.

### B.6 Files that would need modification (implementation phase — not done here)

- New: `target/build.py` — the `build_target()` orchestration function.
- `worker/matching_handler.py` — update its import if `_target_artifact` is relocated.
- `target/cli.py` — new `build` subcommand + argparse wiring, lazy engine import inside the handler.
- `worker/main.py` — possibly unaffected if the new module constructs its own engine/media_store rather than importing `worker.main`; worth deciding explicitly during implementation to preserve the CLI's "never imports worker.main" property.

### B.7 Explicitly NOT to change

- `TargetRegistry.get_or_build_segment_embedding`, `target/lock.py`, or the
  cache's atomic-write/validation logic — all already correct and
  sufficient for this feature.
- `embedding/frames.py`'s timeout value or ffmpeg invocation — orthogonal
  to this feature. The `build` command does not fix Part A's root cause; it
  only moves *when* that timeout is discovered (from inside a live job to
  an operator-controlled command).
- `TargetService`'s Redis/torch-free boundary — do not add embedding logic
  into it directly.
- The lazy build-on-miss path in `worker/matching_handler.py` — keep as
  fallback per B.4.I.
- Redis queue contracts, stream names, consumer groups, or job schemas —
  untouched by this feature in every proposed form above.

---

## Appendix — Raw evidence collected this session

**Redis job-state records** (`fingerprint:job:{job_id}:state`, read-only):

```
job e0ca02802c1dcf46f258074f60b18145:
  status=failed  attempt=1  claimed_at=1787909339.074  failed_at=1787909645.953  (Δ=306.9s)
  failure_reason=target 'blast' version 'v1' media is unusable: ffmpeg timed out
                 extracting segment frames from /home/dhanush/Videos/Blast.mp4

job 89d31e763b28b1f6c7a9bb368ee67be2:
  status=failed  attempt=1  claimed_at=1787908276.093  failed_at=1787908589.192  (Δ=313.1s)
  failure_reason=target 'blast' version 'v1' media is unusable: ffmpeg timed out
                 extracting segment frames from /home/dhanush/Videos/Blast.mp4
```

(The remaining ~30 job-state records inspected in the same sweep were an
unrelated earlier crawler batch — mostly `unsupported content type:
'application/pdf'`, one `404 Not Found` — not part of this diagnosis.)

**Target registry / file verification:**

```
$ target.cli get blast --version v1 --json
content_sha256=3151aaf09eba7bafb681d8b704835554aff40742786262e2cfebb8bbafc522bb
media_path=/home/dhanush/Videos/Blast.mp4

$ sha256sum /home/dhanush/Videos/Blast.mp4
3151aaf09eba7bafb681d8b704835554aff40742786262e2cfebb8bbafc522bb   (match)

$ ffprobe ... /home/dhanush/Videos/Blast.mp4
duration=8495.552000  size=1816320506  codec_name=hevc  width=1920  height=800

$ find target_cache -type f | wc -l
0
```

**Candidate URL verification (ranged request, 65KB only):**

```
$ curl -sS -D - -o /dev/null --max-time 20 -H "Range: bytes=0-65535" "<preview URL>"
HTTP/2 206
content-type: video/mp4
content-range: bytes 0-65535/4364548
```

**ffmpeg decode-speed sanity check** (scratch dir, deleted after use):

```
$ ffmpeg -v error -t 600 -i /home/dhanush/Videos/Blast.mp4 -vf "fps=0.2" seg_%06d.png
real 0m20.047s   (120 frames written)
```
Extrapolated full-file time: `8495.552 / 600 * 20.047 ≈ 284s` — within ~6%
of the 300s timeout under idle conditions alone.
