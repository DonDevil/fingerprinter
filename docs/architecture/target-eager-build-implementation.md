# Target Eager-Build Implementation — `target.cli build`

## 1. Status

**IMPLEMENTED.** This document describes the code actually merged for the
explicit, operator-triggered target-embedding build command, following:

- `docs/architecture/target-eager-build-audit.md` — Part A: root-cause
  diagnosis of a real `blast/v1` job failure (a full-length movie's
  segment-embedding build exceeding the fixed ffmpeg segment-extraction
  timeout during live job processing). Part B: the approved feasibility
  design for `target.cli build` this implementation follows.

Like `docs/architecture/target-management-implementation.md`, this
document describes what exists in the repository today, not a proposal.
No architecture was changed to deliver this: no new registry, cache, lock,
Redis key shape, queue contract, or persistence system was introduced —
see §7 for the explicit list of what this deliberately did **not** touch.

## 2. What was delivered

| Capability | Entry point |
|---|---|
| Eagerly build (or confirm already-built) a target's segment embeddings, ahead of any live job | `target.build.build_target()` / `target.cli build` |

Everything else in the target lifecycle (`add`/`list`/`get`/
`update-metadata`/`delete`/`reindex`) is unchanged — see
`docs/architecture/target-management-implementation.md` for that surface.

## 3. Why this exists

Audit Part A traced a real production failure: `blast/v1` (a registered
~2h21m, 1.7GB movie) failed its first fingerprint job at the
target-segment-resolution stage. The worker's lazy build-on-miss path
(`worker.matching_handler._resolve_target_segments` →
`TargetRegistry.get_or_build_segment_embedding`) had to decode the
*entire* target file via `ffmpeg` for the first time, which exceeded
`embedding.frames.DEFAULT_SEGMENT_FFMPEG_TIMEOUT_S` (300s, fixed,
duration-unaware). The job failed as a `PermanentFailure`, and because the
build never completed, the target cache stayed empty — every subsequent
job against that target would fail identically.

This feature does not change that timeout or fix the underlying decode
cost (see §7, "explicitly not changed") — it moves *when* the problem is
discovered: from inside a live, already-claimed job to an operator-run
command, executed after `add` and before any job is submitted against the
target.

## 4. Architecture (as built)

```
target.cli build ID --version VERSION [--json]
        │
        ▼
target.build.build_target(registry, engine, target_id, target_version, media_store=None)
        │
        ├── registry.get_target(...)                       -- not-found check
        ├── _segment_spec_for_engine(engine)                -- spec from engine config, no candidate needed
        └── registry.get_or_build_segment_embedding(...)     -- pre-existing, unchanged
                │
                ├── cache hit  -> return immediately (already_built=True)
                └── cache miss -> acquire per-(target,spec) lock -> build() -> register -> release
                        │
                        build() = target.artifact.target_media_artifact(record, media_store)
                                  + engine.embed_video_segments(artifact)
```

`target.build.build_target()` is a thin orchestration function, not a new
mechanism: it is the exact same `get_or_build_segment_embedding` call
`worker/matching_handler.py::_resolve_target_segments` already makes,
called proactively instead of reactively. Both call sites now share one
`target_media_artifact()` implementation (`target/artifact.py`, see §5)
instead of two independent copies.

## 5. New/changed modules

### `target/artifact.py` (new)

`target_media_artifact(record, media_store=None) -> (MediaArtifact, bool)`
— relocated out of `worker/matching_handler.py`'s private `_target_artifact`
(Phase 10/13D logic, unchanged byte-for-byte) so both the lazy path and the
new eager path use one implementation. Depends only on `TargetRecord`/
`MediaArtifact`/`SharedTargetMediaStore` — nothing worker/job-specific — so
`target/` is the correct home per the audit's own recommendation (§B.4.G).

`worker/matching_handler.py` re-exports it under its original private name
(`from target.artifact import target_media_artifact as _target_artifact`)
so existing callers (`benchmarks/*.py`) that import `_target_artifact`
from that module are unaffected.

### `target/build.py` (new)

```python
@dataclass(frozen=True)
class BuildResult:
    target_id: str
    target_version: str
    already_built: bool
    entry: SegmentEmbeddingCacheEntry


def build_target(
    registry: TargetRegistry,
    engine: "DINOv2EmbeddingEngine",
    target_id: str,
    target_version: str,
    media_store: Optional[SharedTargetMediaStore] = None,
) -> BuildResult: ...
```

- Raises `target.errors.TargetNotFoundError` if the target isn't
  registered — checked up front, before any embedding work, and again
  (translated from the registry's own `KeyError`) if the target is
  deleted in the narrow window between that check and the build call.
- The segment `EmbeddingSpec` is derived entirely from `engine`'s own
  `model_id`/`model_version`/`preprocessing_config`/`segment_sampling_config`
  — no candidate or job is needed to know what an eager build should
  produce or check for (audit §B.2/§B.4.A: every field of
  `VideoSegmentEmbeddingResult.to_embedding_spec()` already comes from the
  engine alone).
- `already_built` is set from a cache check performed immediately before
  the cache-first `get_or_build_segment_embedding` call — accurate for the
  single-operator use this command is for; see the field's own docstring
  for the narrow, purely cosmetic race it doesn't resolve (correctness of
  "at most one build happens" is unaffected either way, since that
  guarantee lives entirely in `get_or_build_segment_embedding`'s lock, not
  in this flag).
- `DINOv2EmbeddingEngine` is imported only under `TYPE_CHECKING` — this
  module never imports torch/transformers at runtime, matching
  `target/registry.py`'s existing torch-free property
  (`tests/test_embedding_lazy_import.py` has a dedicated test for this).

### `target/cli.py` (changed)

New subcommand:

```
python -m target.cli build ID --version VERSION [--json]
```

- Every other subcommand still calls exactly one `TargetService` method
  through a `TargetService` instance built from the environment
  (unchanged). `build` is the one exception — `TargetService` is
  deliberately Redis/torch-free and has no embedding-build method, so
  `_cmd_build` reaches `target.build.build_target()` directly, using this
  module's own registry/media_store wiring.
- To give `build` access to the `TargetRegistry`/`SharedTargetMediaStore`
  it needs (which `TargetService` doesn't expose), `main()`'s wiring was
  refactored from a bare `TargetService` into a small `_Context(service,
  registry, media_store)` built once per invocation. Every existing
  `_cmd_*` handler now takes `context` instead of `service` and reads
  `context.service` — a mechanical, behavior-preserving change (confirmed
  by the full existing `test_target_cli.py` suite passing unchanged).
- `_cmd_build` imports `DINOv2EmbeddingEngine` lazily, inside itself, after
  confirming the target exists — so a not-found target is reported without
  ever loading the model, and every other subcommand's "never imports
  torch" property is untouched.
- New environment variables read by `build` only: `EMBEDDING_DEVICE`,
  `TORCH_NUM_THREADS` — same names/defaults `worker/main.py` already uses,
  for operational consistency (no new configuration surface invented).
- `_print_error(args, error_type, message)` factors out the
  JSON-or-stderr error formatting `main()`'s `TargetServiceError` handler
  already did, reused by `_cmd_build` for the embedding/storage-specific
  errors below (no behavior change to existing error formatting).

### `worker/matching_handler.py` (changed)

Only its imports changed: the module-local `_target_artifact` definition
was removed in favor of importing the relocated function under the same
name (see §5, `target/artifact.py`). `build_matching_handler`,
`_resolve_target_segments`, and every error-mapping decision are
byte-for-byte unchanged.

## 6. Failure/error semantics

| Condition | Exception raised by `build_target()` | CLI behavior |
|---|---|---|
| Target/version not registered | `target.errors.TargetNotFoundError` | Reported via the module's shared `TargetServiceError` handling in `main()`; exit 1 |
| Target media missing/undecodable/ffmpeg timeout | `embedding.errors.UnsupportedMediaError` | Caught in `_cmd_build`, reported with its real type name + message; exit 1 |
| Model forward-pass failure (transient) | `embedding.errors.InferenceError` | Same; exit 1 (message makes clear this is retryable, not a permanent target defect) |
| Shared artifact store unreachable | `target.shared_storage.SharedArtifactStoreError` | Same; exit 1 |
| Another process already building this exact target+spec, poll budget exceeded | `TimeoutError` | Same; exit 1 |
| Invalid `EMBEDDING_DEVICE`/`TORCH_NUM_THREADS` | `ValueError` | Same; exit 1 |
| Already built | *(none)* | `status: "already_built"`, exit 0 |
| Fresh, successful build | *(none)* | `status: "built"`, exit 0 |
| Bad command line (missing `--version`, unknown subcommand) | *(argparse)* | Exit 2, unchanged pre-existing behavior |

No broad `except Exception` was introduced anywhere in this feature. A
failed build never leaves a false "success" state: the underlying cache
write (`FilesystemSegmentEmbeddingCache`/`SharedFilesystemSegmentEmbeddingCache`)
is `tempfile` + `os.replace` atomic, and the build lock releases in a
`finally` inside `get_or_build_segment_embedding` (both pre-existing,
unchanged) — so a crash or exception mid-build leaves nothing visible
under the real cache key, and a subsequent `build` retries a full build
from scratch, never a corrupt partial one.

## 7. Idempotency / rebuild behavior

`build` is idempotent by construction, inherited entirely from
`TargetRegistry.get_or_build_segment_embedding`'s existing cache-first
check — no new "built" marker or force/rebuild flag was added, because
none was needed:

- Re-running `build` against an already-built target is a pure cache hit:
  the embedding engine is never invoked, the build lock is never touched,
  and the CLI reports `already_built` instead of `built`.
- There is deliberately **no** `--force` option. The audit's own
  idempotency analysis (§B.4.E) found the existing mechanism already
  sufficient, and inventing a force flag was explicitly out of scope
  unless the design called for one (it didn't). To replace a target's
  media, the pre-existing target-management contract already requires
  registering a new `target_version` (content-swap-in-place is rejected at
  `TargetService.create_target`) — `build` that new version instead of
  trying to force-rebuild the old one.

## 8. Explicitly NOT changed

Per the audit's own scoping (§B.7) and this implementation's own
verification:

- `TargetRegistry.get_or_build_segment_embedding`, `target/lock.py`, and
  every cache's atomic-write/validation logic — reused verbatim.
- `embedding/frames.py`'s `DEFAULT_SEGMENT_FFMPEG_TIMEOUT_S` (300s) or its
  `ffmpeg` invocation — this feature relocates *when* a timeout is
  discovered, it does not raise the timeout or make decoding faster. See
  §10 for why this remains a live, open limitation.
- `TargetService`'s Redis/torch-free boundary — no embedding logic was
  added to it; `build` bypasses it deliberately (§5).
- The lazy build-on-miss path in `worker/matching_handler.py` — kept as
  the fallback exactly as before. A target that was never eagerly built
  still works via the first job that claims it; it is just slower and
  more timeout-prone on that first hit, which is exactly the operational
  cost this command lets an operator avoid.
- Redis queue contracts, stream names, consumer groups, or job schemas.
- Target add/list/get/update-metadata/delete semantics.

## 9. Tests added

- `tests/test_target_build.py` (new, 7 tests) — `target.build.build_target()`
  against a synthetic engine (no real DINOv2/ffmpeg): successful build,
  target-not-found (engine never called), idempotent already-built no-op,
  media-failure propagation (and the cache staying empty afterward),
  inference-failure propagation, retry-after-failure performing a full
  rebuild (not a resume), and — directly exercising the operational
  property this feature is for — proof that `TargetRegistry.
  get_or_build_segment_embedding` (the same call the worker's lazy path
  makes) resolves an eagerly-built target from cache without invoking its
  own build callback.
- `tests/test_target_cli.py` (+4 tests) — the `build` subcommand's
  success/JSON output, idempotent second run, not-found exit code, and
  media-failure exit code, using a monkeypatched
  `embedding.dinov2_engine.DINOv2EmbeddingEngine`.
- `tests/test_embedding_lazy_import.py` (+1 test) — `target.build` itself
  imports no heavy ML dependency, checked in a fresh subprocess the same
  way the file's existing tests check `target.cli`/`target.registry`/
  `target.service`.

None of the new/changed tests depend on real media, real ffmpeg, or the
real DINOv2 model — every one uses a synthetic engine or synthetic
segment/coarse-vector data, matching the existing style of
`tests/test_target_build_on_miss.py`.

Full suite result at merge time: **381 passed, 0 failed, 0 skipped**
(369 pre-existing + 12 new).

## 10. Known, currently-live limitation (real-media verification finding)

**Verified against the real, already-registered `blast`/`v1` target**
(`/home/dhanush/Videos/Blast.mp4`, ~2h21m, 1.7GB, the exact target from
audit Part A):

```
$ python -m target.cli build blast --version v1 --json
{"error": "UnsupportedMediaError", "message": "ffmpeg timed out extracting segment frames from /home/dhanush/Videos/Blast.mp4"}
```

Exit code 1. `target_cache/` confirmed still empty afterward (0 files) —
no false-success state was left behind, and the registered target record
itself was unaffected.

This is **exactly** the failure this feature was built to relocate, not to
fix (§8): `blast`/`v1`'s full-length decode still exceeds the fixed 300s
`DEFAULT_SEGMENT_FFMPEG_TIMEOUT_S` on this hardware, whether triggered by
`build` or by a live job's lazy path. The practical difference is that this
failure is now visible at a controlled command instead of failing a
claimed job mid-stream and leaving the target cache silently empty for
every subsequent attempt.

**Consequence for `blast`/`v1` specifically:** until one of the two options
the audit already named (Part A.9) is acted on —

1. re-register `blast`/`v1` from a shorter reference clip, or
2. get explicit design sign-off to raise
   `DEFAULT_SEGMENT_FFMPEG_TIMEOUT_S` (or otherwise make segment
   extraction duration-aware) —

running `target.cli build blast --version v1` will not succeed, no segment
cache entry will exist for it, and a fingerprint worker processing a job
against `blast`/`v1` will still fall back to the lazy build-on-miss path
and hit the same timeout. Neither option was in scope for, or applied by,
this implementation.

## 11. Operational recommendation

For any target expected to be long enough to risk the segment-extraction
timeout: run `target.cli add`, then `target.cli build`, and confirm exit
code `0` **before** submitting any real fingerprint jobs against it. If
`build` fails with `UnsupportedMediaError` naming a timeout, do not retry
it as-is — treat it the same way audit Part A does: as a signal that this
target's media is too long for the current fixed timeout, requiring one of
the two options in §10, not a transient condition that resolves itself.
