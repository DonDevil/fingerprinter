# Fingerprinter usage guide

This guide covers the distinct implementations currently in this repository:

1. **V1 worker** (`worker/`, `embedding/`, `matching/`, `target/`,
   `work_queue/`, `integration/`): consumes candidate jobs from Redis Streams
   and returns a match/no-match/processing-failure result.
2. **P9 encoder** (`fingerprinter/`): consumes Crawler V2's
   `encode.requested.v1`, stores DINOv2 representations, and publishes
   representation events. It does not match candidate media to a target.
3. **P10 matcher** (`fingerprinter/`): indexes P9 representations and
   generates verified match decisions. It is implemented but its M3
   acceptance gate is not passed; `match.found` publication is held.

Do not mix the V1 job queue with the P9 event pipeline. The two paths use
different input contracts, persistence, target lifecycle, and output format.

Status labels: **Code-verified** means present in current source/configuration;
**Verified here** means actually executed in this environment;
**Environment-dependent** means it needs services, model files, credentials,
or hardware; **Not production-ready** means a project gate remains open.

## Purpose and processing flow

### V1 job worker

```text
FingerprintCandidate Python API
  -> fingerprint:jobs:stream:{high|default|low} Redis Stream
  -> worker/main.py (consumer group, lease/reclaim/retry)
  -> guarded HTTP(S) acquisition -> ffprobe validation
  -> DINOv2 image/video segment embeddings
  -> target registry/cache (target embeddings may build on first use)
  -> temporal segment matching -> result aggregation
  -> atomic Redis result/state update + fingerprint:results:stream:{priority}
```

The V1 worker matches one candidate against a registered target version.
It does not crawl, discover URLs, maintain a URL frontier, or read Crawler
V2's evidence database. Its URL submission API is a Python API, not a
standalone job-submission CLI or HTTP service.

### P9 encoder

```text
Crawler V2 media discovery -> probe -> full-byte hash/content identity
  -> encode.requested.v1 on shared Redis Streams
  -> Fingerprinter P9 intake (leased, cursor/gap aware)
  -> acquire bytes from external HTTP or internal MinIO object
  -> ffprobe + FFmpeg decode/5-second sampling + preprocess/v1
  -> DINOv2 CLS embeddings (768 floats, normalized)
  -> versioned representation artifact in MinIO + Scylla commit
  -> representation.ready.v1 or encode.failed.v1 in Redis Streams
  -> Crawler V2 media-registry consumer updates its own status projection
```

Representations are content-addressed; source URL/locator is provenance,
not identity. P9 has an independent keyspace/buckets and shares only the
backend services and contract package with Crawler V2. P9 does not decide
whether a candidate matches a target.

### P10 vector matcher

P10 builds Qdrant indexes from the P9 Scylla representation and target
tables, creates matching tasks, uses ANN candidate retrieval followed by
artifact-verified temporal checks, and stores decisions. Its phases include
index generations, capacity reservations, reconciliation, and held event
publication. A task can be `DONE`, `INCOMPLETE`, `WAITING_INDEX`, or
`FAILED`; task completion is not the same as a positive match.

**Limitation:** the current phase report says M3 is not passed (including
equivalence recall and capacity/calibration/end-to-end gates). Some decisions
may be stored but `match.found` is held. Do not treat this as a production
matching service or infer that a similarity score is a calibrated
probability.

## Prerequisites and resource expectations

### V1 basic CPU worker

- Python 3.12 is the documented/tested runtime; `pyproject.toml` does not
  declare `requires-python`.
- `requirements.txt` packages: Redis client, requests, PyTorch,
  torchvision, Transformers, Pillow, and NumPy. Install dev/test dependencies
  separately from `requirements-dev.txt`.
- `ffmpeg` and `ffprobe` on `PATH`.
- A reachable Redis server. V1's default `REDIS_URL` is
  `redis://localhost:6379/0`.
- The pinned DINOv2 model snapshot in the local Hugging Face cache. The V1
  engine defaults to `local_files_only=True`, so normal worker startup does
  not fetch model weights.
- A local target-cache directory. For multiple hosts, an optional genuinely
  shared artifact mount is required; Redis alone does not make local cache
  files shared.

CPU execution is the practical minimal route. The V1 code supports
`EMBEDDING_DEVICE=auto|cpu|cuda`; GPU execution in this repository has not
been validated on real CUDA hardware. No minimum CPU/RAM or throughput
specification is established.

### P9 encoder

- Python 3.12, FFmpeg (P9 runtime profile pins a particular FFmpeg build),
  DINOv2 model revision `f9e44c814b77203eaa57a6bdbbd535f21ede1415` in the
  Hugging Face cache.
- ScyllaDB, Redis, MinIO, and the shared
  `antipiracy-contracts` package from the Crawler V2 `contracts/` checkout.
- P9's direct additions are listed in `requirements-p9.txt`; the complete
  pinned runtime record is `requirements-p9.lock`. The lock file alone is
  not an install recipe for a clean system: its PyTorch CUDA wheel and shared
  contracts source require their respective configured installation sources.
- CPU FP32 encoding is supported by code. GPU requires a matching CUDA
  PyTorch build and hardware. The P9 phase has recorded approved-scope
  validation; it is not production-deployed. FP16 remains gated.
- Scratch space must cover an acquired media file and decode/intermediate
  data. `scratch_max_bytes` defaults to 20 GiB; `acquire.max_bytes` defaults
  to 8 GiB.

### P10

- P9 dependencies and migrated Fingerprinter Scylla state.
- Qdrant. The supplied dev Compose service is Qdrant 1.15.1, loopback HTTP
  port 16333 and gRPC 16334, with an API key and hard 1.5 GiB memory limit.
- Index capacity defaults are provisional; the configured capacity override
  requires a passing, gate-eligible E-R1 evidence report.
- No hardware recommendation is established. GPU is not required for the
  P10 vector index/matcher itself.

## Supported inputs and outputs

### V1

`integration.submission.FingerprintJobSubmitter.submit()` accepts a
`FingerprintCandidate` with:

| Field | Required/default | Meaning and validation |
|---|---|---|
| `candidate_url` | required | Must begin `http://` or `https://`; acquisition performs the actual SSRF/media checks later. |
| `media_evidence_id` | required | Caller-owned opaque reference returned in results. |
| `media_type` | required | Non-empty; the handler/media validator determines whether bytes are usable. |
| `source_domain` | required | Non-empty source label. |
| `target_id`, `target_version` | required | Must refer to a registered target version. |
| `techniques` | defaults to `("dinov2_temporal",)` | Non-empty tuple; V1 currently implements the temporal DINOv2 technique. |
| `priority` | `NORMAL` | `HIGH`, `NORMAL`, `LOW`; selects stream `high`, `default`, or `low`. |
| `max_attempts` | 3 | Integer >=1, stored per job; not a worker setting. |

Invalid candidates return `REJECTED_INVALID`; full/unknown backlog returns
`REJECTED_BACKPRESSURE`; repeated idempotency key returns
`DUPLICATE_SUPPRESSED`; successful enqueue returns `ENQUEUED`, deterministic
`job_id`, and Redis entry ID. The marker is set after backpressure passes
and removed if enqueue fails.

Example (programmatic interface; use a Redis endpoint and target that you
control):

```python
from redis import Redis
from integration.candidate import FingerprintCandidate
from integration.submission import FingerprintJobSubmitter

client = Redis.from_url("redis://localhost:6379/0", decode_responses=True)
submitter = FingerprintJobSubmitter(client)
result = submitter.submit(FingerprintCandidate(
    candidate_url="https://media.example.test/clip.mp4",
    media_evidence_id="evidence-123",
    media_type="video",
    source_domain="media.example.test",
    target_id="target-123",
    target_version="v1",
))
print(result)
```

The candidate URL must be an authorized, reachable HTTP(S) media resource.
The worker rejects loopback/private destinations by default; the test
fixture's private-network exception is not a production setting. There is
no public job-submit CLI or HTTP endpoint.

V1 target registration takes a local media path using
`python -m target.cli add`; the file is content-hashed and registered under
`(target_id, target_version)`. Matching results are stored in Redis hashes
and a results stream; a `NO_MATCH`/`MATCH` decision is separate from the
worker's processing status. Check
[system architecture](docs/architecture/system-architecture.md) for the
current result schema before building a consumer.

### P9/P10

P9's normal input is the shared `encode.requested.v1` event, not an arbitrary
local file. It contains `content: ContentKey`, `source: Media`,
optional `referer: UrlRef`, optional `required_spec`, and `priority`. An
absent `required_spec` selects the Fingerprinter's default spec. It returns
either:

- `representation.ready.v1`: `representation` (content/spec identity,
  representation ID, artifact metadata and dimensions according to the
  shared contract), or
- `encode.failed.v1`: `request_id`, `content_id`, a typed `failure`,
  `retryable`, positive `attempt`, optional `spec` and short `detail`.

These are P9 internal events consumed by Crawler V2's media registry; they
are not V1 job results. The source schema is
[`fingerprinting.py`](../crawler2/contracts/src/antipiracy_contracts/events/fingerprinting.py).

P10 input is derived from P9 media representations and registered target
representations in Scylla. There is no supported CLI that accepts a raw
candidate file and immediately matches it. P10 results have a contract
`MatchResult` with verdict, confidence/calibration fields and aligned
segments, but current publication is gated/held.

## Runnable entry points

| Component | Purpose | Entry point | Dependencies | Inputs | Run / stop | Verification |
|---|---|---|---|---|---|---|
| V1 worker | Process candidate jobs | `python -m worker.main` | Redis, local model, FFmpeg, cache | Redis Streams job | foreground; Ctrl-C/SIGTERM graceful stop | `worker_ready` log and run record |
| V1 target CLI | Register/manage V1 targets | `python -m target.cli` | Redis, target cache; model only for `build` | local target media and target metadata | one-shot | `list`/`get`, then `build` output |
| V1 producer | Submit candidate jobs | Python `FingerprintJobSubmitter`; lower-level `JobProducer` exists | Redis | `FingerprintCandidate` | called by Python application | `SubmissionResult` and stream length |
| P9 operator/runtime | Schema, runtime profile, spec, intake, encode, target, publish, backfill, reconciliation | `python -m fingerprinter ...` | P9: Redis+Scylla+MinIO+contracts; P10 adds Qdrant | events, target media paths or operator files | one-shot commands or role loops; Ctrl-C/SIGTERM | CLI JSON/status and durable rows |
| P9 workers | intake, scheduler, encoder, reconciler/publisher/sweeper | `python -m fingerprinter` followed by `intake`, `scheduler`, `encoder`, or `reconcile` commands | same as P9 | shared event streams and work state | long-running `run`; controlled shutdown | status, logs, metrics, database state |
| P10 matcher | Index and match tasks | `python -m fingerprinter` followed by `matcher`, `index`, or `match` commands | Scylla, Redis, MinIO, Qdrant | P9 rows/targets | `matcher run`, reconciliation | task state/index status; not M3 acceptance |
| Qdrant dev service | Derived vector index | `deploy/qdrant/compose.yaml` | Docker Compose, local API key | P10 index writes | `docker compose ... up/down` | Qdrant health/API and `index status` |
| Tests | Unit, integration, P9/P10 | pytest under `tests/`, `tests/v2/`, `tests/p10/` | see test matrix | checked-in synthetic fixtures; opt-in services | pytest | exit status/report |
| Benchmarks | V1, P9 encoder, P10 evaluation/capacity | `benchmarks/`, `benchmarks/p9-encoder/`, `benchmarks/p10-index/` | models, datasets, sometimes real services/hardware | licensed/approved media or generated/synthetic data | manual, long-running | JSON results and phase-specific gates |

No Docker image or Compose application worker for V1/P9 is defined here.
Qdrant is the only Fingerprinter-owned Compose service. The P9 services
normally reuse a developer Crawler V2 backend stack with separate
Fingerprinter keyspace/prefix/buckets.

## CLI reference

The project has no installed `fingerprinter-*` console scripts. The main
operator interface is `python -m fingerprinter`; target management for V1
is separate at `python -m target.cli`; the V1 worker has no CLI parser.
Argparse supports `-h/--help`; the main CLI's global flags must appear before
the tool name.

The main CLI global flags are `--json` (false; render supported output as
JSON) and `--operator NAME` (unset; audit actor defaults to the current
operating-system username). They must precede the tool name. They do not
replace environment configuration. P10 commands emit JSON regardless of
`--json`.

### Main P9/P10 CLI

```text
python -m fingerprinter [--json] [--operator NAME] TOOL COMMAND [OPTIONS]
```

`--json` selects JSON output; `--operator` supplies the audit actor (otherwise
the current OS user). State-changing commands write audit records. Typed
refusals return nonzero (typically exit 2); unhandled infrastructure failures
remain errors.

| Tool/command | Positional / required options | Optional options and defaults |
|---|---|---|
| `migrate` | none | no command-specific flags; creates P9 schema/buckets and seeds stream registry |
| `runtime fingerprint` | none | `--device {cpu,cuda}` (`cpu`) |
| `runtime init-profile`, `runtime admit-cuda` | none | none; `admit-cuda` requires functioning CUDA hardware |
| `runtime check` | `--spec DIGEST` | `--device cpu` |
| `runtime approve-compat` | `--spec`, `--profile`, `--evidence` | none |
| `spec add` | optional `DOCUMENT` | `--builtin {dinov2-seg/1,dinov2-seg/1-fp16,dinov2-seg/2,dinov2-seg/2-fp16}`; supply document or builtin |
| `spec list` | none | none |
| `spec enable` | `DIGEST`, `--evidence FILE` | none |
| `spec {set-default,deprecate,retire}` | `DIGEST` | none |
| `spec retire-sweep` | none | none |
| `intake init` | exactly one of `--from-start` / `--from-now` | `--reason TEXT` (empty; required by implementation for from-now) |
| `intake status` | none | none; nonzero while a gap is open/uninitialized |
| `intake run` | none | `--until-idle` |
| `intake reinit` | required `--after-reset`, `--reason TEXT` | none |
| `intake gap list` / `show GAP_ID` | `GAP_ID` for show | none |
| `intake gap replay-start GAP_ID` | `--from ISO`, `--to ISO` | none |
| `intake gap replay-done GAP_ID` | `GAP_ID` | none |
| `intake gap mark-unreplayable GAP_ID` | `--reason TEXT` | none |
| `intake accept-gap GAP_ID` | `--reason TEXT` | none; audited acceptance of possible permanent loss |
| `encode status` | none | `--content ID` optional |
| `encode retry` | none | `--content ID`; `--spec-key default`; `--failure CLASS`; `--since ISO` |
| `encoder run` | none | `--device DEVICE` (unset; settings default); `--idle-exit-s FLOAT` (unset); `--max-attempts INT` (unset) |
| `encoder recalibrate` | none | `--device cuda` |
| `scheduler run` | none | none |
| `reconcile run` | none | `--once` |
| `reconcile full` | none | none |
| `reconcile sweep` | none | `--grace-s FLOAT` (settings default) |
| `publisher status` | none | none |
| `publisher register-consumer` | `--stream NAME`, `--group NAME` | none |
| `publisher republish` | `--stream NAME` | none |
| `target register` | `--title`, `--media PATH` | `--kind movie`; repeat `--alias`; `--release-year INT`; `--target-id` |
| `target show ID`, `target list` | target ID for show | none |
| `target retire ID` | `--reason TEXT` | none |
| `target purge ID` | target ID | destructive to target artifacts; inspect exact target and references first |
| `backfill create` | `--from-spec`, `--to-spec` | `--targets-only` |
| `backfill run JOB_ID` | job ID | `--max-items INT` (unset; no CLI item cap) |
| `backfill {pause,resume,cancel,status}` | `JOB_ID` | none |
| `matcher migrate` | none | none |
| `matcher run` | none | `--until-idle` |
| `matcher reconcile` | none | `--once` |
| `index init` | `--spec DIGEST` | none |
| `index status` | none | none |
| `index rebuild` | `--spec`, `--role {media,target}` | `--promote` |
| `index {activate,check,audit}` | required `--spec DIGEST`, `--role {media,target}` | `--generation INT` (active generation when unset) |
| `index drop` | required `--spec DIGEST` | optional `--role {media,target}`, `--generation INT`; with generation, deletes one FAILED generation's collection (generation requires role); without it, drops the spec's index state |
| `match params-register` | `--version`, `--from PATH` | none |
| `match status SUBJECT_ID` | `--spec DIGEST` | none |
| `match summary` | none | none |
| `match show` | `--content ID` or `--target ID` (parser permits neither; supply one; content takes precedence if both are provided) | none |
| `match {rerun,retry} SUBJECT_ID` | required `--spec DIGEST`, `--kind TEXT` | `--raise-cap INT` |
| `match publish-held` | `--version` | none; does not make an uncalibrated decision acceptable |
| `equivalence show CONTENT_ID` | content ID | none |
| `equivalence revoke A B` | `--spec`, `--version`, `--reason` | none |
| `lowinfo {status,refresh}` | required `--spec DIGEST` | none; low-information filter is disabled by default |

This covers arguments defined by `fingerprinter/cli.py` and
`fingerprinter/p10cli.py`. Defaults inherited from the configuration model
are listed below. Exact parser help is available without connecting to
services:

```bash
python -m fingerprinter --help
python -m fingerprinter intake --help
python -m fingerprinter match --help
```

### V1 target CLI

The V1 target tool is `python -m target.cli`. Common form:

```text
python -m target.cli COMMAND [--debug] [--json]
```

`--debug` is supported by every subcommand; `--json` is subcommand-specific.
Commands and arguments:

| Command | Arguments |
|---|---|
| `add MEDIA_PATH` | required `--id ID`, `--version VERSION`; repeat `--metadata KEY=VALUE`; optional `--debug`, `--json` |
| `list` | optional `--debug`, `--json` |
| `get ID` | required `--version VERSION`; optional `--debug`, `--json` |
| `update-metadata ID` | required `--version VERSION`; repeat `--set KEY=VALUE`, `--unset KEY`; optional `--debug`, `--json` |
| `delete ID` | required `--version VERSION`; optional `--debug`, `--json`; removes target and exclusive caches/media as applicable, not historical results |
| `build ID` | required `--version VERSION`; optional `--debug`, `--json`; eagerly processes the full target with no FFmpeg subprocess timeout |
| `clear-db` | `--include-targets` (false); optional `--debug`, `--json`; **destructive** to V1 job/result/retry/match state and optionally target records/caches |
| `reindex` | `--dry-run` (false); optional `--debug`, `--json`; list index rebuild is additive |

`--debug` and `--json` are available on every target subcommand; JSON output
is selected per command. Run target operations using the same
Redis/cache/shared-store environment as the worker. `build` loads the V1
model; other lifecycle operations do not. `clear-db` is not a routine
test/setup command.

### V1 worker and Python APIs

`python -m worker.main` accepts no command-line flags. The V1 producer is
`FingerprintJobSubmitter` (recommended) or low-level `JobProducer`; there is
no supported CLI for job submission. See
[V1 usage in the legacy guide](docs/usage.md) only as supplementary context;
this root guide is current for the identified V1/P9/P10 boundaries.

## Configuration reference

### V1 worker environment

`worker.main.WorkerConfig.from_env()` reads the following environment
variables directly. It does not read a config file. There is no precedence
layer beyond environment value or code default.

| Variable | Type/default | Purpose / validation |
|---|---|---|
| `REDIS_URL` | URL; `redis://localhost:6379/0` | accepts `redis://`, `rediss://`, `unix://`; credentials are redacted from worker configuration logs. |
| `WORKER_CONSUMER_NAME` | string; generated `worker-host-pid-thread` | Redis Streams consumer identity. |
| `WORKER_LEASE_MS` | int; 30000 | >0 claim lease. |
| `WORKER_BLOCK_MS` | int; 5000 | >0 blocking read. |
| `WORKER_RECLAIM_INTERVAL_MS` | optional int; lease value when unset | if set must be >0. |
| `WORKER_MAX_ATTEMPTS` | optional int; unset | **no effect**; max attempts is per submitted job. A warning is logged if set. |
| `EMBEDDING_DEVICE` | `auto` | `auto`, `cpu`, or `cuda`; explicit cuda fails if unavailable. |
| `TORCH_NUM_THREADS` | int; 1 | >=1; important for avoiding CPU process oversubscription. |
| `TARGET_CACHE_PATH` | path; `./target_cache` | host-local cache, not multi-host shared by default. |
| `SHARED_ARTIFACT_STORE_PATH` | path; unset | must be a genuinely shared mount; failure does not silently fall back. |
| `MEDIA_MAX_BYTES` | int; 104857600 | >0 maximum acquisition bytes. |
| `WORKER_RUNTIME_MINUTES` | int; -1 | -1 disables process lifetime limit; otherwise >=0. |
| `WORKER_OBSERVABILITY_INTERVAL_MS` | int; 60000 | >0 health summary cadence. |
| `WORKER_RUN_OUTPUT` | path; unset | if configured, run record and startup marker are written. |
| `WORKER_LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`. |
| `WORKER_LOG_FORMAT` | `auto` | `auto`, `json`, `human`; auto chooses readable terminal vs JSON stream. |

`target.cli` uses `REDIS_URL`, `TARGET_CACHE_PATH`,
`SHARED_ARTIFACT_STORE_PATH`, `EMBEDDING_DEVICE`, and `TORCH_NUM_THREADS`
with corresponding defaults. V1 `MediaAcquirer` SSRF policy is code-wired,
not an operator environment allowlist. By default, it rejects private or
reserved destinations on every redirect hop and its internally created
HTTP session pins sockets to validated addresses. A caller-supplied
`requests.Session` is not modified by the pinning adapter, and setting
`allow_private_networks=True` explicitly disables the protection; neither
is appropriate for untrusted production URLs. The DNS-rebinding regression
tests use scripted DNS answers and a local server, not live external DNS.
See [SSRF / outbound fetch security](docs/architecture/system-architecture.md#7-ssrf-outbound-fetch-security).

### P9/P10 settings

`fingerprinter.config.Settings.from_env()` accepts only `FINGERPRINTER_`
variables. Nested paths use `__`, e.g.
`FINGERPRINTER_SCYLLA__KEYSPACE`; list values are comma-separated. There is
no config file or `.env` loader. Code defaults are active unless overridden.
Models are frozen, reject extra fields, and validate cross-field constraints.

| Section | Fields (default) | Operational meaning |
|---|---|---|
| Root | `host_id=host-1`; `roles=intake,scheduler,encoder,reconciler`; `scratch_dir=var/scratch`; `scratch_max_bytes=20 GiB`; `orphan_grace_s=86400`; `retire_grace_s=604800`; `model_cache_dir=None`; `metrics_port=None`; `due_shards=16`; `intent_shards=16` | identity, role assignment, local scratch, cleanup, optional metrics. Orphan grace must exceed encoder attempt timeout. |
| `scylla` | contact points `127.0.0.1`; port 9042; keyspace `fingerprinter`; replication factor 1; DC unset; request timeout 20s | P9/P10 durable database; `contact_points` comma-separated. |
| `redis` | host `127.0.0.1`; port 6379; DB 0; password unset; prefix `fp:`; stream prefix `events:`; socket timeout 10s | queues, locks, event streams. Prefixes must agree with producers/consumers. |
| `minio` | endpoint `127.0.0.1:9000`; access/secret empty; secure false; buckets `fingerprinter-representations`, `fingerprinter-targets` | artifacts and target media; production credentials must be supplied. |
| `encoder` | device auto; max/cpu batch 256/8; calibration frames/reps/tolerance 64/3/0.05; queue unset; decode/claim/inference threads 3/2/unset; lease 60000ms, GPU lease/renew 30000/10000ms; max attempts/claims 4/10; attempt timeout 14400s and lease/renew 600/120s; RSS 4 GiB; shutdown 60s; backoff 600s; stable_after 500; poll 1s; local-files-only true | bounded decode/inference, retries, GPU ownership and runtime model policy. |
| `acquire` | max bytes 8 GiB; total/connect/read timeout 3600/10/60s; redirects 5; user-agent `fingerprinter/2 (+anti-piracy research)`; SSRF allow CIDRs empty | external HTTP and internal object acquisition; CIDR exceptions are dev/test only. |
| `intake` | reader `encode-requested`; lease TTL/renew 60/20s; batch 200; poll 1s; replay margin 86400s; Crawler outbox retention 1209600s | single leased cursor, gap detection/replay. |
| `publisher` | stream budget 64 MiB; hold alarm 3600s | bounded event publication and consumer-group holds. |
| `reconciler` | intent grace 120s; interval 60s; full pass 86400s | repair and consistency scans. |
| `backfill` | priority 10; max in-flight 200; max/hour unset; queue high-water 1000; buckets 64 | controlled spec re-encoding. |
| `index` | URL `http://127.0.0.1:16333`; API key unset; timeout 10s; instance `dev-1`; collection prefix `fp`; max points 250000; capacity margin 10000; rebuild headroom 0.05; active TTL 5s; search ef 384; oversampling 2; upsert batch 256; mutation lease/attempts 120s/8; clock margin 5s; drain 900s; revalidate 3600s; build lease 300s; verify cycle 3600s; probe queries 8 | Qdrant adapter, capacity and index generation; changing max points from provisional 250000 requires a valid E-R1 report. |
| `matching` | k0 32; max k A/B/equivalence 256/4096/512; floors 0.60/0.70/0.70; min hits 2; cap A 50; page B 200; candidate caps B/equivalence 5000/2000; search/verify timeout 120/30s; verify retries 3; workers 2; task lease 300s; max attempts 20; backoff 30/900s; technique version 2.1; equivalence disabled, rule `equiv/v1`; publish probable false; lowinfo disabled/limit 256/threshold .95/max attempts 5; artifact cache 256 MiB; poll 1s | candidate generation, temporal verification, task scheduling and event gating. Floors are not probabilities; algorithm defaults include provisional values. |
| `matcher_reconcile` | interval 60s; intent grace 120s; scan page 500; target-key page 100; scan rate 200 rows/s; reps/targets intervals 3600/60s; reader batch 200; lease 60s; capacity audit 60s; repair backoff 30/900s and 10 attempts; rerun 5/s; verify page 512 | P10 hint readers, backstop scans and repair work. |

Additional P10 cross-checks fail closed: capacity margin below max points;
drain grace at least `2*active_ttl + task_lease`; `k0 <= k_max`; `page_b <=
cap_b_total`; known equivalence rule; `backoff_base <= backoff_max`.
Consult [config.py](fingerprinter/config.py) for all field constraints.
P9 runtime profile validation also fingerprints model/library/FFmpeg/device;
do not upgrade those components casually or bypass a compatibility refusal.

## Step-by-step workflows

### V1: target and worker

1. Follow [installation.md](installation.md) for Python, Redis, FFmpeg, and
   model setup.
2. Register a local target. Use a media file you are authorized to process.

   ```bash
   python -m target.cli add <path-to-target-video> --id target-123 --version v1
   python -m target.cli build target-123 --version v1
   ```

   `add` is idempotent for identical content; changed content should use a
   new target version. `build` can take as long as decoding the complete
   target and has no FFmpeg timeout. It is a one-shot preprocessing command.

3. Start a worker in the foreground:

   ```bash
   REDIS_URL=redis://localhost:6379/0 python -m worker.main
   ```

   Wait for `worker_ready`; stop with Ctrl-C or SIGTERM. The worker finishes
   the blocking read and leaves unfinished work recoverable.

4. Submit with the Python `FingerprintJobSubmitter` example above. Observe
   the `SubmissionResult`, then check job state/result using Redis commands
   below. A local HTTP media server is not a supported production input:
   the worker's SSRF guard rejects private/loopback addresses.

### V1: inspect job status

The default stream is `fingerprint:jobs:stream:default`; results are
correlated by `job_id` and caller `media_evidence_id`.

```bash
redis-cli XLEN fingerprint:jobs:stream:default
redis-cli XINFO GROUPS fingerprint:jobs:stream:default
redis-cli XPENDING fingerprint:jobs:stream:default fingerprinter-workers
redis-cli HGETALL fingerprint:job:<job_id>:state
redis-cli HGETALL fingerprint:result:<job_id>
redis-cli XRANGE fingerprint:results:stream:default - +
```

Use `integration.outcome.resolve_outcome()` for a higher-level status query.
V1 logs go to stdout; optionally set `WORKER_RUN_OUTPUT=./worker_run.json`.
The marker file indicates startup, while a clean shutdown writes the run
record and removes the marker. No dashboard/metrics service is shipped for
V1.

### P9: first-time backend/spec setup

This requires the sibling Crawler V2 repository and its disposable local
development stack. It is separate from the V1 setup:

```bash
# From the Fingerprinter root; activate the P9-capable environment first.
. scripts/p9-env.sh
python -m fingerprinter migrate
python -m fingerprinter runtime init-profile
python -m fingerprinter spec add --builtin dinov2-seg/2
```

Record the printed spec digest privately and set it as the default:

```bash
python -m fingerprinter spec set-default <spec-digest>
python -m fingerprinter intake init --from-start
```

`--from-now --reason TEXT` is the alternative; it explicitly records an
accepted bootstrap gap and does not read earlier stream events. Intake
initialization is one-time. On stream reset, use the documented gap/reinit
procedure; do not drop/trim shared production streams.

Run P9 services in separate terminals:

```bash
export FINGERPRINTER_ROLES=intake,encoder,reconciler
python -m fingerprinter intake run
python -m fingerprinter scheduler run
python -m fingerprinter encoder run --device cpu
python -m fingerprinter reconcile run
```

The default `FINGERPRINTER_ROLES` includes `scheduler`; the encoder invokes
the scheduler when its work queue is empty. The export above removes that
embedded scheduling role so the separate `scheduler run` process is the only
scheduler. Alternatively, leave `scheduler` in the roles list and omit the
separate scheduler process. List values are comma-separated. CPU encoder is
the minimal P9 path. Ctrl-C/SIGTERM requests drain/shutdown. Encoder exit 0
is normal; controlled exits include
69 (model/CUDA unavailable), 70 (device failure), 73 (GPU lease held), 75
(controlled OOM/RSS/lease exit), and 78 (runtime/config refusal). Diagnose
before restarting; do not blindly loop on 69/70/78.

The crawler side must have migrated its own schema and run media resolver,
probe/hash, and representation consumers. P9 `intake status`, `encode
status`, and `publisher status` provide state; open intake gaps are not
automatically recovered.

### P10: development-only index/matcher

Create a protected Qdrant API key environment file and start the supplied
development service:

```bash
umask 077
printf 'FINGERPRINTER_INDEX__API_KEY=%s\n' "$(openssl rand -hex 24)" > deploy/qdrant/.env
docker compose -f deploy/qdrant/compose.yaml --env-file deploy/qdrant/.env up -d
```

The generated key is written to an ignored local file; do not print or commit
it. Set the same value in `FINGERPRINTER_INDEX__API_KEY` for Fingerprinter
processes, and point `FINGERPRINTER_INDEX__URL` to the running Qdrant endpoint.
Initialize/migrate P10 and inspect parser help before running a state-changing
index command:

```bash
python -m fingerprinter matcher migrate
python -m fingerprinter index status
```

`index rebuild`, `drop`, activation, match reruns, and held publication are
operator state changes. Review the P10 phase guide before running them. The
P10 integration tests use isolated keyspace/prefix/buckets and Qdrant
collections only when `scripts/p10-env.sh` is sourced; do not run them
against production services.

## Models, indexes, and data management

- The V1 `DINOv2EmbeddingEngine` uses `facebook/dinov2-base` at revision
  `f9e44c814b77203eaa57a6bdbbd535f21ede1415`; its image/video output
  dimension is 768. The target cache and segment cache are local by default.
- P9 has explicit immutable specs `dinov2-seg/1`, `/2`, and gated FP16
  variants. Specs define decode/sampling/preprocessing/model/runtime and
  artifact identity. `/2` adds explicit FFmpeg truncation indicators.
  Model weights are local-cache-only at runtime. P9 representation artifacts
  are stored under content/spec identity in MinIO with metadata in Scylla.
- Changing model, preprocessing, sampling, runtime profile, or compatible
  library versions changes representation identity or causes fail-closed
  runtime incompatibility. Re-encode/backfill through the supported
  lifecycle; do not manually edit representation records.
- P10 Qdrant collections are derived from authoritative P9 tables and can
  be rebuilt through P10 index commands; never delete a production collection
  or volume based only on the dev Compose comment.
- V1 target media, result history, P9 Scylla rows/MinIO objects, and source
  media are valuable persistent data. V1 caches may be rebuildable from
  target media, but verify that original media is available before cleanup.
- Scratch is separate from authoritative stores. Inspect disk use and
  configured `scratch_max_bytes`; never remove directories while a worker
  is using them.

## Results and interpretation

V1 distinguishes processing outcome from match verdict. Acquisition,
decode, or inference failures are not `NO_MATCH`. V1 uses temporal segment
matching and a `confidence` output with provisional threshold/configuration;
do not interpret a similarity/confidence value as calibrated probability
without the matching configuration's explicit calibration semantics.

P9 `representation.ready` means an encoding artifact exists; it is not a
match. `encode.failed` means no decision was made. P10 match tasks can be
incomplete or waiting on an index. A positive decision is stored only after
temporal verification and commit protocol. `match.found.v1` publication is
conditional on technique calibration and consumer/stream hold state; current
M3 gate status leaves publication held. Check actual status and
`publisher status` rather than treating a queued representation or candidate
as final evidence.

## Cross-repository integration

There are two different integration contracts:

1. **V1 submission library:** `FingerprintJobSubmitter` is a reusable Python
   API for any producer. It writes V1 job fields (`job_id`,
   `media_evidence_id`, `media_url`, `media_type`, `source_domain`,
   `target_id`, `target_version`, comma-separated `techniques`,
   `max_attempts`, and schema version) to a V1 priority stream. No current
   bridge drains Crawler V2's own evidence-job queue into this V1 API. The
   caller owns submission and result correlation.
2. **P8/P9 event contract:** Crawler V2 media registry emits
   `encode.requested.v1` via its Scylla outbox/Redis Streams relay. The P9
   intake reads the stream `events:encode.requested.v1` (prefix configured
   by `FINGERPRINTER_REDIS__STREAM_PREFIX`) without a consumer group, using a
   leased durable cursor and explicit gap protocol. P9 output events use the
   same event envelope transport; Crawler V2 `media-registry` creates its
   groups and applies `representation.ready.v1` and `encode.failed.v1` to
   its own projection. The `encode.requested` event fields are documented in
   [the shared event contract](../crawler2/contracts/src/antipiracy_contracts/events/fingerprinting.py).

Both projects must point to the same Redis service/stream prefix for P9;
Fingerprinter has its own Scylla keyspace and MinIO buckets. Crawler V2's
P9 cross-service integration test is
`crawler2/tests/integration/fingerprinter/test_p9_cross_service.py`, invoked
through `crawler2/scripts/test-fingerprinter-p9.sh`. It uses throwaway
names and synthetic local media. It is not evidence of a production rollout.
P10 derives its inputs from P9 records; it does not establish a complete,
accepted Crawler→match→evidence workflow.

## Tests and benchmarks

| Category | Location/command | Dependencies / effects | Limitations |
|---|---|---|---|
| V1 unit/integration | `python -m pytest -q`; `python -m pytest tests/test_crash_recovery.py -q` | Test Redis DB 15 by default; fixture calls `FLUSHDB` before/after tests; model cache/FFmpeg needed for media cases | **Never** point test URL at data you need. |
| V1 focused test | `python -m pytest tests/test_worker_main.py::test_config_validation_error_exits_nonzero -q` | pytest; no production service expected by this test | Current suite was not run here. |
| P9 V2 tests | `python -m pytest tests/v2 -q` | P9 test configuration; integration tests are environment-gated; services/model for integration | Use `scripts/p9-env.sh` only with disposable Crawler V2 stack. `RUN_P9_SLOW=1` opts into tests that write about 8–15 GiB of scratch data. |
| P10 tests | `python -m pytest tests/p10 -q` | Unit tests and opt-in integration; real stack/Qdrant for integration | `scripts/p10-env.sh` configures dedicated test keyspace, Redis prefix, buckets, and Qdrant collections; test reset is destructive only to those isolated names. |
| Benchmarks | `python -m benchmarks.bench_embedding`, `bench_matching`, `bench_integration_overhead`, `bench_pipeline`; P9/P10 tools under their benchmark folders | Model, synthetic/generated or licensed datasets, Redis DB 14 for some V1 scripts; P9/P10 hardware/service requirements vary | Redis DB14 is flushed by V1 integration/pipeline benchmark. Never point it at production. P10 gate benchmarks are potentially expensive. |
| Lint/type/security | none configured in this repo's `pyproject.toml` | no supported repository-wide lint/type/security command declared | Do not assume `make lint`, `mypy`, or `bandit` exists. |

### Direct benchmark-script CLI

These are development/research tools, not production worker commands. Run
them from the repository root in the matching environment. The options below
are the arguments declared by the scripts; `-h/--help` is available on the
argparse scripts. Their phase reports describe dataset and acceptance
requirements. No benchmark or data-generation script was run for this guide.

| Script | Syntax and options |
|---|---|
| `python -m benchmarks.bench_embedding` | No CLI options; CPU-only fixed synthetic-video workload; prints measurements. |
| `python -m benchmarks.bench_matching` | No CLI options; deterministic synthetic embedding workload; prints measurements. |
| `python -m benchmarks.bench_pipeline` | No CLI options; fixed local pipeline workload; uses and flushes Redis DB 14. |
| `python -m benchmarks.bench_integration_overhead` | No CLI options; fixed warm-cache integration workload; uses and flushes Redis DB 14. |
| `python -m benchmarks.gen_test_video [--force]` | `--force` overwrites the generated benchmark MP4 fixtures; without it existing fixtures are retained. |
| `python benchmarks/p9-encoder/make_synthetic_corpus.py` | required `--out DIR`; `--count` (60), `--seed` (7), `--max-seconds` (90), `--corrupt` (4), `--audio-only` (2). Writes synthetic media/test metadata. |
| `python benchmarks/p9-encoder/make_real_corpora.py STEP` | `STEP` is `reel`, `c-prec`, `l-pairs`, `soak-1000`, or `verify-soak`; `--workers` (3), `--threads` (4), `--seed` (unset), `--positives` (150), `--synthetic` (30). Corpus creation can acquire/use media; only use approved sources and storage. |
| `python benchmarks/p9-encoder/bench_e2e.py` | required `--corpus DIR`, `--out FILE`; `--device` (`cpu`), `--decode-only`, `--limit N` (unset). |
| `python benchmarks/p9-encoder/bench_inference.py` | required `--corpus DIR`, `--out FILE`; `--frames` (256), `--per-item N` (unset), `--reps` (3), `--cpu-batch N` (encoder setting), `--gpu-batch VALUE` (`calibrated` or integer; default `calibrated`). |
| `python benchmarks/p9-encoder/calibrate.py` | required `--out FILE`; `--runs` (2), `--dtype` (`float32`). |
| `python benchmarks/p9-encoder/precision.py` | required `--clips DIR`, `--out FILE`; optional `--pairs FILE`. |
| `python benchmarks/p9-encoder/soak.py` | required `--corpus DIR`, `--out FILE`; `--devices` (`cuda,cpu`), `--orphan-grace-s` (900), `--attempt-timeout-s` (600), `--attempt-lease-s` (600), `--reconcile-s` (30), `--max-hours` (12), `--keyspace` (`fingerprinter_soak`). Requires isolated P9 backend state. |
| `python benchmarks/p10-index/encode_corpus.py` | required `--corpus DIR`, `--out DIR`; `--device` (`cuda`), `--kinds` (comma-separated `video,clip,target`). |
| `python benchmarks/p10-index/ann_recall.py` | required `--vectors DIR`, `--out FILE`; `--data DIR` (`~/anti_piracy/datasets/p9`), `--clip-queries` (500). |
| `python benchmarks/p10-index/ann_bench.py` | required `--vectors DIR`, `--data DIR`, `--out FILE`; `--qdrant ADDRESS` (unset), `--threads` (4), `--qa` (2000), `--growth-only`, `--qdrant-only NAMES` (comma-separated configuration names; skips FAISS variants). |
| `python benchmarks/p10-index/e_r1_diag.py` | required `--vectors DIR`, `--out FILE`; `--container` (`qdrant-e-r1d`), `--http-port` (26343), `--grpc-port` (26344), `--device` (`sda`), `--caps` (`1536m,640m`), `--queries` (300), `--warmup` (50), `--keep`. By default it removes its named temporary container and volume; review the script and host state before running. |
| `python benchmarks/p10-index/tune_eval.py` | required `--data DIR`, `--vectors DIR`, `--out FILE`; `--workers` (6). |
| `python benchmarks/p10-index/equiv_tune.py` | required `--tune DIR`, `--tune-vectors DIR`, `--heldout DIR`, `--heldout-vectors DIR`, `--out FILE`; `--workers` (4). |
| `python benchmarks/p10-index/direction_b_neighbours.py` | required `--data DIR`, `--vectors DIR`, `--out FILE`. |
| `python benchmarks/p10-index/equiv_density.py` | required `--set DIR`, `--vectors DIR`, `--dense-out DIR`, `--out FILE`; `--dense-s` (1.0), `--device` (`cuda`), `--encode-all`, `--grid`, `--heldout DIR`, `--heldout-dense DIR`, `--workers` (4). |
| `python benchmarks/p10-index/make_eset.py` | required `--data DIR`, `--out DIR`; `--excerpts` (20), `--seed` (8), `--prefix` (`e`), `--avoid MANIFEST` (unset), `--corpus` (`E-SET`). |
| `python benchmarks/p10-index/eset_eval.py` | required `--eset DIR`, `--vectors DIR`, `--out FILE`; `--live`. |
| `python benchmarks/p10-index/lpairs_eval.py` | required `--cache DIR`, `--corpus DIR`, `--reel DIR`, `--out FILE`. |
| `python benchmarks/p10-index/fetch_soak_vectors.py` | required `--bucket NAME`, `--keys FILE` (one object key per line), `--manifest FILE`, `--out DIR`; accesses the configured object store. |
| `python benchmarks/p10-index/p8_set.py COMMAND` | `COMMAND` is `build` or `eval`; required `--out DIR`; optional `--vectors DIR`, `--report FILE`. |
| `python benchmarks/p10-index/v1_e2e_baseline.py` | required `--data DIR`, `--scratch DIR`, `--out FILE`; `--device` (`cuda`), `--redis-url` (`redis://localhost:6379/15`), `--limit N` (unset; harness check, not a gate). Never point its Redis URL at valuable or production state. |
| `python benchmarks/p10-index/capacity_e_r1.py` | required `--vectors DIR`, `--cgroup DIR`, `--out FILE`, `--samples JSONL`; `--copies` (6), `--http-port` (26333), `--grpc-port` (26334), `--api-key` (defaults to `FINGERPRINTER_INDEX__API_KEY`), `--container` (`qdrant-e-r1`), `--steady-s` (300), `--subset N` (unset; harness check only). This is a capacity experiment; review service/container state before execution. |
| `python benchmarks/p10-index/m3_live.py` | required `--data DIR`, `--vectors DIR`, `--out FILE`; `--soak`, `--soak-limit N`, `--drop-rebuild`, `--rerun-all`, `--rerun-targets`, `--exact-rerun`. This mutates live P10 index/match state and is not a routine verification command. |

The P10 helper scripts and benchmarks require phase-specific datasets and
may create/delete Qdrant collections, containers, or derived index state.
They do not establish that the P10 M3 gate has passed. Check each phase
validation document and isolate all services before using them.

The pytest config sets `pythonpath=["."]` and `testpaths=["tests"]`; tests
are pytest tests, not all tests are safe to run against arbitrary services.
`tests/conftest.py` defaults to `FINGERPRINTER_TEST_REDIS_URL` or DB 15 and
flushes that database before and after each test. Use a disposable Redis
instance/db; do not run tests while it contains data. P9 integration tests
are enabled with `RUN_P9_INTEGRATION=1` (set by `scripts/p9-env.sh`) and
P10 integration tests with `RUN_P10_INTEGRATION=1` (set by
`scripts/p10-env.sh`). Those helpers target specially named test resources;
read them before sourcing and never point them at production. `RUN_P9_SLOW=1`
enables large scratch-data tests. Most tests use synthetic/generated
fixtures; benchmark data can include licensed media and should stay in
approved locations.

**Verified here:** `python -m fingerprinter --help` and
`python -m target.cli --help` exited successfully. No test suite, model
inference, backend integration, benchmark, or GPU path was run for this
documentation task. For new failures, use `python -m pytest PATH::TEST -q`;
exit 0 is pass, nonzero is failure, and `-ra` reports skips/xfails where
configured. Pytest output/artifacts are emitted to the terminal/cache unless
the selected script explicitly writes results.

## Troubleshooting

| Symptom | Likely cause | Diagnose | Corrective action | Verify |
|---|---|---|---|---|
| V1 worker exits before ready | invalid worker environment or Redis unreachable | inspect structured `worker_fatal_error`; check URL scheme and `redis-cli ping` | correct `REDIS_URL` or numeric/device config | worker logs `worker_ready` |
| `ModelLoadError` / `ModelUnavailableError` | model snapshot not cached or cache path differs | check local HF cache and `HF_HOME`/`FINGERPRINTER_MODEL_CACHE_DIR` | download exact revision once from trusted network; keep runtime local-only | run runtime fingerprint/profile check, then small approved encode |
| `ffmpeg`/`ffprobe` not found | missing OS media tools | `command -v ffmpeg ffprobe`; `ffmpeg -version` | install distro package; P9 must match runtime profile build | rerun the relevant probe/decoder test |
| unsupported/corrupt media | no video stream, unsupported format, decode failure, or truncation policy | run `ffprobe -v error -show_streams <file>`; check typed failure class | use valid supported media or address source; `/2` rejects only its explicit truncation indicators | retry a bounded fixture |
| Redis pending jobs do not move | worker absent, group mismatch, stream priority mismatch | inspect `XLEN`, `XINFO GROUPS`, `XPENDING`, worker health logs | start matching worker/priority and verify Redis config | pending count/stream state changes |
| target not found / repeated target build failure | target ID/version not registered or media not decodable | `python -m target.cli get ID --version VERSION`; check target path | register correct version; run explicit `build` before long-target jobs | build returns `built` or `already_built` |
| loopback candidate rejected | SSRF guard blocks private/loopback addresses by design | inspect typed acquisition failure and candidate host | use an authorized public media endpoint; private-network allowlist is test-only | candidate acquisition passes without disabling guard |
| CPU is unexpectedly slow | default `TORCH_NUM_THREADS=1` prioritizes multi-process scaling | inspect worker startup snapshot and CPU thread load | if exactly one worker occupies host, benchmark a higher explicit thread count | compare controlled benchmark records |
| CUDA unavailable / encoder exit 69 or 70 | CPU-only torch wheel, driver/device failure, or runtime mismatch | `python -c 'import torch; print(torch.cuda.is_available())'`; inspect controlled exit reason | install compatible CUDA wheel/driver; do not retry device failure blindly | runtime fingerprint and golden check on actual GPU |
| encoder exit 73 | GPU lease already held for host ID | inspect process/host identity and lease | ensure only one active GPU owner; let valid lease expire only after owner is confirmed stopped | encoder starts and acquires lease |
| encoder exit 75 / OOM | batch or single-item memory pressure, RSS, lost lease | controlled-exit report under scratch and metrics | lower batch within supported config, provision memory, investigate lease owner | bounded retry succeeds; avoid repeated poison attempts |
| P9 reports runtime incompatible / exit 78 | model, FFmpeg, device, PyTorch/Transformers or profile differs | `python -m fingerprinter runtime fingerprint`; `runtime check --spec ...` | restore pinned environment or complete compatibility approval/backfill process | `runtime check` allows intended spec |
| P9 intake status has gap | event stream trimmed/reset or reader missed retention | `intake status`, `intake gap list/show` | replay from Crawler V2 outbox if within retention; otherwise record explicit audited unreplayable/accepted gap | status indicates closed replay or explicit accepted state |
| output event held | required consumer group absent or no calibrated publication | `publisher status`, required group registration | start/register the actual consumer; do not manually publish unvalidated matches | status reports unheld; event is consumed |
| Qdrant/index unavailable | Qdrant stopped, API key/URL mismatch, capacity/index generation issue | Compose status/logs, `index status`, service config | start dev Qdrant; align protected API key and URL; investigate capacity refusal | `index status` shows usable active generation |
| P10 task incomplete or waiting | candidate cap/saturation, missing generation, unavailable index or verification failure | `match status SUBJECT --spec DIGEST`, `match summary`, index checks | diagnose recorded reason, index state, and P10 gate before operator rerun | task settles; this does not pass M3 |
| low match recall / unexpected score | provisional matching floors/thresholds or not calibrated dataset | inspect technique version, calibration status, input spec and index generation | do not tune thresholds against holdout; use approved calibration/evaluation procedure | only claim validated behavior for the measured dataset |
| disk exhaustion | scratch/acquisition/intermediates, HF cache, MinIO objects, Qdrant volume | filesystem usage, scratch and bucket metrics | expand storage or safely manage known regenerable caches; preserve source media and authoritative data | rerun a small job and inspect available space |

## Quick reference

```bash
# V1
python -m target.cli --help
python -m target.cli add <path-to-target-video> --id target-123 --version v1
python -m target.cli build target-123 --version v1
python -m worker.main

# P9/P10 help and status
python -m fingerprinter --help
python -m fingerprinter intake status
python -m fingerprinter encode status
python -m fingerprinter publisher status
python -m fingerprinter index status
python -m fingerprinter match summary

# Tests/benchmarks (isolated resources; see safety notes)
python -m pytest tests/test_crash_recovery.py -q
python -m pytest tests/v2 -q
python -m pytest tests/p10 -q
python -m benchmarks.bench_embedding
```

For a clean install, see [installation.md](installation.md). More historical
design and phase evidence is linked from [the README](README.md).
