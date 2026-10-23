# Fingerprinter installation

Fingerprinter contains separate V1 and P9/P10 workflows. Installing the V1
worker does not install the Crawler V2 services or P9/P10 service stack.
Use a separate virtual environment from Crawler V2; the dependency versions
are not shared.

## Supported paths and status

| Path | Purpose | Status |
|---|---|---|
| V1 local CPU worker | Redis-backed candidate jobs and local DINOv2 matching | Supported by current V1 source/docs; clean-machine install not executed here |
| P9 CPU encoder | Consume Crawler V2 encode events and publish representations | Implemented; requires shared Crawler V2 backend/contracts and runtime profile |
| P9 GPU encoder | CUDA encoding | Code and phase validation exist; production deployment not established; only run after real GPU/runtime check |
| P10 matcher | Qdrant-backed matching of P9 representations | Implemented but M3 not passed; not production-ready |
| Docker | Qdrant development index only | `deploy/qdrant/compose.yaml`; no all-in-one Fingerprinter app image/Compose stack |

## System prerequisites

- Linux is the documented/tested target; no other OS is certified.
- Python 3.12 is the recorded runtime. `pyproject.toml` does not declare a
  Python floor or packaging/build configuration; installation is via the
  checked-in requirements files.
- Git, pip, and a Python 3.12 virtual environment.
- `ffmpeg` and `ffprobe` for V1 acquisition/decode and P9 media processing.
- Redis for V1 and P9. P9 additionally requires ScyllaDB, MinIO, and the
  shared `antipiracy-contracts` package. The repository supplies no Redis,
  ScyllaDB, or MinIO Compose services of its own.
- DINOv2 model snapshot in Hugging Face cache. P9 defaults to local-files-only
  model access; install/download once before offline runtime.
- For P10, Qdrant 1.15.1 dev service and protected API key.
- CPU is sufficient for the V1 CPU path and P9 FP32 CPU path. There are no
  established minimum hardware recommendations. GPU needs a CUDA-compatible
  PyTorch wheel and working driver; install separately and validate on
  actual hardware.

## V1 CPU installation

1. Obtain and enter the repository:

   ```bash
   git clone <fingerprinter-repository-url> fingerprinter
   cd fingerprinter
   ```

   If already checked out, use that repository root.

2. Verify Python and package tools:

   ```bash
   python3.12 --version
   python3.12 -m pip --version
   ```

   The documented runtime is Python 3.12. Check the local interpreter
   actually used for the venv, not only the system default.

3. Install FFmpeg and FFprobe. On Debian/Ubuntu:

   ```bash
   sudo apt-get update
   sudo apt-get install ffmpeg
   ```

   Verify both commands:

   ```bash
   ffmpeg -version
   ffprobe -version
   ```

4. Create and activate an independent virtual environment:

   ```bash
   python3.12 -m venv .venv
   source .venv/bin/activate
   python --version
   ```

5. Install runtime and test dependencies:

   ```bash
   python -m pip install --upgrade pip
   python -m pip install -r requirements.txt -r requirements-dev.txt
   ```

   `requirements.txt` installs V1 runtime dependencies; `requirements-dev.txt`
   includes it and adds pytest. The ML dependencies are not pinned to a
   complete reproducible set. Save a known-good environment for deployment;
   do not upgrade model/runtime dependencies on a P9 installation without
   its runtime compatibility procedure.

6. Start a disposable local Redis for development. With Docker:

   ```bash
   docker run --rm --name fingerprinter-redis \
     -p 127.0.0.1:6379:6379 redis:8
   ```

   Keep this foreground terminal open. Alternatively use an isolated
   system-managed Redis service. Do not expose unauthenticated development
   Redis to an untrusted network. Verify from another terminal:

   ```bash
   redis-cli -h 127.0.0.1 -p 6379 ping
   ```

   Expected response is `PONG`.

7. Pre-cache the exact V1 model snapshot with network access. This is a
   one-time download; runtime code uses local-only loading by default.

   ```bash
   python -c "from transformers import AutoImageProcessor, AutoModel; model='facebook/dinov2-base'; rev='f9e44c814b77203eaa57a6bdbbd535f21ede1415'; AutoImageProcessor.from_pretrained(model, revision=rev); AutoModel.from_pretrained(model, revision=rev)"
   ```

   Hugging Face uses its standard cache (commonly `~/.cache/huggingface`);
   set `HF_HOME` before downloading if a different protected cache location
   is needed. This command needs external network access. Do not put model
   credentials in the repository.

8. Check imports and the worker parser-free entrypoint prerequisites:

   ```bash
   python -c "import redis, requests, torch, torchvision, transformers, PIL, numpy; print('imports ok')"
   python -m target.cli --help
   ```

   The target help command does not connect to Redis. The worker has no
   `--help` parser and starts a process; do not use it as an import check.

9. Set the worker environment and start it in the foreground:

   ```bash
   export REDIS_URL=redis://localhost:6379/0
   export EMBEDDING_DEVICE=cpu
   export TARGET_CACHE_PATH=./target_cache
   python -m worker.main
   ```

   Success includes a `worker_ready` event. Stop with Ctrl-C/SIGTERM; the
   worker requests graceful shutdown. V1 does not include database migration
   because its durable queue/result state is Redis.

## P9/P10 environment installation

P9/P10 is independent of the V1-only requirements and shares infrastructure
with the sibling Crawler V2 project. Do not assume that the V1 installation
above is sufficient.

1. Install the Crawler V2 repository in a separate environment and start its
   local development services. Follow
   [Crawler V2 installation](../crawler2/installation.md); those services
   include Redis, ScyllaDB, and MinIO.

2. Create a separate Fingerprinter environment and install V1 base packages
   plus P9 direct dependencies:

   ```bash
   cd <fingerprinter-repository>
   python3.12 -m venv .venv-p9
   source .venv-p9/bin/activate
   python -m pip install --upgrade pip
   python -m pip install -r requirements.txt -r requirements-p9.txt -r requirements-dev.txt
   ```

   `requirements-p9.txt` describes additional direct P9 dependencies; its
   comments recommend preserving the existing environment with a constraints
   snapshot. `requirements-dev.txt` adds pytest and the repository's test
   tooling. The full `requirements-p9.lock` records one P9 runtime and
   includes a CUDA PyTorch wheel; it is not a standalone clean-install
   recipe. Follow the runtime-profile process in `docs/v2-encoder.md` before
   using any locked/reproducible P9 environment.

3. Install the shared contract package from a checked-out, compatible
   Crawler V2 repository. With the sibling layout shown in this workspace:

   ```bash
   python -m pip install -e ../crawler2/contracts
   ```

   Confirm that the package version matches the contract version required
   by the current Fingerprinter source. Do not independently alter event
   schemas.

4. Install/verify the FFmpeg build required by the P9 runtime profile and
   pre-cache the exact DINOv2 revision from step 7 of the V1 procedure.
   P9 fails closed if runtime profile or model compatibility does not match.

5. Configure a local test backend. The project-provided helper sources
   `scripts/p9-env.sh` and uses the Crawler V2 development stack with
   throwaway keyspace, prefixes, and bucket names:

   ```bash
   . scripts/p9-env.sh
   ```

   It must be sourced in the same shell where commands/tests run. Read the
   script before use; it sources Crawler V2 `.env`, uses Docker and `sg
   docker` to discover Scylla's container address, and sets test-only
   keyspace/prefix/bucket names. Do not use its throwaway names for
   production.

6. Run the first-time database/spec procedure from
   [usage.md](usage.md#p9-first-time-backendspec-setup): `migrate`,
   runtime profile initialization, spec registration/default selection, and
   intake initialization. `runtime init-profile`/`runtime check` are
   required to confirm the actual environment; code presence alone is not
   enough.

7. Optional P10 Qdrant service: create a protected local API-key file and
   start the supplied Qdrant Compose project:

   ```bash
   umask 077
   printf 'FINGERPRINTER_INDEX__API_KEY=%s\n' "$(python -c 'import secrets; print(secrets.token_hex(24))')" > deploy/qdrant/.env
   docker compose -f deploy/qdrant/compose.yaml --env-file deploy/qdrant/.env up -d
   ```

   Export that same value as `FINGERPRINTER_INDEX__API_KEY` into
   Fingerprinter processes and configure the URL. The key is not displayed
   by the command. Qdrant is derived P10 state; do not treat index deletion
   as a normal installation reset.

## GPU installation and verification

GPU is optional. `requirements.txt` does not select a CUDA-specific PyTorch
wheel. Install the PyTorch build appropriate for the machine's driver using
the official PyTorch instructions; do not guess a CUDA version from the
repository. Then:

```bash
python -c "import torch; print(torch.__version__); print(torch.cuda.is_available())"
python -m fingerprinter runtime fingerprint --device cuda
```

For P9, follow with the approved runtime-profile/GPU golden admission
procedure (`runtime init-profile`, `runtime admit-cuda`, `runtime check`).
For V1, set `EMBEDDING_DEVICE=cuda` and run a small authorized inference.
`torch.cuda.is_available()` only confirms device visibility, not model
correctness or usable memory. P9/P10 worker exit codes and golden checks are
described in [usage.md](usage.md). Do not mark GPU support verified until
the model actually runs on the target hardware.

## Installation verification

### V1 CPU checklist

```bash
python --version
command -v ffmpeg ffprobe
redis-cli -h 127.0.0.1 -p 6379 ping
python -c "import torch; print('cuda available:', torch.cuda.is_available())"
python -m target.cli --help
```

Then verify model load with the exact model-caching command above. Register
and build a small authorized target. For candidate processing, use a test
fixture-driven test rather than disabling SSRF protection to reach loopback.
Success is verified by a target `build` result and, for a job, the Redis
result/state records.

### P9/P10 checklist

- Check required `FINGERPRINTER_*` variables without printing credentials.
- Confirm shared service health from Crawler V2:
  `docker compose exec app crawler2-check --wait 180`.
- Confirm contract import: `python -c "import antipiracy_contracts; print('ok')"`
- Confirm model/runtime compatibility: `python -m fingerprinter runtime
  fingerprint --device cpu` and `runtime check --spec <digest> --device cpu`.
- Confirm FFmpeg/FFprobe versions; compare with runtime profile.
- Confirm P9 schema and buckets using `python -m fingerprinter migrate` only
  in the intended disposable keyspace/bucket.
- Run a small Crawler V2 P9 cross-service fixture test only with the
  documented isolated test names; do not use production data.
- For P10, verify Qdrant connectivity and API-key alignment, then inspect
  `python -m fingerprinter index status`.

The full P9/P10 workflow needs services and was not executed during this
documentation task.

## Tests and safe recovery

Before tests, read [usage.md](usage.md#tests-and-benchmarks). V1 test fixtures
flush Redis DB 15 before and after each test; set
`FINGERPRINTER_TEST_REDIS_URL` to a disposable Redis DB, never a production
or valuable database. P9/P10 service tests use dedicated configuration and
may write/drop test-specific keyspaces or collections. Do not run them
against a shared deployment.

Code updates and package reinstall are separate from data migrations:

```bash
source .venv/bin/activate
python -m pip install -r requirements.txt -r requirements-dev.txt
```

For P9, use its separate virtual environment and its runtime compatibility
gate. Never casually upgrade torch, transformers, numpy, Pillow, FFmpeg, or
model weights in a P9 deployment. A migration (`python -m fingerprinter
migrate`) changes the selected Scylla schema/bucket setup; run only against
the intended environment with a backup/recovery plan.

Safe-to-regenerate artifacts depend on their owner: P10 Qdrant index
collections can be rebuilt from P9 authoritative records, but Crawler V2
media, P9 representation records/objects, V1 target originals, and result
history are not disposable caches. Preserve source media and persistent
backend volumes. Do not use `target clear-db`, `FLUSHDB`, `docker compose
down -v`, or manual bucket deletion as routine recovery.

## Troubleshooting

| Symptom | Diagnose | Corrective action / verification |
|---|---|---|
| Python import/dependency error | Confirm active venv and package list; V1 vs P9 dependencies differ | Install the matching requirements in the correct venv; rerun import check |
| Redis connection failure | `redis-cli ping`; inspect `REDIS_URL` or `FINGERPRINTER_REDIS__*` | Start/reach the intended service and align host/port/DB/prefix; verify ping |
| P9 Scylla or MinIO unavailable | `crawler2-check`, service logs, P9 endpoint/keyspace/bucket settings | Wait until ready, correct endpoint/credentials, rerun a non-destructive status check |
| Missing FFmpeg | `command -v ffmpeg ffprobe`, `ffmpeg -version` | Install tools; for P9 also match the recorded runtime profile |
| Model load failure | verify exact DINOv2 ID/revision and HF cache | download exact pinned model once; run local-only load check |
| Unsupported or corrupt media | `ffprobe -v error -show_streams`; inspect `encode.failed.failure` | provide valid video; use failure type to determine source vs decode cause |
| CUDA unavailable | `torch.cuda.is_available()`, driver and installed torch build | install compatible wheel/driver; run golden/runtime compatibility check on actual GPU |
| CUDA OOM / slow encoding | controlled exit report, RSS/VRAM, batch and thread config | reduce supported batch/concurrency or increase resources; retry after diagnosing |
| Intake gap / status unhealthy | `python -m fingerprinter intake status`; `intake gap list/show` | replay from Crawler outbox within retention; expired data requires audited gap acceptance, not an assumed recovery |
| P9 event does not appear | `encode status`, intake status, publisher status, stream prefix and consumer groups | align shared Redis stream prefix, register actual consumer before publishing, use supported replay path |
| Qdrant unavailable | Compose status/logs; URL/API key settings | start the dev service, keep key aligned and secret; verify index status |
| Index generation incompatible | `index status`, `index check --spec ... --role ...` | rebuild using current compatible spec; do not drop active generation without the phase runbook |
| Insufficient disk | scratch, HF cache, MinIO buckets, Qdrant volume | expand or safely relocate known cache; preserve authoritative artifacts and data |
| Test suite fails to reach Redis | `FINGERPRINTER_TEST_REDIS_URL`; ensure dedicated DB | start isolated Redis and point tests to disposable DB; remember suite flushes it |

## Quick start

Minimal V1 CPU install:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt -r requirements-dev.txt
sudo apt-get install ffmpeg
docker run --rm --name fingerprinter-redis -p 127.0.0.1:6379:6379 redis:8
```

In another terminal, verify Redis, pre-cache the DINOv2 model snapshot,
register/build an authorized target, and run `python -m worker.main`.
This is a working V1 CPU environment when those steps succeed; it is not the
integrated P9/P10 deployment. For P9/P10, use the independent-service setup
above and the detailed [usage guide](usage.md).
