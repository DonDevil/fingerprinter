# Installation

This covers installing and verifying the fingerprinter only. The crawler is
a separate repository with its own environment and installation guide — see
its own `docs/installation.md`; do **not** try to share a virtualenv or
`requirements.txt` between the two projects, they are intentionally
independent deployments.

## System requirements

- **Python 3.12.** Verified against the checked-in `.venv`
  (`python == 3.12.3`); `pyproject.toml` sets `pythonpath = ["."]` for
  pytest but does not pin a Python version itself. Any recent 3.12.x
  should work; earlier 3.x has not been tested against this codebase.
- **Redis 8** (the `redis` Python client is pinned `>=8,<9`; a Redis 6/7
  server will likely work for the plain commands used here, but Streams
  consumer-group `lag` reporting and `XAUTOCLAIM` semantics were verified
  against Redis 8 — use it unless you have a specific reason not to).
  No Redis modules are required — only core Streams, Hashes, Sets, ZSETs,
  and Lua `EVAL`/scripting (all standard since Redis 5+, no RediSearch/
  RedisJSON/etc.).
- **`ffmpeg` and `ffprobe` on `PATH`.** Required, not optional:
  `acquisition/validation.py` uses `ffprobe` to validate every acquired
  media file, and `embedding/frames.py` uses `ffmpeg` to extract video
  frames for embedding. If either binary is missing, acquisition/embedding
  fail immediately and explicitly (`RuntimeError`/`UnsupportedMediaError`)
  rather than silently degrading.
- **Linux.** `worker/observability.py`'s resource sampling reads
  `/proc/self/status` and `/proc/self/fd` directly — this is documented as
  Linux-only (it returns `None` rather than guessing on any other
  platform). The rest of the codebase has no other Linux-specific
  dependency, but this project's own testing has only been done on Linux.
- **GPU/CUDA: optional, unvalidated.** See
  "[Optional GPU setup](#optional-gpu-setup)" below.

## Fingerprinter installation

```bash
cd /home/darkdevil/Desktop/anti_piracy/fingerprinter

python3.12 -m venv .venv
source .venv/bin/activate

pip install -r requirements.txt -r requirements-dev.txt
```

- `requirements.txt` — runtime dependencies only (`redis`, `requests`,
  `torch`, `torchvision`, `transformers`, `Pillow`, `numpy`).
- `requirements-dev.txt` — adds `pytest` (`-r requirements.txt` plus the
  test runner). Install both for local development; a production worker
  deployment only strictly needs `requirements.txt`.

Confirm `ffmpeg`/`ffprobe` are present:

```bash
ffmpeg -version | head -1
ffprobe -version | head -1
```

### DINOv2 model weights

`embedding/dinov2_engine.py` loads `facebook/dinov2-base` pinned to a
specific snapshot revision
(`f9e44c814b77203eaa57a6bdbbd535f21ede1415`) with
`local_files_only=True` — **by default it will not fetch the model over
the network at runtime**, matching this project's "no surprise network
calls from a production worker" posture. You must pre-populate the
`huggingface_hub` cache once, with network access, before starting a
worker for the first time on a given host:

```bash
python -c "
from transformers import AutoImageProcessor, AutoModel
AutoImageProcessor.from_pretrained('facebook/dinov2-base', revision='f9e44c814b77203eaa57a6bdbbd535f21ede1415')
AutoModel.from_pretrained('facebook/dinov2-base', revision='f9e44c814b77203eaa57a6bdbbd535f21ede1415')
"
```

This downloads into the standard `huggingface_hub` cache (`~/.cache/
huggingface` by default; override with `HF_HOME` if you need a different
location, e.g. for a shared/pre-baked worker image). If a worker starts
without the weights cached, it fails fast at startup with `ModelLoadError`
and an explicit hint (`worker_fatal_error`, `reason=
component_construction_failed`) rather than hanging on an unexpected
network fetch.

## Redis setup

### Local development

```bash
# Debian/Ubuntu
sudo apt-get install redis-server
redis-server --daemonize yes

# or, without installing anything system-wide:
docker run -d --name fingerprinter-redis -p 6379:6379 redis:8
```

Verify connectivity:

```bash
redis-cli ping   # -> PONG
```

### Database / namespace conventions

This project does not use multiple Redis *databases* to separate
concerns in production — all fingerprinter keys share one logical
database, namespaced entirely by **key prefix**: `fingerprint:*`
(`work_queue/keys.py`, `target/keys.py`, `integration/keys.py`). This is
deliberate — see `docs/architecture/system-architecture.md`, "Relationship
to the crawler repository," for why key-prefix separation (not database
separation) is what actually keeps this project's data disjoint from the
sibling crawler repository's `crawler:*`/`evidence:*` keys if the two ever
share one Redis server.

Logical database numbers *are* used to separate environments during
development/testing:

| Use              | Redis URL                        | Configured in |
|------------------|-----------------------------------|----------------|
| Production/dev worker | `redis://localhost:6379/0` (default) | `REDIS_URL` env var, see `docs/usage.md` |
| Automated tests   | `redis://localhost:6379/15` | `tests/conftest.py`, override with `FINGERPRINTER_TEST_REDIS_URL` |
| Benchmarks        | `redis://localhost:6379/14` | `benchmarks/*.py` (`BENCH_REDIS_URL` constant) |

Do not point a real worker's `REDIS_URL` at db 14 or 15 — the test suite
and benchmarks both `FLUSHDB` their database before and after every
run.

### Production Redis

This project has no built-in Redis authentication/TLS configuration beyond
whatever the `REDIS_URL` you provide already encodes (`rediss://` for TLS,
credentials in the URL). Standing up Redis with no authentication and a
publicly reachable port is not a safe default for a production deployment
— configure `requirepass`/ACLs and network-level access control the same
way you would for any other Redis-backed service; this repository does not
document or provide that hardening itself.

## Optional GPU setup

`EMBEDDING_DEVICE` selects the DINOv2 inference device:

| Value  | Behavior |
|--------|----------|
| `auto` (default) | CUDA if `torch.cuda.is_available()`, else CPU |
| `cpu`  | Always CPU |
| `cuda` | CUDA required; raises `DeviceUnavailableError` at worker startup if no CUDA device is visible |

Install `torch`/`torchvision` with CUDA support the normal PyTorch way
(a CUDA-enabled wheel, matching your driver/CUDA toolkit version) if you
want GPU inference — `requirements.txt` does not pin a specific PyTorch
build/index, so `pip install -r requirements.txt` alone typically
resolves to a CPU-only wheel unless your environment/index is already
configured for CUDA wheels.

**GPU status: code path implemented, correct by inspection, `REQUIRES
GPU VALIDATION`.** Per
`docs/architecture/phase-13-production-hardening.md`, §5 ("GPU audit"):
device selection and per-inference GPU hygiene (explicit `.to(device)`
calls, `torch.cuda.empty_cache()` after freeing tensors) are implemented,
but **no GPU benchmark numbers exist anywhere in this repository** and
this project's own multi-process CPU-thread-pinning fix
(`TORCH_NUM_THREADS`) is a no-op on the GPU path. Do not treat
`EMBEDDING_DEVICE=cuda` as production-validated; treat it as
implemented-but-unverified until it has actually been run against real
hardware and measured.

## Verification

Before running a real crawl-driven job, verify the install end to end
with a **small, local, no-network test** — the automated test suite does
exactly this against tiny fixture files
(`tests/fixtures/tiny_video.mp4`, `tests/fixtures/tiny_image.png`) served
over loopback HTTP, and is the fastest way to catch a broken install:

```bash
source .venv/bin/activate
redis-cli ping                     # -> PONG (Redis reachable)
ffmpeg -version >/dev/null && echo ok
ffprobe -version >/dev/null && echo ok

python -m pytest -q               # see docs/usage.md for the full test workflow
```

If the model weights described above are not yet cached, the tests that
exercise `DINOv2EmbeddingEngine` will fail with `ModelLoadError` — that
failure is expected and tells you exactly what step to go back to.

A full `269 passed, 0 failed` run (this repository's current baseline —
see `docs/development.md`) is the signal that acquisition, embedding,
matching, target caching, the Redis job contract, and the worker
observability layer are all wired correctly in your environment.

**What this verification does and does not prove:** it proves your local
environment (Redis, ffmpeg, model weights, Python deps) is correctly
wired end to end on one host. It does **not** exercise a real multi-host
deployment, real GPU hardware, or the crawler integration boundary — see
`docs/architecture/system-architecture.md`, §10, for what has and hasn't
been validated.
