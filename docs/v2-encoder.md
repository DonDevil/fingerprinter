# V2 encoder package (`fingerprinter/`, crawler2 phase P9)

Local guide for developing, testing and running the V2 encoder package. The
package encodes media that crawler2 asks for into DINOv2 segment
representations, keeps a representation registry and a target manager, and
publishes the results. It does **no matching**: matching is a later phase
(P10).

Source of truth for anything that crosses the service boundary is
**crawler2**. That covers the approved design, the decisions, the ADRs, the
gate evidence and the cross-service runbook (configuration table, intake-gap
procedure, target and spec lifecycle). This file covers only what you need
locally in this repository.

| Topic | crawler2 document (`~/anti_piracy/crawler2/docs/…`) |
|---|---|
| Design (source of truth) | `phases/p09-fingerprinter-encoder/design.md` |
| Decisions OF-1…OF-5, risks | `phases/p09-fingerprinter-encoder/decisions.md` |
| Gate status and limitations | `phases/p09-fingerprinter-encoder/exit.md` |
| Measured evidence | `phases/p09-fingerprinter-encoder/validation.md` |
| Corpora and datasets | `phases/p09-fingerprinter-encoder/data-preparation.md` |
| Operations runbook | `architecture/fingerprinter-encoder.md` |
| ADRs | `adr/ADR-025…027` |

**Status:** P9 is complete for its approved scope, with the limitations
listed in crawler2's `exit.md` §6. It is **not production-deployed**: it runs
against crawler2's development compose stack and is not wired into any
deployed stack.

## 1. Boundaries

- **Separate from V1.** The V1 worker (`worker/`, `embedding/`, `matching/`,
  `target/`, `work_queue/`, `integration/`) is unchanged and does not use this
  package. The package reuses only `acquisition.ssrf_guard` as code. The
  precision benchmark imports V1's `matching.matcher` as a frozen oracle.
- **crawler2 contracts.** The package consumes crawler2's
  `encode.requested.v1` events and publishes `representation.ready.v1`,
  `encode.failed.v1`, `target.registered.v1` and `target.retired.v1`. The
  event shapes and IDs come from the shared contracts package
  `antipiracy-contracts` (crawler2 `contracts/`, P1/P8, frozen). Never edit
  those contracts from this repository.
- **Own durable state.** The package has its own Scylla keyspace
  (`fingerprinter`) and MinIO buckets (`fingerprinter-representations`,
  `fingerprinter-targets`). Its Redis keys use the prefix `fp:` and can be
  rebuilt. It shares crawler2's services but none of crawler2's tables.

## 2. Modules

| Area | Modules |
|---|---|
| Identity and runtime | `spec.py` (canonical spec documents `dinov2-seg/1`, `/2` and their gated `-fp16` variants), `specs.py` (spec lifecycle), `runtime.py` (runtime profile `rt-1`, fail-closed checks), `config.py`, `errors.py` |
| Decode and model | `media.py` (probe, sequential ffmpeg decode, truncation indicators), `model.py` (DINOv2, pinned revision), `artifact.py` (`fp-seg-npz/v1`), `calibration.py` (GPU batch `throughput/v1`) |
| State and lifecycle | `db.py` (schema, `migrate`), `coord.py` (Redis, clocks), `store.py`, `objects.py` (MinIO), `requests.py`, `intake.py` (group-less reader with gap protocol), `commit.py` (durable intents), `publish.py` (held publication), `reconcile.py` (intents, sweeper) |
| Encoding | `sources.py` (typed acquisition), `encoder.py` (CPU/GPU encoder process), `services.py` (wiring) |
| Targets and backfill | `targets.py`, `backfill.py` |
| Interfaces | `cli.py`, `__main__.py`, `metrics.py` |

## 3. Environment

Requirements:

- Python 3.12;
- ffmpeg 6.1.1-3ubuntu5 (Ubuntu 24.04 system package);
- the existing `env/` virtual environment, with the P9 dependencies added;
- the model `facebook/dinov2-base` at revision
  `f9e44c814b77203eaa57a6bdbbd535f21ede1415`, already in the local Hugging Face
  cache. The model is loaded with `local_files_only=True`; pre-caching works as
  for V1 (`docs/installation.md`), but with this revision;
- for the GPU encoder: an NVIDIA driver that works with the CUDA 13.0 torch
  wheel.

The two dependency files have different roles:

| File | Role |
|---|---|
| `requirements-p9.txt` | The **direct** P9 dependencies to install on top of `env/`. Install them with a constraints file so that no package already present is upgraded. |
| `requirements-p9.lock` | A `pip freeze` of the complete environment, captured 2026-10-10. It is the pip part of runtime profile `rt-1`. It is a record to compare against and restore from, not an installable recipe on its own: torch comes from the CUDA 13.0 wheel index (`2.13.0+cu130`), and `antipiracy-contracts` comes from crawler2's checkout. |

```bash
cd ~/anti_piracy/fingerprinter
env/bin/pip freeze > /tmp/before.txt
env/bin/pip install -c /tmp/before.txt -r requirements-p9.txt
env/bin/pip install ~/anti_piracy/crawler2/contracts      # antipiracy-contracts 1.1.0
```

The lock file alone does not reproduce `rt-1`. The runtime profile also pins
the Python version, the ffmpeg build, the model revision and the model file
hashes. It records the torch build and the CUDA device as well.

At start-up the encoder compares its environment with the profile. If they
differ, the encoder **refuses** the spec (exit 78). After an upgrade of torch,
transformers, numpy, Pillow or ffmpeg, the encoder therefore refuses work
until a new runtime profile passes the compatibility gate or a new spec has
been backfilled (crawler2 ADR-027).

## 4. Configuration and database

Settings are read from environment variables named `FINGERPRINTER_<FIELD>`,
with `__` between a section and its field. Examples:
`FINGERPRINTER_SCYLLA__KEYSPACE` and `FINGERPRINTER_ENCODER__MAX_RSS_BYTES`.
The fields are defined in `fingerprinter/config.py`. The values and their
reasons are in the crawler2 runbook, §4. Start-up refuses a configuration in
which `orphan_grace_s` is not above `encoder.attempt_timeout_s`.

For development and tests, source `scripts/p9-env.sh`:

```bash
. scripts/p9-env.sh
```

It reads crawler2's `.env` and points the package at crawler2's compose stack
(Scylla, Redis, MinIO), using **throwaway** names that never touch a real
deployment:

- keyspace `fingerprinter_it_p9`;
- buckets `fp-it-*`;
- Redis keys `fpit:*`.

It also sets `RUN_P9_INTEGRATION=1`. Set any `FINGERPRINTER_*` variable before
sourcing to override that value. A real deployment exports its own
`FINGERPRINTER_*` values and does not use this script.

First-time setup of an installation:

```bash
python -m fingerprinter migrate                            # keyspace, tables, buckets, stream registry
python -m fingerprinter runtime init-profile               # record rt-1 for this host (cpu + cuda variants)
python -m fingerprinter runtime admit-cuda                 # CUDA FP32 golden check against the CPU golden
python -m fingerprinter spec add --builtin dinov2-seg/2    # FP32 default spec; prints its digest
python -m fingerprinter spec set-default <digest>
python -m fingerprinter intake init --from-start           # or: --from-now --reason "…" (records a gap)
```

Use `env/bin/python` when the environment is not activated.

## 5. Running the service roles

The general form is `python -m fingerprinter [--json] [--operator NAME] <tool>
<command>`. Every state-changing command writes an audit record.

| Role | Command |
|---|---|
| intake | `python -m fingerprinter intake run` (`--until-idle` for tests) |
| scheduler | `python -m fingerprinter scheduler run` |
| GPU encoder | `python -m fingerprinter encoder run --device cuda` (one per host) |
| CPU encoder | `python -m fingerprinter encoder run --device cpu` (FP32 only) |
| reconciler, publisher, sweeper | `python -m fingerprinter reconcile run` (`--once` for one pass) |

Other tools:

- `intake status|gap …|accept-gap|reinit`
- `encode status|retry`
- `encoder recalibrate`
- `reconcile full|sweep`
- `publisher status|register-consumer|republish`
- `target register|show|list|retire|purge`
- `spec add|list|enable|set-default|deprecate|retire|retire-sweep`
- `backfill create|run|pause|resume|cancel|status`
- `runtime fingerprint|check|approve-compat`

Run `python -m fingerprinter <tool> --help` for the options, and see the
crawler2 runbook, §5, for the procedures.

To serve Prometheus metrics on `/metrics` from the long-running roles, set
`FINGERPRINTER_METRICS_PORT`.

### Shutdown, recovery and exit codes

| `encoder run` exit | Meaning | What to do |
|---|---|---|
| 0 | normal exit. On SIGTERM the encoder stops claiming and lets in-flight attempts finish within `encoder.shutdown_grace_s`; the rest go back to the queue. | — |
| 75 | controlled exit: single-item GPU OOM, GPU lease lost, or the RSS limit was reached | restart |
| 70 | device error | restart only after a device health check |
| 73 | another process owns this host's GPU | do not restart |
| 69 | CUDA or the model is unavailable | fix the environment |
| 78 | runtime or golden-check refusal | fix the environment; do not retry blindly |

Each exit appends a line to `<scratch_dir>/exit_reports.jsonl`.

Crash recovery needs no manual step:

- expired leases put the work back into the due index;
- the reconciler completes or cleans up every durable intent;
- the sweeper deletes only unreferenced objects older than `orphan_grace_s`.

A storage error in one `reconcile run` pass is logged as
`reconcile_pass_storage_unavailable`, and the loop retries at the next
interval (fix X-10). `reconcile run --once` still exits non-zero on that
error. Intake gaps are never closed automatically; see crawler2 ADR-025 and
the runbook, §5.

## 6. Tests

```bash
cd ~/anti_piracy/fingerprinter
env/bin/python -m pytest -p no:cacheprovider tests/v2 -q    # without the stack: unit tests only, the rest skip
. scripts/p9-env.sh                                         # needs crawler2's compose stack running
env/bin/python -m pytest -p no:cacheprovider tests/v2 -q    # unit + real stack + GPU + simulated two-host
RUN_P9_SLOW=1 env/bin/python -m pytest -p no:cacheprovider \
    tests/v2/test_sources.py tests/v2/test_targets.py -k "8gib or 5gib"   # opt-in; writes 8 GiB / ~15 GiB
env/bin/python -m pytest tests --ignore=tests/v2 -q        # V1 suite (Redis db 15)
```

Which tests run where:

- **Integration tests** (marked with `integration` in `tests/v2/conftest.py`)
  run only with `RUN_P9_INTEGRATION=1`. Each test wipes the test keyspace and
  uses its own Redis prefixes and its own buckets.
- **GPU tests** skip when CUDA is unavailable.
- **The two `RUN_P9_SLOW` tests** need an idle disk.

`pyproject.toml` collects all of `tests/`, so a bare `pytest` runs the V1
suite and `tests/v2` together.

The cross-service test lives in crawler2: run
`scripts/test-fingerprinter-p9.sh` there. It runs this package against
crawler2's own P8 requester and projection.

On a laptop with systemd-oomd, run the full suite, the slow tests and all
benchmarks as their own memory-capped `systemd-run --user` unit, with output
going to a log file (crawler2 runbook, §7).

## 7. Benchmarks and corpora

The scripts are in `benchmarks/p9-encoder/`:

| Script | Purpose |
|---|---|
| `make_real_corpora.py {reel\|c-prec\|l-pairs\|soak-1000\|verify-soak}` | Builds the gate corpora from the licensed films (S-SRC). The SOAK-1000 build checks every expected-failure item against the real decoder and refuses to write a manifest if any item fails differently. `verify-soak` re-runs that check. |
| `make_synthetic_corpus.py` | Seeded synthetic corpus for harness checks. Never counts for a gate. |
| `calibrate.py` | B-CAL: GPU batch calibration, run the way the encoder runs it |
| `bench_inference.py` | B-GPU-a: inference throughput, GPU vs CPU on the same tensors (G-5) |
| `precision.py` | B-PREC: FP16 vs CPU FP32 precision (G-2) and labelled decisions under OF-4 (G-3) |
| `bench_e2e.py` | B-CPU / B-GPU-b / B-DEC: end-to-end throughput against the real stack (G-6) |
| `soak.py` | B-SOAK: the observable soak harness (G-8 on SOAK-1000) |

Exact command lines are in the crawler2 runbook, §7.

A report counts towards a gate only for the gate's approved input set:
C-PREC, L-PAIRS or SOAK-1000. For any other corpus the report says
`gate_eligible: false` / `NOT MEASURED`.

**Where things live:**

- **Media, corpora, manifests and run logs** are in
  `~/anti_piracy/datasets/p9/`. They are **never in git**.
- **Small result JSONs** are in `benchmarks/p9-encoder/results/`, and they
  are versioned as evidence. Superseded results stay there as history:
  `soak-1000.json` is the failed run 2, and `b-prec-l-pairs.json` is the
  strict G-3 reading from before OF-4. The current results are
  `soak-1000-run5.json` and `b-prec-l-pairs-of4.json`.

## 8. Precision policy

- **FP32 is the default** (spec `dinov2-seg/2`). CUDA FP32 and the CPU
  fallback give the same representations: cosine 1.0 on C-PREC.
- **FP16 (`dinov2-seg/1-fp16`, `dinov2-seg/2-fp16`) stays `gated` and must
  not be enabled.** Its
  precision gate G-2 failed on C-PREC: 7 of 10,267 segments had a cosine below
  0.999, with a minimum of 0.990623. G-3 passes only under OF-4, and that does
  not ungate FP16.
- FP16 and FP32 are separate identities, so a representation is never
  relabelled from one to the other.
- `spec enable` refuses unless the evidence shows both gates as `PASS`.

## 9. Known limitations and deferred work

The complete list is in crawler2's `exit.md` §6. The ones you are most likely
to run into locally:

- **Truncation detection is partial.** `dinov2-seg/2` detects only
  ffmpeg's explicit `matroska_file_ended_prematurely` and `mov_partial_file`
  reports. MP4 files that ffmpeg reports only as "packet corrupt" are
  accepted.
- **Soak coverage.** One passing SOAK-1000 run on one host (run 5). Controlled
  exits and single-item OOMs never occurred during it, so soak criteria 3 and
  4 were met only vacuously. Recovery from those events is shown by targeted
  tests.
- **Multi-machine.** Tested only as a simulated two-host setup: processes on
  one machine. Real multi-machine validation and the DNS-rebinding fix are
  deferred to P14.
- **Not implemented or not wired:**
  - NVDEC (decoding runs on the CPU);
  - metrics scraping and alerts;
  - matching (P10).
- **Initial parameter values.** Timeouts, leases and batch parameters are
  approved starting values that have not been tuned with production data.
