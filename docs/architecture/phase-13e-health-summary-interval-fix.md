# Phase 13E — Health-summary interval-gate fix

## Status: IMPLEMENTED, VERIFIED (single-host; no multi-host dependency)

---

## 1. Problem

`ObservingWorkerObserver.on_loop_tick()` (`worker/observability.py`, added
in Phase 13C) is supposed to emit one `worker_health` log event per
`health_interval_s` (default `WORKER_OBSERVABILITY_INTERVAL_MS` = 60s), and
explicitly **not** on every loop iteration — Phase 13C's own documentation
(`docs/architecture/phase-13-production-hardening.md` §20, "Health
interval") states this in so many words: "emits one `worker_health` event
— never one log line per job or per loop iteration."

That guarantee was untrue as shipped. `tests/test_worker_observability.py::
test_health_summary_not_emitted_before_interval_elapses` — asserting that
two `on_loop_tick()` calls with a 9999-second interval emit nothing —
**failed on `main`** before this fix (VERIFIED, this session, before any
code change: `1 failed, 268 passed`).

This was not a new discovery: Phase 13D's own results section
(`docs/architecture/phase-13d-distributed-target-artifacts.md` §12) already
recorded the same failure during its own full-suite run and reproduced it
on the pre-13D commit (`c3bfdd5`) via `git stash`, correctly identifying it
as pre-existing and unrelated to that phase's scope. It was documented but
never root-caused or fixed — this phase closes that gap.

---

## 2. Existing behavior (before this fix)

```python
self._started_monotonic = time.monotonic()
self._started_at = time.time()
self._last_health_emit = 0.0          # <-- bug

def on_loop_tick(self) -> None:
    now = time.monotonic()
    if now - self._last_health_emit < self._health_interval_s:
        return
    self._last_health_emit = now
    self.emit_health_summary()
```

`time.monotonic()` returns a value from an arbitrary reference point (on
Linux, seconds since the last boot, per CPython's implementation using
`CLOCK_MONOTONIC`) — it is **not** epoch-relative and is never guaranteed
to be small. Comparing it against a hardcoded `0.0` baseline means
`now - 0.0` equals `time.monotonic()` itself, which on any real machine
(uptime almost always exceeds any realistic `health_interval_s`, default
60s) is already far larger than `health_interval_s` the very first time
`on_loop_tick()` is called — so the very first tick was *always* judged
"overdue," regardless of the configured interval.

## 3. Intended behavior

Per Phase 13C's own design intent (§20, "Health interval") and both
existing tests:

- `test_health_summary_is_emitted_with_expected_fields` — with
  `health_interval_s=0.0`, the first `on_loop_tick()` after construction
  should emit immediately (elapsed time since construction is always
  `>= 0`).
- `test_health_summary_not_emitted_before_interval_elapses` — with
  `health_interval_s=9999.0`, the first two `on_loop_tick()` calls
  (effectively no wall-clock time elapsed) should emit nothing.

The interval is meant to be measured **from observer construction (worker
startup)**, not from an arbitrary zero point.

## 4. Root cause

`self._last_health_emit` was initialized to the literal `0.0` instead of
the same `time.monotonic()` reading already captured one line above it as
`self._started_monotonic`. The two clocks being compared
(`time.monotonic()` in `on_loop_tick()` vs. a hardcoded `0.0` baseline)
were never on commensurate footing — `0.0` is not "no time has passed
since startup," it is "monotonic clock zero," a point in the past that has
no relationship to when the process actually started.

## 5. Fix

One-line change, `worker/observability.py`:

```python
self._started_monotonic = time.monotonic()
self._started_at = time.time()
self._last_health_emit = self._started_monotonic   # was: 0.0
```

`_last_health_emit` now starts at the same monotonic reading already used
as the process's own startup reference (`_started_monotonic`), so
`on_loop_tick()`'s elapsed-time calculation is measured from construction
time, matching both tests' expectations and the original design intent.
No other line in `worker/observability.py`, `worker/fingerprint_worker.py`,
or `worker/main.py` was touched — the bug was fully contained to this one
initialization.

---

## 6. Failure semantics

Unchanged by this fix. `on_loop_tick()`'s control flow, `emit_health_summary()`,
and every downstream consumer (`worker/main.py`'s health-tick wiring, the
shutdown-summary path, the `WORKER_RUN_OUTPUT` run record) are untouched —
this is purely a timer-baseline correction, not a behavioral or interface
change. A worker that previously emitted one extra, effectively-immediate
`worker_health` event on process startup (before the first real interval
had elapsed) now correctly waits the full configured interval before its
first emission, exactly as already documented.

---

## 7. Tests

No new tests were added — the two existing tests that already specify this
contract (`test_health_summary_is_emitted_with_expected_fields`,
`test_health_summary_not_emitted_before_interval_elapses`) are sufficient
to pin both directions of the fix (immediate-due case and not-yet-due
case). Adding a third test would duplicate coverage those two already
provide together.

## 8. Results

**MEASURED, this session:**

```text
# Before fix (confirms the bug, matches Phase 13D's own recorded finding)
python -m pytest -q
  -> 268 passed, 1 failed
     (tests/test_worker_observability.py::test_health_summary_not_emitted_before_interval_elapses)

# After fix
python -m pytest tests/test_worker_observability.py -q
  -> 20 passed

python -m pytest tests/test_worker_observability.py tests/test_worker_main.py \
    tests/test_worker.py tests/test_crash_recovery.py tests/test_matching_handler.py -q
  -> 67 passed

python -m pytest -q   (full suite)
  -> 269 passed, 0 failed, 0 skipped
```

No pre-existing failures were hidden — the one pre-existing failure is the
bug this phase fixes; no other test's pass/fail status changed (268 -> 269,
exactly +1, the fixed test).

---

## 9. Performance impact

None. The change replaces one float literal with a variable already
computed on the previous line — no new computation, no new branch shape,
no change to `on_loop_tick()`'s call frequency from `Worker.run()`
(unchanged: once per loop iteration, an O(1) comparison). Not separately
benchmarked; **INFERRED** to be immaterial given the change's shape, and
consistent with Phase 13C's own `on_loop_tick()` overhead measurement
(§20, "Overhead" — sub-microsecond per call for the gating check itself),
which this fix does not alter.

---

## 10. Limitations

- This fix only corrects the interval-gate's timer baseline. It does not
  change, and this phase makes no claim about, anything else in Phase
  13C's observability surface (event schema, counters, latency stats,
  Redis health snapshot, resource sampling, run-record/marker semantics) —
  all of that remains exactly as documented in
  `docs/architecture/phase-13-production-hardening.md` §20.
- Fleet-wide aggregation, a metrics backend/dashboard, and GPU-specific
  metrics remain **DEFERRED**, unchanged from Phase 13C.
- This fix has no dependency on and no bearing on Phase 13D's own
  **REQUIRES MULTI-HOST VALIDATION** status — that status is unchanged by
  this phase, per this session's explicit instruction not to reopen or
  reclassify it.

## 11. Future work

None identified specifically for this fix — it closes the one gap it was
scoped to close. Broader observability future work (fleet aggregation, a
metrics backend) remains exactly as listed in Phase 13C's own "Known
limitations" section, unaffected by this phase.

---

## 12. Updated Phase 13 status

| Item | Status |
| --- | --- |
| 13A — SSRF protection | COMPLETE |
| 13B — production entrypoint | COMPLETE |
| 13C — observability | COMPLETE |
| 13D — distributed target artifacts | IMPLEMENTED — SIMULATED MULTI-HOST; REAL MULTI-HOST VALIDATION REQUIRED (unchanged by this phase) |
| 13E — health-summary interval-gate fix | COMPLETE |

All five originally-audited production blockers (§10 of
`phase-13-production-hardening.md`: worker entrypoint, multi-host target
cache, SSRF, observability, CPU-thread plumbing) are now implemented.
Remaining open items, none of which are classified as production blockers
by the original audit:

- Real multi-host validation of Phase 13D (explicitly out of scope this
  session — no second host/shared filesystem available).
- `integration.backpressure.count_outstanding`'s `lag is None` fallback
  branch remains untested (OPERATIONAL FOLLOW-UP, audit §9 finding #9;
  explicitly labeled "not urgently" needed in the audit's own §15).
- Startup crash-loop/readiness-probe policy and Redis HA/TLS/connection-
  pool tuning beyond `REDIS_URL` remain explicitly DEFERRED to an external
  process supervisor/orchestrator, per Phase 13B's own scope decision —
  not reopened by this phase.
