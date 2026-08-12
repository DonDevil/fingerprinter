"""Phase 11 benchmark harness.

Not production code — nothing under `worker/`, `embedding/`, `matching/`,
`target/`, `work_queue/`, or `acquisition/` imports this package, and
nothing here is imported by them. These scripts *drive* production code
(construct real `DINOv2EmbeddingEngine`/`TargetRegistry`/`MediaAcquirer`/
`Worker` instances and call their real methods) to measure it, and in a
couple of places wrap those objects with timing instrumentation, but never
modify production module source to make measurement easier — see
docs/architecture/phase-11-performance-benchmarks.md, "Benchmark
methodology".

Each `bench_*.py` module is runnable standalone (`python -m
benchmarks.bench_matching`, etc.) and writes one uniquely-named JSON file
per run into `benchmarks/results/` via `common.save_result` — existing
result files are never overwritten, so historical runs stay comparable.
"""
