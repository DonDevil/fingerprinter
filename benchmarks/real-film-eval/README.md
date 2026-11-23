# Real-film evaluation (A15, 2026-11-23)
Entry point: `taskset -c 0-3 nice -n 15 env/bin/python benchmarks/real-film-eval/run.py --stages all`
(start `monitor.py` first: it is the shared system monitor + thermal guard; stages are resumable via `metrics/stage-*.done.json`).
Stages: inventory, embed_full, segment, validate, excerpts, clips, embed_clips, windows, hardneg (run before `windows`), evaluate; then
`label_hardneg.py` (reviewer labels) and `summarize_monitor.py`. Data: `local-data/real-film-evaluation/` (gitignored).
Source films are read-only and anonymised (movie_001..010). Results and report: workspace `benchmarks/real-film-eval/REPORT.md`.
Note: the `report` stage name in run.py is a placeholder (the report is written by hand); `hardneg` must precede `windows`.
