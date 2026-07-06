# Fingerprinter Usage

This guide explains how to run the fingerprinter, what processing cycle it follows, and all supported CLI flags.

The matcher now uses segment-based video/audio fingerprints with configurable sequence alignment:

- Video segmentation for target and candidate streams (separate durations).
- Sequence alignment with `constrained` sliding or `dtw`.
- Audio segmentation with offset estimation via `offset_xcorr` or `dtw`.
- High-intensity mode that switches candidate segment duration.

## Environment

Use only the local virtual environment.

```bash
cd /home/darkdevil/Desktop/anti_piracy/fingerprinter
./.venv/bin/python -m main --help
```

## Processing Cycle

Default queue mode (`python -m main`) does this in order:

1. Reclaims stale `claimed` jobs back to `pending` (unless `--no-resume-unfinished` is passed).
2. Claims one `pending` job from `sample_jobs`.
3. Runs gates (`non-video`, `too-short`), then multi-signal fingerprint compare against target.
4. Persists run and per-stage scores to `storage/processing.db`.
5. Updates queue status in crawler DB (`matched`, `no_match_pending_review`, etc.).
6. Keeps matched evidence; non-matches may be deleted by retention policy.
7. Repeats until stopped (or exits when bounded by `--once`/`--max-jobs`).

### Completed vs Unfinished Tracking

Compare task state is tracked in `storage/processing.db`, table `compare_tasks`:

- `completed`: compare finished successfully (match or no-match outcome).
- `in_progress`: compare started but not completed yet.
- `failed`: compare run failed.

Queue-mode unfinished jobs are also tracked in crawler `sample_jobs` statuses. Stale `claimed` rows are reclaimed automatically based on `queue.reclaim_claimed_after_seconds`.

### What no_match_pending_review Means

`no_match_pending_review` means the run completed and the final piracy score was at or below the low threshold.

- It is currently treated as non-match in automated flow.
- The `pending_review` suffix means you can still manually inspect stage scores if needed.
- Storage policy may delete the local candidate file after this status is recorded.

So it can appear in DB even if the file is no longer in `storage/downloads`.

### Why Some Asset IDs Are Not In storage/downloads

Examples like `1044`, `1629` can exist in metadata tables while file is missing because:

- the file was deleted by retention (`delete_non_matching_assets`) after non-match,
- `--clear-assets` was used,
- the source was URL/queue-only and local copy was transient,
- local file path changed outside the worker.

Decision/status history remains in DB by design for auditability.

## Tables and Purpose

### Local DB: storage/processing.db

- `processed_assets`: one row per processed local candidate path + decision + deletion marker.
- `processing_runs`: one row per fingerprint run result (final status + score).
- `stage_results`: per-stage evidence for each run.
- `piracy_matches`: confirmed match events with evidence payload.
- `crawler_feedback_events`: feedback actions sent to crawler prioritization.
- `compare_tasks`: compare lifecycle (`in_progress`, `completed`, `failed`) for queue/file/dir modes.

### Crawler DB (external): ../crawler/storage/media_evidence.db

- `media_assets`: crawler-discovered asset metadata and current crawler-side status.
- `sample_jobs`: queue rows used for claim/retry/status updates.

## Main Commands

### 1) Queue Worker (normal mode)

```bash
./.venv/bin/python -m main
```

### 2) Queue Worker, single job

```bash
./.venv/bin/python -m main --once
```

### 3) Queue Worker, max N jobs then exit

```bash
./.venv/bin/python -m main --max-jobs 10
```

### 4) Compare exactly one video to target

Use local file:

```bash
./.venv/bin/python -m main --compare-file storage/downloads/asset_1013.mp4
```

Use URL:

```bash
./.venv/bin/python -m main --compare-file "https://example.com/video.mp4"
```

### 5) Compare files from one directory to target

Default directory (`storage/downloads`):

```bash
./.venv/bin/python -m main --compare-dir
```

Explicit directory:

```bash
./.venv/bin/python -m main --compare-dir /path/to/videos
```

Limit to first N files:

```bash
./.venv/bin/python -m main --compare-dir storage/downloads --max-jobs 20
```

Directory mode skips files already marked completed in `compare_tasks` for the same target + technique set.

### 6) Show compare tracking status

```bash
./.venv/bin/python -m main --status
```

Prints:

- completed compare count
- in-progress compare count
- failed compare count
- queue pending count

### 7) Reset local processing metadata tables

```bash
./.venv/bin/python -m main --reset
```

This clears all local processing tables in `storage/processing.db`.

### 8) Clear all downloaded assets

```bash
./.venv/bin/python -m main --clear-assets
```

This deletes files under configured downloader directory (default `storage/downloads`) and marks matching rows as deleted in `processed_assets`.

### 9) Keep non-matches for manual review

```bash
./.venv/bin/python -m main --compare-dir storage/downloads --keep-non-matches
```

### 10) Reset and clear assets together

```bash
./.venv/bin/python -m main --reset --clear-assets
```

## Technique Selection Flags

By default, all techniques are enabled.

```bash
./.venv/bin/python -m main --techniques all
```

Specify subset:

```bash
./.venv/bin/python -m main --compare-dir --techniques metadata,visual
./.venv/bin/python -m main --compare-file storage/downloads/asset_1056.mp4 --techniques visual,audio
```

Supported technique names:

- `metadata`
- `visual`
- `audio`
- `temporal`

If an unknown technique is provided, the run exits with a validation error.

## Segment and Alignment Flags

Override video segmentation:

```bash
./.venv/bin/python -m main --target-segment-seconds 1.0 --candidate-segment-seconds 2.0 --frame-sample-fps 2.5
```

Enable high-intensity candidate segmentation:

```bash
./.venv/bin/python -m main --compare-dir storage/downloads --high-intensity
```

Override high-intensity candidate segment duration:

```bash
./.venv/bin/python -m main --high-intensity --candidate-segment-seconds-high-intensity 0.75
```

Override sequence alignment method and band:

```bash
./.venv/bin/python -m main --sequence-alignment-method dtw --sequence-band-ratio 0.2
```

Override audio segmentation and alignment:

```bash
./.venv/bin/python -m main --audio-segment-seconds 1.5 --audio-alignment-method offset_xcorr
./.venv/bin/python -m main --audio-segment-seconds 1.2 --audio-alignment-method dtw --audio-band-ratio 0.25
```

Notes:

- `--sequence-alignment-method` supports `constrained` or `dtw`.
- `--audio-alignment-method` supports `offset_xcorr` or `dtw`.
- Segment and band overrides are applied to queue mode, `--compare-file`, and `--compare-dir`.

## Target Selection Flags

Override target title:

```bash
./.venv/bin/python -m main --target "Blast"
```

Target file path is configured in `config.yaml`:

```yaml
pipeline:
  target_file_path: target/Blast.mp4
```

## Unfinished Resume Flag

Default behavior is to resume unfinished queue claims by reclaiming stale `claimed` jobs.

Disable that behavior:

```bash
./.venv/bin/python -m main --no-resume-unfinished
```

## Full Flag Reference

- `--config PATH`: path to YAML config file.
- `--once`: process one claimed queue job and exit.
- `--max-jobs N`: process up to N queue jobs, or up to N files in `--compare-dir` mode.
- `--target TITLE`: override target title.
- `--compare-file PATH_OR_URL`: compare one file/URL to target and exit.
- `--compare-dir [DIR]`: compare files in directory to target; defaults to `storage/downloads` when no DIR is provided.
- `--techniques LIST`: comma-separated techniques (`metadata,visual,audio,temporal`) or `all`.
- `--status`: print compare task status counters and exit.
- `--no-resume-unfinished`: skip reclaiming stale `claimed` queue jobs.
- `--reset`: clear local processing metadata tables in `storage/processing.db`.
- `--clear-assets`: remove all files from downloader directory and mark matching processed rows deleted.
- `--keep-non-matches`: retain local files for non-match outcomes instead of auto-deleting them.
- `--target-segment-seconds FLOAT`: override target/movie segment duration.
- `--candidate-segment-seconds FLOAT`: override candidate segment duration.
- `--candidate-segment-seconds-high-intensity FLOAT`: override high-intensity candidate segment duration.
- `--high-intensity`: enable high-intensity candidate segmentation mode.
- `--frame-sample-fps FLOAT`: override sampled frame rate used by segment fingerprinting.
- `--sequence-alignment-method {constrained,dtw}`: override video sequence alignment method.
- `--sequence-band-ratio FLOAT`: override sequence alignment DTW/constrained band ratio.
- `--audio-segment-seconds FLOAT`: override audio segment duration.
- `--audio-alignment-method {offset_xcorr,dtw}`: override audio alignment method.
- `--audio-band-ratio FLOAT`: override audio DTW band ratio.

## Config Defaults For Segment Matching

Default values in `config.yaml`:

```yaml
video_fingerprint:
  target_segment_seconds: 1.0
  candidate_segment_seconds: 2.0
  candidate_segment_seconds_high_intensity: 1.0
  frame_sample_fps: 2.0

sequence_alignment:
  method: constrained
  band_ratio: 0.15

audio_fingerprint:
  segment_seconds: 1.5
  alignment_method: offset_xcorr
  alignment_band_ratio: 0.2
```

## Useful Debug Commands

See latest compare tasks:

```bash
sqlite3 storage/processing.db "SELECT task_key, status, outcome_status, attempt_count, updated_at FROM compare_tasks ORDER BY id DESC LIMIT 20;"
```

See recent runs with scores:

```bash
sqlite3 storage/processing.db "SELECT id, asset_id, final_status, piracy_score, created_at FROM processing_runs ORDER BY id DESC LIMIT 20;"
```

See stage-level details for latest run:

```bash
sqlite3 storage/processing.db "SELECT run_id, stage_name, score, decision, note FROM stage_results ORDER BY id DESC LIMIT 30;"
```

Check queue states in crawler DB:

```bash
sqlite3 ../crawler/storage/media_evidence.db "SELECT status, COUNT(*) FROM sample_jobs GROUP BY status ORDER BY status;"
```

## Quick Verification For Your Existing Samples

If you already have known target clips in downloads, run:

```bash
./.venv/bin/python -m main --compare-dir storage/downloads --techniques all
./.venv/bin/python -m main --status
```

Then inspect latest runs and stage scores using the debug SQL commands above.
