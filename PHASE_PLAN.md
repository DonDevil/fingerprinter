# Fingerprinter Project Structure & Phase Plan

## Project Structure

```
fingerprinter/
├── downloader/           # Video downloader modules
│   └── ...
├── fingerprint/          # Fingerprinting engines (audio, video, hybrid, etc.)
│   └── ...
├── matcher/              # Matching logic (sliding window, escalation)
│   └── ...
├── storage/              # Evidence, unmatched, and error file management
│   └── ...
├── queue/                # Job queue and worker logic
│   └── ...
├── logger/               # Logging utilities
│   └── ...
├── admin/                # CLI/admin tools for logs/db
│   └── ...
├── tests/                # All test files
│   └── ...
├── main.py               # Entrypoint for the fingerprinting pipeline
├── requirements.txt      # Python dependencies
├── .gitignore            # Version control ignore rules
└── PHASE_PLAN.md         # This plan file
```

## Phase Plan

### Phase 1: Integration & Queue Management
- Design a queue system for video fingerprinting jobs, integrating with crawler’s media asset discovery.
- Implement worker/consumer pattern for the fingerprinting tool to claim/process jobs.
- Integrate with logging system for unified tracking.

### Phase 2: Video Downloading & Storage
- Build a modular video downloader (partial/sliding window & full download).
- Organize downloaded files into `/storage/` with subfolders for matched, unmatched, and error files.
- Store download/processing status in SQLite.

### Phase 3: Fingerprinting Engine
- Implement modular pipeline for multiple fingerprinting techniques:
  - Phase correlation (fast)
  - Audio fingerprinting (chromaprint, ACRCloud, etc.)
  - Perceptual hashing (pHash, dHash, frame sampling)
  - Vector mapping/deep learning (CNN-based)
  - Hybrid/sliding window (combine above)
- Escalate from least to most costly check.
- Store full film fingerprints for comparison.

### Phase 4: Matching & Evidence Management
- Run sliding window fingerprinting against reference films.
- If match: mark source site as pirate, log evidence.
- If not: move file to unmatched/error storage.
- Track all unmatched/failed samples for review.

### Phase 5: Admin & Maintenance Tools
- CLI/UI tools to clear logs, reset databases, manage storage.
- Generate reports on matches, unmatched files, and system status.

## Testing
- All modules will have corresponding tests in `tests/`.
- Each phase will be tested before proceeding to the next.
