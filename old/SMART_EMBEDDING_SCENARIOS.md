# Smart Target Embedding - Test Scenarios

## Key Improvements Summary

### Before (Old Behavior)
```
--reprocess-target --compare-dir "storage/downloads"
├── Candidate 1: Rebuild target ❌
├── Candidate 2: Rebuild target ❌ (unnecessary!)
├── Candidate 3: Rebuild target ❌ (unnecessary!)
└── Candidate 4: Rebuild target ❌ (unnecessary!)
```
**Problem**: Target rebuilt 4 times for 4 candidates (wasteful!)

### After (New Behavior)
```
--reprocess-target --compare-dir "storage/downloads"
├── Candidate 1: Rebuild target ✅ (--reprocess-target applied)
├── Candidate 2: Reuse cached ✅
├── Candidate 3: Reuse cached ✅
└── Candidate 4: Reuse cached ✅
```
**Solution**: Target rebuilt only once, reused for all candidates!

---

## Test Scenario 1: Multiple Candidates Same Target

### Command
```bash
python3 main.py --target "target/Blast.mp4" --compare-dir "storage/downloads" --debug --reprocess-target
```

### Expected Output (Debug Logs)
```
=== Starting directory comparison: /home/.../storage/downloads ===
Processing file 1/?: video1.mp4
=== Starting comparison for candidate: /home/.../video1.mp4 ===
Target embedding cache key: abc123def456
Target file: /home/.../target/Blast.mp4
--reprocess-target flag set: forcing rebuild on first target load        👈 ONLY HERE
Building DINOv2 target embedding for: /home/.../target/Blast.mp4
Extracting target frames: 120/2500 [00:05<01:20, 18.5 frames/s]        👈 PROGRESS
Target embedding built: 120 frames extracted at 2.0 FPS

Processing file 2/?: video2.mp4
=== Starting comparison for candidate: /home/.../video2.mp4 ===
Target embedding cache key: abc123def456
Target file: /home/.../target/Blast.mp4
--reprocess-target flag already applied once this session, using cached embedding  👈 REUSES
Using cached target embedding (in-memory)                                👈 INSTANT

Processing file 3/?: video3.mp4
[Similar to video2 - instant cache reuse]

Directory comparison complete: processed=3
```

---

## Test Scenario 2: Automatic Target File Change Detection

### Command 1: Process with target Blast.mp4
```bash
python3 main.py --target "target/Blast.mp4" --compare-file "video1.mp4" --debug
```

### Command 2: Process with different target movie.mp4
```bash
python3 main.py --target "target/movie.mp4" --compare-file "video1.mp4" --debug
```

### Expected Output (Debug Logs)
```
First run with Blast.mp4:
Target embedding cache key: xyz789abc
Target file: /home/.../target/Blast.mp4
Building DINOv2 target embedding for: /home/.../target/Blast.mp4
Target embedding built: 120 frames extracted at 2.0 FPS

Second run with movie.mp4:
Target embedding cache key: xyz789abc  (same cache key algorithm)
Target file: /home/.../target/movie.mp4
Target file changed: was /home/.../target/Blast.mp4 now /home/.../target/movie.mp4  👈 DETECTED
Deleted old target embedding cache: storage/target_fingerprints/xyz789abc.npz
Building DINOv2 target embedding for: /home/.../target/movie.mp4        👈 AUTO-REBUILD
Target embedding built: 150 frames extracted at 2.0 FPS
```

---

## Test Scenario 3: Normal Operation (No Reprocess Flag)

### Command
```bash
python3 main.py --target "target/Blast.mp4" --compare-dir "storage/downloads" --debug
```

### Expected Output
```
Run 1 (first time):
Target embedding cache key: abc123def456
Building DINOv2 target embedding for: /home/.../target/Blast.mp4
Target embedding built: 120 frames extracted at 2.0 FPS

Run 2 (same command again):
Target embedding cache key: abc123def456
Target file: /home/.../target/Blast.mp4
Successfully loaded cached target embedding: 120 frames
Using cached target embedding (in-memory)  👈 NO REBUILD, INSTANT
```

---

## Test Scenario 4: Queue Mode (Continuous Processing)

### Command
```bash
python3 main.py --debug --reprocess-target
```

### Expected Output (Multiple Jobs)
```
=== Processing job: asset_id=1001, URL=https://... ===
Target embedding cache key: abc123def456
--reprocess-target flag set: forcing rebuild on first target load        👈 JOB 1
Building DINOv2 target embedding for: /home/.../target/Blast.mp4
Target embedding built: 120 frames extracted at 2.0 FPS

=== Processing job: asset_id=1002, URL=https://... ===
Target embedding cache key: abc123def456
--reprocess-target flag already applied once this session, using cached embedding  👈 JOB 2
Using cached target embedding (in-memory)

=== Processing job: asset_id=1003, URL=https://... ===
Target embedding cache key: abc123def456
Using cached target embedding (in-memory)                               👈 JOB 3
```

---

## Metadata File Example

File: `storage/target_fingerprints/abc123def456.meta.json`

```json
{
  "embedding_count": 120,
  "file_mtime": 1720550400000000000,
  "file_size": 524288000,
  "model_name": "facebook/dinov2-base",
  "target_path": "/home/darkdevil/Desktop/anti_piracy/fingerprinter/target/Blast.mp4",
  "target_sample_fps": 2.0
}
```

---

## Performance Comparison

| Scenario | Old System | New System | Improvement |
|----------|-----------|-----------|------------|
| 10 candidates, same target, --reprocess-target | 10× target build | 1× target build + 9× reuse | **10x faster** ⚡ |
| Switch target mid-run | Manual restart needed | Auto-detected, auto-rebuilt | **Seamless** ✅ |
| Same target across runs | Rebuilds each run | Cached, instant load | **Near instant** ⚡ |
| Memory usage | Cleared after each candidate | Persistent in-memory cache | **More efficient** ✅ |

