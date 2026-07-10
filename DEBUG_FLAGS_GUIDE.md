# Debug Flags and Target Re-embedding Guide

## Smart Target Embedding System

The system now automatically detects target file changes and caches embeddings efficiently.

### How It Works

#### 1. **File Mapping and Change Detection**
The metadata file (`storage/target_fingerprints/{cache_key}.meta.json`) now stores:
- `target_path`: Absolute path to the target video file
- `file_size`: File size in bytes
- `file_mtime`: File modification time (nanoseconds)
- `model_name`: DINOv2 model name
- `target_sample_fps`: Sampling FPS used
- `embedding_count`: Number of frames extracted

#### 2. **Automatic Change Detection**
The system compares:
- Current target file path → Cached metadata `target_path`
- If paths differ → **Different file detected** → Rebuild embedding

#### 3. **One-Time Reprocessing with `--reprocess-target`**
- First time a target is loaded with `--reprocess-target` flag → **Forces rebuild**
- Subsequent targets in the same run → Uses smart caching
- Does **NOT** reprocess for every candidate video

### Usage Examples

#### Scenario 1: Process multiple videos against same target
```bash
# Loads target.mp4 once, reuses for all candidates
python3 main.py --target "target/Blast.mp4" --compare-dir "storage/downloads" --debug
```

#### Scenario 2: Force fresh target embedding
```bash
# Force rebuild target embedding once, then reuse
python3 main.py --target "target/Blast.mp4" --reprocess-target --compare-dir "storage/downloads" --debug
```

#### Scenario 3: Switch target files mid-run (queue mode)
```bash
# If target changes during processing, system auto-detects and rebuilds
python3 main.py --reprocess-target --debug
```

### Debug Output Examples

#### Normal Cache Hit (same target)
```
Target embedding cache key: 1a2b3c4d5e6f7g8h9i0j1k2l3m4n5o6p
Target file: /home/user/target/Blast.mp4
Using cached target embedding (in-memory)
Successfully loaded cached target embedding: 120 frames
```

#### File Changed (auto-detection)
```
Target embedding cache key: 1a2b3c4d5e6f7g8h9i0j1k2l3m4n5o6p
Target file: /home/user/target/AnotherMovie.mp4
Target file changed: was /home/user/target/Blast.mp4 now /home/user/target/AnotherMovie.mp4
Deleted old target embedding cache: storage/target_fingerprints/old_cache.npz
Building DINOv2 target embedding for: /home/user/target/AnotherMovie.mp4
Target embedding built: 150 frames extracted at 2.0 FPS
```

#### Force Reprocess (--reprocess-target flag)
```
Target embedding cache key: 1a2b3c4d5e6f7g8h9i0j1k2l3m4n5o6p
Target file: /home/user/target/Blast.mp4
--reprocess-target flag set: forcing rebuild on first target load
Deleted old target embedding cache: storage/target_fingerprints/cache.npz
Building DINOv2 target embedding for: /home/user/target/Blast.mp4
Target embedding built: 120 frames extracted at 2.0 FPS
```

#### Second Target in Same Run (reprocess-target already applied)
```
Target embedding cache key: 1a2b3c4d5e6f7g8h9i0j1k2l3m4n5o6p
Target file: /home/user/target/Blast.mp4
--reprocess-target flag already applied once this session, using cached embedding
Using cached target embedding (in-memory)
```

### Progress Bars

When using `--debug` flag, you'll see progress bars for:
- **Frame extraction**: Shows real-time progress while extracting frames from video
- Format: `Extracting target frames: 150/2000 [07:30<15:22, 1.62 frames/s]`

### Flag Combinations

| Command | Behavior |
|---------|----------|
| `--compare-dir "..."` | Uses existing cache or builds if not exists |
| `--debug` | Prints detailed info + shows progress bars |
| `--reprocess-target` | Force rebuild target embedding once per run |
| `--debug --reprocess-target` | Force rebuild + detailed logs + progress bars |
| Multiple `--compare-file` with same target | Auto-detects changes, reuses when possible |

### Cache Location

All embeddings and metadata stored in:
```
storage/target_fingerprints/
├── {cache_key}.npz              # Embedding data (numpy arrays)
└── {cache_key}.meta.json        # File mapping metadata
```

### Performance Impact

- **First run (new target)**: Full embedding extraction (slow)
- **Subsequent runs (same target)**: Cache hit (instant)
- **Different target detected**: Auto-rebuild (slow)
- **With `--reprocess-target`**: One rebuild + cache reuse (optimized)

