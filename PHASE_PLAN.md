# Fingerprinter + Crawler Joint Research Plan

## Objective

Build a research-grade anti-piracy matching system that:

1. Takes a trusted input film (reference title) and builds robust multi-modal fingerprints.
2. Consumes candidate media assets discovered by the crawler.
3. Runs a staged matching pipeline from cheapest to most expensive methods.
4. Escalates uncertain cases to stronger methods.
5. Confirms piracy with calibrated confidence and sends feedback to crawler intelligence.


## Current Crawler Reality (Verified)

The crawler already provides a usable handoff contract for the fingerprinter:

- Media assets are captured into a dedicated evidence DB with observations and sample jobs.
- A worker can claim pending jobs.
- A matched asset can be marked and source domain score is increased.
- Multiple crawler engines exist (async/http/tor/playwright/selenium/scrapling) plus hybrid escalation.

This means fingerprinter should not maintain an independent queue as source of truth. It should consume crawler sample jobs directly.


## System Integration Contract

### Crawler -> Fingerprinter

Source DB: crawler storage/media_evidence.db

Tables already available:

- media_assets
- media_observations
- sample_jobs
- manifest_variants

Worker handshake (already supported by crawler CLI today):

- Claim job
- Process media sample
- Complete status
- Mark confirmed match and update source-domain score

### Fingerprinter -> Crawler Feedback

For confirmed match:

- Update sample_jobs.status and media_assets.status to matched
- Store matched_title and match_confidence
- Increase domain reputation score in crawler domain DB

For uncertain/no match:

- Set status to sampled, hashed, or no_match_pending_review
- Keep evidence so future stronger models can re-check


## Staged Matching Pipeline

Design principle: triage fast, escalate only when needed.

### Stage 0: Pre-filter and Candidate Sanitization (very cheap)

Inputs:

- URL, MIME type, content length, source domain score, discovery method

Actions:

- Reject obvious non-target assets (very short clips, unsupported formats)
- Prioritize likely full-content assets (manifests, long MP4, repeated observations)
- Build processing budget per asset (time/compute cap)

Output:

- Decision: reject | process stage 1

### Stage 1: Byte-level and Header Heuristics (cheap)

Actions:

- Fetch tiny byte windows (head/tail/mid if range supported)
- Container parse: duration estimate, codec, stream count, bitrate profile
- Hash stable metadata signatures (container-level)

Use:

- Fast elimination for mismatched duration/codec fingerprints
- Detect exact re-uploads of same file container

Output:

- Strong mismatch -> stop
- Strong exact-file similarity -> high-confidence candidate
- Uncertain -> stage 2

### Stage 2: Visual Keyframe Fingerprint (cheap-medium)

Actions:

- Sample sparse keyframes at fixed timeline anchors (for example every N seconds)
- Compute robust frame descriptors:
  - pHash/dHash/aHash ensemble
  - color layout histograms
  - low-dim embedding from lightweight visual encoder

Matching:

- Compare against reference-film fingerprint index
- Use temporal anchor agreement, not just single-frame nearest neighbor

Output:

- High agreement -> probable match
- Borderline -> stage 3
- Low -> stop or queue for periodic recheck

### Stage 3: Audio Fingerprint Alignment (medium)

Actions:

- Extract mono audio snippets from multiple windows
- Compute robust audio signatures:
  - chroma landmarks / peak constellation
  - MFCC statistics
  - optional learned audio embedding

Matching:

- Cross-correlate with reference-film audio fingerprints
- Require multi-window consensus to handle intros/outros/cuts

Output:

- Audio + visual consensus -> likely piracy
- Conflicting evidence -> stage 4

### Stage 4: Temporal Sequence Matching (medium-high)

Actions:

- Build sequence signatures over time for video and audio windows
- Run sliding window alignment with tolerance for:
  - re-encoding
  - cropping/letterboxing
  - watermark overlays
  - speed perturbation within bounds

Matching:

- Dynamic time warping / sequence alignment score
- Robustness penalties for inconsistent segment identity

Output:

- Sequence-consistent -> very likely match
- Still ambiguous -> stage 5

### Stage 5: Deep Multimodal Verification (expensive)

Actions:

- Compute multimodal embeddings (video frames + audio)
- Compare with reference embeddings using calibrated classifier
- Optional ensemble meta-classifier over all stage features

Output:

- Final confidence score + calibration interval
- Decision: confirmed_match | uncertain_manual_review | no_match


## Decision Policy and Escalation

Each stage emits:

- match_score (0..1)
- uncertainty_score (0..1)
- quality_score (asset usability)
- rationale features

Policy:

- If match_score >= high_threshold and uncertainty low: accept early.
- If match_score <= low_threshold and uncertainty low: reject early.
- Else escalate.

Research requirement:

- thresholds are not hardcoded constants forever; they are calibrated from validation data and periodically re-estimated.


## Data Model Extensions Needed in Fingerprinter

Create local processing DB (for reproducibility and experiments), not to replace crawler DB.

Suggested tables:

- processing_runs
- stage_results
- reference_catalog
- reference_fingerprints
- match_decisions
- failure_events

Key requirement:

- every decision must be reproducible from stored stage artifacts and model versions.


## Reference Film Ingestion Pipeline

For each trusted input film:

1. Normalize source (container, fps, audio sample rate).
2. Build stage-compatible fingerprints once.
3. Store per-title and per-segment signatures.
4. Version fingerprints when algorithm/model changes.


## Proposed Runtime Pipeline

1. Fingerprinter worker claims one crawler sample job.
2. Download strategy picks partial/full windows based on media type:
  - manifest: fetch playlist and selected variants
  - direct file: byte-range windows first
3. Run staged matcher with escalation policy.
4. Persist stage outputs and evidence.
5. If confirmed:
  - mark matched in crawler media DB
  - pass back title + confidence
  - update crawler domain score
6. If uncertain:
  - set review state and retain artifacts
7. If no match:
  - set completed_no_match


## Confidence and False-Positive Control

Research-grade constraint: minimize false accusations.

Minimum rule for automatic piracy confirmation:

- at least two independent modalities agree (visual + audio), or
- one modality plus strong sequence alignment and high calibration confidence

Everything else goes to uncertain_manual_review.


## Evaluation Framework

Datasets needed:

- Positive pairs: known pirated copies with transformations
- Hard negatives: trailers, clips, fan edits, commentary videos, similarly themed content
- Domain-shift set: different codecs, cams, watermarks, subtitles, frame crops

Primary metrics:

- Precision at high confidence
- False positive rate at decision threshold
- Recall at fixed precision targets
- Escalation rate per stage
- Average compute cost per asset
- Median decision latency


## Implementation Phases (Execution)

### Phase A: Direct crawler queue integration (1-2 weeks)

- Replace local-only queue flow with crawler sample job consumer adapter.
- Add worker loop: claim -> process -> update status.
- Implement robust downloader with byte-range windows + retries.
- Persist processing telemetry and artifacts.

Deliverable: end-to-end processing loop with stub matcher stages and status updates.

### Phase B: Stage 0-2 baseline (2-3 weeks)

- Metadata/byte heuristics + keyframe perceptual fingerprints.
- Reference film ingestion and index builder.
- Decision policy with conservative thresholds.

Deliverable: fast baseline matcher with measurable precision and escalation behavior.

### Phase C: Stage 3-4 robustness (3-4 weeks)

- Audio fingerprints and sequence alignment.
- Multi-window evidence aggregation.
- Hard-negative evaluation suite.

Deliverable: strong multimodal verifier with low false-positive rate.

### Phase D: Stage 5 research verifier (4-6 weeks)

- Deep multimodal embeddings and calibrated meta-classifier.
- Active-learning loop for uncertain cases.

Deliverable: research-grade model stack and periodic calibration tooling.

### Phase E: Ops and governance (ongoing)

- Model/version governance, drift monitoring, evidence retention policy.
- Reporting dashboard and audit trails for legal defensibility.


## Immediate Next Actions

1. Finalize job-state contract between crawler and fingerprinter (status vocabulary and retries).
2. Implement a crawler media DB adapter in fingerprinter queue module.
3. Implement stage interface skeleton:
  - run_stage_0_precheck
  - run_stage_1_container_signature
  - run_stage_2_visual_quick_match
  - run_stage_3_audio_match
  - run_stage_4_temporal_alignment
  - run_stage_5_deep_verify
4. Add integration tests with a temporary crawler media DB fixture.


## Phase A Policy Baseline

Current operational baseline for Phase A worker:

1. Queue backend consumes crawler sample jobs directly from media evidence DB.
2. Duration threshold is config-driven:
  - threshold >= 0 rejects only videos shorter than threshold
  - threshold = -1 disables short-video rejection
3. Stage-0 non-video filter rejects obvious image/file assets before download-heavy steps.
4. Retry policy is config-driven with max retry count:
  - failures are re-queued as pending until max retries
  - then moved to failed state
5. Local processing metadata is persisted for each asset run (path, size, duration, decision, note).
6. Storage retention policy keeps rejected/test files under configured limits:
  - max rejected file count
  - max rejected total size
  - oldest rejected files are deleted first when overflow is enabled


## Non-negotiables

- No automatic piracy flag from single weak signal.
- Every confirmed match must carry explainable evidence.
- Every decision must be traceable to exact code/model versions.
- Escalation must optimize both precision and compute budget.
