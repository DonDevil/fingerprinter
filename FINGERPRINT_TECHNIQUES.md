# Fingerprinting Techniques Research (No AI Models)

This note summarizes practical techniques for anti-piracy fingerprinting, from low-cost checks to high-cost robust matching.

## Source References

- https://en.wikipedia.org/wiki/Perceptual_hashing
- https://en.wikipedia.org/wiki/AcoustID
- https://github.com/acoustid/chromaprint
- https://en.wikipedia.org/wiki/Dynamic_time_warping
- https://ffmpeg.org/ffmpeg-filters.html

## 1) Metadata and Container Matching (Very Low Cost)

What it uses:
- Duration, resolution, stream count, codecs, bitrate profile, container extension.
- URL/path lexical clues (title tokens, year, release tags).

Strengths:
- Extremely cheap and fast.
- Great for early rejection.

Weaknesses:
- Easy to evade with transcoding and metadata edits.
- High false positive risk if used alone.

Recommended use:
- Stage 0/1 pre-filter and weak prior scoring only.

## 2) Perceptual Hashing (pHash/dHash/aHash-style) (Low-Medium Cost)

What it uses:
- Image/frame content transformed to robust short hashes.
- Compare Hamming distance or similarity against target keyframes.

Strengths:
- Better than cryptographic hashes against re-encoding and mild edits.
- Efficient indexing and retrieval.

Weaknesses:
- Vulnerable to heavy crops, overlays, or adversarial manipulations.
- Needs temporal consistency checks for video-level confidence.

Recommended use:
- Sparse keyframe stage with multiple anchor timestamps.

## 3) FFmpeg Signature and Similarity Metrics (Medium Cost)

Useful filters/tooling:
- `signature` (MPEG-7 video signature), `corr`, `msad`, `ssim`, `psnr`, `vif`, `xpsnr`.
- `showinfo`, `signalstats`, `astats`, `aspectralstats` for telemetry features.

Strengths:
- Mature tooling and reproducible metrics.
- Good for deterministic offline validation.

Weaknesses:
- Some metrics are quality-oriented, not copy-identity by themselves.
- Need policy/threshold tuning on hard negatives.

Recommended use:
- Secondary evidence stage and calibration datasets.

## 4) Audio Fingerprinting (Chromaprint-style) (Medium Cost)

What it uses:
- Chroma-based compact acoustic signatures robust to many encodings.
- Chromaprint/AcoustID ecosystem is battle-tested for near-duplicate audio.

Strengths:
- Very strong modality for movie copy detection.
- Less affected by visual watermarks/cropping.

Weaknesses:
- Not ideal for very short snippets in all cases.
- Speed and pitch changes still require alignment logic.

Recommended use:
- Multi-window audio fingerprint stage with consensus voting.

## 5) Temporal Sequence Alignment with DTW (Medium-High Cost)

What it uses:
- Aligns time series under speed variation and local timing drift.
- Works for audio feature sequences and visual feature timelines.

Strengths:
- Handles timing shifts better than pointwise comparison.
- Useful for cut/reordered edits if configured with constraints.

Weaknesses:
- Classic DTW is O(NM) without pruning/windowing.
- Requires careful constraints to avoid over-flexible matching.

Recommended use:
- Late-stage confirmer with locality windows and early-abandon bounds.

## 6) High-Resource Non-AI Advanced Techniques

Examples:
- Dense keyframe extraction + robust local descriptors + geometric verification.
- Segment-level motion vector consistency using transcoding-invariant features.
- Multi-reference consensus (visual + audio + temporal) and calibrated confidence.
- Large-scale ANN indexing for signatures plus second-pass exact recheck.

Strengths:
- Higher robustness to real-world piracy transformations.

Weaknesses:
- More compute, storage, and engineering complexity.

Recommended use:
- Trigger only on uncertain/borderline candidates after cheaper stages.

## Practical Policy Guidance

- Never auto-confirm from one weak modality.
- Confirm piracy when at least two independent modalities agree, or one very strong modality plus temporal alignment.
- Persist per-stage score, threshold, and rationale for auditability.
- Keep matched files as evidence; remove non-matches to control storage costs.
- Feed back matched source domains to crawler prioritization loops.
