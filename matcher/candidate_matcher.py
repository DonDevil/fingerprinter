from __future__ import annotations

from pathlib import Path
from urllib.parse import urlparse
import re

TOKEN_RE = re.compile(r"[a-z0-9]+")


def tokenize_text(value: str) -> set[str]:
    tokens = {token for token in TOKEN_RE.findall((value or "").lower()) if len(token) >= 3}
    return tokens


def build_container_signature(media_url: str, local_path: str, duration_seconds: float | None) -> dict[str, str | float | None]:
    parsed = urlparse(media_url)
    url_suffix = Path(parsed.path).suffix.lower()
    file_suffix = Path(local_path).suffix.lower()

    return {
        "url_extension": url_suffix or None,
        "file_extension": file_suffix or None,
        "duration_seconds": duration_seconds,
    }


def score_title_token_overlap(target_title: str, candidate_text: str) -> dict[str, object]:
    target_tokens = tokenize_text(target_title)
    candidate_tokens = tokenize_text(candidate_text)

    if not target_tokens:
        return {
            "score": 0.0,
            "target_tokens": sorted(target_tokens),
            "matched_tokens": [],
        }

    matched = sorted(target_tokens & candidate_tokens)
    score = len(matched) / len(target_tokens)
    return {
        "score": score,
        "target_tokens": sorted(target_tokens),
        "matched_tokens": matched,
    }


def classify_match_score(score: float, *, low_threshold: float, high_threshold: float) -> tuple[str, str]:
    if high_threshold < low_threshold:
        high_threshold = low_threshold

    if score >= high_threshold:
        return "sampled", f"quick_match_high score={score:.3f}"

    if score <= low_threshold:
        return "no_match_pending_review", f"quick_match_low score={score:.3f}"

    return "uncertain_manual_review", f"quick_match_uncertain score={score:.3f}"
