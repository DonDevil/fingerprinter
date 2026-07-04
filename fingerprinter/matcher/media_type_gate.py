from __future__ import annotations

from urllib.parse import urlparse


VIDEO_EXTENSIONS = {
    ".mp4",
    ".mkv",
    ".avi",
    ".mov",
    ".webm",
    ".m4v",
    ".mpg",
    ".mpeg",
    ".m3u8",
    ".mpd",
    ".ts",
    ".m4s",
}

IMAGE_EXTENSIONS = {
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".webp",
    ".svg",
}


def should_reject_non_video(
    media_type: str,
    media_url: str,
    *,
    enable_tor: bool = False,
) -> tuple[bool, str | None]:
    normalized_type = (media_type or "").strip().lower()
    parsed_url = urlparse(media_url)
    host = (parsed_url.hostname or "").lower()

    if host.endswith(".onion") and not enable_tor:
        return True, "Rejected: .onion asset requires Tor support (disabled)"

    if normalized_type in {"video", "stream-manifest", "audio", "unknown"}:
        pass
    elif normalized_type:
        return True, f"Rejected: unsupported media_type={normalized_type}"

    path = (parsed_url.path or "").lower()
    for extension in IMAGE_EXTENSIONS:
        if path.endswith(extension):
            return True, f"Rejected: image asset extension {extension}"

    if any(path.endswith(extension) for extension in VIDEO_EXTENSIONS):
        return False, None

    # Unknown extension is allowed and will be judged by downstream stages.
    return False, None
