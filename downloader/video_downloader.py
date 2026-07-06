import os
import shutil
import requests
from typing import Optional
from urllib.parse import urlparse


class VideoDownloader:
    """Downloads video files, supporting partial (byte range) and full downloads."""
    def __init__(
        self,
        download_dir: str = "storage/downloads",
        request_timeout_seconds: int = 30,
    ):
        self.download_dir = download_dir
        self.request_timeout_seconds = request_timeout_seconds
        os.makedirs(self.download_dir, exist_ok=True)

    def download(
        self,
        url: str,
        filename: Optional[str] = None,
        byte_range: Optional[tuple[int, int]] = None,
    ) -> str:
        """
        Download a video file from a URL.
        If byte_range is provided, only that part is downloaded (start, end).
        Returns the path to the downloaded file.
        """
        parsed = urlparse(url)
        local_filename = filename or url.split("/")[-1].split("?")[0]
        local_path = os.path.join(self.download_dir, local_filename)

        # Allow local path ingestion (absolute/relative path or file:// URL)
        if parsed.scheme == "file" or (parsed.scheme == "" and os.path.exists(url)):
            source_path = parsed.path if parsed.scheme == "file" else url
            if byte_range is None:
                shutil.copy2(source_path, local_path)
                return local_path

            start, end = byte_range
            with open(source_path, "rb") as src, open(local_path, "wb") as dst:
                src.seek(max(0, int(start)))
                remaining = max(0, int(end) - int(start) + 1)
                chunk_size = 8192
                while remaining > 0:
                    chunk = src.read(min(chunk_size, remaining))
                    if not chunk:
                        break
                    dst.write(chunk)
                    remaining -= len(chunk)
            return local_path

        headers: dict[str, str] = {}
        if byte_range:
            headers['Range'] = f'bytes={byte_range[0]}-{byte_range[1]}'
        with requests.get(url, headers=headers, stream=True, timeout=self.request_timeout_seconds) as r:
            r.raise_for_status()
            # If partial content is not supported, fallback to full download
            if byte_range and r.status_code != 206:
                # Retry without Range header
                return self.download(url, filename, None)
            with open(local_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        return local_path
