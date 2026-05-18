import os
import requests
from typing import Optional

class VideoDownloader:
    """Downloads video files, supporting partial (byte range) and full downloads."""
    def __init__(self, download_dir: str = "fingerprinter/storage/downloads"):
        self.download_dir = download_dir
        os.makedirs(self.download_dir, exist_ok=True)

    def download(self, url: str, filename: Optional[str] = None, byte_range: Optional[tuple] = None) -> str:
        """
        Download a video file from a URL.
        If byte_range is provided, only that part is downloaded (start, end).
        Returns the path to the downloaded file.
        """
        local_filename = filename or url.split("/")[-1].split("?")[0]
        local_path = os.path.join(self.download_dir, local_filename)
        headers = {}
        if byte_range:
            headers['Range'] = f'bytes={byte_range[0]}-{byte_range[1]}'
        with requests.get(url, headers=headers, stream=True, timeout=30) as r:
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
