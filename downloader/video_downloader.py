import os
import shutil
import requests
from typing import Optional
from urllib.parse import urlparse
from loguru import logger


class VideoDownloadError(RuntimeError):
    """Raised when video download/copy operation fails with actionable context."""


class VideoDownloader:
    """Downloads video files, supporting partial (byte range) and full downloads."""
    def __init__(
        self,
        download_dir: str = "storage/downloads",
        request_timeout_seconds: int = 30,
        enable_tor: bool = False,
        tor_proxy_url: str = "socks5h://127.0.0.1:9050",
    ):
        self.download_dir = download_dir
        self.request_timeout_seconds = request_timeout_seconds
        self.enable_tor = bool(enable_tor)
        self.tor_proxy_url = tor_proxy_url
        os.makedirs(self.download_dir, exist_ok=True)

    def _build_proxies(self) -> dict[str, str] | None:
        if not self.enable_tor:
            return None
        return {
            "http": self.tor_proxy_url,
            "https": self.tor_proxy_url,
        }

    @staticmethod
    def _safe_remove(path: str) -> None:
        try:
            if os.path.exists(path):
                os.remove(path)
        except OSError:
            # Keep original exception flow; cleanup best-effort only.
            pass

    def _copy_local(
        self,
        source_path: str,
        local_path: str,
        byte_range: Optional[tuple[int, int]],
    ) -> str:
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
        logger.debug(
            "Starting download: url={} target_path={} range={} tor_enabled={}",
            url,
            local_path,
            byte_range,
            self.enable_tor,
        )

        # Allow local path ingestion (absolute/relative path or file:// URL)
        if parsed.scheme == "file" or (parsed.scheme == "" and os.path.exists(url)):
            source_path = parsed.path if parsed.scheme == "file" else url
            try:
                path = self._copy_local(source_path, local_path, byte_range)
                logger.debug("Local copy completed: source={} destination={}", source_path, path)
                return path
            except FileNotFoundError as exc:
                self._safe_remove(local_path)
                raise VideoDownloadError(
                    f"Local source file not found: source={source_path} destination={local_path}"
                ) from exc
            except PermissionError as exc:
                self._safe_remove(local_path)
                raise VideoDownloadError(
                    f"Permission denied during local copy: source={source_path} destination={local_path}"
                ) from exc
            except OSError as exc:
                self._safe_remove(local_path)
                raise VideoDownloadError(
                    f"Local copy failed: source={source_path} destination={local_path} error={exc}"
                ) from exc

        if parsed.scheme not in {"http", "https"}:
            raise VideoDownloadError(
                f"Unsupported URL scheme: scheme={parsed.scheme or '<empty>'} url={url}"
            )

        headers: dict[str, str] = {}
        if byte_range:
            headers['Range'] = f'bytes={byte_range[0]}-{byte_range[1]}'
        proxies = self._build_proxies()

        try:
            with requests.get(
                url,
                headers=headers,
                stream=True,
                timeout=self.request_timeout_seconds,
                proxies=proxies,
            ) as r:
                r.raise_for_status()
                # If partial content is not supported, fallback to full download
                if byte_range and r.status_code != 206:
                    logger.debug(
                        "Range request not honored (status={}); retrying full download for url={}",
                        r.status_code,
                        url,
                    )
                    return self.download(url, filename, None)
                with open(local_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
        except requests.exceptions.Timeout as exc:
            self._safe_remove(local_path)
            raise VideoDownloadError(
                f"Timed out while downloading url={url} timeout={self.request_timeout_seconds}s"
            ) from exc
        except requests.exceptions.ConnectionError as exc:
            self._safe_remove(local_path)
            if self.enable_tor:
                raise VideoDownloadError(
                    "Connection error while downloading via Tor proxy. "
                    f"url={url} proxy={self.tor_proxy_url} error={exc}"
                ) from exc
            raise VideoDownloadError(f"Connection error while downloading url={url}: {exc}") from exc
        except requests.exceptions.HTTPError as exc:
            self._safe_remove(local_path)
            status_code = exc.response.status_code if exc.response is not None else "unknown"
            raise VideoDownloadError(
                f"HTTP error while downloading url={url} status={status_code}"
            ) from exc
        except requests.exceptions.RequestException as exc:
            self._safe_remove(local_path)
            msg = str(exc)
            if "Missing dependencies for SOCKS support" in msg:
                raise VideoDownloadError(
                    "SOCKS proxy requested but SOCKS dependencies are missing. "
                    "Install PySocks or requests[socks]."
                ) from exc
            raise VideoDownloadError(f"Request failed for url={url}: {exc}") from exc
        except OSError as exc:
            self._safe_remove(local_path)
            raise VideoDownloadError(
                f"Failed writing download to disk: destination={local_path} error={exc}"
            ) from exc

        logger.debug("Download completed: url={} destination={}", url, local_path)
        return local_path
