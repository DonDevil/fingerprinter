import os

import http.server
import threading
import socketserver
import tempfile
import shutil
import time
import pytest
import requests
from old.downloader.video_downloader import VideoDownloader, VideoDownloadError

@pytest.fixture(scope="module")
def http_server():
    temp_dir = tempfile.mkdtemp()
    file_path = os.path.join(temp_dir, "test.mp4")
    with open(file_path, "wb") as f:
        f.write(b"A" * 1024 * 1024)  # 1MB dummy file
    os.chdir(temp_dir)
    handler = http.server.SimpleHTTPRequestHandler
    httpd = socketserver.TCPServer(("", 0), handler)  # Bind to random available port
    port = httpd.server_address[1]
    thread = threading.Thread(target=httpd.serve_forever)
    thread.daemon = True
    thread.start()
    time.sleep(1)
    yield f"http://localhost:{port}/test.mp4"
    httpd.shutdown()
    thread.join()
    shutil.rmtree(temp_dir)


def test_full_download(http_server):
    downloader = VideoDownloader(download_dir="storage/test_downloads")
    path = downloader.download(http_server)
    assert os.path.exists(path)
    assert os.path.getsize(path) == 1024 * 1024
    os.remove(path)


def test_partial_download(http_server):
    downloader = VideoDownloader(download_dir="storage/test_downloads")
    path = downloader.download(http_server, filename="partial.mp4", byte_range=(0, 499999))
    assert os.path.exists(path)
    size = os.path.getsize(path)
    # If server does not support byte ranges, fallback will download full file
    if size > 600000:
        pytest.skip("Server does not support partial content; full file downloaded.")
    assert 400000 < size < 600000
    os.remove(path)


def test_local_file_copy_download(tmp_path):
    source = tmp_path / "source.mp4"
    source.write_bytes(b"hello-world")

    downloader = VideoDownloader(download_dir=str(tmp_path / "downloads"))
    path = downloader.download(str(source), filename="copied.mp4")

    assert os.path.exists(path)
    with open(path, "rb") as handle:
        assert handle.read() == b"hello-world"


def test_tor_proxy_configuration_is_passed(monkeypatch, tmp_path):
    captured = {}

    class _Response:
        status_code = 200

        def raise_for_status(self):
            return None

        def iter_content(self, chunk_size=8192):
            yield b"payload"

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def _fake_get(url, headers=None, stream=None, timeout=None, proxies=None):
        captured["url"] = url
        captured["headers"] = headers
        captured["timeout"] = timeout
        captured["proxies"] = proxies
        return _Response()

    monkeypatch.setattr(requests, "get", _fake_get)

    downloader = VideoDownloader(
        download_dir=str(tmp_path),
        enable_tor=True,
        tor_proxy_url="socks5h://127.0.0.1:9999",
    )
    out = downloader.download("https://example.com/video.mp4", filename="tor.mp4")

    assert os.path.exists(out)
    assert captured["proxies"] == {
        "http": "socks5h://127.0.0.1:9999",
        "https": "socks5h://127.0.0.1:9999",
    }


def test_http_timeout_is_wrapped_in_download_error(monkeypatch, tmp_path):
    def _fake_get(*_args, **_kwargs):
        raise requests.exceptions.Timeout("timed out")

    monkeypatch.setattr(requests, "get", _fake_get)
    downloader = VideoDownloader(download_dir=str(tmp_path), request_timeout_seconds=1)

    with pytest.raises(VideoDownloadError) as exc_info:
        downloader.download("https://example.com/video.mp4", filename="timeout.mp4")

    assert "Timed out while downloading" in str(exc_info.value)


def test_unsupported_scheme_raises_download_error(tmp_path):
    downloader = VideoDownloader(download_dir=str(tmp_path))
    with pytest.raises(VideoDownloadError) as exc_info:
        downloader.download("ftp://example.com/video.mp4")
    assert "Unsupported URL scheme" in str(exc_info.value)
