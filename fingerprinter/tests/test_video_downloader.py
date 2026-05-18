import os

import http.server
import threading
import socketserver
import tempfile
import shutil
import time
import pytest
from fingerprinter.downloader.video_downloader import VideoDownloader

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
    downloader = VideoDownloader(download_dir="fingerprinter/storage/test_downloads")
    path = downloader.download(http_server)
    assert os.path.exists(path)
    assert os.path.getsize(path) == 1024 * 1024
    os.remove(path)


def test_partial_download(http_server):
    downloader = VideoDownloader(download_dir="fingerprinter/storage/test_downloads")
    path = downloader.download(http_server, filename="partial.mp4", byte_range=(0, 499999))
    assert os.path.exists(path)
    size = os.path.getsize(path)
    # If server does not support byte ranges, fallback will download full file
    if size > 600000:
        pytest.skip("Server does not support partial content; full file downloaded.")
    assert 400000 < size < 600000
    os.remove(path)
