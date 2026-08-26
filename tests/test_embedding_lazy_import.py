"""`embedding/__init__.py`'s lazy `DINOv2EmbeddingEngine`/`DEFAULT_MODEL_ID`/
`DEFAULT_MODEL_REVISION` exposure (PEP 562 module `__getattr__`).

Import-graph assertions ("torch is not in sys.modules after importing X")
are only meaningful in a *fresh* process -- by the time any test in this
suite runs, other test modules collected earlier in the same pytest session
have very likely already imported torch (e.g. tests/test_embedding.py,
tests/test_matching_handler.py), which would make an in-process
`sys.modules` check pass trivially regardless of whether the import graph
under test actually avoids torch. Every "does not import torch" assertion
below therefore runs in a genuinely separate `subprocess`, matching the
pattern `tests/test_worker_main.py` already uses for subprocess-based
assertions in this suite.
"""
from __future__ import annotations

import os
import subprocess
import sys

_HEAVY_MODULE_NAMES = ("torch", "transformers", "numpy", "PIL")


def _run(code: str) -> subprocess.CompletedProcess:
    return subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=30)


def test_importing_target_cli_does_not_import_the_ml_stack():
    result = _run(
        "import sys\n"
        "import target.cli\n"
        f"heavy = [m for m in {_HEAVY_MODULE_NAMES!r} if m in sys.modules]\n"
        "print(','.join(heavy))\n"
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "", f"unexpected heavy modules imported: {result.stdout.strip()}"


def test_importing_target_registry_does_not_import_the_ml_stack():
    """target.registry is what target.cli/target.service are built on --
    this is the module the audit traced the eager import chain through
    (target.registry -> target.segment_cache -> embedding.result ->
    embedding/__init__.py -> embedding.dinov2_engine)."""
    result = _run(
        "import sys\n"
        "import target.registry\n"
        f"heavy = [m for m in {_HEAVY_MODULE_NAMES!r} if m in sys.modules]\n"
        "print(','.join(heavy))\n"
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "", f"unexpected heavy modules imported: {result.stdout.strip()}"


def test_importing_target_service_does_not_import_the_ml_stack():
    result = _run(
        "import sys\n"
        "import target.service\n"
        f"heavy = [m for m in {_HEAVY_MODULE_NAMES!r} if m in sys.modules]\n"
        "print(','.join(heavy))\n"
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "", f"unexpected heavy modules imported: {result.stdout.strip()}"


def test_importing_embedding_package_alone_does_not_import_torch():
    """The package itself -- config/errors/result stay eager (they're
    torch-free), only the dinov2_engine-derived names became lazy."""
    result = _run(
        "import sys\n"
        "import embedding\n"
        f"heavy = [m for m in {_HEAVY_MODULE_NAMES!r} if m in sys.modules]\n"
        "print(','.join(heavy))\n"
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "", f"unexpected heavy modules imported: {result.stdout.strip()}"


def test_full_cli_command_cycle_never_imports_torch(redis_client, tmp_path):
    """End-to-end: running target.cli.main() for add/list/get/update-
    metadata/delete in-process is not a valid torch-absence proof (see
    module docstring) -- exercise it in a subprocess instead, against the
    real test Redis, and assert on the child's own sys.modules."""
    media = tmp_path / "movie.mp4"
    media.write_bytes(b"hello world")
    redis_url = os.environ.get("FINGERPRINTER_TEST_REDIS_URL", "redis://localhost:6379/15")

    code = f"""
import sys
import os
os.environ["REDIS_URL"] = {redis_url!r}
os.environ["TARGET_CACHE_PATH"] = {str(tmp_path / "cache")!r}
from target.cli import main
assert main(["add", {str(media)!r}, "--id", "blast", "--version", "v1"]) == 0
assert main(["list", "--json"]) == 0
assert main(["get", "blast", "--version", "v1"]) == 0
assert main(["update-metadata", "blast", "--version", "v1", "--set", "genre=action"]) == 0
assert main(["delete", "blast", "--version", "v1"]) == 0
heavy = [m for m in {_HEAVY_MODULE_NAMES!r} if m in sys.modules]
print("HEAVY_MODULES:" + ",".join(heavy))
"""
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    marker_lines = [line for line in result.stdout.splitlines() if line.startswith("HEAVY_MODULES:")]
    assert marker_lines == ["HEAVY_MODULES:"], f"unexpected heavy modules imported: {marker_lines}"


# ---------------------------------------------------------------------------
# Lazy attribute correctness (in-process is fine here -- these tests *want*
# torch to load, to prove the lazy path still resolves to the real thing).
# ---------------------------------------------------------------------------


def test_lazy_dinov2_engine_attribute_still_resolves():
    import embedding
    from embedding.dinov2_engine import DINOv2EmbeddingEngine as _Direct

    assert embedding.DINOv2EmbeddingEngine is _Direct


def test_lazy_default_model_constants_still_resolve():
    import embedding
    from embedding.dinov2_engine import DEFAULT_MODEL_ID as _ID, DEFAULT_MODEL_REVISION as _REV

    assert embedding.DEFAULT_MODEL_ID == _ID
    assert embedding.DEFAULT_MODEL_REVISION == _REV


def test_unknown_attribute_still_raises_attributeerror():
    import embedding

    import pytest as _pytest

    with _pytest.raises(AttributeError):
        embedding.this_attribute_does_not_exist


def test_eager_torch_free_names_are_unaffected():
    """config/errors/result exports were never lazy and stay that way --
    only the dinov2_engine-derived names changed."""
    import embedding

    assert embedding.SegmentEmbedding is not None
    assert embedding.EmbeddingError is not None
    assert embedding.SegmentSamplingConfig is not None
