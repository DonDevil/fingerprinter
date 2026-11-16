"""Explicit Hugging Face Hub offline mode for model loading.

``local_files_only=True`` only stops *file resolution* from hitting the Hub.
huggingface_hub >= 1.x also builds a User-Agent for its request helpers, which
(unless ``HF_HUB_OFFLINE`` is set) fetches ``GET {endpoint}/api/agent-harnesses``
(AI-agent detection registry): the unintended request seen in the 2026-11-16
Blast test (validation.md §13.38).

``HF_HUB_OFFLINE`` is process-wide and is read into
``huggingface_hub.constants`` at import time, so both the env var and the
imported constant are set for the duration of a load and then restored.
A re-entrant lock is held for the whole load: concurrent loads in one process
are serialized, so one loader's setting can never be restored under, or
observed by, another loader (loads happen once at startup, so the cost is nil).
"""

from __future__ import annotations

import contextlib
import os
import threading
from collections.abc import Iterator

_LOCK = threading.RLock()


@contextlib.contextmanager
def hub_offline(enabled: bool) -> Iterator[None]:
    """Force huggingface_hub offline inside the block when ``enabled`` (no-op otherwise)."""
    if not enabled:
        yield
        return
    from huggingface_hub import constants

    with _LOCK:
        old_env = os.environ.get("HF_HUB_OFFLINE")
        old_const = constants.HF_HUB_OFFLINE
        os.environ["HF_HUB_OFFLINE"] = "1"
        constants.HF_HUB_OFFLINE = True
        try:
            yield
        finally:
            constants.HF_HUB_OFFLINE = old_const
            if old_env is None:
                os.environ.pop("HF_HUB_OFFLINE", None)
            else:
                os.environ["HF_HUB_OFFLINE"] = old_env
