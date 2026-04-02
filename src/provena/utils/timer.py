"""Execution timer context manager."""

from __future__ import annotations

import time
from contextlib import contextmanager
from typing import Generator


@contextmanager
def execution_timer() -> Generator[dict[str, float], None, None]:
    """Context manager that tracks execution time in milliseconds."""
    result: dict[str, float] = {"elapsed_ms": 0.0}
    start = time.monotonic()
    try:
        yield result
    finally:
        result["elapsed_ms"] = (time.monotonic() - start) * 1000
