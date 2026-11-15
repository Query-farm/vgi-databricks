"""Shared fixtures: the strict Databricks replay server."""

from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

from databricks_worker.client import clear_caches

ROOT = Path(__file__).resolve().parent.parent


def _serve(**env: str) -> Iterator[str]:
    proc = subprocess.Popen(
        [sys.executable, str(ROOT / "test" / "mock_api" / "server.py")],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        env={**os.environ, **env},
    )
    assert proc.stdout is not None
    line = proc.stdout.readline().strip()
    assert line.startswith("PORT:"), line
    yield f"http://127.0.0.1:{line.removeprefix('PORT:')}"
    proc.terminate()
    proc.wait(timeout=10)


@pytest.fixture(scope="session")
def mock_api() -> Iterator[str]:
    """Replay that answers the first request to every path with 429 and pages task arrays by two."""
    yield from _serve(MOCK_FLAKY="1", MOCK_TASK_PAGE="2")


@pytest.fixture(scope="session")
def mock_plain() -> Iterator[str]:
    """Replay without rate limiting, for request-log assertions."""
    yield from _serve()


@pytest.fixture(autouse=True)
def _fresh_caches() -> None:
    clear_caches()
