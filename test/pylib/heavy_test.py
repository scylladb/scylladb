#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Inter-process semaphore for limiting concurrent heavy test execution.

Provides a file-based semaphore that works across pytest-xdist worker processes.
Tests that request the `heavy` fixture will acquire a lock before running,
ensuring at most one heavy test executes at a time in debug/sanitize modes.

Usage in tests:
    async def test_big_cluster(heavy, manager, ...):
        ...

The `heavy` fixture (in cluster/conftest.py) gates execution in debug/sanitize
modes. In dev/release modes it's a no-op.
"""

from __future__ import annotations

import asyncio
import fcntl
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator


@asynccontextmanager
async def heavy_test_semaphore(base_dir: Path) -> AsyncIterator[None]:
    """Async context manager that serializes heavy tests across processes.

    Uses a file lock under `base_dir` to ensure at most one heavy test
    runs at a time. Waits asynchronously if the lock is held by another
    process.
    """
    lock_dir = base_dir / "heavy_locks"
    lock_dir.mkdir(parents=True, exist_ok=True)
    lock_path = lock_dir / "gate.lock"

    fd = os.open(str(lock_path), os.O_CREAT | os.O_WRONLY)
    try:
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except (OSError, BlockingIOError):
                await asyncio.sleep(0.5)
        yield
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)
