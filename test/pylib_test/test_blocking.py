#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Tests for the dtest shim's synchronous access to async objects."""

import asyncio
from concurrent.futures import ThreadPoolExecutor

import pytest

from test.cluster.dtest.ccmlib.common import Blocking, run_sync


def test_run_sync_leaves_the_current_loop_alone():
    """run_sync() runs the coroutine on a loop of its own, closes it and keeps the thread's current loop current."""

    async def running_loop() -> asyncio.AbstractEventLoop:
        return asyncio.get_running_loop()

    def check() -> None:
        # A thread with a current loop that isn't running, like dtest's main
        # thread between the async fixtures of pytest-asyncio.
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            used = run_sync(running_loop())
            assert used is not loop
            assert used.is_closed()
            assert asyncio.get_event_loop() is loop
        finally:
            asyncio.set_event_loop(None)
            loop.close()

    with ThreadPoolExecutor(1) as executor:
        executor.submit(check).result()


class _Pending:
    def __await__(self):
        yield


class _Server:
    def __init__(self) -> None:
        self.name = "s1"

    async def add(self, a: int, b: int) -> int:
        await asyncio.sleep(0)
        return a + b

    def describe(self) -> str:
        return f"server {self.name}"

    def pending(self) -> _Pending:
        return _Pending()


def test_blocking_runs_coroutines_and_passes_the_rest_through():
    """Blocking runs calls that return a coroutine, rejects other awaitables and passes everything else through."""

    server = _Server()
    view = Blocking(server)

    assert view.add(1, 2) == 3
    assert view.describe() == "server s1"
    assert view.name == "s1"
    with pytest.raises(TypeError, match="cannot run"):
        view.pending()

    view.name = "s2"
    assert server.name == "s2"
