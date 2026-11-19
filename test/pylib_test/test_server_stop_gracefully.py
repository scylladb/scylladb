#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Tests for a ScyllaServer.stop_gracefully() that is cancelled before the process exits.

A paused process doesn't act on SIGTERM, so the graceful stop is still waiting
when it is cancelled.  The process has to stay with the server, so that stop()
can still kill it.
"""

import asyncio
import pathlib
from collections.abc import Iterator

import pytest

from test.pylib.scylla_server import ScyllaServer
from test.pylib_test._sleeping_server import kill_process, make_server, start_process, wait_until, wait_until_stopped


@pytest.fixture
def server(tmp_path: pathlib.Path) -> Iterator[ScyllaServer]:
    server = make_server(tmp_path)
    # make_server() bypasses the constructor, which creates these for stop_gracefully()'s decorators.
    server.stop_event = asyncio.Event()
    server.start_stop_lock = asyncio.Lock()
    yield server
    kill_process(server)


async def test_cancelled_graceful_stop_keeps_the_process(server: ScyllaServer) -> None:
    await start_process(server)
    server.pause()
    await wait_until_stopped(server)
    process = server.cmd
    assert process is not None
    try:
        stopping = asyncio.ensure_future(server.stop_gracefully(timeout=60))
        await asyncio.sleep(0.1)
        stopping.cancel()
        with pytest.raises(asyncio.CancelledError):
            await stopping

        assert server.cmd is process, "the cancelled graceful stop disowned a live process"
        assert process.returncode is None

        await server.stop()

        assert process.returncode is not None
        assert server.cmd is None
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()


class UnreapedProcess:
    """A process whose exit never gets confirmed: it ignores SIGTERM, and wait() blocks even after SIGKILL."""

    returncode = None
    killed = False

    def terminate(self) -> None:
        pass

    def kill(self) -> None:
        self.killed = True

    async def wait(self) -> int:
        await asyncio.Event().wait()
        return 0


async def test_graceful_stop_cancelled_after_the_kill_keeps_the_process(server: ScyllaServer) -> None:
    process = UnreapedProcess()
    server.cmd = process  # type: ignore[assignment]
    try:
        stopping = asyncio.ensure_future(server.stop_gracefully(timeout=0.1))
        await wait_until(lambda: process.killed)
        stopping.cancel()
        with pytest.raises(asyncio.CancelledError):
            await stopping

        assert server.cmd is process, "the cancelled graceful stop disowned a process it had not seen exit"
    finally:
        server.cmd = None
