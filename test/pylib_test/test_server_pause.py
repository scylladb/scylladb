#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""
Tests for pausing and unpausing a server.

Whether a server is paused is decided by the status of its process. The process
may be stopped by something else than pause(), e.g. a SIGSTOP error injection,
and a signal takes a moment to take effect, so the manager operations wait for
the status to change before returning.
"""

import logging
import os
import pathlib
import signal
import threading
from collections.abc import AsyncIterator, Iterator

import psutil
import pytest

from test.pylib.internal_types import ServerNum
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.scylla_server import ScyllaServer
from test.pylib_test._sleeping_server import (
    kill_process,
    make_server,
    start_process,
    wait_until_not_stopped,
    wait_until_stopped,
)

SERVER_ID = ServerNum(1)
SIGNAL_DELAY = 0.3


@pytest.fixture
def server(tmp_path: pathlib.Path) -> Iterator[ScyllaServer]:
    server = make_server(tmp_path)
    yield server
    kill_process(server)


async def test_pause_stops_the_process(server: ScyllaServer):
    await start_process(server)

    server.pause()

    await wait_until_stopped(server)


async def test_unpause_continues_the_process(server: ScyllaServer):
    await start_process(server)
    server.pause()
    await wait_until_stopped(server)

    server.unpause()

    await wait_until_not_stopped(server)


async def test_pause_asserts_if_already_paused(server: ScyllaServer):
    await start_process(server)
    server.pause()
    await wait_until_stopped(server)

    with pytest.raises(AssertionError, match="already paused"):
        server.pause()


async def test_unpause_asserts_if_not_paused(server: ScyllaServer):
    await start_process(server)

    with pytest.raises(AssertionError, match="not paused"):
        server.unpause()


async def test_unpause_of_server_that_stopped_itself(server: ScyllaServer):
    # This is what the SIGSTOP error injections do: the server stops itself and
    # pause() is never called.
    await start_process(server)
    assert server.cmd
    server.cmd.send_signal(signal.SIGSTOP)
    await wait_until_stopped(server)

    server.unpause()

    await wait_until_not_stopped(server)


class StubCluster:
    """Stands in for ScyllaCluster, delivering the signals with a delay.

    A real server doesn't change its status the moment the signal is sent, and
    the manager must not return before it does.
    """

    def __init__(self, server: ScyllaServer) -> None:
        self.server = server
        self.api = None
        self.logger = logging.getLogger("test_server_pause")

    def _send_later(self, sig: signal.Signals) -> None:
        assert self.server.cmd
        threading.Timer(SIGNAL_DELAY, os.kill, (self.server.cmd.pid, sig)).start()

    def server_pause(self, _server_id: ServerNum) -> None:
        self._send_later(signal.SIGSTOP)

    def server_unpause(self, _server_id: ServerNum) -> None:
        self._send_later(signal.SIGCONT)

    def server_get_process_status(self, _server_id: ServerNum) -> str:
        return self.server.process_status()

    async def stop(self) -> None:
        pass


@pytest.fixture
async def manager(server: ScyllaServer) -> AsyncIterator[ScyllaClusterManager]:
    await start_process(server)
    mgr = ScyllaClusterManager(
        test_name="test_server_pause::stub",
        cluster=StubCluster(server),  # type: ignore[arg-type]
        port=9042,
        use_ssl=False,
    )
    async with mgr.run_in_thread():
        yield mgr


async def test_manager_waits_for_server_to_pause(manager: ScyllaClusterManager, server: ScyllaServer):
    await manager.server_pause(SERVER_ID)

    # No waiting here: the signal was delivered after a delay, so the process
    # would be still running if server_pause() hadn't waited for it.
    assert server.process_status() == psutil.STATUS_STOPPED


async def test_manager_waits_for_server_to_unpause(manager: ScyllaClusterManager, server: ScyllaServer):
    server.pause()
    await wait_until_stopped(server)

    await manager.server_unpause(SERVER_ID)

    # No waiting here: see test_manager_waits_for_server_to_pause.
    assert server.process_status() != psutil.STATUS_STOPPED
