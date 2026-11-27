#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""
Tests for pausing and unpausing a server.

Whether a server is paused is decided by the status of its process. The process
may be stopped by something else than pause(), e.g. a SIGSTOP error injection,
and a signal takes a moment to take effect, so the cluster operations wait for
the status to change before returning.
"""

import os
import pathlib
import signal
import threading
from collections.abc import AsyncIterator, Iterator

import psutil
import pytest

from test.pylib.internal_types import ServerNum
from test.pylib.scylla_cluster import ScyllaCluster
from test.pylib.scylla_server import ScyllaServer
from test.pylib_test._sleeping_server import (
    cluster_running,
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


@pytest.fixture
async def cluster(server: ScyllaServer, tmp_path: pathlib.Path) -> AsyncIterator[ScyllaCluster]:
    """A cluster whose server gets the signals of pause() and unpause() with a delay.

    A real server doesn't change its status the moment the signal is sent, and
    the cluster must not return before it does.
    """
    await start_process(server)

    def send_later(sig: signal.Signals) -> None:
        assert server.cmd
        threading.Timer(SIGNAL_DELAY, os.kill, (server.cmd.pid, sig)).start()

    server.pause = lambda: send_later(signal.SIGSTOP)  # type: ignore[method-assign]
    server.unpause = lambda: send_later(signal.SIGCONT)  # type: ignore[method-assign]
    async with cluster_running(server, tmp_path) as cluster:
        yield cluster


async def test_cluster_waits_for_server_to_pause(cluster: ScyllaCluster, server: ScyllaServer):
    await cluster.server_pause(SERVER_ID)

    # No waiting here: the signal was delivered after a delay, so the process
    # would be still running if server_pause() hadn't waited for it.
    assert server.process_status() == psutil.STATUS_STOPPED


async def test_cluster_waits_for_server_to_unpause(cluster: ScyllaCluster, server: ScyllaServer):
    server.pause()
    await wait_until_stopped(server)

    await cluster.server_unpause(SERVER_ID)

    # No waiting here: see test_cluster_waits_for_server_to_pause.
    assert server.process_status() != psutil.STATUS_STOPPED
