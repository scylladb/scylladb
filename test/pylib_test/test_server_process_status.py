#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Tests for reporting the status of the process of a server."""

import pathlib
import signal
from collections.abc import Iterator

import psutil
import pytest

from test.pylib.internal_types import ServerNum
from test.pylib.scylla_server import ScyllaServer
from test.pylib_test._sleeping_server import (
    cluster_running,
    kill_process,
    make_server,
    start_process,
    wait_until_not_stopped,
    wait_until_stopped,
)


@pytest.fixture
def server(tmp_path: pathlib.Path) -> Iterator[ScyllaServer]:
    server = make_server(tmp_path)
    yield server
    kill_process(server)


def test_status_of_server_without_process(server: ScyllaServer):
    assert server.process_status() == psutil.STATUS_DEAD


async def test_status_of_running_server(server: ScyllaServer):
    await start_process(server)

    # An idle process is sleeping; it's running only while it's on a CPU.
    assert server.process_status() in (psutil.STATUS_RUNNING, psutil.STATUS_SLEEPING)


async def test_status_of_stopped_server(server: ScyllaServer):
    await start_process(server)
    assert server.cmd
    server.cmd.send_signal(signal.SIGSTOP)

    await wait_until_stopped(server)


async def test_status_of_server_that_continued(server: ScyllaServer):
    await start_process(server)
    assert server.cmd
    server.cmd.send_signal(signal.SIGSTOP)
    await wait_until_stopped(server)

    server.cmd.send_signal(signal.SIGCONT)

    await wait_until_not_stopped(server)
    assert server.process_status() in (psutil.STATUS_RUNNING, psutil.STATUS_SLEEPING)


async def test_status_of_server_whose_process_exited(server: ScyllaServer):
    await start_process(server)
    assert server.cmd
    server.cmd.kill()
    await server.cmd.wait()

    assert server.process_status() == psutil.STATUS_DEAD


async def test_cluster_reports_status_of_running_server(server: ScyllaServer, tmp_path: pathlib.Path):
    await start_process(server)
    server_id = ServerNum(1)

    async with cluster_running(server, tmp_path) as cluster:
        assert await cluster.server_get_process_status(server_id) == server.process_status()
        assert server.cmd
        server.cmd.send_signal(signal.SIGSTOP)
        await wait_until_stopped(server)
        assert await cluster.server_get_process_status(server_id) == psutil.STATUS_STOPPED
