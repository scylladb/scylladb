#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""
Helpers for tests that need a ScyllaServer with a process they can signal.

No Scylla process is involved: the process of the server is a `sleep`.
"""

import asyncio
import logging
import os
import pathlib
import signal
import time
from collections.abc import Callable

import psutil

from test.pylib.internal_types import ServerNum
from test.pylib.scylla_server import ScyllaServer


def make_server(tmp_path: pathlib.Path) -> ScyllaServer:
    """Create a server without a process. See start_process()."""
    # Bypass the constructor: it needs a whole configuration, and the code
    # under test only looks at a handful of attributes.
    server = object.__new__(ScyllaServer)
    server.logger = logging.getLogger(__name__)
    server.workdir = tmp_path
    server.config = {"listen_address": "127.0.0.99"}
    server.server_id = ServerNum(1)
    server.control_connection = None
    server.control_cluster = None
    server.cmd = None
    # stop() does not care about the notify socket; keep it out of the way.
    server._cleanup_notify_socket = lambda: None  # type: ignore[method-assign]
    return server


async def start_process(server: ScyllaServer) -> None:
    """Give the server a process that does nothing.

    Return once the process has settled, i.e. it's sleeping. Right after it's
    spawned, it's still starting up and its status may change at any moment."""
    server.cmd = await asyncio.create_subprocess_exec("sleep", "600")
    await wait_until(lambda: server.process_status() == psutil.STATUS_SLEEPING)


def kill_process(server: ScyllaServer) -> None:
    """Kill the process of the server, if it's still around."""
    if server.cmd and server.cmd.returncode is None:
        # The process may be stopped; SIGKILL works regardless.
        os.kill(server.cmd.pid, signal.SIGKILL)


async def wait_until(condition: Callable[[], bool], timeout: float = 60) -> None:
    deadline = time.time() + timeout
    while not condition():
        assert time.time() < deadline, "timed out waiting for the process status to change"
        await asyncio.sleep(0.01)


async def wait_until_stopped(server: ScyllaServer) -> None:
    await wait_until(lambda: server.process_status() == psutil.STATUS_STOPPED)


async def wait_until_not_stopped(server: ScyllaServer) -> None:
    await wait_until(lambda: server.process_status() != psutil.STATUS_STOPPED)
