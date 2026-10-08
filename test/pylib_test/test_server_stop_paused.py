#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""
Tests for how ScyllaServer.stop() treats a paused server.

A server paused with SIGSTOP cannot answer REST requests, so stop() must not
try to dump the LLVM profile through the API: the request would only block
until it times out. No Scylla process is involved; the process is a stand-in.
"""

import logging
import pathlib
import signal
from typing import Any

import pytest

from test.pylib import scylla_server
from test.pylib.scylla_server import ScyllaServer


class FakeProcess:
    """Stands in for the asyncio subprocess of a running server."""

    def __init__(self) -> None:
        self.returncode: int | None = None
        self.signals: list[signal.Signals] = []

    def send_signal(self, sig: signal.Signals) -> None:
        self.signals.append(sig)

    def kill(self) -> None:
        self.returncode = -signal.SIGKILL

    async def wait(self) -> int | None:
        return self.returncode


@pytest.fixture
def profile_dumps(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """The addresses for which an LLVM profile dump was requested."""
    dumps = list[str]()

    class FakeRESTAPIClient:
        async def dump_llvm_profile(self, ip_addr: str) -> None:
            dumps.append(ip_addr)

    monkeypatch.setattr(scylla_server, "ScyllaRESTAPIClient", FakeRESTAPIClient)
    return dumps


def make_server(tmp_path: pathlib.Path) -> ScyllaServer:
    # Bypass the constructor: it needs a whole configuration, and stop() only
    # looks at a handful of attributes.
    server = object.__new__(ScyllaServer)
    server.logger = logging.getLogger(__name__)
    server.workdir = tmp_path
    server.config = {"listen_address": "127.0.0.99"}
    server.server_id = 1
    server.paused = False
    server.control_connection = None
    server.control_cluster = None
    server.cmd = FakeProcess()  # type: ignore[assignment]
    # stop() does not care about the notify socket; keep it out of the way.
    server._cleanup_notify_socket = lambda: None  # type: ignore[method-assign]
    return server


async def test_stop_dumps_profile_of_running_server(tmp_path: pathlib.Path, profile_dumps: list[str]):
    server = make_server(tmp_path)

    await server.stop()

    assert profile_dumps == [server.ip_addr]
    assert server.cmd is None


async def test_stop_skips_profile_dump_of_paused_server(tmp_path: pathlib.Path, profile_dumps: list[str]):
    server = make_server(tmp_path)
    process: Any = server.cmd

    server.pause()
    assert server.paused
    assert process.signals == [signal.SIGSTOP]

    await server.stop()

    assert profile_dumps == []
    assert process.returncode == -signal.SIGKILL
    assert server.cmd is None


async def test_unpause_makes_server_answer_again(tmp_path: pathlib.Path, profile_dumps: list[str]):
    server = make_server(tmp_path)
    process: Any = server.cmd

    server.pause()
    server.unpause()
    assert not server.paused
    assert process.signals == [signal.SIGSTOP, signal.SIGCONT]

    await server.stop()

    assert profile_dumps == [server.ip_addr]
