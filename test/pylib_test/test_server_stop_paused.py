#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""
Tests for how ScyllaServer.stop() treats a server whose process is stopped.

Such a server cannot answer REST requests, so stop() must not try to dump the
LLVM profile through the API: the request would only block until it times out.
"""

import pathlib
import signal
from collections.abc import Iterator

import pytest

from test.pylib import scylla_server
from test.pylib.scylla_server import ScyllaServer
from test.pylib_test._sleeping_server import kill_process, make_server, start_process, wait_until_not_stopped, wait_until_stopped


@pytest.fixture
def profile_dumps(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """The addresses for which an LLVM profile dump was requested."""
    dumps = list[str]()

    class FakeRESTAPIClient:
        async def dump_llvm_profile(self, ip_addr: str) -> None:
            dumps.append(ip_addr)

    monkeypatch.setattr(scylla_server, "ScyllaRESTAPIClient", FakeRESTAPIClient)
    return dumps


@pytest.fixture
def server(tmp_path: pathlib.Path) -> Iterator[ScyllaServer]:
    server = make_server(tmp_path)
    yield server
    kill_process(server)


async def test_stop_dumps_profile_of_running_server(server: ScyllaServer, profile_dumps: list[str]):
    await start_process(server)

    await server.stop()

    assert profile_dumps == [server.ip_addr]
    assert server.cmd is None


async def test_stop_skips_profile_dump_of_paused_server(server: ScyllaServer, profile_dumps: list[str]):
    await start_process(server)
    server.pause()
    await wait_until_stopped(server)

    await server.stop()

    assert profile_dumps == []
    assert server.cmd is None


async def test_stop_skips_profile_dump_of_server_that_stopped_itself(server: ScyllaServer, profile_dumps: list[str]):
    # This is what the SIGSTOP error injections do: the server stops itself and
    # pause() is never called.
    await start_process(server)
    assert server.cmd
    server.cmd.send_signal(signal.SIGSTOP)
    await wait_until_stopped(server)

    await server.stop()

    assert profile_dumps == []
    assert server.cmd is None


async def test_stop_dumps_profile_of_unpaused_server(server: ScyllaServer, profile_dumps: list[str]):
    await start_process(server)
    server.pause()
    await wait_until_stopped(server)
    server.unpause()
    await wait_until_not_stopped(server)

    await server.stop()

    assert profile_dumps == [server.ip_addr]
