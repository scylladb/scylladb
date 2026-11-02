#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""ScyllaServer.start(cmdline_options_override=...) keeps the harness defaults.

A restart that drops `--smp 2 -m 1G --overprovisioned ...` lets one node take the
whole CI host. No Scylla process is involved here: the spawn is stubbed and only
the argv it would get is checked.
"""

import asyncio
import logging
import pathlib

import pytest

from test.pylib.internal_types import ServerUpState
from test.pylib.scylla_server import (SCYLLA_CMDLINE_OPTIONS, ScyllaServer, ScyllaVersionDescription,
                                      merge_cmdline_options)


async def test_start_override_merges_into_the_harness_defaults(tmp_path: pathlib.Path,
                                                               monkeypatch: pytest.MonkeyPatch) -> None:
    spawned: list[list[str]] = []

    async def fake_exec(exe: pathlib.Path, *args: str, **kwargs) -> object:
        spawned.append(list(args))
        return object()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec)
    server = ScyllaServer(
        logger=logging.getLogger(__name__),
        vardir=tmp_path,
        version=ScyllaVersionDescription(path="/nonexistent/scylla", config={}, argv=[]),
        cmdline_options=merge_cmdline_options(SCYLLA_CMDLINE_OPTIONS, ["--smp", "1"]),
        config_options={},
        property_file={},
        append_env={},
    )
    def smp(argv: list[str]) -> str:
        assert argv.count("--smp") == 1
        return argv[argv.index("--smp") + 1]

    try:
        await server.start(api=None, expected_server_up_state=ServerUpState.PROCESS_STARTED,
                           cmdline_options_override=["--smp", "4", "--logger-log-level", "view=debug"])
        [argv] = spawned
        assert smp(argv) == "4"
        assert "-m" in argv and "--overprovisioned" in argv, "the harness defaults were dropped"
        assert "view=debug" in argv and "raft_topology=debug" in argv, "--logger-log-level must append"
        assert smp(server.cmdline_options) == "1", "the override must not persist"

        await server.start(api=None, expected_server_up_state=ServerUpState.PROCESS_STARTED)
        assert smp(spawned[1]) == "1" and "view=debug" not in spawned[1], "the override leaked into a plain restart"

        await server.start(api=None, expected_server_up_state=ServerUpState.PROCESS_STARTED,
                           cmdline_options_override=["--smp", "4"], replace_cmdline_options=True)
        assert spawned[2] == ["--smp", "4"]
    finally:
        server._cleanup_notify_socket()
