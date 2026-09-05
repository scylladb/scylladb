#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""Tests for what ScyllaCluster.add_server() does when a server cannot be built.

A server's configuration is assembled inside add_server(), before there is a
ScyllaServer to speak of, so the failure path has to cope with not having one.
No Scylla process is involved here: the configuration error comes first.
"""

import logging
import pathlib

import pytest

from test.pylib.host_registry import Host
from test.pylib.scylla_cluster import ScyllaCluster


IP_ADDR = "127.0.0.99"


async def test_add_server_reports_why_the_server_was_not_built(tmp_path: pathlib.Path,
                                                               monkeypatch: pytest.MonkeyPatch) -> None:
    """The error that stopped the server has to be the error the caller sees.

    "--smp" without its dashes is rejected by merge_cmdline_options, that is,
    while add_server() is still assembling the command line.
    """
    cluster = ScyllaCluster(
        logger=logging.getLogger(__name__),
        vardir=tmp_path,
        mode="dev",
        cmdline_options=[],
        cmdline_options_override=[],
        config_options={},
        append_env={},
        scylla_exe="/nonexistent/scylla",
    )

    # Lease one address of our own, to watch it being released.
    leased = set[str]()

    async def lease_host() -> str:
        leased.add(IP_ADDR)
        return IP_ADDR

    async def release_host(host: Host) -> None:
        leased.remove(host)

    monkeypatch.setattr(cluster.host_registry, "lease_host", lease_host)
    monkeypatch.setattr(cluster.host_registry, "release_host", release_host)

    with pytest.raises(ValueError, match="invalid argument name smp=2"):
        await cluster.add_server(cmdline=["smp=2"])

    assert not leased, "the address leased for the server was not released"
    assert not cluster.leased_ips
    assert not cluster.starting and not cluster.stopped and not cluster.running
