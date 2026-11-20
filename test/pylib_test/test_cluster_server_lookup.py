#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Tests for how ScyllaCluster operations treat a server ID the cluster doesn't know."""

import pathlib

import pytest

from test.pylib.internal_types import ServerNum
from test.pylib_test._cluster import make_cluster


@pytest.mark.parametrize("call", [
    lambda cluster: cluster.server_get_config(ServerNum(1)),
    lambda cluster: cluster.server_get_workdir(ServerNum(1)),
    lambda cluster: cluster.server_switch_executable(ServerNum(1), "/nonexistent/scylla"),
], ids=["server_get_config", "server_get_workdir", "server_switch_executable"])
async def test_unknown_server_is_reported(tmp_path: pathlib.Path, call) -> None:
    cluster = make_cluster(tmp_path)
    try:
        async with cluster.run_in_thread():
            with pytest.raises(AssertionError, match="Server 1 unknown"):
                await call(cluster)
    finally:
        await cluster.recycle()
