#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import time

from test.pylib.internal_types import ServerInfo
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for
import pytest

# system.cluster_status shows one row per node, as the queried node sees it:
# its address, dc, host id, ownership, status and whether it is up.
# scylla-dtest gates these tests on release mode only.
pytestmark = pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")

VNODES = {"tablets_mode_for_new_keyspaces": "disabled"}
# The dc of nodes started without a property file.
DEFAULT_DC = "datacenter1"


async def cluster_status(cql) -> dict:
    return {row.peer: row for row in await cql.run_async("SELECT * FROM system.cluster_status")}


async def check_running(manager: ScyllaClusterManager, row, server: ServerInfo, dc: str):
    assert row.dc == dc
    assert str(row.host_id) == await manager.get_host_id(server.server_id)
    assert row.status.upper() == "NORMAL"
    assert row.up


async def test_content_all_nodes_are_up(manager: ScyllaClusterManager):
    servers = await manager.servers_add(3, config=VNODES)
    status = await cluster_status(await manager.get_cql_exclusive(servers[0]))
    for s in servers:
        await check_running(manager, status[s.ip_addr], s, DEFAULT_DC)


async def test_content_add_new_node(manager: ScyllaClusterManager):
    servers = [await manager.server_add(config=VNODES)]
    servers.append(await manager.server_add(config=VNODES))
    await manager.server_sees_other_server(servers[0].ip_addr, servers[1].ip_addr)
    status = await cluster_status(await manager.get_cql_exclusive(servers[0]))
    assert len(status) == len(servers)
    assert round(sum(row.owns for row in status.values()), 4) == 1
    await check_running(manager, status[servers[1].ip_addr], servers[1], DEFAULT_DC)


async def test_content_stop_node(manager: ScyllaClusterManager):
    servers = await manager.servers_add(2, config=VNODES)
    await manager.server_stop_gracefully(servers[1].server_id)
    cql = await manager.get_cql_exclusive(servers[0])

    async def stopped_is_shown_down():
        row = (await cluster_status(cql))[servers[1].ip_addr]
        return True if not row.up and row.status.upper() == "SHUTDOWN" else None
    await wait_for(stopped_is_shown_down, time.time() + 60)


async def test_content_remove_node(manager: ScyllaClusterManager):
    servers = await manager.servers_add(3, config=VNODES)
    await manager.server_stop(servers[1].server_id, convict=False)
    await manager.remove_node(servers[0].server_id, servers[1].server_id)
    cql = await manager.get_cql_exclusive(servers[0])

    async def removed_is_gone():
        return True if servers[1].ip_addr not in await cluster_status(cql) else None
    await wait_for(removed_is_gone, time.time() + 60)


async def test_content_multi_dc_all_nodes_are_up(manager: ScyllaClusterManager):
    servers = await manager.servers_add(2, property_file=[{"dc": "dc1", "rack": "r1"}, {"dc": "dc2", "rack": "r1"}], config=VNODES)
    cql = await manager.get_cql_exclusive(servers[0])
    status = await cluster_status(cql)
    for s in servers:
        await check_running(manager, status[s.ip_addr], s, s.datacenter)
    servers.append(await manager.server_add(property_file={"dc": "dc2", "rack": "r2"}, config=VNODES))
    await manager.server_sees_other_server(servers[0].ip_addr, servers[2].ip_addr)
    status = await cluster_status(cql)
    assert round(sum(row.owns for row in status.values()), 4) == 1
    await check_running(manager, status[servers[2].ip_addr], servers[2], "dc2")
