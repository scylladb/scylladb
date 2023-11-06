#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from test.pylib.scylla_cluster import ReplaceConfig
from test.pylib.manager_client import ManagerClient
import asyncio
import pytest


@pytest.mark.asyncio
async def test_replacing_alive_node_fails(manager: ManagerClient) -> None:
    """Try replacing an alive node and check that it fails"""
    servers = await manager.running_servers()
    await asyncio.gather(*(manager.server_sees_others(srv.server_id, len(servers) - 1) for srv in servers))

    # We test for every server because we expect a different error depending on
    # whether we try to replace the topology coordinator. We want to test both cases.
    # Both errors contain the expected string below.
    for srv in servers:
        replace_cfg = ReplaceConfig(replaced_id = srv.server_id, reuse_ip_addr = False,
                                    use_host_id = False, wait_replaced_dead = False)
        await manager.server_add(replace_cfg=replace_cfg,
                                 expected_error="the topology coordinator rejected request to join the cluster")
