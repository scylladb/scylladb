#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from test.pylib.manager_client import ManagerClient
from test.pylib.scylla_cluster import ReplaceConfig
import pytest
import uuid
import logging

logger = logging.getLogger(__name__)

@pytest.mark.asyncio
async def test_global_ignored_nodes_list(manager: ManagerClient, random_tables) -> None:
    """This test creates a 5 node cluster with two nodes down (A and B). It removes A while
       specifying B in ignored nodes list. Then is downs one more node and replaces it while
       B is down and still in the cluster without specifying it as ignored, but it still succeeds
       since ignore node is permanent now and B is removed from the quorum early so it is enough to
       have two live nodes for the quorum.
    """
    await manager.servers_add(2)
    servers = await manager.running_servers()
    await manager.server_stop_gracefully(servers[3].server_id)
    await manager.server_stop_gracefully(servers[4].server_id)
    # test that non existing uuid is rejected
    rnd_id = str(uuid.uuid4())
    await manager.remove_node(servers[1].server_id, servers[4].server_id, [rnd_id],
                        expected_error=f"Node {rnd_id} is not found in the cluster")
    # now remove one dead node while ignoring another one
    s3_id = await manager.get_host_id(servers[3].server_id)
    await manager.remove_node(servers[1].server_id, servers[4].server_id, [s3_id])
    # down one more node and try to replace it without providing server 3 as ignored
    # this has to succeed since it was ignored previously and is non voter now so the quorum
    # is 2
    await manager.server_stop_gracefully(servers[2].server_id)
    replace_cfg = ReplaceConfig(replaced_id = servers[2].server_id, reuse_ip_addr = False, use_host_id = True)
    await manager.server_add(start=False, replace_cfg=replace_cfg)


