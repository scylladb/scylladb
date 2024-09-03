#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
import pytest
from test.pylib.manager_client import ManagerClient
from test.pylib.scylla_cluster import ReplaceConfig

@pytest.mark.asyncio
async def test_replace_with_encryption(manager: ManagerClient):
    """Test that a node can be replaced if inter-dc encryption is enabled.
       The test creates 6 node cluster with two DCs and replaces one node in
       each DC"""
    config = {"endpoint_snitch": "GossipingPropertyFileSnitch"}
    property_file_dc1 = {"dc": "dc1", "rack": "myrack"}
    property_file_dc2 = {"dc": "dc2", "rack": "myrack"}

    s1 = await manager.servers_add(3,
            config=config,
            property_file=property_file_dc1,
            server_encryption="dc"
          )
    s2 = await manager.servers_add(3,
            config=config,
            property_file=property_file_dc2,
            server_encryption="dc"
          )

    await manager.server_stop_gracefully(s1[1].server_id)

    replace_cfg = ReplaceConfig(replaced_id = s1[1].server_id, reuse_ip_addr = False,
                                use_host_id = True)

    await manager.server_add(replace_cfg=replace_cfg, config=config,
                             property_file=property_file_dc1,
                             server_encryption="dc"
                            )

    await manager.server_stop_gracefully(s2[0].server_id)

    replace_cfg = ReplaceConfig(replaced_id = s2[0].server_id, reuse_ip_addr = False,
                                use_host_id = True)

    await manager.server_add(replace_cfg=replace_cfg, config=config,
                             property_file=property_file_dc2,
                             server_encryption="dc"
                            )
