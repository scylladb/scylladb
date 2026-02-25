#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
import pytest
from test.pylib.manager_client import ManagerClient
from test.pylib.scylla_cluster import ReplaceConfig


@pytest.mark.asyncio
@pytest.mark.parametrize("join_ring", [True, False])
async def test_replace_with_encryption(manager: ManagerClient, join_ring):
    """Test that a node can be replaced if inter-dc encryption is enabled.
       The test creates 6 node cluster with two DCs and replaces one node in
       each DC. The test is parametrized to confirm behavior in zero token node cases as well."""
    config = {"endpoint_snitch": "GossipingPropertyFileSnitch"}
    config_maybe_zero_token = {"endpoint_snitch": "GossipingPropertyFileSnitch", "join_ring": join_ring}
    property_file_dc1 = {"dc": "dc1", "rack": "myrack"}
    property_file_dc2 = {"dc": "dc2", "rack": "myrack"}

    await manager.servers_add(2,
            config=config,
            property_file=property_file_dc1,
            server_encryption="dc"
          )
    await manager.servers_add(2,
            config=config,
            property_file=property_file_dc2,
            server_encryption="dc"
          )

    s1 = await manager.server_add(config=config_maybe_zero_token,
                                  property_file=property_file_dc1,
                                  server_encryption="dc"
                                  )
    s2 = await manager.server_add(config=config_maybe_zero_token,
                                  property_file=property_file_dc2,
                                  server_encryption="dc"
                                  )

    await manager.server_stop_gracefully(s1.server_id)

    replace_cfg = ReplaceConfig(replaced_id = s1.server_id, reuse_ip_addr = False,
                                use_host_id = True)

    await manager.server_add(replace_cfg=replace_cfg, config=config_maybe_zero_token,
                             property_file=property_file_dc1,
                             server_encryption="dc"
                            )

    await manager.server_stop_gracefully(s2.server_id)

    replace_cfg = ReplaceConfig(replaced_id = s2.server_id, reuse_ip_addr = False,
                                use_host_id = True)

    await manager.server_add(replace_cfg=replace_cfg, config=config_maybe_zero_token,
                             property_file=property_file_dc2,
                             server_encryption="dc"
                            )
