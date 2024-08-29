#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
import pytest
import logging
import time

from cassandra.cluster import ConsistencyLevel

from test.pylib.manager_client import ManagerClient
from test.pylib.scylla_cluster import ReplaceConfig
from test.pylib.util import wait_for_cql_and_get_hosts
from test.topology.util import check_node_log_for_failed_mutations, start_writes


@pytest.mark.asyncio
@pytest.mark.parametrize('tablets_enabled', [True, False])
async def test_zero_token_nodes_topology_ops(manager: ManagerClient, tablets_enabled: bool):
    """
    Test that:
    - adding a zero-token node in the gossip-based topology fails
    - topology operations in the Raft-based topology involving zero-token nodes succeed
    - client requests to normal nodes in the presence of zero-token nodes (2 normal nodes, RF=2, CL=2) succeed
    """
    logging.info('Trying to add a zero-token server in the gossip-based topology')
    await manager.server_add(config={'join_ring': False, 'force_gossip_topology_changes': True},
                             expected_error='the raft-based topology is disabled')

    normal_cfg = {'enable_tablets': tablets_enabled}
    zero_token_cfg = {'enable_tablets': tablets_enabled, 'join_ring': False}

    logging.info('Adding the first server')
    server_a = await manager.server_add(config=normal_cfg)

    logging.info('Adding the second server as zero-token')
    server_b = await manager.server_add(config=zero_token_cfg)

    logging.info('Adding the third server')
    server_c = await manager.server_add(config=normal_cfg)

    await wait_for_cql_and_get_hosts(manager.cql, [server_a, server_c], time.time() + 60)
    finish_writes = await start_writes(manager.cql, 2, ConsistencyLevel.TWO)

    logging.info('Adding the fourth server as zero-token')
    await manager.server_add(config=zero_token_cfg)  # Necessary to preserve the Raft majority.

    logging.info(f'Restarting {server_b}')
    await manager.server_stop_gracefully(server_b.server_id)
    await manager.server_start(server_b.server_id)

    logging.info(f'Stopping {server_b}')
    await manager.server_stop_gracefully(server_b.server_id)

    replace_cfg_b = ReplaceConfig(replaced_id=server_b.server_id, reuse_ip_addr=False, use_host_id=False)
    logging.info(f'Trying to replace {server_b} with a token-owing server')
    await manager.server_add(replace_cfg_b, config=normal_cfg, expected_error='Cannot replace the zero-token node')

    logging.info(f'Replacing {server_b}')
    server_b = await manager.server_add(replace_cfg_b, config=zero_token_cfg)

    logging.info(f'Stopping {server_b}')
    await manager.server_stop_gracefully(server_b.server_id)

    replace_cfg_b = ReplaceConfig(replaced_id=server_b.server_id, reuse_ip_addr=True, use_host_id=False)
    logging.info(f'Replacing {server_b} with the same IP')
    server_b = await manager.server_add(replace_cfg_b, config=zero_token_cfg)

    logging.info(f'Decommissioning {server_b}')
    await manager.decommission_node(server_b.server_id)

    logging.info('Adding two zero-token servers')
    [server_b, server_d] = await manager.servers_add(2, config=zero_token_cfg)

    logging.info(f'Rebuilding {server_b}')
    await manager.rebuild_node(server_b.server_id)

    logging.info(f'Stopping {server_b}')
    await manager.server_stop_gracefully(server_b.server_id)

    logging.info(f'Stopping {server_d}')
    await manager.server_stop_gracefully(server_d.server_id)

    logging.info(f'Initiating removenode of {server_b} by {server_a}, ignore_dead={[server_d.ip_addr]}')
    await manager.remove_node(server_a.server_id, server_b.server_id, [server_d.ip_addr])

    logging.info(f'Initiating removenode of {server_d} by {server_a}')
    await manager.remove_node(server_a.server_id, server_d.server_id)

    logging.info('Adding a zero-token server')
    await manager.server_add(config=zero_token_cfg)

    # FIXME: Finish writes after the last server_add call once scylladb/scylladb#19737 is fixed.
    logging.info('Checking results of the background writes')
    await finish_writes()

    logging.info('Adding a normal server')
    server_e = await manager.server_add(config=normal_cfg)

    logging.info(f'Stopping {server_e}')
    await manager.server_stop_gracefully(server_e.server_id)

    replace_cfg_e = ReplaceConfig(replaced_id=server_e.server_id, reuse_ip_addr=False, use_host_id=False)
    logging.info(f'Trying to replace {server_e} with a zero-token server')
    await manager.server_add(replace_cfg_e, config=zero_token_cfg,
                             expected_error='Cannot replace the token-owning node')

    await check_node_log_for_failed_mutations(manager, server_a)
    await check_node_log_for_failed_mutations(manager, server_c)
