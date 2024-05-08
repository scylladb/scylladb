#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import pytest
import time
import logging

from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for_cql_and_get_hosts
from test.topology.util import reconnect_driver, enter_recovery_state, \
        delete_raft_data_and_upgrade_state, wait_until_upgrade_finishes, \
        wait_for_token_ring_and_group0_consistency
from test.topology.conftest import skip_mode


logger = logging.getLogger(__name__)


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_raft_fix_broken_snapshot(manager: ManagerClient):
    """Reproducer for scylladb/scylladb#16683.

       Simulate upgrade-to-Raft in old cluster (which doesn't have ff386e7a445)
       using RECOVERY mode and error injection.
       Then bootstrap a new server.

       Thanks to the new logic we will detect lack of snapshot and create one,
       which the new server will receive, resulting in correct schema transfer.
    """

    cfg = {'enable_user_defined_functions': False,
           'force_gossip_topology_changes': True,
           'error_injections_at_startup': ['raft_sys_table_storage::bootstrap/init_index_0']}
    srv = await manager.server_add(config=cfg)
    cql = manager.get_cql()
    h = (await wait_for_cql_and_get_hosts(cql, [srv], time.time() + 60))[0]

    # Enter RECOVERY mode, create a keyspace, leave RECOVERY to create new group 0
    # but with error injection that causes the snapshot to have index 0 (as in ScyllaDB 5.2).
    logger.info(f"Entering recovery state on {srv}")
    await enter_recovery_state(cql, h)
    await manager.server_restart(srv.server_id)
    cql = await reconnect_driver(manager)
    await wait_for_cql_and_get_hosts(cql, [srv], time.time() + 60)

    logger.info(f"Creating keyspace")
    await cql.run_async(
        "create keyspace ks with replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}")
    await cql.run_async("create table ks.t (pk int primary key)")

    logger.info(f"Leaving recovery state")
    await delete_raft_data_and_upgrade_state(cql, h)
    await manager.server_stop_gracefully(srv.server_id)
    await manager.server_start(srv.server_id)
    cql = await reconnect_driver(manager)
    await wait_for_cql_and_get_hosts(cql, [srv], time.time() + 60)

    logger.info(f"Waiting for group 0 upgrade to finish")
    await wait_until_upgrade_finishes(cql, h, time.time() + 60)

    # The Raft log will only contain this change,
    # older schema changes can only be obtained through snapshot transfer.
    await cql.run_async("create table ks.t2 (pk int primary key)")

    # Restarting the server should trigger snapshot creation.
    await manager.server_restart(srv.server_id)
    cql = await reconnect_driver(manager)
    await wait_for_cql_and_get_hosts(cql, [srv], time.time() + 60)

    await manager.server_add(config=cfg)
    await manager.server_sees_others(srv.server_id, 1)
    await wait_for_token_ring_and_group0_consistency(manager, time.time() + 60)

    # This would fail if snapshot creation wasn't triggered,
    # second node reporting 'Failed to apply mutation ... no_such_column_family`
    await cql.run_async("insert into ks.t (pk) values (0)", host=h)
