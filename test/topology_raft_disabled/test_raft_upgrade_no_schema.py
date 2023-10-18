#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import asyncio
import pytest
import logging
import time
from test.pylib.manager_client import ManagerClient
from test.pylib.random_tables import RandomTables
from test.pylib.util import wait_for_cql_and_get_hosts, wait_for_feature
from test.topology.util import reconnect_driver
from test.topology_raft_disabled.util import restart, enable_raft, \
        enable_raft_and_restart, wait_for_upgrade_state, wait_until_upgrade_finishes, \
        delete_raft_data, log_run_time

@pytest.mark.asyncio
@log_run_time
async def test_durability_with_no_schema_commitlog (manager: ManagerClient):
    new_server_info = await manager.server_add(config={
        'consistent_cluster_management': False,
        'force_schema_commit_log': False,
        'flush_schema_tables_after_modification': True
    })
    await manager.server_stop(new_server_info.server_id)
    await manager.server_start(new_server_info.server_id)


@pytest.mark.asyncio
@log_run_time
async def test_upgrade_with_no_schema_commitlog(manager: ManagerClient, random_tables: RandomTables):
    # RAFT without schema_commit_log should produce error at node startup
    # We can't test this on the already running nodes, since schema commitlog feature
    # may have already been enabled on them
    new_server_info = await manager.server_add(start=False, config={
        'consistent_cluster_management': 'True',
        'force_schema_commit_log': 'False'
    })
    await manager.server_start(new_server_info.server_id,
                               "Bad configuration: consistent_cluster_management "
                               "requires schema commit log to be enabled")

    # Rollback RAFT and restart new node
    # Now it's the same as initially running nodes, except with force_schema_commit_log = False
    await manager.server_update_config(new_server_info.server_id, 'consistent_cluster_management', False)
    await manager.server_stop_gracefully(new_server_info.server_id)
    await manager.server_start(new_server_info.server_id)
    cql = await reconnect_driver(manager)
    servers = await manager.running_servers()

    # Wait for schema commitlog feature on all nodes, including the new one.
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    await asyncio.gather(*(wait_for_feature("SCHEMA_COMMITLOG", cql, h, time.time() + 60) for h in hosts))

    # restart all the nodes with RAFT enabled
    await asyncio.gather(*(enable_raft_and_restart(manager, srv) for srv in servers))
    cql = await reconnect_driver(manager)
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    # wait until upgrade finishes
    await asyncio.gather(*(wait_until_upgrade_finishes(cql, h, time.time() + 60) for h in hosts))

    # upgrade finished, try to create a new table
    table = await random_tables.add_table(ncolumns=5)

    # check group history
    rs = await cql.run_async("select * from system.group0_history")
    assert(rs)
    logging.info(f"group0_history entry description: '{rs[0].description}'")
    assert(table.full_name in rs[0].description)


