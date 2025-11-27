#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import time
import asyncio
import logging
import pytest
from test.pylib.rest_client import read_barrier, get_host_api_address
from test.pylib.util import unique_name, wait_for_cql_and_get_hosts
from test.pylib.manager_client import ManagerClient
from test.cluster.auth_cluster import extra_scylla_config_options as auth_config

@pytest.mark.asyncio
async def __test_attach_service_level_to_user(request, manager: ManagerClient, is_raft: bool):
    user = f"test_user_{unique_name()}"

    # Start nodes with correct topology
    if is_raft:
        servers = await manager.servers_add(3, config=auth_config)
    else:
        conf = {**auth_config, 'force_gossip_topology_changes': True, 'tablets_mode_for_new_keyspaces': 'disabled'}
        servers = [await manager.server_add(config=conf) for _ in range(3)]

    cql = manager.get_cql()
    logging.info("Waiting until driver connects to every server")
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    ips = [get_host_api_address(host) for host in hosts]

    logging.info("Creating User")
    await cql.run_async(f"CREATE ROLE {user} WITH login = true AND password='{user}' AND superuser = true")

    connections = await cql.run_async(f"SELECT username, scheduling_group, shard_id FROM system.clients WHERE client_type='cql' AND username='{user}' ALLOW FILTERING")

    verify_service_level = lambda sl : all([conn.scheduling_group == sl for conn in connections])
    assert verify_service_level("default"), "All connections should be in default service level"

    logging.info("Creating service levels")
    sls = ["sl" + unique_name() for _ in range(2)]
    for i, sl in enumerate(sls):
        await cql.run_async(f"CREATE SERVICE LEVEL {sl} WITH shares = {100 * (i+1)}")

    logging.info("Attach Service Levels to user")
    for sl in sls:
        await cql.run_async(f"ATTACH SERVICE LEVEL {sl} TO {user}")

        #if we are not using raft we have to switch the tenant and wait for it to take effect
        if not is_raft:
            for ip in ips:
                await manager.api.client.post('/service_levels/switch_tenants', host=ip)
                # Switching tenants may be blocked if a connection is waiting for a request (see 'generic_server::connection::process_until_tenant_switch()').
                # Execute enough cheap statements, so that connection on each shard will process at one statement and update its tenant.
                for _ in range(100):
                    read_barrier(manager.api, ip)

        assert verify_service_level(sl), f"All connections should be in {sl} service level"
        await cql.run_async(f"DETACH SERVICE LEVEL FROM {user}")

    await cql.run_async(f"DROP ROLE {user}")
    for sl in sls:
        await cql.run_async(f"DROP SERVICE LEVEL {sl}")


@pytest.mark.asyncio
async def test_attach_service_level_with_raft(request, manager: ManagerClient):
    await __test_attach_service_level_to_user(request, manager, is_raft=True)


@pytest.mark.asyncio
async def test_attach_service_level_with_gossip(request, manager: ManagerClient):
    await __test_attach_service_level_to_user(request, manager, is_raft=False)