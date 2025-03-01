#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
import pytest
import time
import asyncio
import logging
from test.pylib.rest_client import get_host_api_address, read_barrier
from test.pylib.util import unique_name, wait_for_cql_and_get_hosts
from test.pylib.manager_client import ManagerClient
from test.topology.util import trigger_snapshot, wait_until_topology_upgrade_finishes, enter_recovery_state, reconnect_driver, \
        delete_raft_topology_state, delete_raft_data_and_upgrade_state, wait_until_upgrade_finishes, wait_for_token_ring_and_group0_consistency
from test.topology.conftest import skip_mode
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from cassandra.protocol import InvalidRequest
from cassandra.auth import PlainTextAuthProvider


logger = logging.getLogger(__name__)

@pytest.mark.asyncio
async def test_service_levels_snapshot(manager: ManagerClient):
    """
        Cluster with 3 nodes.
        Add 10 service levels. Start new server and it should get a snapshot on bootstrap.
        Stop 3 `old` servers and query the new server to validete if it has the same service levels.
    """
    servers = await manager.servers_add(3)
    cql = manager.get_cql()
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    await manager.servers_see_each_other(servers)

    sls = ["sl" + unique_name() for _ in range(5)]
    for sl in sls:
        await cql.run_async(f"CREATE SERVICE LEVEL {sl}")

    # we don't know who the leader is, so trigger the snapshot on all nodes
    for server in servers:
        await trigger_snapshot(manager, server)

    host0 = cql.cluster.metadata.get_host(servers[0].ip_addr)
    result = await cql.run_async("SELECT service_level FROM system.service_levels_v2", host=host0)

    new_server = await manager.server_add()
    all_servers = servers + [new_server]
    await wait_for_cql_and_get_hosts(cql, all_servers, time.time() + 60)
    await manager.servers_see_each_other(all_servers)

    await asyncio.gather(*[manager.server_stop_gracefully(s.server_id)
                           for s in servers])

    await manager.driver_connect(server=new_server)
    cql = manager.get_cql()
    new_result = await cql.run_async("SELECT service_level FROM system.service_levels_v2")

    assert set([sl.service_level for sl in result]) == set([sl.service_level for sl in new_result])

@pytest.mark.asyncio
async def test_service_levels_upgrade(request, manager: ManagerClient):
    # First, force the first node to start in legacy mode
    cfg = {'force_gossip_topology_changes': True}

    servers = [await manager.server_add(config=cfg)]
    # Enable raft-based node operations for subsequent nodes - they should fall back to
    # using gossiper-based node operations
    del cfg['force_gossip_topology_changes']

    servers += [await manager.server_add(config=cfg) for _ in range(2)]
    cql = manager.get_cql()
    assert(cql)

    logging.info("Waiting until driver connects to every server")
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    logging.info("Checking the upgrade state on all nodes")
    for host in hosts:
        status = await manager.api.raft_topology_upgrade_status(host.address)
        assert status == "not_upgraded"

    sls = ["sl" + unique_name() for _ in range(5)]
    for sl in sls:
        await cql.run_async(f"CREATE SERVICE LEVEL {sl}")

    result = await cql.run_async("SELECT service_level FROM system_distributed.service_levels")
    assert set([sl.service_level for sl in result]) == set(sls)

    logging.info("Triggering upgrade to raft topology")
    await manager.api.upgrade_to_raft_topology(hosts[0].address)

    logging.info("Waiting until upgrade finishes")
    await asyncio.gather(*(wait_until_topology_upgrade_finishes(manager, h.address, time.time() + 60) for h in hosts))

    result_v2 = await cql.run_async("SELECT service_level FROM system.service_levels_v2")
    assert set([sl.service_level for sl in result_v2]) == set(sls)

    sl_v2 = "sl" + unique_name()
    await cql.run_async(f"CREATE SERVICE LEVEL {sl_v2}")

    await asyncio.gather(*(read_barrier(manager.api, get_host_api_address(host)) for host in hosts))
    result_with_sl_v2 = await cql.run_async(f"SELECT service_level FROM system.service_levels_v2")
    assert set([sl.service_level for sl in result_with_sl_v2]) == set(sls + [sl_v2])

@pytest.mark.asyncio
async def test_service_levels_work_during_recovery(manager: ManagerClient):
    # FIXME: move this test to the Raft-based recovery procedure or remove it if unneeded.
    servers = await manager.servers_add(3)

    logging.info("Waiting until driver connects to every server")
    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    logging.info("Creating a bunch of service levels")
    sls = ["sl" + unique_name() for _ in range(5)]
    for sl in sls:
        await cql.run_async(f"CREATE SERVICE LEVEL {sl}")
    
    # insert a service levels into old table as if it was created before upgrade to v2 and later removed after upgrade
    sl_v1 = "sl" + unique_name()
    await cql.run_async(f"INSERT INTO system_distributed.service_levels (service_level) VALUES ('{sl_v1}')")

    logging.info("Validating service levels were created in v2 table")
    result = await cql.run_async("SELECT service_level FROM system.service_levels_v2")
    for sl in result:
        assert sl.service_level in sls

    logging.info(f"Restarting hosts {hosts} in recovery mode")
    await asyncio.gather(*(enter_recovery_state(cql, h) for h in hosts))
    await manager.rolling_restart(servers)
    cql = await reconnect_driver(manager)

    logging.info("Cluster restarted, waiting until driver reconnects to every server")
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    logging.info("Checking service levels can be read and v2 table is used")
    recovery_result = await cql.run_async("LIST ALL SERVICE LEVELS")
    assert sl_v1 not in [sl.service_level for sl in recovery_result]
    assert set([sl.service_level for sl in recovery_result]) == set(sls)

    logging.info("Checking changes to service levels are forbidden during recovery")
    with pytest.raises(InvalidRequest, match="The cluster is in recovery mode. Changes to service levels are not allowed."):
        await cql.run_async(f"CREATE SERVICE LEVEL sl_{unique_name()}")
    with pytest.raises(InvalidRequest, match="The cluster is in recovery mode. Changes to service levels are not allowed."):
        await cql.run_async(f"ALTER SERVICE LEVEL {sls[0]} WITH timeout = 1h")
    with pytest.raises(InvalidRequest, match="The cluster is in recovery mode. Changes to service levels are not allowed."):
        await cql.run_async(f"DROP SERVICE LEVEL {sls[0]}")

    logging.info("Restoring cluster to normal status")
    await asyncio.gather(*(delete_raft_topology_state(cql, h) for h in hosts))
    await asyncio.gather(*(delete_raft_data_and_upgrade_state(cql, h) for h in hosts))

    await manager.rolling_restart(servers)
    cql = await reconnect_driver(manager)
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    await asyncio.gather(*(wait_until_upgrade_finishes(cql, h, time.time() + 60) for h in hosts))
    for host in hosts:
        status = await manager.api.raft_topology_upgrade_status(host.address)
        assert status == "not_upgraded"

    await manager.servers_see_each_other(servers)
    await manager.api.upgrade_to_raft_topology(hosts[0].address)
    await asyncio.gather(*(wait_until_topology_upgrade_finishes(manager, h.address, time.time() + 60) for h in hosts))

    logging.info("Validating service levels works in v2 mode after leaving recovery")
    new_sl = "sl" + unique_name()
    await cql.run_async(f"CREATE SERVICE LEVEL {new_sl}")

    sls_list = await cql.run_async("LIST ALL SERVICE LEVELS")
    assert sl_v1 not in [sl.service_level for sl in sls_list]
    assert set([sl.service_level for sl in sls_list]) == set(sls + [new_sl])

def default_timeout(mode):
    if mode == "dev":
        return "30s"
    elif mode == "debug":
        return "3m"
    else:
        # this branch shouldn't be reached
        assert False

def create_roles_stmts():
    return [
        "CREATE ROLE r1 WITH password='r1' AND login=true",
        "CREATE ROLE r2 WITH password='r2' AND login=true",
        "CREATE ROLE r3 WITH password='r3' AND login=true",
    ]

def create_service_levels_stmts():
    return [
        "CREATE SERVICE LEVEL sl1 WITH timeout=30m AND workload_type='interactive' AND shares=1000",
        "CREATE SERVICE LEVEL sl2 WITH timeout=1h AND workload_type='batch' AND shares=500",
        "CREATE SERVICE LEVEL sl3 WITH timeout=30s AND shares=800",
    ]

def attach_service_levels_stms():
    return [
        "ATTACH SERVICE LEVEL sl1 TO r1",
        "ATTACH SERVICE LEVEL sl2 TO r2",
        "ATTACH SERVICE LEVEL sl3 TO r3",
    ]

def grant_roles_stmts():
    return [
        "GRANT r2 TO r1",
        "GRANT r3 TO r2",
    ]

async def get_roles_connections(manager: ManagerClient, servers):
    roles = ["r1", "r2", "r3"]
    cluster_connections = []
    sessions = []

    for role in roles:
        cluster = manager.con_gen([s.ip_addr for s in servers], 
            manager.port, manager.use_ssl, PlainTextAuthProvider(username=role, password=role))
        connection = cluster.connect()
        cluster_connections.append(cluster)
        sessions.append(connection)
    
    return cluster_connections, sessions

async def assert_connections_params(manager: ManagerClient, hosts, expect):
    for host in hosts:
        params = await manager.api.client.get_json("/cql_server_test/connections_params", host=host.address)
        for param in params:
            role = param["role_name"]
            if not role in expect:
                continue
            assert param["workload_type"] == expect[role]["workload_type"]
            assert param["timeout"] == expect[role]["timeout"]
            assert param["scheduling_group"]

@pytest.mark.asyncio
@skip_mode('release', 'cql server testing REST API is not supported in release mode')
async def test_connections_parameters_auto_update(manager: ManagerClient, build_mode):
    servers = await manager.servers_add(3)
    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    logging.info("Creating user roles and their connections")
    await asyncio.gather(*(
        cql.run_async(stmt) for stmt in create_roles_stmts()))
    cluster_connections, sessions = await get_roles_connections(manager, servers)

    logging.info("Asserting all connections have default parameters")
    await assert_connections_params(manager, hosts, {
        "r1": {
            "workload_type": "unspecified",
            "timeout": default_timeout(build_mode),
            "scheduling_group": "sl:default",
        },
        "r2": {
            "workload_type": "unspecified",
            "timeout": default_timeout(build_mode),
            "scheduling_group": "sl:default",
        },
        "r3": {
            "workload_type": "unspecified",
            "timeout": default_timeout(build_mode),
            "scheduling_group": "sl:default",
        },
    })

    logging.info("Creating service levels and attaching them to corresponding roles")
    await asyncio.gather(*(
        cql.run_async(stmt) for stmt in create_service_levels_stmts()))
    await asyncio.gather(*(
        cql.run_async(stmt) for stmt in attach_service_levels_stms()))
    await asyncio.gather(*(read_barrier(manager.api, get_host_api_address(host)) for host in hosts))
    
    logging.info("Asserting all connections have parameters from their service levels")
    await assert_connections_params(manager, hosts, {
        "r1": {
            "workload_type": "interactive",
            "timeout": "30m",
            "scheduling_group": "sl:sl1",
        },
        "r2": {
            "workload_type": "batch",
            "timeout": "1h",
            "scheduling_group": "sl:sl2",
        },
        "r3": {
            "workload_type": "unspecified",
            "timeout": "30s",
            "scheduling_group": "sl:sl3",
        },
    })

    logging.info("Granting roles and creating roles hierarchy")
    await asyncio.gather(*(
        cql.run_async(stmt) for stmt in grant_roles_stmts()))
    await asyncio.gather(*(read_barrier(manager.api, get_host_api_address(host)) for host in hosts))

    logging.info("Asserting all connections have correct parameters based on roles hierarchy")
    await assert_connections_params(manager, hosts, {
        "r1": {
            "workload_type": "batch",
            "timeout": "30s",
            "scheduling_group": "sl:sl2",
        },
        "r2": {
            "workload_type": "batch",
            "timeout": "30s",
            "scheduling_group": "sl:sl2",
        },
        "r3": {
            "workload_type": "unspecified",
            "timeout": "30s",
            "scheduling_group": "sl:sl3",
        },
    })

    for cluster_conn in cluster_connections:
        cluster_conn.shutdown()

@pytest.mark.asyncio
async def test_service_level_cache_after_restart(manager: ManagerClient):
    servers = await manager.servers_add(1)
    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    await cql.run_async(f"CREATE SERVICE LEVEL sl1 WITH timeout=500ms AND workload_type='batch'")

    sls_list_before = await cql.run_async("LIST ALL SERVICE LEVELS")
    assert len(sls_list_before) == 1

    await manager.rolling_restart(servers)
    cql = await reconnect_driver(manager)
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    # after restart the service level cache is repopulated.
    # we want to verify it's populated, and that operations that use
    # the cache behave as expected.

    sls_list_after = await cql.run_async("LIST ALL SERVICE LEVELS")
    assert sls_list_after == sls_list_before

    await cql.run_async(f"ALTER SERVICE LEVEL sl1 WITH timeout = 400ms")

    result = await cql.run_async("SELECT workload_type FROM system.service_levels_v2")
    assert len(result) == 1 and result[0].workload_type == 'batch'

@pytest.mark.asyncio
@skip_mode('release', 'error injection is disabled in release mode')
async def test_shares_check(manager: ManagerClient):
    srv = await manager.server_add(config={
        "error_injections_at_startup": [
            { "name": "suppress_features", "value": "WORKLOAD_PRIORITIZATION"}
        ]
    })
    await manager.server_start(srv.server_id)

    sl1 = f"sl_{unique_name()}"
    sl2 = f"sl_{unique_name()}"
    cql = manager.get_cql()

    await cql.run_async(f"CREATE SERVICE LEVEL {sl1}")
    with pytest.raises(InvalidRequest, match="`shares` option can only be used when the cluster is fully upgraded to enterprise"):
        await cql.run_async(f"CREATE SERVICE LEVEL {sl2} WITH shares=500")
    with pytest.raises(InvalidRequest, match="`shares` option can only be used when the cluster is fully upgraded to enterprise"):
        await cql.run_async(f"ALTER SERVICE LEVEL {sl1} WITH shares=100")

    await manager.server_stop_gracefully(srv.server_id)
    await manager.server_update_config(srv.server_id, "error_injections_at_startup", [])
    await manager.server_start(srv.server_id)
    await wait_for_cql_and_get_hosts(manager.get_cql(), [srv], time.time() + 60)

    cql = manager.get_cql()
    await cql.run_async(f"CREATE SERVICE LEVEL {sl2} WITH shares=500")
    await cql.run_async(f"ALTER SERVICE LEVEL {sl1} WITH shares=100")

@pytest.mark.asyncio
@skip_mode('release', 'error injection is not supported in release mode')
async def test_workload_prioritization_upgrade(manager: ManagerClient):
    # This test simulates OSS->enterprise upgrade in v1 service levels.
    # Using error injection, the test disables WORKLOAD_PRIORITIZATION feature
    # and removes `shares` column from system_distributed.service_levels table.
    config = {
        'authenticator': 'AllowAllAuthenticator',
        'authorizer': 'AllowAllAuthorizer',
        'force_gossip_topology_changes': True,
        'error_injections_at_startup': [
            {
                'name': 'suppress_features',
                'value': 'WORKLOAD_PRIORITIZATION'
            },
            {
                'name': 'service_levels_v1_table_without_shares'
            }
        ]
    }
    servers = [await manager.server_add(config=config) for _ in range(3)]
    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    # Validate that service levels' table has no `shares` column
    sl_schema = await cql.run_async("DESC TABLE system_distributed.service_levels")
    assert "shares int" not in sl_schema[0].create_statement
    with pytest.raises(InvalidRequest):
        await cql.run_async("CREATE SERVICE LEVEL sl1 WITH shares = 100")
    
    # Do rolling restart of the cluster and remove error injections
    for server in servers:
        await manager.server_update_config(server.server_id, 'error_injections_at_startup', [])
    await manager.rolling_restart(servers)

    # Validate that `shares` column was added
    logs = [await manager.server_open_log(server.server_id) for server in servers]
    await logs[0].wait_for("Workload prioritization v1 started|Workload prioritization v1 is already started", timeout=10)
    sl_schema_upgraded = await cql.run_async("DESC TABLE system_distributed.service_levels")
    assert "shares int" in sl_schema_upgraded[0].create_statement
    await cql.run_async("CREATE SERVICE LEVEL sl2 WITH shares = 100")

@pytest.mark.asyncio
@skip_mode('release', 'error injection is disabled in release mode')
async def test_service_levels_over_limit(manager: ManagerClient):
    srv = await manager.server_add(config={
        "error_injections_at_startup": ['allow_service_level_over_limit']
    })
    await manager.server_start(srv.server_id)
    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, [srv], time.time() + 60)

    SL_LIMIT = 7
    sls = []
    for i in range(SL_LIMIT + 1):
        sl = f"sl_{i}_{unique_name()}"
        sls.append(sl)
        await cql.run_async(f"CREATE SERVICE LEVEL {sl}")
    
    log = await manager.server_open_log(srv.server_id)
    mark = await log.mark()
    await cql.run_async(f"ATTACH SERVICE LEVEL {sls[-1]} TO CASSANDRA")
    await log.wait_for(f"Service level {sls[-1]} is effectively dropped and its values are ignored.", timeout=10, from_mark=mark)

    mark = await log.mark()
    # When service levels exceed the limit, last service levels in alphabetical order are effectively dropped
    sl_name = f"aaa_sl_{unique_name()}"
    await cql.run_async(f"CREATE SERVICE LEVEL {sl_name}")
    await log.wait_for(f"service level \"{sls[-2]}\" will be effectively dropped to make scheduling group available to \"{sl_name}\", please consider removing a service level.", timeout=10, from_mark=mark)

# Reproduces issue scylla-enterprise#4912
@pytest.mark.asyncio
async def test_service_level_metric_name_change(manager: ManagerClient) -> None:
    servers = await manager.servers_add(2)
    s = servers[0]
    cql = manager.get_cql()
    [h] = await wait_for_cql_and_get_hosts(cql, [s], time.time() + 60)
    await wait_for_token_ring_and_group0_consistency(manager, time.time() + 30)

    sl1 = unique_name()
    sl2 = unique_name()

    # All service level commands need to run on the first node. It is the logic
    # that is exercised during service level data reload from group0 which is
    # prone to name reuse and, hence, could trigger the error fixed by 4912.

    # creates scheduling group `sl:sl1`
    await cql.run_async(f"CREATE SERVICE LEVEL {sl1}", host=h)
    # renames scheduling group `sl:sl1` to `sl_deleted:sl1`
    await cql.run_async(f"DROP SERVICE LEVEL {sl1}", host=h)
    # renames scheduling group `sl_deleted:sl1` to `sl:sl2`
    await cql.run_async(f"CREATE SERVICE LEVEL {sl2}", host=h)
    # creates scheduling group `sl:sl1`
    await cql.run_async(f"CREATE SERVICE LEVEL {sl1}", host=h)
    # In issue #4912, service_level_controller thought there was no room
    # for `sl:sl1` scheduling group because create_scheduling_group() failed due to
    # `seastar::metrics::double_registration (registering metrics twice for metrics: transport_cql_requests_count)`
    # but the scheduling group was actually created.
    # When sl2 is dropped, service_level_controller tries to rename its
    # scheduling group to `sl:sl1`, triggering 
    # `seastar::metrics::double_registration (registering metrics twice for metrics: scheduler_runtime_ms)`
    await cql.run_async(f"DROP SERVICE LEVEL {sl2}", host=h)

    # Check if group0 is healthy
    s2 = await manager.server_add()
    await wait_for_token_ring_and_group0_consistency(manager, time.time() + 30)
