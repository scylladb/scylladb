#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import pytest
import time
import asyncio
import logging
from test.pylib.rest_client import get_host_api_address, read_barrier
from test.pylib.util import unique_name, wait_for_cql_and_get_hosts, wait_for
from test.pylib.manager_client import ManagerClient
from test.pylib.driver_utils import safe_driver_shutdown
from test.cluster.util import trigger_snapshot, reconnect_driver, \
        wait_for_token_ring_and_group0_consistency, get_topology_coordinator, \
        find_server_by_host_id
from test.cqlpy.test_service_levels import MAX_USER_SERVICE_LEVELS
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from cassandra.protocol import InvalidRequest, QueryMessage
from cassandra.auth import PlainTextAuthProvider
from test.cluster.auth_cluster import extra_scylla_config_options as auth_config


logger = logging.getLogger(__name__)
DRIVER_SL_NAME = "driver"

async def test_service_levels_snapshot(manager: ManagerClient):
    """
        Cluster with 3 nodes.
        Add 10 service levels. Start new server and it should get a snapshot on bootstrap.
        Stop 3 `old` servers and query the new server to validete if it has the same service levels.
    """
    servers = await manager.servers_add(3, config=auth_config)
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

    new_server = await manager.server_add(config=auth_config)
    all_servers = servers + [new_server]
    await wait_for_cql_and_get_hosts(cql, all_servers, time.time() + 60)
    await manager.servers_see_each_other(all_servers)

    await asyncio.gather(*[manager.server_stop_gracefully(s.server_id)
                           for s in servers])

    await manager.driver_connect(server=new_server)
    cql = manager.get_cql()
    new_result = await cql.run_async("SELECT service_level FROM system.service_levels_v2")

    assert set([sl.service_level for sl in result]) == set([sl.service_level for sl in new_result])


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

@pytest.mark.skip_mode(mode='release', reason='cql server testing REST API is not supported in release mode')
async def test_connections_parameters_auto_update(manager: ManagerClient, build_mode):
    servers = await manager.servers_add(3, config=auth_config, auto_rack_dc="dc1")
    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    logging.info("Creating user roles and their connections")
    await asyncio.gather(*(
        cql.run_async(stmt) for stmt in create_roles_stmts()))
    cluster_connections, sessions = await get_roles_connections(manager, servers)

    logging.info("Asserting all connections have default parameters")
    str_to = lambda build_mode : "1m" if build_mode=="dev" else ("1m30s" if build_mode=="debug" else f"Faild bad mode {build_mode}")
    await assert_connections_params(manager, hosts, {
        "r1": {
            "workload_type": "unspecified",
            "timeout": str_to(build_mode),
            "scheduling_group": "sl:default",
        },
        "r2": {
            "workload_type": "unspecified",
            "timeout": str_to(build_mode),
            "scheduling_group": "sl:default",
        },
        "r3": {
            "workload_type": "unspecified",
            "timeout": str_to(build_mode),
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
        safe_driver_shutdown(cluster_conn)

async def test_service_level_cache_after_restart(manager: ManagerClient):
    servers = await manager.servers_add(1, config=auth_config, auto_rack_dc="dc1")
    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    await cql.run_async(f"CREATE SERVICE LEVEL sl1 WITH timeout=500ms AND workload_type='batch'")

    sls_list_before = await cql.run_async("LIST ALL SERVICE LEVELS")
    assert len(sls_list_before) == 2

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
    assert len(result) == 2 and result[0].workload_type == 'batch' and result[1].workload_type == 'batch'

@pytest.mark.skip_mode(mode='release', reason='error injection is disabled in release mode')
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

@pytest.mark.skip_mode(mode='release', reason='error injection is disabled in release mode')
async def test_service_levels_over_limit(manager: ManagerClient):
    srv = await manager.server_add(config={**auth_config,
        "error_injections_at_startup": ['allow_service_level_over_limit']
    })
    await manager.server_start(srv.server_id)
    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, [srv], time.time() + 60)

    sls = []
    for i in range(MAX_USER_SERVICE_LEVELS + 1):
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
async def test_service_level_metric_name_change(manager: ManagerClient) -> None:
    servers = await manager.servers_add(2, config=auth_config, auto_rack_dc="dc1")
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
    s2 = await manager.server_add(config=auth_config, property_file={"dc": "dc1", "rack": "rack3"})
    await wait_for_token_ring_and_group0_consistency(manager, time.time() + 30)

# Reproduces scylladb/scylladb#24792.
@pytest.mark.skip_mode(mode='release', reason='error injection is disabled in release mode')
async def test_reload_service_levels_after_auth_service_is_stopped(manager: ManagerClient):
    config = {**auth_config, "error_injections_at_startup": ["reload_service_level_cache_after_auth_service_is_stopped"]}
    s1 = await manager.server_add(config=config)
    await manager.server_stop_gracefully(s1.server_id)

# Reproduces scylladb/scylladb#26190
async def test_service_level_reuse_name(manager: ManagerClient):
    servers = await manager.servers_add(1, config=auth_config, auto_rack_dc="dc1")
    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    sl1 = "sl1"
    sl2 = "sl2"

    async def create_sl_and_use(cql, name):
        # Create a service level and attach it to the default cassandra user
        await cql.run_async(f"CREATE SERVICE LEVEL {name}")
        await cql.run_async(f"ATTACH SERVICE LEVEL {name} to cassandra")
        # Reconnect the driver to make sure the new service level is used
        cql = await reconnect_driver(manager)
        # Run any `select` query to create a `cql3_select.{name}` execution stage
        await cql.run_async(f"select * from system.clients;")
        return cql

    cql = await create_sl_and_use(cql, sl1)
    await cql.run_async(f"DROP SERVICE LEVEL {sl1}")

    cql = await create_sl_and_use(cql, sl2)
    cql = await create_sl_and_use(cql, sl1)

async def test_driver_service_level(manager: ManagerClient) -> None:
    servers = await manager.servers_add(2, config=auth_config, auto_rack_dc="dc1")

    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    await wait_for_token_ring_and_group0_consistency(manager, time.time() + 30)

    logger.info("Verify that sl:driver is created properly on system startup")
    service_levels = await cql.run_async("LIST ALL SERVICE LEVELS")
    assert len(service_levels) == 1
    assert service_levels[0].service_level == "driver"
    assert service_levels[0].workload_type == "batch"
    assert service_levels[0].shares == 200
    assert (await cql.run_async("SELECT value FROM system.scylla_local WHERE key = 'service_level_driver_created'"))[0].value == "true"

    logger.info("Verify that sl:driver can be removed")
    await cql.run_async(f"DROP SERVICE LEVEL driver")
    assert len(await cql.run_async("LIST ALL SERVICE LEVELS")) == 0

    logger.info("Add a new server so that the Raft state machine snapshot is used")
    new_servers = await manager.servers_add(1, config=auth_config, auto_rack_dc="dc1")

    logger.info("Verify that sl:driver is not re-created even after topology coordinator reload")
    coord = await get_topology_coordinator(manager)
    coord_serv = await find_server_by_host_id(manager, servers, coord)
    await manager.api.reload_raft_topology_state(coord_serv.ip_addr)

    hosts = await wait_for_cql_and_get_hosts(cql, servers + new_servers, time.time() + 60)
    for host in hosts:
        assert len(await cql.run_async("LIST ALL SERVICE LEVELS", host=host)) == 0

async def test_driver_service_creation_failure(manager: ManagerClient) -> None:
    servers = await manager.servers_add(2, config=auth_config, auto_rack_dc="dc1")

    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    await wait_for_token_ring_and_group0_consistency(manager, time.time() + 30)

    logger.info("Drop sl:driver to prepare the system state and re-create it later")
    await cql.run_async(f"DROP SERVICE LEVEL driver")

    logger.info("Create new service levels to occupy all slots, including the slot of the removed sl:driver")
    for i in range(MAX_USER_SERVICE_LEVELS + 1):
        await cql.run_async(f"CREATE SERVICE_LEVEL new_sl_{i}")

    logger.info("Verify that all newly created service levels exist")
    service_levels = await cql.run_async("LIST ALL SERVICE LEVELS")
    service_level_names = [sl.service_level for sl in service_levels]
    for i in range(MAX_USER_SERVICE_LEVELS + 1):
        assert f"new_sl_{i}" in service_level_names
    assert "driver" not in service_level_names

    logger.info("Check the logs to see that sl:driver creation failed")
    coord = await get_topology_coordinator(manager)
    coord_serv = await find_server_by_host_id(manager, servers, coord)
    log_file = await manager.server_open_log(coord_serv.server_id)
    mark = await log_file.mark()

    logger.info("Set service_level_driver_created=false in system.scylla_local and reload topology coordinator")
    for host in hosts:
        await cql.run_async(f"UPDATE system.scylla_local SET value = 'false' WHERE key = 'service_level_driver_created'", host=host)
    await manager.api.reload_raft_topology_state(coord_serv.ip_addr)
    await log_file.wait_for("Failed to create service level for driver", from_mark=mark)

    logger.info("Verify topology coordinator is not blocked despite the failure")
    mark = await log_file.mark()
    await manager.api.reload_raft_topology_state(coord_serv.ip_addr)
    await log_file.wait_for("topology coordinator fiber has nothing to do. Sleeping.", from_mark=mark)

    logger.info("Double-check that the driver is not re-created")
    for host in hosts:
        service_levels = await cql.run_async("LIST ALL SERVICE LEVELS", host=host)
        service_level_names = [sl.service_level for sl in service_levels]
        assert "driver" not in service_level_names

async def _verify_requests_count_metrics(manager, server, used_group, unused_group, func):
    number_of_requests = 1000
    # If the service level is changed, the scheduling group is changed in connection::process()
    # after a request is finished. Therefore, a small number of initial requests can be
    # processed in the previous scheduling group. To handle it and prevent test flakiness,
    # we add a relatively large margin of requests that are allowed to be processed by a group 
    # different from used_group.
    expected_number_of_requests = number_of_requests * 0.9

    def get_requests_for_group(metrics, group):
        res = metrics.get("scylla_transport_cql_requests_count", {'scheduling_group_name': group})
        logger.info(f"group={group}, _transport_cql_requests_count={res}")

        if res is None:
            return 0
        return res

    metrics = await manager.metrics.query(server.ip_addr)
    initial_requests_processed_by_used_group = get_requests_for_group(metrics, used_group)
    initial_requests_processed_by_unused_group = get_requests_for_group(metrics, unused_group)

    await asyncio.gather(*[asyncio.to_thread(func) for i in range(number_of_requests)])

    metrics = await manager.metrics.query(server.ip_addr)
    requests_processed_by_used_group = get_requests_for_group(metrics, used_group)
    requests_processed_by_unused_group = get_requests_for_group(metrics, unused_group)
    assert requests_processed_by_used_group - initial_requests_processed_by_used_group >= expected_number_of_requests
    assert requests_processed_by_unused_group - initial_requests_processed_by_unused_group < expected_number_of_requests

async def test_driver_service_level_not_used_for_user_queries(manager: ManagerClient) -> None:
    server = await manager.server_add(config=auth_config)

    cql = manager.get_cql()
    [h] = await wait_for_cql_and_get_hosts(cql, [server], time.time() + 60)
    await wait_for_token_ring_and_group0_consistency(manager, time.time() + 30)

    func = lambda: cql.execute(f"SELECT * from system.peers")
    await _verify_requests_count_metrics(manager, server, 'sl:default', 'sl:driver', func)

    await cql.run_async(f"CREATE SERVICE LEVEL test", host=h)
    await cql.run_async(f"ATTACH SERVICE LEVEL test TO cassandra", host=h)

    func = lambda: cql.execute(f"SELECT * from system.peers")
    await _verify_requests_count_metrics(manager, server, 'sl:test', 'sl:driver', func)

async def test_driver_service_level_used_for_driver_queries(manager: ManagerClient) -> None:
    server = await manager.server_add(config=auth_config)

    cql = manager.get_cql()
    [h] = await wait_for_cql_and_get_hosts(cql, [server], time.time() + 60)
    await wait_for_token_ring_and_group0_consistency(manager, time.time() + 30)

    await cql.run_async(f"CREATE SERVICE LEVEL test", host=h)
    await cql.run_async(f"ATTACH SERVICE LEVEL test TO cassandra", host=h)

    async def get_control_connection_query_function(manager):
        await manager.driver_connect() # restart control connection
        cql = manager.get_cql()
        control_connection = cql.cluster.control_connection._connection
        query = QueryMessage("select * from system.peers", 1)
        return cql, lambda: control_connection.wait_for_response(query)

    cql, func = await get_control_connection_query_function(manager)
    await _verify_requests_count_metrics(manager, server, 'sl:driver', 'sl:test', func)

    await cql.run_async(f"DROP SERVICE LEVEL driver", host=h)
    cql, func = await get_control_connection_query_function(manager)
    await _verify_requests_count_metrics(manager, server, 'sl:test', 'sl:driver', func)

    await cql.run_async(f"CREATE SERVICE LEVEL driver", host=h)
    cql, func = await get_control_connection_query_function(manager)
    await _verify_requests_count_metrics(manager, server, 'sl:driver', 'sl:test', func)

# Reproduces scylladb/scylladb#26040
async def test_anonymous_user(manager: ManagerClient) -> None:
    allow_all_config = {'authenticator':'AllowAllAuthenticator', 'authorizer':'AllowAllAuthorizer'}
    server = await manager.server_add(config=allow_all_config)
    cql = manager.get_cql()
    [h] = await wait_for_cql_and_get_hosts(cql, [server], time.time() + 60)

    async def connections_ready():
        rows = list(cql.execute("SELECT connection_stage, username, scheduling_group FROM system.clients"))
        if len(rows) == 0:
            return None
        for row in rows:
            if row.connection_stage != "READY":
                return None
        return rows

    rows = await wait_for(connections_ready, time.time() + 60)
    for r in rows:
        assert r.username == 'anonymous'
        assert r.scheduling_group in ['sl:default', 'sl:driver']
        if r.scheduling_group == 'sl:default':
            return

    assert False, f"None of clients use sl:default, rows={rows}"

@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_per_service_level_cql_requests_serving(manager: ManagerClient) -> None:
    """Test that the per-service-level cql_requests_serving metric correctly
    reflects the number of in-flight CQL requests for each service level.

    Uses error injection to pause requests mid-flight, then verifies the gauge
    shows exactly the right count per scheduling group. Sends two requests on
    a custom service level and one on sl:default (the cassandra superuser) to
    verify both per-SL counters independently.
    """
    cmdline = ['--logger-log-level', 'debug_error_injection=debug']
    server = await manager.server_add(config=auth_config, cmdline=cmdline)
    cql, _ = await manager.get_ready_cql([server])

    # Create one new service level and one new user attached to it.
    # The cassandra superuser already uses sl:default.
    sl_a = unique_name()
    await cql.run_async(f"CREATE SERVICE LEVEL {sl_a}")
    await cql.run_async(f"CREATE ROLE user_a WITH PASSWORD = 'pass_a' AND LOGIN = true")
    await cql.run_async(f"ATTACH SERVICE LEVEL {sl_a} TO user_a")

    # Open dedicated driver sessions for user_a and the cassandra superuser.
    # Disable schema and token metadata refresh on both to prevent background
    # driver queries from interfering with the error injection and metrics.
    cluster_a = manager.con_gen([server.ip_addr],
        manager.port, manager.use_ssl, PlainTextAuthProvider(username='user_a', password='pass_a'))
    cluster_a.schema_metadata_enabled = False
    cluster_a.token_metadata_enabled = False
    session_a = cluster_a.connect()

    cluster_default = manager.con_gen([server.ip_addr],
        manager.port, manager.use_ssl, PlainTextAuthProvider(username='cassandra', password='cassandra'))
    cluster_default.schema_metadata_enabled = False
    cluster_default.token_metadata_enabled = False
    session_default = cluster_default.connect()

    try:
        # Warm up both sessions to ensure the driver has established connections
        # before enabling injection. Without this, execute() may fail with
        # NoHostAvailable and the query never reaches the server.
        session_a.execute("SELECT key FROM system.local")
        session_default.execute("SELECT key FROM system.local")

        # Enable error injection to pause CQL requests.
        await manager.api.enable_injection(server.ip_addr, "transport_cql_request_pause", False)

        # Send two requests from user_a (sl:sl_a) and one from the cassandra
        # superuser (sl:default). Each pauses at the injection point, keeping
        # the cql_requests_serving gauge incremented.
        task_a1 = asyncio.ensure_future(asyncio.to_thread(session_a.execute, "SELECT * FROM system.local"))
        await manager.api.wait_for_injection_enter(server.ip_addr, "transport_cql_request_pause")

        task_a2 = asyncio.ensure_future(asyncio.to_thread(session_a.execute, "SELECT * FROM system.local"))
        await manager.api.wait_for_injection_enter(server.ip_addr, "transport_cql_request_pause", threshold=2)

        task_default = asyncio.ensure_future(asyncio.to_thread(session_default.execute, "SELECT * FROM system.local"))
        await manager.api.wait_for_injection_enter(server.ip_addr, "transport_cql_request_pause", threshold=3)

        # All three requests are now paused. Verify the per-SL gauges.
        # Use >= because a stray request from the manager's CQL session
        # (whose metadata refresh we cannot disable) could also get paused,
        # inflating the sl:default counter.
        metrics = await manager.metrics.query(server.ip_addr)
        serving_a = metrics.get("scylla_transport_cql_requests_serving",
                                {'scheduling_group_name': f'sl:{sl_a}'})
        serving_default = metrics.get("scylla_transport_cql_requests_serving",
                                      {'scheduling_group_name': 'sl:default'})
        assert serving_a >= 2, \
            f"Expected at least 2 in-flight requests for sl:{sl_a}, got {serving_a}"
        assert serving_default >= 1, \
            f"Expected at least 1 in-flight request for sl:default, got {serving_default}"

        # Release paused requests by sending messages. Each message_injection
        # call sends one message per shard, waking one handler per shard.
        # Send enough to cover our three requests plus any stray ones.
        total_paused = int(serving_a + serving_default)
        for _ in range(total_paused):
            await manager.api.message_injection(server.ip_addr, "transport_cql_request_pause")

        # Wait for all requests to complete.
        await task_a1
        await task_a2
        await task_default

        # Disable injection before checking metrics to avoid pausing any
        # stray requests that might arrive.
        await manager.api.disable_injection(server.ip_addr, "transport_cql_request_pause")

        # Verify gauges are back to zero. Use a retry loop because a stray
        # request from the manager's CQL session may be briefly in-flight.
        async def metrics_are_zero():
            metrics = await manager.metrics.query(server.ip_addr)
            serving_a = metrics.get("scylla_transport_cql_requests_serving",
                                    {'scheduling_group_name': f'sl:{sl_a}'})
            serving_default = metrics.get("scylla_transport_cql_requests_serving",
                                          {'scheduling_group_name': 'sl:default'})
            assert serving_a == 0, \
                f"Expected 0 in-flight requests for sl:{sl_a} after release, got {serving_a}"
            assert serving_default == 0, \
                f"Expected 0 in-flight requests for sl:default after release, got {serving_default}"
            return True
        await wait_for(metrics_are_zero, deadline=time.time() + 60)
    finally:
        await manager.api.disable_injection(server.ip_addr, "transport_cql_request_pause")
        safe_driver_shutdown(cluster_a)
        safe_driver_shutdown(cluster_default)


@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_per_service_level_cql_request_latency_histogram(manager: ManagerClient) -> None:
    """Test that the per-service-level cql_request_latency_histogram metric
    records transport-level request latencies for each service level.

    Pauses a query on a custom service level and verifies that the histogram
    is updated only after the response is sent.
    """
    server = await manager.server_add(config=auth_config)
    cql, _ = await manager.get_ready_cql([server])

    # Create a service level and a user attached to it.
    sl_a = unique_name()
    await cql.run_async(f"CREATE SERVICE LEVEL {sl_a}")
    await cql.run_async(f"CREATE ROLE user_a WITH PASSWORD = 'pass_a' AND LOGIN = true")
    await cql.run_async(f"ATTACH SERVICE LEVEL {sl_a} TO user_a")

    cluster_a = manager.con_gen([server.ip_addr],
        manager.port, manager.use_ssl, PlainTextAuthProvider(username='user_a', password='pass_a'))
    cluster_a.schema_metadata_enabled = False
    cluster_a.token_metadata_enabled = False
    session_a = cluster_a.connect()
    query_task = None

    def metric_value(metrics, suffix):
        value = metrics.get(f"scylla_transport_cql_request_latency_histogram_{suffix}",
                            {'scheduling_group_name': f'sl:{sl_a}'})
        return 0 if value is None else value

    try:
        # Warm up the session before enabling the injection, so the paused query
        # is not mixed with connection initialization.
        session_a.execute("SELECT key FROM system.local")

        metrics = await manager.metrics.query(server.ip_addr)
        baseline_count = metric_value(metrics, "count")
        baseline_sum = metric_value(metrics, "sum")

        await manager.api.enable_injection(server.ip_addr, "transport_cql_request_pause", False)
        log = await manager.server_open_log(server.server_id)
        mark = await log.mark()
        query_task = asyncio.ensure_future(asyncio.to_thread(session_a.execute, "SELECT key FROM system.local"))
        await log.wait_for("transport_cql_request_pause: waiting for message", from_mark=mark)

        metrics = await manager.metrics.query(server.ip_addr)
        paused_count = metric_value(metrics, "count")

        await manager.api.message_injection(server.ip_addr, "transport_cql_request_pause")
        await query_task

        assert paused_count == baseline_count, \
            f"Expected no new latency sample while request is paused, got {paused_count - baseline_count}"

        async def latency_sample_recorded():
            metrics = await manager.metrics.query(server.ip_addr)
            latency_count = metric_value(metrics, "count")
            latency_sum = metric_value(metrics, "sum")
            assert latency_count >= baseline_count + 1, \
                f"Expected at least one new latency sample for sl:{sl_a}, got {latency_count - baseline_count}"
            assert latency_sum >= baseline_sum, \
                f"Expected latency sum to increase for sl:{sl_a}, before={baseline_sum}, after={latency_sum}"
            return True

        await wait_for(latency_sample_recorded, deadline=time.time() + 60)
    finally:
        # If the test failed while the request was still paused at the injection
        # point, unblock it so it doesn't hang forever, and clean up the injection
        # to avoid affecting subsequent tests.
        if query_task is not None:
            if not query_task.done():
                try:
                    await manager.api.message_injection(server.ip_addr, "transport_cql_request_pause")
                except Exception:
                    logger.warning("Failed to unblock paused CQL request", exc_info=True)
            try:
                await query_task
            except Exception:
                logger.warning("Paused CQL request failed while cleaning up", exc_info=True)
        await manager.api.disable_injection(server.ip_addr, "transport_cql_request_pause")
        safe_driver_shutdown(cluster_a)
