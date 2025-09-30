#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
from test.pylib.manager_client import ManagerClient
from test.cluster.conftest import skip_mode
from test.cluster.util import new_test_keyspace
from cassandra import WriteFailure
import pytest
import logging
import asyncio

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_no_cleanup_when_unnecessary(manager: ManagerClient):
    """The test runs two bootstraps and checks that there is no cleanup in between.
       Then it runs a decommission and checks that cleanup runs automatically and then
       it runs one more decommission and checks that no cleanup runs again.
       Second part checks manual cleanup triggering. It adds a node. Triggers cleanup
       through the REST API, checks that is runs, decommissions a node and check that the
       cleanup did not run again.
    """
    logger.info("start first server")
    await manager.server_add(property_file={"dc": "dc1", "rack": "rack1"})

    logger.info("start another two servers")
    servers = await manager.running_servers()
    logs = [await manager.server_open_log(srv.server_id) for srv in servers]
    marks = [await log.mark() for log in logs]
    await manager.server_add(property_file={"dc": "dc1", "rack": "rack2"})
    await manager.server_add(property_file={"dc": "dc1", "rack": "rack3"})
    matches = [await log.grep("raft_topology - start vnodes_cleanup", from_mark=mark) for log, mark in zip(logs, marks)]
    assert sum(len(x) for x in matches) == 0

    servers = await manager.running_servers()
    host_id_2  = await manager.get_host_id(servers[2].server_id)
    logger.info(f"decommission {servers[2].server_id}")
    logs = [await manager.server_open_log(srv.server_id) for srv in servers]
    marks = [await log.mark() for log in logs]
    await manager.decommission_node(servers[2].server_id)
    matches = [await log.grep("raft_topology - start vnodes_cleanup", from_mark=mark)
               for log, mark in zip(logs, marks)]
    assert sum(len(x) for x in matches) == 2
    coordinator_log_matches = await logs[0].grep(
        f"vnodes cleanup required by 'leave' of the node {host_id_2}: running global_token_metadata_barrier",
        from_mark=marks[0])
    assert len(coordinator_log_matches) == 1

    servers = await manager.running_servers()
    logger.info(f"decommission {servers[1].server_id}")
    logs = [await manager.server_open_log(srv.server_id) for srv in servers]
    marks = [await log.mark() for log in logs]
    await manager.decommission_node(servers[1].server_id)
    matches = [await log.grep("raft_topology - start vnodes_cleanup", from_mark=mark) for log, mark in zip(logs, marks)]
    assert sum(len(x) for x in matches) == 0

    logger.info("add another server")
    await manager.server_add(property_file={"dc": "dc1", "rack": "rack4"})
    servers = await manager.running_servers()
    logs = [await manager.server_open_log(srv.server_id) for srv in servers]
    marks = [await log.mark() for log in logs]
    await manager.api.cleanup_all(servers[0].ip_addr)
    matches = [await log.grep("raft_topology - start vnodes_cleanup", from_mark=mark) for log, mark in zip(logs, marks)]
    assert sum(len(x) for x in matches) == 1

    logger.info(f"decommission {servers[1].server_id}")
    marks = [await log.mark() for log in logs]
    await manager.decommission_node(servers[1].server_id)
    matches = [await log.grep("raft_topology - start vnodes_cleanup", from_mark=mark) for log, mark in zip(logs, marks)]
    assert sum(len(x) for x in matches) == 0


@pytest.mark.asyncio
@skip_mode('debug', 'dev is enough')
@skip_mode('release', 'error injections are not supported in release mode')
async def test_cleanup_waits_for_stale_writes(manager: ManagerClient):
    """Scenario:
       * Start two nodes, a vnodes-based table with an rf=2
       * Run insert while bootstrapping another node, suspend this insert in database_apply_wait injection
       * Bootstrap succeeds, capture the final version
       * Start decommission -> triggers global barrier, which we fail on another injection
       * This failure is not fatal, the cleanup procedure continues and blocks on waiting for the stale write
       * We release the database_apply_wait injection, cleanup succeeds, write fails with 'stale topology exception'
    """

    config = {'tablets_mode_for_new_keyspaces': 'disabled'}

    logger.info("start first server")
    servers = [await manager.server_add(property_file={"dc": "dc1", "rack": "rack1"}, config=config)]
    servers += [await manager.server_add(property_file={"dc": "dc1", "rack": "rack2"}, config=config)]

    (cql, hosts) = await manager.get_ready_cql(servers)
    log0 = await manager.server_open_log(servers[0].server_id)
    log1 = await manager.server_open_log(servers[1].server_id)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}") as ks:
        logger.info("Create table my_test_table")
        await cql.run_async(f"CREATE TABLE {ks}.my_test_table (pk int PRIMARY KEY, c int);")

        # Have a bootstrapping node hang in write_both_read_new
        logger.info("Add third node")
        servers += [await manager.server_add(
            property_file={"dc": "dc1", "rack": "rack3"},
            config=config,
            start=False
        )]
        logger.info("Enable 'topology_coordinator/write_both_read_new/after_barrier' injection")
        await manager.api.enable_injection(servers[0].ip_addr,
                                           'topology_coordinator/write_both_read_new/after_barrier',
                                           True)
        logger.info("Start bootstrapping the third node")
        bootstrap_task = asyncio.create_task(manager.server_start(servers[2].server_id))
        logger.info("Waiting for topology_coordinator/write_both_read_new/after_barrier")
        await log0.wait_for("topology_coordinator/write_both_read_new/after_barrier: waiting for message")

        # Have a write request with write_both_read_new version stuck on both nodes:
        # - On the first node, this exercises the coordinator fencing code path.
        # - On the second node, this exercises the replica code path.
        logger.info("Enable 'database_apply_wait' injection")
        for s in servers[:-1]:
            await manager.api.enable_injection(s.ip_addr, 'database_apply_wait',
                                               False, parameters={'cf_name': 'my_test_table'})
        logger.info("Start write")
        write_task = cql.run_async(f"INSERT INTO {ks}.my_test_table (pk, c) VALUES (1, 1)", host=hosts[0])
        logger.info("Waiting for database_apply_wait")
        await log0.wait_for("database_apply_wait: wait")
        await log1.wait_for("database_apply_wait: wait")

        # Finish bootstrapping the node
        logger.info("Trigger topology_coordinator/write_both_read_new/after_barrier")
        await manager.api.message_injection(servers[0].ip_addr, "topology_coordinator/write_both_read_new/after_barrier")
        await bootstrap_task
        rows = await cql.run_async(
            "select version from system.topology where key = 'topology'",
            host=hosts[0])
        version_after_node2_bootstrap = rows[0].version
        host1_id = await manager.get_host_id(servers[1].server_id)

        # Have a cleanup started by decommission and failed on global barrier wait for the stale write
        logger.info("Enable 'raft_topology_barrier_and_drain_fail_before' and 'raft_topology_barrier_fail' injections")
        await manager.api.enable_injection(servers[0].ip_addr, 'raft_topology_barrier_and_drain_fail_before', True)
        await manager.api.enable_injection(servers[0].ip_addr, 'raft_topology_barrier_fail', True)
        await manager.api.enable_injection(servers[1].ip_addr, 'raft_topology_barrier_and_drain_fail_before', True)
        logger.info("Start decommission the new node")
        decommission_task = asyncio.create_task(
            manager.decommission_node(servers[1].server_id))
        logger.info("Waiting for global_token_metadata_barrier to fail")
        await log0.wait_for(f"vnodes cleanup required by 'leave' of the node {host1_id}: global_token_metadata_barrier threw an error", timeout=15)
        await log0.wait_for(f"update_fence_version: new fence_version {version_after_node2_bootstrap} is set, prev fence_version {version_after_node2_bootstrap - 1}, pending stale writes 1", timeout=15)
        await log0.wait_for("vnodes_cleanup: wait for stale pending writes", timeout=15)
        flush_matches = await log0.grep("vnodes_cleanup: flush_all_tables")
        assert len(flush_matches) == 0
        await log1.wait_for(f"update_fence_version: new fence_version {version_after_node2_bootstrap} is set, prev fence_version {version_after_node2_bootstrap - 1}, pending stale writes 1", timeout=15)
        await log1.wait_for("vnodes_cleanup: wait for stale pending writes", timeout=15)
        flush_matches = await log0.grep("vnodes_cleanup: flush_all_tables")
        assert len(flush_matches) == 0

        # Release the write -- the cleanup process should resume and the decommission succeed
        await manager.api.message_injection(servers[0].ip_addr, "database_apply_wait")
        await log0.wait_for("vnodes_cleanup: flush_all_tables", timeout=15)
        await manager.api.message_injection(servers[1].ip_addr, "database_apply_wait")
        await log1.wait_for("vnodes_cleanup: flush_all_tables", timeout=15)

        await decommission_task

        with pytest.raises(WriteFailure, match="stale topology exception"):
            await write_task
