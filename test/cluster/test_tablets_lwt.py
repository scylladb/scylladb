#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from cassandra import WriteFailure
from cassandra.auth import PlainTextAuthProvider
from cassandra.pool import Host # type: ignore # pylint: disable=no-name-in-module
from cassandra.query import SimpleStatement, ConsistencyLevel
from cassandra.protocol import InvalidRequest
from cassandra import Unauthorized

from test.cluster.util import new_test_keyspace, unique_name, reconnect_driver
from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for_cql_and_get_hosts
from test.cluster.conftest import skip_mode
from test.pylib.internal_types import ServerInfo
from test.pylib.tablets import get_all_tablet_replicas
from test.pylib.scylla_cluster import ReplaceConfig
from test.pylib.rest_client import inject_error_one_shot

import pytest
import asyncio
import logging
import time
import re

logger = logging.getLogger(__name__)


async def inject_error_on(manager, error_name, servers):
    errs = [manager.api.enable_injection(
        s.ip_addr, error_name, False) for s in servers]
    await asyncio.gather(*errs)

async def disable_injection_on(manager, error_name, servers):
    errs = [manager.api.disable_injection(
        s.ip_addr, error_name) for s in servers]
    await asyncio.gather(*errs)

async def inject_error_one_shot_on(manager, error_name, servers):
    errs = [inject_error_one_shot(manager.api, s.ip_addr, error_name) for s in servers]
    await asyncio.gather(*errs)


@pytest.mark.asyncio
async def test_lwt(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    cmdline = [
        '--logger-log-level', 'paxos=trace'
    ]
    servers = await manager.servers_add(3, cmdline=cmdline, auto_rack_dc='my_dc')
    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    quorum = len(hosts) // 2 + 1

    await asyncio.gather(*(manager.api.disable_tablet_balancing(s.ip_addr) for s in servers))

    # We use capital letters to check that the proper quotes are used in paxos store queries.
    ks = unique_name() + '_Test'
    await cql.run_async(f"CREATE KEYSPACE IF NOT EXISTS \"{ks}\" WITH replication={{'class': 'NetworkTopologyStrategy', 'replication_factor': 3}} AND tablets={{'initial': 1}}")

    async def check_paxos_state_table(exists: bool, replicas: int):
        async def query_host(h: Host):
            q = f"""
                SELECT table_name FROM system_schema.tables
                WHERE keyspace_name='{ks}' AND table_name='Test$paxos'
            """
            rows = await cql.run_async(q, host=h)
            return len(rows) > 0
        results = await asyncio.gather(*(query_host(h) for h in hosts))
        return results.count(exists) >= replicas

    # We run the checks several times to check that the base table
    # recreation is handled correctly.
    for i in range(3):
        await cql.run_async(f"CREATE TABLE \"{ks}\".\"Test\" (pk int PRIMARY KEY, c int);")

        await cql.run_async(f"INSERT INTO \"{ks}\".\"Test\" (pk, c) VALUES ({i}, {i}) IF NOT EXISTS")
        # For a Paxos round to succeed, a quorum of replicas must respond.
        # Therefore, the Paxos state table must exist on at least a quorum of replicas,
        # though it doesn't need to exist on all of them.
        # The driver calls wait_for_schema_agreement only for DDL queries,
        # so we need to handle quorums explicitly here.
        assert await check_paxos_state_table(True, quorum)

        await cql.run_async(f"UPDATE \"{ks}\".\"Test\" SET c = {i * 10} WHERE pk = {i} IF c = {i}")

        rows = await cql.run_async(f"SELECT pk, c FROM \"{ks}\".\"Test\";")
        assert len(rows) == 1
        row = rows[0]
        assert row.pk == i
        assert row.c == i * 10

        # TRUNCATE goes through the topology_coordinator and triggers a full topology state
        # reload on replicas. We use this to verify that system.tablets remains in a consistent state—
        # e.g., that no orphan tablet maps remain after dropping the Paxos state tables.
        await cql.run_async(f"TRUNCATE TABLE \"{ks}\".\"Test\"")

        await cql.run_async(f"DROP TABLE \"{ks}\".\"Test\"")
        # The driver implicitly calls wait_for_schema_agreement,
        # so the table must be dropped from all replicas, not just a quorum.
        assert await check_paxos_state_table(False, len(hosts))

    await manager.get_cql().run_async(f"DROP KEYSPACE \"{ks}\"")


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_lwt_state_is_preserved_on_tablet_migration(manager: ManagerClient):
    # Scenario:
    # 1. Cells c1 and c2 of some partition are not set.
    # 2. An LWT on {n1, n2} writes 1 to c1, stores accepts on {n1, n2} and learn on n1.
    # 3. Tablet migrates from n2 to n4.
    # 4. Another LWT comes, accepts/learns on {n3, n4} a value "2 if c1 != 1" for the cell c2.
    #    If paxos state was not preserved, this LWT would succeed since the quorum {n3, n4} is completely
    #    unaware of the decided value 1 for c1. We would miss this value and
    #    commit a new write which contradicts it. Depending in the status of the first LWT, this could
    #    be valid or. If the first LWT has failed (e.g. cl_learn == quorum == 2, learn succeeded on n1
    #    and failed on n2) this is OK, since the user hasn't observed the effects of the first LWT.
    #    In this test we use cl_learn == 1 for simplicity (see the comment below), so this ignoring
    #    the effects of the first LWT is not valid -- this breaks linearizability.
    # 5. Do the SERIAL (paxos) read of c1 and c2 from {n1, n4}. If paxos state was not preserved, this
    #    read would see c1==1 (from learned value on n1) and c2 == 2 (from n4),
    #    which contradicts the CQL from 4.

    logger.info("Bootstrap a cluster with three nodes")
    cmdline = [
        '--logger-log-level', 'paxos=trace'
    ]
    servers = await manager.servers_add(3, cmdline=cmdline, auto_rack_dc='my_dc')
    cql = manager.get_cql()

    async def set_injection(set_to: list[ServerInfo], injection: str):
        logger.info(f"Injecting {injection} on {set_to}")
        await inject_error_on(manager, injection, set_to)
        unset_from = [s for s in servers if s not in set_to]
        logger.info(f"Disabling {injection} on {unset_from}")
        await disable_injection_on(manager, injection, unset_from)

    logger.info("Resolve hosts")
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    logger.info("Disable tablet balancing")
    await asyncio.gather(*(manager.api.disable_tablet_balancing(s.ip_addr) for s in servers))

    logger.info("Create a keyspace")
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND tablets = {'initial': 1}") as ks:
        logger.info("Create a table")
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c1 int, c2 int);")

        # Step 2: Inject paxos_error_before_save_proposal - on [n3], paxos_error_before_learn on [n2, n3],
        # so that the paxos proposal for the LWT would be accepted
        # only on n1 and n2 and learnt only on n1. The new value for the CAS
        # operation would be decided.
        # Note: we use cl_learn=1 here to simplify the test, the same problem
        # would be with the default cl_learn=quorum, if learn succeeded on n1 but
        # failed on n2.
        await set_injection([servers[2]], 'paxos_error_before_save_proposal')
        await set_injection([servers[1], servers[2]], 'paxos_error_before_learn')
        logger.info("Execute CAS(c1 := 1)")
        lwt1 = SimpleStatement(f"INSERT INTO {ks}.test (pk, c1) VALUES (1, 1) IF NOT EXISTS",
                               consistency_level=ConsistencyLevel.ONE)
        await cql.run_async(lwt1, host=hosts[0])

        # Step3: start n4, migrate the single table tablet from n2 to n4.
        servers += [await manager.server_add(cmdline=cmdline, property_file={'dc': 'my_dc', 'rack': 'rack2'})]
        n4_host_id = await manager.get_host_id(servers[3].server_id)
        logger.info("Migrating the tablet from n2 to n4")
        n2_host_id = await manager.get_host_id(servers[1].server_id)
        tablets = await get_all_tablet_replicas(manager, servers[0], ks, 'test')
        assert len(tablets) == 1
        tablet = tablets[0]
        n2_replica = next(r for r in tablet.replicas if r[0] == n2_host_id)
        await manager.api.move_tablet(servers[0].ip_addr, ks, "test", *n2_replica,
                                      *(n4_host_id, 0), tablet.last_token)

        # Step 4: execute LWT on n3, n4
        await set_injection([servers[0]], 'paxos_error_before_save_promise')
        await set_injection([servers[0]], 'paxos_error_before_save_proposal')
        await set_injection([servers[0]], 'paxos_error_before_learn')
        logger.info("Execute CAS(c2 := 2 IF c1 != 1)")
        await cql.run_async(f"UPDATE {ks}.test SET c2 = 2 WHERE pk = 1 IF c1 != 1", host=hosts[2])

        # Step 5. Do the SERIAL (paxos) read of c1, c2 from {n1, n4}.
        await set_injection([servers[2]], 'paxos_error_before_save_promise')
        await set_injection([servers[2]], 'paxos_error_before_save_proposal')
        await set_injection([servers[2]], 'paxos_error_before_learn')
        logger.info("Execute paxos read")
        lwt_read = SimpleStatement(f"SELECT * FROM {ks}.test WHERE pk = 1;",
                                   consistency_level=ConsistencyLevel.SERIAL)
        rows = await cql.run_async(lwt_read, host=hosts[1])
        assert len(rows) == 1
        row = rows[0]
        assert row.pk == 1
        assert row.c1 == 1
        assert row.c2 is None


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_no_lwt_with_tablets_feature(manager: ManagerClient):
    config = {
        'error_injections_at_startup': [
            {
                'name': 'suppress_features',
                'value': 'LWT_WITH_TABLETS'
            }
        ]
    }
    await manager.server_add(config=config)

    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (key int PRIMARY KEY, val int)")
        await cql.run_async(f"INSERT INTO {ks}.test (key, val) VALUES(1, 0)")
        with pytest.raises(InvalidRequest, match=f"{ks}\.test.*LWT is not yet supported with tablets"):
            await cql.run_async(f"INSERT INTO {ks}.test (key, val) VALUES(1, 1) IF NOT EXISTS")
        # The query is rejected during the execution phase,
        # so preparing the LWT query is expected to succeed.
        stmt = cql.prepare(
            f"UPDATE {ks}.test SET val = 1 WHERE KEY = ? IF EXISTS")
        with pytest.raises(InvalidRequest, match=f"{ks}\.test.*LWT is not yet supported with tablets"):
            await cql.run_async(stmt, [1])
        with pytest.raises(InvalidRequest, match=f"{ks}\.test.*LWT is not yet supported with tablets"):
            await cql.run_async(f"DELETE FROM {ks}.test WHERE key = 1 IF EXISTS")
        res = await cql.run_async(f"SELECT val FROM {ks}.test WHERE key = 1")
        assert res[0].val == 0


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_lwt_state_is_preserved_on_tablet_rebuild(manager: ManagerClient):
    # Scenario:
    # 1. A cluster with 3 nodes, rf=3.
    # 2. A successful LWT(c := 1) with cl_learn = 1 comes, stores accepts on n1 and n2, learn -- only on n1.
    #    The user has observed its effects (the LWT is successfull), so we are not allowed to lose it.
    # 3. Node n1 is lost permanently.
    # 4. New node n4 is added to replace n1. The tablet is rebuilt on n4 based on n2 and n3.
    # 5. Do the SERIAL (paxos) read of c from {n3, n4}. If paxos state was not rebuilt, we would
    #    miss the decided value c == 1 of the first LWT.

    logger.info("Bootstrap a cluster with three nodes")
    cmdline = [
        '--logger-log-level', 'paxos=trace'
    ]
    servers = await manager.servers_add(3, cmdline=cmdline, auto_rack_dc='my_dc')
    cql = manager.get_cql()

    logger.info("Resolve hosts")
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    logger.info("Disable tablet balancing")
    await asyncio.gather(*(manager.api.disable_tablet_balancing(s.ip_addr) for s in servers))

    logger.info("Create a keyspace")
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND tablets = {'initial': 1}") as ks:
        logger.info("Create a table")
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")

        stopped_servers = set()

        async def set_injection(set_to: list[ServerInfo], injection: str):
            logger.info(f"Injecting {injection} on {set_to}")
            await inject_error_on(manager, injection, set_to)
            unset_from = [
                s for s in servers if s not in set_to and s not in stopped_servers]
            logger.info(f"Disabling {injection} on {unset_from}")
            await disable_injection_on(manager, injection, unset_from)

        # Step2. Inject paxos_error_before_save_proposal - on [n3], paxos_error_before_learn on [n2, n3]
        await set_injection([servers[2]], 'paxos_error_before_save_proposal')
        await set_injection([servers[1], servers[2]], 'paxos_error_before_learn')
        logger.info("Execute CAS(c := 1)")
        lwt1 = SimpleStatement(f"INSERT INTO {ks}.test (pk, c) VALUES (1, 1) IF NOT EXISTS",
                               consistency_level=ConsistencyLevel.ONE)
        await cql.run_async(lwt1, host=hosts[0])

        # Step3: Node n1 is lost permanently
        logger.info("Stop n1")
        await manager.server_stop(servers[0].server_id)
        stopped_servers.add(servers[0])
        cql = await reconnect_driver(manager)

        # Step 4: New node n4 is added to replace n1.
        logger.info("Start n4")
        replace_cfg = ReplaceConfig(
            replaced_id=servers[0].server_id,
            reuse_ip_addr=False,
            use_host_id=True,
            wait_replaced_dead=True
        )
        servers += [await manager.server_add(replace_cfg=replace_cfg,
                                             cmdline=cmdline,
                                             property_file={'dc': 'my_dc', 'rack': 'rack1'})]
        # Check we've actually run rebuild
        logs = []
        for s in servers:
            logs.append(await manager.server_open_log(s.server_id))
        assert sum([len(await log.grep('Initiating repair phase of tablet rebuild')) for log in logs]) == 1

        # Step 5. Do the SERIAL (paxos) read of c from {n3, n4}.
        await set_injection([servers[1]], 'paxos_error_before_save_promise')
        await set_injection([servers[1]], 'paxos_error_before_save_proposal')
        await set_injection([], 'paxos_error_before_learn')
        logger.info("Execute paxos read")
        lwt_read = SimpleStatement(f"SELECT * FROM {ks}.test WHERE pk = 1;",
                                   consistency_level=ConsistencyLevel.SERIAL)
        rows = await cql.run_async(lwt_read)
        assert len(rows) == 1
        row = rows[0]
        assert row.pk == 1
        assert row.c == 1


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_lwt_concurrent_base_table_recreation(manager: ManagerClient):
    # The test checks that the node doesn't crash when the base table is recreated
    # during LWT execution. A no_such_column_family exception is thrown, and the LWT
    # fails as a result.

    cmdline = [
        '--logger-log-level', 'paxos=trace'
    ]

    server = await manager.server_add(cmdline=cmdline)
    cql = manager.get_cql()

    logger.info("Create a keyspace")
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1}") as ks:
        async def recreate_table():
            logger.info(f"Drop table if exists {ks}.test")
            await cql.run_async(f"DROP TABLE IF EXISTS {ks}.test;")
            logger.info(f"Create table {ks}.test")
            await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")

        await recreate_table()

        logger.info("Inject load_paxos_state-enter")
        await inject_error_one_shot_on(manager, "load_paxos_state-enter", [server])

        logger.info("Start LWT")
        lwt_task = cql.run_async(
            f"INSERT INTO {ks}.test (pk, c) VALUES (1, 1) IF NOT EXISTS")

        logger.info("Open log")
        log = await manager.server_open_log(server.server_id)

        logger.info("Wait for load_paxos_state-enter injection")
        await log.wait_for('load_paxos_state-enter: waiting for message')

        await recreate_table()

        logger.info("Trigger load_paxos_state-enter")
        await manager.api.message_injection(server.ip_addr, "load_paxos_state-enter")

        with pytest.raises(WriteFailure):
            await lwt_task


@pytest.mark.asyncio
@skip_mode('debug', 'aarch64/debug is unpredictably slow', platform_key='aarch64')
@skip_mode('release', 'error injections are not supported in release mode')
async def test_lwt_timeout_while_creating_paxos_state_table(manager: ManagerClient, build_mode):
    timeout = 10000 if build_mode == 'debug' else 1000
    config = {
        'write_request_timeout_in_ms': timeout,
        'error_injections_at_startup': [
            {
                'name': 'raft-group-registry-fd-threshold-in-ms',
                'value': '500'
            }
        ]
    }

    logger.info("Bootstrap a cluster with three nodes")
    servers = await manager.servers_add(3, config=config)
    cql = manager.get_cql()

    logger.info("Create a keyspace")
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1}") as ks:
        logger.info(f"Create table {ks}.test")
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")

        logger.info("Stop the second and third nodes")
        await asyncio.gather(manager.server_stop_gracefully(servers[1].server_id),
                             manager.server_stop_gracefully(servers[2].server_id))

        logger.info(f"Running an LWT with timeout {timeout}")
        with pytest.raises(Exception, match="raft operation \\[read_barrier\\] timed out, there is no raft quorum"):
            await cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES (1, 1) IF NOT EXISTS")

        logger.info("Start the second and third nodes")
        await asyncio.gather(manager.server_start(servers[1].server_id),
                             manager.server_start(servers[2].server_id))


@pytest.mark.asyncio
async def test_lwt_for_tablets_is_not_supported_without_raft(manager: ManagerClient):
    # This test checks that LWT for tablets requires raft-based schema management.

    cmdline = [
        '--logger-log-level', 'paxos=trace'
    ]

    servers = [await manager.server_add(cmdline=cmdline)]
    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    logger.info("Create a keyspace")
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")

        # In RECOVERY mode the node doesn't join group 0 or start the group 0 Raft server,
        # it performs all operations as in `use_pre_raft_procedures` and doesn't attempt to
        # perform the upgrade.
        # We use this mode to verify the behaviour of LWT for tables when RAFT is disabled.
        logger.info("Set group0_upgrade_state := 'recovery'")
        await cql.run_async("UPDATE system.scylla_local SET value = 'recovery' WHERE key = 'group0_upgrade_state'",
                            host = hosts[0])

        logger.info(f"Rebooting {servers[0]} in RECOVERY mode")
        await manager.server_restart(servers[0].server_id)

        cql = await reconnect_driver(manager)
        logger.info("Waiting for driver")
        await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

        logger.info("Run an LWT")
        with pytest.raises(Exception, match=f"Cannot create paxos state table for {ks}.test because raft-based schema management is not enabled."):
            await cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES (1, 1) IF NOT EXISTS")


@pytest.mark.asyncio
async def test_paxos_state_table_permissions(manager: ManagerClient):
    # This test checks permission handling for paxos state tables:
    #   * Only a superuser is allowed to access a paxos state table
    #   * Even a superuser is not allowed to run ALTER or DROP on paxos state tables

    cmdline = [
        '--logger-log-level', 'paxos=trace'
    ]
    config = {
        'authenticator': 'PasswordAuthenticator',
        'authorizer': 'CassandraAuthorizer'
    }

    manager.auth_provider = PlainTextAuthProvider(
        username="cassandra", password="cassandra")

    servers = [await manager.server_add(cmdline=cmdline, config=config)]
    cql, _ = await manager.get_ready_cql(servers)

    logger.info("Create a keyspace")
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1}") as ks:
        logger.info("Create a table")
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")

        logger.info("Run an LWT1")
        await cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES (1, 1) IF NOT EXISTS")

        logger.info("Create a role_1")
        await cql.run_async("CREATE ROLE role_1 WITH SUPERUSER = true AND PASSWORD = 'psw' AND LOGIN = true")

        logger.info("Create a role_2")
        await cql.run_async("CREATE ROLE role_2 WITH SUPERUSER = false AND PASSWORD = 'psw' AND LOGIN = true")

        logger.info("Grant all permissions on test table to role_2")
        await cql.run_async(f"GRANT ALL PERMISSIONS ON TABLE {ks}.test TO role_2")

        # We don't attempt to prevent users from granting permissions on $paxos tables,
        # for simplicity. These permissions simply won't take effect — for example,
        # role_2 will still be denied access to the $paxos table.
        logger.info("Grant all permissions on test$paxos table to role_2")
        await cql.run_async(f"GRANT ALL PERMISSIONS ON TABLE \"{ks}\".\"test$paxos\" TO role_2")

        logger.info("Login as role_1")
        await manager.driver_connect(auth_provider=PlainTextAuthProvider(username="role_1", password="psw"))
        cql = manager.get_cql()

        logger.info("Run an LWT2")
        await cql.run_async(f"UPDATE {ks}.test SET c = 2 WHERE pk = 1 IF c = 1")

        logger.info("Select data after LWT2")
        rows = await cql.run_async(SimpleStatement(f"SELECT * FROM {ks}.test WHERE pk = 1;",
                                           consistency_level=ConsistencyLevel.SERIAL))
        assert len(rows) == 1
        row = rows[0]
        assert row.pk == 1
        assert row.c == 2

        logger.info("Select paxos state as role_1")
        rows = await cql.run_async(SimpleStatement(f"SELECT * FROM {ks}.\"test$paxos\""))
        assert len(rows) == 1

        logger.info("Attempt to run DROP TABLE for paxos state table as role_1")
        with pytest.raises(Unauthorized, match=re.escape(f"Cannot DROP <table {ks}.test$paxos>")):
            await cql.run_async(SimpleStatement(f"DROP TABLE {ks}.\"test$paxos\""))

        logger.info("Login as role_2")
        await manager.driver_connect(auth_provider=PlainTextAuthProvider(username="role_2", password="psw"))
        cql = manager.get_cql()

        logger.info("Run an LWT3")
        await cql.run_async(f"UPDATE {ks}.test SET c = 3 WHERE pk = 1 IF c = 2")

        logger.info("Select data after LWT3")
        rows = await cql.run_async(SimpleStatement(f"SELECT * FROM {ks}.test WHERE pk = 1;",
                                                   consistency_level=ConsistencyLevel.SERIAL))
        assert len(rows) == 1
        row = rows[0]
        assert row.pk == 1
        assert row.c == 3

        logger.info("Select paxos state as role_2")
        with pytest.raises(Unauthorized, match=re.escape(f"Only superusers are allowed to SELECT <table {ks}.test$paxos>")):
            await cql.run_async(SimpleStatement(f"SELECT * FROM {ks}.\"test$paxos\""))

        logger.info("Attempt to run DROP TABLE for paxos state table as role_2")
        with pytest.raises(Unauthorized, match=re.escape(f"Cannot DROP <table {ks}.test$paxos>")):
            await cql.run_async(SimpleStatement(f"DROP TABLE {ks}.\"test$paxos\""))

        # Relogin as a default user to be able to DROP the test keyspace
        await manager.driver_connect()
        cql = manager.get_cql()


@pytest.mark.asyncio
async def test_lwt_coordinator_shard(manager: ManagerClient):
    # The test checks that an LWT coordinator runs on a replica shard, and not on a 'default' (zero) shard.
    # Scenario:
    # 1. Start a cluster with one node with --smp 2
    # 2. Create a table with one tablet and one replica, move tablet to the second shard
    # 3. Start another node
    # 4. Run an LWT on the second node, check the logs and assert that an LWT was executed on shard 1
    # Note: Before the changes in storage_proxy/get_cas_shard, the last LWT would be executed
    # on shard 0 of the second node because this node doesn't host any replicas of the
    # tablet, and sharder.shard_for_reads would return 0 as the 'default' shard.

    logger.info("Starting the first node")
    cmdline = [
        '--logger-log-level', 'paxos=trace',
        '--smp', '2'
    ]
    servers = [await manager.server_add(cmdline=cmdline)]
    cql = manager.get_cql()

    logger.info("Disable tablet balancing")
    await asyncio.gather(*(manager.api.disable_tablet_balancing(s.ip_addr) for s in servers))

    logger.info("Create a keyspace")
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1}") as ks:
        logger.info("Create a table")
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")

        logger.info("Migrating the tablet to shard 1")
        n1_host_id = await manager.get_host_id(servers[0].server_id)
        tablets = await get_all_tablet_replicas(manager, servers[0], ks, 'test')
        assert len(tablets) == 1
        tablet = tablets[0]
        assert len(tablet.replicas) == 1
        old_replica = tablet.replicas[0]
        await manager.api.move_tablet(servers[0].ip_addr, ks, "test", *old_replica,
                                      *(n1_host_id, 1), tablet.last_token)

        logger.info("Starting a second node")
        servers += [await manager.server_add(cmdline=cmdline)]

        logger.info("Wait for cql and get hosts")
        hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

        logger.info(f"Execute CAS on {hosts[1]}")
        await cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES (1, 1) IF NOT EXISTS", host=hosts[1])

        n2_log = await manager.server_open_log(servers[1].server_id)
        matches = await n2_log.grep("CAS\\[0\\] successful")
        assert len(matches) == 1
        assert "shard 1" in matches[0][0]
