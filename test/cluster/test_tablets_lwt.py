#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from cassandra.pool import Host # type: ignore # pylint: disable=no-name-in-module

from test.cluster.util import new_test_keyspace, unique_name, reconnect_driver
from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for_cql_and_get_hosts
from test.cluster.conftest import skip_mode
from test.pylib.internal_types import ServerInfo
from test.pylib.tablets import get_all_tablet_replicas
from cassandra.query import SimpleStatement, ConsistencyLevel
from cassandra.protocol import InvalidRequest
from test.pylib.scylla_cluster import ReplaceConfig

import pytest
import asyncio
import logging
import time

logger = logging.getLogger(__name__)


async def inject_error_on(manager, error_name, servers):
    errs = [manager.api.enable_injection(
        s.ip_addr, error_name, False) for s in servers]
    await asyncio.gather(*errs)

async def disable_injection_on(manager, error_name, servers):
    errs = [manager.api.disable_injection(
        s.ip_addr, error_name) for s in servers]
    await asyncio.gather(*errs)


@pytest.mark.asyncio
async def test_lwt(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    cmdline = [
        '--logger-log-level', 'paxos=trace'
    ]
    config = {
        'rf_rack_valid_keyspaces': False
    }
    servers = await manager.servers_add(3, cmdline=cmdline, config=config)
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
    config = {
        'rf_rack_valid_keyspaces': False
    }
    servers = await manager.servers_add(3, cmdline=cmdline, config=config)
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
        servers += [await manager.server_add(cmdline=cmdline, config=config)]
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
        'rf_rack_valid_keyspaces': False,
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
    config = {
        'rf_rack_valid_keyspaces': False
    }
    servers = await manager.servers_add(3, cmdline=cmdline, config=config)
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
        servers += [await manager.server_add(replace_cfg=replace_cfg, cmdline=cmdline, config=config)]
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
