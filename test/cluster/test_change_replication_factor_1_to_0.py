#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import logging
import pytest
import asyncio
import time
from contextlib import asynccontextmanager

from cassandra import ConsistencyLevel, Unavailable, WriteFailure, WriteTimeout  # type: ignore
from cassandra.policies import FallthroughRetryPolicy  # type: ignore
from cassandra.query import SimpleStatement  # type: ignore
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for_cql_and_get_hosts
from test.cluster.util import new_test_keyspace, wait_for_token_ring_and_group0_consistency, get_topology_coordinator
from test.pylib.tablets import get_tablet_info
from test.pylib.util import wait_for

logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "use_tablets",
    [
        pytest.param(False, id="vnodes"),
        pytest.param(True, id="tablets", marks=[pytest.mark.skip_bug(
            link="https://github.com/scylladb/scylladb/issues/20282", reason="Coredump and error of: failed to log message: fmt='Requested location for node {} not in topology",),pytest.mark.tier2])
    ]
)
async def test_change_replication_factor_1_to_0(request: pytest.FixtureRequest, manager: ScyllaClusterManager, use_tablets: bool) -> None:
    CONFIG = {"endpoint_snitch": "GossipingPropertyFileSnitch", "tablets_mode_for_new_keyspaces": "enabled" if use_tablets else "disabled"}
    logger.info("Creating a new cluster")
    for i in range(2):
        await manager.server_add(
            config=CONFIG,
            property_file={'dc': f'dc{i}', 'rack': f'myrack{i}'})

    cql = manager.get_cql()
    async with new_test_keyspace(manager, "with replication = {'class': 'NetworkTopologyStrategy', 'dc0': 1, 'dc1': 1}") as ks:
        await cql.run_async(f"create table {ks}.t (pk int primary key)")

        srvs = await manager.running_servers()
        await wait_for_cql_and_get_hosts(cql, srvs, time.time() + 60)

        stmt = cql.prepare(f"SELECT * FROM {ks}.t where pk = ?")
        stmt.consistency_level = ConsistencyLevel.LOCAL_QUORUM

        stop_event = asyncio.Event()

        async def do_reads() -> None:
            iteration = 0
            while not stop_event.is_set():
                start_time = time.time()
                try:
                    await cql.run_async(stmt, [0])
                except Exception as e:
                    logger.error(f"Read started {time.time() - start_time}s ago failed: {e}")
                    raise
                iteration += 1
                await asyncio.sleep(0.01)
            logger.info(f"Finishing with iter {iteration}")

        tasks = [asyncio.create_task(do_reads()) for _ in range(3)]

        await cql.run_async(f"alter keyspace {ks} with replication = {{'class': 'NetworkTopologyStrategy', 'dc0': 1, 'dc1': 0}}")

        await asyncio.sleep(1)
        stop_event.set()
        await asyncio.gather(*tasks)

# Tests #22688 - we should be able to both do further alter:s of a keyspace
# even after removing replication factor fully from a dc and decommission of said
# dc.
@pytest.mark.parametrize(
    "use_tablets",
    [
        pytest.param(False, id="vnodes"),
        pytest.param(True, id="tablets"),
    ],
)
async def test_change_replication_factor_1_to_0_and_decommission(request: pytest.FixtureRequest, manager: ScyllaClusterManager, use_tablets: bool) -> None:
    CONFIG = {"endpoint_snitch": "GossipingPropertyFileSnitch", "tablets_mode_for_new_keyspaces": "enabled" if use_tablets else "disabled"}
    logger.info("Creating a new cluster")
    for i in range(2):
        await manager.server_add(
            config=CONFIG,
            property_file={'dc': f'dc{i}', 'rack': 'myrack'})

    cql = manager.get_cql()
    async with new_test_keyspace(manager, "with replication = {'class': 'NetworkTopologyStrategy', 'dc0': 1, 'dc1': 1}") as ks:
        await cql.run_async(f"create table {ks}.t (pk int primary key)")

        srvs = await manager.running_servers()
        sorted(srvs, key=lambda si: si.datacenter)
        assert(srvs[1].datacenter == "dc1")

        await wait_for_cql_and_get_hosts(cql, srvs, time.time() + 60)

        keys = range(256)
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.t (pk) VALUES ({k});") for k in keys])

        # dc1 = 0 -> remove me from said dc
        await cql.run_async(f"alter keyspace {ks} with replication = {{'class': 'NetworkTopologyStrategy', 'dc0': 1, 'dc1': 0}}")

        logger.info(f"Decommissioning node {srvs[1]}")

        # decommission dc1
        await manager.decommission_node(srvs[1].server_id)
        await wait_for_token_ring_and_group0_consistency(manager, time.time() + 30)

        # ensure this no-op alter still works
        async with asyncio.timeout(30):
            await cql.run_async(f"alter keyspace {ks} with replication = {{'class': 'NetworkTopologyStrategy', 'dc0': 1}}")


# Adding dc1 with rack lists moves the tablets first and switches the schema last. In
# between, a tablet reads from a dc1 replica while the schema has no dc1. An EACH_QUORUM
# write must still wait for dc0, and the ack of the dc1 replica must not complete it.
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_each_quorum_write_ignores_dc_not_in_schema(manager: ScyllaClusterManager) -> None:
    CONFIG = {"endpoint_snitch": "GossipingPropertyFileSnitch", "tablets_mode_for_new_keyspaces": "enabled"}
    servers = [await manager.server_add(config=CONFIG, property_file={'dc': f'dc{i}', 'rack': f'myrack{i}'}) for i in range(2)]
    dc0, dc1 = servers
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "with replication = {'class': 'NetworkTopologyStrategy', 'dc0': ['myrack0']} and tablets = {'initial': 1}") as ks:
        await cql.run_async(f"create table {ks}.t (pk int primary key)")
        hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
        dc1_host = next(h for h in hosts if h.datacenter == "dc1")

        coord = await get_topology_coordinator(manager)
        coord_serv = await manager.find_server_by_host_id(servers, coord)
        await manager.api.enable_injection(coord_serv.ip_addr, "cleanup_tablet_wait", one_shot=False)
        alter = cql.run_async(
            f"alter keyspace {ks} with replication = {{'class': 'NetworkTopologyStrategy', 'dc0': ['myrack0'], 'dc1': ['myrack1']}}")
        try:
            # In the cleanup stage the tablet reads from and writes to its dc1 replica, and
            # the schema switch waits for the migration to end.
            async def tablet_in_cleanup():
                tablet = await get_tablet_info(manager, dc0, ks, "t", 0)
                return True if tablet.stage == "cleanup" else None
            await wait_for(tablet_in_cleanup, time.time() + 60)
            ks_row = (await cql.run_async(f"select next_replication from system_schema.keyspaces where keyspace_name = '{ks}'"))[0]
            assert ks_row.next_replication is not None

            # The dc0 ack completes the write, so a timeout below is caused by blocking dc0.
            stmt = SimpleStatement(f"insert into {ks}.t (pk) values (1)", consistency_level=ConsistencyLevel.EACH_QUORUM)
            await cql.run_async(stmt, host=dc1_host)

            await manager.api.enable_injection(dc0.ip_addr, "database_apply", one_shot=False,
                                               parameters={"ks_name": ks, "cf_name": "t", "what": "wait"})
            try:
                # Coordinated in dc1: a coordinator holds its write handler until its own local
                # write finishes, so a blocked local replica would delay the timeout response.
                stmt = SimpleStatement(f"insert into {ks}.t (pk) values (0) using timeout 5s", consistency_level=ConsistencyLevel.EACH_QUORUM)
                with pytest.raises(WriteTimeout):
                    await asyncio.wait_for(cql.run_async(stmt, host=dc1_host), 60)
            finally:
                await manager.api.disable_injection(dc0.ip_addr, "database_apply")
        finally:
            await manager.api.disable_injection(coord_serv.ip_addr, "cleanup_tablet_wait")
            # Wakes the topology coordinator, which otherwise sleeps before it retries cleanup.
            await manager.enable_tablet_balancing()
        await alter


# Runs the ALTER with every tablet of ks.t held in write_both_read_new. With rack lists the
# schema keeps the old replication until the migration ends, as asserted on next_replication.
@asynccontextmanager
async def tablets_in_write_both_read_new(manager: ScyllaClusterManager, servers, ks: str, replication: str):
    cql = manager.get_cql()
    coord = await get_topology_coordinator(manager)
    coord_serv = await manager.find_server_by_host_id(servers, coord)
    await manager.api.enable_injection(coord_serv.ip_addr, "write_both_read_new_tablet_wait", one_shot=False)
    alter = cql.run_async(f"alter keyspace {ks} with replication = {{'class': 'NetworkTopologyStrategy', {replication}}}")
    try:
        # Asked of every server: the read barrier makes each coordinator use the stage.
        async def tablet_in_stage():
            for server in servers:
                tablet = await get_tablet_info(manager, server, ks, "t", 0)
                if tablet.stage != "write_both_read_new":
                    return None
            return True
        await wait_for(tablet_in_stage, time.time() + 60)
        ks_row = (await cql.run_async(f"select next_replication from system_schema.keyspaces where keyspace_name = '{ks}'"))[0]
        assert ks_row.next_replication is not None
        yield
    finally:
        await manager.api.disable_injection(coord_serv.ip_addr, "write_both_read_new_tablet_wait")
        # Wakes the topology coordinator, which otherwise sleeps before it retries the stage.
        await manager.enable_tablet_balancing()
    await alter


# Adding dc1: the write set is the dc0 replica and the dc1 replica is pending, while reads
# already use both. A QUORUM write needs the quorum of the write set plus the pending
# replica, 2 of 2. Sizing the quorum from the read set asks for 3 and is always unavailable.
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_quorum_write_available_while_adding_dc(manager: ScyllaClusterManager) -> None:
    CONFIG = {"endpoint_snitch": "GossipingPropertyFileSnitch", "tablets_mode_for_new_keyspaces": "enabled"}
    servers = [await manager.server_add(config=CONFIG, property_file={'dc': f'dc{i}', 'rack': f'myrack{i}'}) for i in range(2)]
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "with replication = {'class': 'NetworkTopologyStrategy', 'dc0': ['myrack0']} and tablets = {'initial': 1}") as ks:
        await cql.run_async(f"create table {ks}.t (pk int primary key)")
        await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
        async with tablets_in_write_both_read_new(manager, servers, ks, "'dc0': ['myrack0'], 'dc1': ['myrack1']"):
            stmt = SimpleStatement(f"insert into {ks}.t (pk) values (0)", consistency_level=ConsistencyLevel.QUORUM)
            try:
                await cql.run_async(stmt)
            except Unavailable as e:
                pytest.fail(f"QUORUM write unavailable with all replicas up: {e}")


# Dropping dc1: the dc1 replica still takes writes, while reads use only dc0. A QUORUM
# write must wait for 2 of the 2 write replicas. Sizing the quorum from the read set
# asks for 1, so the dc1 ack alone completes a write that a dc0 read then misses.
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_quorum_write_waits_for_remaining_dc_while_dropping_dc(manager: ScyllaClusterManager) -> None:
    CONFIG = {"endpoint_snitch": "GossipingPropertyFileSnitch", "tablets_mode_for_new_keyspaces": "enabled"}
    servers = [await manager.server_add(config=CONFIG, property_file={'dc': f'dc{i}', 'rack': f'myrack{i}'}) for i in range(2)]
    dc0 = servers[0]
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "with replication = {'class': 'NetworkTopologyStrategy', 'dc0': ['myrack0'], 'dc1': ['myrack1']} and tablets = {'initial': 1}") as ks:
        await cql.run_async(f"create table {ks}.t (pk int primary key)")
        hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
        dc1_host = next(h for h in hosts if h.datacenter == "dc1")
        async with tablets_in_write_both_read_new(manager, servers, ks, "'dc0': ['myrack0'], 'dc1': []"):
            # Both replicas ack, so a timeout below is caused by blocking dc0.
            stmt = SimpleStatement(f"insert into {ks}.t (pk) values (1)", consistency_level=ConsistencyLevel.QUORUM)
            await cql.run_async(stmt, host=dc1_host)

            await manager.api.enable_injection(dc0.ip_addr, "database_apply", one_shot=False,
                                               parameters={"ks_name": ks, "cf_name": "t", "what": "wait"})
            try:
                # Coordinated in dc1, so the blocked dc0 replica does not delay the timeout response.
                stmt = SimpleStatement(f"insert into {ks}.t (pk) values (0) using timeout 5s", consistency_level=ConsistencyLevel.QUORUM)
                with pytest.raises(WriteTimeout):
                    await asyncio.wait_for(cql.run_async(stmt, host=dc1_host), 60)
            finally:
                await manager.api.disable_injection(dc0.ip_addr, "database_apply")


# Dropping dc1 with dc0 on two racks: reads use the two dc0 replicas, writes also go to
# dc1. A CL ALL read which finds the dc0 replicas diverged repairs them, and the repair
# must wait for 2 of its 2 targets. Sizing it from the write set asks for 3 and fails the read.
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_read_repair_completes_while_dropping_dc(manager: ScyllaClusterManager) -> None:
    CONFIG = {"endpoint_snitch": "GossipingPropertyFileSnitch", "tablets_mode_for_new_keyspaces": "enabled",
              "hinted_handoff_enabled": False}
    locations = [('dc0', 'myrack0'), ('dc0', 'myrack1'), ('dc1', 'myrack2')]
    servers = [await manager.server_add(config=CONFIG, property_file={'dc': dc, 'rack': rack}) for dc, rack in locations]
    fresh, stale = servers[:2]
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "with replication = {'class': 'NetworkTopologyStrategy', 'dc0': ['myrack0', 'myrack1'], 'dc1': ['myrack2']} and tablets = {'initial': 1}") as ks:
        await cql.run_async(f"create table {ks}.t (pk int primary key, v int)")
        hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
        fresh_host = next(h for h in hosts if h.address == fresh.ip_addr)

        async with tablets_in_write_both_read_new(manager, servers, ks, "'dc0': ['myrack0', 'myrack1'], 'dc1': []"):
            # The stale replica rejects the write, so the dc0 replicas diverge. Done in the held
            # stage, because the rebuild_repair stage before it syncs the replicas.
            await manager.api.enable_injection(stale.ip_addr, "database_apply", one_shot=False,
                                               parameters={"ks_name": ks, "cf_name": "t", "what": "throw"})
            try:
                stmt = SimpleStatement(f"insert into {ks}.t (pk, v) values (0, 1)", consistency_level=ConsistencyLevel.ALL)
                with pytest.raises(WriteFailure):
                    await cql.run_async(stmt, host=fresh_host)
            finally:
                await manager.api.disable_injection(stale.ip_addr, "database_apply")

            # Not retried: the read repair of a failed read already fixed the replicas, so a
            # retried read would succeed.
            stmt = SimpleStatement(f"select v from {ks}.t where pk = 0 using timeout 10s", consistency_level=ConsistencyLevel.ALL,
                                   retry_policy=FallthroughRetryPolicy())
            rows = await asyncio.wait_for(cql.run_async(stmt, host=fresh_host), 60)
            assert [r.v for r in rows] == [1]
