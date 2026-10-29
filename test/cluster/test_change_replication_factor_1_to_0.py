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
from test.cluster.util import new_test_keyspace, wait_for_token_ring_and_group0_consistency, get_topology_coordinator, \
    release_tablets_and_await_alter, raise_if_alter_ended, get_replication
from test.pylib.tablets import get_tablet_info
from test.pylib.rest_client import read_barrier
from test.pylib.util import wait_for

logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "use_tablets",
    [
        pytest.param(False, id="vnodes"),
        pytest.param(True, id="tablets", marks=pytest.mark.tier2),
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
        body_failed = True
        try:
            # In the cleanup stage the tablet reads from and writes to its dc1 replica, and
            # the schema switch waits for the migration to end.
            async def tablet_in_cleanup():
                raise_if_alter_ended(alter)
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
            body_failed = False
        finally:
            await release_tablets_and_await_alter(manager, coord_serv, "cleanup_tablet_wait", alter, body_failed)


# Runs the ALTER with every tablet of ks.t held in write_both_read_new. Rack lists keep the old
# schema until the migration ends, numeric RFs switch it first, as asserted on next_replication.
@asynccontextmanager
async def tablets_in_write_both_read_new(manager: ScyllaClusterManager, servers, ks: str, replication: str, rack_lists: bool = True):
    cql = manager.get_cql()
    assert isinstance(get_replication(cql, ks)["dc0"], list) == rack_lists
    coord = await get_topology_coordinator(manager)
    coord_serv = await manager.find_server_by_host_id(servers, coord)
    await manager.api.enable_injection(coord_serv.ip_addr, "write_both_read_new_tablet_wait", one_shot=False)
    alter = cql.run_async(f"alter keyspace {ks} with replication = {{'class': 'NetworkTopologyStrategy', {replication}}}")
    body_failed = True
    try:
        # Asked of every server: the read barrier makes each coordinator use the stage.
        async def tablet_in_stage():
            raise_if_alter_ended(alter)
            for server in servers:
                tablet = await get_tablet_info(manager, server, ks, "t", 0)
                if tablet.stage != "write_both_read_new":
                    return None
            return True
        await wait_for(tablet_in_stage, time.time() + 60)
        ks_row = (await cql.run_async(f"select next_replication from system_schema.keyspaces where keyspace_name = '{ks}'"))[0]
        assert (ks_row.next_replication is not None) == rack_lists
        yield
        body_failed = False
    finally:
        await release_tablets_and_await_alter(manager, coord_serv, "write_both_read_new_tablet_wait", alter, body_failed)


# Adding dc1: the write set is the dc0 replica and the dc1 replica is pending, while reads
# already use both. A QUORUM write needs the quorum of the write set plus the pending replica,
# 2 of 2. The read set, or a numeric schema RF which already counts dc1, asks for 3.
@pytest.mark.parametrize("rack_lists", [True, False], ids=["rack_lists", "numeric"])
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_quorum_write_available_while_adding_dc(manager: ScyllaClusterManager, rack_lists: bool) -> None:
    CONFIG = {"endpoint_snitch": "GossipingPropertyFileSnitch", "tablets_mode_for_new_keyspaces": "enabled",
              "rf_rack_valid_keyspaces": rack_lists}
    servers = [await manager.server_add(config=CONFIG, property_file={'dc': f'dc{i}', 'rack': f'myrack{i}'}) for i in range(2)]
    cql = manager.get_cql()
    old, new = ("'dc0': ['myrack0']", "'dc0': ['myrack0'], 'dc1': ['myrack1']") if rack_lists else ("'dc0': 1", "'dc0': 1, 'dc1': 1")
    async with new_test_keyspace(manager, f"with replication = {{'class': 'NetworkTopologyStrategy', {old}}} and tablets = {{'initial': 1}}") as ks:
        await cql.run_async(f"create table {ks}.t (pk int primary key)")
        await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
        async with tablets_in_write_both_read_new(manager, servers, ks, new, rack_lists):
            stmt = SimpleStatement(f"insert into {ks}.t (pk) values (0)", consistency_level=ConsistencyLevel.QUORUM)
            try:
                await cql.run_async(stmt)
            except Unavailable as e:
                pytest.fail(f"QUORUM write unavailable with all replicas up: {e}")


# Dropping dc1: the dc1 replica still takes writes, while reads use only dc0. A QUORUM write
# must wait for 2 of the 2 write replicas. The read set, or a numeric schema RF which already
# dropped dc1, asks for 1, so the dc1 ack alone completes a write that a dc0 read misses.
@pytest.mark.parametrize("rack_lists", [True, False], ids=["rack_lists", "numeric"])
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_quorum_write_waits_for_remaining_dc_while_dropping_dc(manager: ScyllaClusterManager, rack_lists: bool) -> None:
    CONFIG = {"endpoint_snitch": "GossipingPropertyFileSnitch", "tablets_mode_for_new_keyspaces": "enabled",
              "rf_rack_valid_keyspaces": rack_lists}
    servers = [await manager.server_add(config=CONFIG, property_file={'dc': f'dc{i}', 'rack': f'myrack{i}'}) for i in range(2)]
    dc0 = servers[0]
    cql = manager.get_cql()
    old, new = ("'dc0': ['myrack0'], 'dc1': ['myrack1']", "'dc0': ['myrack0'], 'dc1': []") if rack_lists else ("'dc0': 1, 'dc1': 1", "'dc0': 1, 'dc1': 0")
    async with new_test_keyspace(manager, f"with replication = {{'class': 'NetworkTopologyStrategy', {old}}} and tablets = {{'initial': 1}}") as ks:
        await cql.run_async(f"create table {ks}.t (pk int primary key)")
        hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
        dc1_host = next(h for h in hosts if h.datacenter == "dc1")
        async with tablets_in_write_both_read_new(manager, servers, ks, new, rack_lists):
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


# A rack-list RF change moves the tablets of base tables before those of views, and switches the
# schema last. While the tablets of tv move, the tablets of t have a dc1 replica and no transition,
# and the schema RF is still 1. A QUORUM write to t must wait for 2 of the 2 replicas.
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_quorum_write_waits_for_moved_tablet_while_adding_dc(manager: ScyllaClusterManager) -> None:
    CONFIG = {"endpoint_snitch": "GossipingPropertyFileSnitch", "tablets_mode_for_new_keyspaces": "enabled"}
    servers = [await manager.server_add(config=CONFIG, property_file={'dc': f'dc{i}', 'rack': f'myrack{i}'}) for i in range(2)]
    dc1 = servers[1]
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "with replication = {'class': 'NetworkTopologyStrategy', 'dc0': ['myrack0']} and tablets = {'initial': 4}") as ks:
        await cql.run_async(f"create table {ks}.t (pk int primary key, v int)")
        await cql.run_async(f"create materialized view {ks}.tv as select * from {ks}.t where pk is not null and v is not null primary key (v, pk)")
        hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
        dc0_host = next(h for h in hosts if h.datacenter == "dc0")
        table_ids = {name: await manager.get_table_or_view_id(ks, name) for name in ("t", "tv")}

        # Asked of every server: the read barrier makes each coordinator use the latest tablet metadata.
        async def tablets_of_every_server():
            result = []
            for server in servers:
                await read_barrier(manager.api, server.ip_addr)
                host = cql.cluster.metadata.get_host(server.ip_addr)
                result.append({name: await cql.run_async(f"select stage, replicas from system.tablets where table_id = {table_id}", host=host)
                               for name, table_id in table_ids.items()})
            return result

        coord = await get_topology_coordinator(manager)
        coord_serv = await manager.find_server_by_host_id(servers, coord)
        await manager.api.enable_injection(coord_serv.ip_addr, "write_both_read_new_tablet_wait", one_shot=False)
        alter = cql.run_async(f"alter keyspace {ks} with replication = {{'class': 'NetworkTopologyStrategy', 'dc0': ['myrack0'], 'dc1': ['myrack1']}}")
        body_failed = True
        try:
            # No tablet of tv moves until every tablet of t has its dc1 replica.
            async def t_held():
                raise_if_alter_ended(alter)
                for tablets in await tablets_of_every_server():
                    if any(row.stage != "write_both_read_new" for row in tablets["t"]):
                        return None
                return True
            await wait_for(t_held, time.time() + 60)

            # The tablets of t have passed streaming, so holding streaming holds only the tablets of tv.
            await manager.api.enable_injection(coord_serv.ip_addr, "stream_tablet_wait", one_shot=False)
            await manager.api.disable_injection(coord_serv.ip_addr, "write_both_read_new_tablet_wait")
            await manager.enable_tablet_balancing()
            async def t_moved_tv_held():
                raise_if_alter_ended(alter)
                for tablets in await tablets_of_every_server():
                    if any(row.stage is not None or len(row.replicas) != 2 for row in tablets["t"]):
                        return None
                    if not any(row.stage == "streaming" for row in tablets["tv"]):
                        return None
                return True
            await wait_for(t_moved_tv_held, time.time() + 60)
            ks_row = (await cql.run_async(f"select next_replication from system_schema.keyspaces where keyspace_name = '{ks}'"))[0]
            assert ks_row.next_replication is not None
            assert "dc1" not in get_replication(cql, ks)

            # Both replicas ack, so a timeout below is caused by blocking dc1.
            stmt = SimpleStatement(f"insert into {ks}.t (pk) values (1)", consistency_level=ConsistencyLevel.QUORUM)
            await cql.run_async(stmt, host=dc0_host)

            await manager.api.enable_injection(dc1.ip_addr, "database_apply", one_shot=False,
                                               parameters={"ks_name": ks, "cf_name": "t", "what": "wait"})
            try:
                # Coordinated in dc0, so the blocked dc1 replica does not delay the timeout response.
                stmt = SimpleStatement(f"insert into {ks}.t (pk) values (0) using timeout 5s", consistency_level=ConsistencyLevel.QUORUM)
                with pytest.raises(WriteTimeout):
                    await asyncio.wait_for(cql.run_async(stmt, host=dc0_host), 60)
            finally:
                await manager.api.disable_injection(dc1.ip_addr, "database_apply")
            body_failed = False
        finally:
            await manager.api.disable_injection(coord_serv.ip_addr, "write_both_read_new_tablet_wait")
            await release_tablets_and_await_alter(manager, coord_serv, "stream_tablet_wait", alter, body_failed)


# Dropping dc1 with dc0 on two racks (scylladb/scylladb#20625): reads use the two dc0 replicas,
# writes also go to dc1. A CL ALL read repairs the diverged dc0 replicas, and the repair must wait
# for 2 of its 2 targets. Sizing the repair from the write set asks for 3 and fails the read.
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_read_repair_completes_while_dropping_dc(manager: ScyllaClusterManager) -> None:
    CONFIG = {"endpoint_snitch": "GossipingPropertyFileSnitch", "tablets_mode_for_new_keyspaces": "enabled",
              "hinted_handoff_enabled": False}
    locations = [('dc0', 'myrack0'), ('dc0', 'myrack1'), ('dc1', 'myrack2')]
    servers = [await manager.server_add(config=CONFIG, property_file={'dc': dc, 'rack': rack}) for dc, rack in locations]
    fresh, stale = servers[:2]
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "with replication = {'class': 'NetworkTopologyStrategy', 'dc0': ['myrack0', 'myrack1'], 'dc1': ['myrack2']} "
                                          "and tablets = {'initial': 1}") as ks:
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
