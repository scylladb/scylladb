#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from cassandra.protocol import ConfigurationException
from cassandra.query import SimpleStatement, ConsistencyLevel
from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import HTTPError
from test.pylib.tablets import get_all_tablet_replicas
from test.pylib.util import read_barrier
from test.topology.util import wait_for_cql_and_get_hosts
import time
import pytest
import logging
import asyncio
import re
import requests
import random

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_tablet_replication_factor_enough_nodes(manager: ManagerClient):
    cfg = {'enable_user_defined_functions': False,
           'experimental_features': ['tablets']}
    servers = await manager.servers_add(2, config=cfg)

    cql = manager.get_cql()
    res = await cql.run_async("SELECT data_center FROM system.local")
    this_dc = res[0].data_center

    await cql.run_async(f"CREATE KEYSPACE test WITH replication = {{'class': 'NetworkTopologyStrategy', '{this_dc}': 3}}")
    with pytest.raises(ConfigurationException, match=f"Datacenter {this_dc} doesn't have enough nodes"):
        await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int);")

    await cql.run_async(f"ALTER KEYSPACE test WITH replication = {{'class': 'NetworkTopologyStrategy', '{this_dc}': 2}}")
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int);")


@pytest.mark.asyncio
async def test_tablet_cannot_decommision_below_replication_factor(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    cfg = {'enable_user_defined_functions': False, 'experimental_features': ['tablets']}
    servers = await manager.servers_add(4, config=cfg)

    logger.info("Creating table")
    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}")
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int);")

    logger.info("Populating table")
    keys = range(256)
    await asyncio.gather(*[cql.run_async(f"INSERT INTO test.test (pk, c) VALUES ({k}, {k});") for k in keys])

    logger.info("Decommission some node")
    await manager.decommission_node(servers[0].server_id)

    with pytest.raises(HTTPError, match="Decommission failed"):
        logger.info("Decommission another node")
        await manager.decommission_node(servers[1].server_id)

    # Three nodes should still provide CL=3
    logger.info("Checking table")
    query = SimpleStatement("SELECT * FROM test.test;", consistency_level=ConsistencyLevel.THREE)
    rows = await cql.run_async(query)
    assert len(rows) == len(keys)
    for r in rows:
        assert r.c == r.pk

async def test_reshape_with_tablets(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    cfg = {'enable_user_defined_functions': False, 'experimental_features': ['tablets']}
    server = (await manager.servers_add(1, config=cfg, cmdline=['--smp', '1']))[0]

    logger.info("Creating table")
    cql = manager.get_cql()
    number_of_tablets = 2
    await cql.run_async(f"CREATE KEYSPACE test WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} and tablets = {{'initial': {number_of_tablets} }}")
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int);")

    logger.info("Disabling autocompaction for the table")
    await manager.api.disable_autocompaction(server.ip_addr, "test", "test")

    logger.info("Populating table")
    loop_count = 32
    for _ in range(loop_count):
        await asyncio.gather(*[cql.run_async(f"INSERT INTO test.test (pk, c) VALUES ({k}, {k});") for k in range(64)])
        await manager.api.keyspace_flush(server.ip_addr, "test", "test")
    # After populating the table, expect loop_count number of sstables per tablet
    sstable_info = await manager.api.get_sstable_info(server.ip_addr, "test", "test")
    assert len(sstable_info[0]['sstables']) == number_of_tablets * loop_count

    log = await manager.server_open_log(server.server_id)
    mark = await log.mark()

    # Restart the server and verify that the sstables have been reshaped down to one sstable per tablet
    logger.info("Restart the server")
    await manager.server_restart(server.server_id)

    await log.wait_for("Reshape test.test .* Reshaped 32 sstables to .*", mark, 30)
    sstable_info = await manager.api.get_sstable_info(server.ip_addr, "test", "test")
    assert len(sstable_info[0]['sstables']) == number_of_tablets


@pytest.mark.parametrize("direction", ["up", "down", "none"])
@pytest.mark.asyncio
async def test_tablet_rf_change(manager: ManagerClient, direction):
    cfg = {'enable_user_defined_functions': False,
           'experimental_features': ['tablets']}
    servers = await manager.servers_add(3, config=cfg)
    for s in servers:
        await manager.api.disable_tablet_balancing(s.ip_addr)

    cql = manager.get_cql()
    res = await cql.run_async("SELECT data_center FROM system.local")
    this_dc = res[0].data_center

    if direction == 'up':
        rf_from = 2
        rf_to = 3
    if direction == 'down':
        rf_from = 3
        rf_to = 2
    if direction == 'none':
        rf_from = 2
        rf_to = 2

    await cql.run_async(f"CREATE KEYSPACE test WITH replication = {{'class': 'NetworkTopologyStrategy', '{this_dc}': {rf_from}}}")
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int);")

    logger.info("Populating table")
    await asyncio.gather(*[cql.run_async(f"INSERT INTO test.test (pk, c) VALUES ({k}, {k});") for k in range(128)])

    async def check_allocated_replica(expected: int):
        replicas = await get_all_tablet_replicas(manager, servers[0], 'test', 'test')
        for r in replicas:
            logger.info(f"{r.replicas}")
            assert len(r.replicas) == expected

    logger.info(f"Checking {rf_from} allocated replicas")
    await check_allocated_replica(rf_from)

    logger.info(f"Altering RF {rf_from} -> {rf_to}")
    await cql.run_async(f"ALTER KEYSPACE test WITH replication = {{'class': 'NetworkTopologyStrategy', '{this_dc}': {rf_to}}}")

    logger.info(f"Checking {rf_to} re-allocated replicas")
    await check_allocated_replica(rf_to)

    fragments = { pk: set() for pk in random.sample(range(128), 17) }
    for s in servers:
        host_id = await manager.get_host_id(s.server_id)
        host = await wait_for_cql_and_get_hosts(cql, [s], time.time() + 30)
        await read_barrier(manager.get_cql(), host[0]) # scylladb/scylladb#18199
        for k in fragments:
            res = await cql.run_async(f"SELECT partition_region FROM MUTATION_FRAGMENTS(test.test) WHERE pk={k}", host=host[0])
            for fragment in res:
                if fragment.partition_region == 0: # partition start
                    fragments[k].add(host_id)
    logger.info("Checking fragments")
    for k in fragments:
        assert len(fragments[k]) == rf_to, f"Found mutations for {k} key on {fragments[k]} hosts, but expected only {rf_to} of them"


# Reproducer for https://github.com/scylladb/scylladb/issues/18110
# Check that an existing cached read, will be cleaned up when the tablet it reads
# from is migrated away.
@pytest.mark.asyncio
async def test_saved_readers_tablet_migration(manager: ManagerClient, mode):
    cfg = {
            'enable_user_defined_functions': False,
            'experimental_features': ['tablets'],
    }

    if mode != "release":
        cfg['error_injections_at_startup'] = [{'name': 'querier-cache-ttl-seconds', 'value': 999999999}]

    servers = await manager.servers_add(2, config=cfg)

    cql = manager.get_cql()

    await cql.run_async("CREATE KEYSPACE test WITH"
                        " replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"
                        " and tablets = {'initial': 1}")
    await cql.run_async("CREATE TABLE test.test (pk int, ck int, c int, PRIMARY KEY (pk, ck));")

    logger.info("Populating table")
    await asyncio.gather(*[cql.run_async(f"INSERT INTO test.test (pk, ck, c) VALUES (0, {k}, 0);") for k in range(128)])

    statement = SimpleStatement("SELECT * FROM test.test WHERE pk = 0", fetch_size=10)
    cql.execute(statement)

    def get_querier_cache_population(server):
        metrics = requests.get(f"http://{server.ip_addr}:9180/metrics").text
        pattern = re.compile("^scylla_database_querier_cache_population")
        for metric in metrics.split('\n'):
            if pattern.match(metric) is not None:
                return int(float(metric.split()[1]))

    assert any(map(lambda x: x > 0, [get_querier_cache_population(server) for server in servers]))

    table_id = await cql.run_async("SELECT id FROM system_schema.tables WHERE keyspace_name = 'test' AND table_name = 'test'")
    table_id = table_id[0].id

    tablet_infos = await cql.run_async(f"SELECT last_token, replicas FROM system.tablets WHERE table_id = {table_id}")
    tablet_infos = list(tablet_infos)

    assert len(tablet_infos) == 1
    tablet_info = tablet_infos[0]
    assert len(tablet_info.replicas) == 1

    hosts = {await manager.get_host_id(server.server_id) for server in servers}
    print(f"HOSTS: {hosts}")
    source_host, source_shard = tablet_info.replicas[0]

    hosts.remove(str(source_host))
    target_host, target_shard = list(hosts)[0], source_shard

    await manager.api.move_tablet(
           node_ip=servers[0].ip_addr,
           ks="test",
           table="test",
           src_host=source_host,
           src_shard=source_shard,
           dst_host=target_host,
           dst_shard=target_shard,
           token=tablet_info.last_token)

    # The tablet move should have evicted the cached reader.
    assert all(map(lambda x: x == 0, [get_querier_cache_population(server) for server in servers]))
