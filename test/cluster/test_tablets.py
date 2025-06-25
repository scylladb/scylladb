#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
from cassandra.protocol import ConfigurationException, InvalidRequest, SyntaxException
from cassandra.query import SimpleStatement, ConsistencyLevel
from test.cluster.test_tablets2 import safe_rolling_restart
from test.pylib.internal_types import ServerInfo
from test.pylib.manager_client import ManagerClient
from test.pylib.repair import create_table_insert_data_for_repair
from test.pylib.rest_client import HTTPError, read_barrier
from test.pylib.scylla_cluster import ReplaceConfig
from test.pylib.tablets import get_tablet_replica, get_all_tablet_replicas
from test.pylib.util import unique_name, wait_for
from test.cluster.conftest import skip_mode
from test.cluster.util import wait_for_cql_and_get_hosts, create_new_test_keyspace, new_test_keyspace, reconnect_driver, get_topology_coordinator
from contextlib import nullcontext as does_not_raise
import time
import pytest
import logging
import asyncio
import re
import requests
import random
import os
import glob
import shutil

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_tablet_replication_factor_enough_nodes(manager: ManagerClient):
    cfg = {'enable_user_defined_functions': False, 'tablets_mode_for_new_keyspaces': 'enabled'}
    # This test verifies that Scylla rejects creating a table if there are too few token-owning nodes.
    # That means that a keyspace must already be in place, but that's impossible with RF-rack-valid
    # keyspaces being enforced. We could go over this constraint by creating 3 nodes and then
    # decommissioning one of them before attempting to create a table, but if we decide to constraint
    # decommission later on, this test will have to be modified again. Let's simply disable the option.
    cfg = cfg | {'rf_rack_valid_keyspaces': False}
    servers = await manager.servers_add(2, config=cfg)

    cql = manager.get_cql()
    res = await cql.run_async("SELECT data_center FROM system.local")
    this_dc = res[0].data_center

    async with new_test_keyspace(manager, f"WITH replication = {{'class': 'NetworkTopologyStrategy', '{this_dc}': 3}}") as ks:
        with pytest.raises(ConfigurationException, match=f"Datacenter {this_dc} doesn't have enough token-owning nodes"):
            await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")

        await cql.run_async(f"ALTER KEYSPACE {ks} WITH replication = {{'class': 'NetworkTopologyStrategy', '{this_dc}': 2}}")
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")


@pytest.mark.asyncio
async def test_tablet_scaling_option_is_respected(manager: ManagerClient):
    # 32 is high enough to ensure we demand more tablets than the default choice.
    cfg = {'tablets_mode_for_new_keyspaces': 'enabled', 'tablets_initial_scale_factor': 32}
    servers = await manager.servers_add(1, config=cfg, cmdline=['--smp', '2'])

    cql = manager.get_cql()

    await cql.run_async(f"CREATE KEYSPACE test WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}}")
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int);")

    tablets = await get_all_tablet_replicas(manager, servers[0], 'test', 'test')
    assert len(tablets) == 64


@pytest.mark.asyncio
async def test_tablet_cannot_decommision_below_replication_factor(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    cfg = {'enable_user_defined_functions': False, 'tablets_mode_for_new_keyspaces': 'enabled'}
    servers = await manager.servers_add(4, config=cfg, property_file=[
        {"dc": "dc1", "rack": "r1"},
        {"dc": "dc1", "rack": "r1"},
        {"dc": "dc1", "rack": "r2"},
        {"dc": "dc1", "rack": "r3"}
    ])

    logger.info("Creating table")
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")

        logger.info("Populating table")
        keys = range(256)
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({k}, {k});") for k in keys])

        logger.info("Decommission some node")
        await manager.decommission_node(servers[0].server_id)

        with pytest.raises(HTTPError, match="Decommission failed"):
            logger.info("Decommission another node")
            await manager.decommission_node(servers[1].server_id)

        # Three nodes should still provide CL=3
        logger.info("Checking table")
        query = SimpleStatement(f"SELECT * FROM {ks}.test;", consistency_level=ConsistencyLevel.THREE)
        rows = await cql.run_async(query)
        assert len(rows) == len(keys)
        for r in rows:
            assert r.c == r.pk

async def test_reshape_with_tablets(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    cfg = {'enable_user_defined_functions': False, 'tablets_mode_for_new_keyspaces': 'enabled'}
    server = (await manager.servers_add(1, config=cfg, cmdline=['--smp', '1']))[0]

    logger.info("Creating table")
    cql = manager.get_cql()
    number_of_tablets = 2
    async with new_test_keyspace(manager, f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} and tablets = {{'initial': {number_of_tablets} }}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")

        logger.info("Disabling autocompaction for the table")
        await manager.api.disable_autocompaction(server.ip_addr, ks, "test")

        logger.info("Populating table")
        loop_count = 32
        for _ in range(loop_count):
            await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({k}, {k});") for k in range(64)])
            await manager.api.keyspace_flush(server.ip_addr, ks, "test")
        # After populating the table, expect loop_count number of sstables per tablet
        sstable_info = await manager.api.get_sstable_info(server.ip_addr, ks, "test")
        assert len(sstable_info[0]['sstables']) == number_of_tablets * loop_count

        log = await manager.server_open_log(server.server_id)
        mark = await log.mark()

        # Restart the server and verify that the sstables have been reshaped down to one sstable per tablet
        logger.info("Restart the server")
        await manager.server_restart(server.server_id)
        await reconnect_driver(manager)

        await log.wait_for(f"Reshape {ks}.test .* Reshaped 32 sstables to .*", from_mark=mark, timeout=30)
        sstable_info = await manager.api.get_sstable_info(server.ip_addr, ks, "test")
        assert len(sstable_info[0]['sstables']) == number_of_tablets


@pytest.mark.parametrize("direction", ["up", "down", "none"])
@pytest.mark.asyncio
async def test_tablet_rf_change(manager: ManagerClient, direction):
    cfg = {'enable_user_defined_functions': False, 'tablets_mode_for_new_keyspaces': 'enabled'}
    servers = await manager.servers_add(2, config=cfg, auto_rack_dc="dc1")
    for s in servers:
        await manager.api.disable_tablet_balancing(s.ip_addr)

    cql = manager.get_cql()
    res = await cql.run_async("SELECT data_center FROM system.local")
    this_dc = res[0].data_center

    if direction == 'up':
        rf_from = 1
        rf_to = 2
    if direction == 'down':
        rf_from = 2
        rf_to = 1
    if direction == 'none':
        rf_from = 1
        rf_to = 1

    async with new_test_keyspace(manager, f"WITH replication = {{'class': 'NetworkTopologyStrategy', '{this_dc}': {rf_from}}}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")
        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.test_mv AS SELECT pk FROM {ks}.test WHERE pk IS NOT NULL PRIMARY KEY (pk)")

        logger.info("Populating table")
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({k}, {k});") for k in range(128)])

        async def check_allocated_replica(expected: int):
            replicas = await get_all_tablet_replicas(manager, servers[0], ks, 'test')
            replicas = replicas + await get_all_tablet_replicas(manager, servers[0], ks, 'test_mv', is_view=True)
            for r in replicas:
                logger.info(f"{r.replicas}")
                assert len(r.replicas) == expected

        logger.info(f"Checking {rf_from} allocated replicas")
        await check_allocated_replica(rf_from)

        logger.info(f"Altering RF {rf_from} -> {rf_to}")
        await cql.run_async(f"ALTER KEYSPACE {ks} WITH replication = {{'class': 'NetworkTopologyStrategy', '{this_dc}': {rf_to}}}")

        logger.info(f"Checking {rf_to} re-allocated replicas")
        await check_allocated_replica(rf_to)

        if direction != 'up':
            # Don't check fragments for up/none changes, scylla crashes when checking nodes
            # that (validly) miss the replica, see scylladb/scylladb#18786
            return

        fragments = { pk: set() for pk in random.sample(range(128), 17) }
        for s in servers:
            host_id = await manager.get_host_id(s.server_id)
            host = await wait_for_cql_and_get_hosts(cql, [s], time.time() + 30)
            await read_barrier(manager.api, s.ip_addr)  # scylladb/scylladb#18199
            for k in fragments:
                res = await cql.run_async(f"SELECT partition_region FROM MUTATION_FRAGMENTS({ks}.test) WHERE pk={k}", host=host[0])
                for fragment in res:
                    if fragment.partition_region == 0: # partition start
                        fragments[k].add(host_id)
        logger.info("Checking fragments")
        for k in fragments:
            assert len(fragments[k]) == rf_to, f"Found mutations for {k} key on {fragments[k]} hosts, but expected only {rf_to} of them"


@pytest.mark.asyncio
async def test_tablet_mutation_fragments_unowned_partition(manager: ManagerClient):
    """Check that MUTATION_FRAGMENTS() queries handle the case when a partition
    not owned by the node is attempted to be read."""
    cfg = {'enable_user_defined_functions': False,
           'tablets_mode_for_new_keyspaces': 'enabled' }
    servers = await manager.servers_add(3, config=cfg, property_file=[
        {"dc": "dc1", "rack": "r1"},
        {"dc": "dc1", "rack": "r1"},
        {"dc": "dc1", "rack": "r2"}
    ])

    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")

        logger.info("Populating table")
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({k}, {k});") for k in range(4)])

        for s in servers:
            host_id = await manager.get_host_id(s.server_id)
            host = await wait_for_cql_and_get_hosts(cql, [s], time.time() + 30)
            for k in range(4):
                await cql.run_async(f"SELECT partition_region FROM MUTATION_FRAGMENTS({ks}.test) WHERE pk={k}", host=host[0])


# ALTER KEYSPACE cannot change the replication factor by more than 1 at a time.
# That provides us with a guarantee that the old and the new QUORUM overlap.
# In this test, we verify that in a simple scenario with one DC. We explicitly disable
# enforcing RF-rack-valid keyspaces to be able to perform more flexible alterations.
@pytest.mark.asyncio
async def test_singledc_alter_tablets_rf(manager: ManagerClient):
    await manager.server_add(config={"rf_rack_valid_keyspaces": "false", "enable_tablets": "true"}, property_file={"dc": "dc1", "rack": "r1"})
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 1}") as ks:
        async def change_rf(rf):
            await cql.run_async(f"ALTER KEYSPACE {ks} WITH replication = {{'class': 'NetworkTopologyStrategy', 'dc1': {rf}}}")

        await change_rf(2) # Increasing the RF by 1 should be OK.
        await change_rf(3) # Increasing the RF by 1 again should also be OK.
        await change_rf(3) # Setting the same RF shouldn't cause any problems.
        await change_rf(4) # Increasing the RF by 1 again should still be OK.
        await change_rf(3) # Decreasing the RF by 1 should be OK.

        with pytest.raises(InvalidRequest):
            await change_rf(5) # Trying to increase the RF by 2 should fail.
        with pytest.raises(InvalidRequest):
            await change_rf(1) # Trying to decrease the RF by 2 should fail.
        with pytest.raises(InvalidRequest):
            await change_rf(10) # Trying to increase the RF by more than 2 should fail.
        with pytest.raises(InvalidRequest):
            await change_rf(0) # Trying to decrease the RF by more than 2 should fail.


# ALTER tablets KS cannot change RF of any DC by more than 1 at a time.
# In a multi-dc environment, we can create replicas in a DC that didn't have replicas before,
# but the above requirement should still be honoured, because we'd be changing RF from 0 to N in the new DC.
# Reproduces https://github.com/scylladb/scylladb/issues/20039#issuecomment-2271365060
# See also `test_singledc_alter_tablets_rf` above for basic scenarios tested
@pytest.mark.asyncio
async def test_multidc_alter_tablets_rf(request: pytest.FixtureRequest, manager: ManagerClient) -> None:
    config = {"endpoint_snitch": "GossipingPropertyFileSnitch", "tablets_mode_for_new_keyspaces": "enabled"}

    logger.info("Creating a new cluster of 2 nodes in 1st DC and 2 nodes in 2nd DC")
    # we have to have at least 2 nodes in each DC if we want to try setting RF to 2 in each DC
    await manager.servers_add(2, config=config, property_file={'dc': f'dc1', 'rack': 'myrack'})
    await manager.servers_add(2, config=config, property_file={'dc': f'dc2', 'rack': 'myrack'})

    cql = manager.get_cql()
    async with new_test_keyspace(manager, "with replication = {'class': 'NetworkTopologyStrategy', 'dc1': 1}") as ks:
        # need to create a table to not change only the schema, but also tablets replicas
        await cql.run_async(f"create table {ks}.t (pk int primary key)")
        with pytest.raises(InvalidRequest, match="Only one DC's RF can be changed at a time and not by more than 1"):
            # changing RF of dc2 from 0 to 2 should fail
            await cql.run_async(f"alter keyspace {ks} with replication = {{'class': 'NetworkTopologyStrategy', 'dc2': 2}}")

        # changing RF of dc2 from 0 to 1 should succeed
        await cql.run_async(f"alter keyspace {ks} with replication = {{'class': 'NetworkTopologyStrategy', 'dc2': 1}}")
        # ensure that RFs of both DCs are equal to 1 now, i.e. that omitting dc1 in above command didn't change it
        res = await cql.run_async(f"SELECT * FROM system_schema.keyspaces  WHERE keyspace_name = '{ks}'")
        assert res[0].replication['dc1'] == '1'
        assert res[0].replication['dc2'] == '1'

        # incrementing RF of 2 DCs at once should NOT succeed, because it'd leave 2 pending tablets replicas
        with pytest.raises(InvalidRequest, match="Only one DC's RF can be changed at a time and not by more than 1"):
            await cql.run_async(f"alter keyspace {ks} with replication = {{'class': 'NetworkTopologyStrategy', 'dc1': 2, 'dc2': 2}}")
        # as above, but decrementing
        with pytest.raises(InvalidRequest, match="Only one DC's RF can be changed at a time and not by more than 1"):
            await cql.run_async(f"alter keyspace {ks} with replication = {{'class': 'NetworkTopologyStrategy', 'dc1': 0, 'dc2': 0}}")
        # as above, but decrement 1 RF and increment the other
        with pytest.raises(InvalidRequest, match="Only one DC's RF can be changed at a time and not by more than 1"):
            await cql.run_async(f"alter keyspace {ks} with replication = {{'class': 'NetworkTopologyStrategy', 'dc1': 2, 'dc2': 0}}")
        # as above, but RFs are swapped
        with pytest.raises(InvalidRequest, match="Only one DC's RF can be changed at a time and not by more than 1"):
            await cql.run_async(f"alter keyspace {ks} with replication = {{'class': 'NetworkTopologyStrategy', 'dc1': 0, 'dc2': 2}}")

        # check that we can remove all replicas from dc2 by changing RF from 1 to 0
        await cql.run_async(f"alter keyspace {ks} with replication = {{'class': 'NetworkTopologyStrategy', 'dc2': 0}}")
        # check that we can remove all replicas from the cluster, i.e. change RF of dc1 from 1 to 0 as well:
        await cql.run_async(f"alter keyspace {ks} with replication = {{'class': 'NetworkTopologyStrategy', 'dc1': 0}}")


# Reproducer for https://github.com/scylladb/scylladb/issues/18110
# Check that an existing cached read, will be cleaned up when the tablet it reads
# from is migrated away.
@pytest.mark.asyncio
async def test_saved_readers_tablet_migration(manager: ManagerClient, build_mode):
    cfg = {'enable_user_defined_functions': False, 'tablets_mode_for_new_keyspaces': 'enabled'}

    if build_mode != "release":
        cfg['error_injections_at_startup'] = [{'name': 'querier-cache-ttl-seconds', 'value': 999999999}]

    servers = await manager.servers_add(2, config=cfg)

    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH"
                        " replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"
                        " and tablets = {'initial': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int, ck int, c int, PRIMARY KEY (pk, ck));")

        logger.info("Populating table")
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, ck, c) VALUES (0, {k}, 0);") for k in range(128)])

        statement = SimpleStatement(f"SELECT * FROM {ks}.test WHERE pk = 0", fetch_size=10)
        cql.execute(statement)

        def get_querier_cache_population(server):
            metrics = requests.get(f"http://{server.ip_addr}:9180/metrics").text
            pattern = re.compile("^scylla_database_querier_cache_population")
            for metric in metrics.split('\n'):
                if pattern.match(metric) is not None:
                    return int(float(metric.split()[1]))

        assert any(map(lambda x: x > 0, [get_querier_cache_population(server) for server in servers]))

        table_id = await cql.run_async(f"SELECT id FROM system_schema.tables WHERE keyspace_name = '{ks}' AND table_name = 'test'")
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
            ks=ks,
            table="test",
            src_host=source_host,
            src_shard=source_shard,
            dst_host=target_host,
            dst_shard=target_shard,
            token=tablet_info.last_token)

        # The tablet move should have evicted the cached reader.
        assert all(map(lambda x: x == 0, [get_querier_cache_population(server) for server in servers]))

# Reproducer for https://github.com/scylladb/scylladb/issues/19052
#   1) table A has N tablets and views
#   2) migration starts for a tablet of A from node 1 to 2.
#   3) migration is at write_both_read_old stage
#   4) coordinator will push writes to both nodes
#   5) A has view, so writes to it will also result in reads (table::push_view_replica_updates())
#   6) tablet's update_effective_replication_map() is not refreshing tablet sstable set (for new tablet migrating in)
#   7) so read on step 5 is not being able to find sstable set for tablet migrating in
@pytest.mark.parametrize("with_cache", ['false', 'true'])
@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_read_of_pending_replica_during_migration(manager: ManagerClient, with_cache):
    logger.info("Bootstrapping cluster")
    cfg = {'enable_user_defined_functions': False, 'tablets_mode_for_new_keyspaces': 'enabled'}
    cmdline = [
        '--logger-log-level', 'storage_service=debug',
        '--logger-log-level', 'raft_topology=debug',
        '--enable-cache', with_cache,
    ]
    servers = [await manager.server_add(cmdline=cmdline, config=cfg)]

    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1};") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")
        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv1 AS \
            SELECT * FROM {ks}.test WHERE pk IS NOT NULL AND c IS NOT NULL \
            PRIMARY KEY (c, pk);")

        servers.append(await manager.server_add(cmdline=cmdline, config=cfg))

        key = 7 # Whatever
        tablet_token = 0 # Doesn't matter since there is one tablet
        await cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({key}, 0)")
        rows = await cql.run_async(f"SELECT pk from {ks}.test")
        assert len(list(rows)) == 1

        replica = await get_tablet_replica(manager, servers[0], ks, 'test', tablet_token)

        s0_host_id = await manager.get_host_id(servers[0].server_id)
        s1_host_id = await manager.get_host_id(servers[1].server_id)
        dst_shard = 0

        await manager.api.enable_injection(servers[1].ip_addr, "stream_mutation_fragments", one_shot=True)
        s1_log = await manager.server_open_log(servers[1].server_id)
        s1_mark = await s1_log.mark()

        # Drop cache to remove dummy entry indicating that underlying mutation source is empty
        await manager.api.drop_sstable_caches(servers[1].ip_addr)

        migration_task = asyncio.create_task(
            manager.api.move_tablet(servers[0].ip_addr, ks, "test", replica[0], replica[1], s1_host_id, dst_shard, tablet_token))

        await s1_log.wait_for('stream_mutation_fragments: waiting', from_mark=s1_mark)
        s1_mark = await s1_log.mark()

        await cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({key}, 1)")
        rows = await cql.run_async(f"SELECT pk from {ks}.test")
        assert len(list(rows)) == 1

        # Release abandoned streaming
        await manager.api.message_injection(servers[1].ip_addr, "stream_mutation_fragments")
        await s1_log.wait_for('stream_mutation_fragments: done', from_mark=s1_mark)

        logger.info("Waiting for migration to finish")
        await migration_task
        logger.info("Migration done")

        rows = await cql.run_async(f"SELECT pk from {ks}.test")
        assert len(list(rows)) == 1


# Reproducer for https://github.com/scylladb/scylladb/issues/20073
@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_explicit_tablet_movement_during_decommission(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    cfg = {'enable_user_defined_functions': False, 'enable_tablets': True}
    cmdline = [
        '--logger-log-level', 'storage_service=debug',
        '--logger-log-level', 'raft_topology=debug',
    ]

    # Launch the cluster with two nodes.
    server_tasks = [asyncio.create_task(manager.server_add(cmdline=cmdline, config=cfg)) for _ in range(2)]
    servers = [await task for task in server_tasks]

    # Disable the load balancer so that it does not move tablets behind our back mid-test. This does not disable automatic tablet movement in response to
    # decommission, but we'll block the latter by injecting a wait-for-message.
    #
    # Load balancing being enabled or disabled is a cluster-global property; we can use any node to toggle it.
    logger.info("Disabling load balancing")
    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    logger.info("Populating tablet")
    # Create a table with just one partition and RF=1, so we have exactly one tablet.
    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1};")
    await cql.run_async("CREATE TABLE test.tabmv_decomm (pk int PRIMARY KEY);")
    await cql.run_async("INSERT INTO test.tabmv_decomm (pk) VALUES (0)")
    rows = await cql.run_async("SELECT pk FROM test.tabmv_decomm")
    assert len(list(rows)) == 1

    logger.info("Identifying source, destination, coordinator and non-coordinator nodes")
    # Get the sole replica (see RF=1 above) for the sole tablet. (The token value is irrelevant due to there being only one tablet.) We can ask either one of
    # the nodes.
    token = 0
    source_task = asyncio.create_task(get_tablet_replica(manager, servers[0], 'test', 'tabmv_decomm', token))

    # Get the IDs of both nodes.
    node_id_tasks = [asyncio.create_task(manager.get_host_id(srv.server_id)) for srv in servers]

    # Get the ID of the topology coordinator.
    crd_task = asyncio.create_task(get_topology_coordinator(manager))

    # Open the logs of both servers.
    log_tasks = [asyncio.create_task(manager.server_open_log(srv.server_id)) for srv in servers]

    # Collect results (completion order doesn't matter).
    src_node_id, src_shard = await source_task
    node_ids = [await task for task in node_id_tasks]
    crd_id = await crd_task
    logs = [await task for task in log_tasks]

    # The destination node is the node that is not the source node. We always use shard#0 on the destination.
    src = node_ids.index(src_node_id)
    dst = 1 - src
    dst_shard = 0

    # The coordinator node is one of the two nodes. The non-coordinator node is the other node.
    crd = node_ids.index(crd_id)
    ncr = 1 - crd

    # Four variations are possible:
    #
    # source  destination  coordinator  non-coordinator
    # ------  -----------  -----------  ---------------
    # node#0       node#1       node#0           node#1
    # node#0       node#1       node#1           node#0
    # node#1       node#0       node#0           node#1
    # node#1       node#0       node#1           node#0
    logger.info(f"src id={servers[src].server_id} ip={servers[src].ip_addr} node={node_ids[src]} shard={src_shard}")
    logger.info(f"dst id={servers[dst].server_id} ip={servers[dst].ip_addr} node={node_ids[dst]} shard={dst_shard}")
    logger.info(f"crd id={servers[crd].server_id} ip={servers[crd].ip_addr} node={node_ids[crd]}")
    logger.info(f"ncr id={servers[ncr].server_id} ip={servers[ncr].ip_addr} node={node_ids[ncr]}")

    logger.info("Decommissioning src")
    # Inject a wait-for-message into the topology coordinator. We're going to block decommission right after entering the "tablet draining" transition state.
    await manager.api.enable_injection(servers[crd].ip_addr, "suspend_decommission", one_shot=True)

    # Initiate decommissioning the source node, and wait until the coordinator reaches "tablet draining".
    crd_log_mark = await logs[crd].mark()
    decomm_task = asyncio.create_task(manager.decommission_node(servers[src].server_id))
    await logs[crd].wait_for('entered `tablet draining` transition state', from_mark=crd_log_mark)

    logger.info("Moving tablet from src to dst")
    # Move the tablet from the source node to the destination node. Ask the non-coordinator node to do it, as the coordinator node is suspended. Wait until the
    # storage service on the non-coordinator node confirms it has seen the topology state machine as busy, and that it has kept the transition state intact.
    ncr_log_mark = await logs[ncr].mark()
    move_task = asyncio.create_task(manager.api.move_tablet(servers[ncr].ip_addr, "test", "tabmv_decomm",
                                                            node_ids[src], src_shard, node_ids[dst], dst_shard, token))
    await logs[ncr].wait_for(r'transit_tablet\([^)]+\): topology busy, keeping transition state', from_mark=ncr_log_mark)

    logger.info("Completing decommissioning and tablet movement")
    # Resume decommissioning.
    await manager.api.message_injection(servers[crd].ip_addr, "suspend_decommission")

    # Complete both the decommissioning and the explicit tablet movement (completion order does not matter).
    #
    # Completion of "decomm_task" shows that the decommission flow doesn't get stuck.
    await decomm_task
    await move_task


# This test checks that --enable-tablets option and the TABLETS parameters of the CQL CREATE KEYSPACE
# statemement are mutually correct from the "the least surprising behavior" concept. See comments inside
# the test code for more details.
@pytest.mark.parametrize("with_tablets", [True, False])
@pytest.mark.parametrize("replication_strategy", ["NetworkTopologyStrategy", "SimpleStrategy", "EverywhereStrategy", "LocalStrategy"])
@pytest.mark.asyncio
async def test_keyspace_creation_cql_vs_config_sanity(manager: ManagerClient, with_tablets, replication_strategy):
    cfg = {'tablets_mode_for_new_keyspaces': 'enabled' if with_tablets else 'disabled'}
    server = await manager.server_add(config=cfg)
    cql = manager.get_cql()

    # Tablets are only possible when the replication strategy is NetworkTopology
    tablets_possible = (replication_strategy == 'NetworkTopologyStrategy')
    tablets_enabled_by_default = tablets_possible and with_tablets

    # First, check if a kesypace is able to be created with default CQL statement that
    # doesn't contain tablets parameters. When possible, tablets should be activated
    async with new_test_keyspace(manager, f"WITH replication = {{'class': '{replication_strategy}', 'replication_factor': 1}}") as ks:
        res = cql.execute(f"SELECT initial_tablets FROM system_schema.scylla_keyspaces WHERE keyspace_name = '{ks}'").one()
        if tablets_enabled_by_default:
            assert res.initial_tablets == 0
        else:
            assert res is None

    # Next, check that explicit CQL request for enabling tablets can only be satisfied when
    # tablets are possible. Tablets must be activated in this case
    if tablets_possible:
        expectation = does_not_raise()
    else:
        expectation = pytest.raises(ConfigurationException)
    with expectation:
        ks = await create_new_test_keyspace(cql, f"WITH replication = {{'class': '{replication_strategy}', 'replication_factor': 1}} AND TABLETS = {{'enabled': true}}")
        res = cql.execute(f"SELECT initial_tablets FROM system_schema.scylla_keyspaces WHERE keyspace_name = '{ks}'").one()
        assert res.initial_tablets == 0
        await cql.run_async(f"drop keyspace {ks}")

    # Finally, check that explicitly disabling tablets in CQL results in vnode-based keyspace
    # whenever tablets are enabled or not in config
    async with new_test_keyspace(manager, f"WITH replication = {{'class': '{replication_strategy}', 'replication_factor': 1}} AND TABLETS = {{'enabled': false}}") as ks:
        res = cql.execute(f"SELECT initial_tablets FROM system_schema.scylla_keyspaces WHERE keyspace_name = '{ks}'").one()
        assert res is None

@pytest.mark.asyncio
async def test_tablets_and_gossip_topology_changes_are_incompatible(manager: ManagerClient):
    cfg = {"tablets_mode_for_new_keyspaces": "enabled", "force_gossip_topology_changes": True}
    with pytest.raises(Exception, match="Failed to add server"):
        await manager.server_add(config=cfg)

@pytest.mark.asyncio
async def test_tablets_disabled_with_gossip_topology_changes(manager: ManagerClient):
    cfg = {"tablets_mode_for_new_keyspaces": "disabled", "force_gossip_topology_changes": True}
    await manager.server_add(config=cfg)
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks_name:
        res = cql.execute(f"SELECT * FROM system_schema.scylla_keyspaces WHERE keyspace_name = '{ks_name}'").one()
        logger.info(res)

    for enabled in ["false", "true"]:
        expected = r"Error from server: code=2000 \[Syntax error in CQL query\] message=\"line 1:126 no viable alternative at input 'tablets'\""
        with pytest.raises(SyntaxException, match=expected):
            ks_name = unique_name()
            await cql.run_async(f"CREATE KEYSPACE {ks_name} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} AND tablets {{'enabled': {enabled}}};")

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_tablet_streaming_with_unbuilt_view(manager: ManagerClient):
    """
    Reproducer for https://github.com/scylladb/scylladb/issues/21564
        1) Create a table with 1 initial tablet and populate it
        2) Create a view on the table but prevent the generator from processing it using error injection
        3) Start migration of the tablet from node 1 to 2
        4) Once migration completes, the view should have the correct number of rows
    """
    logger.info("Starting Node 1")
    cfg = {'enable_user_defined_functions': False, 'tablets_mode_for_new_keyspaces': 'enabled'}
    cmdline = [
        '--logger-log-level', 'storage_service=debug',
        '--logger-log-level', 'raft_topology=debug',
        '--logger-log-level', 'view_building_coordinator=debug',
        '--logger-log-level', 'view_building_worker=debug',
    ]
    servers = [await manager.server_add(cmdline=cmdline, config=cfg)]
    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    logger.info("Create table, populate it and flush the table to disk")
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1};") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")
        num_of_rows = 64
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({k}, {k%3});") for k in range(num_of_rows)])
        await manager.api.keyspace_flush(servers[0].ip_addr, ks, "test")

        logger.info("Starting Node 2")
        servers.append(await manager.server_add(cmdline=cmdline, config=cfg))
        s1_host_id = await manager.get_host_id(servers[1].server_id)

        logger.info("Inject error to make view building worker pause before processing the sstable")
        injection_name = "view_building_worker_pause_before_consume"
        await manager.api.enable_injection(servers[0].ip_addr, injection_name, one_shot=True)

        logger.info("Create view")
        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv1 AS \
                SELECT * FROM {ks}.test WHERE pk IS NOT NULL AND c IS NOT NULL \
                PRIMARY KEY (c, pk);")

        logger.info("Migrate the tablet to node 2")
        tablet_token = 0 # Doesn't matter since there is one tablet
        replica = await get_tablet_replica(manager, servers[0], ks, 'test', tablet_token)
        await manager.api.move_tablet(servers[0].ip_addr, ks, "test", replica[0], replica[1], s1_host_id, 0, tablet_token)
        logger.info("Migration done")

        async def check_table():
            result = await cql.run_async(f"SELECT * FROM system.built_views WHERE keyspace_name='{ks}' AND view_name='mv1'")
            if len(result) == 1:
                return True
            else:
                return None
        await wait_for(check_table, time.time() + 30)

        # Verify the table has expected number of rows
        rows = await cql.run_async(f"SELECT pk from {ks}.test")
        assert len(list(rows)) == num_of_rows
        # Verify that the view has the expected number of rows
        rows = await cql.run_async(f"SELECT c from {ks}.mv1")
        assert len(list(rows)) == num_of_rows

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_tablet_streaming_with_staged_sstables(manager: ManagerClient):
    """
    Reproducer for https://github.com/scylladb/scylladb/issues/19149
        1) Create a table with 1 initial tablet and populate it
        2) Create a view on the table but prevent the generator
        3) Inject error to prevent processing of new sstables in view generator
        4) Create an sstable, move it into upload directory of test table and start upload
        5) Start migration of the tablet from node 1 to 2
        6) Once migration completes, the view should have the correct number of rows
    """
    logger.info("Starting Node 1")
    cfg = {'enable_user_defined_functions': False, 'tablets_mode_for_new_keyspaces': 'enabled'}
    cmdline = [
        '--logger-log-level', 'storage_service=debug',
        '--logger-log-level', 'raft_topology=debug',
        '--logger-log-level', 'view_building_coordinator=debug',
        '--logger-log-level', 'view_building_worker=debug',
    ]
    servers = [await manager.server_add(cmdline=cmdline, config=cfg)]
    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    logger.info("Create the test table, populate few rows and flush to disk")
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1};") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({k}, {k%3});") for k in range(64)])
        await manager.api.keyspace_flush(servers[0].ip_addr, ks, "test")

        logger.info("Create view")
        # Pause view building
        await manager.api.enable_injection(servers[0].ip_addr, "view_building_worker_pause_before_consume", one_shot=True)
        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv1 AS \
                SELECT * FROM {ks}.test WHERE pk IS NOT NULL AND c IS NOT NULL \
                PRIMARY KEY (c, pk);")
        
        # Check if view building was started
        async def check_mv_status():
            mv_status = await cql.run_async(f"SELECT status FROM system.view_build_status_v2 WHERE keyspace_name='{ks}' AND view_name='mv'")
            return len(mv_status) == 1 and mv_status[0].status == "STARTED"
        await wait_for(check_mv_status, time.time() + 30)

        logger.info("Generate an sstable and move it to upload directory of test table")
        # create an sstable using a dummy table
        await cql.run_async(f"CREATE TABLE {ks}.dummy (pk int PRIMARY KEY, c int);")
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.dummy (pk, c) VALUES ({k}, {k%3});") for k in range(64, 128)])
        await manager.api.keyspace_flush(servers[0].ip_addr, ks, "dummy")
        node_workdir = await manager.server_get_workdir(servers[0].server_id)
        dummy_table_dir = glob.glob(os.path.join(node_workdir, "data", ks, "dummy-*"))[0]
        test_table_upload_dir = glob.glob(os.path.join(node_workdir, "data", ks, "test-*", "upload"))[0]
        for src_path in glob.glob(os.path.join(dummy_table_dir, "me-*")):
            dst_path = os.path.join(test_table_upload_dir, os.path.basename(src_path))
            os.rename(src_path, dst_path)
        await cql.run_async(f"DROP TABLE {ks}.dummy;")

        logger.info("Starting Node 2")
        servers.append(await manager.server_add(cmdline=cmdline, config=cfg))
        s1_host_id = await manager.get_host_id(servers[1].server_id)

        # logger.info("Inject error to prevent view generator from processing staged sstables")
        # injection_name = "view_update_generator_consume_staging_sstable"
        # await manager.api.enable_injection(servers[0].ip_addr, injection_name, one_shot=True)

        logger.info("Load the sstables from upload directory")
        await manager.api.load_new_sstables(servers[0].ip_addr, ks, "test")

        # The table now has both staged and unstaged sstables.
        # Verify that tablet migration handles them both without causing any base-view inconsistencies.
        logger.info("Migrate the tablet to node 2")
        tablet_token = 0 # Doesn't matter since there is one tablet
        replica = await get_tablet_replica(manager, servers[0], ks, 'test', tablet_token)
        await manager.api.move_tablet(servers[0].ip_addr, ks, "test", replica[0], replica[1], s1_host_id, 0, tablet_token)
        logger.info("Migration done")

        async def check_view_is_built():
            result = await cql.run_async(f"SELECT * FROM system.built_views WHERE keyspace_name='{ks}' AND view_name='mv1'")
            if len(result) == 1:
                return True
            else:
                return None
        await wait_for(check_view_is_built, time.time() + 60)

        expected_num_of_rows = 128
        # Verify the table has expected number of rows
        rows = await cql.run_async(f"SELECT pk from {ks}.test")
        assert len(list(rows)) == expected_num_of_rows
        # Verify that the view has the expected number of rows
        rows = await cql.run_async(f"SELECT c from {ks}.mv1")
        assert len(list(rows)) == expected_num_of_rows

@pytest.mark.asyncio
async def test_orphaned_sstables_on_startup(manager: ManagerClient):
    """
    Reproducer for https://github.com/scylladb/scylladb/issues/18038
        1) Start a node (node1)
        2) Create a table with 1 initial tablet and populate it
        3) Start another node (node2)
        4) Migrate the existing tablet from node1 to node2
        5) Stop node1
        6) Copy the sstables from node2 to node1
        7) Attempting to start node1 should fail as it now has an 'orphaned' sstable
    """
    logger.info("Starting Node 1")
    cfg = {'enable_user_defined_functions': False, 'tablets_mode_for_new_keyspaces': 'enabled'}
    cmdline = [
        '--logger-log-level', 'storage_service=debug',
        '--logger-log-level', 'raft_topology=debug',
    ]
    servers = [await manager.server_add(cmdline=cmdline, config=cfg)]
    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    logger.info("Create the test table, populate few rows and flush to disk")
    cql = manager.get_cql()
    ks = await create_new_test_keyspace(cql, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 2}")
    await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")
    await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({k}, {k%3});") for k in range(256)])
    await manager.api.keyspace_flush(servers[0].ip_addr, ks, "test")
    node0_workdir = await manager.server_get_workdir(servers[0].server_id)
    node0_table_dir = glob.glob(os.path.join(node0_workdir, "data", ks, "test-*"))[0]

    logger.info("Start Node 2")
    servers.append(await manager.server_add(cmdline=cmdline, config=cfg))
    await manager.api.disable_tablet_balancing(servers[1].ip_addr)
    node1_workdir = await manager.server_get_workdir(servers[1].server_id)
    node1_table_dir = glob.glob(os.path.join(node1_workdir, "data", ks, "test-*"))[0]
    s1_host_id = await manager.get_host_id(servers[1].server_id)

    logger.info("Migrate the tablet from node1 to node2")
    tablet_token = 0 # Doesn't matter since there is one tablet
    replica = await get_tablet_replica(manager, servers[0], ks, 'test', tablet_token)
    await manager.api.move_tablet(servers[0].ip_addr, ks, "test", replica[0], replica[1], s1_host_id, 0, tablet_token)
    logger.info("Migration done")

    logger.info("Stop node1 and copy the sstables from node2")
    await manager.server_stop(servers[0].server_id)
    for src_path in glob.glob(os.path.join(node1_table_dir, "me-*")):
        dst_path = os.path.join(node0_table_dir, os.path.basename(src_path))
        shutil.copy(src_path, dst_path)

    # try starting the server again
    logger.info("Start node1 with the orphaned sstables and expect it to fail")
    # Error thrown is of format : "Unable to load SSTable {sstable_name} : Storage wasn't found for tablet {tablet_id} of table {ks}.test"
    await manager.server_start(servers[0].server_id, expected_error="Storage wasn't found for tablet")

@pytest.mark.asyncio
@pytest.mark.parametrize("with_zero_token_node", [False, True])
async def test_remove_failure_with_no_normal_token_owners_in_dc(manager: ManagerClient, with_zero_token_node: bool):
    """
    Reproducer for #21826
    Verify that a node cannot be removed with tablets when
    there are not enough nodes in a datacenter to satisfy the configured replication factor,
    even when there is a zero-token node in the same datacenter and in another datacenter,
    and when there is another down node in the datacenter, leaving no normal token owners.
    """
    servers: dict[str, list[ServerInfo]] = dict()
    servers['dc1'] = await manager.servers_add(servers_num=2, property_file=[
        {'dc': 'dc1', 'rack': 'rack1_1'},
        {'dc': 'dc1', 'rack': 'rack1_2'}])
    # if testing with no zero-token-node, add an additional node to dc2 to maintain raft quorum
    extra_node = 0 if with_zero_token_node else 1
    servers['dc2'] = await manager.servers_add(servers_num=2 + extra_node, property_file={'dc': 'dc2', 'rack': 'rack2'})
    if with_zero_token_node:
        servers['dc1'].append(await manager.server_add(config={'join_ring': False}, property_file={'dc': 'dc1', 'rack': 'rack1_1'}))
        servers['dc3'] = [await manager.server_add(config={'join_ring': False}, property_file={'dc': 'dc3', 'rack': 'rack3'})]

    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = { 'class': 'NetworkTopologyStrategy', 'dc1': 2, 'dc2': 1 } AND tablets = { 'initial': 1 }") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")

        node_to_remove = servers['dc1'][0]
        node_to_replace = servers['dc1'][1]
        replaced_host_id = await manager.get_host_id(node_to_replace.server_id)
        initiator_node = servers['dc2'][0]

        # Stop both token owners in dc1 to leave no token owners in the datacenter
        await manager.server_stop_gracefully(node_to_remove.server_id)
        await manager.server_stop_gracefully(node_to_replace.server_id)

        logger.info("Attempting removenode - expected to fail")
        await manager.remove_node(initiator_node.server_id, server_id=node_to_remove.server_id, ignore_dead=[replaced_host_id],
                                expected_error="Removenode failed. See earlier errors (Rolled back: Failed to drain tablets: std::runtime_error (There are nodes with tablets to drain")

        logger.info(f"Replacing {node_to_replace} with a new node")
        replace_cfg = ReplaceConfig(replaced_id=node_to_remove.server_id, reuse_ip_addr = False, use_host_id=True, wait_replaced_dead=True)
        await manager.server_add(replace_cfg=replace_cfg, property_file=node_to_remove.property_file())

@pytest.mark.asyncio
@pytest.mark.parametrize("with_zero_token_node", [False, True])
async def test_remove_failure_then_replace(manager: ManagerClient, with_zero_token_node: bool):
    """
    Verify that a node cannot be removed with tablets when
    there are not enough nodes in a datacenter to satisfy the configured replication factor,
    even when there is a zero-token node in the same datacenter and in another datacenter.
    And then verify that that node can be replaced successfully.
    """
    servers: dict[str, list[ServerInfo]] = dict()
    servers['dc1'] = await manager.servers_add(servers_num=2, property_file=[
        {'dc': 'dc1', 'rack': 'rack1_1'},
        {'dc': 'dc1', 'rack': 'rack1_2'}])
    servers['dc2'] = await manager.servers_add(servers_num=2, property_file={'dc': 'dc2', 'rack': 'rack2'})
    if with_zero_token_node:
        servers['dc1'].append(await manager.server_add(config={'join_ring': False}, property_file={'dc': 'dc1', 'rack': 'rack1_1'}))
        servers['dc3'] = [await manager.server_add(config={'join_ring': False}, property_file={'dc': 'dc3', 'rack': 'rack3'})]

    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = { 'class': 'NetworkTopologyStrategy', 'dc1': 2, 'dc2': 1 } AND tablets = { 'initial': 1 }") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")

        node_to_remove = servers['dc1'][0]
        initiator_node = servers['dc2'][0]

        await manager.server_stop_gracefully(node_to_remove.server_id)

        logger.info("Attempting removenode - expected to fail")
        await manager.remove_node(initiator_node.server_id, server_id=node_to_remove.server_id,
                                expected_error="Removenode failed. See earlier errors (Rolled back: Failed to drain tablets: std::runtime_error (Unable to find new replica for tablet")

        logger.info(f"Replacing {node_to_remove} with a new node")
        replace_cfg = ReplaceConfig(replaced_id=node_to_remove.server_id, reuse_ip_addr = False, use_host_id=True, wait_replaced_dead=True)
        await manager.server_add(replace_cfg=replace_cfg, property_file=node_to_remove.property_file())

@pytest.mark.asyncio
@pytest.mark.parametrize("with_zero_token_node", [False, True])
async def test_replace_with_no_normal_token_owners_in_dc(manager: ManagerClient, with_zero_token_node: bool):
    """
    Verify that nodes can be successfully replaced with tablets when
    even when there are not enough nodes in a datacenter to satisfy the configured replication factor,
    with and without zero-token nodes in the same datacenter and in another datacenter,
    and when there is another down node in the datacenter, leaving no normal token owners,
    but other datacenters can be used to rebuild the data.
    """
    servers: dict[str, list[ServerInfo]] = dict()
    servers['dc1'] = await manager.servers_add(servers_num=2, property_file=[
        {'dc': 'dc1', 'rack': 'rack1_1'},
        {'dc': 'dc1', 'rack': 'rack1_2'}])
    # if testing with no zero-token-node, add an additional node to dc2 to maintain raft quorum
    extra_node = 0 if with_zero_token_node else 1
    servers['dc2'] = await manager.servers_add(servers_num=2 + extra_node, property_file={'dc': 'dc2', 'rack': 'rack2'})
    if with_zero_token_node:
        servers['dc1'].append(await manager.server_add(config={'join_ring': False}, property_file={'dc': 'dc1', 'rack': 'rack1_1'}))
        servers['dc3'] = [await manager.server_add(config={'join_ring': False}, property_file={'dc': 'dc3', 'rack': 'rack3'})]

    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = { 'class': 'NetworkTopologyStrategy', 'dc1': 2, 'dc2': 1 } AND tablets = { 'initial': 1 }") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")

        stmt = cql.prepare(f"INSERT INTO {ks}.test (pk, c) VALUES (?, ?)")
        stmt.consistency_level = ConsistencyLevel.ALL
        keys = range(256)
        await asyncio.gather(*[cql.run_async(stmt, [k, k]) for k in keys])

        nodes_to_replace = servers['dc1'][0:2]
        replaced_host_id = await manager.get_host_id(nodes_to_replace[1].server_id)

        # Stop both token owners in dc1 to leave no token owners in the datacenter
        for node in nodes_to_replace:
            await manager.server_stop_gracefully(node.server_id)

        logger.info(f"Replacing {nodes_to_replace[0]} with a new node")
        replace_cfg = ReplaceConfig(replaced_id=nodes_to_replace[0].server_id, reuse_ip_addr = False, use_host_id=True, wait_replaced_dead=True,
                                    ignore_dead_nodes=[replaced_host_id])
        await manager.server_add(replace_cfg=replace_cfg, property_file=nodes_to_replace[0].property_file())

        logger.info(f"Replacing {nodes_to_replace[1]} with a new node")
        replace_cfg = ReplaceConfig(replaced_id=nodes_to_replace[1].server_id, reuse_ip_addr = False, use_host_id=True, wait_replaced_dead=True)
        await manager.server_add(replace_cfg=replace_cfg, property_file=nodes_to_replace[1].property_file())

        logger.info("Verifying data")
        for node in servers['dc2']:
            await manager.server_stop_gracefully(node.server_id)
        query = SimpleStatement(f"SELECT * FROM {ks}.test;", consistency_level=ConsistencyLevel.ONE)
        rows = await cql.run_async(query)
        assert len(rows) == len(keys)
        for r in rows:
            assert r.c == r.pk

        # For dropping the keyspace
        await asyncio.gather(*[manager.server_start(node.server_id) for node in servers['dc2']])

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_drop_keyspace_while_split(manager: ManagerClient):

    # Reproducer for: https://github.com/scylladb/scylladb/issues/22431
    # This tests if the split ready compaction groups are correctly created
    # on a shard with several storage groups for the same table

    logger.info("Bootstrapping cluster")
    cmdline = [ '--target-tablet-size-in-bytes', '8192',
                '--smp', '2' ]
    config = { 'error_injections_at_startup': ['short_tablet_stats_refresh_interval'] }
    servers = [await manager.server_add(config=config, cmdline=cmdline)]

    s0_log = await manager.server_open_log(servers[0].server_id)

    cql = manager.get_cql()
    await wait_for_cql_and_get_hosts(cql, [servers[0]], time.time() + 60)

    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    # create a table so that it has at least 2 tablets (and storage groups) per shard
    ks = await create_new_test_keyspace(cql, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 4};")
    await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")

    await manager.api.disable_autocompaction(servers[0].ip_addr, ks)

    keys = range(2048)
    await asyncio.gather(*[cql.run_async(f'INSERT INTO {ks}.test (pk, c) VALUES ({k}, {k});') for k in keys])
    await manager.api.flush_keyspace(servers[0].ip_addr, ks)

    await manager.api.enable_injection(servers[0].ip_addr, 'truncate_compaction_disabled_wait', one_shot=False)
    await manager.api.enable_injection(servers[0].ip_addr, 'split_storage_groups_wait', one_shot=False)

    # enable the load balancer which should emmit a tablet split
    await manager.api.enable_tablet_balancing(servers[0].ip_addr)

    # wait for compaction groups to be created and split to begin
    await s0_log.wait_for('split_storage_groups_wait: wait')

    # start a DROP and wait for it to disable compaction
    drop_ks_task = cql.run_async(f'DROP KEYSPACE {ks};')
    await s0_log.wait_for('truncate_compaction_disabled_wait: wait')

    # release split
    await manager.api.message_injection(servers[0].ip_addr, "split_storage_groups_wait")

    # release drop and wait for it to complete
    await manager.api.message_injection(servers[0].ip_addr, "truncate_compaction_disabled_wait")
    await drop_ks_task

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_two_tablets_concurrent_repair_and_migration(manager: ManagerClient):
    injection = "repair_shard_repair_task_impl_do_repair_ranges"
    servers, cql, hosts, ks, table_id = await create_table_insert_data_for_repair(manager)
    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    all_replicas = await get_all_tablet_replicas(manager, servers[0], ks, "test")
    all_replicas.sort(key=lambda x: x.last_token)
    assert len(all_replicas) >= 3
    repair_replicas = all_replicas[1]
    migration_replicas = all_replicas[0]

    logs = [await manager.server_open_log(s.server_id) for s in servers]
    marks = [await log.mark() for log in logs]

    async def repair_task():
        [await manager.api.enable_injection(s.ip_addr, injection, one_shot=True) for s in servers]
        await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", repair_replicas.last_token)

    async def migration_task():
        done, pending = await asyncio.wait([asyncio.create_task(log.wait_for('Started to repair', from_mark=mark)) for log, mark in zip(logs, marks)], return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            task.cancel()
        await manager.api.move_tablet(servers[0].ip_addr, ks, "test", migration_replicas.replicas[0][0], migration_replicas.replicas[0][1], migration_replicas.replicas[0][0], 0 if migration_replicas.replicas[0][1] != 0 else 1, migration_replicas.last_token)
        [await manager.api.message_injection(s.ip_addr, injection) for s in servers]
        [await manager.api.disable_injection(s.ip_addr, injection) for s in servers]

    await asyncio.gather(repair_task(), migration_task())

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_tablet_split_finalization_with_migrations(manager: ManagerClient):
    """
    Reproducer for https://github.com/scylladb/scylladb/issues/21762
        1) Start a cluster with two nodes with error injected to prevent resize finalisatio
        2) Create and populate `test` table
        3) Trigger a split in the table by increasing `min_tablet_count`
        4) Wait for the table `test` to reach split finalization stage
        5) Create and populate another table `blocker`
        6) Disable tablet balancing and move all tablets of `test` and `blocker` table from node 2 to node 1
        7) Enable tablet balancing and disable error injection to allow migration and finalisation to proceed.
        8) Expect finalization in `test` to be preferred over migrations in both tables
    """
    logger.info("Starting Cluster")
    cfg = {
        'enable_user_defined_functions': False, 'enable_tablets': True,
        'error_injections_at_startup': [
            'short_tablet_stats_refresh_interval',
            # intially disable transitioning into tablet_resize_finalization topology state
            'tablet_split_finalization_postpone',
            ]
        }
    cmdline = [
        '--logger-log-level', 'raft_topology=debug',
        '--logger-log-level', 'load_balancer=debug',
    ]
    servers = await manager.servers_add(2, cmdline=cmdline, config=cfg)

    logger.info("Create and populate test table")
    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 4};")
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int);")
    await manager.api.disable_autocompaction(servers[0].ip_addr, "test")
    await asyncio.gather(*[cql.run_async(f"INSERT INTO test.test (pk, c) VALUES ({k}, {k%3});") for k in range(64)])
    await manager.api.keyspace_flush(servers[0].ip_addr, "test", "test")
    test_table_id = (await cql.run_async("SELECT id FROM system_schema.tables WHERE keyspace_name = 'test' AND table_name = 'test'"))[0].id

    logger.info("Trigger split in table")
    await cql.run_async("ALTER TABLE test.test WITH tablets = {'min_tablet_count': 8};")

    # Wait for splits to finalise; they don't execute yet as they are prevented by the error injection
    logger.info("Wait for tablets to split")
    log = await manager.server_open_log(servers[0].server_id)
    await log.wait_for(f"Finalizing resize decision for table {test_table_id} as all replicas agree on sequence number 1")

    logger.info("Create and populate `blocker` table")
    await cql.run_async("CREATE TABLE test.blocker (pk int PRIMARY KEY, c int);")
    await asyncio.gather(*[cql.run_async(f"INSERT INTO test.blocker (pk, c) VALUES ({k}, {k%3});") for k in range(128)])
    await manager.api.keyspace_flush(servers[0].ip_addr, "test", "blocker")
    blocker_table_id = (await cql.run_async("SELECT id FROM system_schema.tables WHERE keyspace_name = 'test' AND table_name = 'blocker'"))[0].id

    s0_host_id = await manager.get_host_id(servers[0].server_id)
    for cf in ["test", "blocker"]:
        logger.info(f"Move all tablets of test.{cf} from Node 2 to Node 1")
        await manager.api.disable_tablet_balancing(servers[0].ip_addr)
        s1_replicas = await get_all_tablet_replicas(manager, servers[1], "test", cf)
        migration_tasks = [
            manager.api.move_tablet(servers[0].ip_addr, "test", cf,
                                    tablet.replicas[0][0], tablet.replicas[0][1],
                                    s0_host_id, 0, tablet.last_token)
            for tablet in s1_replicas
        ]
        await asyncio.gather(*migration_tasks)

    logger.info("Re-enable tablet balancing; it should be blocked by pending split finalization")
    await manager.api.enable_tablet_balancing(servers[0].ip_addr)
    mark, _ = await log.wait_for("Setting tablet balancing to true")

    logger.info("Unblock resize finalisation and verify that the finalisation is preferred over migrations")
    await manager.api.disable_injection(servers[0].ip_addr, "tablet_split_finalization_postpone")
    split_finalization_mark, _ = await log.wait_for("Finished tablet split finalization", from_mark=mark)
    for table_id in [test_table_id, blocker_table_id]:
        migration_mark, _ = await log.wait_for(f"Will set tablet {table_id}:\\d+ stage to write_both_read_old", from_mark=mark)
        assert split_finalization_mark < migration_mark, f"Tablet migration of {table_id} was scheduled before resize finalization"

    # ensure all migrations complete
    logger.info("Waiting for migrations to complete")
    await log.wait_for("Tablet load balancer did not make any plan", from_mark=migration_mark)

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_two_tablets_concurrent_repair_and_migration_repair_writer_level(manager: ManagerClient):
    injection = "repair_writer_impl_create_writer_wait"
    cmdline = [
        '--logger-log-level', 'repair=debug',
    ]
    servers, cql, hosts, ks, table_id = await create_table_insert_data_for_repair(manager, cmdline=cmdline)
    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    async def insert_with_down(down_server):
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({k}, {k + 1});") for k in range(10)])

    cql = await safe_rolling_restart(manager, [servers[0]], with_down=insert_with_down)

    await wait_for_cql_and_get_hosts(manager.get_cql(), servers, time.time() + 30)

    all_replicas = await get_all_tablet_replicas(manager, servers[1], ks, "test")
    all_replicas.sort(key=lambda x: x.last_token)
    assert len(all_replicas) >= 3
    repair_replicas = all_replicas[1]
    migration_replicas = all_replicas[0]

    logs = [await manager.server_open_log(s.server_id) for s in servers]
    marks = [await log.mark() for log in logs]

    async def repair_task():
        [await manager.api.enable_injection(s.ip_addr, injection, one_shot=True) for s in servers]
        await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", repair_replicas.last_token)

    async def migration_task():
        done, pending = await asyncio.wait([asyncio.create_task(log.wait_for(f'repair_writer: keyspace={ks}', from_mark=mark)) for log, mark in zip(logs, marks)], return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            task.cancel()
        await manager.api.move_tablet(servers[0].ip_addr, ks, "test", migration_replicas.replicas[0][0], migration_replicas.replicas[0][1], migration_replicas.replicas[0][0], 0 if migration_replicas.replicas[0][1] != 0 else 1, migration_replicas.last_token)
        [await manager.api.message_injection(s.ip_addr, injection) for s in servers]
        [await manager.api.disable_injection(s.ip_addr, injection) for s in servers]

    await asyncio.gather(repair_task(), migration_task())

async def check_tablet_rebuild_with_repair(manager: ManagerClient, fail: bool):
    logger.info("Bootstrapping cluster")
    cfg = {'enable_user_defined_functions': False, 'enable_tablets': True}
    if fail:
        cfg['error_injections_at_startup'] = ['rebuild_repair_stage_fail']
    host_ids = []
    servers = []

    async def make_server(rack: str):
        s = await manager.server_add(config=cfg, property_file={"dc": "dc1", "rack": rack})
        servers.append(s)
        host_ids.append(await manager.get_host_id(s.server_id))
        await manager.api.disable_tablet_balancing(s.ip_addr)

    await make_server("r1")
    await make_server("r1")
    await make_server("r2")

    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2} AND tablets = {'initial': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")
        keys = range(256)
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({k}, {k});") for k in keys])

        replicas = await get_all_tablet_replicas(manager, servers[0], ks, 'test')
        logger.info(f"Tablet is on [{replicas}]")
        assert len(replicas) == 1 and len(replicas[0].replicas) == 2
        replicas = [ r[0] for r in replicas[0].replicas ]
        for h in host_ids:
            if h not in replicas:
                new_replica = (h, 0)
                break
        else:
            assert False, "Cannot find node without replica"

        logs = []
        for s in servers:
            logs.append(await manager.server_open_log(s.server_id))

        logger.info(f"Adding replica to tablet, host {new_replica[0]}")
        await manager.api.add_tablet_replica(servers[0].ip_addr, ks, "test", new_replica[0], new_replica[1], 0)

        assert sum([len(await log.grep(rf'.*Will set tablet .* stage to rebuild_repair.*')) for log in logs]) == 1

        replicas = await get_all_tablet_replicas(manager, servers[0], ks, 'test')
        logger.info(f"Tablet is now on [{replicas}]")
        assert len(replicas) == 1
        replicas = [ r[0] for r in replicas[0].replicas ]

        assert len(replicas) == 2 if fail else len(replicas) == 3

        for h, s in zip(host_ids, servers):
            host = await wait_for_cql_and_get_hosts(cql, [s], time.time() + 30)
            if h != host_ids[0]:
                await read_barrier(manager.api, host[0].address)  # host-0 did the barrier in get_all_tablet_replicas above
            res = await cql.run_async(f"SELECT COUNT(*) FROM MUTATION_FRAGMENTS({ks}.test)", host=host[0])
            logger.info(f"Host {h} reports {res} as mutation fragments count")
            if h in replicas:
                assert res[0].count != 0
            else:
                assert res[0].count == 0

@pytest.mark.asyncio
async def test_tablet_rebuild(manager: ManagerClient):
    await check_tablet_rebuild_with_repair(manager, False)

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_tablet_rebuild_failure(manager: ManagerClient):
    await check_tablet_rebuild_with_repair(manager, True)
