#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from cassandra.protocol import ConfigurationException, InvalidRequest
from cassandra.query import SimpleStatement, ConsistencyLevel
from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import HTTPError, read_barrier
from test.pylib.tablets import get_tablet_replica, get_all_tablet_replicas
from test.topology.conftest import skip_mode
from test.topology.util import wait_for_cql_and_get_hosts, get_topology_coordinator
from contextlib import nullcontext as does_not_raise
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
    cfg = {'enable_user_defined_functions': False, 'enable_tablets': True}
    servers = await manager.servers_add(2, config=cfg)

    cql = manager.get_cql()
    res = await cql.run_async("SELECT data_center FROM system.local")
    this_dc = res[0].data_center

    await cql.run_async(f"CREATE KEYSPACE test WITH replication = {{'class': 'NetworkTopologyStrategy', '{this_dc}': 3}}")
    with pytest.raises(ConfigurationException, match=f"Datacenter {this_dc} doesn't have enough token-owning nodes"):
        await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int);")

    await cql.run_async(f"ALTER KEYSPACE test WITH replication = {{'class': 'NetworkTopologyStrategy', '{this_dc}': 2}}")
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int);")


@pytest.mark.asyncio
async def test_tablet_cannot_decommision_below_replication_factor(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    cfg = {'enable_user_defined_functions': False, 'enable_tablets': True}
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
    cfg = {'enable_user_defined_functions': False, 'enable_tablets': True}
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
    cfg = {'enable_user_defined_functions': False, 'enable_tablets': True}
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
    await cql.run_async("CREATE MATERIALIZED VIEW test.test_mv AS SELECT pk FROM test.test WHERE pk IS NOT NULL PRIMARY KEY (pk)")

    logger.info("Populating table")
    await asyncio.gather(*[cql.run_async(f"INSERT INTO test.test (pk, c) VALUES ({k}, {k});") for k in range(128)])

    async def check_allocated_replica(expected: int):
        replicas = await get_all_tablet_replicas(manager, servers[0], 'test', 'test')
        replicas = replicas + await get_all_tablet_replicas(manager, servers[0], 'test', 'test_mv', is_view=True)
        for r in replicas:
            logger.info(f"{r.replicas}")
            assert len(r.replicas) == expected

    logger.info(f"Checking {rf_from} allocated replicas")
    await check_allocated_replica(rf_from)

    logger.info(f"Altering RF {rf_from} -> {rf_to}")
    await cql.run_async(f"ALTER KEYSPACE test WITH replication = {{'class': 'NetworkTopologyStrategy', '{this_dc}': {rf_to}}}")

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
            res = await cql.run_async(f"SELECT partition_region FROM MUTATION_FRAGMENTS(test.test) WHERE pk={k}", host=host[0])
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
           'enable_tablets': True }
    servers = await manager.servers_add(3, config=cfg)

    cql = manager.get_cql()

    await cql.run_async(f"CREATE KEYSPACE test WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 2}}")
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int);")

    logger.info("Populating table")
    await asyncio.gather(*[cql.run_async(f"INSERT INTO test.test (pk, c) VALUES ({k}, {k});") for k in range(4)])

    for s in servers:
        host_id = await manager.get_host_id(s.server_id)
        host = await wait_for_cql_and_get_hosts(cql, [s], time.time() + 30)
        for k in range(4):
            await cql.run_async(f"SELECT partition_region FROM MUTATION_FRAGMENTS(test.test) WHERE pk={k}", host=host[0])


# ALTER tablets KS cannot change RF of any DC by more than 1 at a time.
# In a multi-dc environment, we can create replicas in a DC that didn't have replicas before,
# but the above requirement should still be honoured, because we'd be changing RF from 0 to N in the new DC.
# Reproduces https://github.com/scylladb/scylladb/issues/20039#issuecomment-2271365060
# See also cql-pytest/test_tablets.py::test_alter_tablet_keyspace_rf for basic scenarios tested
@pytest.mark.asyncio
async def test_multidc_alter_tablets_rf(request: pytest.FixtureRequest, manager: ManagerClient) -> None:
    config = {"endpoint_snitch": "GossipingPropertyFileSnitch", "enable_tablets": "true"}

    logger.info("Creating a new cluster of 2 nodes in 1st DC and 2 nodes in 2nd DC")
    # we have to have at least 2 nodes in each DC if we want to try setting RF to 2 in each DC
    await manager.servers_add(2, config=config, property_file={'dc': f'dc1', 'rack': 'myrack'})
    await manager.servers_add(2, config=config, property_file={'dc': f'dc2', 'rack': 'myrack'})

    cql = manager.get_cql()
    await cql.run_async("create keyspace if not exists ks with replication = {'class': 'NetworkTopologyStrategy', 'dc1': 1}")
    # need to create a table to not change only the schema, but also tablets replicas
    await cql.run_async("create table ks.t (pk int primary key)")
    with pytest.raises(InvalidRequest, match="Only one DC's RF can be changed at a time and not by more than 1"):
        # changing RF of dc2 from 0 to 2 should fail
        await cql.run_async("alter keyspace ks with replication = {'class': 'NetworkTopologyStrategy', 'dc2': 2}")

    # changing RF of dc2 from 0 to 1 should succeed
    await cql.run_async("alter keyspace ks with replication = {'class': 'NetworkTopologyStrategy', 'dc2': 1}")
    # ensure that RFs of both DCs are equal to 1 now, i.e. that omitting dc1 in above command didn't change it
    res = await cql.run_async("SELECT * FROM system_schema.keyspaces  WHERE keyspace_name = 'ks'")
    assert res[0].replication['dc1'] == '1'
    assert res[0].replication['dc2'] == '1'

    # incrementing RF of 2 DCs at once should NOT succeed, because it'd leave 2 pending tablets replicas
    with pytest.raises(InvalidRequest, match="Only one DC's RF can be changed at a time and not by more than 1"):
        await cql.run_async("alter keyspace ks with replication = {'class': 'NetworkTopologyStrategy', 'dc1': 2, 'dc2': 2}")
    # as above, but decrementing
    with pytest.raises(InvalidRequest, match="Only one DC's RF can be changed at a time and not by more than 1"):
        await cql.run_async("alter keyspace ks with replication = {'class': 'NetworkTopologyStrategy', 'dc1': 0, 'dc2': 0}")
    # as above, but decrement 1 RF and increment the other
    with pytest.raises(InvalidRequest, match="Only one DC's RF can be changed at a time and not by more than 1"):
        await cql.run_async("alter keyspace ks with replication = {'class': 'NetworkTopologyStrategy', 'dc1': 2, 'dc2': 0}")
    # as above, but RFs are swapped
    with pytest.raises(InvalidRequest, match="Only one DC's RF can be changed at a time and not by more than 1"):
        await cql.run_async("alter keyspace ks with replication = {'class': 'NetworkTopologyStrategy', 'dc1': 0, 'dc2': 2}")

    # check that we can remove all replicas from dc2 by changing RF from 1 to 0
    await cql.run_async("alter keyspace ks with replication = {'class': 'NetworkTopologyStrategy', 'dc2': 0}")
    # check that we can remove all replicas from the cluster, i.e. change RF of dc1 from 1 to 0 as well:
    await cql.run_async("alter keyspace ks with replication = {'class': 'NetworkTopologyStrategy', 'dc1': 0}")


# Reproducer for https://github.com/scylladb/scylladb/issues/18110
# Check that an existing cached read, will be cleaned up when the tablet it reads
# from is migrated away.
@pytest.mark.asyncio
async def test_saved_readers_tablet_migration(manager: ManagerClient, mode):
    cfg = {'enable_user_defined_functions': False, 'enable_tablets': True}

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
    cfg = {'enable_user_defined_functions': False, 'enable_tablets': True}
    cmdline = [
        '--logger-log-level', 'storage_service=debug',
        '--logger-log-level', 'raft_topology=debug',
        '--enable-cache', with_cache,
    ]
    servers = [await manager.server_add(cmdline=cmdline, config=cfg)]

    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1};")
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int);")
    await cql.run_async("CREATE MATERIALIZED VIEW test.mv1 AS \
        SELECT * FROM test.test WHERE pk IS NOT NULL AND c IS NOT NULL \
        PRIMARY KEY (c, pk);")

    servers.append(await manager.server_add(cmdline=cmdline, config=cfg))

    key = 7 # Whatever
    tablet_token = 0 # Doesn't matter since there is one tablet
    await cql.run_async(f"INSERT INTO test.test (pk, c) VALUES ({key}, 0)")
    rows = await cql.run_async("SELECT pk from test.test")
    assert len(list(rows)) == 1

    replica = await get_tablet_replica(manager, servers[0], 'test', 'test', tablet_token)

    s0_host_id = await manager.get_host_id(servers[0].server_id)
    s1_host_id = await manager.get_host_id(servers[1].server_id)
    dst_shard = 0

    await manager.api.enable_injection(servers[1].ip_addr, "stream_mutation_fragments", one_shot=True)
    s1_log = await manager.server_open_log(servers[1].server_id)
    s1_mark = await s1_log.mark()

    # Drop cache to remove dummy entry indicating that underlying mutation source is empty
    await manager.api.drop_sstable_caches(servers[1].ip_addr)

    migration_task = asyncio.create_task(
        manager.api.move_tablet(servers[0].ip_addr, "test", "test", replica[0], replica[1], s1_host_id, dst_shard, tablet_token))

    await s1_log.wait_for('stream_mutation_fragments: waiting', from_mark=s1_mark)
    s1_mark = await s1_log.mark()

    await cql.run_async(f"INSERT INTO test.test (pk, c) VALUES ({key}, 1)")
    rows = await cql.run_async("SELECT pk from test.test")
    assert len(list(rows)) == 1

    # Release abandoned streaming
    await manager.api.message_injection(servers[1].ip_addr, "stream_mutation_fragments")
    await s1_log.wait_for('stream_mutation_fragments: done', from_mark=s1_mark)

    logger.info("Waiting for migration to finish")
    await migration_task
    logger.info("Migration done")

    rows = await cql.run_async("SELECT pk from test.test")
    assert len(list(rows)) == 1


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_drop_keyspace_while_split(manager: ManagerClient):

    # Reproducer for: https://github.com/scylladb/scylladb/issues/22431
    # This tests if the split ready compaction groups are correctly created
    # on a shard with several storage groups for the same table

    logger.info("Bootstrapping cluster")
    cmdline = [ '--target-tablet-size-in-bytes', '8192',
                '--smp', '2' ]
    config = { 'error_injections_at_startup': ['short_tablet_stats_refresh_interval'],
               'enable_tablets': True }
    servers = [await manager.server_add(config=config, cmdline=cmdline)]

    s0_log = await manager.server_open_log(servers[0].server_id)

    cql = manager.get_cql()
    await wait_for_cql_and_get_hosts(cql, [servers[0]], time.time() + 60)

    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    # create a table so that it has at least 2 tablets (and storage groups) per shard
    await cql.run_async("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 4};")
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int);")

    await manager.api.disable_autocompaction(servers[0].ip_addr, 'test')

    keys = range(2048)
    await asyncio.gather(*[cql.run_async(f'INSERT INTO test.test (pk, c) VALUES ({k}, {k});') for k in keys])
    await manager.api.flush_keyspace(servers[0].ip_addr, 'test')

    await manager.api.enable_injection(servers[0].ip_addr, 'truncate_compaction_disabled_wait', one_shot=False)
    await manager.api.enable_injection(servers[0].ip_addr, 'split_storage_groups_wait', one_shot=False)

    # enable the load balancer which should emmit a tablet split
    await manager.api.enable_tablet_balancing(servers[0].ip_addr)

    # wait for compaction groups to be created and split to begin
    await s0_log.wait_for('split_storage_groups_wait: wait')

    # start a DROP and wait for it to disable compaction
    drop_ks_task = cql.run_async('DROP KEYSPACE test;')
    await s0_log.wait_for('truncate_compaction_disabled_wait: wait')

    # release split
    await manager.api.message_injection(servers[0].ip_addr, "split_storage_groups_wait")

    # release drop and wait for it to complete
    await manager.api.message_injection(servers[0].ip_addr, "truncate_compaction_disabled_wait")
    await drop_ks_task


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
    cfg = {'enable_tablets': with_tablets}
    server = await manager.server_add(config=cfg)
    cql = manager.get_cql()

    # Tablets are only possible when enabled and the replication strategy is NetworkTopology one
    tablets_possible = (replication_strategy == 'NetworkTopologyStrategy') and with_tablets

    # First, check if a kesypace is able to be created with default CQL statement that
    # doesn't contain tablets parameters. When possible, tablets should be activated
    await cql.run_async(f"CREATE KEYSPACE test_d WITH replication = {{'class': '{replication_strategy}', 'replication_factor': 1}};")
    res = cql.execute(f"SELECT initial_tablets FROM system_schema.scylla_keyspaces WHERE keyspace_name = 'test_d'").one()
    if tablets_possible:
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
        await cql.run_async(f"CREATE KEYSPACE test_y WITH replication = {{'class': '{replication_strategy}', 'replication_factor': 1}} AND TABLETS = {{'enabled': true}};")
        res = cql.execute(f"SELECT initial_tablets FROM system_schema.scylla_keyspaces WHERE keyspace_name = 'test_y'").one()
        assert res.initial_tablets == 0

    # Finally, check that explicitly disabling tablets in CQL results in vnode-based keyspace
    # whenever tablets are enabled or not in config
    await cql.run_async(f"CREATE KEYSPACE test_n WITH replication = {{'class': '{replication_strategy}', 'replication_factor': 1}} AND TABLETS = {{'enabled': false}};")
    res = cql.execute(f"SELECT initial_tablets FROM system_schema.scylla_keyspaces WHERE keyspace_name = 'test_n'").one()
    assert res is None
