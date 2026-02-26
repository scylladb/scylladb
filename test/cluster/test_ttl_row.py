# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

# Multi-node tests for the CQL per-row TTL feature. Note that this is a
# distinct feature from CQL's traditional per-write (per-cell) TTL.
#
# Please note that most functional tests for this feature are single-node
# and can be found in test/cqlpy/test_ttl_row.py. Most functional tests
# of the many different syntax features and capabilities of this feature
# don't need more than a single node to be tested and are easier to write,
# and faster to run, in the cqlpy framework. So only the minority of tests
# that do need a bigger cluster should be here.

import pytest
import asyncio
import time

from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from cassandra.protocol import InvalidRequest

from test.pylib.manager_client import ManagerClient
from test.cluster.util import new_test_keyspace, new_test_table

async def get_cpu_metrics(manager: ManagerClient):
    """Utility function for getting the current amount of work (in CPU ms)
       done across all nodes and shards in different scheduling groups.
       We expect this to increase considerably for the streaming group while
       expiration scanning is proceeding, but not increase at all for the
       statement group because there are no requests being executed.
       This function also returns the total number of items deleted by
       TTL expiration - which will allow us to figure out when expiration
       is done without a separate SELECT call.
    """
    ms_streaming = 0
    ms_statement = 0
    items_deleted = 0
    ips = [server.ip_addr for server in await manager.running_servers()]
    for ip in ips:
        metrics = await manager.metrics.query(ip)
        ms_streaming += metrics.get('scylla_scheduler_runtime_ms', {'group': 'streaming'})
        # Today, the default statement scheduling group is called "sl:default",
        # not "statement".
        ms_statement += metrics.get('scylla_scheduler_runtime_ms', {'group': 'sl:default'})
        items_deleted += (metrics.get('scylla_expiration_items_deleted') or 0)
    return (ms_streaming, ms_statement, items_deleted)


async def test_row_ttl_scheduling_group(manager: ManagerClient):
    """Verify that the expiration scans and deletion operations done by the
       per-row TTL feature are done entirely in the "streaming" scheduling
       group, not in the user's statement scheduling group.
       This is the CQL version of the orignal test for Alternator TTL,
       test_alternator.py::test_alternator_ttl_scheduling_group, which
       reproduced issue #18719. That issue was about a bug in in inheritance
       of scheduling groups through RPC causing some of the work to end
       up being done on the statement scheduling group.
       This test assumes that the cluster is not concurrently busy with
       running any other workload - so we won't see any work appearing
       in the wrong scheduling group. We can assume this because we don't
       run multiple tests in parallel on the same cluster.
    """
    config = {
        'alternator_ttl_period_in_seconds': '0.5',
    }
    servers = await manager.servers_add(3, config=config, auto_rack_dc='dc1')
    cql = manager.get_cql()
    ksdef = "WITH REPLICATION = { 'class': 'NetworkTopologyStrategy', 'replication_factor': 3 }"
    async with new_test_keyspace(manager, ksdef) as keyspace:
        # Create a test table with attribute "e" that will later function
        # as a per-row TTL - but not yet enabled:
        async with new_test_table(manager, keyspace, 'p int primary key, e bigint') as table:
            # Insert N rows, setting them with past expiration times so they
            # will expire as soon as TTL is enabled and the data is scanned.
            # Do the writes with CL=ALL to ensure that when this loop is done,
            # we don't expect further backlogged work to appear in the
            # statement scheduling group.
            e = int(time.time()) - 10
            N = 200
            for p in range(N):
                await cql.run_async(SimpleStatement(f'INSERT INTO {table} (p, e) VALUES ({p}, {e})', consistency_level=ConsistencyLevel.ALL))

            ms_streaming_before, ms_statement_before, items_deleted_before = await get_cpu_metrics(manager)
            # Only now, after getting the metrics, enable TTL, so the
            # expiration scanner is guaranteed to run after this point.
            await cql.run_async(f'ALTER TABLE {table} TTL e')
            # Wait until all rows expire (this should happen very quicky),
            # and get the CPU metrics again.
            timeout = time.time() + 60
            while time.time() < timeout:
                time.sleep(0.5)
                ms_streaming_after, ms_statement_after, items_deleted = await get_cpu_metrics(manager)
                if items_deleted - items_deleted_before == N:
                    break
            # Sanity check (that the items_deleted check actually worked) -
            # the expiration did delete all the items.
            assert 0 == len(list(await cql.run_async(SimpleStatement(f'SELECT p FROM {table} LIMIT 1', consistency_level=ConsistencyLevel.QUORUM))))

    # Between the calls to get_cpu_metrics() above, at least one expiration
    # scan took place and also all the items got deleted. We expect all
    # that work to have happened in the streaming group, not statement group,
    # so "ratio" calculate below should be very small. It is not zero because
    # we did some work (ALTER TABLE and REST API) in the statement group.
    # But issue #18719 was fixed, it was fairly large - 0.58.
    # Let's assert it is <0.2 instead of zero.
    ms_streaming = ms_streaming_after - ms_streaming_before
    assert ms_streaming > 0 # sanity check
    ms_statement = ms_statement_after - ms_statement_before
    ratio = ms_statement / ms_streaming
    assert ratio < 0.2, f'statement {ms_statement} streaming {ms_streaming} ratio {ratio}'

@pytest.mark.parametrize("with_down_node", [False, True], ids=["all_nodes_up", "one_node_down"])
async def test_row_ttl_multinode_expiration(manager: ManagerClient, with_down_node):
    """When the cluster has multiple nodes, different nodes are responsible
       for checking expiration in different token ranges (each responsible
       for its "primary ranges"). Let's check that this expiration really
       does happen - for the entire token range - by writing many partitions
       that will span the entire token range, and seeing that they all expire.
       We also don't check that nodes don't do more work than they should -
       an inefficient implementation where every node scans the entire data
       set will also pass this test.
       When the test is run a second time with with_down_node=True, we verify
       that TTL expiration works correctly even when one of the nodes is
       brought down. This node's TTL scanner is responsible for scanning part
       of the token range, so when this node is down, part of the data might
       not get expired. At that point - other node(s) should take over
       expiring data in that range - and this test verifies that this indeed
       happens.
       This is a CQL version of the Alternator dtest reproducing issue #9787:
       alternator_ttl_tests.py::test_multinode_expiration
    """
    config = {
        'alternator_ttl_period_in_seconds': '0.5',
    }
    servers = await manager.servers_add(3, config=config, auto_rack_dc='dc1')

    if with_down_node:
        # Bring down one of nodes. Everything we do below, like creating a
        # table, or reading and writing with CL=QUORUM, should continue to
        # work with one node down.
        await manager.server_stop_gracefully(servers[2].server_id) 

    cql = manager.get_cql()

    ksdef = "WITH REPLICATION = { 'class': 'NetworkTopologyStrategy', 'replication_factor': 3 }"
    async with new_test_keyspace(manager, ksdef) as keyspace:
        # Create a test table per-row TTL on attribute "e"
        async with new_test_table(manager, keyspace, 'p int primary key, e bigint ttl') as table:
            # Insert 50 rows, in different partitions, so the murmur3 hash
            # maps them all over the token space so different nodes would
            # be responsible for expiring them. All items are marked to
            # expire 10 seconds in the past, so should all expire as soon
            # as possible, during this test.
            e = int(time.time()) - 10
            for p in range(50):
                await cql.run_async(SimpleStatement(f'INSERT INTO {table} (p, e) VALUES ({p}, {e})', consistency_level=ConsistencyLevel.QUORUM))
            # Expect that after a short delay, all items in the table will
            # have expired - so a scan should return no responses. This
            # should happen even though one of the nodes is down and not
            # doing its usual expiration-scanning work.
            timeout = time.time() + 120
            while time.time() < timeout:
                if 0 == len(list(await cql.run_async(SimpleStatement(f'SELECT p FROM {table}', consistency_level=ConsistencyLevel.QUORUM)))):
                    break
                time.sleep(0.1)
            assert 0 == len(list(await cql.run_async(SimpleStatement(f'SELECT p FROM {table}', consistency_level=ConsistencyLevel.QUORUM))))

@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_row_ttl_upgrade(manager: ManagerClient):
    """This test verifies that a rolling upgrade works as designed for the
       the CQL per-row TTL feature:
       1. When some of the nodes are not yet upgraded and do not yet support
          the per-row TTL feature, it's forbidden on any node (even an
          upgraded one) to use CREATE TABLE or ALTER TABLE to enable the
          row TTL feature on a table.
       2. When the last node is finally upgraded to support per-row TTL
          feature, soon after CREATE TABLE / ALTER TABLE with row TTL
          begin to work.
       3. Moreover, at this point the entire TTL feature fully works, with
          all nodes (even those that were upgraded earlier) correctly
          running the expiration-scanning thread.
       To avoid needing multiple versions of Scylla, we simulate the
       partially-upgraded cluster using a suppress_features injection.
    """
    config_old = { # A "pre-upgrade" node
        'error_injections_at_startup': [
            {'name': 'suppress_features', 'value': 'CQL_ROW_TTL'}],
        'alternator_ttl_period_in_seconds': '0.5',
    }
    config_new = { # A "post-upgrade" node
        'alternator_ttl_period_in_seconds': '0.5',
    }
    servers = []
    # Add three nodes - one "old" node (not supporting CQL_ROW_TTL), and
    # two "new" nodes (supporting CQL_ROW_TTL).
    servers.extend(await manager.servers_add(1, config=config_old, property_file={'dc': 'dc1', 'rack': 'rack1'}))
    servers.extend(await manager.servers_add(1, config=config_new, property_file={'dc': 'dc1', 'rack': 'rack2'}))
    servers.extend(await manager.servers_add(1, config=config_new, property_file={'dc': 'dc1', 'rack': 'rack3'}))
    # Send our requests to servers[2]. This node supports the CQL_ROW_TTL
    # feature, but we expect it to realize that not the entire cluster does.
    cql = await manager.get_cql_exclusive(servers[2])
    await cql.run_async("CREATE KEYSPACE ks WITH REPLICATION = { 'class': 'NetworkTopologyStrategy', 'replication_factor': 3 }")
    # If we now try create a table with a TTL column, it should fail because
    # one of the nodes (servers[0]) doesn't support the CQL_ROW_TTL feature.
    with pytest.raises(InvalidRequest, match='not yet supported by this cluster'):
        await cql.run_async("CREATE TABLE ks.tbl1 (p int primary key, e bigint ttl)")
    # For the same reason, ALTER TABLE to add a TTL should fail:
    await cql.run_async("CREATE TABLE ks.tbl1 (p int primary key, e bigint)")
    with pytest.raises(InvalidRequest, match='not yet supported by this cluster'):
        await cql.run_async("ALTER TABLE ks.tbl1 TTL e")

    # Let's reboot servers[0], which did not support CQL_ROW_TTL, removing
    # the suppress_features error injection from its configuration so that
    # it finally does support CQL_ROW_TTL.
    await manager.server_stop_gracefully(servers[0].server_id)
    await manager.server_remove_config_option(servers[0].server_id, 'error_injections_at_startup')
    await manager.server_start(servers[0].server_id)

    # Quickly (but perhaps not immediately so we need to retry) servers[2]
    # (to which 'cql' is connected) will realize that the entire cluster now
    # supports the CQL_ROW_TTL feature, and will begin to allow CREATE TABLE
    # with a TTL:
    timeout = time.time() + 120
    success = False
    while not success and time.time() < timeout:
        try:
            await cql.run_async("CREATE TABLE ks.tbl2 (p int primary key, e bigint ttl)")
            success = True
            break
        except InvalidRequest as e:
            # Maybe servers[2] didn't notice yet that servers[0] has been
            # upgraded. Try again.
            assert 'not yet supported by this cluster' in str(e)
            time.sleep(0.1)
    assert success

    # At this point, the TTL feature should work fully - if we write expired
    # items they will quickly all disappear. servers[0] and servers[1] did
    # NOT need to be rebooted to start participating in the expired-item
    # scanning.
    e = int(time.time()) - 10
    for p in range(20):
        await cql.run_async(SimpleStatement(f'INSERT INTO ks.tbl2 (p, e) VALUES ({p}, {e})', consistency_level=ConsistencyLevel.QUORUM))
    timeout = time.time() + 120
    while time.time() < timeout:
        if 0 == len(list(await cql.run_async(SimpleStatement(f'SELECT p FROM ks.tbl2', consistency_level=ConsistencyLevel.QUORUM)))):
            break
        time.sleep(0.1)
    assert 0 == len(list(await cql.run_async(SimpleStatement(f'SELECT p FROM ks.tbl2', consistency_level=ConsistencyLevel.QUORUM))))


async def test_row_ttl_multi_dc(manager: ManagerClient):
    """Check that the TTL feature works correctly on a setup with multiple
       data centers. Rows added in one DC will, of course, get copied to all
       DCs, and when they expire, should expire from all DCs.
       Note that this test doesn't check the efficiency of the multi-DC
       expiration - i.e., whether each DC scans the same data or each DC
       does just a part of the scan and sends deletions accross DCs.
       We just check here the correctness of the expiration under the
       assumption that data is correctly replicated to all data centers.
    """
    config = {
        'alternator_ttl_period_in_seconds': '0.5',
    }
    # Set up a cluster with 6 nodes - three nodes in three racks in each
    # of two data centers.
    futures = []
    for dc in range(2):
        for rack in range(3):
            futures.append(manager.servers_add(1, config=config, property_file={'dc': f'dc{dc+1}', 'rack': f'rack{rack+1}'}))
    servers = [x[0] for x in await asyncio.gather(*futures)]
    cql = manager.get_cql()
    # Setting replication_factor:3 means RF=3 for each of the two DCs.
    # (this is the so-called "auto-expansion" feature).
    ksdef = "WITH REPLICATION = { 'class': 'NetworkTopologyStrategy', 'replication_factor': 3 }"
    async with new_test_keyspace(manager, ksdef) as keyspace:
        # Create a test table, don't enable TTL yet to give us time to check
        # the initial write to the table succeeded.
        async with new_test_table(manager, keyspace, 'p int primary key, e bigint') as table:
            # Insert 50 rows, all given an expiration time 10 seconds in the
            # past (but they will not expire yet, since TTL was not yet
            # enabled on the column "e").
            e = int(time.time()) - 10
            for p in range(50):
                # Do the writes with CL=ALL to ensure that when this loop is
                # done, the data appeared on all nodes on all data centers.
                await cql.run_async(SimpleStatement(f'INSERT INTO {table} (p, e) VALUES ({p}, {e})', consistency_level=ConsistencyLevel.ALL))

            # As a sanity check, before enabling TTL, verify that all rows
            # actually exist, in each DC. We need to make separate connections
            # to two nodes in the two dcs:
            cql_dc = [await manager.get_cql_exclusive(servers[0]),
                      await manager.get_cql_exclusive(servers[3])]
            for dc in range(2):
                assert 50 == len(list(await cql_dc[dc].run_async(SimpleStatement(f'SELECT p FROM {table}', consistency_level=ConsistencyLevel.LOCAL_QUORUM))))
            # Enable TTL on column "e". Very soon (usually after 0.5
            # seconds, which is alternator_ttl_period_in_seconds) we should
            # expect all the data to be gone from both DCs
            await cql.run_async(f'ALTER TABLE {table} TTL e')
            timeout = time.time() + 120
            success = False
            while not success and time.time() < timeout:
                success = True
                for dc in range(2):
                    if 0 != len(list(await cql_dc[dc].run_async(SimpleStatement(f'SELECT p FROM {table}', consistency_level=ConsistencyLevel.LOCAL_QUORUM)))):
                        success = False
                        break
                if not success:
                    time.sleep(0.1)
            for dc in range(2):
                assert 0 == len(list(await cql_dc[dc].run_async(SimpleStatement(f'SELECT p FROM {table}', consistency_level=ConsistencyLevel.LOCAL_QUORUM))))
