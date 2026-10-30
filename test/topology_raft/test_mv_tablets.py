#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

# Tests for interaction of materialized views with *tablets*

from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import read_barrier
from test.pylib.util import wait_for_cql_and_get_hosts
from test.pylib.internal_types import ServerInfo
from test.topology.conftest import skip_mode

from .test_alternator import get_alternator, alternator_config, full_query

import pytest
import asyncio
import logging
import time


logger = logging.getLogger(__name__)

# This convenience function takes the name of a table or a view, and a token,
# and returns the list of host_id,shard pairs holding tablets for this token
# and view.
# You also need to specify a specific server to use for the requests, to
# ensure that if you send tablet-migration commands to one server, you also
# read the replicas information from the same server (it takes time for this
# information to propagate to all servers).
async def get_tablet_replicas(manager: ManagerClient, server: ServerInfo, keyspace_name: str, table_or_view_name: str, token: int):
    host = (await wait_for_cql_and_get_hosts(manager.cql, [server], time.time() + 60))[0]
    await read_barrier(manager.api, server.ip_addr)

    rows = await manager.cql.run_async(f"SELECT last_token, replicas FROM system.tablets where "
                                       f"keyspace_name = '{keyspace_name}' and "
                                       f"table_name = '{table_or_view_name}'"
                                       " ALLOW FILTERING", host=host)
    for row in rows:
        if row.last_token >= token:
            return row.replicas

# This convenience function assumes a table has RF=1 and only a single tablet,
# and moves it to one specific node "server" - and pins it there (disabling
# further tablet load-balancing). It is not specified which *shard* on that
# node will receive the tablet.
async def pin_the_only_tablet(manager, keyspace_name, table_or_view_name, server):
    # We need to send load-balancing commands to one of the nodes and they
    # will be propagated to all of them. Since we already know of
    # target_server, let's just use that.
    await manager.api.disable_tablet_balancing(server.ip_addr)
    tablet_token = 0 # Doesn't matter since there is one tablet
    source_replicas = await get_tablet_replicas(manager, server, keyspace_name, table_or_view_name, tablet_token)
    # We assume RF=1 so get_tablet_replicas() returns just one replica
    assert len(source_replicas) == 1
    source_host_id, source_shard = source_replicas[0]

    target_host_id = await manager.get_host_id(server.server_id)
    target_shard = 0 # We don't care which shard to use

    # Currently migrating a tablet in the same node is not allowed.
    # We need to just do nothing in this case - the tablet is already in
    # its desired node (and we didn't specify which shard is desired).
    # The str() is needed because we can't compare HostId to string :-(
    if str(target_host_id) == str(source_host_id):
        return

    # Finally move the tablet. We can send the command to any of the hosts,
    # it will propagate it to all of them.
    await manager.api.move_tablet(server.ip_addr, keyspace_name, table_or_view_name, source_host_id, source_shard, target_host_id, target_shard, tablet_token)

# Assert that the given table uses tablets, and has only one. It helps
# verify that a test that attempted to enable tablets - and set up only
# one tablet for the entire table - actually succeeded in doing that.
async def assert_one_tablet(cql, keyspace_name, table_or_view_name):
    rows = await cql.run_async(f"SELECT last_token, replicas FROM system.tablets where keyspace_name = '{keyspace_name}' and table_name = '{table_or_view_name}' ALLOW FILTERING")
    assert len(rows) == 1


@pytest.mark.asyncio
async def test_tablet_mv_create(manager: ManagerClient):
    """A basic test for creating a materialized view on a table stored
       with tablets on a one-node cluster. We just create the view and
       delete it - that's it, we don't read or write the table.
       Reproduces issue #16194.
    """
    servers = await manager.servers_add(1)
    cql = manager.get_cql()

    await cql.run_async("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 100}")
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int)")
    await cql.run_async("CREATE MATERIALIZED VIEW test.tv AS SELECT * FROM test.test WHERE c IS NOT NULL AND pk IS NOT NULL PRIMARY KEY (c, pk)")
    await cql.run_async("DROP KEYSPACE test")


@pytest.mark.asyncio
async def test_tablet_mv_simple(manager: ManagerClient):
    """A simple test for reading and writing a materialized view on a table
       stored with tablets on a one-node cluster. Because it's a one-node
       cluster, we don't don't need any sophisticated mappings or pairings
       to work correctly for this test to pass - everything is on this single
       node anyway.
       Reproduces issue #16209.
    """
    servers = await manager.servers_add(1)
    cql = manager.get_cql()

    await cql.run_async("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 100}")
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int)")
    await cql.run_async("CREATE MATERIALIZED VIEW test.tv AS SELECT * FROM test.test WHERE c IS NOT NULL AND pk IS NOT NULL PRIMARY KEY (c, pk) WITH SYNCHRONOUS_UPDATES = TRUE")
    await cql.run_async("INSERT INTO test.test (pk, c) VALUES (2, 3)")
    # We used SYNCHRONOUS_UPDATES=TRUE, so the view should be updated:
    assert [(3,2)] == list(await cql.run_async("SELECT * FROM test.tv WHERE c=3"))
    await cql.run_async("DROP KEYSPACE test")

@pytest.mark.asyncio
async def test_tablet_mv_simple_6node(manager: ManagerClient):
    """A simple reproducer for a bug of forgetting that the view table has a
       different tablet mapping from the base: Using the wrong tablet mapping
       for the base table or view table can cause us to send a view update
       to the wrong view replica - or not send a view update at all. A row
       that we write on the base table will not be readable in the view.
       We start a large-enough cluster (6 nodes) to increase the probability
       that if the mapping is different for the one row we write, and the test
       will fail if the bug exists.
       Reproduces #16227.
    """
    servers = await manager.servers_add(6)
    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 100}")
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int)")
    await cql.run_async("CREATE MATERIALIZED VIEW test.tv AS SELECT * FROM test.test WHERE c IS NOT NULL AND pk IS NOT NULL PRIMARY KEY (c, pk) WITH SYNCHRONOUS_UPDATES = TRUE")
    await cql.run_async("INSERT INTO test.test (pk, c) VALUES (2, 3)")
    # We used SYNCHRONOUS_UPDATES=TRUE, so the view should be updated:
    assert [(3,2)] == list(await cql.run_async("SELECT * FROM test.tv WHERE c=3"))
    await cql.run_async("DROP KEYSPACE test")

async def inject_error_on(manager, error_name, servers):
    errs = [manager.api.enable_injection(s.ip_addr, error_name, False) for s in servers]
    await asyncio.gather(*errs)

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_tablet_alternator_lsi_consistency(manager: ManagerClient):
    """A reproducer for a bug where Alternator LSI was not using synchronous
       view updates when tablets are enabled, which could cause strongly-
       consistent read of the LSI to miss the data just written to the base.

       We use a cluster of just two nodes and RF=1, and control the tablets
       so all base tablets will be in node 0 and all view tablets will be
       in node 1, to ensure that the view update is remote and therefore
       not synchronous by default. To make the test failure even more
       likely on a fast machine, we use the "delay_before_remote_view_update"
       injection point to add a delay to the view update more than usual.
       Reproduces #16313.
    """
    servers = await manager.servers_add(2, config=alternator_config)
    cql = manager.get_cql()
    alternator = get_alternator(servers[0].ip_addr)
    # Tell Alternator to create a table with just *one* tablet, via a
    # special tag.
    tablets_tags = [{'Key': 'experimental:initial_tablets', 'Value': '1'}]
    # Create a table with an LSI
    table_name = 'tbl'
    index_name = 'ind'
    table = alternator.create_table(TableName=table_name,
        BillingMode='PAY_PER_REQUEST',
        KeySchema=[
            {'AttributeName': 'p', 'KeyType': 'HASH' },
            {'AttributeName': 'c', 'KeyType': 'RANGE' }
        ],
        AttributeDefinitions=[
            {'AttributeName': 'p', 'AttributeType': 'S' },
            {'AttributeName': 'c', 'AttributeType': 'S' },
            {'AttributeName': 'd', 'AttributeType': 'S' }
        ],
        LocalSecondaryIndexes=[
            {   'IndexName': index_name,
                'KeySchema': [
                    { 'AttributeName': 'p', 'KeyType': 'HASH' },
                    { 'AttributeName': 'd', 'KeyType': 'RANGE' },
                ],
                'Projection': { 'ProjectionType': 'ALL' }
            }
        ],
        Tags=tablets_tags)

    # This is how Alternator calls the CQL tables that back up the Alternator
    # tables:
    cql_keyspace_name = 'alternator_' + table_name
    cql_table_name = table_name
    cql_view_name = table_name + '!:' + index_name

    # Verify that the above setup managed to correctly enable tablets, and
    # ensure there is just one tablet for each table.
    await assert_one_tablet(cql, cql_keyspace_name, cql_table_name)
    await assert_one_tablet(cql, cql_keyspace_name, cql_view_name)

    # Move the base tablet (there's just one) to node 0, and the view tablet
    # to node 1. In particular, all view updates will then be remote: node 0
    # will send view updates to node 1.
    await pin_the_only_tablet(manager, cql_keyspace_name, cql_table_name, servers[0])
    await pin_the_only_tablet(manager, cql_keyspace_name, cql_view_name, servers[1])

    await inject_error_on(manager, "delay_before_remote_view_update", servers);

    # Write to the base table (which is on node 0) and read from the LSI
    # (which is on node 1). In a DynamoDB LSI, it is allowed to use strong
    # consistency for the read, and it must return the just-written value.
    item = {'p': 'dog', 'c': 'c0', 'd': 'd0'}
    table.put_item(Item=item)
    assert [item] == full_query(table, IndexName=index_name,
        KeyConditions={
            'p': {'AttributeValueList': ['dog'], 'ComparisonOperator': 'EQ'},
            'd': {'AttributeValueList': ['d0'], 'ComparisonOperator': 'EQ'}
        }
    )
    table.delete()

@pytest.mark.asyncio
async def test_tablet_si_create(manager: ManagerClient):
    """A basic test for creating a secondary index on a table stored
       with tablets on a one-node cluster. We just create the index and
       delete it - that's it, we don't read or write the table.
       Reproduces issue #16194.
    """
    servers = await manager.servers_add(1)
    cql = manager.get_cql()

    await cql.run_async("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 100}")
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int)")
    await cql.run_async("CREATE INDEX my_idx ON test.test(c)")
    await cql.run_async("DROP INDEX test.my_idx")
    await cql.run_async("DROP KEYSPACE test")

async def test_tablet_lsi_create(manager: ManagerClient):
    """A basic test for creating a *local* secondary index on a table stored
       with tablets on a one-node cluster. We just create the index and
       delete it - that's it, we don't read or write the table.
       Reproduces issue #16194.
    """
    servers = await manager.servers_add(1)
    cql = manager.get_cql()

    await cql.run_async("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 100}")
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int)")
    await cql.run_async("CREATE INDEX my_idx ON test.test((pk),c)")
    await cql.run_async("DROP INDEX test.my_idx")
    await cql.run_async("DROP KEYSPACE test")

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_tablet_cql_lsi(manager: ManagerClient):
    """A simple reproducer for issue #16371 where CQL LSI (local secondary
       index) was not using synchronous view updates when tablets are enabled,
       contrary to what the documentation for local SI says. In other words,
       we could write to a table with CL=QUORUM and then try to read with
       CL=QUORUM using the index - and not find the data.

       We use a cluster of just two nodes and RF=1, and control the tablets
       so all base tablets will be in node 0 and all view tablets will be
       in node 1, to ensure that the view update is remote and therefore
       not synchronous by default. To make the test failure even more
       likely on a fast machine, we use the "delay_before_remote_view_update"
       injection point to add a delay to the view update more than usual.
       Reproduces #16371.
    """
    servers = await manager.servers_add(2)
    cql = manager.get_cql()

    # Create a table with an LSI, using tablets. Use just 1 tablets,
    # which is silly in any real-world use case, but makes this test simpler
    # and faster.
    await cql.run_async("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1}")
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int)")
    await cql.run_async("CREATE INDEX my_idx ON test.test((pk),c)")

    # Move the base tablet (there's just one) to node 0, and the view tablet
    # (of the view backing the index) to node 1. In particular all view
    # updates will then be remote: node 0 will send view updates to node 1.
    await pin_the_only_tablet(manager, 'test', 'test', servers[0])
    await pin_the_only_tablet(manager, 'test', 'my_idx_index', servers[1])

    # Add a fixed (0.5 second) delay before view updates, to increase the
    # likehood that if the write didn't wait for the view update, we can try
    # reading before the view update happened and fail the test.
    await inject_error_on(manager, "delay_before_remote_view_update", servers);

    # Write to the base table (whose only replica is on node 0).
    zzz = time.time()
    await cql.run_async(f"INSERT INTO test.test (pk, c) VALUES (7, 42)")
    # If synchronous update worked, this log message should say more
    # than 0.5 seconds (the delay added by injection). If it didn't work,
    # the time will be less than 0.5 seconds and the read is likely to fail.
    logger.info(f"Insert took {time.time()-zzz}")
    # Read using the index (whose only replica is on node 1, and delayed
    # by the injection above). LSI should use synchronous view updates,
    # so the data should be searchable through the local secondary index
    # immediately after the previous INSERT returned.
    assert [(7,42)] == list(await cql.run_async(f"SELECT * FROM test.test WHERE pk=7 AND c=42"))

    await cql.run_async("DROP KEYSPACE test")
