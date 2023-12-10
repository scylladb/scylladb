#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

# Tests for interaction of materialized views with *tablets*

from test.pylib.manager_client import ManagerClient
from test.topology.conftest import skip_mode

import pytest
import asyncio
import logging
import random


logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_tablet_mv_create(manager: ManagerClient):
    """A basic test for creating a materialized view on a table stored
       with tablets on a one-node cluster. We just create the view and
       delete it - that's it, we don't read or write the table.
       Reproduces issue #16194.
    """
    servers = await manager.servers_add(1)
    cql = manager.get_cql()

    await cql.run_async("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1, 'initial_tablets': 100}")
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

    await cql.run_async("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1, 'initial_tablets': 100}")
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
    await cql.run_async("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1, 'initial_tablets': 100}")
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int)")
    await cql.run_async("CREATE MATERIALIZED VIEW test.tv AS SELECT * FROM test.test WHERE c IS NOT NULL AND pk IS NOT NULL PRIMARY KEY (c, pk) WITH SYNCHRONOUS_UPDATES = TRUE")
    await cql.run_async("INSERT INTO test.test (pk, c) VALUES (2, 3)")
    # We used SYNCHRONOUS_UPDATES=TRUE, so the view should be updated:
    assert [(3,2)] == list(await cql.run_async("SELECT * FROM test.tv WHERE c=3"))
    await cql.run_async("DROP KEYSPACE test")

# Convenience function to open a connection to Alternator usable by the
# AWS SDK. When more than one test needs it, we can move this function to
# a separate library file. Until then, let's just have it in front of the
# only test that needs it.
import boto3
import botocore
alternator_config = {
    'alternator_port': 8000,
    'alternator_write_isolation': 'only_rmw_uses_lwt',
}
def get_alternator(ip):
    url = f"http://{ip}:{alternator_config['alternator_port']}"
    return boto3.resource('dynamodb', endpoint_url=url,
        region_name='us-east-1',
        aws_access_key_id='alternator',
        aws_secret_access_key='secret_pass',
        config=botocore.client.Config(
            retries={"max_attempts": 0},
            read_timeout=300)
    )

# Alternator convenience function for fetching the entire result set of a
# query into an array of items.
def full_query(table, ConsistentRead=True, **kwargs):
    response = table.query(ConsistentRead=ConsistentRead, **kwargs)
    items = response['Items']
    while 'LastEvaluatedKey' in response:
        response = table.query(ExclusiveStartKey=response['LastEvaluatedKey'],
            ConsistentRead=ConsistentRead, **kwargs)
        items.extend(response['Items'])
    return items

async def inject_error_on(manager, error_name, servers):
    errs = [manager.api.enable_injection(s.ip_addr, error_name, False) for s in servers]
    await asyncio.gather(*errs)

# FIXME: boto3 is NOT async. So this test is not async. We could use
# the aioboto3 library to write a really asynchronous test, or implement
# an async wrapper to the boto3 functions ourselves (e.g., run them in a
# separate thread) ourselves.
@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
@skip_mode('debug', '10-node test is too slow in debug mode')
async def test_tablet_alternator_lsi_consistency(manager: ManagerClient):
    """A reproducer for a bug where Alternator LSI was not using synchronous
       view updates when tablets are enabled, which could cause strongly-
       consistent read of the LSI to miss the data just written to the base.
       We use a fairly large cluster to increase the chance that the tablet
       randomization results in non-local pairing of base and view tablets.
       We could make this test fail more reliably and only need 4 nodes if
       we had an API to control the tablet placement.
       We also use the "delay_before_remote_view_update" injection point
       to add a delay to the view update - without it it's almost impossible
       to reproduce this issue on a fast machine.
       Reproduces #16313.
    """
    servers = await manager.servers_add(10, config=alternator_config)
    cql = manager.get_cql()
    alternator = get_alternator(servers[0].ip_addr)
    # Currently, to create an Alternator table with tablets, a special
    # tag must be given. See issue #16203. In the future this should be
    # automatic - any Alternator table will use tablets.
    tablets_tags = [{'Key': 'experimental:initial_tablets', 'Value': '100'}]
    # Create a table with an LSI
    table = alternator.create_table(TableName='alternator_table',
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
            {   'IndexName': 'ind',
                'KeySchema': [
                    { 'AttributeName': 'p', 'KeyType': 'HASH' },
                    { 'AttributeName': 'd', 'KeyType': 'RANGE' },
                ],
                'Projection': { 'ProjectionType': 'ALL' }
            }
        ],
        Tags=tablets_tags)
    # Above we connected to a one node to perform Alternator requests.
    # Now we want to be able to connect to the above-created table through
    # each one of the nodes. Let's define tables[i] as the same table accessed
    # through node i (all of them refer to the same table):
    alternators = [get_alternator(server.ip_addr) for server in servers]
    tables = [alternator.Table(table.name) for alternator in alternators]

    await inject_error_on(manager, "delay_before_remote_view_update", servers);

    # write to the table through one node (table1) and read from the view
    # through another node (table2). In an LSI, it is allowed to use strong
    # consistency for the read, and it must return the just-written value.
    # Try 10 times, in different combinations of nodes, to increase the
    # chance of reproducing the bug.
    for i in range(10):
        table1 = tables[random.randrange(0, len(tables))]
        table2 = tables[random.randrange(0, len(tables))]
        item = {'p': 'dog', 'c': 'c'+str(i), 'd': 'd'+str(i)}
        table1.put_item(Item=item)
        assert [item] == full_query(table2, IndexName='ind',
            KeyConditions={
                'p': {'AttributeValueList': ['dog'], 'ComparisonOperator': 'EQ'},
                'd': {'AttributeValueList': ['d'+str(i)], 'ComparisonOperator': 'EQ'}
            }
        )
    table.delete()
