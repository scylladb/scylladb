#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

# Multi-node tests for Alternator.
#
# Please note that most tests for Alternator are single-node tests and can
# be found in the test/alternator directory. Most functional testing of the
# many different syntax features that Alternator provides don't need more
# than a single node to be tested, and should be able to run also on DynamoDB
# - not just on Alternator, which the test/alternator framework allows to do.
# So only the minority of tests that do need a bigger cluster should be here.

import pytest
import asyncio
import logging
import time
import boto3
import botocore

logger = logging.getLogger(__name__)

# Convenience function to open a connection to Alternator usable by the
# AWS SDK.
alternator_config = {
    'alternator_port': 8000,
    'alternator_write_isolation': 'only_rmw_uses_lwt',
    'alternator_ttl_period_in_seconds': '0.5',
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

# FIXME: boto3 is NOT async. So all tests that use it are not really async.
# We could use the aioboto3 library to write a really asynchronous test, or
# implement an async wrapper to the boto3 functions ourselves (e.g., run them
# in a separate thread) ourselves.

@pytest.fixture(scope="module")
async def alternator3(manager_internal):
    """A fixture with a 3-node Alternator cluster that can be shared between
       multiple tests. These test should not modify the cluster's topology,
       and should each use unique table names and/or unique keys to avoid
       being confused by other tests.
       Returns the manager object and 3 boto3 resource objects for making
       DynamoDB API requests to each of the nodes in the Alternator cluster.
    """
    manager = manager_internal()
    servers = await manager.servers_add(3, config=alternator_config)
    yield [manager] + [get_alternator(server.ip_addr) for server in servers]
    await manager.stop()

test_table_prefix = 'alternator_Test_'
def unique_table_name():
    current_ms = int(round(time.time() * 1000))
    # If unique_table_name() is called twice in the same millisecond...
    if unique_table_name.last_ms >= current_ms:
        current_ms = unique_table_name.last_ms + 1
    unique_table_name.last_ms = current_ms
    return test_table_prefix + str(current_ms)
unique_table_name.last_ms = 0


async def test_alternator_ttl_scheduling_group(alternator3):
    """A reproducer for issue #18719: The expiration scans and deletions
       initiated by the Alternator TTL feature are supposed to run entirely in
       the "streaming" scheduling group. But because of a bug in inheritence
       of scheduling groups through RPC, some of the work ended up being done
       on the "statement" scheduling group.
       This test verifies that Alternator TTL work is done on the right
       scheduling group.
       This test assumes that the cluster is not concurrently busy with
       running any other workload - so we won't see any work appearing
       in the wrong scheduling group. We can assume this because we don't
       run multiple tests in parallel on the same cluster.
    """
    manager, alternator, *_ = alternator3
    table = alternator.create_table(TableName=unique_table_name(),
        BillingMode='PAY_PER_REQUEST',
        KeySchema=[
            {'AttributeName': 'p', 'KeyType': 'HASH' },
        ],
        AttributeDefinitions=[
            {'AttributeName': 'p', 'AttributeType': 'N' },
        ])
    # Enable expiration (TTL) on attribute "expiration"
    table.meta.client.update_time_to_live(TableName=table.name, TimeToLiveSpecification={'AttributeName': 'expiration', 'Enabled': True})

    # Insert N rows, setting them all to expire 4 seconds from now.
    N = 100
    expiration = int(time.time())+4
    with table.batch_writer() as batch:
        for p in range(N):
            batch.put_item(Item={'p': p, 'expiration': expiration})

    # Unfortunately, Alternator has no way of doing the writes above with
    # CL=ALL, only CL=QUORUM. So at this point we're not sure all the writes
    # above have completed. We want to wait until they are over, so that we
    # won't measure any of those writes in the statement scheduling group.
    # Unfortunately, there is no mechanism to wait for this, but hopefully
    # sleeping 2 seconds will allow the just 100 writes above to complete.
    # And even if not, most of them will complete and the remaining work
    # will be very small (this is why below we don't check that it's 0,
    # but that it's less than 1% of the work in the streaming group).
    time.sleep(2)

    ips = [server.ip_addr for server in await manager.running_servers()]
    # Get the current amount of work (in CPU ms) done across all nodes and
    # shards in different scheduling groups. We expect this to increase
    # considerably for the streaming group while expiration scanning is
    # proceeding, but not increase at all for the statement group because
    # there are no requests being executed.
    async def get_cpu_metrics():
        ms_streaming = 0
        ms_statement = 0
        for ip in ips:
            metrics = await manager.metrics.query(ip)
            ms_streaming += metrics.get('scylla_scheduler_runtime_ms', {'group': 'streaming'})
            ms_statement += metrics.get('scylla_scheduler_runtime_ms', {'group': 'statement'})
        return (ms_streaming, ms_statement)

    ms_streaming_before, ms_statement_before = await get_cpu_metrics()

    # Wait until all rows expire, and get the CPU metrics again
    time.sleep(4) # items were set to expire in 4 seconds, we already slept 2.
    ms_streaming_after, ms_statement_after = await get_cpu_metrics()

    # As a sanity check, verify all the data really expired, so the table
    # is empty:
    assert 0 == table.scan(ConsistentRead=True, Select='COUNT')['Count']

    # Between the calls to get_cpu_metrics() above, several expiration scans
    # took place (we configured scans to happen every 0.5 seconds), and also
    # a lot of deletes when the expiration time was reached. We expect all
    # that work to have happened in the streaming group, not statement group,
    # so "ratio" calculate below should be tiny, even exactly zero. Before
    # issue #18719 was fixed, it was not tiny at all - 0.58.
    # Just in case there are other unknown things happening (including
    # unfinished work from the original writes), let's assert it is <0.01
    # instead of zero.
    ms_streaming = ms_streaming_after - ms_streaming_before
    ms_statement = ms_statement_after - ms_statement_before
    ratio = ms_statement / ms_streaming
    assert ratio < 0.01

    table.delete()
