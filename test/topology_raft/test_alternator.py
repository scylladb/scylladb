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
from botocore.exceptions import ClientError
import requests
import json
from cassandra.auth import PlainTextAuthProvider

from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for
from test.topology.conftest import skip_mode

logger = logging.getLogger(__name__)

# Convenience function to open a connection to Alternator usable by the
# AWS SDK.
alternator_config = {
    'alternator_port': 8000,
    'alternator_write_isolation': 'only_rmw_uses_lwt',
    'alternator_ttl_period_in_seconds': '0.5',
}
def get_alternator(ip, user='alternator', passwd='secret_pass'):
    url = f"http://{ip}:{alternator_config['alternator_port']}"
    return boto3.resource('dynamodb', endpoint_url=url,
        region_name='us-east-1',
        aws_access_key_id=user,
        aws_secret_access_key=passwd,
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
       the "streaming" scheduling group. But because of a bug in inheritance
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

    # Insert N rows, setting them all to expire 3 seconds from now.
    N = 100
    expiration = int(time.time())+3
    with table.batch_writer() as batch:
        for p in range(N):
            batch.put_item(Item={'p': p, 'expiration': expiration})


    # Unfortunately, Alternator has no way of doing the writes above with
    # CL=ALL, only CL=QUORUM. So at this point we're not sure all the writes
    # above have completed. We want to wait until they are over, so that we
    # won't measure any of those writes in the statement scheduling group.
    # Let's do it by checking the metrics of background writes and wait for
    # them to drop to zero.
    ips = [server.ip_addr for server in await manager.running_servers()]
    timeout = time.time() + 60
    while True:
        if time.time() > timeout:
            pytest.fail("timed out waiting for background writes to complete")
        bg_writes = 0
        for ip in ips:
            metrics = await manager.metrics.query(ip)
            bg_writes += metrics.get('scylla_storage_proxy_coordinator_background_writes')
        if bg_writes == 0:
            break # done waiting for the background writes to finish
        await asyncio.sleep(0.1)

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

    # Wait until all rows expire, and get the CPU metrics again. All items
    # were set to expire in 3 seconds, and the expiration thread is set up
    # in alternator_config to scan the whole table in 0.5 seconds, and the
    # whole table is just 100 rows, so we expect all the data to be gone in
    # 4 seconds. Let's wait 5 seconds just in case. Even if not all the data
    # will have been deleted by then, we do expect some deletions to have
    # happened, and certainly several scans, all taking CPU which we expect
    # to be in the right scheduling group.
    await asyncio.sleep(5)
    ms_streaming_after, ms_statement_after = await get_cpu_metrics()

    # As a sanity check, verify some of the data really expired, so there
    # was some TTL work actually done. We actually expect all of the data
    # to have been expired by now, but in some extremely slow builds and
    # test machines, this may not be the case.
    assert N > table.scan(ConsistentRead=True, Select='COUNT')['Count']

    # Between the calls to get_cpu_metrics() above, several expiration scans
    # took place (we configured scans to happen every 0.5 seconds), and also
    # a lot of deletes when the expiration time was reached. We expect all
    # that work to have happened in the streaming group, not statement group,
    # so "ratio" calculate below should be tiny, even exactly zero. Before
    # issue #18719 was fixed, it was not tiny at all - 0.58.
    # Just in case there are other unknown things happening, let's assert it
    # is <0.1 instead of zero.
    ms_streaming = ms_streaming_after - ms_streaming_before
    ms_statement = ms_statement_after - ms_statement_before
    ratio = ms_statement / ms_streaming
    assert ratio < 0.1

    table.delete()

@pytest.mark.asyncio
async def test_localnodes_broadcast_rpc_address(manager: ManagerClient):
    """Test that if the "broadcast_rpc_address" of a node is set, the
       "/localnodes" request returns not the node's internal IP address,
       but rather the one set in broadcast_rpc_address as passed between
       nodes via gossip. The case where this parameter is not configured is
       tested separately, in test/alternator/test_scylla.py.
       Reproduces issue #18711.
    """
    # Run two Scylla nodes telling both their broadcast_rpc_address is 1.2.3.4
    # (this is silly, but servers_add() doesn't let us use a different config
    # per server). We need to run two nodes to check that the node to which
    # we send the /localnodes request knows not only its own modified
    # address, but also the other node's (which it learnt by gossip).
    # This address isn't used for any communication, but it will be
    # produced by "/localnodes" and this is what we want to check
    config = alternator_config | {
        'broadcast_rpc_address': '1.2.3.4'
    }
    servers = await manager.servers_add(2, config=config)
    for server in servers:
        # We expect /localnodes to return ["1.2.3.4", "1.2.3.4"]
        # (since we configured both nodes with the same broadcast_rpc_address).
        # We need the retry loop below because the second node might take a
        # bit of time to bootstrap after coming up, and only then will it
        # appear on /localnodes (see #19694).
        url = f"http://{server.ip_addr}:{config['alternator_port']}/localnodes"
        timeout = time.time() + 60
        while True:
            assert time.time() < timeout
            response = requests.get(url, verify=False)
            j = json.loads(response.content.decode('utf-8'))
            if j == ['1.2.3.4', '1.2.3.4']:
                break # done
            await asyncio.sleep(0.1)

@pytest.mark.asyncio
async def test_localnodes_drained_node(manager: ManagerClient):
    """Test that if in a cluster one node is brought down with "nodetool drain"
       a "/localnodes" request should NOT return that node. This test does
       NOT reproduce issue #19694 - a DRAINED node is not considered is_alive()
       and even before the fix of that issue, "/localnodes" didn't return it.
    """
    # Start a cluster with two nodes and verify that at this point,
    # "/localnodes" on the first node returns both nodes.
    # We the retry loop below because the second node might take a
    # bit of time to bootstrap after coming up, and only then will it
    # appear on /localnodes (see #19694).
    servers = await manager.servers_add(2, config=alternator_config)
    localnodes_request = f"http://{servers[0].ip_addr}:{alternator_config['alternator_port']}/localnodes"
    async def check_localnodes_two():
        response = requests.get(localnodes_request)
        j = json.loads(response.content.decode('utf-8'))
        if set(j) == {servers[0].ip_addr, servers[1].ip_addr}:
            return True
        elif set(j).issubset({servers[0].ip_addr, servers[1].ip_addr}):
            return None # try again
        else:
            return False
    assert await wait_for(check_localnodes_two, time.time() + 60)
    # Now "nodetool" drain on the second node, leaving the second node
    # in DRAINED state.
    await manager.api.client.post("/storage_service/drain", host=servers[1].ip_addr)
    # After that, "/localnodes" should no longer return the second node.
    # It might take a short while until the first node learns what happened
    # to node 1, so we may need to retry for a while
    async def check_localnodes_one():
        response = requests.get(localnodes_request)
        j = json.loads(response.content.decode('utf-8'))
        if set(j) == {servers[0].ip_addr, servers[1].ip_addr}:
            return None # try again
        elif set(j) == {servers[0].ip_addr}:
            return True
        else:
            return False
    assert await wait_for(check_localnodes_one, time.time() + 60)

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_localnodes_joining_nodes(manager: ManagerClient):
    """Test that if a cluster is being enlarged and a node is coming up but
       not yet responsive, a "/localnodes" request should NOT return that node.
       Reproduces issue #19694.
    """
    # Start a cluster with one node, and then bring up a second node,
    # pausing its bootstrap (with an injection) in JOINING state.
    # We need to start the second node in the background, because server_add()
    # will wait for the bootstrap to complete - which we don't want to do.
    server = await manager.server_add(config=alternator_config)
    task = asyncio.create_task(manager.server_add(config=alternator_config | {'error_injections_at_startup': ['delay_bootstrap_120s']}))
    # Sleep until the first node knows of the second one as a "live node"
    # (we check this with the REST API's /gossiper/endpoint/live.
    async def check_two_live_nodes():
        j = await manager.api.client.get_json("/gossiper/endpoint/live", host=server.ip_addr)
        if len(j) == 1:
            return None # try again
        elif len(j) == 2:
            return True
        else:
            return False
    assert await wait_for(check_two_live_nodes, time.time() + 60)

    # At this point the second node is live, but hasn't finished bootstrapping
    # (we delayed that with the injection). So the "/localnodes" should still
    # return just one node - not both. Reproduces #19694 (two nodes used to
    # be returned)
    localnodes_request = f"http://{server.ip_addr}:{alternator_config['alternator_port']}/localnodes"
    response = requests.get(localnodes_request)
    j = json.loads(response.content.decode('utf-8'))
    assert len(j) == 1

    # We don't want to wait for the second server to finish its long
    # injection-caused bootstrap delay, so we won't check here that when the
    # second server finally comes up, both nodes will finally be visible in
    # /localnodes. This case is checked in other tests, where bootstrap
    # finishes normally, so we don't need to check this case again here.
    # But we can't just finish here with "task" unwaited or we'll get a
    # warning about an unwaited coroutine, and the ScyllaClusterManager's
    # tasks_history will wait for it anyway. For the same reason we can't
    # task.cancel() (this will cause ScyllaClusterManager's tasks_history
    # to report the ScyllaClusterManager got BROKEN and fail the next test).
    # Sadly even abruptly killing the servers (with manager.server_stop())
    # (with the intention to then "await task" quickly) doesn't work,
    # probably because of a bug in the library. So we "await task"
    # anyway, and this test takes 2 minutes :-(
    #for server in await manager.all_servers():
    #    await manager.server_stop(server.server_id)
    await task

# TODO: add a more thorough test for /localnodes, creating a cluster with
# multiple nodes in multiple data centers, and check that we can get a list
# of nodes in each data center.

# We have in test/alternator/test_cql_rbac.py many functional tests for
# CQL-based Role Based Access Control (RBAC) and all those tests use the
# same one-node cluster with authentication and authorization enabled.
# Here in this file we have the opportunity to create clusters with different
# configurations, so we can check how these configuration settings affect RBAC.
@pytest.mark.asyncio
async def test_alternator_enforce_authorization_false(manager: ManagerClient):
    """A basic test for how Alternator authentication and authorization
       work when alternator_enfore_authorization is *false* (and CQL's
       authenticator/authorizer options are also unset):
       1. Username and signature is not checked - a request with a bad
          username is accepted.
       2. Any user (or even non-existent user) has permissions to do any
          operation.
    """
    servers = await manager.servers_add(1, config=alternator_config)
    # Requests from a non-existent user with garbage password work,
    # and can perform privildged operations like CreateTable, etc.
    alternator = get_alternator(servers[0].ip_addr, 'nonexistent_user', 'garbage')
    table = alternator.create_table(TableName=unique_table_name(),
        BillingMode='PAY_PER_REQUEST',
        KeySchema=[ {'AttributeName': 'p', 'KeyType': 'HASH' } ],
        AttributeDefinitions=[ {'AttributeName': 'p', 'AttributeType': 'N' } ])
    table.put_item(Item={'p': 42})
    table.get_item(Key={'p': 42})
    table.delete()

def get_secret_key(cql, user):
    """The secret key used for a user in Alternator is its role's salted_hash.
       This function retrieves it from the system table.
    """
    # Newer Scylla places the "roles" table in the "system" keyspace, but
    # older versions used "system_auth_v2" or "system_auth"
    for ks in ['system', 'system_auth_v2', 'system_auth']:
        try:
            e = list(cql.execute(f"SELECT salted_hash FROM {ks}.roles WHERE role = '{user}'"))
            if e != [] and e[0].salted_hash is not None:
                return e[0].salted_hash
        except:
            pass
    pytest.fail(f"Couldn't get secret key for user {user}")

@pytest.mark.skip("flaky, needs to be fixed, see https://github.com/scylladb/scylladb/pull/20135")
@pytest.mark.asyncio
async def test_alternator_enforce_authorization_true(manager: ManagerClient):
    """A basic test for how Alternator authentication and authorization
       work when authentication and authorization is enabled in CQL, and
       additionally alternator_enfore_authorization is *true*:
       1. The username and signature is verified (a request with a bad
          username or password is rejected)
       2. A new user works, and can do things that don't need permissions
          (such as ListTables) but can't perform operations that do need
          permissions (e.g., CreateTable).
    """
    config = alternator_config | {
        'alternator_enforce_authorization': True,
        'authenticator': 'PasswordAuthenticator',
        'authorizer': 'CassandraAuthorizer'
    }
    servers = await manager.servers_add(1, config=config,
        driver_connect_opts={'auth_provider': PlainTextAuthProvider(username='cassandra', password='cassandra')})
    cql = manager.get_cql()
    # Any requests from a non-existent user with garbage password is
    # rejected - even requests that don't need special permissions
    alternator = get_alternator(servers[0].ip_addr, 'nonexistent_user', 'garbage')
    with pytest.raises(ClientError, match='UnrecognizedClientException'):
        alternator.meta.client.list_tables()
    # We know that Scylla is set up with a "cassandra" user. If we retrieve
    # its correct secret key, the ListTables will work.
    alternator = get_alternator(servers[0].ip_addr, 'cassandra', get_secret_key(cql, 'cassandra'))
    alternator.meta.client.list_tables()
    # Privileged operations also work for the superuser account "cassandra":
    table = alternator.create_table(TableName=unique_table_name(),
        BillingMode='PAY_PER_REQUEST',
        KeySchema=[ {'AttributeName': 'p', 'KeyType': 'HASH' } ],
        AttributeDefinitions=[ {'AttributeName': 'p', 'AttributeType': 'N' } ])
    table.put_item(Item={'p': 42})
    table.get_item(Key={'p': 42})
    table.delete()
    # Create a new role "user2" and make a new connection "alternator2" with it:
    cql.execute("CREATE ROLE user2 WITH PASSWORD = 'user2' AND LOGIN=TRUE")
    alternator2 = get_alternator(servers[0].ip_addr, 'user2', get_secret_key(cql, 'user2'))
    # In the new role, ListTables works, but other privileged operations
    # don't.
    alternator2.meta.client.list_tables()
    with pytest.raises(ClientError, match='AccessDeniedException'):
        alternator2.create_table(TableName=unique_table_name(),
            BillingMode='PAY_PER_REQUEST',
            KeySchema=[ {'AttributeName': 'p', 'KeyType': 'HASH' } ],
            AttributeDefinitions=[ {'AttributeName': 'p', 'AttributeType': 'N' } ])
    # We could further test how GRANT works, but this would be unnecessary
    # repeating of the tests in test/alternator/test_cql_rbac.py.
