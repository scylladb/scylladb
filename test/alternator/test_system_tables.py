# Copyright 2020-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

# Tests for accessing Scylla-only system tables. All tests are marked
# "scylla_only" so are skipped on DynamoDB.

import pytest
import requests

from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key

from .util import full_scan

internal_prefix = '.scylla.alternator.'

# Test that fetching key columns from system tables works
def test_fetch_from_system_tables(scylla_only, dynamodb, rest_api):
    client = dynamodb.meta.client
    tables_response = client.scan(TableName=internal_prefix+'system_schema.tables',
            AttributesToGet=['keyspace_name','table_name'])

    # #13332 - don't rely on "system" prefix to decide what is user keyspace or not.
    resp_user = requests.get(f'{rest_api}/storage_service/keyspaces?type=user', timeout=1)
    resp_user.raise_for_status()
    keyspaces_user = resp_user.json()

    keyspaces_done = []
    for item in tables_response['Items']:
        ks_name = item['keyspace_name']
        table_name = item['table_name']

        if ks_name in keyspaces_user:
            continue

        col_response = client.query(TableName=internal_prefix+'system_schema.columns',
                KeyConditionExpression=Key('keyspace_name').eq(ks_name) & Key('table_name').eq(table_name))

        key_columns = [item['column_name'] for item in col_response['Items'] if item['kind'] == 'clustering' or item['kind'] == 'partition_key']
        qualified_name = "{}{}.{}".format(internal_prefix, ks_name, table_name)
        import time
        start = time.time()
        response = client.scan(TableName=qualified_name, AttributesToGet=key_columns, Limit=50)
        print(ks_name, table_name, len(str(response)), time.time()-start)
        keyspaces_done.append(ks_name)

    assert keyspaces_done != {}

def test_block_access_to_non_system_tables_with_virtual_interface(scylla_only, test_table_s, dynamodb):
    client = dynamodb.meta.client
    with pytest.raises(ClientError, match='ResourceNotFoundException.*{}'.format(internal_prefix)):
        tables_response = client.scan(TableName="{}alternator_{}.{}".format(internal_prefix, test_table_s.name, test_table_s.name))

def test_block_creating_tables_with_reserved_prefix(scylla_only, dynamodb):
    client = dynamodb.meta.client
    for wrong_name_postfix in ['', 'a', 'xxx', 'system_auth.roles', 'table_name']:
        with pytest.raises(ClientError, match=internal_prefix):
            dynamodb.create_table(TableName=internal_prefix+wrong_name_postfix,
                    BillingMode='PAY_PER_REQUEST',
                    KeySchema=[{'AttributeName':'p', 'KeyType':'HASH'}],
                    AttributeDefinitions=[{'AttributeName':'p', 'AttributeType': 'S'}]
            )

# Test that the system.clients virtual table is readable, and lists ongoing
# Alternator requests, and lists the client SDK's User-Agent (usually
# containing its language, version, and other information) as "driver_name".
# Since we are making the Scan request with Boto3, we expect to find in
# the result of the Scan at least one client using Boto3.
# Reproduces #24993.
def test_system_clients(scylla_only, dynamodb):
    clients = dynamodb.Table(internal_prefix + 'system.clients')
    success = False
    clients = full_scan(clients)
    assert len(clients) > 0
    for client in clients:
        if 'Boto3' in client['driver_name']:
            success = True
            # Verify that some other fields that we expect to appear in
            # Alternator's system.clients entry do appear. For most of
            # them we don't know exactly which value we expect to see,
            # but we know we expect it to appear.
            assert client['client_type'] == 'alternator'
            assert 'address' in client
            assert 'port' in client
            assert 'shard_id' in client
            assert 'connection_stage' in client
            is_ssl = dynamodb.meta.client._endpoint.host.startswith('https')
            # Alternator converts the boolean in the table scanned through
            # the Alternator API to a string 'true' or 'false'. This is
            # probably a bug (because Alternator does have a boolean type
            # it could use!), but it's not the intention of this test to
            # check how scanning CQL tables in Alternator works, so let's
            # just accept both.
            is_ssl_string = 'true' if is_ssl else 'false'
            assert client['ssl_enabled'] == is_ssl or client['ssl_enabled'] == is_ssl_string
            assert 'username' in client
            assert 'scheduling_group' in client
    assert success
