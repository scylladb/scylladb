# Copyright 2020-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

# Tests for accessing alternator-only system tables (from Scylla).

import pytest
import requests

from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key

internal_prefix = '.scylla.alternator.'

# reproduces scylladb/scylladb#26960
# page break over range tombstone was causing use-after-free, because `encode_paging_state`
# assumed position had always data for each column in clustering key
# which is not true for range tombstones - if clustering key has two columns,
# use can range delete by specify only first column, which produces key prefix with only first column filled.
# we use scylla_table_schema_history here, because it has a composite clustering key (schema_version, column_name)
def test_page_break_over_range_tombstone_asan(scylla_only, dynamodb, rest_api, cql):
    client = dynamodb.meta.client

    # scylla_table_schema_history has a primary key of (cf_id, schema_version, column_name)
    # insert some rows with same cf_id, but different schema_version (column_name is unimportant)
    # any number greater than 2 should do
    count = 20

    # any uuid will work here for s and c, but values inserted must follow ordering in such a way, that
    # first and last value was alive and those in middle were deleted.
    s = f'72f224cf0000'
    for y in range(0, count):
        c = f'82f224cf{y:04}'
        cql.execute(f"INSERT INTO system.scylla_table_schema_history (cf_id, schema_version, column_name, clustering_order, column_name_bytes, kind, position, type) VALUES (eee7eb26-a372-4eb4-aeaa-{s}, eee7eb26-a372-4eb4-aeaa-{c}, 'a', 'a', 0x1234, 'k', 1, 't');")
    # range delete previously created rows by specifing only schema_version (first column of clustering key),
    # skipping second column (column_name) - this will create range tombstone per each delete.
    # we delete each row separately to create multiple range tombstones.
    # leave first and last row intact
    for y in range(1, count - 1):
        c = f'82f224cf{y:04}'
        cql.execute(f'delete from system.scylla_table_schema_history where cf_id = eee7eb26-a372-4eb4-aeaa-{s} and schema_version = eee7eb26-a372-4eb4-aeaa-{c};')

    qualified_name = f"{internal_prefix}system.scylla_table_schema_history"
    pos = None
    args = {}

    # update tombstone page limit to be smaller than tombstone count between two values.
    # this will force scan to break page over range tombstone
    # should return 1 item on first iteration and break over tombstone
    # then resume and return second one.
    with scylla_config_temporary(dynamodb, 'query_tombstone_page_limit', str(count - 2)):
        items_found = []
        while True:
            response = client.scan(TableName=qualified_name, Limit=10, **args)
            pos = response.get('LastEvaluatedKey', None)
            cnt = 0
            for i in response['Items']:
                if i['cf_id'] == 'eee7eb26-a372-4eb4-aeaa-72f224cf0000':
                    items_found.append(i['schema_version'])
            if not pos:
                break
            args['ExclusiveStartKey'] = pos

    assert items_found == [ 'eee7eb26-a372-4eb4-aeaa-82f224cf0000', 'eee7eb26-a372-4eb4-aeaa-82f224cf0019' ]

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

