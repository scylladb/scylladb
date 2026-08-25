# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# Tests for the Scylla-only "tablets" feature.
#
# Ideally, tablets are just an implementation detail (replacing the
# old vnodes), that the DynamoDB API user would not even be aware
# of. So there should be very few, if any, tests in this file.
# However, temporarily - while the tablets feature is only partially
# working, it is useful
# to have here a few tests that clarify the situation and how to
# override it. Most of these tests, or perhaps even this entire file,
# will probably go away eventually.

import ast

import pytest
from botocore.exceptions import ClientError

from .util import new_test_table, scylla_config_read, scylla_config_temporary

# All tests in this file are scylla-only
@pytest.fixture(scope="function", autouse=True)
def all_tests_are_scylla_only(scylla_only):
    pass

# Utility function for checking if a given table is using tablets
# or not. We rely on some knowledge of Alternator internals:
# 1. For table with name X, Scylla creates a keyspace called alternator_X
# 2. We can read a CQL system table using the ".scylla.alternator." prefix.
def uses_tablets(dynamodb, table):
    info = dynamodb.Table('.scylla.alternator.system_schema.scylla_keyspaces')
    try:
        response = info.query(
            KeyConditions={'keyspace_name': {
                    'AttributeValueList': ['alternator_'+table.name],
                    'ComparisonOperator':  'EQ'}})
    except dynamodb.meta.client.exceptions.ResourceNotFoundException:
        # The internal Scylla table doesn't even exist, either this isn't
        # Scylla or it's older Scylla and doesn't use tablets.
        return False
    if not 'Items' in response or not response['Items']:
        return False
    if 'initial_tablets' in response['Items'][0] and response['Items'][0]['initial_tablets']:
        return True
    return False

# Utility function for checking whether using tablets by a given table
# is in-line with the global Scylla configuration flag regarding tablets.
def assert_tablets_usage_follows_config(dynamodb, table):
    tablets_default = scylla_config_read(dynamodb, 'tablets_mode_for_new_keyspaces')
    if tablets_default in ("\"enabled\"", "\"enforced\"", None):
        assert uses_tablets(dynamodb, table)
    else:
        assert not uses_tablets(dynamodb, table)

# New Alternator tables are created with tablets or vnodes, according to the
# "tablets_mode_for_new_keyspaces" configuration flag.
def test_default_tablets(dynamodb):
    schema = {
        'KeySchema': [ { 'AttributeName': 'p', 'KeyType': 'HASH' } ],
        'AttributeDefinitions': [ { 'AttributeName': 'p', 'AttributeType': 'S' }]}
    with new_test_table(dynamodb, **schema) as table:
        assert_tablets_usage_follows_config(dynamodb, table)

# Tests for the initial_tablets tag named "system:initial_tablets".
# This tag was earlier called "experimental:initial_tablets".
# Ref. #26211
initial_tablets_tag = 'system:initial_tablets'

# Check that a table created with a number as initial_tablets will use 
# tablets. Different numbers have different meanings (0 asked to use
# default number, any other number overrides the default) but they
# all enable tablets.
def test_initial_tablets_int(dynamodb):
    for value in ['0', '4']:
        schema = {
            'Tags': [{'Key': initial_tablets_tag, 'Value': value}],
            'KeySchema': [ { 'AttributeName': 'p', 'KeyType': 'HASH' } ],
            'AttributeDefinitions': [ { 'AttributeName': 'p', 'AttributeType': 'S' }]}
        with new_test_table(dynamodb, **schema) as table:
            assert uses_tablets(dynamodb, table)

# Check that a table created with a non-number (e.g., the string "none")
# as initial_tablets, will not use tablets.
def test_initial_tablets_not_int(dynamodb):
    schema = {
        'Tags': [{'Key': initial_tablets_tag, 'Value': 'none'}],
        'KeySchema': [ { 'AttributeName': 'p', 'KeyType': 'HASH' } ],
        'AttributeDefinitions': [ { 'AttributeName': 'p', 'AttributeType': 'S' }]}
    with new_test_table(dynamodb, **schema) as table:
        assert not uses_tablets(dynamodb, table)

# Usage of tablets is determined by the configuration flag
# "tablets_mode_for_new_keyspaces", as well as by the per-table
# "system:initial_tablets" tag. The tag overrides the configuration,
# except when the configuration flag's value is "enforced" -
# then if the tag asks for vnodes, an error is generated.
def test_tablets_tag_vs_config(dynamodb):
    schema = {
        'KeySchema': [ { 'AttributeName': 'p', 'KeyType': 'HASH' } ],
        'AttributeDefinitions': [ { 'AttributeName': 'p', 'AttributeType': 'S' }]
    }
    schema_tablets = {**schema, 'Tags': [{'Key': initial_tablets_tag, 'Value': '0'}]}
    schema_vnodes = {**schema, 'Tags': [{'Key': initial_tablets_tag, 'Value': 'none'}]}
    # With tablets_mode_for_new_keyspaces=enabled, tablets are used unless
    # the user explicitly asks for vnodes (schema_vnodes).
    with scylla_config_temporary(dynamodb, 'tablets_mode_for_new_keyspaces', 'enabled'):
        with new_test_table(dynamodb, **schema) as table:
            assert uses_tablets(dynamodb, table)
        with new_test_table(dynamodb, **schema_tablets) as table:
            assert uses_tablets(dynamodb, table)
        with new_test_table(dynamodb, **schema_vnodes) as table:
            assert not uses_tablets(dynamodb, table)
    # With tablets_mode_for_new_keyspaces=disabled, vnodes are used unless
    # the user explicitly asks tablets (schema_tablets)
    with scylla_config_temporary(dynamodb, 'tablets_mode_for_new_keyspaces', 'disabled'):
        with new_test_table(dynamodb, **schema) as table:
            assert not uses_tablets(dynamodb, table)
        with new_test_table(dynamodb, **schema_tablets) as table:
            assert uses_tablets(dynamodb, table)
        with new_test_table(dynamodb, **schema_vnodes) as table:
            assert not uses_tablets(dynamodb, table)
    # With tablets_mode_for_new_keyspaces=enforced, tablets are used except
    # when the user requests vnodes, which is a ValidationException.
    with scylla_config_temporary(dynamodb, 'tablets_mode_for_new_keyspaces', 'enforced'):
        with new_test_table(dynamodb, **schema) as table:
            assert uses_tablets(dynamodb, table)
        with new_test_table(dynamodb, **schema_tablets) as table:
            assert uses_tablets(dynamodb, table)
        with pytest.raises(ClientError, match='ValidationException.*tablets'):
            with new_test_table(dynamodb, **schema_vnodes) as table:
                pass

# For a while (see #18068) it was possible to create an Alternator table with
# tablets enabled and choose LWT for write isolation (always_use_lwt)
# but the writes themselves failed. This test verifies that this is no longer
# the case, and the LWT writes succeed even when tablets are used.
def test_alternator_tablets_and_lwt(dynamodb):
    schema = {
        'Tags': [
            {'Key': initial_tablets_tag, 'Value': '0'},
            {'Key': 'system:write_isolation', 'Value': 'always_use_lwt'}],
        'KeySchema': [ { 'AttributeName': 'p', 'KeyType': 'HASH' } ],
        'AttributeDefinitions': [ { 'AttributeName': 'p', 'AttributeType': 'S' }]}
    with new_test_table(dynamodb, **schema) as table:
        assert_tablets_usage_follows_config(dynamodb, table)
        # This put_item() failed before #18068 was fixed:
        table.put_item(Item={'p': 'hello'})
        assert table.get_item(Key={'p': 'hello'}, ConsistentRead=True)['Item'] == {'p': 'hello'}

# An Alternator table created with tablets and with a write isolation
# mode that doesn't use LWT ("forbid_rmw") works normally, even
# before #18068 is fixed.
def test_alternator_tablets_without_lwt(dynamodb):
    schema = {
        'Tags': [
            {'Key': initial_tablets_tag, 'Value': '0'},
            {'Key': 'system:write_isolation', 'Value': 'forbid_rmw'}],
        'KeySchema': [ { 'AttributeName': 'p', 'KeyType': 'HASH' } ],
        'AttributeDefinitions': [ { 'AttributeName': 'p', 'AttributeType': 'S' }]}
    with new_test_table(dynamodb, **schema) as table:
        assert_tablets_usage_follows_config(dynamodb, table)
        table.put_item(Item={'p': 'hello'})
        assert table.get_item(Key={'p': 'hello'})['Item'] == {'p': 'hello'}

# Utility function for reading the tombstone_gc mode of an Alternator table,
# or - if index_name is given - of the materialized view backing that index
# (index_delim distinguishes the view names of GSIs (':') and LSIs ('!:')).
# Returns the mode as a string, or None if the table or view doesn't have
# the tombstone_gc property at all.
def tombstone_gc_mode(cql, table, index_name=None, index_delim=None):
    keyspace = 'alternator_' + table.name
    if index_name:
        describe = f'DESCRIBE MATERIALIZED VIEW "{keyspace}"."{table.name}{index_delim}{index_name}"'
    else:
        describe = f'DESCRIBE TABLE "{keyspace}"."{table.name}"'
    stmt = cql.execute(describe).one().create_statement
    # The returned create statement lists the tombstone_gc property as a map,
    # e.g., "tombstone_gc = {'mode': 'repair', ...}", so after partition()
    # rest begins with the map literal, which ends at the first '}' (the map
    # has no nested braces) and is also a valid Python literal. Parse it and
    # return its 'mode' value - 'repair' in the example above - or None if
    # the statement has no tombstone_gc property at all.
    _, found, rest = stmt.partition('tombstone_gc = ')
    return ast.literal_eval(rest[:rest.index('}') + 1])['mode'] if found else None

# Alternator tables and their GSI/LSI views should get the same default
# tombstone_gc mode as CQL-created tables and indexes: 'repair', except for
# views co-located with their base table (a view of a tablets-using table
# with the same partition key - e.g., an LSI view), which don't support
# repair and should get 'timeout'. This is what lsi_mode expects below.
# Reproduces SCYLLADB-3759, where the mode was left unset - so all Alternator
# tables silently defaulted to 'timeout'.
@pytest.mark.parametrize('initial_tablets,lsi_mode', [
    pytest.param('0', 'timeout', id='tablets'),
    pytest.param('none', 'repair', id='vnodes')])
def test_tombstone_gc_default(dynamodb, cql, initial_tablets, lsi_mode):
    schema = {
        'Tags': [{'Key': initial_tablets_tag, 'Value': initial_tablets}],
        'KeySchema': [ { 'AttributeName': 'p', 'KeyType': 'HASH' },
                       { 'AttributeName': 'c', 'KeyType': 'RANGE' } ],
        'AttributeDefinitions': [ { 'AttributeName': 'p', 'AttributeType': 'S' },
                                  { 'AttributeName': 'c', 'AttributeType': 'S' },
                                  { 'AttributeName': 'x', 'AttributeType': 'S' },
                                  { 'AttributeName': 'y', 'AttributeType': 'S' } ],
        'GlobalSecondaryIndexes': [
            { 'IndexName': 'gsi',
              'KeySchema': [ { 'AttributeName': 'x', 'KeyType': 'HASH' } ],
              'Projection': { 'ProjectionType': 'ALL' } } ],
        'LocalSecondaryIndexes': [
            { 'IndexName': 'lsi',
              'KeySchema': [ { 'AttributeName': 'p', 'KeyType': 'HASH' },
                             { 'AttributeName': 'y', 'KeyType': 'RANGE' } ],
              'Projection': { 'ProjectionType': 'ALL' } } ]}
    with new_test_table(dynamodb, **schema) as table:
        assert uses_tablets(dynamodb, table) == (initial_tablets != 'none')
        assert tombstone_gc_mode(cql, table) == 'repair'
        assert tombstone_gc_mode(cql, table, 'gsi', ':') == 'repair'
        assert tombstone_gc_mode(cql, table, 'lsi', '!:') == lsi_mode
        # A GSI created later by UpdateTable takes a different code path
        # than CreateTable, so check it too:
        dynamodb.meta.client.update_table(TableName=table.name,
            AttributeDefinitions=[{ 'AttributeName': 'z', 'AttributeType': 'S' }],
            GlobalSecondaryIndexUpdates=[ { 'Create':
                { 'IndexName': 'gsi2',
                  'KeySchema': [ { 'AttributeName': 'z', 'KeyType': 'HASH' } ],
                  'Projection': { 'ProjectionType': 'ALL' } }}])
        assert tombstone_gc_mode(cql, table, 'gsi2', ':') == 'repair'
