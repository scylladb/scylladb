# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

# Tests for the underlying CQL schema (keyspace and table) which is used to
# store an Alternator table and all its sub-tables (holding GSIs, LSIs and
# CDC logs).
# Arguably, since those properties aren't directly visible through the
# DynamoDB API, we do not really need to guarantee backward compatibility
# in this area, so we shouldn't have tests for it. However, these tests are
# still useful for understanding what the underlying CQL schema looks like,
# and to prevent unintended drift between CQL's and Alternator's defaults
# or unintended drift to our on-disk encoding (see issue #19770) our
# existing storage. If we ever do *intended* changes to these properties
# of Alternator tables, we will need to change these tests.
#
# Because this file is all about testing the Scylla-only CQL-based APIs,
# all tests in this file are skipped when running against Amazon DynamoDB.
#
# There are additional files with tests related to configuring certain
# properties of Alternator tables through CQL: See test_cql_rbac.py for
# configuring role-based access control, and test_service_levels.py for
# configuring service levels - both need to be done through CQL.

import string
import pytest

from .util import full_query, global_random, new_test_table
from .test_scylla import this_dc

# All tests in this file are scylla-only
@pytest.fixture(scope="function", autouse=True)
def all_tests_are_scylla_only(scylla_only):
    pass

# Utility function for getting (using Alternator's system-table access)
# the options set for the given CQL table given by a keyspace, name and
# is_view triplet. The is_view boolean decides if the name is assumed to
# refer to a materialized view or a base table. In Scylla's system tables
# we have a separate system table listing tables and views.
def scylla_get_schema(dynamodb, ks_name_and_isview):
    ks, name, is_view = ks_name_and_isview
    what = 'view' if is_view else 'table'
    sys = dynamodb.Table(f'.scylla.alternator.system_schema.{what}s')
    # We need to read just a single key, but strangely Alternator doesn't
    # allow GetItem on system tables, just Query/Scan, so we use Query:
    res = full_query(sys,
        KeyConditionExpression=f'keyspace_name=:ks and {what}_name=:name',
        ExpressionAttributeValues={':ks': ks, ':name' : name})
    assert len(res) == 1
    return res[0]

# Utility functions for getting the keyspace and table name used to store
# an Alternator base-table, GSI, LSI and Streams respectively. Tests below
# will use those functions, so will ensure that this table naming doesn't
# accidentally change. These functions return a triplet ks,name,is_view
# as expected by scylla_get_schema().
def cql_table_for(table):
    return 'alternator_'+table.name, table.name, False

def cql_table_for_gsi(table, gsi):
    return 'alternator_'+table.name, table.name + ':' + gsi, True

def cql_table_for_lsi(table, lsi):
    return 'alternator_'+table.name, table.name + '!:' + lsi, True

def cql_table_for_cdclog(table):
    return 'alternator_'+table.name, table.name + '_scylla_cdc_log', False

# Confirm that for Alternator table table_name, a CQL table is created with
# keyspace "alternator_{table_name}" and table "{table_name}".
def test_cql_keyspace_and_table(dynamodb, test_table):
    scylla_get_schema(dynamodb, cql_table_for(test_table))

# The fixtures cql_keyspace, cql_table create a new CQL keyspace and table,
# so we can compare their properties to those of a table created by Alternator.
def unique_name():
    return ''.join(global_random.choice(string.ascii_lowercase) for x in range(20))

@pytest.fixture(scope='module')
def cql_keyspace(cql, this_dc):
    name = unique_name()
    cql.execute("CREATE KEYSPACE " + name + " WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', '" + this_dc + "' : 1 }")
    yield name
    cql.execute("DROP KEYSPACE " + name)

@pytest.fixture(scope='module')
def cql_table(cql, cql_keyspace):
    cf = unique_name()
    cql.execute(f'CREATE TABLE {cql_keyspace}.{cf} (k INT PRIMARY KEY)')
    yield cf
    cql.execute(f'DROP TABLE {cql_keyspace}.{cf}')

# Check that a new Alternator table gets the same default configuration as
# a new CQL table - for a selection of configuration parameters like
# compression and speculative_retry. We don't check what it is (e.g., at the
# time of this writing, compression is LZ4 compression), but we want it to
# have the same default in Alternator as in CQL.
# Reproduces #26914 ('compression' difference).
@pytest.mark.parametrize('option',
    [pytest.param('compression', marks=pytest.mark.xfail(reason='#issue #26914')),
    'speculative_retry'])
def test_alternator_vs_cql(dynamodb, test_table, cql_keyspace, cql_table, option):
    alternator_schema = scylla_get_schema(dynamodb, cql_table_for(test_table))
    cql_schema = scylla_get_schema(dynamodb, (cql_keyspace, cql_table, False))
    assert alternator_schema[option] == cql_schema[option]
    # If you're curious what are the current default values of these options,
    # run this test with "-s" to see the print output.
    print(f'{option}: {alternator_schema[option]}')

# Fixture for an Alternator table with a GSI, LSI and Streams, allowing us
# to check the schemas of not just the Alternator table itself, but also
# its materialized views and CDC logs.
@pytest.fixture(scope='module')
def table1(dynamodb):
    with new_test_table(dynamodb,
        # Alternator Streams is expected to fail with tablets due to #23838.
        # To ensure that this test still runs, instead of xfailing it, we
        # temporarily coerce Alternator to avoid using default tablets
        # setting, even if it's available. Remove this "Tags=" line when
        # issue #23838 is solved.
        Tags=[{'Key': 'system:initial_tablets', 'Value': 'none'}],
        KeySchema=[
            # Must have both hash key and range key to allow LSI creation
            { 'AttributeName': 'p', 'KeyType': 'HASH' },
            { 'AttributeName': 'c', 'KeyType': 'RANGE' }
        ],
        AttributeDefinitions=[
            { 'AttributeName': 'p', 'AttributeType': 'S' },
            { 'AttributeName': 'c', 'AttributeType': 'S' },
            { 'AttributeName': 'x', 'AttributeType': 'S' },
        ],
        LocalSecondaryIndexes=[
            {   'IndexName': 'lsi_name',
                'KeySchema': [
                    { 'AttributeName': 'p', 'KeyType': 'HASH' },
                    { 'AttributeName': 'x', 'KeyType': 'RANGE' },
                ],
                'Projection': { 'ProjectionType': 'ALL' }
            }
        ],
        GlobalSecondaryIndexes=[
            {   'IndexName': 'gsi_name',
                'KeySchema': [{ 'AttributeName': 'x', 'KeyType': 'HASH' }],
                'Projection': { 'ProjectionType': 'ALL' }
            }
        ],
        StreamSpecification={
            'StreamEnabled': True, 'StreamViewType': 'KEYS_ONLY'
        }
        ) as table:
        yield table

# Check that Alternator's auxiliary tables - holding a GSI, LSI or CDC log,
# get the same configuration an Alternator base table. In the previous test
# (test_alternator_vs_cql) we already checked that the base-table
# configuration is the same as CQL's default.
@pytest.mark.parametrize('option',
    ['compression',
    'speculative_retry'])
def test_alternator_aux_tables(dynamodb, table1, option):
    alternator_base_schema = scylla_get_schema(dynamodb, cql_table_for(table1))
    # Check GSI table:
    alternator_gsi_schema = scylla_get_schema(dynamodb, cql_table_for_gsi(table1, 'gsi_name'))
    assert alternator_base_schema[option] == alternator_gsi_schema[option]
    # Check LSI table:
    alternator_lsi_schema = scylla_get_schema(dynamodb, cql_table_for_lsi(table1, 'lsi_name'))
    assert alternator_base_schema[option] == alternator_lsi_schema[option]
    # Check Streams log table:
    alternator_cdc_schema = scylla_get_schema(dynamodb, cql_table_for_cdclog(table1))
    assert alternator_base_schema[option] == alternator_cdc_schema[option]
