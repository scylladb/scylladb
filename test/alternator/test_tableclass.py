# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

# Tests for the TableClass table option. See #10431.
# DynamoDB announced this option in December 2021:
# https://aws.amazon.com/blogs/aws/new-dynamodb-table-class-save-up-to-60-in-your-dynamodb-costs/
# TableClass can be STANDARD or STANDARD_INFREQUENT_ACCESS, where the former
# is the default and the latter gives lower storage costs and higher read and
# write costs - but everything else, including performance, remains the same.

import pytest
import time
from botocore.exceptions import ClientError
from test.alternator.util import new_test_table, full_query

# DescribeTable should return the table's table class in a "TableClassSummary"
# object. However, it turns out that a table that has the default TableClass
# and it was never explicitly set, doesn't report a TableClassSummary at all.
# This may have been a deliberate decision by the DynamoDB designers (so
# an old application that doesn't set TableClass in CreateTable also doesn't
# get it back in DescribeTable) - so let's implement it too.
def test_tableclass_describe_table_default(test_table):
    got = test_table.meta.client.describe_table(TableName=test_table.name)['Table']
    assert not 'TableClassSummary' in got

# Test CreateTable creating a table with the non-default TableClass
# "STANDARD_INFREQUENT_ACCESS".
@pytest.mark.xfail(reason="#10431 - TableClass not yet supported")
def test_tableclass_create_table_sia(dynamodb):
    schema = {
        'KeySchema': [{ 'AttributeName': 'p', 'KeyType': 'HASH' }],
        'AttributeDefinitions': [{ 'AttributeName': 'p', 'AttributeType': 'S'}],
        'TableClass' : 'STANDARD_INFREQUENT_ACCESS'
    }
    with new_test_table(dynamodb, **schema) as table:
        # Check that DescribeTable reports correctly that the new table is
        # in the STANDARD_INFREQUENT_ACCESS table class. TableClassSummary
        # should contain the expected TableClass, but NOT contain
        # LastUpdateDateTime (apparently it only gets set when UpdateTable
        # changes the TableClass).
        got = table.meta.client.describe_table(TableName=table.name)['Table']
        assert got['TableClassSummary'] == {
            'TableClass': 'STANDARD_INFREQUENT_ACCESS'
        }

# Test CreateTable creating a table with the default "STANDARD" TableClass
# explicitly specified.
@pytest.mark.xfail(reason="#10431 - TableClass not yet supported")
def test_tableclass_create_table_standard(dynamodb):
    schema = {
        'KeySchema': [{ 'AttributeName': 'p', 'KeyType': 'HASH' }],
        'AttributeDefinitions': [{ 'AttributeName': 'p', 'AttributeType': 'S'}],
        'TableClass' : 'STANDARD'
    }
    with new_test_table(dynamodb, **schema) as table:
        # Check that DescribeTable reports correctly that the new table is
        # in the STANDARD class. Curiously, although we saw above in
        # test_tableclass_describe_table_default that when the TableClass is
        # the default DescribeTable doesn't return a TableClassSummary
        # at all - here we discover that when TableClass *is* explicitly
        # passed in CreateTable, it is also returned in DescribeTable
        # even when it has the default value.
        got = table.meta.client.describe_table(TableName=table.name)['Table']
        assert got['TableClassSummary'] == {
            'TableClass': 'STANDARD'
        }

# Test that setting TableClass to unsupported names produces an error.
# This test also confirms that the TableClass string is cases sensitive -
# 'STANDARD' works (as we checked above) but lowercase 'standard' doesn't.
@pytest.mark.xfail(reason="#10431 - TableClass not yet supported")
def test_tableclass_create_table_bad_tableclass(dynamodb):
    for tableclass in ['invalid_tableclass_name', 'standard']:
        schema = {
            'KeySchema': [{ 'AttributeName': 'p', 'KeyType': 'HASH' }],
            'AttributeDefinitions': [{ 'AttributeName': 'p', 'AttributeType': 'S'}],
            'TableClass' : tableclass
        }
        # DynamoDB responds with the error: "Invalid table-class parameter
        # provided. Please try again with a valid table-class value:
        # [STANDARD, STANDARD_INFREQUENT_ACCESS]."
        with pytest.raises(ClientError, match='ValidationException.*table.class'):
            with new_test_table(dynamodb, **schema) as table:
                pass

# UpdateTable for changing the TableClass is an asynchronous operation.
# While this change happening, we cannot do other changes to the table
# or even delete it when the test ends - so we need to wait for the update
# to finish. While the update is happening, the table's TableStatus is
# changed from ACTIVE to UPDATING - and we need to wait for it to become
# ACTIVE again.
def wait_for_active(table):
    timeout = time.time() + 60
    while time.time() < timeout:
        desc = table.meta.client.describe_table(TableName=table.name)
        if desc['Table']['TableStatus'] == 'ACTIVE':
            return
        time.sleep(1)
    raise AssertionError('wait_for_active did not complete until timeout')

# UpdateTable allows changing the TableClass. Note that according to DynamoDB
# documentation explains how this change happens in practice, and how it's
# limited to only two changes per month:
#   "Table class updates is a background process. The time to update your
#    table class depends on your table traffic, storage size, and other
#    related variables. You can still access your table normally while it is
#    converted. Note that no more than two table class updates on your table
#    are allowed in a 30-day trailing period."
@pytest.mark.xfail(reason="#10431 - TableClass not yet supported")
def test_tableclass_update_table(dynamodb):
    schema = {
        'KeySchema': [{ 'AttributeName': 'p', 'KeyType': 'HASH' }],
        'AttributeDefinitions': [{ 'AttributeName': 'p', 'AttributeType': 'S'}],
        'TableClass' : 'STANDARD'
    }
    with new_test_table(dynamodb, **schema) as table:
        # The table started with STANDARD table class:
        got = table.meta.client.describe_table(TableName=table.name)['Table']
        assert got['TableClassSummary'] == {
            'TableClass': 'STANDARD'
        }
        # Update the table to STANDARD_INFREQUENT_ACCESS 
        table.meta.client.update_table(TableName=table.name, TableClass='STANDARD_INFREQUENT_ACCESS')
        wait_for_active(table)
        # DescribeTable should now list the new TableClass, and also the
        # LastUpdateDateTime of the change.
        got = table.meta.client.describe_table(TableName=table.name)['Table']
        assert got['TableClassSummary']['TableClass'] == 'STANDARD_INFREQUENT_ACCESS'
        assert 'LastUpdateDateTime' in got['TableClassSummary']
        # Update the table back to STANDARD. Should work (two changes are
        # allowed per day).
        table.meta.client.update_table(TableName=table.name, TableClass='STANDARD')
        wait_for_active(table)
        got = table.meta.client.describe_table(TableName=table.name)['Table']
        assert got['TableClassSummary']['TableClass'] == 'STANDARD'
        assert 'LastUpdateDateTime' in got['TableClassSummary']
        # Trying to change the table class to non-existent class 'junk'
        # should fail:
        with pytest.raises(ClientError, match='ValidationException.*table.class'):
            table.meta.client.update_table(TableName=table.name, TableClass='junk')

# Utility function for getting - from Scylla-specific system tables - the
# sstable compression options set for the given table.
def scylla_get_compression(dynamodb, table):
    info = dynamodb.Table('.scylla.alternator.system_schema.tables')
    # We need to read just a single key, but strangely Alternator doesn't
    # allow GetItem on system tables, just Query/Scan, so we use Query:
    res = full_query(info,
        KeyConditionExpression='keyspace_name=:ks and table_name=:cf',
        ExpressionAttributeValues={
            ':ks': 'alternator_'+table.name, ':cf' : table.name})
    assert len(res) == 1
    return res[0]['compression']

# A Scylla-only test for checking the internal implications of setting the
# TableClass. Here we check that the default (STANDARD) table class translates
# to using LZ4 compression in the sstables.
# If we ever change the implementation of what the different table classes
# mean, we'll need to change this test.
def test_tableclass_default_uses_lz4(dynamodb, test_table, scylla_only):
    assert 'LZ4Compressor' in scylla_get_compression(dynamodb, test_table)

# And here we check that the STANDARD_INFREQUENT_ACCESS table class translates
# to using ZSTD compression in the sstables.
# If we ever change the implementation of what the different table classes
# mean, we'll need to change this test.
def test_tableclass_sia_uses_zstd(dynamodb, test_table, scylla_only):
    schema = {
        'KeySchema': [{ 'AttributeName': 'p', 'KeyType': 'HASH' }],
        'AttributeDefinitions': [{ 'AttributeName': 'p', 'AttributeType': 'S'}],
        'TableClass' : 'STANDARD_INFREQUENT_ACCESS'
    }
    with new_test_table(dynamodb, **schema) as table:
        assert 'ZstdCompressor' in scylla_get_compression(dynamodb, table)
