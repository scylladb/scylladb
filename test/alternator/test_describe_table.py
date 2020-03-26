# Copyright 2019 ScyllaDB
#
# This file is part of Scylla.
#
# Scylla is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Scylla is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with Scylla.  If not, see <http://www.gnu.org/licenses/>.

# Tests for the DescribeTable operation.
# Some attributes used only by a specific major feature will be tested
# elsewhere:
#  1. Tests for describing tables with global or local secondary indexes
#     (the GlobalSecondaryIndexes and LocalSecondaryIndexes attributes)
#     are in test_gsi.py and test_lsi.py.
#  2. Tests for the stream feature (LatestStreamArn, LatestStreamLabel,
#     StreamSpecification) will be in the tests devoted to the stream
#     feature.
#  3. Tests for describing a restored table (RestoreSummary, TableId)
#     will be together with tests devoted to the backup/restore feature.

import pytest
from botocore.exceptions import ClientError
import re
import time
from util import multiset

# Test that DescribeTable correctly returns the table's name and state
def test_describe_table_basic(test_table):
    got = test_table.meta.client.describe_table(TableName=test_table.name)['Table']
    assert got['TableName'] == test_table.name
    assert got['TableStatus'] == 'ACTIVE'

# Test that DescribeTable correctly returns the table's schema, in
# AttributeDefinitions and KeySchema attributes
def test_describe_table_schema(test_table):
    got = test_table.meta.client.describe_table(TableName=test_table.name)['Table']
    expected = { # Copied from test_table()'s fixture
        'KeySchema': [ { 'AttributeName': 'p', 'KeyType': 'HASH' },
                    { 'AttributeName': 'c', 'KeyType': 'RANGE' }
        ],
        'AttributeDefinitions': [
                    { 'AttributeName': 'p', 'AttributeType': 'S' },
                    { 'AttributeName': 'c', 'AttributeType': 'S' },
        ]
    }
    assert got['KeySchema'] == expected['KeySchema']
    # The list of attribute definitions may be arbitrarily reordered
    assert multiset(got['AttributeDefinitions']) == multiset(expected['AttributeDefinitions'])

# Test that DescribeTable correctly returns the table's billing mode,
# in the BillingModeSummary attribute.
def test_describe_table_billing(test_table):
    got = test_table.meta.client.describe_table(TableName=test_table.name)['Table']
    assert got['BillingModeSummary']['BillingMode'] == 'PAY_PER_REQUEST'
    # The BillingModeSummary should also contain a
    # LastUpdateToPayPerRequestDateTime attribute, which is a date.
    # We don't know what date this is supposed to be, but something we
    # do know is that the test table was created already with this billing
    # mode, so the table creation date should be the same as the billing
    # mode setting date.
    assert 'LastUpdateToPayPerRequestDateTime' in got['BillingModeSummary']
    assert got['BillingModeSummary']['LastUpdateToPayPerRequestDateTime'] == got['CreationDateTime']

# Test that DescribeTable correctly returns the table's creation time.
# We don't know what this creation time is supposed to be, so this test
# cannot be very thorough... We currently just tests against something we
# know to be wrong - returning the *current* time, which changes on every
# call.
@pytest.mark.xfail(reason="DescribeTable does not return table creation time")
def test_describe_table_creation_time(test_table):
    got = test_table.meta.client.describe_table(TableName=test_table.name)['Table']
    assert 'CreationDateTime' in got
    time1 = got['CreationDateTime']
    time.sleep(1) 
    got = test_table.meta.client.describe_table(TableName=test_table.name)['Table']
    time2 = got['CreationDateTime']
    assert time1 == time2

# Test that DescribeTable returns the table's estimated item count
# in the ItemCount attribute. Unfortunately, there's not much we can
# really test here... The documentation says that the count can be
# delayed by six hours, so the number we get here may have no relation
# to the current number of items in the test table. The attribute should exist,
# though. This test does NOT verify that ItemCount isn't always returned as
# zero - such stub implementation will pass this test.
@pytest.mark.xfail(reason="DescribeTable does not return table item count")
def test_describe_table_item_count(test_table):
    got = test_table.meta.client.describe_table(TableName=test_table.name)['Table']
    assert 'ItemCount' in got

# Similar test for estimated size in bytes - TableSizeBytes - which again,
# may reflect the size as long as six hours ago.
@pytest.mark.xfail(reason="DescribeTable does not return table size")
def test_describe_table_size(test_table):
    got = test_table.meta.client.describe_table(TableName=test_table.name)['Table']
    assert 'TableSizeBytes' in got

# Test the ProvisionedThroughput attribute returned by DescribeTable.
# This is a very partial test: Our test table is configured without
# provisioned throughput, so obviously it will not have interesting settings
# for it. DynamoDB returns zeros for some of the attributes, even though
# the documentation suggests missing values should have been fine too.
@pytest.mark.xfail(reason="DescribeTable does not return provisioned throughput")
def test_describe_table_provisioned_throughput(test_table):
    got = test_table.meta.client.describe_table(TableName=test_table.name)['Table']
    assert got['ProvisionedThroughput']['NumberOfDecreasesToday'] == 0
    assert got['ProvisionedThroughput']['WriteCapacityUnits'] == 0
    assert got['ProvisionedThroughput']['ReadCapacityUnits'] == 0

# This is a silly test for the RestoreSummary attribute in DescribeTable -
# it should not exist in a table not created by a restore. When testing
# the backup/restore feature, we will have more meaninful tests for the
# value of this attribute in that case.
def test_describe_table_restore_summary(test_table):
    got = test_table.meta.client.describe_table(TableName=test_table.name)['Table']
    assert not 'RestoreSummary' in got

# This is a silly test for the SSEDescription attribute in DescribeTable -
# by default, a table is encrypted with AWS-owned keys, not using client-
# owned keys, and the SSEDescription attribute is not returned at all.
def test_describe_table_encryption(test_table):
    got = test_table.meta.client.describe_table(TableName=test_table.name)['Table']
    assert not 'SSEDescription' in got

# This is a silly test for the StreamSpecification attribute in DescribeTable -
# when there are no streams, this attribute should be missing.
def test_describe_table_stream_specification(test_table):
    got = test_table.meta.client.describe_table(TableName=test_table.name)['Table']
    assert not 'StreamSpecification' in got

# Test that the table has an ARN, a unique identifier for the table which
# includes which zone it is on, which account, and of course the table's
# name. The ARN format is described in
# https://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html#genref-arns
def test_describe_table_arn(test_table):
    got = test_table.meta.client.describe_table(TableName=test_table.name)['Table']
    assert 'TableArn' in got and got['TableArn'].startswith('arn:')

# Test that the table has a TableId.
# TODO: Figure out what is this TableId supposed to be, it is just a
# unique id that is created with the table and never changes? Or anything
# else?
@pytest.mark.xfail(reason="DescribeTable does not return TableId")
def test_describe_table_id(test_table):
    got = test_table.meta.client.describe_table(TableName=test_table.name)['Table']
    assert 'TableId' in got

# DescribeTable error path: trying to describe a non-existent table should
# result in a ResourceNotFoundException.
def test_describe_table_non_existent_table(dynamodb):
    with pytest.raises(ClientError, match='ResourceNotFoundException') as einfo:
        dynamodb.meta.client.describe_table(TableName='non_existent_table')
    # As one of the first error-path tests that we wrote, let's test in more
    # detail that the error reply has the appropriate fields:
    response = einfo.value.response
    print(response)
    err = response['Error']
    assert err['Code'] == 'ResourceNotFoundException'
    assert re.match(err['Message'], 'Requested resource not found: Table: non_existent_table not found')
