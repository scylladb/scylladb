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

# Tests for basic table operations: CreateTable, DeleteTable, ListTables.

import pytest
from botocore.exceptions import ClientError
from util import list_tables, test_table_name, create_test_table, random_string

# Utility function for create a table with a given name and some valid
# schema.. This function initiates the table's creation, but doesn't
# wait for the table to actually become ready.
def create_table(dynamodb, name, BillingMode='PAY_PER_REQUEST', **kwargs):
    return dynamodb.create_table(
        TableName=name,
        BillingMode=BillingMode,
        KeySchema=[
            {
                'AttributeName': 'p',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'c',
                'KeyType': 'RANGE'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'p',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'c',
                'AttributeType': 'S'
            },
        ],
        **kwargs
    )

# Utility function for creating a table with a given name, and then deleting
# it immediately, waiting for these operations to complete. Since the wait
# uses DescribeTable, this function requires all of CreateTable, DescribeTable
# and DeleteTable to work correctly.
# Note that in DynamoDB, table deletion takes a very long time, so tests
# successfully using this function are very slow.
def create_and_delete_table(dynamodb, name, **kwargs):
    table = create_table(dynamodb, name, **kwargs)
    table.meta.client.get_waiter('table_exists').wait(TableName=name)
    table.delete()
    table.meta.client.get_waiter('table_not_exists').wait(TableName=name)

##############################################################################

# Test creating a table, and then deleting it, waiting for each operation
# to have completed before proceeding. Since the wait uses DescribeTable,
# this tests requires all of CreateTable, DescribeTable and DeleteTable to
# function properly in their basic use cases.
# Unfortunately, this test is extremely slow with DynamoDB because deleting
# a table is extremely slow until it really happens.
def test_create_and_delete_table(dynamodb):
    create_and_delete_table(dynamodb, 'alternator_test')

# Test that recreating a table right after deleting it works without issues
def test_recreate_table(dynamodb):
    create_and_delete_table(dynamodb, 'alternator_recr_test')
    create_and_delete_table(dynamodb, 'alternator_recr_test')

# DynamoDB documentation specifies that table names must be 3-255 characters,
# and match the regex [a-zA-Z0-9._-]+. Names not matching these rules should
# be rejected, and no table be created.
def test_create_table_unsupported_names(dynamodb):
    from botocore.exceptions import ParamValidationError, ClientError
    # Intererstingly, the boto library tests for names shorter than the
    # minimum length (3 characters) immediately, and failure results in
    # ParamValidationError. But the other invalid names are passed to
    # DynamoDB, which returns an HTTP response code, which results in a
    # CientError exception.
    with pytest.raises(ParamValidationError):
        create_table(dynamodb, 'n')
    with pytest.raises(ParamValidationError):
        create_table(dynamodb, 'nn')
    with pytest.raises(ClientError, match='ValidationException'):
        create_table(dynamodb, 'n' * 256)
    with pytest.raises(ClientError, match='ValidationException'):
        create_table(dynamodb, 'nyh@test')

# On the other hand, names following the above rules should be accepted. Even
# names which the Scylla rules forbid, such as a name starting with .
def test_create_and_delete_table_non_scylla_name(dynamodb):
    create_and_delete_table(dynamodb, '.alternator_test')

# names with 255 characters are allowed in Dynamo, but they are not currently
# supported in Scylla because we create a directory whose name is the table's
# name followed by 33 bytes (underscore and UUID). So currently, we only
# correctly support names with length up to 222.
def test_create_and_delete_table_very_long_name(dynamodb):
    # In the future, this should work:
    #create_and_delete_table(dynamodb, 'n' * 255)
    # But for now, only 222 works:
    create_and_delete_table(dynamodb, 'n' * 222)
    # We cannot test the following on DynamoDB because it will succeed
    # (DynamoDB allows up to 255 bytes)
    #with pytest.raises(ClientError, match='ValidationException'):
    #   create_table(dynamodb, 'n' * 223)

# Tests creating a table with an invalid schema should return a
# ValidationException error.
def test_create_table_invalid_schema(dynamodb):
    # The name of the table "created" by this test shouldn't matter, the
    # creation should not succeed anyway.
    with pytest.raises(ClientError, match='ValidationException'):
        dynamodb.create_table(
            TableName='name_doesnt_matter',
            BillingMode='PAY_PER_REQUEST',
            KeySchema=[
                { 'AttributeName': 'p', 'KeyType': 'HASH' },
                { 'AttributeName': 'c', 'KeyType': 'HASH' }
            ],
            AttributeDefinitions=[
                { 'AttributeName': 'p', 'AttributeType': 'S' },
                { 'AttributeName': 'c', 'AttributeType': 'S' },
            ],
        )
    with pytest.raises(ClientError, match='ValidationException'):
        dynamodb.create_table(
            TableName='name_doesnt_matter',
            BillingMode='PAY_PER_REQUEST',
            KeySchema=[
                { 'AttributeName': 'p', 'KeyType': 'RANGE' },
                { 'AttributeName': 'c', 'KeyType': 'RANGE' }
            ],
            AttributeDefinitions=[
                { 'AttributeName': 'p', 'AttributeType': 'S' },
                { 'AttributeName': 'c', 'AttributeType': 'S' },
            ],
        )
    with pytest.raises(ClientError, match='ValidationException'):
        dynamodb.create_table(
            TableName='name_doesnt_matter',
            BillingMode='PAY_PER_REQUEST',
            KeySchema=[
                { 'AttributeName': 'c', 'KeyType': 'RANGE' }
            ],
            AttributeDefinitions=[
                { 'AttributeName': 'c', 'AttributeType': 'S' },
            ],
        )
    with pytest.raises(ClientError, match='ValidationException'):
        dynamodb.create_table(
            TableName='name_doesnt_matter',
            BillingMode='PAY_PER_REQUEST',
            KeySchema=[
                { 'AttributeName': 'c', 'KeyType': 'HASH' },
                { 'AttributeName': 'p', 'KeyType': 'RANGE' },
                { 'AttributeName': 'z', 'KeyType': 'RANGE' }
            ],
            AttributeDefinitions=[
                { 'AttributeName': 'c', 'AttributeType': 'S' },
                { 'AttributeName': 'p', 'AttributeType': 'S' },
                { 'AttributeName': 'z', 'AttributeType': 'S' }
            ],
        )
    with pytest.raises(ClientError, match='ValidationException'):
        dynamodb.create_table(
            TableName='name_doesnt_matter',
            BillingMode='PAY_PER_REQUEST',
            KeySchema=[
                { 'AttributeName': 'c', 'KeyType': 'HASH' },
            ],
            AttributeDefinitions=[
                { 'AttributeName': 'z', 'AttributeType': 'S' }
            ],
        )
    with pytest.raises(ClientError, match='ValidationException'):
        dynamodb.create_table(
            TableName='name_doesnt_matter',
            BillingMode='PAY_PER_REQUEST',
            KeySchema=[
                { 'AttributeName': 'k', 'KeyType': 'HASH' },
            ],
            AttributeDefinitions=[
                { 'AttributeName': 'k', 'AttributeType': 'Q' }
            ],
        )

# Test that trying to create a table that already exists fails in the
# appropriate way (ResourceInUseException)
def test_create_table_already_exists(dynamodb, test_table):
    with pytest.raises(ClientError, match='ResourceInUseException'):
        create_table(dynamodb, test_table.name)

# Test that BillingMode error path works as expected - only the values
# PROVISIONED or PAY_PER_REQUEST are allowed. The former requires
# ProvisionedThroughput to be set, the latter forbids it.
# If BillingMode is outright missing, it defaults (as original
# DynamoDB did) to PROVISIONED so ProvisionedThroughput is allowed.
def test_create_table_billing_mode_errors(dynamodb, test_table):
    with pytest.raises(ClientError, match='ValidationException'):
        create_table(dynamodb, test_table_name(), BillingMode='unknown')
    # billing mode is case-sensitive
    with pytest.raises(ClientError, match='ValidationException'):
        create_table(dynamodb, test_table_name(), BillingMode='pay_per_request')
    # PAY_PER_REQUEST cannot come with a ProvisionedThroughput:
    with pytest.raises(ClientError, match='ValidationException'):
        create_table(dynamodb, test_table_name(),
            BillingMode='PAY_PER_REQUEST', ProvisionedThroughput={'ReadCapacityUnits': 10, 'WriteCapacityUnits': 10})
    # On the other hand, PROVISIONED requires ProvisionedThroughput:
    # By the way, ProvisionedThroughput not only needs to appear, it must
    # have both ReadCapacityUnits and WriteCapacityUnits - but we can't test
    # this with boto3, because boto3 has its own verification that if
    # ProvisionedThroughput is given, it must have the correct form.
    with pytest.raises(ClientError, match='ValidationException'):
        create_table(dynamodb, test_table_name(), BillingMode='PROVISIONED')
    # If BillingMode is completely missing, it defaults to PROVISIONED, so
    # ProvisionedThroughput is required
    with pytest.raises(ClientError, match='ValidationException'):
        dynamodb.create_table(TableName=test_table_name(),
            KeySchema=[{ 'AttributeName': 'p', 'KeyType': 'HASH' }],
            AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }])

# Even before Alternator gains full support for the DynamoDB stream API
# and CreateTable's StreamSpecification option, we should support the
# options which mean it is turned *off*.
def test_table_streams_off(dynamodb):
    # If StreamSpecification is given, but has StreamEnabled=false, it's as
    # if StreamSpecification was missing. StreamViewType isn't needed.
    table = create_test_table(dynamodb, StreamSpecification={'StreamEnabled': False},
        KeySchema=[{ 'AttributeName': 'p', 'KeyType': 'HASH' }],
        AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }]);
    table.delete();
    # DynamoDB doesn't allow StreamSpecification to be empty map - if it
    # exists, it must have a StreamEnabled
    # Unfortunately, new versions of boto3 doesn't let us pass this...
    #with pytest.raises(ClientError, match='ValidationException'):
    #    table = create_test_table(dynamodb, StreamSpecification={},
    #        KeySchema=[{ 'AttributeName': 'p', 'KeyType': 'HASH' }],
    #        AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }]);
    #    table.delete();
    # Unfortunately, boto3 doesn't allow us to pass StreamSpecification=None.
    # This is what we had in issue #5796.

@pytest.mark.xfail(reason="streams not yet implemented")
def test_table_streams_on(dynamodb):
    table = create_test_table(dynamodb,
        StreamSpecification={'StreamEnabled': True, 'StreamViewType': 'OLD_IMAGE'},
        KeySchema=[{ 'AttributeName': 'p', 'KeyType': 'HASH' }],
        AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }]);
    table.delete();

# Our first implementation had a special column name called "attrs" where
# we stored a map for all non-key columns. If the user tried to name one
# of the key columns with this same name, the result was a disaster - Scylla
# goes into a bad state after trying to write data with two updates to same-
# named columns.
special_column_name1 = 'attrs'
special_column_name2 = ':attrs'
@pytest.fixture(scope="session")
def test_table_special_column_name(dynamodb):
    table = create_test_table(dynamodb,
        KeySchema=[
            { 'AttributeName': special_column_name1, 'KeyType': 'HASH' },
            { 'AttributeName': special_column_name2, 'KeyType': 'RANGE' }
        ],
        AttributeDefinitions=[
            { 'AttributeName': special_column_name1, 'AttributeType': 'S' },
            { 'AttributeName': special_column_name2, 'AttributeType': 'S' },
        ],
    )
    yield table
    table.delete()
@pytest.mark.xfail(reason="special attrs column not yet hidden correctly")
def test_create_table_special_column_name(test_table_special_column_name):
    s = random_string()
    c = random_string()
    h = random_string()
    expected = {special_column_name1: s, special_column_name2: c, 'hello': h}
    test_table_special_column_name.put_item(Item=expected)
    got = test_table_special_column_name.get_item(Key={special_column_name1: s, special_column_name2: c}, ConsistentRead=True)['Item']
    assert got == expected

# Test that all tables we create are listed, and pagination works properly.
# Note that the DyanamoDB setup we run this against may have hundreds of
# other tables, for all we know. We just need to check that the tables we
# created are indeed listed.
def test_list_tables_paginated(dynamodb, test_table, test_table_s, test_table_b):
    my_tables_set = {table.name for table in [test_table, test_table_s, test_table_b]}
    for limit in [1, 2, 3, 4, 50, 100]:
        print("testing limit={}".format(limit))
        list_tables_set = set(list_tables(dynamodb, limit))
        assert my_tables_set.issubset(list_tables_set)

# Test that pagination limit is validated
def test_list_tables_wrong_limit(dynamodb):
    # lower limit (min. 1) is imposed by boto3 library checks
    with pytest.raises(ClientError, match='ValidationException'):
        dynamodb.meta.client.list_tables(Limit=101)
