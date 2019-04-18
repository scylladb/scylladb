# Tests for the basic table operations: CreateTable, DeleteTable, DescribeTable.

import pytest
import boto3

dynamodb = boto3.resource('dynamodb',region_name='us-east-1') 

# Utility function for create a table with a given name and some valid
# schema.. This function initiates the table's creation, but doesn't
# wait for the table to actually become ready.
def create_table(name):
    return dynamodb.create_table(
        TableName=name,
        BillingMode='PAY_PER_REQUEST',
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
    )

# Utility function for creating a table with a given name, and then deleting
# it immediately, waiting for these operations to complete. Since the wait
# uses DescribeTable, this function requires all of CreateTable, DescribeTable
# and DeleteTable to work correctly.
# Note that in DynamoDB, table deletion takes a very long time, so tests
# successfully using this function are very slow.
def create_and_delete_table(name):
    table = create_table(name)
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
def test_create_and_delete_table():
    create_and_delete_table('nyh_test')

# DynamoDB documentation specifies that table names must be 3-255 characters,
# and match the regex [a-zA-Z0-9._-]+. Names not matching these rules should
# be rejected, and no table be created.
def test_create_table_unsupported_names():
    from botocore.exceptions import ParamValidationError, ClientError
    # Intererstingly, the boto library tests for names shorter than the
    # minimum length (3 characters) immediately, and failure results in
    # ParamValidationError. But the other invalid names are passed to
    # DynamoDB, which returns an HTTP response code, which results in a
    # CientError exception.
    with pytest.raises(ParamValidationError):
        create_table('n')
    with pytest.raises(ParamValidationError):
        create_table('nn')
    with pytest.raises(ClientError, match='at most 255 characters long'):
        create_table('n' * 256)
    with pytest.raises(ClientError, match='validation error detected'):
        create_table('nyh@test')

# On the other hand, names following the above rules should be accepted. Even
# names which the Scylla rules forbid, such as a name starting with .
def test_create_and_delete_table_non_scylla_name():
    create_and_delete_table('.hello.there')

# TODO: check case sensitivity
