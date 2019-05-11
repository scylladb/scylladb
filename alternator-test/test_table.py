# Tests for the basic table operations: CreateTable, DeleteTable, DescribeTable.

import pytest
from botocore.exceptions import ClientError
import re

# Utility function for create a table with a given name and some valid
# schema.. This function initiates the table's creation, but doesn't
# wait for the table to actually become ready.
def create_table(dynamodb, name):
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
def create_and_delete_table(dynamodb, name):
    table = create_table(dynamodb, name)
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

# DynamoDB's ListTables request returns up to a single page of table names
# (e.g., up to 100) and it is up to the caller to call it again and again
# to get the next page. This is a utility function which calls it repeatedly
# as much as necessary to get the entire list.
# We deliberately return a list and not a set, because we want the caller
# to be able to recognize bugs in ListTables which causes the same table
# to be returned twice.
def list_tables(dynamodb, limit):
    ret = []
    pos = None
    while True:
        if pos:
            page = dynamodb.meta.client.list_tables(Limit=limit, ExclusiveStartTableName=pos);
        else:
            page = dynamodb.meta.client.list_tables(Limit=limit);
        results = page.get('TableNames', None)
        assert(results)
        ret = ret + results
        newpos = page.get('LastEvaluatedTableName', None)
        if not newpos:
            break;
        # It doesn't make sense for Dynamo to tell us we need more pages, but
        # not send anything in *this* page!
        assert len(results) > 0
        assert newpos != pos
        # Note that we only checked that we got back tables, not that we got
        # any new tables not already in ret. So a buggy implementation might
        # still cause an endless loop getting the same tables again and again.
        pos = newpos
    return ret

# Test that all tables we create are listed, and pagination works properly.
# Note that the DyanamoDB setup we run this against may have hundreds of
# other tables, for all we know. We just need to check that the tables we
# created are indeed listed.
def test_list_tables_paginated(dynamodb, test_table, test_2_tables):
    my_tables_set = set([table.name for table in test_2_tables + [test_table]])
    for limit in [1, 2, 3, 4, 50, 100]:
        print("testing limit={}".format(limit))
        list_tables_set = set(list_tables(dynamodb, limit))
        assert my_tables_set.issubset(list_tables_set)

# Test that pagination limit is validated
def test_list_tables_wrong_limit(dynamodb):
    # lower limit (min. 1) is imposed by boto3 library checks
    with pytest.raises(ClientError, match='ValidationException'):
        dynamodb.meta.client.list_tables(Limit=101)
