# This file contains "test fixtures", a pytest concept described in
# https://docs.pytest.org/en/latest/fixture.html.
# A "fixture" is some sort of setup which an invididual test requires to run.
# The fixture has setup code and teardown code, and if multiple tests
# require the same fixture, it can be set up only once - while still allowing
# the user to run individual tests and automatically set up the fixtures they need.



import pytest
import boto3
import time

# "dynamodb" fixture: set up client object for communicating with the DynamoDB
# API. Currently, this connects to the real Amazon on us-east-1, but we should
# make it optional whether to connect to AWS or a local Scylla installation.
# We use scope="session" so that all tests will reuse the same client object.
@pytest.fixture(scope="session")
def dynamodb():
    return boto3.resource('dynamodb',region_name='us-east-1') 

test_table_prefix = 'alternator_test_'
def test_table_name():
    current_ms = int(round(time.time() * 1000))
    # In the off chance that test_table_name() is called twice in the same millisecond...
    if test_table_name.last_ms >= current_ms:
        current_ms = test_table_name.last_ms + 1
    test_table_name.last_ms = current_ms
    return test_table_prefix + str(current_ms)
test_table_name.last_ms = 0

# "test_table" fixture:
# TODO: explain: creates a table with a certain schema.
# TODO: require a fixture which on teardown removes all tables with a particular
# prefix.
# We use scope="session" so that all tests will reuse the same client object.
@pytest.fixture(scope="session")
def test_table(dynamodb):
    name = test_table_name()
    print("test_table() fixture creating new table {}".format(name))
    table = dynamodb.create_table(
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
    table.meta.client.get_waiter('table_exists').wait(TableName=name)
    yield table
    # We get back here when this fixture is torn down. We ask Dynamo to delete
    # this table, but not wait for the deletion to complete. The next time
    # we create a test_table fixture, we'll choose a different table name
    # anyway.
    table.delete()
