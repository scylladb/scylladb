# This file contains "test fixtures", a pytest concept described in
# https://docs.pytest.org/en/latest/fixture.html.
# A "fixture" is some sort of setup which an invididual test requires to run.
# The fixture has setup code and teardown code, and if multiple tests
# require the same fixture, it can be set up only once - while still allowing
# the user to run individual tests and automatically set up the fixtures they need.

import pytest
import boto3
import time

# Run tests with "--local" to run against a local Scylla installation on
# localhost:8080/ instead of AWS.
def pytest_addoption(parser):
    parser.addoption("--local", action="store_true",
        help="run against local Scylla installation instead of AWS")

# "dynamodb" fixture: set up client object for communicating with the DynamoDB
# API. Currently this chooses either Amazon's DynamoDB in the us-east-1 region
# or a local Alternator installation on http://localhost:8080 - depending on the
# existence of the "--local" option. In the future we should provide options
# for choosing other Amazon regions or local installations.
# We use scope="session" so that all tests will reuse the same client object.
@pytest.fixture(scope="session")
def dynamodb(request):
    if request.config.getoption('local'):
        return boto3.resource('dynamodb',endpoint_url='http://localhost:8000')
    else:
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

# "test_table" fixture: Create and return a temporary table to be used in tests
# that need a table to work on. The table is automatically deleted at the end.
# We use scope="session" so that all tests will reuse the same client object.
# This "test_table" creates a table which has a specific key schema: both a
# partition key and a sort key, and both are strings. Other fixtures (below)
# can be used to create different types of tables.
#
# TODO: Although we are careful about deleting temporary tables when the
# fixture is torn down, in some cases (e.g., interrupted tests) we can be left
# with some tables not deleted, and they will never be deleted. Because all
# our temporary tables have the same test_table_prefix, we can actually find
# and remove these old tables with this prefix. We can have a fixture, which
# test_table will require, which on teardown will delete all remaining tables
# (possibly from an older run). Because the table's name includes the current
# time, we can also remove just tables older than a particular age. Such
# mechanism will allow running tests in parallel, without the risk of deleting
# a parallel run's temporary tables.

def create_test_table(dynamodb, KeySchema, AttributeDefinitions):
    name = test_table_name()
    print("fixture creating new table {}".format(name))
    table = dynamodb.create_table(TableName=name,
        BillingMode='PAY_PER_REQUEST',
        KeySchema=KeySchema,
        AttributeDefinitions=AttributeDefinitions)
    waiter = table.meta.client.get_waiter('table_exists')
    # recheck every second instead of the default, lower, frequency. This can
    # save a few seconds on AWS with its very slow table creation, but can
    # more on tests on Scylla with its faster table creation turnaround.
    waiter.config.delay = 1
    waiter.config.max_attempts = 60
    waiter.wait(TableName=name)
    return table

@pytest.fixture(scope="session")
def test_table(dynamodb):
    table = create_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' },
                    { 'AttributeName': 'c', 'KeyType': 'RANGE' }
        ],
        AttributeDefinitions=[
                    { 'AttributeName': 'p', 'AttributeType': 'S' },
                    { 'AttributeName': 'c', 'AttributeType': 'S' },
        ])
    yield table
    # We get back here when this fixture is torn down. We ask Dynamo to delete
    # this table, but not wait for the deletion to complete. The next time
    # we create a test_table fixture, we'll choose a different table name
    # anyway.
    table.delete()

# The following fixtures test_table_* are similar to test_table but create
# tables with different key schemas.
@pytest.fixture(scope="session")
def test_table_s(dynamodb):
    table = create_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }, ],
        AttributeDefinitions=[ { 'AttributeName': 'p', 'AttributeType': 'S' } ])
    yield table
    table.delete()

@pytest.fixture(scope="session")
def test_2_tables(dynamodb):
    tables = [create_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' },
                    { 'AttributeName': 'c', 'KeyType': 'RANGE' }
        ],
        AttributeDefinitions=[
                    { 'AttributeName': 'p', 'AttributeType': 'S' },
                    { 'AttributeName': 'c', 'AttributeType': 'S' },
        ]) for _ in range(2)]
    yield tables
    [table.delete() for table in tables]

# "filled_test_table" fixture:  Create a temporary table to be used in tests
# that involve reading data - GetItem, Scan, etc. The table is filled with
# 328 items - each consisting of a partition key, clustering key and two
# string attributes. 164 of the items are in a single partition (with the
# partition key 'long') and the 164 other items are each in a separate
# partition. Finally, a 329th item is added with different attributes.
# This table is supposed to be read from, not updated nor overwritten.
# This fixture returns both a table object and the description of all items
# inserted into it.
@pytest.fixture(scope="session")
def filled_test_table(dynamodb):
    table = create_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' },
                    { 'AttributeName': 'c', 'KeyType': 'RANGE' }
        ],
        AttributeDefinitions=[
                    { 'AttributeName': 'p', 'AttributeType': 'S' },
                    { 'AttributeName': 'c', 'AttributeType': 'S' },
        ])
    count = 164
    items = [{
        'p': str(i),
        'c': str(i),
        'attribute': "x" * 7,
        'another': "y" * 16
    } for i in range(count)]
    items = items + [{
        'p': 'long',
        'c': str(i),
        'attribute': "x" * 7,
        'another': "y" * 16
    } for i in range(count)]
    items.append({'p': 'hello', 'c': 'world', 'str': 'and now for something completely different'})

    with table.batch_writer() as batch:
        for item in items:
            batch.put_item(item)

    yield table, items
    table.delete()
