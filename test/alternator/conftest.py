# Copyright 2019-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

# This file contains "test fixtures", a pytest concept described in
# https://docs.pytest.org/en/latest/fixture.html.
# A "fixture" is some sort of setup which an individual test requires to run.
# The fixture has setup code and teardown code, and if multiple tests
# require the same fixture, it can be set up only once - while still allowing
# the user to run individual tests and automatically set up the fixtures they need.

import pytest
import boto3
import requests
import re

from test.pylib.report_plugin import ReportPlugin
from test.alternator.util import create_test_table, is_aws, scylla_log
from urllib.parse import urlparse
from functools import cache

# Test that the Boto libraries are new enough. These tests want to test a
# large variety of DynamoDB API features, and to do this we need a new-enough
# version of the the Boto libraries (boto3 and botocore) so that they can
# access all these API features.
# In particular, the BillingMode feature was added in botocore 1.12.54.
import botocore
import sys
from packaging.version import Version
if (Version(botocore.__version__) < Version('1.12.54')):
    pytest.exit("Your Boto library is too old. Please upgrade it,\ne.g. using:\n    sudo pip{} install --upgrade boto3".format(sys.version_info[0]))

# By default, tests run against a local Scylla installation on localhost:8080/.
# The "--aws" option can be used to run against Amazon DynamoDB in the us-east-1
# region.
def pytest_addoption(parser):
    parser.addoption("--aws", action="store_true",
        help="run against AWS instead of a local Scylla installation")
    parser.addoption("--https", action="store_true",
        help="communicate via HTTPS protocol on port 8043 instead of HTTP when"
            " running against a local Scylla installation")
    parser.addoption("--url", action="store",
        help="communicate with given URL instead of defaults")
    parser.addoption("--runveryslow", action="store_true",
        help="run tests marked veryslow instead of skipping them")
    # Used by the wrapper script only, not by pytest, added here so it appears
    # in --help output and so that pytest's argparser won't protest against its
    # presence.
    parser.addoption('--omit-scylla-output', action='store_true',
        help='Omit scylla\'s output from the test output')
    parser.addoption('--host', action='store', default='localhost',
        help='Scylla server host to connect to')
    parser.addoption('--mode', action='store', default='no_mode',
                     help='Scylla build mode. Tests can use it to adjust their behavior.')
    parser.addoption('--run_id', action='store', default=1,
                     help='Run id for the test run')
def pytest_configure(config):
    config.addinivalue_line("markers", "veryslow: mark test as very slow to run")
    config.pluginmanager.register(ReportPlugin())

def pytest_collection_modifyitems(config, items):
    if config.getoption("--runveryslow"):
        # --runveryslow given in cli: do not skip veryslow tests
        return
    skip_veryslow = pytest.mark.skip(reason="need --runveryslow option to run")
    for item in items:
        if "veryslow" in item.keywords:
            item.add_marker(skip_veryslow)

# When testing Alternator running with --alternator-enforce-authorization=1,
# we need to find a valid username and secret key to use in the connection.
# Alternator allows any CQL role as the username any CQL role, and the key
# is that role's password's salted hash. We can read a valid role/hash
# from the appropriate system table, but can't do it with Alternator (because
# we don't know yet the secret key!), so we need to do it with CQL.
@cache
def get_valid_alternator_role(url):
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider
    auth_provider = PlainTextAuthProvider(
        username='cassandra', password='cassandra')
    with (
        Cluster([urlparse(url).hostname], auth_provider=auth_provider,
            connect_timeout = 60, control_connection_timeout = 60) as cluster,
        cluster.connect() as session
    ):
        # Newer Scylla places the "roles" table in the "system" keyspace, but
        # older versions used "system_auth_v2" or "system_auth"
        for ks in ['system', 'system_auth_v2', 'system_auth']:
            try:
                # We could have looked for any role/salted_hash pair, but we
                # already know a role "cassandra" exists (we just used it to
                # connect to CQL!), so let's just use that role.
                role = 'cassandra'
                salted_hash = list(session.execute(f"SELECT salted_hash FROM {ks}.roles WHERE role = '{role}'"))[0].salted_hash
                if salted_hash is None:
                    break
                return (role, salted_hash)
            except:
                pass
    # If we couldn't find a valid role, let's hope that
    # alternator-enforce-authorization is not enabled so anything will work
    return ('unknown_user', 'unknown_secret')

# "dynamodb" fixture: set up client object for communicating with the DynamoDB
# API. Currently this chooses either Amazon's DynamoDB in the default region
# or a local Alternator installation on http://localhost:8080 - depending on the
# existence of the "--aws" option. In the future we should provide options
# for choosing other Amazon regions or local installations.
# We use scope="session" so that all tests will reuse the same client object.
@pytest.fixture(scope="session")
def dynamodb(request):
    # Disable boto3's client-side validation of parameters. This validation
    # only makes it impossible for us to test various error conditions,
    # because boto3 checks them before we can get the server to check them.
    boto_config = botocore.client.Config(parameter_validation=False)
    if request.config.getoption('aws'):
        return boto3.resource('dynamodb', config=boto_config)
    else:
        # Even though we connect to the local installation, Boto3 still
        # requires us to specify dummy region and credential parameters,
        # otherwise the user is forced to properly configure ~/.aws even
        # for local runs.
        if request.config.getoption('url') != None:
            local_url = request.config.getoption('url')
        elif request.config.getoption('host') is not None:
            # this argument needed for compatibility with PythonTestSuite without modifying the previous behavior
            local_url = f"http://{request.config.getoption('host')}:8000"
        else:
            local_url = 'https://localhost:8043' if request.config.getoption('https') else 'http://localhost:8000'
        # Disable verifying in order to be able to use self-signed TLS certificates
        verify = not request.config.getoption('https')
        user, secret = get_valid_alternator_role(local_url)
        return boto3.resource('dynamodb', endpoint_url=local_url, verify=verify,
            region_name='us-east-1', aws_access_key_id=user, aws_secret_access_key=secret,
            config=boto_config.merge(botocore.client.Config(retries={"max_attempts": 0}, read_timeout=300)))

def new_dynamodb_session(request, dynamodb):
    ses = boto3.Session()
    host = urlparse(dynamodb.meta.client._endpoint.host)
    conf = botocore.client.Config(parameter_validation=False)
    if request.config.getoption('aws'):
        return boto3.resource('dynamodb', config=conf)
    if host.hostname == 'localhost':
        conf = conf.merge(botocore.client.Config(retries={"max_attempts": 0}, read_timeout=300))
    user, secret = get_valid_alternator_role(dynamodb.meta.client._endpoint.host)
    return ses.resource('dynamodb', endpoint_url=dynamodb.meta.client._endpoint.host, verify=host.scheme != 'http',
        region_name='us-east-1', aws_access_key_id=user, aws_secret_access_key=secret,
        config=conf)

@pytest.fixture(scope="session")
def dynamodbstreams(request):
    # Disable boto3's client-side validation of parameters. This validation
    # only makes it impossible for us to test various error conditions,
    # because boto3 checks them before we can get the server to check them.
    boto_config = botocore.client.Config(parameter_validation=False)
    if request.config.getoption('aws'):
        return boto3.client('dynamodbstreams', config=boto_config)
    else:
        # Even though we connect to the local installation, Boto3 still
        # requires us to specify dummy region and credential parameters,
        # otherwise the user is forced to properly configure ~/.aws even
        # for local runs.
        if request.config.getoption('url') != None:
            local_url = request.config.getoption('url')
        elif request.config.getoption('host') is not None:
            # this argument needed for compatibility with PythonTestSuite without modifying the previous behavior
            local_url = f"http://{request.config.getoption('host')}:8000"
        else:
            local_url = 'https://localhost:8043' if request.config.getoption('https') else 'http://localhost:8000'
        # Disable verifying in order to be able to use self-signed TLS certificates
        verify = not request.config.getoption('https')
        user, secret = get_valid_alternator_role(local_url)
        return boto3.client('dynamodbstreams', endpoint_url=local_url, verify=verify,
            region_name='us-east-1', aws_access_key_id=user, aws_secret_access_key=secret,
            config=boto_config.merge(botocore.client.Config(retries={"max_attempts": 0}, read_timeout=300)))

# A function-scoped autouse=True fixture allows us to test after every test
# that the server is still alive - and if not report the test which crashed
# it and stop running any more tests.
@pytest.fixture(scope="function", autouse=True)
def dynamodb_test_connection(dynamodb, request, optional_rest_api):
    scylla_log(optional_rest_api, f'test/alternator: Starting {request.node.parent.name}::{request.node.name}', 'info')
    yield
    try:
        # We want to run a do-nothing DynamoDB command. The health-check
        # URL is the fastest one.
        url = dynamodb.meta.client._endpoint.host
        response = requests.get(url, verify=False)
        assert response.ok
    except:
        pytest.exit(f"Scylla appears to have crashed in test {request.node.parent.name}::{request.node.name}")
    scylla_log(optional_rest_api, f'test/alternator: Ended {request.node.parent.name}::{request.node.name}', 'info')


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
# test_table_s_2 has exactly the same schema as test_table_s, and is useful
# for tests which need two different tables with the same schema.
@pytest.fixture(scope="session")
def test_table_s_2(dynamodb):
    table = create_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }, ],
        AttributeDefinitions=[ { 'AttributeName': 'p', 'AttributeType': 'S' } ])
    yield table
    table.delete()
@pytest.fixture(scope="session")
def test_table_b(dynamodb):
    table = create_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }, ],
        AttributeDefinitions=[ { 'AttributeName': 'p', 'AttributeType': 'B' } ])
    yield table
    table.delete()
@pytest.fixture(scope="session")
def test_table_sb(dynamodb):
    table = create_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }, { 'AttributeName': 'c', 'KeyType': 'RANGE' } ],
        AttributeDefinitions=[ { 'AttributeName': 'p', 'AttributeType': 'S' }, { 'AttributeName': 'c', 'AttributeType': 'B' } ])
    yield table
    table.delete()
@pytest.fixture(scope="session")
def test_table_sn(dynamodb):
    table = create_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }, { 'AttributeName': 'c', 'KeyType': 'RANGE' } ],
        AttributeDefinitions=[ { 'AttributeName': 'p', 'AttributeType': 'S' }, { 'AttributeName': 'c', 'AttributeType': 'N' } ])
    yield table
    table.delete()
@pytest.fixture(scope="session")
def test_table_ss(dynamodb):
    table = create_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }, { 'AttributeName': 'c', 'KeyType': 'RANGE' } ],
        AttributeDefinitions=[ { 'AttributeName': 'p', 'AttributeType': 'S' }, { 'AttributeName': 'c', 'AttributeType': 'S' } ])
    yield table
    table.delete()

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
        'attribute': "x" * (1 + i % 7),
        'another': "y" * (1 + i % 16)
    } for i in range(count)]
    items.append({'p': 'hello', 'c': 'world', 'str': 'and now for something completely different'})

    with table.batch_writer() as batch:
        for item in items:
            batch.put_item(item)

    yield table, items
    table.delete()

# The "scylla_only" fixture can be used by tests for Scylla-only features,
# which do not exist on AWS DynamoDB. A test using this fixture will be
# skipped if running with "--aws".
@pytest.fixture(scope="session")
def scylla_only(dynamodb):
    if is_aws(dynamodb):
        pytest.skip('Scylla-only feature not supported by AWS')

# The "test_table_s_forbid_rmw" fixture is the same as test_table_s, except
# with the "forbid_rmw" write isolation mode. This is useful for verifying
# that writes that we think should not need a read-before-write in fact do
# not need it.
# Because forbid_rmw is a Scylla-only feature, this test is skipped when not
# running against Scylla.
@pytest.fixture(scope="session")
def test_table_s_forbid_rmw(dynamodb, scylla_only):
    table = create_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }, ],
        AttributeDefinitions=[ { 'AttributeName': 'p', 'AttributeType': 'S' } ])
    arn = table.meta.client.describe_table(TableName=table.name)['Table']['TableArn']
    table.meta.client.tag_resource(ResourceArn=arn, Tags=[{'Key': 'system:write_isolation', 'Value': 'forbid_rmw'}])
    yield table
    table.delete()

# A fixture allowing to make Scylla-specific REST API requests.
# If we're not testing Scylla, or the REST API port (10000) is not available,
# the test using this fixture will be skipped with a message about the REST
# API not being available.
@pytest.fixture(scope="session")
def rest_api(dynamodb, optional_rest_api):
    if optional_rest_api is None:
        pytest.skip('Cannot connect to Scylla REST API')
    return optional_rest_api
@pytest.fixture(scope="session")
def optional_rest_api(dynamodb):
    if is_aws(dynamodb):
        return None
    url = dynamodb.meta.client._endpoint.host
    # The REST API is on port 10000, and always http, not https.
    url = re.sub(r':[0-9]+(/|$)', ':10000', url)
    url = re.sub(r'^https:', 'http:', url)
    # Scylla's REST API does not have an official "ping" command,
    # so we just list the keyspaces as a (usually) short operation
    try:
        requests.get(f'{url}/column_family/name/keyspace', timeout=1).raise_for_status()
    except:
        return None
    return url

# Fixture to check once whether newly created Alternator tables use the
# tablet feature. It is used by the xfail_tablets and skip_tablets fixtures
# below to xfail or skip a test which is known to be failing with tablets.
# This is a temporary measure - eventually everything in Scylla should work
# correctly with tablets, and these fixtures can be removed.
@pytest.fixture(scope="session")
def has_tablets(dynamodb, test_table):
    # We rely on some knowledge of Alternator internals:
    # 1. For table with name X, Scylla creates a keyspace called alternator_X
    # 2. We can read a CQL system table using the ".scylla.alternator." prefix.
    info = dynamodb.Table('.scylla.alternator.system_schema.scylla_keyspaces')
    try:
        response = info.query(
            KeyConditions={'keyspace_name': {
                        'AttributeValueList': ['alternator_'+test_table.name],
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

@pytest.fixture(scope="function")
def xfail_tablets(request, has_tablets):
    if has_tablets:
        request.node.add_marker(pytest.mark.xfail(reason='Test expected to fail when Alternator tables use tablets'))

@pytest.fixture(scope="function")
def skip_tablets(has_tablets):
    if has_tablets:
        pytest.skip("Test may crash when Alternator tables use tablets")
