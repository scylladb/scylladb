# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

# Tests for how CQL's Role-Based Access Control (RBAC) commands - CREATE ROLE,
# GRANT, REVOKE, etc., can be used on Alternator for authentication and for
# authorization. For example if the low-level name of an Alternator table "x"
# is alternator_x.x, and a certain user is not granted permission to "modify"
# keyspace alternator_x, Alternator write requests (PutItem, UpdateItem,
# DeleteItem, BatchWriteItem) by that user will be denied.
#
# Because this file is all about testing the Scylla-only CQL-based RBAC,
# all tests in this file are skipped when running against Amazon DynamoDB.

import pytest
import boto3
from botocore.exceptions import ClientError
import time
from contextlib import contextmanager
from test.alternator.util import is_aws, unique_table_name

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, ConsistencyLevel, ExecutionProfile, EXEC_PROFILE_DEFAULT, NoHostAvailable
from cassandra.policies import RoundRobinPolicy
import re

# This file is all about testing RBAC as configured via CQL, so we need to
# connect to CQL to set these tests up. The "cql" fixture below enables that.
# If we're not testing Scylla, or the CQL port is not available on the same
# IP address as the Alternator IP address, a test using this fixture will
# be skipped with a message about the CQL API not being available.
@pytest.fixture(scope="module")
def cql(dynamodb):
    if is_aws(dynamodb):
        pytest.skip('Scylla-only CQL API not supported by AWS')
    url = dynamodb.meta.client._endpoint.host
    host, = re.search(r'.*://([^:]*):', url).groups()
    profile = ExecutionProfile(
        load_balancing_policy=RoundRobinPolicy(),
        consistency_level=ConsistencyLevel.LOCAL_QUORUM,
        serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL)
    cluster = Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: profile},
        contact_points=[host],
        port=9042,
        protocol_version=4,
        auth_provider=PlainTextAuthProvider(username='cassandra', password='cassandra'),
    )
    try:
        ret = cluster.connect()
        # "BEGIN BATCH APPLY BATCH" is the closest to do-nothing I could find
        ret.execute("BEGIN BATCH APPLY BATCH")
    except NoHostAvailable:
        pytest.skip('Could not connect to Scylla-only CQL API')
    yield ret
    cluster.shutdown()

# new_role() is a context manager for temporarily creating a new role with
# a unique name and returning its name and the secret key needed to connect
# to it with the DynamoDB API.
# The "login" and "superuser" flags are passed to the CREATE ROLE statement.
@contextmanager
def new_role(cql, login=True, superuser=False):
    # The role name is not a table's name but it doesn't matter. Because our
    # unique_table_name() uses (deliberately) a non-lower-case character, the
    # role name has to be quoted in double quotes when used in CQL below.
    role = unique_table_name()
    # The password set for the new role is identical to the user name (not
    # very secure ;-)) - but we later need to retrieve the "salted hash" of
    # this password, which serves in Alternator as the secret key of the role.
    cql.execute(f"CREATE ROLE \"{role}\" WITH PASSWORD = '{role}' AND SUPERUSER = {superuser} AND LOGIN = {login}")
    # Newer Scylla places the "roles" table in the "system" keyspace, but
    # older versions used "system_auth_v2" or "system_auth"
    key = None
    for ks in ['system', 'system_auth_v2', 'system_auth']:
        try:
            e = list(cql.execute(f"SELECT salted_hash FROM {ks}.roles WHERE role = '{role}'"))
            if e != []:
                key = e[0].salted_hash
                if key is not None:
                    break
        except:
            pass
    assert key is not None
    try:
        yield (role, key)
    finally:
        cql.execute(f'DROP ROLE "{role}"')

# Create a new DynamoDB API resource (connection object) similar to the
# existing "dynamodb" resource - but authenticating with the given role
# and key.
@contextmanager
def new_dynamodb(dynamodb, role, key):
    url = dynamodb.meta.client._endpoint.host
    config = dynamodb.meta.client._client_config
    verify = not url.startswith('https')
    ret = boto3.resource('dynamodb', endpoint_url=url, verify=verify,
        aws_access_key_id=role, aws_secret_access_key=key,
        region_name='us-east-1', config=config)
    try:
        yield ret
    finally:
        ret.meta.client.close()

# A basic test for creating a new role. The ListTables operation is allowed
# to any role, so it should work in the new role when given the right password
# and fail with the wrong password.
def test_new_role(dynamodb, cql):
    with new_role(cql) as (role, key):
        with new_dynamodb(dynamodb, role, key) as d:
            # ListTables should not fail (we don't care what is the result)
            d.meta.client.list_tables()
        # Trying to use the wrong key for the new role should fail to perform
        # any request. The new_dynamodb() function can't detect the error,
        # it is detected when attempting to perform a request with it.
        with new_dynamodb(dynamodb, role, 'wrongkey') as d:
            with pytest.raises(ClientError, match='UnrecognizedClientException'):
                d.meta.client.list_tables()

# A role without "login" permissions cannot be used to authenticate requests.
# Reproduces #19735.
def test_login_false(dynamodb, cql):
    with new_role(cql, login=False) as (role, key):
        with new_dynamodb(dynamodb, role, key) as d:
            with pytest.raises(ClientError, match='UnrecognizedClientException.*login=false'):
                d.meta.client.list_tables()
