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
from test.alternator.util import is_aws, unique_table_name, random_string
from functools import cache

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

# Quote an identifier if it needs to be double-quoted in CQL. Quoting is
# *not* needed if the identifier matches [a-z][a-z0-9_]*, otherwise it does.
# double-quotes ('"') in the string are doubled.
def maybe_quote(identifier):
    if re.match('^[a-z][a-z0-9_]*$', identifier):
        return identifier
    return '"' + identifier.replace('"', '""') + '"'

# Currently, some time may pass after calling GRANT or REVOKE until this
# change actually takes affect: There can be group0 delays as well as caching
# of the permissions up to permissions_validity_in_ms milliseconds.
# This is why we need authorized() and unauthorized() in tests below - these
# functions will retry the operation until it's authorized or not authorized.
# To make tests fast, the permissions_validity_in_ms parameter should
# be configured (e.g. test/cql-pytest/run.py) to be as low as possible.
# But these tests should handle any configured value, as authorized() and
# unauthorized() use exponential backoff until a long timeout.
#
# However, note that the long timeout means that a *failing* test will take
# a long time. This is a big problem for xfailing tests, so those should
# explicitly set timeout to a multiple of permissions_validitity_in_ms()
# (see below).
def authorized(fun, timeout=10):
    deadline = time.time() + timeout
    sleep = 0.001
    while time.time() < deadline:
        try:
            return fun()
        except ClientError as e:
            if e.response['Error']['Code'] != 'AccessDeniedException':
                raise
            time.sleep(sleep)
            sleep *= 1.5
    return fun()

def unauthorized(fun, timeout=10):
    deadline = time.time() + timeout
    sleep = 0.001
    while time.time() < deadline:
        try:
            fun()
            time.sleep(sleep)
            sleep *= 1.5
        except ClientError as e:
            if e.response['Error']['Code'] == 'AccessDeniedException':
                return
            raise
    try:
        fun()
        pytest.fail(f'No AccessDeniedException until timeout of {timeout}')
    except ClientError as e:
        if e.response['Error']['Code'] == 'AccessDeniedException':
            return
        raise

@cache
def permissions_validity_in_ms(cql):
    return 0.001 * int(list(cql.execute("SELECT value FROM system.config WHERE name = 'permissions_validity_in_ms'"))[0][0])

# Convenience context manager for temporarily GRANTing some permission and
# then revoking it.
@contextmanager
def temporary_grant(cql, permission, resource, role):
    role = maybe_quote(role)
    cql.execute(f"GRANT {permission} ON {resource} TO {role}")
    try:
        yield
    finally:
        cql.execute(f"REVOKE {permission} ON {resource} FROM {role}")

@contextmanager
def temporary_grant_role(cql, role_src, role_dst):
    role_src = maybe_quote(role_src)
    role_dst = maybe_quote(role_dst)
    cql.execute(f"GRANT {role_src} TO {role_dst}")
    try:
        yield
    finally:
        cql.execute(f"REVOKE {role_src} FROM {role_dst}")

# Convenience function for getting the full CQL table name (ksname.cfname)
# for the given Alternator table. This uses our insider knowledge that
# table named "x" is stored in keyspace called "alternator_x", and if we
# ever change this we'll need to change this function too.
def cql_table_name(tab):
    return maybe_quote('alternator_' + tab.name) + '.' + maybe_quote(tab.name)

def cql_keyspace_name(tab):
    return maybe_quote('alternator_' + tab.name)

# Test GetItem's support of permissions.
# A fresh new role has no permissions to read a table, and we can allow it
# by granting a "SELECT" permission on the specific table, keyspace, or all
# keyspaces, or by assigning another role with these permissions to the given
# role.
# Because the GetItem test is the first, we check in this case all these
# cases like role inheritence. Once we know role inheritence works, the
# following tests for other DynamoDB API operations will not need to repeat
# them again and again.
def test_rbac_getitem(dynamodb, cql, test_table_s):
    p = random_string()
    v = random_string()
    item = {'p': p, 'v': v}
    test_table_s.put_item(Item=item)
    # Sanity check: we can read the item we just wrote using the superuser
    # role.
    assert item == test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']
    # If we now create a fresh new role, it can't read the item:
    with new_role(cql) as (role, key):
        with new_dynamodb(dynamodb, role, key) as d:
            tab = d.Table(test_table_s.name)
            unauthorized(lambda: tab.get_item(Key={'p': p}, ConsistentRead=True))
            # If we now add the permissions to read this table, GetItem
            # will work:
            with temporary_grant(cql, 'SELECT', cql_table_name(tab), role):
                assert item == authorized(lambda: tab.get_item(Key={'p': p}, ConsistentRead=True)['Item'])
            # After revoking the temporary permission grant, we get access
            # denied again:
            unauthorized(lambda: tab.get_item(Key={'p': p}, ConsistentRead=True))
            # Check that granting permissions to the entire keyspace also works:
            with temporary_grant(cql, 'SELECT', 'KEYSPACE ' + cql_keyspace_name(tab), role):
                assert item == authorized(lambda: tab.get_item(Key={'p': p}, ConsistentRead=True)['Item'])
            unauthorized(lambda: tab.get_item(Key={'p': p}, ConsistentRead=True))
            # check that granting permissions to all keyspaces also works:
            with temporary_grant(cql, 'SELECT', 'ALL KEYSPACES', role):
                assert item == authorized(lambda: tab.get_item(Key={'p': p}, ConsistentRead=True)['Item'])
            unauthorized(lambda: tab.get_item(Key={'p': p}, ConsistentRead=True))
            # Test an inherited role: Create a second role, give that
            # second role permission to read this table, give the first
            # role the permissions of the second role - and see that the
            # first role can read the table.
            with new_role(cql, login=False) as (role2, _):
                with temporary_grant(cql, 'SELECT', cql_table_name(tab), role2):
                    with temporary_grant_role(cql, role2, role):
                        assert item == authorized(lambda: tab.get_item(Key={'p': p}, ConsistentRead=True)['Item'])
            unauthorized(lambda: tab.get_item(Key={'p': p}, ConsistentRead=True))

# Test PutItem's support of permissions.
# PutItem, and other data-modifying operations (DeleteItem, UpdateItem, etc.)
# usually only write, and require the "MODIFY" permission to do that.
# But these operations can also be asked to read old values from the table
# by using ReturnValues=ALL_OLD, so we have a separate test for this case
# (checking which permissions it requires) below.
def test_rbac_putitem_write(dynamodb, cql, test_table_s):
    p = random_string()
    # In a new role without permissions, we can't write an item:
    with new_role(cql) as (role, key):
        with new_dynamodb(dynamodb, role, key) as d:
            tab = d.Table(test_table_s.name)
            v = random_string()
            unauthorized(lambda: tab.put_item(Item={'p': p, 'v': v}))
            # If we now add the permissions to MODIFY this table, PutItem
            # will work:
            with temporary_grant(cql, 'MODIFY', cql_table_name(tab), role):
                v = random_string()
                authorized(lambda: tab.put_item(Item={'p': p, 'v': v}))
                # Let's verify that put_item not only didn't fail, it actually
                # did the right thing. This check is quite redundant - we
                # already know from many other tests that if we have
                # permission to do PutItem, it works correctly. So let's
                # check it just once here but not do it again in other
                # tests for other operations below.
                # We don't yet have permissions to read the item back:
                unauthorized(lambda: tab.get_item(Key={'p': p}, ConsistentRead=True))
                # Let's also add SELECT permissions to read the item back:
                with temporary_grant(cql, 'SELECT', cql_table_name(tab), role):
                    assert {'p': p, 'v': v} == authorized(lambda: tab.get_item(Key={'p': p}, ConsistentRead=True)['Item'])
            # After revoking the temporary permission grant of MODIFY, we get
            # access denied again on PutItem:
            unauthorized(lambda: tab.put_item(Item={'p': p, 'v': v}))

# As explained above, this test confirms that even when PutItem *reads*
# an item (by using ReturnValues=ALL_OLD), it still requires only the MODIFY
# permission and not SELECT.
def test_rbac_putitem_read(dynamodb, cql, test_table_s):
    p = random_string()
    v1 = random_string()
    test_table_s.put_item(Item={'p': p, 'v': v1})
    with new_role(cql) as (role, key):
        with new_dynamodb(dynamodb, role, key) as d:
            tab = d.Table(test_table_s.name)
            v2 = random_string()
            unauthorized(lambda: tab.put_item(Item={'p': p, 'v': v2}))
            # With just the MODIFY permission, not SELECT permission, we
            # can PutItem with ReturnValues=ALL_OLD and read the item:
            with temporary_grant(cql, 'MODIFY', cql_table_name(tab), role):
                ret = authorized(lambda: tab.put_item(Item={'p': p, 'v': v2}, ReturnValues='ALL_OLD'))
                assert ret['Attributes'] == {'p': p, 'v': v1}
    assert {'p': p, 'v': v2} == authorized(lambda: test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'])
