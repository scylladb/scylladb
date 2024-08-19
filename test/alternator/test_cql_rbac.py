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
from test.alternator.util import is_aws, unique_table_name, random_string, new_test_table
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

@contextmanager
def new_dynamodb_streams(dynamodb, role, key):
    url = dynamodb.meta.client._endpoint.host
    config = dynamodb.meta.client._client_config
    verify = not url.startswith('https')
    ret = boto3.client('dynamodbstreams', endpoint_url=url, verify=verify,
        aws_access_key_id=role, aws_secret_access_key=key,
        region_name='us-east-1', config=config)
    try:
        yield ret
    finally:
        ret.close()

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

def cql_gsi_name(tab, gsi):
    return maybe_quote('alternator_' + tab.name) + '.' + maybe_quote(tab.name + ":" + gsi)

def cql_cdclog_name(tab):
    return maybe_quote('alternator_' + tab.name) + '.' + maybe_quote(tab.name + "_scylla_cdc_log")

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

# Test DeleteItem's support of permissions.
# As PutItem above, DeleteItem requires the "MODIFY" permission, both for
# its usual write-only operation and also for ReturnValues=ALL_OLD
def test_rbac_deleteitem_write(dynamodb, cql, test_table_s):
    p = random_string()
    v = random_string()
    test_table_s.put_item(Item={'p': p, 'v': v})
    with new_role(cql) as (role, key):
        with new_dynamodb(dynamodb, role, key) as d:
            tab = d.Table(test_table_s.name)
            unauthorized(lambda: tab.delete_item(Key={'p': p}))
            with temporary_grant(cql, 'MODIFY', cql_table_name(tab), role):
                authorized(lambda: tab.delete_item(Key={'p': p}))
    assert not 'Item' in test_table_s.get_item(Key={'p': p}, ConsistentRead=True)

def test_rbac_deleteitem_read(dynamodb, cql, test_table_s):
    p = random_string()
    v = random_string()
    test_table_s.put_item(Item={'p': p, 'v': v})
    with new_role(cql) as (role, key):
        with new_dynamodb(dynamodb, role, key) as d:
            tab = d.Table(test_table_s.name)
            unauthorized(lambda: tab.delete_item(Key={'p': p}))
            # With just the MODIFY permission, not SELECT permission, we
            # can DeleteItem with ReturnValues=ALL_OLD and read the item:
            with temporary_grant(cql, 'MODIFY', cql_table_name(tab), role):
                ret = authorized(lambda: tab.delete_item(Key={'p': p}, ReturnValues='ALL_OLD'))
                assert ret['Attributes'] == {'p': p, 'v': v}
    assert not 'Item' in test_table_s.get_item(Key={'p': p}, ConsistentRead=True)

# Test UpdateItem's support of permissions.
# As PutItem above, UpdateItem requires the "MODIFY" permission, both for
# its usual write-only operation and also read-modify-write and even for
# ReturnValues=ALL_OLD.
def test_rbac_updateitem_write(dynamodb, cql, test_table_s):
    p = random_string()
    v = random_string()
    with new_role(cql) as (role, key):
        with new_dynamodb(dynamodb, role, key) as d:
            tab = d.Table(test_table_s.name)
            unauthorized(lambda: tab.update_item(Key={'p': p},
                UpdateExpression='SET v = :val',
                ExpressionAttributeValues={':val': v}))
            with temporary_grant(cql, 'MODIFY', cql_table_name(tab), role):
                authorized(lambda: tab.update_item(Key={'p': p},
                    UpdateExpression='SET v = :val',
                    ExpressionAttributeValues={':val': v}))
    assert {'p': p, 'v': v} == test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']

def test_rbac_updateitem_read(dynamodb, cql, test_table_s):
    p = random_string()
    v1 = random_string()
    test_table_s.put_item(Item={'p': p, 'v': v1})
    with new_role(cql) as (role, key):
        with new_dynamodb(dynamodb, role, key) as d:
            tab = d.Table(test_table_s.name)
            v2 = 42
            unauthorized(lambda: tab.update_item(Key={'p': p},
                UpdateExpression='SET v = :val',
                ExpressionAttributeValues={':val': v2},
                ReturnValues='ALL_OLD'))
            # With just the MODIFY permission, not SELECT permission, we
            # can UpdateItem with ReturnValues=ALL_OLD and read the item:
            with temporary_grant(cql, 'MODIFY', cql_table_name(tab), role):
                ret = authorized(lambda: tab.update_item(Key={'p': p},
                    UpdateExpression='SET v = :val',
                    ExpressionAttributeValues={':val': v2},
                    ReturnValues='ALL_OLD'))
                assert ret['Attributes'] == {'p': p, 'v': v1}
                # Just MODIFY permission, not SELECT permission, also allows
                # us to do a read-modify-write expression:
                ret = authorized(lambda: tab.update_item(Key={'p': p},
                    UpdateExpression='SET v =  v + :val',
                    ExpressionAttributeValues={':val': 1}))
    assert {'p': p, 'v': v2 + 1} == test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']

# Test Query's support of permissions - the "SELECT" permission is needed.
def test_rbac_query(dynamodb, cql, test_table):
    p = random_string()
    c = random_string()
    item = {'p': p, 'c': c}
    test_table.put_item(Item=item)
    with new_role(cql) as (role, key):
        with new_dynamodb(dynamodb, role, key) as d:
            tab = d.Table(test_table.name)
            # Without SELECT permissions, the Query will fail:
            unauthorized(lambda: tab.query(ConsistentRead=True,
                KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}}))
            # Adding SELECT permissions, the Query will succeed:
            with temporary_grant(cql, 'SELECT', cql_table_name(tab), role):
                assert [item] == authorized(lambda: tab.query(ConsistentRead=True,
                        KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}})['Items'])

# When reading with Query (or Scan), the IndexName option can ask to read
# from a view (GSI or LSI) instead of from the base table. We decided that
# the base table and each individual view can have *separate* permissions,
# so granting SELECT permission on the base table does not allow reading
# a view, and vice versa. This test verifies this.
# Note that if a table is created *by* a role, the auto-grant feature
# (tested below) ensures that this role can read the base and all views.
# So here the table is not created by the role - but rather by the superuser,
# and the permissions are granted explicitly to the role on a specific table.
def test_rbac_query_separate_index_permissions(dynamodb, cql):
    schema = {
        'KeySchema': [ { 'AttributeName': 'p', 'KeyType': 'HASH' } ],
        'AttributeDefinitions': [
                    { 'AttributeName': 'p', 'AttributeType': 'S' },
                    { 'AttributeName': 'x', 'AttributeType': 'S' } ],
        'GlobalSecondaryIndexes': [
            {   'IndexName': 'hello',
                'KeySchema': [
                    { 'AttributeName': 'x', 'KeyType': 'HASH' },
                ],
                'Projection': { 'ProjectionType': 'ALL' }
            } ]
    }
    with new_test_table(dynamodb, **schema) as table:
        with new_role(cql) as (role, key):
            with new_dynamodb(dynamodb, role, key) as d:
                tab = d.Table(table.name)
                # Without extra permissions, the new role can read neither
                # the base table nore the view:
                unauthorized(lambda: tab.query(KeyConditions={'p': {'AttributeValueList': ['hi'], 'ComparisonOperator': 'EQ'}}))
                unauthorized(lambda: tab.query(IndexName='hello', KeyConditions={'x': {'AttributeValueList': ['hi'], 'ComparisonOperator': 'EQ'}}))
                # Granting permissions on the base table we can read the
                # base table but NOT the view:
                with temporary_grant(cql, 'SELECT', cql_table_name(tab), role):
                    authorized(lambda: tab.query(KeyConditions={'p': {'AttributeValueList': ['hi'], 'ComparisonOperator': 'EQ'}}))
                    unauthorized(lambda: tab.query(IndexName='hello', KeyConditions={'x': {'AttributeValueList': ['hi'], 'ComparisonOperator': 'EQ'}}))
                # Granting permissions on the GSI we can read the GSI but not
                # the base table:
                with temporary_grant(cql, 'SELECT', cql_gsi_name(tab, 'hello'), role):
                    unauthorized(lambda: tab.query(KeyConditions={'p': {'AttributeValueList': ['hi'], 'ComparisonOperator': 'EQ'}}))
                    authorized(lambda: tab.query(IndexName='hello', KeyConditions={'x': {'AttributeValueList': ['hi'], 'ComparisonOperator': 'EQ'}}))

# Test Scan's support of permissions - the "SELECT" permission is needed.
def test_rbac_scan(dynamodb, cql, test_table):
    # We will Scan an existing (empty or not empty) table with Limit=1
    # just to check if Scan is`allowed or not - we won't check the results'
    # correctness - there are plenty of other tests for that.
    with new_role(cql) as (role, key):
        with new_dynamodb(dynamodb, role, key) as d:
            tab = d.Table(test_table.name)
            # Without SELECT permissions, the Scan will fail:
            unauthorized(lambda: tab.scan(ConsistentRead=True, Limit=1))
            # Adding SELECT permissions, the Scan will succeed:
            with temporary_grant(cql, 'SELECT', cql_table_name(tab), role):
                authorized(lambda: tab.scan(ConsistentRead=True, Limit=1))

# Test DeleteTable's permissions checks. The DeleteTable operation requires
# a DROP permission on the specific table (or on something which contains it -
# the keyspace or ALL KEYSPACES).
def test_rbac_deletetable(dynamodb, cql):
    schema = {
        'KeySchema': [ { 'AttributeName': 'p', 'KeyType': 'HASH' } ],
        'AttributeDefinitions': [ { 'AttributeName': 'p', 'AttributeType': 'S' }]
    }
    with new_test_table(dynamodb, **schema) as table:
        with new_role(cql) as (role, key):
            with new_dynamodb(dynamodb, role, key) as d:
                tab = d.Table(table.name)
                # Without DROP permissions, DeleteTable won't work:
                unauthorized(lambda: tab.delete())
                # Adding the DROP permissions on this specific table, it
                # can be deleted. We could also add the permissions on the
                # keyspace, on ALL KEYSPACES, or on an inherited role, but
                # we already have other tests above for this kind of
                # inheritence so don't need to test it again for DeleteTable.
                with temporary_grant(cql, 'DROP', cql_table_name(tab), role):
                    tabname = tab.name
                    authorized(lambda: tab.delete())
                    # officially, the DynamoDB API requires waiting for
                    # delete to be done, although it's not currently
                    # necessary in Alternator.
                    tab.meta.client.get_waiter('table_not_exists').wait(TableName=tabname)
                    # When we'll go out of scope on temporary_grant() and
                    # new_test_table(), they expect the table to exist and
                    # complain that it doesn't. So let's recreate the table,
                    # using our original, super-user, role.
                    table = dynamodb.create_table(TableName=tabname,
                        BillingMode='PAY_PER_REQUEST', **schema)
                    table.meta.client.get_waiter('table_exists').wait(TableName=tabname)

@contextmanager
def new_named_table(dynamodb, tabname, **kwargs):
    table = authorized(lambda: dynamodb.create_table(TableName=tabname,
                BillingMode='PAY_PER_REQUEST', **kwargs))
    table.meta.client.get_waiter('table_exists').wait(TableName=tabname)
    try:
        yield table
    finally:
        table.delete()

def new_named_table_unauthorized(dynamodb, tabname, **kwargs):
    try:
        unauthorized(lambda: dynamodb.create_table(TableName=tabname,
                BillingMode='PAY_PER_REQUEST', **kwargs))
    except:
        # Oops, if the table creation *was* authorized, delete the
        # table 
        dynamodb.meta.client.get_waiter('table_exists').wait(TableName=tabname)
        dynamodb.meta.client.delete_table(TableName=tabname)


# When a role is allowed to CreateTable, the role is automatically granted
# permissions to use the new table (and all its materialized views), and
# to eventually delete them. However, when the table and views are finally
# deleted, we need to remove those grants - we don't want them to stay in the
# permissions table and if later a _second_ role creates a table with
# the same name, the first role will still have access to it!
# The following test creates this scenario with two roles, and confirms that
# DeleteTable revokes the permissions from the table.
def test_rbac_deletetable_autorevoke(dynamodb, cql):
    # An example table schema, with a GSI to allow us to also test the
    # permissions on the view.
    schema = {
        'KeySchema': [ { 'AttributeName': 'p', 'KeyType': 'HASH' } ],
        'AttributeDefinitions': [
                    { 'AttributeName': 'p', 'AttributeType': 'S' },
                    { 'AttributeName': 'x', 'AttributeType': 'S' } ],
        'GlobalSecondaryIndexes': [
            {   'IndexName': 'hello',
                'KeySchema': [
                    { 'AttributeName': 'x', 'KeyType': 'HASH' },
                ],
                'Projection': { 'ProjectionType': 'ALL' }
            } ]
    }
    # Table name to use throughout the test below (role1 will create it,
    # delete it, and then role2 will re-create it).
    table_name = unique_table_name()
    # Create two roles and two DynamoDB sessions for them (d1, d2):
    with new_role(cql) as (role1, key1), new_role(cql) as (role2, key2):
        with new_dynamodb(dynamodb, role1, key1) as d1, new_dynamodb(dynamodb, role2, key2) as d2:
            # Allow role1 to create a table, create it and immediately
            # delete it:
            with temporary_grant(cql, 'CREATE', 'ALL KEYSPACES', role1):
                with new_named_table(d1, table_name, **schema) as tab:
                    pass
            # Allow role2 to re-create a table with the same name that
            # role1 just deleted, and create it:
            with temporary_grant(cql, 'CREATE', 'ALL KEYSPACES', role2):
                with new_named_table(d2, table_name, **schema) as tab:
                    # At this point, role2 should have permissions to use
                    # this table, but role1 should not!
                    authorized(lambda: d2.Table(table_name).get_item(Key={'p': 'dog'}, ConsistentRead=True))
                    unauthorized(lambda: d1.Table(table_name).get_item(Key={'p': 'dog'}, ConsistentRead=True))
                    # Same for the view - it should be usable by role2,
                    # but not by role1!
                    authorized(lambda: d2.Table(table_name).query(IndexName='hello',
                        Select='ALL_PROJECTED_ATTRIBUTES',
                        KeyConditions={'x': {'AttributeValueList': ['hi'], 'ComparisonOperator': 'EQ'}}))
                    unauthorized(lambda: d1.Table(table_name).query(IndexName='hello',
                        Select='ALL_PROJECTED_ATTRIBUTES',
                        KeyConditions={'x': {'AttributeValueList': ['hi'], 'ComparisonOperator': 'EQ'}}))

# Test CreateTable's support for permissions. Because a CreateTable operation
# creates a keyspace and a table, it requires the "CREATE" permission on
# "ALL KEYSPACES" - nothing less is enough.
# Test that additionally, CreateTable's has an "autogrant" feature: If a role
# is allowed to create a table, this role is automatically given full (SELECT
# and MODIFY) permissions to the newly-created table.
def test_rbac_createtable(dynamodb, cql):
    # An example table schema, with a GSI to make the example even more
    # interesting (it needs to create a table and a view)
    schema = {
        'KeySchema': [ { 'AttributeName': 'p', 'KeyType': 'HASH' } ],
        'AttributeDefinitions': [
                    { 'AttributeName': 'p', 'AttributeType': 'S' },
                    { 'AttributeName': 'x', 'AttributeType': 'S' } ],
        'GlobalSecondaryIndexes': [
            {   'IndexName': 'hello',
                'KeySchema': [
                    { 'AttributeName': 'x', 'KeyType': 'HASH' },
                ],
                'Projection': { 'ProjectionType': 'ALL' }
            } ]
    }
    with new_role(cql) as (role, key):
        with new_dynamodb(dynamodb, role, key) as d:
            # Without CREATE permissions, CreateTable won't work:
            table_name = unique_table_name()
            new_named_table_unauthorized(d, table_name, **schema)
            # Adding CREATE permissions, it should work
            with temporary_grant(cql, 'CREATE', 'ALL KEYSPACES', role):
                with new_named_table(d, table_name, **schema) as tab:
                    # After being allowed to create the table, this role
                    # is automatically granted permissions to SELECT or MODIFY
                    # it (and also its GSI).
                    p = random_string()
                    x = random_string()
                    v = random_string()
                    authorized(lambda: tab.put_item(Item={'p': p, 'x': x, 'v': v}))
                    assert {'p': p, 'x': x, 'v': v} == authorized(lambda: tab.get_item(Key={'p': p}, ConsistentRead=True)['Item'])
                    # Check this role can also read from the view. We don't
                    # check the data itself (maybe the view wasn't yet updated)
                    # just that we have permissions.
                    authorized(lambda: tab.query(IndexName='hello',
                        Select='ALL_PROJECTED_ATTRIBUTES',
                        KeyConditions={'x': {'AttributeValueList': ['hi'], 'ComparisonOperator': 'EQ'}}))
                    # Check that ALTER permissions were auto-granted too.
                    # The UpdateTable below doesn't actually change anything
                    # (it just sets BillingMode to what it was), but the
                    # access check is done even if there's nothing to do.
                    authorized(lambda: tab.meta.client.update_table(TableName=tab.name,
                        BillingMode='PAY_PER_REQUEST'))
                    # Note that when we now go out of scope, the new table
                    # will be deleted under the new role, so this test also
                    # verifies that the role was correctly auto-granted the
                    # DROP permission.

# Test UpdateTable's support for permissions. It requires the "ALTER"
# permission permission on the given table (or, as usual, something
# containing it - like a keyspace, all keyspaces, or another role).
def test_rbac_updatetable(dynamodb, cql):
    schema = {
        'KeySchema': [ { 'AttributeName': 'p', 'KeyType': 'HASH' } ],
        'AttributeDefinitions': [ { 'AttributeName': 'p', 'AttributeType': 'S' }]
    }
    with new_test_table(dynamodb, **schema) as table:
        with new_role(cql) as (role, key):
            with new_dynamodb(dynamodb, role, key) as d:
                tab = d.Table(table.name)
                # Without ALTER permissions, UpdateTable won't work.
                # The "update" doesn't actually change anything (it just
                # sets BillingMode to what it was), but the access check is
                # done even if there's nothing to do
                unauthorized(lambda: tab.meta.client.update_table(TableName=tab.name,
                    BillingMode='PAY_PER_REQUEST'))
                # With ALTER permissions, it works.
                with temporary_grant(cql, 'ALTER', cql_table_name(tab), role):
                    authorized(lambda: tab.meta.client.update_table(TableName=tab.name,
                        BillingMode='PAY_PER_REQUEST'))

# A test for API operations that do not require any permissions, so can be
# performed on a new role with no grants. This currently includes
# ListTables, DescribeTable, DescribeEndpoints, ListTagsOfResource,
# DescribeTimeToLive, DescribeContinuousBackups
def test_no_permissions_needed(dynamodb, cql, test_table):
    with new_role(cql) as (role, key):
        with new_dynamodb(dynamodb, role, key) as d:
            # Try the various operations that don't need any permissions,
            # and check that they don't fail (we don't check what is the
            # result).
            d.meta.client.list_tables()
            d.meta.client.describe_endpoints()
            r = d.meta.client.describe_table(TableName=test_table.name)
            arn = r['Table']['TableArn']
            d.meta.client.list_tags_of_resource(ResourceArn=arn)
            d.meta.client.describe_time_to_live(TableName=test_table.name)
            d.meta.client.describe_continuous_backups(TableName=test_table.name)

# A test for permission checks in BatchWriteItem. BatchWriteItem needs the
# "MODIFY" permission, but one BatchWriteItem may write to several tables
# so needs MODIFY permissions on all of them, not just one. If any of the
# operations in the batch are not permitted, we want the entire batch to
# fail and not do anything (there is no API to return a separate error
# for each item in the batch).
def test_rbac_batchwriteitem(dynamodb, cql, test_table, test_table_s):
    p = random_string()
    c = random_string()
    v = random_string()
    with new_role(cql) as (role, key):
        with new_dynamodb(dynamodb, role, key) as d:
            # Without MODIFY permission to *both* tables, the BatchWriteItem
            # operation will fail (and we can check through the superuser
            # role neither item was written). Only after we grant MODIFY on
            # both tables, it will work.
            batch = {
                test_table.name: [{'PutRequest': {'Item': {'p': p, 'c': c, 'v': v}}}],
                test_table_s.name: [{'PutRequest': {'Item': {'p': p, 'c': c, 'v': v}}}],
            }
            unauthorized(lambda: d.meta.client.batch_write_item(RequestItems = batch))
            with temporary_grant(cql, 'MODIFY', cql_table_name(test_table), role):
                unauthorized(lambda: d.meta.client.batch_write_item(RequestItems = batch))
                # Verify through the superuser role that until now, neither
                # item was written.
                assert not 'Item' in test_table.get_item(Key={'p': p, 'c': c}, ConsistentRead=True)
                assert not 'Item' in test_table_s.get_item(Key={'p': p}, ConsistentRead=True)
                with temporary_grant(cql, 'MODIFY', cql_table_name(test_table_s), role):
                    # Finally, with both grants, the write should work:
                    authorized(lambda: d.meta.client.batch_write_item(RequestItems = batch))
                    assert 'Item' in test_table.get_item(Key={'p': p, 'c': c}, ConsistentRead=True)
                    assert 'Item' in test_table_s.get_item(Key={'p': p}, ConsistentRead=True)

# A test for permission checks in BatchGetItem. BatchGetItem needs the
# "SELECT" permission, but one BatchGetItem may read from several tables
# so needs SELECT permissions on all of them, not just one. If any of the
# operations in the batch are not permitted, we want the entire batch to
# fail and not do anything (there is no API to return a separate error
# for each item in the batch).
def test_rbac_batchgetitem(dynamodb, cql, test_table, test_table_s):
    p = random_string()
    c = random_string()
    v = random_string()
    test_table.put_item(Item={'p': p, 'c': c, 'v': v})
    test_table_s.put_item(Item={'p': p, 'v': v})
    with new_role(cql) as (role, key):
        with new_dynamodb(dynamodb, role, key) as d:
            # Without SELECT permission on *both* tables, the BatchGetItem
            # operation will fail. Only after we grant SELECT to both tables,
            # it will work.
            batch = {
                test_table.name: {'Keys': [{'p': p, 'c': c}], 'ConsistentRead': True},
                test_table_s.name: {'Keys': [{'p': p}], 'ConsistentRead': True}
            }
            unauthorized(lambda: d.meta.client.batch_get_item(RequestItems = batch))
            with temporary_grant(cql, 'SELECT', cql_table_name(test_table), role):
                unauthorized(lambda: d.meta.client.batch_get_item(RequestItems = batch))
                with temporary_grant(cql, 'SELECT', cql_table_name(test_table_s), role):
                    # Finally, with both grants, the read should work:
                    r = authorized(lambda: d.meta.client.batch_get_item(RequestItems = batch)['Responses'])
                    assert r[test_table.name] == [{'p': p, 'c': c, 'v': v}]
                    assert r[test_table_s.name] == [{'p': p, 'v': v}]

# A test for permission checks in TagResource and UntagResource. We
# consider this a variant of UpdateTable, because one of its uses is to
# modify non-standard parameters of the table (such as its write isolation
# policy), so require the ALTER permission on both.
def test_rbac_tagresource(dynamodb, cql):
    schema = {
        'KeySchema': [ { 'AttributeName': 'p', 'KeyType': 'HASH' } ],
        'AttributeDefinitions': [ { 'AttributeName': 'p', 'AttributeType': 'S' }]
    }
    with new_test_table(dynamodb, **schema) as table:
        arn = table.meta.client.describe_table(TableName=table.name)['Table']['TableArn']
        with new_role(cql) as (role, key):
            with new_dynamodb(dynamodb, role, key) as d:
                tab = d.Table(table.name)
                # Without ALTER permission, TagResource and UntagResource
                # are refused
                tags = [{'Key': 'hello', 'Value': 'dog'},
                        {'Key': 'hi', 'Value': '42'}]
                unauthorized(lambda: d.meta.client.tag_resource(ResourceArn=arn, Tags=tags))
                unauthorized(lambda: d.meta.client.untag_resource(ResourceArn=arn, TagKeys=['hello']))
                # With granting ALTER permissions, it works:
                with temporary_grant(cql, 'ALTER', cql_table_name(table), role):
                    authorized(lambda: d.meta.client.tag_resource(ResourceArn=arn, Tags=tags))
                    authorized(lambda: d.meta.client.untag_resource(ResourceArn=arn, TagKeys=['hello']))

# Test that UpdateTimeToLive requires the ALTER permissions, similar to
# UpdateTable.
def test_rbac_updatetimetolive(dynamodb, cql):
    with new_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' } ],
        AttributeDefinitions=[ { 'AttributeName': 'p', 'AttributeType': 'S' }],
        # Work around issue #16567 that Alternator TTL doesn't work with
        # tablets. When that issue is solved, the following Tags should be
        # removed.
        Tags=[{'Key': 'experimental:initial_tablets', 'Value': 'none'}]
    ) as table:
        with new_role(cql) as (role, key):
            with new_dynamodb(dynamodb, role, key) as d:
                # Without ALTER permissions, UpdateTimeToLive will fail with
                # AccessDeniedException. With the ALTER permissions, it will
                # succeed.
                unauthorized(lambda: d.meta.client.update_time_to_live(TableName=table.name,
                    TimeToLiveSpecification={'AttributeName': 'dog', 'Enabled': True}))
                with temporary_grant(cql, 'ALTER', cql_table_name(table), role):
                    authorized(lambda: d.meta.client.update_time_to_live(TableName=table.name,
                        TimeToLiveSpecification={'AttributeName': 'dog', 'Enabled': True}))

@pytest.fixture(scope="module")
def dot_table(dynamodb):
    name = "." + unique_table_name() + ".hello"
    table = dynamodb.create_table(TableName=name, BillingMode='PAY_PER_REQUEST',
        KeySchema=[{ 'AttributeName': 'p', 'KeyType': 'HASH' }],
        AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }])
    table.meta.client.get_waiter('table_exists').wait(TableName=name)
    yield table
    table.delete()

# The rules on Alternator table names are not identical to CQL table names.
# In particular, an Alternator table's name may contain a dot and the
# resulting name is not a valid CQL table (the fixture "dot_table" creates
# a table with such a name). We want to be able to pass such names to GRANT
# commands.
def test_rbac_table_name_with_dot(dynamodb, cql, dot_table):
    p = random_string()
    with new_role(cql) as (role, key):
        with new_dynamodb(dynamodb, role, key) as d:
            tab = d.Table(dot_table.name)
            # Before granting MODIFY permissions, PutItem fails
            unauthorized(lambda: tab.put_item(Item={'p': p}))
            # Check that grant works despite the illegal (CQL-wise) table name
            with temporary_grant(cql, 'MODIFY', cql_table_name(tab), role):
                authorized(lambda: tab.put_item(Item={'p': p}))

# A "superuser" role should be able to do any operation. We'll just test
# this on PutItem, but assuming the permission-checking code uses the same
# logic for all operations, it should apply to all operations
def test_rbac_superuser(dynamodb, cql, test_table_s):
    p = random_string()
    # A non-super role can't write to a pre-existing table:
    with new_role(cql) as (role, key):
        with new_dynamodb(dynamodb, role, key) as d:
            tab = d.Table(test_table_s.name)
            unauthorized(lambda: tab.put_item(Item={'p': p}))
    # But a superuser role, can:
    with new_role(cql, superuser=True) as (role, key):
        with new_dynamodb(dynamodb, role, key) as d:
            tab = d.Table(test_table_s.name)
            authorized(lambda: tab.put_item(Item={'p': p}))

# In the Streams API, the functions ListStreams, DescribeStream and
# GetShardIterator don't read data and are similar in spirit to DescribeTable
# and don't require any permissions. But GetRecords actually reads data, so
# we require the SELECT permissions. The following test checks all that.
# The test does not intend to test correctness of Alternator Streams, just
# the permissions, so it uses an empty table with no data.
def test_rbac_streams(dynamodb, cql):
    schema = {
        'KeySchema': [ { 'AttributeName': 'p', 'KeyType': 'HASH' } ],
        'AttributeDefinitions': [ { 'AttributeName': 'p', 'AttributeType': 'S' }],
        'StreamSpecification': {'StreamEnabled': True, 'StreamViewType': 'KEYS_ONLY'},
        # Work around issue #16137 that Alternator Streams doesn't work with
        # tablets. When that issue is solved, the following Tags should be
        # removed.
        'Tags': [{'Key': 'experimental:initial_tablets', 'Value': 'none'}]
    }
    with new_test_table(dynamodb, **schema) as table:
        with new_role(cql) as (role, key):
            with new_dynamodb_streams(dynamodb, role, key) as ds:
                # Check that we can use ListStreams, DescribeStream and
                # GetShardIterator without being granted permissions.
                # only for GetRecords we'll need permissions below.
                streams = ds.list_streams(TableName=table.name)
                arn = streams['Streams'][0]['StreamArn']
                desc = ds.describe_stream(StreamArn=arn)
                shard_id = desc['StreamDescription']['Shards'][0]['ShardId']
                iter = ds.get_shard_iterator(StreamArn=arn, ShardId=shard_id, ShardIteratorType='LATEST')['ShardIterator']
                unauthorized(lambda: ds.get_records(ShardIterator=iter))
                # GetRecords checks the SELECT permissions on the CDC table,
                # not the base table, so a grant on the base table doesn't
                # help:
                with temporary_grant(cql, 'SELECT', cql_table_name(table), role):
                    unauthorized(lambda: ds.get_records(ShardIterator=iter))
                # Only a grant on the CDC log table helps:
                with temporary_grant(cql, 'SELECT', cql_cdclog_name(table), role):
                    authorized(lambda: ds.get_records(ShardIterator=iter))

# In the test above (test_rbac_streams), the superuser creates a table and
# a stream, and grants a role the ability to read them. In this test, the role
# itself creates the table and the stream, and we need to check that the
# role receives permissions to read the stream it just created (auto-grant).
# We have two tests for the two ways to create a CDC log: creating a table
# with streams enabled up-front during creation - and enabling the stream
# later in an already existing table.
# Reproduces issue #19798
@pytest.mark.xfail(reason="#19798")
@pytest.mark.parametrize("during_creation", [True, False])
def test_rbac_streams_autogrant(dynamodb, cql, during_creation):
    schema = {
        'KeySchema': [ { 'AttributeName': 'p', 'KeyType': 'HASH' } ],
        'AttributeDefinitions': [ { 'AttributeName': 'p', 'AttributeType': 'S' }],
        # Work around issue #16137 that Alternator Streams doesn't work with
        # tablets. When that issue is solved, the following Tags should be
        # removed.
        'Tags': [{'Key': 'experimental:initial_tablets', 'Value': 'none'}]
    }
    enable_stream = {'StreamSpecification': {'StreamEnabled': True, 'StreamViewType': 'KEYS_ONLY'}}
    if during_creation:
        schema.update(enable_stream)
    with new_role(cql) as (role, key):
        with new_dynamodb(dynamodb, role, key) as d, new_dynamodb_streams(dynamodb, role, key) as ds:
            # Allow the new role to create a table. The table created in the
            # new role should permission to work on it - in particular to
            # use the stream (GetRecords).
            with temporary_grant(cql, 'CREATE', 'ALL KEYSPACES', role):
                table_name = unique_table_name()
                with new_named_table(d, table_name, **schema) as table:
                    if not during_creation:
                        authorized(lambda: table.update(**enable_stream))
                    streams = ds.list_streams(TableName=table.name)
                    arn = streams['Streams'][0]['StreamArn']
                    desc = ds.describe_stream(StreamArn=arn)
                    shard_id = desc['StreamDescription']['Shards'][0]['ShardId']
                    iter = ds.get_shard_iterator(StreamArn=arn, ShardId=shard_id, ShardIteratorType='LATEST')['ShardIterator']
                    # Note: use low timeout to avoid slow xfail
                    authorized(lambda: ds.get_records(ShardIterator=iter), timeout=3*permissions_validity_in_ms(cql))

# Once autogrant works (tested in the above test, test_rbac_streams_autogrant)
# we also need auto-revoke - i.e., when the stream is deleted the permissions
# should be deleted - otherwise if role1 creates a table and a stream, deletes
# it, and later role2 creates a table with the same name, role1 might be able
# to read the new stream!
def test_rbac_streams_autorevoke(dynamodb, cql):
    schema = {
        'KeySchema': [ { 'AttributeName': 'p', 'KeyType': 'HASH' } ],
        'AttributeDefinitions': [ { 'AttributeName': 'p', 'AttributeType': 'S' }],
        'StreamSpecification': {'StreamEnabled': True, 'StreamViewType': 'KEYS_ONLY'},
        # Work around issue #16137 that Alternator Streams doesn't work with
        # tablets. When that issue is solved, the following Tags should be
        # removed.
        'Tags': [{'Key': 'experimental:initial_tablets', 'Value': 'none'}]
    }
    table_name = unique_table_name()
    with new_role(cql) as (role1, key1), new_role(cql) as (role2, key2):
        with new_dynamodb(dynamodb, role1, key1) as d1, new_dynamodb(dynamodb, role2, key2) as d2, new_dynamodb_streams(dynamodb, role1, key1) as ds1:
            with temporary_grant(cql, 'CREATE', 'ALL KEYSPACES', role1):
                with new_named_table(d1, table_name, **schema) as table:
                    # role1 created table_name, so autogrant will now give
                    # it permissions to read the table and the CDC log.
                    # we hope this permission is auto-revoked when the
                    # table is deleted when this scope end.
                    pass
                # After role1 deleted the table table_name, let's have
                # role2 create a table with the same name:
                with temporary_grant(cql, 'CREATE', 'ALL KEYSPACES', role2):
                    with new_named_table(d2, table_name, **schema) as table:
                        # At this point, the new table and its stream should
                        # be readable to role2 but NOT to role1. Let's check
                        # its indeed not reable to role1 (get_records will
                        # fail) - so auto-revoke worked:
                        streams = ds1.list_streams(TableName=table.name)
                        arn = streams['Streams'][0]['StreamArn']
                        desc = ds1.describe_stream(StreamArn=arn)
                        shard_id = desc['StreamDescription']['Shards'][0]['ShardId']
                        iter = ds1.get_shard_iterator(StreamArn=arn, ShardId=shard_id, ShardIteratorType='LATEST')['ShardIterator']
                        unauthorized(lambda: ds1.get_records(ShardIterator=iter))
