# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

# Tests for the operations allowed under the four different write isolation
# modes describe in docs/alternator/new-apis.md. We only test here which
# operations are allowed or not allowed in each write isolation mode -
# we do NOT test here the actual isolation provided in these modes between
# different concurrent writes! We will need to check that in a different
# test framework that is allows testing Alternator concurrency and
# consistency (see suggestion in #6350).
#
# We also don't check here the various corner cases of the operation to *set*
# the write isolation - this is done using tags and tested in test_tag.py.
#
# The four write isolation modes fall into two types:
#
# 1. The mode `forbid_rmw` forbids any type of write operation which requires
#    a read before the write (a.k.a. read-modify-write or RMW).
#    As we'll test below, the forbidden operations include writes with
#    conditions, certain update operations (but not all), and writes returning
#    pre-write values. We need to check what kinds of writes are forbidden,
#    but also what is not forbidden.
#
# 2. The modes `always_use_lwt`, `only_rmw_uses_lwt`, and `unsafe_rmw`
#    allow any write operation, including those that need a read-before-write.
#    Below we'll call these three modes "permit rmw" modes.
#
# The result of each allowed operation is the same in all modes that allow
# it (remember we don't check concurrent updates). We want to verify this
# because operations have slightly different code paths for different modes.
# However, note that it is only necessary to check here major feature
# interaction - for example PutItem with or without a ConditionExpression -
# we don't need to check every kind of syntax in ConditionExpression
# because we already test this elsewhere (test_condition_expression.py).
# We don't expect Alternator's handling of different ConditionExpression
# operators to depend on any way on the write isolation mode. Conversely,
# as we'll see below, different UpdateExpressions features do need - or
# don't need - a read before write, so we'll need to check those specific
# UpdateExpressions.
#
# As the name "write isolation" suggests, Alternator guarantees that the
# write isolation mode only affects *writes* - so we don't need to test here
# its effect on read operations. This is not entirely obvious - when we
# started working on Alternator, we thought we might need to use LWT for
# reads as well. But at the end, we decided not to, and the name "write
# isolation mode" now guarantees that it indeed affects only writes.
# Reads are the same in all four modes, and do not use LWT, and we don't
# need to test them here.
#############################################################################

import pytest
from botocore.exceptions import ClientError
from .util import create_test_table, random_string

@pytest.fixture(scope="function", autouse=True)
def all_tests_are_scylla_only(scylla_only):
    pass

# The error that RMW (read-modify-write) operations return in forbid_rmw 
# write isolation mode:
rmw_forbidden='ValidationException.*write isolation policy'

# The "table_*" fixtures are the same as test_table_s (table with a string
# hash key), except with each a different write isolation mode.
# Because write isolation modes are a Scylla-only feature, these fixtures
# are marked scylla_only, so all tests that use one of them are skipped
# when not running against Scylla.
@pytest.fixture(scope='module')
def table_forbid_rmw(dynamodb, scylla_only):
    table = create_test_table(dynamodb,
        Tags=[{'Key': 'system:write_isolation', 'Value': 'forbid_rmw'}],
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }, ],
        AttributeDefinitions=[ { 'AttributeName': 'p', 'AttributeType': 'S' } ])
    yield table
    table.delete()

@pytest.fixture(scope='module')
def table_always_use_lwt(dynamodb, scylla_only):
    table = create_test_table(dynamodb,
        Tags=[{'Key': 'system:write_isolation', 'Value': 'always_use_lwt'}],
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }, ],
        AttributeDefinitions=[ { 'AttributeName': 'p', 'AttributeType': 'S' } ])
    yield table
    table.delete()

@pytest.fixture(scope='module')
def table_only_rmw_uses_lwt(dynamodb, scylla_only):
    table = create_test_table(dynamodb,
        Tags=[{'Key': 'system:write_isolation', 'Value': 'only_rmw_uses_lwt'}],
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }, ],
        AttributeDefinitions=[ { 'AttributeName': 'p', 'AttributeType': 'S' } ])
    yield table
    table.delete()

@pytest.fixture(scope='module')
def table_unsafe_rmw(dynamodb, scylla_only):
    table = create_test_table(dynamodb,
        Tags=[{'Key': 'system:write_isolation', 'Value': 'unsafe_rmw'}],
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }, ],
        AttributeDefinitions=[ { 'AttributeName': 'p', 'AttributeType': 'S' } ])
    yield table
    table.delete()

# "permit rwm" write isolation modes are the three modes besides forbid_rmw.
# These three modes permit all read-modify-write operations. Although these
# modes may isolate concurrent writes differently, there is no difference
# between them when writes are not concurrent.
@pytest.fixture(scope='module')
def tables_permit_rmw(table_always_use_lwt, table_only_rmw_uses_lwt, table_unsafe_rmw):
    yield [table_always_use_lwt, table_only_rmw_uses_lwt, table_unsafe_rmw]


#############################################################################
# "ConditionExpression" tests.
# ConditionExpression applies to PutItem, DeleteItem and UpdateItem.
# Without ConditionExpression, these operations work on all modes. With
# ConditionExpression, they work on the three permit modes, and fail on
# the forbid mode.

# PutItem & ConditionExpression:
def test_isolation_putitem_conditionexpression(table_forbid_rmw, tables_permit_rmw):
    # Without ConditionExpression, PutItem works correctly on all modes
    p = random_string()
    for table in tables_permit_rmw + [table_forbid_rmw]:
        table.put_item(Item={'p': p, 'a': 1})
        assert table.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': 1}
    # With ConditionExpression, PutItem works correctly on permit modes,
    # fails on forbid mode.
    for table in tables_permit_rmw:
        table.put_item(Item={'p': p, 'a': 2},
            ConditionExpression='a = :oldval',
            ExpressionAttributeValues={':oldval': 1})
        assert table.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': 2}
    with pytest.raises(ClientError, match=rmw_forbidden):
        table_forbid_rmw.put_item(Item={'p': p, 'a': 2},
            ConditionExpression='a = :oldval',
            ExpressionAttributeValues={':oldval': 1})

# DeleteItem & ConditionExpression:
def test_isolation_deleteitem_conditionexpression(table_forbid_rmw, tables_permit_rmw):
    # Without ConditionExpression, DeleteItem works correctly on all modes
    p = random_string()
    for table in tables_permit_rmw + [table_forbid_rmw]:
        table.put_item(Item={'p': p, 'a': 1})
        assert table.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': 1}
        table.delete_item(Key={'p': p})
        assert 'Item' not in table.get_item(Key={'p': p}, ConsistentRead=True)
    # With ConditionExpression, DeleteItem works correctly on permit modes,
    # fails on forbid mode.
    for table in tables_permit_rmw:
        table.put_item(Item={'p': p, 'a': 1})
        assert table.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': 1}
        table.delete_item(Key={'p': p},
            ConditionExpression='a = :oldval',
            ExpressionAttributeValues={':oldval': 1})
        assert 'Item' not in table.get_item(Key={'p': p}, ConsistentRead=True)
    with pytest.raises(ClientError, match=rmw_forbidden):
        table_forbid_rmw.delete_item(Key={'p': p},
            ConditionExpression='a = :oldval',
            ExpressionAttributeValues={':oldval': 1})

# UpdateItem & ConditionExpression:
# Note that an UpdateItem always needs some sort of update expression, so
# we deliberately test with an update expression that doesn't itself need
# a read-before-write (we'll check more elaborate UpdateExpression below).
def test_isolation_updateitem_conditionexpression(table_forbid_rmw, tables_permit_rmw):
    # Without ConditionExpression, UpdateItem (with a simple update expression
    # not requiring read-before-write) works correctly on all modes
    p = random_string()
    for table in tables_permit_rmw + [table_forbid_rmw]:
        table.update_item(Key={'p': p},
            UpdateExpression='SET a = :val',
            ExpressionAttributeValues={':val': 1})
        assert table.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': 1}
    # With ConditionExpression, UpdateItem works correctly on permit modes,
    # fails on forbid mode.
    for table in tables_permit_rmw:
        table.update_item(Key={'p': p},
            UpdateExpression='SET a = :newval',
            ConditionExpression='a = :oldval',
            ExpressionAttributeValues={':oldval': 1, ':newval': 2})
        assert table.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': 2}
    with pytest.raises(ClientError, match=rmw_forbidden):
        table_forbid_rmw.update_item(Key={'p': p},
            UpdateExpression='SET a = :newval',
            ConditionExpression='a = :oldval',
            ExpressionAttributeValues={':oldval': 1, ':newval': 2})

#############################################################################
# "Expected" tests. Expected is the old version of ConditionExpression and
# abides by the same rules - so we have very similar tests to those of
# ConditionExpression above. We don't need to test the "without Expected"
# cases, as "without Expected" is the same as "without ConditionExpression" :-)

# PutItem & Expected:
def test_isolation_putitem_expected(table_forbid_rmw, tables_permit_rmw):
    # With Expected, PutItem works correctly on permit modes,
    # fails on forbid mode.
    p = random_string()
    for table in tables_permit_rmw:
        table.put_item(Item={'p': p, 'a': 1})
        table.put_item(Item={'p': p, 'a': 2},
            Expected={'a': {'ComparisonOperator': 'EQ', 'AttributeValueList': [1]}})
        assert table.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': 2}
    with pytest.raises(ClientError, match=rmw_forbidden):
        table_forbid_rmw.put_item(Item={'p': p, 'a': 2},
            Expected={'a': {'ComparisonOperator': 'EQ', 'AttributeValueList': [1]}})

# DeleteItem & Expected:
def test_isolation_deleteitem_expected(table_forbid_rmw, tables_permit_rmw):
    # With Expected, DeleteItem works correctly on permit modes,
    # fails on forbid mode.
    p = random_string()
    for table in tables_permit_rmw:
        table.put_item(Item={'p': p, 'a': 1})
        table.delete_item(Key={'p': p},
            Expected={'a': {'ComparisonOperator': 'EQ', 'AttributeValueList': [1]}})
        assert 'Item' not in table.get_item(Key={'p': p}, ConsistentRead=True)
    with pytest.raises(ClientError, match=rmw_forbidden):
        table_forbid_rmw.delete_item(Key={'p': p},
            Expected={'a': {'ComparisonOperator': 'EQ', 'AttributeValueList': [1]}})

# UpdateItem & Expected:
def test_isolation_updateitem_expected(table_forbid_rmw, tables_permit_rmw):
    # With Expected, UpdateItem works correctly on permit modes,
    # fails on forbid mode.
    p = random_string()
    for table in tables_permit_rmw:
        table.put_item(Item={'p': p, 'a': 1})
        table.update_item(Key={'p': p},
            AttributeUpdates={'a': {'Value': 2, 'Action': 'PUT'}},
            Expected={'a': {'ComparisonOperator': 'EQ', 'AttributeValueList': [1]}})
        assert table.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': 2}
    with pytest.raises(ClientError, match=rmw_forbidden):
        table_forbid_rmw.update_item(Key={'p': p},
            AttributeUpdates={'a': {'Value': 2, 'Action': 'PUT'}},
            Expected={'a': {'ComparisonOperator': 'EQ', 'AttributeValueList': [1]}})

#############################################################################
# "UpdateExpression" tests.
# Obviously, only the UpdateItem operation supports UpdateExpression.
# As we'll see now, certain expressions need to read attributes from the
# existing item and require read-modify-write (so don't work on forbid_rmw
# mode), but other expressions don't.

# Test "write only" update expressions, that do not need to read the old
# value of the item, so work on all write isolation modes:
def test_isolation_updateexpression_write_only(table_forbid_rmw, tables_permit_rmw):
    p = random_string()
    for table in tables_permit_rmw + [table_forbid_rmw]:
        table.update_item(Key={'p': p},
            UpdateExpression='SET a = :val',
            ExpressionAttributeValues={':val': 1})
        assert table.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': 1}
        table.update_item(Key={'p': p},
            UpdateExpression='REMOVE a')
        assert table.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p}

# Test rmw update expressions, that need to read the old value of the item,
# so work on all write isolation modes except forbid_rmw
def test_isolation_updateexpression_rmw(table_forbid_rmw, tables_permit_rmw):
    p = random_string()
    for table in tables_permit_rmw:
        table.put_item(Item={'p': p, 'a': 1})
        # A SET with an attribute in the RHS (right-hand side) needs to read
        # that attribute
        table.update_item(Key={'p': p},
            UpdateExpression='SET a = a + :incr',
            ExpressionAttributeValues={':incr': 1})
        assert table.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': 2}
        # An ADD is also a read-modify-write operation
        table.update_item(Key={'p': p},
            UpdateExpression='ADD a :incr',
            ExpressionAttributeValues={':incr': 1})
        assert table.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': 3}
        # A SET with a top-level attribute in the LHS doesn't need to read
        # that attribute (we checked this in the previous test), however
        # if the attribute is a document path, we do need to read the old
        # item because Scylla needs to read the full top-level attribute,
        # modify only a part of it, and write it back.
        table.put_item(Item={'p': p, 'a': {'x': 'y'}})
        table.update_item(Key={'p': p},
            UpdateExpression='SET a.b = :val',
            ExpressionAttributeValues={':val': 1})
        assert table.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': {'x': 'y', 'b': 1}}
        # A DELETE (removing an element from a set) also requires a read.
        table.put_item(Item={'p': p, 'a': set([2, 4, 6])})
        table.update_item(Key={'p': p},
            UpdateExpression='DELETE a :val',
            ExpressionAttributeValues={':val': set([2])})
        assert table.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': set([4,6])}
    # Check that the same things don't work in forbid_rmw mode:
    table = table_forbid_rmw
    table.put_item(Item={'p': p, 'a': 1})
    with pytest.raises(ClientError, match=rmw_forbidden):
        table.update_item(Key={'p': p},
            UpdateExpression='SET a = a + :incr',
            ExpressionAttributeValues={':incr': 1})
    with pytest.raises(ClientError, match=rmw_forbidden):
        table.update_item(Key={'p': p},
            UpdateExpression='ADD a :incr',
            ExpressionAttributeValues={':incr': 1})
    table.put_item(Item={'p': p, 'a': {'x': 'y'}})
    with pytest.raises(ClientError, match=rmw_forbidden):
        table.update_item(Key={'p': p},
            UpdateExpression='SET a.b = :val',
            ExpressionAttributeValues={':val': 1})
    table.put_item(Item={'p': p, 'a': set([2, 4, 6])})
    with pytest.raises(ClientError, match=rmw_forbidden):
        table.update_item(Key={'p': p},
            UpdateExpression='DELETE a :val',
            ExpressionAttributeValues={':val': set([2])})

#############################################################################
# "AttributeUpdates" tests. These are the old version of UpdateExpression,
# and also mix RMW operations (ADD), with non-RWM (PUT, DELETE)

# Test PUT and DELETE updates, that do not need to read the old
# value of the item, so work on all write isolation modes:
def test_isolation_attributeupdates_write_only(table_forbid_rmw, tables_permit_rmw):
    p = random_string()
    for table in tables_permit_rmw + [table_forbid_rmw]:
        table.update_item(Key={'p': p},
            AttributeUpdates={'a': {'Value': 1, 'Action': 'PUT'}})
        assert table.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': 1}
        table.update_item(Key={'p': p},
            AttributeUpdates={'a': {'Action': 'DELETE'}})
        assert table.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p}

# Test ADD updates, that need to read the old value of the item,
# so work on all write isolation modes except forbid_rmw
def test_isolation_attributeupdates_rmw(table_forbid_rmw, tables_permit_rmw):
    p = random_string()
    for table in tables_permit_rmw:
        table.put_item(Item={'p': p, 'a': 1})
        table.update_item(Key={'p': p},
            AttributeUpdates={'a': {'Value': 1, 'Action': 'ADD'}})
        assert table.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': 2}
    with pytest.raises(ClientError, match=rmw_forbidden):
        table_forbid_rmw.update_item(Key={'p': p},
            AttributeUpdates={'a': {'Value': 1, 'Action': 'ADD'}})

#############################################################################
# "ReturnValues" tests. ALL_OLD, ALL_NEW, UPDATED_OLD requires RMW,
# but NONE and UPDATED_NEW do not.
# ReturnValues is supported by the PutItem, DeleteItem and UpdateItem
# operations.

# PutItem & ReturnValues:
# PutItem supports only ReturnValues = NONE or ALL_OLD
def test_isolation_putitem_returnvalues(table_forbid_rmw, tables_permit_rmw):
    # With ReturnValues=NONE, PutItem works correctly on all modes
    p = random_string()
    for table in tables_permit_rmw + [table_forbid_rmw]:
        table.put_item(Item={'p': p, 'a': 1}, ReturnValues='NONE')
        assert table.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': 1}
    # With ReturnValues=ALL_OLD, PutItem works correctly (also returning the
    # old item) on permit modes, and fails on forbid mode.
    for table in tables_permit_rmw:
        ret = table.put_item(Item={'p': p, 'a': 2}, ReturnValues='ALL_OLD')
        assert ret['Attributes'] == {'p': p, 'a': 1}
        assert table.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': 2}
    with pytest.raises(ClientError, match=rmw_forbidden):
        table_forbid_rmw.put_item(Item={'p': p, 'a': 2}, ReturnValues='ALL_OLD')

# DeleteItem & ReturnValues:
# DeleteItem supports only ReturnValues = NONE, ALL_OLD
def test_isolation_deleteitem_returnvalues(table_forbid_rmw, tables_permit_rmw):
    # With ReturnValues=NONE, DeleteItem works correctly on all modes
    p = random_string()
    for table in tables_permit_rmw + [table_forbid_rmw]:
        table.put_item(Item={'p': p, 'a': 1})
        table.delete_item(Key={'p': p}, ReturnValues='NONE')
        assert 'Item' not in table.get_item(Key={'p': p}, ConsistentRead=True)
    # With ReturnValues=ALL_OLD, DeleteItems works correctly (also returning
    # the old item) on permit modes, and fails on forbid mode.
    for table in tables_permit_rmw:
        table.put_item(Item={'p': p, 'a': 1})
        ret = table.delete_item(Key={'p': p}, ReturnValues='ALL_OLD')
        assert ret['Attributes'] == {'p': p, 'a': 1}
        assert 'Item' not in table.get_item(Key={'p': p}, ConsistentRead=True)
    with pytest.raises(ClientError, match=rmw_forbidden):
        table_forbid_rmw.delete_item(Key={'p': p}, ReturnValues='ALL_OLD')

# UpdateItem & ReturnValues:
# UpdateItem supports all ReturnValues values - NONE, ALL_OLD, UPDATED_OLD,
# ALL_NEW, and UPDATED_NEW. Some of them requiring rwm, some not:
def test_isolation_updateitem_returnvalues(table_forbid_rmw, tables_permit_rmw):
    # With ReturnValues=NONE or UPDATED_NEW, UpdateItem works correctly on
    # all modes - there is no need to read the old item so it's not a
    # read-modify-write operation.
    p = random_string()
    for table in tables_permit_rmw + [table_forbid_rmw]:
        table.update_item(Key={'p': p},
            UpdateExpression='SET a = :val',
            ExpressionAttributeValues={':val': 1},
            ReturnValues='NONE')
        assert table.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': 1}
        ret = table.update_item(Key={'p': p},
            UpdateExpression='SET a = :val',
            ExpressionAttributeValues={':val': 2},
            ReturnValues='UPDATED_NEW')
        assert ret['Attributes'] == {'a': 2}
        assert table.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': 2}
    # All other ReturnValues modes need to read the old item, so work
    # correctly on all write isolation modes except forbid:
    for table in tables_permit_rmw:
        ret = table.update_item(Key={'p': p},
            UpdateExpression='SET a = :val',
            ExpressionAttributeValues={':val': 3},
            ReturnValues='ALL_OLD')
        assert ret['Attributes'] == {'p': p, 'a': 2}
        assert table.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': 3}
        ret = table.update_item(Key={'p': p},
            UpdateExpression='SET a = :val',
            ExpressionAttributeValues={':val': 4},
            ReturnValues='UPDATED_OLD')
        assert ret['Attributes'] == {'a': 3}
        assert table.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': 4}
        ret = table.update_item(Key={'p': p},
            UpdateExpression='SET a = :val',
            ExpressionAttributeValues={':val': 5},
            ReturnValues='ALL_NEW')
        assert ret['Attributes'] == {'p': p, 'a': 5}
        assert table.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': 5}
    for returnvalues in ['ALL_OLD', 'UPDATED_OLD', 'ALL_NEW']:
        with pytest.raises(ClientError, match=rmw_forbidden):
            table_forbid_rmw.update_item(Key={'p': p},
                UpdateExpression='SET a = :val',
                ExpressionAttributeValues={':val': 1},
                ReturnValues=returnvalues)
