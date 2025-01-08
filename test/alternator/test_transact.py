# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

# Tests for the multi-item transaction feature (issue #5064) -
# the TransactWriteItems and TransactGetItems requests.
#
# Note that the tests in this file check only the correct functionality of
# individual calls of these requests. They do not test the consistency or
# isolation guarantees, nor do they test the concurrent invocation of multiple
# requests. Such tests would require a different test framework, such as the
# one requested in issue #6350.

import pytest

from botocore.exceptions import ClientError
from boto3.dynamodb.types import TypeDeserializer

from .util import random_string

##########################################################################
# Test the basic functionality of the TransactWriteItems operation -
# Put, Delete, ConditionCheck, Update - on a single item of a single table.
# A ClientRequestToken is not passed in these basic tests.
##########################################################################

# Single Put action without a condition
@pytest.mark.xfail(reason="#5064 - transactions not yet supported")
def test_transact_write_items_single_put_unconditional(test_table_s):
    p = random_string()
    x = random_string()
    item = {'p': p, 'x': x}
    test_table_s.meta.client.transact_write_items(TransactItems=[{
        'Put': {
            'TableName': test_table_s.name,
            'Item': item
        }}])
    assert item == test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']

# Single Put action with a true condition - succeeds
@pytest.mark.xfail(reason="#5064 - transactions not yet supported")
def test_transact_write_items_single_put_true(test_table_s):
    p = random_string()
    x = random_string()
    item = {'p': p, 'x': x}
    test_table_s.meta.client.transact_write_items(TransactItems=[{
        'Put': {
            'TableName': test_table_s.name,
            'Item': item,
            # The condition attribute_not_exists(p) is true for an
            # item that doesn't exist
            'ConditionExpression': 'attribute_not_exists(p)'
        }}])
    assert item == test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']

# Single Put action with a false condition fails with a
# TransactionCanceledException
@pytest.mark.xfail(reason="#5064 - transactions not yet supported")
def test_transact_write_items_single_put_false(test_table_s):
    p = random_string()
    x = random_string()
    item = {'p': p, 'x': x}
    with pytest.raises(ClientError, match='TransactionCanceledException'):
        test_table_s.meta.client.transact_write_items(TransactItems=[{
            'Put': {
                'TableName': test_table_s.name,
                'Item': item,
                # The condition attribute_exists(p) is false for an
                # item that doesn't exist
                'ConditionExpression': 'attribute_exists(p)'
            }}])
    # The transaction failed, so the item didn't go in:
    assert 'Item' not in test_table_s.get_item(Key={'p': p}, ConsistentRead=True)

# Verify that a transaction's Put action, behaves like a PutItem request,
# and not like a CQL Insert, in that it completely replaces an existing item -
# it doesn't merge the new data into the existing item.
@pytest.mark.xfail(reason="#5064 - transactions not yet supported")
def test_transact_write_items_single_put_replaces(test_table_s):
    p = random_string()
    x = random_string()
    y = random_string()
    test_table_s.put_item(Item={'p': p, 'x': x})
    assert {'p': p, 'x': x} == test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']
    test_table_s.meta.client.transact_write_items(TransactItems=[{
        'Put': {
            'TableName': test_table_s.name,
            'Item': {'p': p, 'y': y}
        }}])
    assert {'p': p, 'y': y} == test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']

# Single Delete action without a condition
@pytest.mark.xfail(reason="#5064 - transactions not yet supported")
def test_transact_write_items_single_delete_unconditional(test_table_s):
    p = random_string()
    x = random_string()
    item = {'p': p, 'x': x}
    test_table_s.put_item(Item=item)
    assert item == test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']
    test_table_s.meta.client.transact_write_items(TransactItems=[{
        'Delete': {
            'TableName': test_table_s.name,
            'Key': {'p': p}
        }}])
    # Item should be gone
    assert 'Item' not in test_table_s.get_item(Key={'p': p}, ConsistentRead=True)

# Single Delete action with a true condition succeeds
@pytest.mark.xfail(reason="#5064 - transactions not yet supported")
def test_transact_write_items_single_delete_true(test_table_s):
    p = random_string()
    x = random_string()
    item = {'p': p, 'x': x}
    test_table_s.put_item(Item=item)
    assert item == test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']
    test_table_s.meta.client.transact_write_items(TransactItems=[{
        'Delete': {
            'TableName': test_table_s.name,
            'Key': {'p': p},
            # The condition attribute_exists(p) is true for an
            # item that exists
            'ConditionExpression': 'attribute_exists(p)'
        }}])
    # Item should be gone
    assert 'Item' not in test_table_s.get_item(Key={'p': p}, ConsistentRead=True)

# Single Delete action with a false condition fails with a
# TransactionCanceledException
@pytest.mark.xfail(reason="#5064 - transactions not yet supported")
def test_transact_write_items_single_delete_false(test_table_s):
    p = random_string()
    x = random_string()
    item = {'p': p, 'x': x}
    test_table_s.put_item(Item=item)
    with pytest.raises(ClientError, match='TransactionCanceledException'):
        test_table_s.meta.client.transact_write_items(TransactItems=[{
            'Delete': {
                'TableName': test_table_s.name,
                'Key': {'p': p},
                # The condition attribute_not_exists(p) is false for an
                # item that does exist
                'ConditionExpression': 'attribute_not_exists(p)'
            }}])
    # The transaction failed, so the item wasn't deleted
    assert item == test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']

# Single ConditionCheck action without a condition is, unsurprisingly,
# not allowed - resulting in ValidationException
@pytest.mark.xfail(reason="#5064 - transactions not yet supported")
def test_transact_write_items_single_conditioncheck_unconditional(test_table_s):
    p = random_string()
    with pytest.raises(ClientError, match='ValidationException.*[cC]onditionExpression'):
        test_table_s.meta.client.transact_write_items(TransactItems=[{
            'ConditionCheck': {
                'TableName': test_table_s.name,
                'Key': {'p': p}
            }}])

# Single ConditionCheck action with a true condition succeeds, but
# doesn't do anything (since it's just a condition check, not a write)
@pytest.mark.xfail(reason="#5064 - transactions not yet supported")
def test_transact_write_items_single_conditioncheck_true(test_table_s):
    p = random_string()
    test_table_s.meta.client.transact_write_items(TransactItems=[{
        'ConditionCheck': {
            'TableName': test_table_s.name,
            'Key': {'p': p},
            # The condition attribute_not_exists(p) is true for an
            # item that doesn't exist
            'ConditionExpression': 'attribute_not_exists(p)'
        }}])

# Single ConditionCheck action with a false condition fails with a
# TransactionCanceledException
@pytest.mark.xfail(reason="#5064 - transactions not yet supported")
def test_transact_write_items_single_conditioncheck_false(test_table_s):
    p = random_string()
    with pytest.raises(ClientError, match='TransactionCanceledException'):
        test_table_s.meta.client.transact_write_items(TransactItems=[{
            'ConditionCheck': {
                'TableName': test_table_s.name,
                'Key': {'p': p},
                # The condition attribute_exists(p) is false for an item that
                # doesn't exist
                'ConditionExpression': 'attribute_exists(p)'
            }}])

# Unlike the previous test_transact_write_items_single_* tests which used
# trivial conditions without ExpressionAttributeNames and
# ExpressionAttributeValues, the following tests for Update actions do use
# those in UpdateExpression and/or ConditionExpression.

# Single Update action without a condition
@pytest.mark.xfail(reason="#5064 - transactions not yet supported")
def test_transact_write_items_single_update_unconditional(test_table_s):
    p = random_string()
    item = {'p': p, 'x': 42}
    test_table_s.put_item(Item=item)
    test_table_s.meta.client.transact_write_items(TransactItems=[{
        'Update': {
            'TableName': test_table_s.name,
            'Key': {'p': p},
            'UpdateExpression': 'SET #var = #var + :one',
            'ExpressionAttributeNames': {'#var': 'x'},
            'ExpressionAttributeValues': {':one': 1},
        }}])
    item['x'] += 1
    assert item == test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']

# Single Update action with a true condition succeeds
@pytest.mark.xfail(reason="#5064 - transactions not yet supported")
def test_transact_write_items_single_update_true(test_table_s):
    p = random_string()
    item = {'p': p, 'x': 42}
    test_table_s.put_item(Item=item)
    test_table_s.meta.client.transact_write_items(TransactItems=[{
        'Update': {
            'TableName': test_table_s.name,
            'Key': {'p': p},
            # x is really 42, so this condition is true
            'ConditionExpression': '#var = :fourtytwo',
            'UpdateExpression': 'SET #var = #var + :one',
            'ExpressionAttributeNames': {'#var': 'x'},
            'ExpressionAttributeValues': {':one': 1, ':fourtytwo': 42},
        }}])
    item['x'] += 1
    assert item == test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']

# Single Update action with a false condition fails with a
# TransactionCanceledException
@pytest.mark.xfail(reason="#5064 - transactions not yet supported")
def test_transact_write_items_single_update_false(test_table_s):
    p = random_string()
    item = {'p': p, 'x': 42}
    test_table_s.put_item(Item=item)
    with pytest.raises(ClientError, match='TransactionCanceledException'):
        test_table_s.meta.client.transact_write_items(TransactItems=[{
            'Update': {
                'TableName': test_table_s.name,
                'Key': {'p': p},
                # x is really 42, not 43, so this condition is false
                'ConditionExpression': '#var = :fourtythree',
                'UpdateExpression': 'SET #var = #var + :one',
                'ExpressionAttributeNames': {'#var': 'x'},
                'ExpressionAttributeValues': {':one': 1, ':fourtythree': 43},
            }}])
    # Since the condition failed, the item remains unchanged
    assert item == test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']

##########################################################################
# Additional tests for less basic TransactWriteItems functionality
##########################################################################

# Check that without ClientRequestToken, a request is not idempotent -
# if we increment the same counter twice it happens twice. But if we do
# use ClientRequestToken and pass the same one twice, only the first increment
# happens.
@pytest.mark.xfail(reason="#5064 - transactions not yet supported")
def test_transact_write_items_clientrequesttoken(test_table_s):
    p = random_string()
    item = {'p': p, 'x': 42}
    test_table_s.put_item(Item=item)

    increment_x = [{
        'Update': {
            'TableName': test_table_s.name, 'Key': {'p': p},
            'UpdateExpression': 'SET x = x + :one',
            'ExpressionAttributeValues': {':one': 1},
        }
    }]

    # Doing two transactions each incrementing x without ClientRequestToken,
    # both happen:
    test_table_s.meta.client.transact_write_items(
        TransactItems=increment_x)
    assert 43 == test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['x']
    test_table_s.meta.client.transact_write_items(
        TransactItems=increment_x)
    assert 44 == test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['x']
    # But if we pass a ClientRequestToken and attempt the same transaction
    # twice, only one is performed. The second transaction succeeds - it
    # just has no affect.
    token = random_string()
    test_table_s.meta.client.transact_write_items(
        ClientRequestToken=token, TransactItems=increment_x)
    assert 45 == test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['x']
    test_table_s.meta.client.transact_write_items(
        ClientRequestToken=token, TransactItems=increment_x)
    assert 45 == test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['x']

# Check that ClientRequestToken can be a string between 1 and 36 characters,
# anything else is rejected.
# Note that there is no reliable way to check the 1 character case: If we
# repeat the same test twice in a 10 minute period and it uses a different
# table name, we can't reuse the same ClientRequestToken or we'll get a
# IdempotentParameterMismatch error. We must use a random token - and
# a random one-character string is not enough to avoid test flakiness.
@pytest.mark.xfail(reason="#5064 - transactions not yet supported")
def test_transact_write_items_clientrequesttoken_length(test_table_s):
    p = random_string()
    item = {'p': p}
    transaction = [{'Put': {'TableName': test_table_s.name, 'Item': item}}]
    # 36 characters work
    test_table_s.meta.client.transact_write_items(
        ClientRequestToken=random_string(length=36),
        TransactItems=transaction)
    # 37 characters is too long for ClientRequestToken, a ValidationException
    # results:
    with pytest.raises(ClientError, match='ValidationException.*[cC]lientRequestToken'):
        test_table_s.meta.client.transact_write_items(
            ClientRequestToken=random_string(length=37),
            TransactItems=transaction)

# Check that if the same ClientRequestToken is used for two transactions,
# but they are different, an IdempotentParameterMismatch error is thrown.
@pytest.mark.xfail(reason="#5064 - transactions not yet supported")
def test_transact_write_items_clientrequesttoken_mismatch(test_table_s):
    transaction1 = [{'Put': {'TableName': test_table_s.name, 'Item': {'p': random_string()}}}]
    transaction2 = [{'Put': {'TableName': test_table_s.name, 'Item': {'p': random_string()}}}]
    token = random_string()
    test_table_s.meta.client.transact_write_items(
        ClientRequestToken=token, TransactItems=transaction1)
    with pytest.raises(ClientError, match='IdempotentParameterMismatchException'):
        test_table_s.meta.client.transact_write_items(
            ClientRequestToken=token, TransactItems=transaction2)

# All tests above involved a transaction with a single action. Here
# we finally begin to check trasactions with multiple actions. Let's
# begin with a successful TransactWriteItems transaction, where some of
# the actions have successful conditions, and some don't have conditions
# at all, and all the actions are performed.
@pytest.mark.xfail(reason="#5064 - transactions not yet supported")
def test_transact_write_items_multi_action_true(test_table_s):
    p1 = random_string()
    p2 = random_string()
    p3 = random_string()
    item1 = {'p': p1, 'x': 'dog'}
    item2 = {'p': p2, 'x': 'cat'}
    test_table_s.meta.client.transact_write_items(TransactItems=[
        # unconditional Put
        { 'Put': {
            'TableName': test_table_s.name,
            'Item': item1
        }},
        # Put with true condition (attribute_not_exists(p) is true
        # when the item doesn't exist).
        { 'Put': {
            'TableName': test_table_s.name,
            'Item': item2,
            'ConditionExpression': 'attribute_not_exists(p)'
        }},
        # ConditionCheck with true condition. It is true, but doesn't
        # cause anything to be written for key p3.
        { 'ConditionCheck': {
            'TableName': test_table_s.name,
            'Key': {'p': p3},
            'ConditionExpression': 'attribute_not_exists(p)'
        }},
        ])
    # p1 and p2 supposed to have been written by the transaction, but p3
    # wasn't because it was only involved in a ConditionCheck.
    assert item1 == test_table_s.get_item(Key={'p': p1}, ConsistentRead=True)['Item']
    assert item2 == test_table_s.get_item(Key={'p': p2}, ConsistentRead=True)['Item']
    assert 'Item' not in test_table_s.get_item(Key={'p': p3}, ConsistentRead=True)

# Test a transaction with several actions, one of which has a false
# condition. The entire transaction should fail, and none of its actions
# (not even those with true conditions or no conditions) should be performed.
@pytest.mark.xfail(reason="#5064 - transactions not yet supported")
def test_transact_write_items_multi_action_false(test_table_s):
    p1 = random_string()
    p2 = random_string()
    p3 = random_string()
    item1 = {'p': p1, 'x': 'dog'}
    item2 = {'p': p2, 'x': 'cat'}
    with pytest.raises(ClientError, match='TransactionCanceledException'):
        test_table_s.meta.client.transact_write_items(TransactItems=[
            # unconditional Put
            { 'Put': {
                'TableName': test_table_s.name,
                'Item': item1
            }},
            # Put with true condition (attribute_not_exists(p) is true
            # when the item doesn't exist).
            { 'Put': {
                'TableName': test_table_s.name,
                'Item': item2,
                'ConditionExpression': 'attribute_not_exists(p)'
            }},
            # ConditionCheck with a *false* condition. It should cause the
            # entire transaction to be rejected.
            { 'ConditionCheck': {
                'TableName': test_table_s.name,
                'Key': {'p': p3},
                'ConditionExpression': 'attribute_exists(p)'
            }},
            ])
    # Because the entire transaction was rejected, none of its actions
    # should have been done. Items for p1, p2 and p3 should all not exist.
    assert 'Item' not in test_table_s.get_item(Key={'p': p1}, ConsistentRead=True)
    assert 'Item' not in test_table_s.get_item(Key={'p': p2}, ConsistentRead=True)
    assert 'Item' not in test_table_s.get_item(Key={'p': p3}, ConsistentRead=True)

# Test that it's not allowed for two actions in the same transaction to
# target the same item (this limitation also includes ConditionCheck
# actions).
@pytest.mark.xfail(reason="#5064 - transactions not yet supported")
def test_transact_write_items_multi_action_conflict(test_table_s):
    p1 = random_string()
    p2 = random_string()
    with pytest.raises(ClientError, match='ValidationException.*one item'):
        test_table_s.meta.client.transact_write_items(TransactItems=[
            { 'Put': {
                'TableName': test_table_s.name,
                'Item': {'p': p1}
            }},
            { 'Put': {
                'TableName': test_table_s.name,
                'Item': {'p': p1}
            }}])
    with pytest.raises(ClientError, match='ValidationException.*one item'):
        test_table_s.meta.client.transact_write_items(TransactItems=[
            { 'Put': {
                'TableName': test_table_s.name,
                'Item': {'p': p1}
            }},
            { 'ConditionCheck': {
                'TableName': test_table_s.name,
                'Key': {'p': p1},
                'ConditionExpression': 'attribute_not_exists(p)'
            }}])
    with pytest.raises(ClientError, match='ValidationException.*one item'):
        test_table_s.meta.client.transact_write_items(TransactItems=[
            { 'Put': {
                'TableName': test_table_s.name,
                'Item': {'p': p1}
            }},
            { 'ConditionCheck': {
                'TableName': test_table_s.name,
                'Key': {'p': p2},
                'ConditionExpression': 'attribute_not_exists(p)'
            }},
            { 'ConditionCheck': {
                'TableName': test_table_s.name,
                'Key': {'p': p2},
                'ConditionExpression': 'attribute_not_exists(p)'
            }}])

# Test that a transaction may involve more than one table, not just more than
# one item.
@pytest.mark.xfail(reason="#5064 - transactions not yet supported")
def test_transact_write_items_multi_table_true(test_table_s, test_table_ss):
    p1 = random_string()
    p2 = random_string()
    c2 = random_string()
    item1 = {'p': p1, 'x': 'dog'}
    item2 = {'p': p2, 'c': c2, 'x': 'cat'}
    test_table_s.meta.client.transact_write_items(TransactItems=[
        # unconditional Put on the first table
        { 'Put': {
            'TableName': test_table_s.name,
            'Item': item1
        }},
        # Put with true condition on the second table
        { 'Put': {
            'TableName': test_table_ss.name,
            'Item': item2,
            'ConditionExpression': 'attribute_not_exists(p)'
        }}
        ])
    # p1 and p2 supposed to have been written by the transaction on the
    # two different tables
    assert item1 == test_table_s.get_item(Key={'p': p1}, ConsistentRead=True)['Item']
    assert item2 == test_table_ss.get_item(Key={'p': p2, 'c': c2}, ConsistentRead=True)['Item']

# Check that a TransactWriteItems with zero items is not allowed.
@pytest.mark.xfail(reason="#5064 - transactions not yet supported")
def test_transact_write_items_empty(test_table_s):
    with pytest.raises(ClientError, match='ValidationException.*[tT]ransactItems'):
        test_table_s.meta.client.transact_write_items(TransactItems=[])

# Check that a TransactWriteItems with 100 items is allowed. The limit
# used to be just 25 items, but it was increased to 100 in September 2022.
# The next test will check that *more* than 100 items is not allowed.
@pytest.mark.xfail(reason="#5064 - transactions not yet supported")
def test_transact_write_items_100(test_table_s):
    p = random_string()
    items = [{'p': p + str(i), 'x': i} for i in range(100)]
    test_table_s.meta.client.transact_write_items(TransactItems=[
        { 'Put': {
            'TableName': test_table_s.name,
            'Item': item,
        }} for item in items])
    # verify that all 100 items went in
    for item in items:
        assert item == test_table_s.get_item(Key={'p': item['p']}, ConsistentRead=True)['Item']

# Check that a transaction with 101 (>100) items is rejected.
@pytest.mark.xfail(reason="#5064 - transactions not yet supported")
def test_transact_write_items_101(test_table_s):
    p = random_string()
    with pytest.raises(ClientError, match='ValidationException.*[tT]ransactItems.*100'):
        test_table_s.meta.client.transact_write_items(TransactItems=[
            { 'Put': {
                'TableName': test_table_s.name,
                'Item': {'p': p + str(i)},
            }} for i in range(101)])

# Beyond limiting a TransactWriteItems to 100 items (verified in tests above)
# DynamoDB has an aditional limit that "the aggregate size of the items in the
# transaction cannot exceed 4 MB". We can't write 5 MB in a transaction even
# if we build it from relatively small items (each not exceeding the 400 KB
# limit the size of an individual item size).
# Note that this test checks only Put actions which include the entire
# new item in the transaction - so the transaction itself reaches 5MB in
# size. In the next test we will check what happens for Update or Delete
# actions which may themselves have small size but refer to large items.
@pytest.mark.xfail(reason="#5064 - transactions not yet supported")
def test_transact_write_items_put_5MB(test_table_s):
    p = random_string()
    # We will write 50 items of roughly 100KB each, reaching around 5MB
    long = 'x'*100000
    items = [{'p': p + str(i), 'x': long} for i in range(50)]
    with pytest.raises(ClientError, match='ValidationException.*size cannot exceed'):
        test_table_s.meta.client.transact_write_items(TransactItems=[
            { 'Put': {
                'TableName': test_table_s.name,
                'Item': item,
            }} for item in items])

# In the previous test (test_transact_write_items_put_5MB) we saw that we
# can't have over 4 MB of new data in a transaction. But the DynamoDB
# TransactWriteItems documentation is ambiguous on what this 4 MB limit
# applies to: The documentation says that it's not allowed that "the aggregate
# size of the items in the transaction exceeds 4 MB". Is this the size of the
# items *mentioned* in the transaction, or the size of items *in* the
# transaction? In other words, if we have a  transaction doing Deletes of
# large items - where the transaction itself is very small but the total item
# size exceeds 4 MB - is this allowed or not?
# It turns out that the answer is that the size of the transaction request
# (DynamoDB refers to it as "transaction payload" in the error messages) is
# what matters, not the size of the items. A delete transaction that deletes
# 5 MB of items but the transaction itself is small - is allowed.
@pytest.mark.xfail(reason="#5064 - transactions not yet supported")
def test_transact_write_items_delete_5MB(test_table_s):
    p = random_string()
    # Write (not in a transaction) 50 items of roughly 100 KB each, so their
    # total size is roughly 5 MB.
    long = 'x'*100000
    items = [{'p': p + str(i), 'x': long} for i in range(50)]
    with test_table_s.batch_writer() as batch:
        for item in items:
            batch.put_item(item)
    # Now try to delete those 50 items in one transaction. The transaction
    # request is very small, but the total size of the items to be deleted
    # is around 5 MB. This transaction *is* allowed showing that DynamoDB's
    # limit is really on the transaction size, not the "aggregate size of the
    # items" mentioned in the documentation.
    try:
        test_table_s.meta.client.transact_write_items(TransactItems=[
            { 'Delete': {
                'TableName': test_table_s.name,
                'Key': {'p': item['p']},
            }} for item in items])
        # Verify the deletes worked
        for item in items:
            assert 'Item' not in test_table_s.get_item(Key={'p': item['p']}, ConsistentRead=True)
    except ClientError as e:
        # A 5 MB write transaction takes roughly 10,000 WCUs, which DynamoDB
        # sometimes refuses to do on a brand-new on-demand table, so the
        # transaction gets rejected. But if this happens, it's an expected
        # failure. It's not a real failure (a real failure would have been
        # the transaction failing on something other than ThrottlingException).
        if e.response['Error']['Code'] == 'ThrottlingException':
            pytest.xfail()
        else:
            raise

# But verify that aggregate transaction size of 3MB is fine. Note that
# individual items are limited to 400KB, so we must a transaction with
# several smaller items.
@pytest.mark.xfail(reason="#5064 - transactions not yet supported")
def test_transact_write_items_put_3MB(test_table_s):
    p = random_string()
    # We will write 30 items of roughly 100KB each, reaching around 3MB
    long = 'x'*100000
    items = [{'p': p + str(i), 'x': long} for i in range(30)]
    test_table_s.meta.client.transact_write_items(TransactItems=[
        { 'Put': {
            'TableName': test_table_s.name,
            'Item': item,
        }} for item in items])
    # verify that all 30 items went in
    for item in items:
        assert item == test_table_s.get_item(Key={'p': item['p']}, ConsistentRead=True)['Item']


# In various tests above we checked that failed conditions result in a
# TransactionCanceledException. But this error comes with CancellationReasons
# and in the following test we want to test these.
# Note that this test only checks the cancellation reasons "None" and
# "ConditionalCheckFailed". The test after it will check the "ValidationError"
# reason. There are additional reasons, but these tests are not designed to
# be able to reach those cases:
# 1. ItemCollectionSizeLimitExceeded - when there is an LSI and the write
#    caused the item collection (Scylla partition) to go over 10 GB.
# 2. TransactionConflict - when there is a conflict with another transaction.
#    As explained above, we don't test concurrent transactions in this test
#    suite.
# 3. ProvisionedThroughputExceeded - self-explanatory, not tested in these
#    tests.
# 4. ThrottlingError - similar, for on-demand tables that haven't scaled
#    enough yet.
@pytest.mark.xfail(reason="#5064 - transactions not yet supported")
def test_transact_write_cancellation_reasons_conditionalcheckfailed(test_table_s):
    p = random_string()
    with pytest.raises(ClientError, match='TransactionCanceledException') as e:
        test_table_s.meta.client.transact_write_items(TransactItems=[{
            'Put': {
                'TableName': test_table_s.name,
                'Item': {'p': p},
                # The condition attribute_exists(p) is false for an
                # item that doesn't exist
                'ConditionExpression': 'attribute_exists(p)'
            }}])
    # The error object should have a CancellationReasons member. Our
    # transaction has one item, and one failure, so the CancellationReasons
    # should be a one-member array. This member has a code, and a message.
    assert 'CancellationReasons' in e.value.response
    reasons = e.value.response['CancellationReasons']
    assert len(reasons) == 1
    assert reasons[0]['Code'] == 'ConditionalCheckFailed'
    # The Message is a user-readable message like "The conditional request
    # failed". We'll assert it exists, but not insist what the text is
    assert 'Message' in reasons[0]

    # Now do the same thing with a transaction with multiple actions, some
    # with failed conditions so the entire transaction gets canceled.
    # We'll see that the CancellationReasons lists each one of the actions
    # in order, and says which ones had a failed condition.
    p1 = random_string()
    p2 = random_string()
    p3 = random_string()
    p4 = random_string()
    with pytest.raises(ClientError, match='TransactionCanceledException') as e:
        test_table_s.meta.client.transact_write_items(TransactItems=[
            # Unconditional Put
            { 'Put': {
                'TableName': test_table_s.name,
                'Item': {'p': p1}
            }},
            # Put with true condition (attribute_not_exists(p) is true
            # when the item doesn't exist).
            { 'Put': {
                'TableName': test_table_s.name,
                'Item': {'p': p2},
                'ConditionExpression': 'attribute_not_exists(p)'
            }},
            # ConditionCheck with a *false* condition. It should cause the
            # entire transaction to be rejected.
            { 'ConditionCheck': {
                'TableName': test_table_s.name,
                'Key': {'p': p3},
                'ConditionExpression': 'attribute_exists(p)'
            }},
            # Another false condition, on a Put action:
            { 'Put': {
                'TableName': test_table_s.name,
                'Item': {'p': p4},
                'ConditionExpression': 'attribute_exists(p)'
            }},
            ])
    assert 'CancellationReasons' in e.value.response
    reasons = e.value.response['CancellationReasons']
    # The CancellationReasons are listed in the same order of the actions
    # in the transaction - we expect the first two actions to have not
    # failed, so they have Code="None", and the third and forth both failed.
    # It's interesting that DynamoDB does report both failures - and doesn't
    # "short circuit" after the first one.
    assert len(reasons) == 4
    assert reasons[0]['Code'] == 'None'
    assert reasons[1]['Code'] == 'None'
    assert reasons[2]['Code'] == 'ConditionalCheckFailed'
    assert reasons[3]['Code'] == 'ConditionalCheckFailed'
    # Actions "None" reason don't come with a user-readable message,
    # but others do.
    assert 'Message' not in reasons[0]
    assert 'Message' not in reasons[1]
    assert 'Message' in reasons[2]
    assert 'Message' in reasons[3]

# Another possible cancellation reason is a ValidationError in one of the
# actions - for example an attempt to write a wrong type into a key or
# exceeding the size limit on a key or the size of an item, or exceeding the
# nesting level limits.
# Note that the actions are validated before any of them actually run,
# so if any action has a ValidationError, other correctly-validated
# actions will return a "None" reason - even if potentially they should
# have been a false condition.
# In fact it turns out (and we'll confirm in this test) that if two actions
# have a validation error, only the first error is detected, and the server
# doesn't continue to find the validation error in the second action. It's
# probably not important that Alternator be compatible with this behavior,
# but if we can, why not.
#
# Surprisingly, many validations actually return a ValidationException
# on the entire transaction instead of a TransactionCanceledException with
# one ValidationError in its cancellation reason. For example (we have a
# test for this below), a reference to a missing ExpressionAttributeNames.
# It's not clear how DynamoDB decided which case will return a
# ValidationException and which a ValidationError, but let's be compatible
# with what DynamoDB does.
@pytest.mark.xfail(reason="#5064 - transactions not yet supported")
def test_transact_write_cancellation_reasons_validationerror(test_table_s):
    p = random_string()
    with pytest.raises(ClientError, match='TransactionCanceledException') as e:
        test_table_s.meta.client.transact_write_items(TransactItems=[
            { 'Put': {
                'TableName': test_table_s.name,
                # The key p should be a string, not a number, so this
                # item has a validation error.
                'Item': {'p': 7}
            }},
            { 'Put': {
                'TableName': test_table_s.name,
                # A second validation error. As explained above, as soon
                # as the first validation error was found above, this action
                # will not be reached and its validation error will not be
                # detected. So we expect "None" reason on this action.
                'Item': {'p': 8}
            }},
            { 'Put': {
                'TableName': test_table_s.name,
                # This condition is false (the item does *not* exist yet),
                # and would have failed the tranaction, but the server won't
                # even get around to checking that because of the validation
                # error in the other actions. So we will expect to get a
                # "None" reason on this action.
                'ConditionExpression': 'attribute_exists(p)',
                'Item': {'p': p}
            }},
            ])
    assert 'CancellationReasons' in e.value.response
    reasons = e.value.response['CancellationReasons']
    assert len(reasons) == 3
    assert reasons[0]['Code'] == 'ValidationError'
    assert reasons[1]['Code'] == 'None'
    assert reasons[2]['Code'] == 'None'
    assert 'Message' in reasons[0]
    # "None" reason doesn't come with a message:
    assert 'Message' not in reasons[1]
    assert 'Message' not in reasons[2]

# Verify that even if there is just *one* action in the transaction,
# on certain types of errors like the incorrect key type we used above
# in test_transact_write_cancellation_reasons_validationerror will get a
# TransactionCanceledException with a single ValidationError - not a
# ValidationException. Below we'll see in other tests that this is not
# always true - in some other types of errors, we actually do get a
# ValidationException.
@pytest.mark.xfail(reason="#5064 - transactions not yet supported")
def test_transact_write_cancellation_reasons_validationerror_one(test_table_s):
    with pytest.raises(ClientError, match='TransactionCanceledException') as e:
        test_table_s.meta.client.transact_write_items(TransactItems=[
            { 'Put': {
                'TableName': test_table_s.name,
                # The key p should be a string, not a number, so this
                # item has a validation error.
                'Item': {'p': 7}
            }}])
    reasons = e.value.response['CancellationReasons']
    assert reasons[0]['Code'] == 'ValidationError'
    assert 'Message' in reasons[0]

# Check the ReturnValuesOnConditionCheckFailure parameter, which allows
# the user to retrieve the content of the item that caused a specific
# condition to fail.
# Note that ReturnValuesOnConditionCheckFailure is specified for a
# specific action, not for the entire transaction.
@pytest.mark.xfail(reason="#5064 - transactions not yet supported")
def test_transact_write_items_returnvaluesonconditioncheckfailure(test_table_s):
    p = random_string()
    item = {'p': p, 'x': 42, 'y': 'dog'}
    test_table_s.put_item(Item=item)
    # Check ReturnValuesOnConditionCheckFailure="ALL_OLD"
    with pytest.raises(ClientError, match='TransactionCanceledException') as e:
        test_table_s.meta.client.transact_write_items(TransactItems=[{
            'Update': {
                'ReturnValuesOnConditionCheckFailure': 'ALL_OLD',
                'TableName': test_table_s.name,
                'Key': {'p': p},
                # x is really 42, not 43, so this condition is false
                'ConditionExpression': 'x = :fourtythree',
                'UpdateExpression': 'SET x = x + :one',
                'ExpressionAttributeValues': {':one': 1, ':fourtythree': 43},
            }}])
    # The relevant CancellationReasons will now have an "Item".
    # The returned reasons[0]['Item'] has DynamoDB JSON encoding, and in
    # this specific place (inside the error object) boto3 forgot to
    # deserialize the JSON encoding for us (like it does automatically
    # in other place) - so we need to do this deserialization manually
    # with TypeDeserializer before we can compare it to the expected "item".
    assert 'CancellationReasons' in e.value.response
    reasons = e.value.response['CancellationReasons']
    assert len(reasons) == 1
    assert reasons[0]['Code'] == 'ConditionalCheckFailed'
    assert 'Message' in reasons[0]
    deserializer = TypeDeserializer()
    got_item = {x:deserializer.deserialize(y) for (x,y) in reasons[0]['Item'].items()}
    assert item == got_item

    # The same transaction with  ReturnValuesOnConditionCheckFailure="NONE"
    # doesn't return the Item:
    with pytest.raises(ClientError, match='TransactionCanceledException') as e:
        test_table_s.meta.client.transact_write_items(TransactItems=[{
            'Update': {
                'ReturnValuesOnConditionCheckFailure': 'NONE',
                'TableName': test_table_s.name,
                'Key': {'p': p},
                'ConditionExpression': 'x = :fourtythree',
                'UpdateExpression': 'SET x = x + :one',
                'ExpressionAttributeValues': {':one': 1, ':fourtythree': 43},
            }}])
    reasons = e.value.response['CancellationReasons']
    assert len(reasons) == 1
    assert reasons[0]['Code'] == 'ConditionalCheckFailed'
    assert 'Item' not in reasons[0]

    # Setting ReturnValuesOnConditionCheckFailure to anything else, e.g.,
    # lowercase 'none', or 'dog', is a validation error. Surprisingly,
    # even though ReturnValuesOnConditionCheckFailure is per-action, and
    # DynamoDB can return TransactionCanceledException with a per-action
    # ValidationError, in this case it doesn't do that - it returns a
    # ValidationException for the entire request.
    for option in ['none', 'dog']:
        with pytest.raises(ClientError, match='ValidationException.*[rR]eturnValuesOnConditionCheckFailure'):
            test_table_s.meta.client.transact_write_items(TransactItems=[{
                'Update': {
                    'ReturnValuesOnConditionCheckFailure': option,
                    'TableName': test_table_s.name,
                    'Key': {'p': p},
                    'ConditionExpression': 'x = :fourtythree',
                    'UpdateExpression': 'SET x = x + :one',
                    'ExpressionAttributeValues': {':one': 1, ':fourtythree': 43},
                }}])

# Various checks for ExpressionAttributeValues and ExpressionAttributeNames
# in a ConditionExpression, such as missing or unused entries in those arrays.
# We already have tests for ConditionExpression in test_condition_expression.py
# but here the problem happens inside one action instead of the whole
# transaction so we need to check if it's also recognized, and whether it
# causes a TransactionCanceledException with one ValidationError or a
# ValidationException on the entire transaction. It turns out that it's
# the latter.

# Check ConditionExpression reference to missing ExpressionAttributeNames
@pytest.mark.xfail(reason="#5064 - transactions not yet supported")
def test_transact_write_items_missing_name(test_table_s):
    p = random_string()
    with pytest.raises(ClientError, match='ValidationException.*#xyz'):
        test_table_s.meta.client.transact_write_items(TransactItems=[
            { 'Put': {
                'TableName': test_table_s.name,
                'Item': {'p': p},
                # The reference "#xyz" is missing in ExpressionAttributeNames
                'ConditionExpression': 'attribute_exists(#xyz)'
            }}])

# Check ConditionExpression value missing in ExpressionAttributeValues
@pytest.mark.xfail(reason="#5064 - transactions not yet supported")
def test_transact_write_items_missing_value(test_table_s):
    p = random_string()
    with pytest.raises(ClientError, match='ValidationException.*:xyz'):
        test_table_s.meta.client.transact_write_items(TransactItems=[
            { 'Put': {
                'TableName': test_table_s.name,
                'Item': {'p': p},
                # The reference ":xyz" is missing in ExpressionAttributeValues
                'ConditionExpression': 'p = :xyz'
            }}])

# Check unused name in ExpressionAttributeName
@pytest.mark.xfail(reason="#5064 - transactions not yet supported")
def test_transact_write_items_unused_name(test_table_s):
    p = random_string()
    with pytest.raises(ClientError, match='ValidationException.*unused.*#xyz'):
        test_table_s.meta.client.transact_write_items(TransactItems=[
            { 'Put': {
                'TableName': test_table_s.name,
                'Item': {'p': p},
                'ConditionExpression': 'attribute_exists(p)',
                # The name "#xyz" isn't used in in any expression
                'ExpressionAttributeNames': {'#xyz': 'x'}
            }}])

# Check unused value in ExpressionAttributeValues
@pytest.mark.xfail(reason="#5064 - transactions not yet supported")
def test_transact_write_items_unused_value(test_table_s):
    p = random_string()
    with pytest.raises(ClientError, match='ValidationException.*unused.*:xyz'):
        test_table_s.meta.client.transact_write_items(TransactItems=[
            { 'Put': {
                'TableName': test_table_s.name,
                'Item': {'p': p},
                'ConditionExpression': 'attribute_exists(p)',
                # The value ":xyz" isn't used in in any expression
                'ExpressionAttributeValues': {':xyz': 7}
            }}])

# Syntax error in a ConditionExpression in one action also returns a
# ValidationException for the entire transaction, not per item:
@pytest.mark.xfail(reason="#5064 - transactions not yet supported")
def test_transact_write_items_syntax_error(test_table_s):
    p = random_string()
    with pytest.raises(ClientError, match='ValidationException.*Syntax error'):
        test_table_s.meta.client.transact_write_items(TransactItems=[
            { 'Put': {
                'TableName': test_table_s.name,
                'Item': {'p': p},
                # "hello!" is a syntax error as an expression
                'ConditionExpression': 'hello!',
            }}])

# Some other errors in a ConditionExpression, like an ExpressionAttributeNames
# with an integer instead of a string (an attribute name), can generate a
# a SerializationException instead of a ValidationException. It's not clear
# to me why this is important, and I think it's fine if Alternator would
# return a ValidationException here and we change the test to accept both.
# But the important point is that the single exception is returned for the
# entire transaction - not per item.
@pytest.mark.xfail(reason="#5064 - transactions not yet supported")
def test_transact_write_items_serialization_exception(test_table_s):
    p = random_string()
    with pytest.raises(ClientError, match='SerializationException'):
        test_table_s.meta.client.transact_write_items(TransactItems=[
            { 'Put': {
                'TableName': test_table_s.name,
                'Item': {'p': p},
                'ConditionExpression': 'attribute_exists(p)',
                # ExpressionAttributeNames expects strings (names of
                # attributes), not the integer 7.
                'ExpressionAttributeNames': {'#xyz': 7}
            }}])

##########################################################################
# Tests for TransactGetItems functionality
##########################################################################

# Test basic transaction with one "Get" action
@pytest.mark.xfail(reason="#5064 - transactions not yet supported")
def test_transact_get_items_one(test_table_s):
    p = random_string()
    x = random_string()
    item = {'p': p, 'x': x}
    test_table_s.put_item(Item=item)
    ret = test_table_s.meta.client.transact_get_items(TransactItems=[
        { 'Get': {
            'TableName': test_table_s.name,
            'Key': {'p': p},
            }}])
    assert 'Responses' in ret
    assert len(ret['Responses']) == 1
    assert 'Item' in ret['Responses'][0]
    assert item == ret['Responses'][0]['Item']

# If a Get transaction can't find an item with the given key, it's not
# an error - one of the Responses entries will just not have an "Item":
@pytest.mark.xfail(reason="#5064 - transactions not yet supported")
def test_transact_get_items_missing(test_table_s):
    p = random_string()
    ret = test_table_s.meta.client.transact_get_items(TransactItems=[
        { 'Get': {
            'TableName': test_table_s.name,
            'Key': {'p': p},
            }}])
    assert 'Responses' in ret
    assert len(ret['Responses']) == 1
    assert 'Item' not in ret['Responses'][0]

# Test the ProjectionExpression parameter for a Get action, asking not to
# get the entire item and rather just get specific attributes. See more
# extensive tests for ProjectionExpression in test_projection_expression.py.
@pytest.mark.xfail(reason="#5064 - transactions not yet supported")
def test_transact_get_items_projection_expression(test_table_s):
    p = random_string()
    item = {'p': p, 'x': 1, 'y': 2, 'z': 3}
    test_table_s.put_item(Item=item)
    ret = test_table_s.meta.client.transact_get_items(TransactItems=[
        { 'Get': {
            'TableName': test_table_s.name,
            'Key': {'p': p},
            'ProjectionExpression': 'x,z'
            }}])
    assert 'Responses' in ret
    assert len(ret['Responses']) == 1
    assert 'Item' in ret['Responses'][0]
    assert {'x': item['x'], 'z': item['z']} == ret['Responses'][0]['Item']

# ProjectionExpression also supports ExpressionAttributeNames.
@pytest.mark.xfail(reason="#5064 - transactions not yet supported")
def test_transact_get_items_projection_expression(test_table_s):
    p = random_string()
    item = {'p': p, 'x': 1, 'y': 2, 'z': 3}
    test_table_s.put_item(Item=item)
    ret = test_table_s.meta.client.transact_get_items(TransactItems=[
        { 'Get': {
            'TableName': test_table_s.name,
            'Key': {'p': p},
            'ProjectionExpression': '#xx,#zz',
            'ExpressionAttributeNames': {'#xx': 'x', '#zz': 'z'}
            }}])
    assert 'Responses' in ret
    assert len(ret['Responses']) == 1
    assert 'Item' in ret['Responses'][0]
    assert {'x': item['x'], 'z': item['z']} == ret['Responses'][0]['Item']

# If ExpressionAttributeNames is missing a name, or has an unused name,
# it's an error. As we saw above in other cases, it's a ValidationException
# for the entire transaction - not a TransactionCanceledException.
@pytest.mark.xfail(reason="#5064 - transactions not yet supported")
def test_transact_get_items_unused_expressionattributenames(test_table_s):
    p = random_string()
    with pytest.raises(ClientError, match='ValidationException.*unused.*#qq'):
        ret = test_table_s.meta.client.transact_get_items(TransactItems=[
            { 'Get': {
                'TableName': test_table_s.name,
                'Key': {'p': p},
                'ProjectionExpression': '#xx,#zz',
                # The reference "#qq" isn't used in the ProjectionExpression
                'ExpressionAttributeNames': {'#xx': 'x', '#zz': 'z', '#qq': 'q'}
            }}])

@pytest.mark.xfail(reason="#5064 - transactions not yet supported")
def test_transact_get_items_missing_expressionattributenames(test_table_s):
    p = random_string()
    with pytest.raises(ClientError, match='ValidationException.*#zz'):
        ret = test_table_s.meta.client.transact_get_items(TransactItems=[
            { 'Get': {
                'TableName': test_table_s.name,
                'Key': {'p': p},
                'ProjectionExpression': '#xx,#zz',
                # The reference "#zz" is missing in ExpressionAttributeNames
                'ExpressionAttributeNames': {'#xx': 'x'}
            }}])

# Test the ability to read multiple items in one transaction. We can actually
# read 100 small items in one transaction - the limit used to be just 25
# items, but it was increased to 100 in September 2022 so let's verify that
# it works.
@pytest.mark.xfail(reason="#5064 - transactions not yet supported")
def test_transact_get_items_100(test_table_s):
    p = random_string()
    items = [{'p': p + str(i), 'x': i} for i in range(100)]
    with test_table_s.batch_writer() as batch:
        for item in items:
            batch.put_item(item)
    ret = test_table_s.meta.client.transact_get_items(TransactItems=[
        { 'Get': {
            'TableName': test_table_s.name,
            'Key': {'p': item['p']},
        }} for item in items])
    # verify that all 100 items were read
    for item in items:
        assert item == test_table_s.get_item(Key={'p': item['p']}, ConsistentRead=True)['Item']
    assert 'Responses' in ret
    assert len(ret['Responses']) == 100
    for i, response in enumerate(ret['Responses']):
        assert 'Item' in response
        assert response['Item'] == items[i]

# A transaction with 100 read actions is the limit, and 101 are not allowed:
@pytest.mark.xfail(reason="#5064 - transactions not yet supported")
def test_transact_get_items_101(test_table_s):
    p = random_string()
    with pytest.raises(ClientError, match='ValidationException.*[tT]ransactItems.*100'):
        test_table_s.meta.client.transact_get_items(TransactItems=[
            { 'Get': {
                'TableName': test_table_s.name,
                'Key': {'p': str(i)},
            }} for i in range(101)])
