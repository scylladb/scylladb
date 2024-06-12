# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

#############################################################################
# Tests for various limits, which did not fit naturally into other test files
#############################################################################

import pytest
from botocore.exceptions import ClientError

from test.alternator.util import random_string, new_test_table, full_query
from test.alternator.test_gsi import assert_index_query


#############################################################################
# The following tests check the limits on attribute name lengths.
# According to the DynamoDB documentation, attribute names are usually
# limited to 64K bytes, and the only exceptions are:
# 1. Secondary index partition/sort key names are limited to 255 characters.
# 2. In LSI, attributes listed for projection.
# We'll test all these cases below in several separate tests.
# We found a additional exceptions - the base-table key names are also limited
# to 255 bytes, and the expiration-time column given to UpdateTimeToLive is
# also limited to 255 character. We test the last fact in a different test
# file: test_ttl.py::test_update_ttl_errors.

# Attribute length test 1: non-key attribute names below 64KB are usable in
# PutItem, UpdateItem, GetItem, and also in various expressions (condition,
# update and projection) and their archaic pre-expression alternatives.
def test_limit_attribute_length_nonkey_good(test_table_s):
    p = random_string()
    too_long_name = random_string(64)*1024
    long_name = too_long_name[:-1]
    # Try legal long_name:
    test_table_s.put_item(Item={'p': p, long_name: 1, 'another': 2 })
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, long_name: 1, 'another': 2 }

    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True,
        ProjectionExpression='#name', ExpressionAttributeNames={'#name': long_name})['Item'] == {long_name: 1}
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True,
        AttributesToGet=[long_name])['Item'] == {long_name: 1}

    test_table_s.update_item(Key={'p': p}, AttributeUpdates={long_name: {'Value': 2, 'Action': 'PUT'}})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, long_name: 2, 'another': 2 }
    test_table_s.update_item(Key={'p': p}, UpdateExpression='SET #name = :val',
        ExpressionAttributeNames={'#name': long_name},
        ExpressionAttributeValues={':val': 3})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, long_name: 3, 'another': 2 }
    test_table_s.update_item(Key={'p': p}, UpdateExpression='SET #name = #name+:val',
        ExpressionAttributeNames={'#name': long_name},
        ExpressionAttributeValues={':val': 1})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, long_name: 4, 'another': 2 }
    test_table_s.update_item(Key={'p': p}, UpdateExpression='SET #name = #name+:val',
        ConditionExpression='#name = :oldval',
        ExpressionAttributeNames={'#name': long_name},
        ExpressionAttributeValues={':val': 1, ':oldval': 4})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, long_name: 5, 'another': 2 }

# Attribute length test 2: attribute names 64KB or above generate an error
# in the aforementioned cases. Note that contrary to what the DynamoDB
# documentation suggests, the length 64KB itself is not allowed - 65535
# (which we tested above) is the last accepted size.
# Reproduces issue #9169.
@pytest.mark.xfail(reason="issue #9169: attribute name limits not enforced")
def test_limit_attribute_length_nonkey_bad(test_table_s):
    p = random_string()
    too_long_name = random_string(64)*1024
    with pytest.raises(ClientError, match='ValidationException.*Attribute name'):
        test_table_s.put_item(Item={'p': p, too_long_name: 1})
    with pytest.raises(ClientError, match='ValidationException.*Attribute name'):
        test_table_s.get_item(Key={'p': p}, ProjectionExpression='#name',
            ExpressionAttributeNames={'#name': too_long_name})
    with pytest.raises(ClientError, match='ValidationException.*Attribute name'):
        test_table_s.get_item(Key={'p': p}, AttributesToGet=[too_long_name])
    with pytest.raises(ClientError, match='ValidationException.*Attribute name'):
        test_table_s.update_item(Key={'p': p}, AttributeUpdates={too_long_name: {'Value': 2, 'Action': 'PUT'}})
    with pytest.raises(ClientError, match='ValidationException.*Attribute name'):
        test_table_s.update_item(Key={'p': p}, UpdateExpression='SET #name = :val',
            ExpressionAttributeNames={'#name': too_long_name},
            ExpressionAttributeValues={':val': 3})
    with pytest.raises(ClientError, match='ValidationException.*Attribute name'):
        test_table_s.update_item(Key={'p': p}, UpdateExpression='SET a = :val',
            ConditionExpression='#name = :val',
            ExpressionAttributeNames={'#name': too_long_name},
            ExpressionAttributeValues={':val': 1})

# Attribute length test 3: Test that *key* (hash and range) attribute names
# up to 255 characters are allowed. In the test below we'll see that larger
# sizes aren't allowed.
def test_limit_attribute_length_key_good(dynamodb):
    long_name1 = random_string(255)
    long_name2 = random_string(255)
    with new_test_table(dynamodb,
            KeySchema=[ { 'AttributeName': long_name1, 'KeyType': 'HASH' },
                        { 'AttributeName': long_name2, 'KeyType': 'RANGE' } ],
            AttributeDefinitions=[
                 { 'AttributeName': long_name1, 'AttributeType': 'S' },
                 { 'AttributeName': long_name2, 'AttributeType': 'S' }]) as table:
        table.put_item(Item={long_name1: 'hi', long_name2: 'ho', 'another': 2 })
        assert table.get_item(Key={long_name1: 'hi', long_name2: 'ho'}, ConsistentRead=True)['Item'] == {long_name1: 'hi', long_name2: 'ho', 'another': 2 }

# Attribute length test 4: Test that *key* attribute names more than 255
# characters are not allowed - not for hash key and not for range key.
# Strangely, this limitation is not explicitly mentioned in the DynamoDB
# documentation - which only mentions that SI keys are limited to 255 bytes,
# but forgets to mention base-table keys.
# Reproduces issue #9169.
@pytest.mark.xfail(reason="issue #9169: attribute name limits not enforced")
def test_limit_attribute_length_key_bad(dynamodb):
    too_long_name = random_string(256)
    with pytest.raises(ClientError, match='ValidationException.*length'):
        with new_test_table(dynamodb,
            KeySchema=[ { 'AttributeName': too_long_name, 'KeyType': 'HASH' } ],
            AttributeDefinitions=[ { 'AttributeName': too_long_name, 'AttributeType': 'S' } ]) as table:
            pass
    with pytest.raises(ClientError, match='ValidationException.*length'):
        with new_test_table(dynamodb,
            KeySchema=[ { 'AttributeName': 'x', 'KeyType': 'HASH',
                          'AttributeName': too_long_name, 'KeyType': 'RANGE' }, ],
            AttributeDefinitions=[ { 'AttributeName': too_long_name, 'AttributeType': 'S' },
                                   { 'AttributeName': 'x', 'AttributeType': 'S' } ]) as table:
            pass

# Attribute length tests 5,6: similar as the above tests for the 255-byte
# limit for base table length, here we check that the same limit also applies
# to key columns in GSI and LSI.
def test_limit_attribute_length_gsi_lsi_good(dynamodb):
    long_name1 = random_string(255)
    long_name2 = random_string(255)
    long_name3 = random_string(255)
    long_name4 = random_string(255)
    with new_test_table(dynamodb,
            KeySchema=[ { 'AttributeName': long_name1, 'KeyType': 'HASH' },
                        { 'AttributeName': long_name2, 'KeyType': 'RANGE' } ],
            AttributeDefinitions=[
                 { 'AttributeName': long_name1, 'AttributeType': 'S' },
                 { 'AttributeName': long_name2, 'AttributeType': 'S' },
                 { 'AttributeName': long_name3, 'AttributeType': 'S' },
                 { 'AttributeName': long_name4, 'AttributeType': 'S' }],
            GlobalSecondaryIndexes=[
                 { 'IndexName': 'gsi', 'KeySchema': [
                      { 'AttributeName': long_name3, 'KeyType': 'HASH' },
                      { 'AttributeName': long_name4, 'KeyType': 'RANGE' },
                   ], 'Projection': { 'ProjectionType': 'ALL' }
                 }
            ],
            LocalSecondaryIndexes=[
                 { 'IndexName': 'lsi', 'KeySchema': [
                      { 'AttributeName': long_name1, 'KeyType': 'HASH' },
                      { 'AttributeName': long_name4, 'KeyType': 'RANGE' },
                   ], 'Projection': { 'ProjectionType': 'ALL' }
                 }
            ]) as table:
        table.put_item(Item={long_name1: 'hi', long_name2: 'ho', long_name3: 'dog', long_name4: 'cat' })
        assert table.get_item(Key={long_name1: 'hi', long_name2: 'ho'}, ConsistentRead=True)['Item'] == {long_name1: 'hi', long_name2: 'ho', long_name3: 'dog', long_name4: 'cat' }
        # Verify the content through the indexes. LSI can use ConsistentRead
        # but GSI might need to retry to find the content:
        assert full_query(table, IndexName='lsi', ConsistentRead=True,
            KeyConditions={
              long_name1: {'AttributeValueList': ['hi'], 'ComparisonOperator': 'EQ'},
              long_name4: {'AttributeValueList': ['cat'], 'ComparisonOperator': 'EQ'},
            }) == [{long_name1: 'hi', long_name2: 'ho', long_name3: 'dog', long_name4: 'cat'}]
        assert_index_query(table, 'gsi',
            [{long_name1: 'hi', long_name2: 'ho', long_name3: 'dog', long_name4: 'cat'}],
            KeyConditions={
              long_name3: {'AttributeValueList': ['dog'], 'ComparisonOperator': 'EQ'},
              long_name4: {'AttributeValueList': ['cat'], 'ComparisonOperator': 'EQ'},
            })

# Reproduces issue #9169.
@pytest.mark.xfail(reason="issue #9169: attribute name limits not enforced")
def test_limit_attribute_length_gsi_lsi_bad(dynamodb):
    too_long_name = random_string(256)
    with pytest.raises(ClientError, match='ValidationException.*length'):
        with new_test_table(dynamodb,
            KeySchema=[ { 'AttributeName': 'a', 'KeyType': 'HASH' },
                        { 'AttributeName': 'b', 'KeyType': 'RANGE' } ],
            AttributeDefinitions=[
                 { 'AttributeName': 'a', 'AttributeType': 'S' },
                 { 'AttributeName': 'b', 'AttributeType': 'S' },
                 { 'AttributeName': too_long_name, 'AttributeType': 'S' } ],
            GlobalSecondaryIndexes=[
                 { 'IndexName': 'gsi', 'KeySchema': [
                      { 'AttributeName': too_long_name, 'KeyType': 'HASH' },
                   ], 'Projection': { 'ProjectionType': 'ALL' }
                 }
            ]) as table:
            pass
    with pytest.raises(ClientError, match='ValidationException.*length'):
        with new_test_table(dynamodb,
            KeySchema=[ { 'AttributeName': 'a', 'KeyType': 'HASH' },
                        { 'AttributeName': 'b', 'KeyType': 'RANGE' } ],
            AttributeDefinitions=[
                 { 'AttributeName': 'a', 'AttributeType': 'S' },
                 { 'AttributeName': 'b', 'AttributeType': 'S' },
                 { 'AttributeName': too_long_name, 'AttributeType': 'S' } ],
            LocalSecondaryIndexes=[
                 { 'IndexName': 'lsi', 'KeySchema': [
                      { 'AttributeName': 'a', 'KeyType': 'HASH' },
                      { 'AttributeName': too_long_name, 'KeyType': 'RANGE' },
                   ], 'Projection': { 'ProjectionType': 'ALL' }
                 }
            ]) as table:
            pass

# Attribute length tests 7,8: In an LSI, projected attribute names are also
# limited to 255 bytes. This is explicitly mentioned in the DynamoDB
# documentation. For GSI this is also true (but not explicitly mentioned).
# This limitation is only true to attributes *explicitly* projected by name -
# attributes projected as part as ALL can be bigger (up to the usual 64KB
# limit).
# Reproduces issue #9169.
@pytest.mark.xfail(reason="issue #9169: attribute name limits not enforced")
def test_limit_attribute_length_gsi_lsi_projection_bad(dynamodb):
    too_long_name = random_string(256)
    with pytest.raises(ClientError, match='ValidationException.*length'):
        with new_test_table(dynamodb,
            KeySchema=[ { 'AttributeName': 'a', 'KeyType': 'HASH' },
                        { 'AttributeName': 'b', 'KeyType': 'RANGE' } ],
            AttributeDefinitions=[
                 { 'AttributeName': 'a', 'AttributeType': 'S' },
                 { 'AttributeName': 'b', 'AttributeType': 'S' },
                 { 'AttributeName': 'c', 'AttributeType': 'S' } ],
            GlobalSecondaryIndexes=[
                 { 'IndexName': 'gsi', 'KeySchema': [
                      { 'AttributeName': 'c', 'KeyType': 'HASH' },
                   ], 'Projection': { 'ProjectionType': 'INCLUDE',
                                      'NonKeyAttributes': [too_long_name]}
                 }
            ]) as table:
            pass
    with pytest.raises(ClientError, match='ValidationException.*length'):
        with new_test_table(dynamodb,
            KeySchema=[ { 'AttributeName': 'a', 'KeyType': 'HASH' },
                        { 'AttributeName': 'b', 'KeyType': 'RANGE' } ],
            AttributeDefinitions=[
                 { 'AttributeName': 'a', 'AttributeType': 'S' },
                 { 'AttributeName': 'b', 'AttributeType': 'S' },
                 { 'AttributeName': 'c', 'AttributeType': 'S' } ],
            LocalSecondaryIndexes=[
                 { 'IndexName': 'lsi', 'KeySchema': [
                      { 'AttributeName': 'a', 'KeyType': 'HASH' },
                      { 'AttributeName': 'c', 'KeyType': 'RANGE' },
                   ], 'Projection': { 'ProjectionType': 'INCLUDE',
                                      'NonKeyAttributes': [too_long_name]}
                 }
            ]) as table:
            pass

# Above we tested asking to project a specific column which has very long
# name, and failed the table creation. Here we show that a GSI/LSI which
# projects ALL, and has some attribute names with >255 but lower than the
# normal attribute name limit of 64KB, gets projected fine.
def test_limit_attribute_length_gsi_lsi_projection_all(dynamodb):
    too_long_name = random_string(256)
    with new_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'a', 'KeyType': 'HASH' },
                    { 'AttributeName': 'b', 'KeyType': 'RANGE' }
        ],
        AttributeDefinitions=[
             { 'AttributeName': 'a', 'AttributeType': 'S' },
             { 'AttributeName': 'b', 'AttributeType': 'S' },
             { 'AttributeName': 'c', 'AttributeType': 'S' }
        ],
        GlobalSecondaryIndexes=[
             { 'IndexName': 'gsi', 'KeySchema': [
                  { 'AttributeName': 'c', 'KeyType': 'HASH' },
               ], 'Projection': { 'ProjectionType': 'ALL' }
             },
        ],
        LocalSecondaryIndexes=[
             { 'IndexName': 'lsi', 'KeySchema': [
                  { 'AttributeName': 'a', 'KeyType': 'HASH' },
                  { 'AttributeName': 'c', 'KeyType': 'RANGE' },
               ], 'Projection': { 'ProjectionType': 'ALL' }
             }
        ]) as table:
        # As we tested above, there is no problem adding a non-key attribute
        # which has a >255 byte name. This is true even if this attribute is
        # implicitly copied to the GSI or LSI by the ProjectionType=ALL.
        table.put_item(Item={'a': 'hi', 'b': 'ho', 'c': 'dog', too_long_name: 'cat' })
        assert table.get_item(Key={'a': 'hi', 'b': 'ho'}, ConsistentRead=True)['Item'] == {'a': 'hi', 'b': 'ho', 'c': 'dog', too_long_name: 'cat' }
        # GSI cannot use ConsistentRead so we may need to retry the read, so
        # we reuse a function that does this
        assert_index_query(table, 'gsi',
            [{'a': 'hi', 'b': 'ho', 'c': 'dog', too_long_name: 'cat'}],
            KeyConditions={'c': {'AttributeValueList': ['dog'],
            'ComparisonOperator': 'EQ'}})
        # LSI can use ConsistentRead:
        assert full_query(table, IndexName='lsi', ConsistentRead=True,
            KeyConditions={
              'a': {'AttributeValueList': ['hi'], 'ComparisonOperator': 'EQ'},
              'c': {'AttributeValueList': ['dog'], 'ComparisonOperator': 'EQ'},
            }) == [{'a': 'hi', 'b': 'ho', 'c': 'dog', too_long_name: 'cat'}]

#############################################################################
# The following tests test various limits of expressions
# (ProjectionExpression, ConditionExpression, UpdateExpression and
# FilterExpression) as documented in
# https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html

# The maximum string length of any of the expression parameters is 4 KB.
# Check that the length 4096 is allowed, 4097 isn't - on all four expression
# types.
def test_limit_expression_len(test_table_s):
    p = random_string()
    string4096 = 'x'*4096
    string4097 = 'x'*4097
    # ProjectionExpression:
    test_table_s.get_item(Key={'p': p}, ProjectionExpression=string4096)
    with pytest.raises(ClientError, match='ValidationException.*ProjectionExpression'):
        test_table_s.get_item(Key={'p': p}, ProjectionExpression=string4097)
    # UpdateExpression:
    spaces4085 = ' '*4085
    test_table_s.update_item(Key={'p': p}, UpdateExpression=f'SET{spaces4085}a = :val',
        ExpressionAttributeValues={':val': 1})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': 1}
    with pytest.raises(ClientError, match='ValidationException.*UpdateExpression'):
        test_table_s.update_item(Key={'p': p}, UpdateExpression=f'SET {spaces4085}a = :val',
            ExpressionAttributeValues={':val': 1})
    # ConditionExpression:
    test_table_s.update_item(Key={'p': p}, UpdateExpression='SET a = :newval',
        ExpressionAttributeValues={':newval': 2, ':oldval': 1},
        ConditionExpression=f'a{spaces4085} = :oldval')
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': 2}
    with pytest.raises(ClientError, match='ValidationException.*ConditionExpression'):
        test_table_s.update_item(Key={'p': p}, UpdateExpression='SET a = :newval',
            ExpressionAttributeValues={':newval': 3, ':oldval': 2},
            ConditionExpression=f'a {spaces4085} = :oldval')
    # FilterExpression:
    assert full_query(test_table_s, ConsistentRead=True,
            KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}},
            FilterExpression=f'a{spaces4085} = :theval',
            ExpressionAttributeValues={':theval': 2}
        ) == [{'p': p, 'a': 2}]
    with pytest.raises(ClientError, match='ValidationException.*FilterExpression'):
        full_query(test_table_s, ConsistentRead=True,
            KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}},
            FilterExpression=f'a {spaces4085} = :theval',
            ExpressionAttributeValues={':theval': 2})

# The previous test (test_limit_expression_len) makes the 4096-byte length
# limit of expressions appear very benign - so what if we accept a 10,000-byte
# expression? Issue #14473 shows one potential harm of long expressions:
# A long expression can also be deeply nested, and recursive algorithms for
# parsing or handling these expressions can cause Scylla to crash. The
# following tests test_limit_expression_len_crash*() used to crash Scylla
# before the expression length limit was enforced (issue #14473).
# These tests use ConditionExpression to demonstrate the problem.
def test_limit_expression_len_crash1(test_table_s):
    # a<b and (a<b and (a<b and (a<b and (a<b and (...))))):
    p = random_string()
    test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'a': {'Value': 1, 'Action': 'PUT'},
                              'b': {'Value': 2, 'Action': 'PUT'}})
    depth = 20000
    condition = "a<b " + "and (a<b "*depth +")"*depth
    # For this expression longer than 4096 bytes, DynamoDB produces the
    # error "Invalid ConditionExpression: Expression size has exceeded the
    # maximum allowed size; expression size: 200004". Scylla used to crash
    # here (after very deep recursion) instead of a clean error.
    with pytest.raises(ClientError, match='ValidationException.*expression size'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :val',
                ConditionExpression=condition,
                ExpressionAttributeValues={':val': 1})

def test_limit_expression_len_crash2(test_table_s):
    # (((((((((((((((a<b)))))))))))))))
    p = random_string()
    test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'a': {'Value': 1, 'Action': 'PUT'},
                              'b': {'Value': 2, 'Action': 'PUT'}})
    depth = 20000
    condition = "("*depth + "a<b" + ")"*depth
    with pytest.raises(ClientError, match='ValidationException.*expression size'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :val',
                ConditionExpression=condition,
                ExpressionAttributeValues={':val': 1})

def test_limit_expression_len_crash3(test_table_s):
    # ((((((((((((((((((((((((((( - a syntax error
    p = random_string()
    test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'a': {'Value': 1, 'Action': 'PUT'},
                              'b': {'Value': 2, 'Action': 'PUT'}})
    condition = "("*15000
    # Although this expression is a syntax error, the fact it is too long
    # should be recognized first.
    with pytest.raises(ClientError, match='ValidationException.*expression size'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :val',
                ConditionExpression=condition,
                ExpressionAttributeValues={':val': 1})

def test_limit_expression_len_crash4(test_table_s):
    # not not not not not ... not a<b
    p = random_string()
    test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'a': {'Value': 1, 'Action': 'PUT'},
                              'b': {'Value': 2, 'Action': 'PUT'}})
    depth = 20000
    condition = "not "*depth + "a<b"
    with pytest.raises(ClientError, match='ValidationException.*expression size'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :val',
                ConditionExpression=condition,
                ExpressionAttributeValues={':val': 1})

def test_limit_expression_len_crash5(test_table_s):
    # a < f(f(f(f(...(b)))))
    p = random_string()
    test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'a': {'Value': 1, 'Action': 'PUT'},
                              'b': {'Value': 2, 'Action': 'PUT'}})
    depth = 20000
    condition = "a < " + "f("*depth + "b" + ")"*depth
    with pytest.raises(ClientError, match='ValidationException.*expression size'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :val',
                ConditionExpression=condition,
                ExpressionAttributeValues={':val': 1})

# The above tests test_limit_expression_len_crash* checked various cases
# where very long (>4096 bytes) and very deeply nested expressions caused
# Scylla to crash. We now need to check check that expressions in the
# allowed length (4096 bytes), even if deeply nested, work fine.
def test_deeply_nested_expression_1(test_table_s):
    # ((((((((((((((((((((((((((( - a syntax error
    p = random_string()
    test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'a': {'Value': 1, 'Action': 'PUT'},
                              'b': {'Value': 2, 'Action': 'PUT'}})
    condition = "(" * 4096
    with pytest.raises(ClientError, match='ValidationException.*ConditionExpression'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :val',
                ConditionExpression=condition,
                ExpressionAttributeValues={':val': 1})

# Continuing the above test, check that Alternator prints a normal "syntax
# error" for just "(((" but a "expression nested too deeply" for 4096
# parentheses. This is a Scylla-only test - DynamoDB doesn't make
# this distinction, and the specific error message is not important.
# But I wanted to test that Alternator's error messages are as designed.
def test_deeply_nested_expression_1a(test_table_s, scylla_only):
    p = random_string()
    with pytest.raises(ClientError, match='ValidationException.*ConditionExpression.*syntax error'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :x', ExpressionAttributeValues={':x': 1},
            ConditionExpression='(((')
    with pytest.raises(ClientError, match='ValidationException.*ConditionExpression.*expression nested too deeply'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :x', ExpressionAttributeValues={':x': 1},
            ConditionExpression='('*4000)

# Even if an expression is shorter than 4096 bytes, DynamoDB can reject
# if it has more than 300 "operators" - in the expression below we fit
# 909 operators ("<" and "or") under 4096 bytes. DynamoDB rejects this
# case with the message "Invalid ConditionExpression: The expression
# contains too many operators; operator count: 301". Scylla currently
# doesn't have this specific limit, but it rejects this expression because
# it exceeds nesting depth MAX_DEPTH. The important thing is that the
# expression is rejected cleanly, without crashing as it used to happen
# on longer expressions.
def test_deeply_nested_expression_2(test_table_s):
    # a<b or (a<b or (a<b or (a<b or (...)))):
    p = random_string()
    test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'a': {'Value': 1, 'Action': 'PUT'},
                              'b': {'Value': 2, 'Action': 'PUT'}})
    # depth=149 has a total of 299 "<" and "or" operators so should work.
    # Importantly, parentheses and spaces are *not* counted among the
    # "operators", only the "<" and "or".
    depth = 149
    condition = "a<b " + "or (a<b "*depth +")"*depth
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :val',
            ConditionExpression=condition,
            ExpressionAttributeValues={':val': 1})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 1
    # depth=454 is still below 4096 bytes, but rejected by DynamoDB because
    # it has more than 300 operators, and by Scylla because 454 > MAX_DEPTH.
    depth = 454
    condition = "a<b " + "or (a<b "*depth +")"*depth
    assert len(condition) < 4096
    with pytest.raises(ClientError, match='ValidationException.*ConditionExpression'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :val',
                ConditionExpression=condition,
                ExpressionAttributeValues={':val': 1})

# Another deeply-recursive expression, (((((((((a<b))))))))), that used to
# crash Scylla when the expression was very long but shouldn't crash it
# for expressions shorter than the 4096-byte limit.
# Currently, DynamoDB and Scylla reject this case with different reasons -
# DynamoDB complains that "Invalid ConditionExpression: The expression has
# redundant parentheses", and Scylla stops parsing after recursing too
# deeply (MAX_DEPTH) and reports "Failed parsing ConditionExpression".
# The really important thing is Scylla doesn't crash on this expression (as
# it used to before implementing MAX_DEPTH).
def test_deeply_nested_expression_3(test_table_s):
    # (((((((((((((((a<b)))))))))))))))
    p = random_string()
    test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'a': {'Value': 1, 'Action': 'PUT'},
                              'b': {'Value': 2, 'Action': 'PUT'}})
    depth = 2046
    condition = "("*depth + "a<b" + ")"*depth
    assert len(condition) < 4096
    with pytest.raises(ClientError, match='ValidationException.*ConditionExpression'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :val',
                ConditionExpression=condition,
                ExpressionAttributeValues={':val': 2})

# Another example of a deeply-nested expression which is shorter than
# the 4096-byte limit, but has more than 300 operators (it has 1000 "NOT"
# operators), so DynamoDB rejects it and Scylla rejects it because the
# recursion is deeper than MAX_DEPTH. Of course the more interesting
# observation is that it doesn't crash Scylla during parsing.
def test_deeply_nested_expression_4(test_table_s):
    # not not not not ... not not a<b
    p = random_string()
    test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'a': {'Value': 1, 'Action': 'PUT'},
                              'b': {'Value': 2, 'Action': 'PUT'}})
    depth = 1000 # even, so condition is equivalent to just a<b
    condition = "not "*depth + "a<b"
    assert len(condition) < 4096
    with pytest.raises(ClientError, match='ValidationException.*ConditionExpression'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :val',
                ConditionExpression=condition,
                ExpressionAttributeValues={':val': 2})

# Another example of a deeply-nested expression shorter than the limit of
# 4096 bytes, a < f(f(f(f(...(b))))). DynamoDB apparently doesn't count
# function calls as "operations", so it isn't limited to 300 nested calls.
# But it should print the correct error message, and obviously not crash.
def test_deeply_nested_expression_5(test_table_s):
    # a < f(f(f(f(...(b)))))
    p = random_string()
    test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'a': {'Value': 1, 'Action': 'PUT'},
                              'b': {'Value': 2, 'Action': 'PUT'}})
    depth = 1355
    condition = "a < " + "f("*depth + "b" + ")"*depth
    assert len(condition) < 4096
    # DynamoDB prints: "Invalid ConditionExpression: Invalid function name;
    # function: f". Scylla fails parsing this expression because the depth
    # exceeds MAX_DEPTH. We think this is fine.
    with pytest.raises(ClientError, match='ValidationException.*ConditionExpression'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :val',
                ConditionExpression=condition,
                ExpressionAttributeValues={':val': 2})

    # a < size(size(size(size(...size(b)))))
    # Here the function exists, but the innermost size returns an integer,
    # which the surrounding size() doesn't like (it's not defined on an int)
    # In Scylla it is still rejected because of MAX_DEPTH.
    depth = 680
    condition = "a < " + "size("*depth + "b" + ")"*depth
    assert len(condition) < 4096
    with pytest.raises(ClientError, match='ValidationException.*ConditionExpression'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :val',
                ConditionExpression=condition,
                ExpressionAttributeValues={':val': 2})

# TODO: additional expression limits documented in DynamoDB's documentation
# that we should test here:
# * a limit on the length of attribute or value references (#name or :val) -
#   the reference together with the first character (# or :) is limited to
#   255 bytes.
# * the sum of length of ExpressionAttributeValues and ExpressionAttributeNames
#   is limited to 2MB (not a very interesting limit...)

#############################################################################

# DynamoDB documentation says that the sort key must be between 1 and 1024
# bytes in length. We already test (test_item.py::test_update_item_empty_key)
# that 0 bytes are not allowed, so here we want to verify that 1024 is
# indeed the limit - i.e., 1024 is allowed, 1025 is isn't. This is true for
# both strings and bytes (and for bytes, it is the actual bytes - not their
# base64 encoding - that is counted).
# We may decide that this test never needs to pass on Alternator, because
# we may adopt a different limit. In this case we'll need to document this
# decision. In any case, Alternator must have some key-length limits (the
# internal implementation limits key length to 64 KB), so the test after this
# one should pass.
@pytest.mark.xfail(reason="issue #10347: sort key limits not enforced")
def test_limit_sort_key_len_1024(test_table_ss, test_table_sb):
    p = random_string()
    # String sort key with length 1024 is fine:
    key = {'p': p, 'c': 'x'*1024}
    test_table_ss.put_item(Item=key)
    assert test_table_ss.get_item(Key=key, ConsistentRead=True)['Item'] == key
    # But sort key with length 1025 is forbidden - in both read and write.
    # DynamoDB's message says "Aggregated size of all range keys has exceeded
    # the size limit of 1024 bytes". It's not clear what "all range keys"
    # actually refers to, as there can be only one. We investigate this
    # further below in test_limit_sort_key_len_lsi().
    key = {'p': p, 'c': 'x'*1025}
    with pytest.raises(ClientError, match='ValidationException.*limit'):
        test_table_ss.put_item(Item=key)
    with pytest.raises(ClientError, match='ValidationException.*limit'):
        test_table_ss.get_item(Key=key, ConsistentRead=True)

    # The same limits are true for the bytes type. The length of a bytes
    # array is its real length - not the length of its base64 encoding.
    key = {'p': p, 'c': bytearray([123]*1024)}
    test_table_sb.put_item(Item=key)
    assert test_table_sb.get_item(Key=key, ConsistentRead=True)['Item'] == key
    key = {'p': p, 'c': bytearray([123]*1025)}
    with pytest.raises(ClientError, match='ValidationException.*limit'):
        test_table_sb.put_item(Item=key)
    with pytest.raises(ClientError, match='ValidationException.*limit'):
        test_table_sb.get_item(Key=key, ConsistentRead=True)

# This is a variant of the above test, where we don't insist that the
# sort key length limit must be exactly 1024 bytes as in DynamoDB, but
# that it be *at least* 1024. I.e., we verify that 1024-byte sort keys
# are allowed, while very long keys that surpass Scylla's low-level
# key-length limit (64 KB) are forbidden with an appropriate error message
# and not an "internal server error". This test should pass even if
# Alternator decides to adopt a different sort-key-length limit from
# DynamoDB. We do have to adopt *some* limit because the internal Scylla
# implementation has a 64 KB limit on key lengths.
@pytest.mark.xfail(reason="issue #10347: sort key limits not enforced")
def test_limit_sort_key_len(test_table_ss, test_table_sb):
    p = random_string()
    # String sort key with length 1024 is fine:
    key = {'p': p, 'c': 'x'*1024}
    test_table_ss.put_item(Item=key)
    assert test_table_ss.get_item(Key=key, ConsistentRead=True)['Item'] == key
    # Sort key of length 64 KB + 1 is forbidden - it obviously exceeds
    # DynamoDB's limit (1024 bytes), but also exceeds Scylla's internal
    # limit on key length (64 KB). We except to get a reasonable error
    # on request validation - not some "internal server error".
    key = {'p': p, 'c': 'x'*65537}
    with pytest.raises(ClientError, match='ValidationException.*limit'):
        test_table_ss.put_item(Item=key)
    with pytest.raises(ClientError, match='ValidationException.*limit'):
        test_table_ss.get_item(Key=key, ConsistentRead=True)

    # The same limits are true for the bytes type. The length of a bytes
    # array is its real length - not the length of its base64 encoding.
    key = {'p': p, 'c': bytearray([123]*1024)}
    test_table_sb.put_item(Item=key)
    assert test_table_sb.get_item(Key=key, ConsistentRead=True)['Item'] == key
    key = {'p': p, 'c': bytearray([123]*65537)}
    with pytest.raises(ClientError, match='ValidationException.*limit'):
        test_table_sb.put_item(Item=key)
    with pytest.raises(ClientError, match='ValidationException.*limit'):
        test_table_sb.get_item(Key=key, ConsistentRead=True)

# As mentioned above, DynamoDB's error about sort key length exceeding the
# 1024 byte limit says that "Aggregated size of all range keys has exceeded
# the size limit of 1024 bytes". This is an odd message, considering that
# there can only be one range key... So there is a question whether when we
# have an LSI and several of the item's attributes become range keys (of
# different tables), perhaps their *total* length is limited. It turns out
# the answer is no. We can write an item with two 1024-byte attributes, where
# one if the base table's sort key and the other is an LSI's sort key.
# DyanamoDB's error message appears to be nothing more than a mistake.
def test_limit_sort_key_len_lsi(dynamodb):
    with new_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'a', 'KeyType': 'HASH' },
                    { 'AttributeName': 'b', 'KeyType': 'RANGE' }
        ],
        AttributeDefinitions=[
             { 'AttributeName': 'a', 'AttributeType': 'S' },
             { 'AttributeName': 'b', 'AttributeType': 'S' },
             { 'AttributeName': 'c', 'AttributeType': 'S' }
        ],
        LocalSecondaryIndexes=[
             { 'IndexName': 'lsi', 'KeySchema': [
                  { 'AttributeName': 'a', 'KeyType': 'HASH' },
                  { 'AttributeName': 'c', 'KeyType': 'RANGE' },
               ], 'Projection': { 'ProjectionType': 'ALL' }
             }
        ]) as table:
        item = {'a': 'hello', 'b': 'x'*1024, 'c': 'y'*1024 }
        table.put_item(Item=item)
        assert table.get_item(Key={'a': 'hello', 'b': 'x'*1024}, ConsistentRead=True)['Item'] == item
        assert table.query(IndexName='lsi', KeyConditions={'a': {'AttributeValueList': ['hello'], 'ComparisonOperator': 'EQ'}, 'c': {'AttributeValueList': ['y'*1024], 'ComparisonOperator': 'EQ'}}, ConsistentRead=True)['Items'] == [item]

# DynamoDB documentation says that the partition key must be between 1 and 2048
# bytes in length. We already test (test_item.py::test_update_item_empty_key)
# that 0 bytes are not allowed, so here we want to verify that 2048 is
# indeed the limit - i.e., 2048 is allowed, 2049 is isn't. This is true for
# both strings and bytes (and for bytes, it is the actual bytes - not their
# base64 encoding - that is counted).
# We may decide that this test never needs to pass on Alternator, because
# we may adopt a different limit. In this case we'll need to document this
# decision. In any case, Alternator must have some key-length limits (the
# internal implementation limits key length to 64 KB), so even if this test
# won't pass, the one after it should pass.
@pytest.mark.xfail(reason="issue #10347: sort key limits not enforced")
def test_limit_partition_key_len_2048(test_table_s, test_table_b):
    # String partition key with length 2048 is fine:
    item = {'p': 'x'*2048, 'z': 'hello'}
    test_table_s.put_item(Item=item)
    assert test_table_s.get_item(Key={'p': 'x'*2048}, ConsistentRead=True)['Item'] == item
    # But partition key with length 2049 is forbidden - in both read and write.
    key = {'p': 'x'*2049}
    with pytest.raises(ClientError, match='ValidationException.*limit'):
        test_table_s.put_item(Item=key)
    with pytest.raises(ClientError, match='ValidationException.*limit'):
        test_table_s.get_item(Key=key, ConsistentRead=True)

    # The same limits are true for the bytes type. The length of a bytes
    # array is its real length - not the length of its base64 encoding.
    item = {'p': bytearray([123]*2048), 'z': 'hello'}
    test_table_b.put_item(Item=item)
    assert test_table_b.get_item(Key={'p': bytearray([123]*2048)}, ConsistentRead=True)['Item'] == item
    key = {'p': bytearray([123]*2049)}
    with pytest.raises(ClientError, match='ValidationException.*limit'):
        test_table_b.put_item(Item=key)
    with pytest.raises(ClientError, match='ValidationException.*limit'):
        test_table_b.get_item(Key=key, ConsistentRead=True)

# This is a variant of the above test, where we don't insist that the
# partition key length limit must be exactly 2048 bytes as in DynamoDB, but
# that it be *at least* 2048. I.e., we verify that 2048-byte sort keys
# are allowed, while very long keys that surpass Scylla's low-level
# key-length limit (64 KB) are forbidden with an appropriate error message
# and not an "internal server error". This test should pass even if
# Alternator decides to adopt a different sort-key-length limit from
# DynamoDB. We do have to adopt *some* limit because the internal Scylla
# implementation has a 64 KB limit on key lengths.
@pytest.mark.xfail(reason="issue #10347: sort key limits not enforced")
def test_limit_partition_key_len(test_table_s, test_table_b):
    # String partition key with length 2048 is fine:
    item = {'p': 'x'*2048, 'z': 'hello'}
    test_table_s.put_item(Item=item)
    assert test_table_s.get_item(Key={'p': 'x'*2048}, ConsistentRead=True)['Item'] == item
    # Partition key of length 64 KB + 1 is forbidden - it obviously exceeds
    # DynamoDB's limit (2048 bytes), but also exceeds Scylla's internal
    # limit on key length (64 KB). We except to get a reasonable error
    # on request validation - not some "internal server error".
    key = {'p': 'x'*65537}
    with pytest.raises(ClientError, match='ValidationException.*limit'):
        test_table_s.put_item(Item=key)
    with pytest.raises(ClientError, match='ValidationException.*limit'):
        test_table_s.get_item(Key=key, ConsistentRead=True)

    # The same limits are true for the bytes type. The length of a bytes
    # array is its real length - not the length of its base64 encoding.
    item = {'p': bytearray([123]*2048), 'z': 'hello'}
    test_table_b.put_item(Item=item)
    assert test_table_b.get_item(Key={'p': bytearray([123]*2048)}, ConsistentRead=True)['Item'] == item
    key = {'p': bytearray([123]*65537)}
    with pytest.raises(ClientError, match='ValidationException.*limit'):
        test_table_b.put_item(Item=key)
    with pytest.raises(ClientError, match='ValidationException.*limit'):
        test_table_b.get_item(Key=key, ConsistentRead=True)
