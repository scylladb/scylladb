# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

#############################################################################
# Tests for various limits, which did not fit naturally into other test files
#############################################################################

import pytest
from .util import random_string, new_test_table, full_query
from botocore.exceptions import ClientError
from .test_gsi import assert_index_query

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
# Strangely, this limitation is not explictly mentioned in the DynamoDB
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
# limited to 255 bytes. This is explicilty mentioned in the DynamoDB
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
@pytest.mark.xfail(reason="limits on expression length not yet enforced")
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

# TODO: additional expression limits documented in DynamoDB's documentation
# that we should test here:
# * a limit on the length of attribute or value references (#name or :val) -
#   the reference together with the first character (# or :) is limited to
#   255 bytes.
# * the sum of length of ExpressionAttributeValues and ExpressionAttributeNames
#   is limited to 2MB (not a very interesting limit...)
# * a limit on the number of operator or functions in an expression: 300
#   (not a very interesting limit...)

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
