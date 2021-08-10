# Copyright 2021-present ScyllaDB
#
# This file is part of Scylla.
#
# Scylla is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Scylla is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with Scylla.  If not, see <http://www.gnu.org/licenses/>.

#############################################################################
# Tests for various limits, which did not fit naturally into other test files
#############################################################################

import pytest
from util import random_string, new_test_table, full_query
from botocore.exceptions import ClientError
from test_gsi import assert_index_query

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
