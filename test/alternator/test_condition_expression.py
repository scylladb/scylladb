# Copyright 2019-present ScyllaDB
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

# Tests for the ConditionExpression parameter which makes certain operations
# (PutItem, UpdateItem and DeleteItem) conditional on the existing attribute
# values. ConditionExpression is a newer and more powerful version of the
# older "Expected" syntax. That older syntax is tested by the separate
# test_condition_expression.py. Many of the tests there are very similar to
# the ones included here.

# NOTE: In this file, we use the b'xyz' syntax to represent DynamoDB's binary
# values. This syntax works as expected only in Python3. In Python2 it
# appears to work, but the "b" is actually ignored and the result is a normal
# string 'xyz'. That means that we end up testing the string type instead of
# the binary type as intended. So this test can run on Python2 but doesn't
# cover testing binary types. The test should be run in Python3 to ensure full
# coverage.

import pytest
from botocore.exceptions import ClientError
from util import random_string
from sys import version_info

# A helper function for changing write isolation policies
def set_write_isolation(table, isolation):
    got = table.meta.client.describe_table(TableName=table.name)['Table']
    arn =  got['TableArn']
    tags = [
        {
            'Key': 'system:write_isolation',
            'Value': isolation
        }
    ]
    table.meta.client.tag_resource(ResourceArn=arn, Tags=tags)

# A helper function to clear previous isolation tags
def clear_write_isolation(table):
    got = table.meta.client.describe_table(TableName=table.name)['Table']
    arn =  got['TableArn']
    table.meta.client.untag_resource(ResourceArn=arn, TagKeys=['system:write_isolation'])

# Most of the tests in this file check that the ConditionExpression
# parameter works for the UpdateItem operation. It should also work the
# same for the PutItem and DeleteItem operations, and we'll make a small
# effort to verify that at the end of the file.

# Somewhat pedanticly, DynamoDB forbids using new-style ConditionExpression
# together with old-style AttributeUpdates... ConditionExpression can only be
# used with UpdateExpression.
def test_condition_expression_attribute_updates(test_table_s):
    p = random_string()
    with pytest.raises(ClientError, match='ValidationException.*ConditionExpression'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'a': {'Value': 1, 'Action': 'PUT'}},
            ConditionExpression='a = :oldval',
            ExpressionAttributeValues={':oldval': 2})

# The following string of tests will test conditions composed of a single
# comparison of two attributes (as usual, each can be an attribute of the
# item or a constant from the request's ExpressionAttributeValues).
# All these tests involve top-level attribute - we'll test the possibility
# of directly-addressing nested attributes in a separate tests below.
# Additional tests below will check additional functions, as well as
# applying boolean logic (AND, OR, NOT, parentheses) on simpler conditions.
# In each case we have tests for the "true" case of the condition, meaning
# that the condition evaluates to true and the update is supposed to happen,
# and the "false" case, where the condition evaluates to false, so the update
# doesn't happen and we get a ConditionalCheckFailedException instead.

# Test for ConditionExpression with operator "=" (equality check):

# Check successful comparisons for values of all known types. 
# We test both the case comparing one of the item's attributes to an
# attribute from the request, and the case of comparing two different
# attributes of the same item (the latter case wasn't possible to express
# with Expected, and becomes possible with ConditionExpression).
def test_update_condition_eq_success(test_table_s):
    p = random_string()
    values = (1, "hello", True, b'xyz', None, ['hello', 42], {'hello': 'world'}, set(['hello', 'world']), set([1, 2, 3]), set([b'xyz', b'hi']))
    i = 0
    for val in values:
        i = i + 1
        print(val)
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'a': {'Value': val, 'Action': 'PUT'},
                              'b': {'Value': val, 'Action': 'PUT'}})
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET c = :i',
            ConditionExpression='a = b',
            ExpressionAttributeValues={':i': i})
        assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['c'] == i
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET d = :i',
            ConditionExpression='a = :val',
            ExpressionAttributeValues={':i': i, ':val': val})
        assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['d'] == i

# Comparing values of *different* types should always fail. Check all the
# combination of different types.
def test_update_condition_eq_different(test_table_s):
    p = random_string()
    values = (1, "hello", True, b'xyz', None, ['hello', 42], {'hello': 'world'}, set(['hello', 'world']), set([1, 2, 3]), set([b'xyz', b'hi']))
    for val1 in values:
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'a': {'Value': val1, 'Action': 'PUT'}})
        for val2 in values:
            print('testing {} {}'.format(val1, val2))
            # Frustratingly, Python considers True == 1, so we have to use
            # this ugly expression instead of the trivial val1 == val2
            if (val1 is True and val2 is True) or (not val1 is True and not val2 is True and val1 == val2):
                # Condition should succeed
                test_table_s.update_item(Key={'p': p},
                    UpdateExpression='SET a = :val1',
                    ConditionExpression='a = :val2',
                    ExpressionAttributeValues={':val1': val1, ':val2': val2})
                assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['a'] == val2
            else:
                # Condition should fail (different types)
                with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
                    test_table_s.update_item(Key={'p': p},
                        UpdateExpression='SET a = :val1',
                        ConditionExpression='a = :val2',
                        ExpressionAttributeValues={':val1': val1, ':val2': val2})

# Also check an actual case of same type, but inequality.
def test_update_condition_eq_unequal(test_table_s):
    p = random_string()
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Value': 1, 'Action': 'PUT'}})
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET a = :val1',
            ConditionExpression='a = :oldval',
            ExpressionAttributeValues={':val1': 3, ':oldval': 2})
    # If the attribute being compared doesn't exist, it's considered a failed
    # condition, not an error:
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET a = :val1',
            ConditionExpression='q = :oldval',
            ExpressionAttributeValues={':val1': 3, ':oldval': 2})

# In test_update_condition_eq_unequal() above we saw that a non-existent
# attribute is not "=" to a value. Here we check what happens when two
# non-existent attributes are checked for equality. It turns out, they should
# *not* be considered equal. In short, an unset attribute is never equal to
# anything - not even to another unset attribute.
# Reproduces issue #8511.
def test_update_condition_eq_two_unset(test_table_s):
    p = random_string()
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET a = :val1',
            ConditionExpression='q = z',
            ExpressionAttributeValues={':val1': 2})
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Value': 1, 'Action': 'PUT'}})
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET a = :val1',
            ConditionExpression='q = z',
            ExpressionAttributeValues={':val1': 3})

# Check that set equality is checked correctly. Unlike string equality (for
# example), it cannot be done with just naive string comparison of the JSON
# representation, and we need to allow for any order. (see issue #5021)
def test_update_condition_eq_set(test_table_s):
    p = random_string()
    # Because boto3 sorts the set values we give it, in order to generate a
    # set with a different order, we need to build it incrementally.
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Value': set(['dog', 'chinchilla']), 'Action': 'PUT'}})
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='ADD a :val1',
        ExpressionAttributeValues={':val1': set(['cat', 'mouse'])})
    # Sanity check - the attribute contains the set we think it does
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['a'] == set(['chinchilla', 'cat', 'dog', 'mouse'])
    # Now finally check that conditio expression check knows the equality too.
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET b = :val1',
        ConditionExpression='a = :oldval',
        ExpressionAttributeValues={':val1': 3, ':oldval': set(['chinchilla', 'cat', 'dog', 'mouse'])})
    assert 'b' in test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']

# The above test (test_update_condition_eq_set()) checked equality of simple
# set attributes. But an attributes can contain a nested document, where the
# set sits in a deep level (the set itself is a leaf in this heirarchy because
# it can only contain numbers, strings or bytes). We need to correctly support
# equality check in that case too.
# Reproduces issue #8514.
def test_update_condition_eq_nested_set(test_table_s):
    p = random_string()
    # Because boto3 sorts the set values we give it, in order to generate a
    # set with a different order, we need to build it incrementally.
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Value': {'b': 'c', 'd': ['e', 'f', set(['g', 'h'])], 'i': set(['j', 'k'])}, 'Action': 'PUT'}})
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='ADD a.d[2] :val1, a.i :val2',
        ExpressionAttributeValues={':val1': set(['l', 'm']), ':val2': set(['n', 'o'])})
    # Sanity check - the attribute contains the set we think it does
    expected = {'b': 'c', 'd': ['e', 'f', set(['g', 'h', 'l', 'm'])], 'i': set(['j', 'k', 'n', 'o'])}
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['a'] == expected
    # Now finally check that condition expression check knows the equality too.
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET b = :val1',
        ConditionExpression='a = :oldval',
        ExpressionAttributeValues={':val1': 3, ':oldval': expected})
    assert 'b' in test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']
    # Check that equality can also fail, if the inner set differs
    wrong = {'b': 'c', 'd': ['e', 'f', set(['g', 'h', 'l', 'bad'])], 'i': set(['j', 'k', 'n', 'o'])}
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET b = :val1',
            ConditionExpression='a = :oldval',
            ExpressionAttributeValues={':val1': 4, ':oldval': wrong})

# Test for ConditionExpression with operator "<>" (non-equality),
def test_update_condition_ne(test_table_s):
    p = random_string()
    # We only check here one type of attributes (numbers), assuming that the
    # inequality code calls the equality-check code which we checked in more
    # detail above.
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Value': 1, 'Action': 'PUT'},
                          'b': {'Value': 2, 'Action': 'PUT'}})
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET c = :newval',
        ConditionExpression='a <> :oldval',
        ExpressionAttributeValues={':newval': 2, ':oldval': 3})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['c'] == 2
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET c = :newval',
        ConditionExpression='a <> b',
        ExpressionAttributeValues={':newval': 1})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['c'] == 1
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET c = :newval',
            ConditionExpression='a <> c',
            ExpressionAttributeValues={':newval': 4})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['c'] == 1

    # If the types are different, this is considered "not equal":
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET c = :newval',
        ConditionExpression='a <> :oldval',
        ExpressionAttributeValues={':newval': 2, ':oldval': "1"})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['c'] == 2

    # If the attribute does not exist at all, this is also considered "not equal":
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET c = :newval',
        ConditionExpression='z <> :oldval',
        ExpressionAttributeValues={':newval': 3, ':oldval': 1})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['c'] == 3

# Check that set inequality is checked correctly. This reproduces the same
# bug #5021 that we reproduced above in test_update_condition_eq_set(), just
# that here we check the inequality operator instead of equality.
# Reproduces issue #8513.
def test_update_condition_ne_set(test_table_s):
    p = random_string()
    # Because boto3 sorts the set values we give it, in order to generate a
    # set with a different order, we need to build it incrementally.
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Value': set(['dog', 'chinchilla']), 'Action': 'PUT'}})
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='ADD a :val1',
        ExpressionAttributeValues={':val1': set(['cat', 'mouse'])})
    # Sanity check - the attribute contains the set we think it does
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['a'] == set(['chinchilla', 'cat', 'dog', 'mouse'])
    # Now check that condition expression check knows there is no inequality
    # here.
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET b = :val1',
            ConditionExpression='a <> :oldval',
            ExpressionAttributeValues={':val1': 2, ':oldval': set(['chinchilla', 'cat', 'dog', 'mouse'])})
    # As a sanity check, also check something which should be unequal:
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET b = :val1',
        ConditionExpression='a <> :oldval',
        ExpressionAttributeValues={':val1': 3, ':oldval': set(['chinchilla', 'cat', 'dog', 'horse'])})
    assert 'b' in test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']

# In test_update_condition_ne() above we saw that a non-existent attribute is
# "not equal" to any value. Here we check what happens when two non-existent
# attributes are checked for non-equality. It turns out, they are also
# considered "not equal". In short, an unset attribute is always "not equal" to
# anything - even to another unset attribute.
# Reproduces issue #8511.
def test_update_condition_ne_two_unset(test_table_s):
    p = random_string()
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET a = :val1',
        ConditionExpression='q <> z',
        ExpressionAttributeValues={':val1': 2})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['a'] == 2
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET a = :val1',
        ConditionExpression='q <> z',
        ExpressionAttributeValues={':val1': 3})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['a'] == 3

# Test for ConditionExpression with operator "<"
def test_update_condition_lt(test_table_s):
    p = random_string()
    # The < operator should work for string, number and binary types
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Value': 1, 'Action': 'PUT'},
                          'b': {'Value': 'cat', 'Action': 'PUT'},
                          'c': {'Value': b'cat', 'Action': 'PUT'}})
    # true cases:
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :newval',
        ConditionExpression='a < :oldval',
        ExpressionAttributeValues={':newval': 2, ':oldval': 2})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 2
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :newval',
        ConditionExpression='b < :oldval',
        ExpressionAttributeValues={':newval': 3, ':oldval': 'dog'})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 3
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :newval',
        ConditionExpression='c < :oldval',
        ExpressionAttributeValues={':newval': 4, ':oldval': b'dog'})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 4
    # false cases:
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :newval',
            ConditionExpression='a < :oldval',
            ExpressionAttributeValues={':newval': 2, ':oldval': 1})
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :newval',
            ConditionExpression='a < :oldval',
            ExpressionAttributeValues={':newval': 2, ':oldval': 0})
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :newval',
            ConditionExpression='b < :oldval',
            ExpressionAttributeValues={':newval': 2, ':oldval': 'cat'})
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :newval',
            ConditionExpression='b < :oldval',
            ExpressionAttributeValues={':newval': 2, ':oldval': 'aardvark'})
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :newval',
            ConditionExpression='c < :oldval',
            ExpressionAttributeValues={':newval': 2, ':oldval': b'cat'})
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :newval',
            ConditionExpression='c < :oldval',
            ExpressionAttributeValues={':newval': 2, ':oldval': b'aardvark'})
    # If the types are different, this is also considered false
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :newval',
            ConditionExpression='a < :oldval',
            ExpressionAttributeValues={':newval': 2, ':oldval': '17'})
    # If the attribute being compared doesn't even exist, this is also
    # considered as a false condition - not an error.
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :newval',
            ConditionExpression='q < :oldval',
            ExpressionAttributeValues={':newval': 2, ':oldval': '17'})
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :newval',
            ConditionExpression=':oldval < q',
            ExpressionAttributeValues={':newval': 2, ':oldval': '17'})
    # If a comparison parameter comes from a constant specified in the query,
    # and it has a type not supported by the comparison (e.g., a list), it's
    # not just a failed comparison - it is considered a ValidationException
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :newval',
            ConditionExpression='a < :oldval',
            ExpressionAttributeValues={':newval': 2, ':oldval': [1,2]})
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :newval',
            ConditionExpression=':oldval < a',
            ExpressionAttributeValues={':newval': 2, ':oldval': [1,2]})
    # However, if when the wrong type comes from an item attribute, not the
    # query, the comparison is simply false - not a ValidationException.
    test_table_s.update_item(Key={'p': p}, AttributeUpdates={'x': {'Value': [1,2,3], 'Action': 'PUT'}})
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :newval',
            ConditionExpression='x < :oldval',
            ExpressionAttributeValues={':newval': 2, ':oldval': 1})
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :newval',
            ConditionExpression=':oldval < x',
            ExpressionAttributeValues={':newval': 2, ':oldval': 1})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 4

# In test_update_condition_lt() above we saw that a non-existent attribute is
# not "<" any value. Here we check what happens when two non-existent
# attributes are compared with "<". It turns out that the result of such
# comparison is also false.
# The same is true for other order operators - any order comparison involving
# one unset attribute should be false - even if the second operand is an
# unset attribute as well. Note that the <> operator is different - it is
# always results in true if one of the operands is an unset attribute (see
# test_update_condition_ne_two_unset() above).
# This test is related to issue #8511 (although it passed even before fixing
# that issue).
def test_update_condition_comparison_two_unset(test_table_s):
    p = random_string()
    ops = ['<', '<=', '>', '>=']
    for op in ops:
        with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
            test_table_s.update_item(Key={'p': p},
                UpdateExpression='SET a = :val1',
                ConditionExpression='q ' + op + ' z',
                ExpressionAttributeValues={':val1': 2})
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET a = :val1',
            ConditionExpression='q between z and x',
            ExpressionAttributeValues={':val1': 2})
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Value': 1, 'Action': 'PUT'}})
    for op in ops:
        with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
            test_table_s.update_item(Key={'p': p},
                UpdateExpression='SET a = :val1',
                ConditionExpression='q ' + op + ' z',
                ExpressionAttributeValues={':val1': 3})
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET a = :val1',
            ConditionExpression='q between z and x',
            ExpressionAttributeValues={':val1': 2})

# Test for ConditionExpression with operator "<="
def test_update_condition_le(test_table_s):
    p = random_string()
    # The <= operator should work for string, number and binary types
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Value': 1, 'Action': 'PUT'},
                          'b': {'Value': 'cat', 'Action': 'PUT'},
                          'c': {'Value': b'cat', 'Action': 'PUT'}})
    # true cases:
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :newval',
        ConditionExpression='a <= :oldval',
        ExpressionAttributeValues={':newval': 2, ':oldval': 2})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 2
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :newval',
        ConditionExpression='a <= :oldval',
        ExpressionAttributeValues={':newval': 3, ':oldval': 1})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 3
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :newval',
        ConditionExpression='b <= :oldval',
        ExpressionAttributeValues={':newval': 4, ':oldval': 'dog'})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 4
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :newval',
        ConditionExpression='b <= :oldval',
        ExpressionAttributeValues={':newval': 5, ':oldval': 'cat'})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 5
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :newval',
        ConditionExpression='c <= :oldval',
        ExpressionAttributeValues={':newval': 6, ':oldval': b'dog'})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 6
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :newval',
        ConditionExpression='c <= :oldval',
        ExpressionAttributeValues={':newval': 7, ':oldval': b'cat'})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 7
    # false cases:
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :newval',
            ConditionExpression='a <= :oldval',
            ExpressionAttributeValues={':newval': 2, ':oldval': 0})
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :newval',
            ConditionExpression='b <= :oldval',
            ExpressionAttributeValues={':newval': 2, ':oldval': 'aardvark'})
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :newval',
            ConditionExpression='c <= :oldval',
            ExpressionAttributeValues={':newval': 2, ':oldval': b'aardvark'})
    # If the types are different, this is also considered false
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :newval',
            ConditionExpression='a <= :oldval',
            ExpressionAttributeValues={':newval': 2, ':oldval': '17'})
    # If the attribute being compared doesn't even exist, this is also
    # considered as a false condition - not an error.
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :newval',
            ConditionExpression='q <= :oldval',
            ExpressionAttributeValues={':newval': 2, ':oldval': '17'})
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :newval',
            ConditionExpression=':oldval <= q',
            ExpressionAttributeValues={':newval': 2, ':oldval': '17'})
    # If a comparison parameter comes from a constant specified in the query,
    # and it has a type not supported by the comparison (e.g., a list), it's
    # not just a failed comparison - it is considered a ValidationException
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :newval',
            ConditionExpression='a <= :oldval',
            ExpressionAttributeValues={':newval': 2, ':oldval': [1,2]})
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :newval',
            ConditionExpression=':oldval <= a',
            ExpressionAttributeValues={':newval': 2, ':oldval': [1,2]})
    # However, if when the wrong type comes from an item attribute, not the
    # query, the comparison is simply false - not a ValidationException.
    test_table_s.update_item(Key={'p': p}, AttributeUpdates={'x': {'Value': [1,2,3], 'Action': 'PUT'}})
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :newval',
            ConditionExpression='x <= :oldval',
            ExpressionAttributeValues={':newval': 2, ':oldval': 1})
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :newval',
            ConditionExpression=':oldval <= x',
            ExpressionAttributeValues={':newval': 2, ':oldval': 1})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 7

# Test for ConditionExpression with operator ">"
def test_update_condition_gt(test_table_s):
    p = random_string()
    # The > operator should work for string, number and binary types
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Value': 1, 'Action': 'PUT'},
                          'b': {'Value': 'cat', 'Action': 'PUT'},
                          'c': {'Value': b'cat', 'Action': 'PUT'}})
    # true cases:
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :newval',
        ConditionExpression='a > :oldval',
        ExpressionAttributeValues={':newval': 2, ':oldval': 0})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 2
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :newval',
        ConditionExpression='b > :oldval',
        ExpressionAttributeValues={':newval': 3, ':oldval': 'aardvark'})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 3
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :newval',
        ConditionExpression='c > :oldval',
        ExpressionAttributeValues={':newval': 4, ':oldval': b'aardvark'})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 4
    # false cases:
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :newval',
            ConditionExpression='a > :oldval',
            ExpressionAttributeValues={':newval': 2, ':oldval': 1})
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :newval',
            ConditionExpression='a > :oldval',
            ExpressionAttributeValues={':newval': 2, ':oldval': 2})
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :newval',
            ConditionExpression='b > :oldval',
            ExpressionAttributeValues={':newval': 2, ':oldval': 'cat'})
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :newval',
            ConditionExpression='b > :oldval',
            ExpressionAttributeValues={':newval': 2, ':oldval': 'dog'})
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :newval',
            ConditionExpression='c > :oldval',
            ExpressionAttributeValues={':newval': 2, ':oldval': b'cat'})
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :newval',
            ConditionExpression='c > :oldval',
            ExpressionAttributeValues={':newval': 2, ':oldval': b'dog'})
    # If the types are different, this is also considered false
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :newval',
            ConditionExpression='a > :oldval',
            ExpressionAttributeValues={':newval': 2, ':oldval': '17'})
    # If the attribute being compared doesn't even exist, this is also
    # considered as a false condition - not an error.
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :newval',
            ConditionExpression='q > :oldval',
            ExpressionAttributeValues={':newval': 2, ':oldval': '17'})
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :newval',
            ConditionExpression=':oldval > q',
            ExpressionAttributeValues={':newval': 2, ':oldval': '17'})
    # If a comparison parameter comes from a constant specified in the query,
    # and it has a type not supported by the comparison (e.g., a list), it's
    # not just a failed comparison - it is considered a ValidationException
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :newval',
            ConditionExpression='a > :oldval',
            ExpressionAttributeValues={':newval': 2, ':oldval': [1,2]})
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :newval',
            ConditionExpression=':oldval > a',
            ExpressionAttributeValues={':newval': 2, ':oldval': [1,2]})
    # However, if when the wrong type comes from an item attribute, not the
    # query, the comparison is simply false - not a ValidationException.
    test_table_s.update_item(Key={'p': p}, AttributeUpdates={'x': {'Value': [1,2,3], 'Action': 'PUT'}})
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :newval',
            ConditionExpression='x > :oldval',
            ExpressionAttributeValues={':newval': 2, ':oldval': 1})
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :newval',
            ConditionExpression=':oldval > x',
            ExpressionAttributeValues={':newval': 2, ':oldval': 1})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 4

# Test for ConditionExpression with operator ">="
def test_update_condition_ge(test_table_s):
    p = random_string()
    # The >= operator should work for string, number and binary types
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Value': 1, 'Action': 'PUT'},
                          'b': {'Value': 'cat', 'Action': 'PUT'},
                          'c': {'Value': b'cat', 'Action': 'PUT'}})
    # true cases:
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :newval',
        ConditionExpression='a >= :oldval',
        ExpressionAttributeValues={':newval': 2, ':oldval': 0})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 2
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :newval',
        ConditionExpression='a >= :oldval',
        ExpressionAttributeValues={':newval': 3, ':oldval': 1})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 3
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :newval',
        ConditionExpression='b >= :oldval',
        ExpressionAttributeValues={':newval': 4, ':oldval': 'aardvark'})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 4
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :newval',
        ConditionExpression='b >= :oldval',
        ExpressionAttributeValues={':newval': 5, ':oldval': 'cat'})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 5
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :newval',
        ConditionExpression='c >= :oldval',
        ExpressionAttributeValues={':newval': 6, ':oldval': b'aardvark'})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 6
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :newval',
        ConditionExpression='c >= :oldval',
        ExpressionAttributeValues={':newval': 7, ':oldval': b'cat'})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 7
    # false cases:
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :newval',
            ConditionExpression='a >= :oldval',
            ExpressionAttributeValues={':newval': 2, ':oldval': 2})
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :newval',
            ConditionExpression='b >= :oldval',
            ExpressionAttributeValues={':newval': 2, ':oldval': 'dog'})
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :newval',
            ConditionExpression='c >= :oldval',
            ExpressionAttributeValues={':newval': 2, ':oldval': b'dog'})
    # If the types are different, this is also considered false
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :newval',
            ConditionExpression='a >= :oldval',
            ExpressionAttributeValues={':newval': 2, ':oldval': '0'})
    # If the attribute being compared doesn't even exist, this is also
    # considered as a false condition - not an error.
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :newval',
            ConditionExpression='q >= :oldval',
            ExpressionAttributeValues={':newval': 2, ':oldval': '17'})
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :newval',
            ConditionExpression=':oldval >= q',
            ExpressionAttributeValues={':newval': 2, ':oldval': '17'})
    # If a comparison parameter comes from a constant specified in the query,
    # and it has a type not supported by the comparison (e.g., a list), it's
    # not just a failed comparison - it is considered a ValidationException
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :newval',
            ConditionExpression='a >= :oldval',
            ExpressionAttributeValues={':newval': 2, ':oldval': [1,2]})
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :newval',
            ConditionExpression=':oldval >= a',
            ExpressionAttributeValues={':newval': 2, ':oldval': [1,2]})
    # However, if when the wrong type comes from an item attribute, not the
    # query, the comparison is simply false - not a ValidationException.
    test_table_s.update_item(Key={'p': p}, AttributeUpdates={'x': {'Value': [1,2,3], 'Action': 'PUT'}})
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :newval',
            ConditionExpression='x >= :oldval',
            ExpressionAttributeValues={':newval': 2, ':oldval': 1})
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :newval',
            ConditionExpression=':oldval >= x',
            ExpressionAttributeValues={':newval': 2, ':oldval': 1})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 7

# Test for ConditionExpression with ternary operator "BETWEEN" (checking
# if a value is between two others, equality included). The keywords
# "BETWEEN" and "AND" are case insensitive.
def test_update_condition_between(test_table_s):
    p = random_string()
    # The BETWEEN operator should work for string, number and binary types
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Value': 1, 'Action': 'PUT'},
                          'b': {'Value': 'cat', 'Action': 'PUT'},
                          'c': {'Value': b'cat', 'Action': 'PUT'}})
    # true cases:
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :newval',
        ConditionExpression='a BETWEEN :oldval1 AND :oldval2',
        ExpressionAttributeValues={':newval': 2, ':oldval1': 0, ':oldval2': 2})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 2
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :newval',
        ConditionExpression='a BETWEEN :oldval1 AND :oldval2',
        ExpressionAttributeValues={':newval': 3, ':oldval1': 1, ':oldval2': 1})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 3
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :newval',
        ConditionExpression='b BETWEEN :oldval1 AND :oldval2',
        ExpressionAttributeValues={':newval': 4, ':oldval1': 'aardvark', ':oldval2': 'dog'})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 4
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :newval',
        ConditionExpression='b BETWEEN :oldval1 AND :oldval2',
        ExpressionAttributeValues={':newval': 5, ':oldval1': 'cat', ':oldval2': 'cat'})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 5
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :newval',
        ConditionExpression='c BETWEEN :oldval1 AND :oldval2',
        ExpressionAttributeValues={':newval': 6, ':oldval1': b'aardvark', ':oldval2': b'dog'})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 6
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :newval',
        ConditionExpression='c BETWEEN :oldval1 AND :oldval2',
        ExpressionAttributeValues={':newval': 7, ':oldval1': b'cat', ':oldval2': b'cat'})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 7
    # All three operands of the BETWEEN operator can be attributes of the
    # item:
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'x': {'Value': 0, 'Action': 'PUT'},
                          'y': {'Value': 2, 'Action': 'PUT'}})
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :newval',
        ConditionExpression='a BETWEEN x AND y',
        ExpressionAttributeValues={':newval': 8})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 8
    # The keywords "BETWEEN" and "AND" are case insensitive
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :newval',
        ConditionExpression='a between x and y',
        ExpressionAttributeValues={':newval': 9})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 9
    # false cases:
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :newval',
            ConditionExpression='a BETWEEN :oldval1 AND :oldval2',
            ExpressionAttributeValues={':newval': 2, ':oldval1': 2, ':oldval2': 7})
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :newval',
            ConditionExpression='b BETWEEN :oldval1 AND :oldval2',
            ExpressionAttributeValues={':newval': 2, ':oldval1': 'dog', ':oldval2': 'zebra'})
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :newval',
            ConditionExpression='c BETWEEN :oldval1 AND :oldval2',
            ExpressionAttributeValues={':newval': 2, ':oldval1': b'dog', ':oldval2': b'zebra'})
    # If the types are different, this is also considered false
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :newval',
            ConditionExpression='a BETWEEN :oldval1 AND :oldval2',
            ExpressionAttributeValues={':newval': 2, ':oldval1': '0', ':oldval2': '2'})
    # If the attribute being compared doesn't even exist, this is also
    # considered as a false condition - not an error.
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :newval',
            ConditionExpression='q BETWEEN :oldval1 AND :oldval2',
            ExpressionAttributeValues={':newval': 2, ':oldval1': b'dog', ':oldval2': b'zebra'})
    # If and operand from the query, and it has a type not supported by the
    # comparison (e.g., a list), it's not just a failed condition - it is
    # considered a ValidationException
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :newval',
            ConditionExpression='a BETWEEN :oldval1 AND :oldval2',
            ExpressionAttributeValues={':newval': 2, ':oldval1': [1,2], ':oldval2': [2,3]})
    # However, if when the wrong type comes from an item attribute, not the
    # query, the comparison is simply false - not a ValidationException.
    test_table_s.update_item(Key={'p': p}, AttributeUpdates={'x': {'Value': [1,2,3], 'Action': 'PUT'},
                                                             'y': {'Value': [2,3,4], 'Action': 'PUT'}})
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :newval',
            ConditionExpression='a BETWEEN x and y',
            ExpressionAttributeValues={':newval': 2})
    # If the two operands come from the query (":val" references) then if they
    # have different types or the wrong order, this is a ValidationException.
    # But if one or more of the operands come from the item, this only causes
    # a false condition - not a ValidationException.
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :newval',
            ConditionExpression='a BETWEEN :oldval1 AND :oldval2',
            ExpressionAttributeValues={':newval': 2, ':oldval1': 2, ':oldval2': 1})
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :newval',
            ConditionExpression='a BETWEEN :oldval1 AND :oldval2',
            ExpressionAttributeValues={':newval': 2, ':oldval1': 2, ':oldval2': 'dog'})
    test_table_s.update_item(Key={'p': p}, AttributeUpdates={'two': {'Value': 2, 'Action': 'PUT'}})
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :newval',
            ConditionExpression='a BETWEEN two AND :oldval',
            ExpressionAttributeValues={':newval': 2, ':oldval': 1})
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :newval',
            ConditionExpression='a BETWEEN :oldval AND two',
            ExpressionAttributeValues={':newval': 2, ':oldval': 3})
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :newval',
            ConditionExpression='a BETWEEN two AND :oldval',
            ExpressionAttributeValues={':newval': 2, ':oldval': 'dog'})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 9

# Test for ConditionExpression with multi-operand operator "IN", checking
# whether a value is equal to one of possibly many values (up to 100 should
# be supported, according to the DynamoDB documentation).
def test_update_condition_in(test_table_s):
    p = random_string()
    
    # The "IN" operator checks equality, and should work for any type.
    # Here we just try the trivial successful equality check of one value:
    values = (1, "hello", True, b'xyz', None, ['hello', 42], {'hello': 'world'}, set(['hello', 'world']), set([1, 2, 3]), set([b'xyz', b'hi']))
    i = 0
    for val in values:
        i = i + 1
        print(val)
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'a': {'Value': val, 'Action': 'PUT'},
                              'b': {'Value': val, 'Action': 'PUT'}})
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET c = :i',
            ConditionExpression='a IN (b)',
            ExpressionAttributeValues={':i': i})
        assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['c'] == i

    # The DynamoDB documentation suggests that IN's list can have up to 100
    # attributes listed, but it actually supports only 99 (100 including
    # the first argument to the operator), so let's check 99 work.
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Value': 74, 'Action': 'PUT'}})
    values = {':val{}'.format(i): i for i in range(99)}
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET c = :val37',
        ConditionExpression='a IN ({})'.format(','.join(values.keys())),
        ExpressionAttributeValues=values)
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['c'] == 37
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Value': 174, 'Action': 'PUT'}})
    with pytest.raises(ClientError, match='ConditionalCheckFailedException.*'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET c = :val37',
            ConditionExpression='a IN ({})'.format(','.join(values.keys())),
            ExpressionAttributeValues=values)
    # Unlike the IN operation in Expected, here it is not a validation error
    # for the different values to have different types (of course, the
    # condition will only end up succeeding if one of the listed values has
    # the correct type - and value.
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET c = :val',
        ConditionExpression='a IN (:x, :y)',
        ExpressionAttributeValues={':val': 1, ':x': 'dog', ':y': 174})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['c'] == 1
    # IN with zero arguments results in a syntax error, not a failed condition
    with pytest.raises(ClientError, match='ValidationException.*yntax error'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET c = :val37',
            ConditionExpression='a IN ()',
            ExpressionAttributeValues=values)
    # If the attribute being compared doesn't even exist, this is also
    # considered as a false condition - not an error.
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET c = :val37',
            ConditionExpression='q IN ({})'.format(','.join(values.keys())),
            ExpressionAttributeValues=values)

# Beyond the above operators, there are also test functions supported -
# attribute_exists, attribute_not_exists, attribute_type, begins_with,
# contains, and size (these function names are case sensitive).
# These functions are listed and described in
# https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html

def test_update_condition_attribute_exists(test_table_s):
    p = random_string()
    test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'a': {'Value': 1, 'Action': 'PUT'}})
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET c = :val',
            ConditionExpression='attribute_exists (a)',
            ExpressionAttributeValues={':val': 1})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['c'] == 1
    with pytest.raises(ClientError, match='ConditionalCheckFailedException.*'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET c = :val',
                ConditionExpression='attribute_exists (z)',
                ExpressionAttributeValues={':val': 3})
    # Somewhat artificially, attribute_exists() requires that its parameter
    # be a path - it cannot be a different sort of value.
    with pytest.raises(ClientError, match='ValidationException.*path'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET c = :val',
                ConditionExpression='attribute_exists (:val)',
                ExpressionAttributeValues={':val': 3})

# Primitive conditions usually look like an operator between two (<, <=,
# etc.), three (BETWEEN) or more (IN) values. Can just a single value be
# a condition? The special case of a single function call *can* be - we saw
# an example attribute_exists(z) in the previous test. However that only
# function calls are supported in this context - not general values (i.e.,
# attribute or value references).
# While DynamoDB does not accept a non-function-call value as a condition
# (it results with with a syntax error), in Alternator currently, for
# simplicity of the parser, this case is parsed correctly and only fails
# later when the calculated value ends up to not be a boolean.
def test_update_condition_single_value_attribute(test_table_s):
    p = random_string()
    test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'a': {'Value': 1, 'Action': 'PUT'}})
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET c = :val',
                ConditionExpression='a',
                ExpressionAttributeValues={':val': 1})

def test_update_condition_attribute_not_exists(test_table_s):
    p = random_string()
    test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'a': {'Value': 1, 'Action': 'PUT'}})
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET c = :val',
            ConditionExpression='attribute_not_exists (b)',
            ExpressionAttributeValues={':val': 1})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['c'] == 1
    with pytest.raises(ClientError, match='ConditionalCheckFailedException.*'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET c = :val',
                ConditionExpression='attribute_not_exists (a)',
                ExpressionAttributeValues={':val': 3})

def test_update_condition_attribute_type(test_table_s):
    p = random_string()
    type_values = [
        ('S', 'hello'),
        ('SS', set(['hello', 'world'])),
        ('N', 2),
        ('NS', set([1, 2])),
        ('B', b'dog'),
        ('BS', set([b'dog', b'cat'])),
        ('BOOL', True),
        ('NULL', None),
        ('L', [1, 'dog']),
        ('M', {'a': 3, 'b': 4})]
    updates={'a{}'.format(i): {'Value': type_values[i][1], 'Action': 'PUT'} for i in range(len(type_values))}
    test_table_s.update_item(Key={'p': p}, AttributeUpdates=updates)
    for i in range(len(type_values)):
        expected_type = type_values[i][0]
        # As explained in a comment in the top of the file, the binary types
        # cannot be tested with Python 2
        if expected_type in ('B', 'BS') and version_info[0] == 2:
            continue
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET c = :val',
            ConditionExpression='attribute_type (a{}, :type)'.format(i),
            ExpressionAttributeValues={':val': i, ':type': expected_type})
        assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['c'] == i
        wrong_type = type_values[(i + 1) % len(type_values)][0]
        with pytest.raises(ClientError, match='ConditionalCheckFailedException.*'):
            test_table_s.update_item(Key={'p': p},
                UpdateExpression='SET c = :val',
                ConditionExpression='attribute_type (a{}, :type)'.format(i),
                ExpressionAttributeValues={':val': i, ':type': wrong_type})
    # The DynamoDB documentation suggests that attribute_type()'s first
    # parameter must be a path (as we saw above, this is indeed the case for
    # attribute_exists()). But in fact, attribute_type() does work fine also
    # for an expression attribute.
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET c = :val',
            ConditionExpression='attribute_type (:val, :type)',
            ExpressionAttributeValues={':val': 0, ':type': 'N'})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['c'] == 0

# The DynamoDB documentation explicitly states that the second argument
# of the attribute_type function - the type to compare to - *must* be an
# expression attribute (:name) - it cannot be an item attribute.
# I don't know why this was important to forbid, but this test confirms that
# DynamoDB does forbid it.
def test_update_condition_attribute_type_second_arg(test_table_s):
    p = random_string()
    test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'a': {'Value': 1, 'Action': 'PUT'},
                              'b': {'Value': 'N', 'Action': 'PUT'}})
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET c = :val',
                ConditionExpression='attribute_type (a, b)',
                ExpressionAttributeValues={':val': 1})

# If the attribute_type() parameter is not one of the known types
# (N,NS,BS,L,SS,NULL,B,BOOL,S,M), an error is generated. We should
# not get a failed condition.
def test_update_condition_attribute_type_unknown(test_table_s):
    p = random_string()
    with pytest.raises(ClientError, match='ValidationException.*DOG'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET c = :val',
                ConditionExpression='attribute_type (a, :type)',
                ExpressionAttributeValues={':val': 1, ':type': 'DOG'})

def test_update_condition_begins_with(test_table_s):
    p = random_string()
    test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'a': {'Value': 'hello', 'Action': 'PUT'},
                              'b': {'Value': b'hi there', 'Action': 'PUT'}})
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET c = :val',
            ConditionExpression='begins_with(a, :arg)',
            ExpressionAttributeValues={':val': 1, ':arg': 'hell'})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['c'] == 1
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET c = :val',
            ConditionExpression='begins_with(b, :arg)',
            ExpressionAttributeValues={':val': 2, ':arg': b'hi'})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['c'] == 2
    with pytest.raises(ClientError, match='ConditionalCheckFailedException.*'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET c = :val',
                ConditionExpression='begins_with(a, :arg)',
                ExpressionAttributeValues={':val': 3, ':arg': 'dog'})
    # begins_with() requires String or Binary operand, giving it a number
    # inside the expression results with a ValidationException (not a normal
    # failed condition):
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET c = :val',
                ConditionExpression='begins_with(a, :arg)',
                ExpressionAttributeValues={':val': 3, ':arg': 2})
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET c = :val',
                ConditionExpression='begins_with(c, :arg)',
                ExpressionAttributeValues={':val': 3, ':arg': 2})
    # However, that extra type check is only done on values inside the
    # expression. It isn't done on values from an item attributes - in that
    # case we got a normal failed condition.
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET c = :val',
                ConditionExpression='begins_with(c, :arg)',
                ExpressionAttributeValues={':val': 3, ':arg': 'dog'})
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET c = :val',
                ConditionExpression='begins_with(c, a)',
                ExpressionAttributeValues={':val': 3})
    # Although the DynamoDB documentation suggests that begins_with()
    # can only take a path as the first parameter and a constant as
    # the second, this isn't actually true - begins_with() works
    # as expected also to compare two attributes, or in reverse order:
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET c = :val',
            ConditionExpression='begins_with(:str, a)',
            ExpressionAttributeValues={':val': 'he', ':str': 'hellohi'})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['c'] == 'he'
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET c = :val',
            ConditionExpression='begins_with(a, c)',
            ExpressionAttributeValues={':val': 5})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['c'] == 5


def test_update_condition_contains(test_table_s):
    p = random_string()
    # contains() can be used for two unrelated things: check substring (in
    # string or binary) and membership (in set or a list). The DynamoDB
    # documentation only mention string and set (not binary or list) but
    # the fact is that binary and list are also support.
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Value': 'hello', 'Action': 'PUT'},
                          'b': {'Value': set([2, 4, 7]), 'Action': 'PUT'},
                          'c': {'Value': [2, 4, 7], 'Action': 'PUT'},
                          'd': {'Value': b'hi there', 'Action': 'PUT'},
                          'e': {'Value': ['hi', set([1,2]), [3, 4]], 'Action': 'PUT'}})
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :val',
            ConditionExpression='contains(a, :arg)',
            ExpressionAttributeValues={':val': 1, ':arg': 'ell'})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 1
    # The DynamoDB documentation incorrectly states that the second operand
    # must always be a string. That's not true - it's fine to test if a
    # set of numbers contains a number, for example.
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :val',
            ConditionExpression='contains(b, :arg)',
            ExpressionAttributeValues={':val': 2, ':arg': 4})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 2
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :val',
            ConditionExpression='contains(c, :arg)',
            ExpressionAttributeValues={':val': 3, ':arg': 4})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 3
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :val',
            ConditionExpression='contains(d, :arg)',
            ExpressionAttributeValues={':val': 4, ':arg': b'here'})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 4
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :val',
                ConditionExpression='contains(d, :arg)',
                ExpressionAttributeValues={':val': 4, ':arg': b'dog'})
    # Moreover, the second parameter to contains() may be *any* type, and
    # contains checks if perhaps the first parameter is a list or a set
    # containing that value!
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :val',
                ConditionExpression='contains(d, :arg)',
                ExpressionAttributeValues={':val': 4, ':arg': set([1, 2])})
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :val',
        ConditionExpression='contains(e, :arg)',
        ExpressionAttributeValues={':val': 5, ':arg': set([1, 2])})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 5
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :val',
        ConditionExpression='contains(e, :arg)',
        ExpressionAttributeValues={':val': 6, ':arg': [3, 4]})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 6


# While both operands of contains() may be item attributes, strangely
# it is explicitly forbidden to have the same attribute as both and
# trying to do so results in a ValidationException. I don't know why it's
# important to make this query fail, when it could have just worked...
# TODO: Is this limitation only for contains() or other functions as well?
@pytest.mark.xfail(reason="extra check for same attribute not implemented yet")
def test_update_condition_contains_same_attribute(test_table_s):
    p = random_string()
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a1': {'Value': 'hello', 'Action': 'PUT'},
                          'a': {'Value': 'hello', 'Action': 'PUT'}})
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :val',
            ConditionExpression='contains(a, a1)',
            ExpressionAttributeValues={':val': 5})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 5
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :val',
                ConditionExpression='contains(a, a)',
                ExpressionAttributeValues={':val': 5})

# The syntax of the size() function is is incorrectly specified in
# https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html
# size() itself is not a boolean function as shown there, but rather a numeric
# function whose return value needs to be further combined with another
# operand using a comparison operation - and it isn't specified which is
# supported.
def test_update_condition_size(test_table_s):
    p = random_string()
    # First verify what size() returns for various types. We use only the
    # "=" comparison for these tests:
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Value': 'hello', 'Action': 'PUT'},
                          'b': {'Value': set([2, 4, 7]), 'Action': 'PUT'},
                          'c': {'Value': [2, 'dog', 7], 'Action': 'PUT'},
                          'd': {'Value': b'hi there', 'Action': 'PUT'},
                          'e': {'Value': {'x': 2, 'y': {'m': 3, 'n': 4}}, 'Action': 'PUT'},
                          'f': {'Value': 5, 'Action': 'PUT'},
                          'g': {'Value': True, 'Action': 'PUT'},
                          'h': {'Value': None, 'Action': 'PUT'}})
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :val',
            ConditionExpression='size(a)=:arg',
            ExpressionAttributeValues={':val': 1, ':arg': 5})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 1
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :val',
            ConditionExpression='size(b)=:arg',
            ExpressionAttributeValues={':val': 2, ':arg': 3})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 2
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :val',
            ConditionExpression='size(c)=:arg',
            ExpressionAttributeValues={':val': 3, ':arg': 3})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 3
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :val',
            ConditionExpression='size(d)=:arg',
            ExpressionAttributeValues={':val': 4, ':arg': 8})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 4
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :val',
            ConditionExpression='size(e)=:arg',
            ExpressionAttributeValues={':val': 5, ':arg': 2})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 5
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :val',
                ConditionExpression='size(f)=:arg',
                ExpressionAttributeValues={':val': 6, ':arg': 1})
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :val',
                ConditionExpression='size(g)=:arg',
                ExpressionAttributeValues={':val': 6, ':arg': 1})
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :val',
                ConditionExpression='size(h)=:arg',
                ExpressionAttributeValues={':val': 6, ':arg': 1})
    # Trying to compare the size() to a non-number results in a normal
    # condition failure, not a ValidationException.
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :val',
                ConditionExpression='size(a)=:arg',
                ExpressionAttributeValues={':val': 6, ':arg': 'dog'})
    # The argument to which the size is being compared to *may* be one of the
    # item attributes too:
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :val',
            ConditionExpression='size(a)=f',
            ExpressionAttributeValues={':val': 6})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 6
    # After testing the "=" operator throughly, check other operators are also
    # supported.
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :val',
            ConditionExpression='size(a)<>:arg',
            ExpressionAttributeValues={':val': 7, ':arg': 4})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 7
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :val',
            ConditionExpression='size(a)<:arg',
            ExpressionAttributeValues={':val': 8, ':arg': 7})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 8
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :val',
            ConditionExpression='size(a)<=:arg',
            ExpressionAttributeValues={':val': 9, ':arg': 7})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 9
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :val',
            ConditionExpression='size(a)>:arg',
            ExpressionAttributeValues={':val': 10, ':arg': 2})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 10
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :val',
            ConditionExpression='size(a)>=:arg',
            ExpressionAttributeValues={':val': 11, ':arg': 2})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 11
    # size() is only allowed one operand; More operands are allowed by the
    # parser, but later result in an error:
    with pytest.raises(ClientError, match='ValidationException.*2'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :val',
            ConditionExpression='size(a, a)=:arg',
            ExpressionAttributeValues={':val': 1, ':arg': 5})

# The above test tested conditions involving size() in a comparison.
# Trying to use just size(a) as a condition (as we use the rest of the
# functions supported by ConditionExpression) does not work - DynamoDB
# reports # that "The function is not allowed to be used this way in an
# expression; function: size".
def test_update_condition_size_alone(test_table_s):
    p = random_string()
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :val',
            ConditionExpression='size(a)',
            ExpressionAttributeValues={':val': 1})

# Similarly, while attribute_exists(a) works alone, it cannot be used in
# a comparison, e.g., attribute_exists(a) < 1 also causes DynamoDB to
# complain about "The function is not allowed to be used in this way in an
# expression.".
def test_update_condition_attribute_exists_in_comparison(test_table_s):
    p = random_string()
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :val',
            ConditionExpression='attribute_exists(a) < :val',
            ExpressionAttributeValues={':val': 1})

# In essense, the size() function tested in the previous test behaves
# exactly like the functions of UpdateExpressions, i.e., it transforms a
# value (attribute from the item or the query) into a new value, which
# can than be operated (in our case, compared). In this test we check
# that other functions supported by UpdateExpression - if_not_exists()
# and list_append() - are not supported.
def test_update_condition_other_funcs(test_table_s):
    p = random_string()
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Value': 'hello', 'Action': 'PUT'}})
    # dog() is an unknown function name:
    with pytest.raises(ClientError, match='ValidationException.*function'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :val',
            ConditionExpression='dog(a)=:arg',
            ExpressionAttributeValues={':val': 1, ':arg': 5})
    # The functions if_not_exists() and list_append() are known functions
    # (they are supported in UpdateExpression) but not allowed in
    # ConditionExpression. This means we can have a single function for
    # evaluation a parsed::value, but it needs to know whether it is
    # called for a UpdateExpression or a ConditionExpression.
    with pytest.raises(ClientError, match='ValidationException.*not allowed'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :val',
            ConditionExpression='if_not_exists(a, a)=:arg',
            ExpressionAttributeValues={':val': 1, ':arg': 5})
    with pytest.raises(ClientError, match='ValidationException.*not allowed'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :val',
            ConditionExpression='list_append(a, a)=:arg',
            ExpressionAttributeValues={':val': 1, ':arg': 5})

# All the previous tests involved top-level attributes to be tested. But
# ConditionExpressions also allows reading nested attributes, and we should
# support that too. This test just checks a few random operators - we don't
# test all the different operators here.
def test_update_condition_nested_attributes(test_table_s):
    p = random_string()
    test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'b': {'Value': {'x': 1, 'y': [-1, 2, 0]}, 'Action': 'PUT'}})
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET c = :val',
            ConditionExpression='attribute_exists (b.x)',
            ExpressionAttributeValues={':val': 1})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['c'] == 1
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET c = :val',
            ConditionExpression='b.x < b.y[1]',
            ExpressionAttributeValues={':val': 2})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['c'] == 2
    # Also check the case of a failing condition
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET c = :val',
                ConditionExpression='b.x < b.y[0]',
            ExpressionAttributeValues={':val': 3})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['c'] == 2
    # A condition involving an attribute which doesn't exist results in
    # failed condition - not an error.
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET c = :val',
                ConditionExpression='b.z < b.y[100]',
            ExpressionAttributeValues={':val': 4})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['c'] == 2

# All the previous tests refered to attributes using their name directly.
# But the DynamoDB API also allows to refer to attributes using a #reference.
# Among other things this allows using attribute names which are usually
# reserved keywords in condition expressions.
def test_update_condition_attribute_reference(test_table_s):
    p = random_string()
    test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'and': {'Value': 1, 'Action': 'PUT'}})
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET c = :val',
            ConditionExpression='attribute_exists (#name)',
            ExpressionAttributeNames={'#name': 'and'},
            ExpressionAttributeValues={':val': 1})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['c'] == 1

def test_update_condition_nested_attribute_reference(test_table_s):
    p = random_string()
    test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'and': {'Value': {'or': 2}, 'Action': 'PUT'}})
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET c = :val',
            ConditionExpression='#name1.#name2 = :two',
            ExpressionAttributeNames={'#name1': 'and', '#name2': 'or'},
            ExpressionAttributeValues={':val': 1, ':two': 2})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['c'] == 1

# All the previous tests involved a single condition. The following tests
# involving building more complex conditions by using AND, OR, NOT and
# parentheses on simpler condition expressions. There's also operator
# precedence involved, and should be tested (see the definitions in
# https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html

def test_update_condition_and(test_table_s):
    p = random_string()
    test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'a': {'Value': 1, 'Action': 'PUT'},
                              'b': {'Value': 2, 'Action': 'PUT'},
                              'c': {'Value': 3, 'Action': 'PUT'}})
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :val',
            ConditionExpression='a < b AND b < c',
            ExpressionAttributeValues={':val': 1})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 1
    # The "AND" keyword is case insensitive
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :val',
            ConditionExpression='a < b aNd b < c',
            ExpressionAttributeValues={':val': 2})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 2
    # A failed "AND" condition:
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :val',
                ConditionExpression='a < b AND c < b',
                ExpressionAttributeValues={':val': 1})

def test_update_condition_or(test_table_s):
    p = random_string()
    test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'a': {'Value': 1, 'Action': 'PUT'},
                              'b': {'Value': 2, 'Action': 'PUT'},
                              'c': {'Value': 3, 'Action': 'PUT'}})
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :val',
            ConditionExpression='a < b OR b < c',
            ExpressionAttributeValues={':val': 1})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 1
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :val',
            ConditionExpression='b < a OR b < c',
            ExpressionAttributeValues={':val': 2})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 2
    # The "OR" keyword is case insensitive
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :val',
            ConditionExpression='a < b oR b < c',
            ExpressionAttributeValues={':val': 3})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 3
    # A failed "OR" condition:
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :val',
                ConditionExpression='b < a OR c < b',
                ExpressionAttributeValues={':val': 1})

def test_update_condition_not(test_table_s):
    p = random_string()
    test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'a': {'Value': 1, 'Action': 'PUT'},
                              'b': {'Value': 2, 'Action': 'PUT'}})
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :val',
            ConditionExpression='NOT b < a',
            ExpressionAttributeValues={':val': 1})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 1
    # The "NOT" keyword is case insensitive
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :val',
            ConditionExpression='nOt b < a',
            ExpressionAttributeValues={':val': 2})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 2
    # A failed "NOT" condition:
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :val',
                ConditionExpression='NOT a < b',
                ExpressionAttributeValues={':val': 1})
    # NOT NOT NOT NOT also works (and does nothing) :-)
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :val',
            ConditionExpression='NOT NOT NOT NOT a < b',
            ExpressionAttributeValues={':val': 3})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 3

def test_update_condition_parentheses(test_table_s):
    p = random_string()
    test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'a': {'Value': 1, 'Action': 'PUT'},
                              'b': {'Value': 2, 'Action': 'PUT'},
                              'c': {'Value': 3, 'Action': 'PUT'}})
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :val',
            ConditionExpression='(a < b OR b < a) AND (b < c AND (a < b OR b < c))',
            ExpressionAttributeValues={':val': 1})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 1

# There is operator precedence that allows a user to use less parentheses.
# We need to implement it correctly:

def test_update_condition_and_before_or(test_table_s):
    p = random_string()
    test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'a': {'Value': 1, 'Action': 'PUT'},
                              'b': {'Value': 2, 'Action': 'PUT'},
                              'c': {'Value': 3, 'Action': 'PUT'}})
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :val',
            ConditionExpression='a < b OR c < b AND b < c',
            ExpressionAttributeValues={':val': 1})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 1

def test_update_condition_not_before_and(test_table_s):
    p = random_string()
    test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'a': {'Value': 1, 'Action': 'PUT'},
                              'b': {'Value': 2, 'Action': 'PUT'},
                              'c': {'Value': 3, 'Action': 'PUT'}})
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :val',
                ConditionExpression='NOT a < b AND c < b',
                ExpressionAttributeValues={':val': 1})

def test_update_condition_between_before_and(test_table_s):
    p = random_string()
    test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'a': {'Value': 1, 'Action': 'PUT'},
                              'b': {'Value': 2, 'Action': 'PUT'},
                              'c': {'Value': 3, 'Action': 'PUT'}})
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :val',
            ConditionExpression='b BETWEEN a AND c AND a < b',
            ExpressionAttributeValues={':val': 1})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 1

# An empty ConditionExpression is not allowed - resulting in a validation
# error, not a failed condition:
def test_update_condition_empty(test_table_s):
    p = random_string()
    with pytest.raises(ClientError, match='ValidationException.*empty'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET z = :val',
                ConditionExpression='',
                ExpressionAttributeValues={':val': 1})

# All of the above tests tested ConditionExpression with the UpdateItem
# operation. We now want to test that it works also with the PutItem and
# DeleteItems operations. We don't need to check again all the different
# sub-cases tested above - we can assume that exactly the same code gets
# used to test the condition. So we just need one test for each operation,
# to verify that this code actually gets called.

def test_delete_item_condition(test_table_s):
    p = random_string()
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Value': 1, 'Action': 'PUT'}})
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.delete_item(Key={'p': p},
            ConditionExpression='a = :oldval',
            ExpressionAttributeValues={':oldval': 2})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': 1}
    test_table_s.delete_item(Key={'p': p},
            ConditionExpression='a = :oldval',
            ExpressionAttributeValues={':oldval': 1})
    assert not 'Item' in test_table_s.get_item(Key={'p': p}, ConsistentRead=True)

def test_put_item_condition(test_table_s):
    p = random_string()
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Value': 1, 'Action': 'PUT'}})
    test_table_s.put_item(Item={'p': p, 'a': 2},
            ConditionExpression='a = :oldval',
            ExpressionAttributeValues={':oldval': 1})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': 2}
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.put_item(Item={'p': p, 'a': 3},
            ConditionExpression='a = :oldval',
            ExpressionAttributeValues={':oldval': 1})

# DynamoDB frowns upon unused entries in ExpressionAttributeValues and
# ExpressionAttributeNames. Check that we do too (in all three operations),
# although it's not terribly important that we be compatible with DynamoDB
# here...
# There's one delicate issue, though. Should we check for unused entries
# during parsing, or during evaluation? The stage we check this changes
# our behavior when the condition was supposed to fail. So we have two
# separate tests here, one for failed condition and one for successful.
def test_update_condition_unused_entries_failed(test_table_s):
    p = random_string()
    # unused val3:
    with pytest.raises(ClientError, match='ValidationException.*val3'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET #name1 = :val1',
            ConditionExpression='#name2 = :val2',
            ExpressionAttributeValues={':val1': 1, ':val2': 2, ':val3': 3},
            ExpressionAttributeNames={'#name1': 'a', '#name2': 'b'})
    with pytest.raises(ClientError, match='ValidationException.*val3'):
        test_table_s.delete_item(Key={'p': p},
            ConditionExpression='#name1 = :val1',
            ExpressionAttributeValues={':val1': 1, ':val3': 3},
            ExpressionAttributeNames={'#name1': 'a'})
    with pytest.raises(ClientError, match='ValidationException.*val3'):
        test_table_s.put_item(Item={'p': p, 'a': 3},
            ConditionExpression='#name1 = :val1',
            ExpressionAttributeValues={':val1': 1, ':val3': 3},
            ExpressionAttributeNames={'#name1': 'a'})
    # unused name3:
    with pytest.raises(ClientError, match='ValidationException.*name3'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET #name1 = :val1',
            ConditionExpression='#name2 = :val2',
            ExpressionAttributeValues={':val1': 1, ':val2': 2},
            ExpressionAttributeNames={'#name1': 'a', '#name2': 'b', '#name3': 'c'})
    with pytest.raises(ClientError, match='ValidationException.*name3'):
        test_table_s.delete_item(Key={'p': p},
            ConditionExpression='#name1 = :val1',
            ExpressionAttributeValues={':val1': 1},
            ExpressionAttributeNames={'#name1': 'a', '#name3': 'c'})
    with pytest.raises(ClientError, match='ValidationException.*name3'):
        test_table_s.put_item(Item={'p': p, 'a': 3},
            ConditionExpression='#name1 = :val1',
            ExpressionAttributeValues={':val1': 1},
            ExpressionAttributeNames={'#name1': 'a', '#name3': 'c'})
def test_update_condition_unused_entries_succeeded(test_table_s):
    p = random_string()
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'b': {'Value': 2, 'Action': 'PUT'}})
    # unused val3:
    with pytest.raises(ClientError, match='ValidationException.*val3'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET #name1 = :val1',
            ConditionExpression='#name2 = :val2',
            ExpressionAttributeValues={':val1': 1, ':val2': 2, ':val3': 3},
            ExpressionAttributeNames={'#name1': 'a', '#name2': 'b'})
    with pytest.raises(ClientError, match='ValidationException.*val3'):
        test_table_s.delete_item(Key={'p': p},
            ConditionExpression='#name2 = :val2',
            ExpressionAttributeValues={':val2': 2, ':val3': 3},
            ExpressionAttributeNames={'#name2': 'b'})
    with pytest.raises(ClientError, match='ValidationException.*val3'):
        test_table_s.put_item(Item={'p': p, 'a': 3},
            ConditionExpression='#name2 = :val2',
            ExpressionAttributeValues={':val2': 2, ':val3': 3},
            ExpressionAttributeNames={'#name2': 'b'})
    # unused name3:
    with pytest.raises(ClientError, match='ValidationException.*name3'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET #name1 = :val1',
            ConditionExpression='#name2 = :val2',
            ExpressionAttributeValues={':val1': 1, ':val2': 2},
            ExpressionAttributeNames={'#name1': 'a', '#name2': 'b', '#name3': 'c'})
    with pytest.raises(ClientError, match='ValidationException.*name3'):
        test_table_s.delete_item(Key={'p': p},
            ConditionExpression='#name2 = :val2',
            ExpressionAttributeValues={':val2': 2},
            ExpressionAttributeNames={'#name2': 'b', '#name3': 'c'})
    with pytest.raises(ClientError, match='ValidationException.*name3'):
        test_table_s.put_item(Item={'p': p, 'a': 3},
            ConditionExpression='#name2 = :val2',
            ExpressionAttributeValues={':val2': 2},
            ExpressionAttributeNames={'#name2': 'b', '#name3': 'c'})

# Another reason why we must test for used references right after parsing
# the expressions, NOT at evaluation time, is that in some cases evaluation
# may short-circuit and not reach certain parts of the expression, and as
# a result we may wrongly think some names were not used, and refuse a
# perfectly good request. Such a bug (see issue #6572) can be fixed by
# either by dropping short-circuit evaluation (i.e., evaluate all parts
# of the expression even if the first OR succeeds), or by testing for
# unused references before evaluating anything.
def test_update_condition_unused_entries_short_circuit(test_table_s):
    p = random_string()
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Value': 1, 'Action': 'PUT'}})
    # If short-circuit evaluation is done for ConditionExpression, it will
    # not use #name2 or :val2. But we should't fail this request claiming
    # these references weren't used... They were used in the expression,
    # just not in the evaluation. This request *should* work.
    test_table_s.update_item(Key={'p': p},
        ConditionExpression='#name1 = :val1 OR #name2 = :val2',
        UpdateExpression='SET #name1 = :val3',
        ExpressionAttributeValues={':val1': 1, ':val2': 2, ':val3': 3},
        ExpressionAttributeNames={'#name1': 'a', '#name2': 'b'})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': 3}

# Test a bunch of cases with permissive write isolation levels,
# i.e. LWT_ALWAYS, LWT_RMW_ONLY and UNSAFE_RMW.
# These test cases make sense only for alternator, so they're skipped
# when run on AWS
def test_condition_expression_with_permissive_write_isolation(scylla_only, dynamodb, test_table_s):
    try:
        for isolation in ['a', 'o', 'u']:
            set_write_isolation(test_table_s, isolation)
            for test_case in [test_update_condition_eq_success,
                              test_update_condition_attribute_exists,
                              test_delete_item_condition,
                              test_put_item_condition,
                              test_update_condition_attribute_reference]:
                test_case(test_table_s)
    finally:
        clear_write_isolation(test_table_s)

# Test that the forbid_rmw isolation level prevents read-modify-write requests
# from working. These test cases make sense only for alternator, so they're skipped
# when run on AWS (test_table_s_forbid_rmw implies scylla_only)
def test_condition_expression_with_forbidden_rmw(dynamodb, test_table_s_forbid_rmw):
    for test_case in [test_update_condition_eq_success, test_update_condition_attribute_exists,
                      test_put_item_condition, test_update_condition_attribute_reference]:
        with pytest.raises(ClientError):
            test_case(test_table_s_forbid_rmw)
    # Ensure that regular writes (without rmw) work just fine
    s = random_string()
    test_table_s_forbid_rmw.put_item(Item={'p': s, 'regular': 'write'})
    assert test_table_s_forbid_rmw.get_item(Key={'p': s}, ConsistentRead=True)['Item'] == {'p': s, 'regular': 'write'}
    test_table_s_forbid_rmw.update_item(Key={'p': s}, AttributeUpdates={'write': {'Value': 'regular', 'Action': 'PUT'}})
    assert test_table_s_forbid_rmw.get_item(Key={'p': s}, ConsistentRead=True)['Item'] == {'p': s, 'regular': 'write', 'write': 'regular'}

# Reproducer for issue #6573: binary strings should be ordered as unsigned
# bytes, i.e., byte 128 comes after 127, not before as with signed bytes.
# Test the five ordering operators: <, <=, >, >=, between
def test_condition_expression_unsigned_bytes(test_table_s):
    p = random_string()
    test_table_s.put_item(Item={'p': p, 'b': bytearray([127])})
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :newval',
        ConditionExpression='b < :oldval',
        ExpressionAttributeValues={':newval': 1, ':oldval': bytearray([128])})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 1
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :newval',
        ConditionExpression='b <= :oldval',
        ExpressionAttributeValues={':newval': 2, ':oldval': bytearray([128])})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 2
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :newval',
        ConditionExpression='b between :oldval1 and :oldval2',
        ExpressionAttributeValues={':newval': 3, ':oldval1': bytearray([126]), ':oldval2': bytearray([128])})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 3

    test_table_s.put_item(Item={'p': p, 'b': bytearray([128])})
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :newval',
        ConditionExpression='b > :oldval',
        ExpressionAttributeValues={':newval': 4, ':oldval': bytearray([127])})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 4
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET z = :newval',
        ConditionExpression='b >= :oldval',
        ExpressionAttributeValues={':newval': 5, ':oldval': bytearray([127])})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 5

# In all other tests above, we use ConditionExpression to check a condition
# on one the non-key attributes. In this test we confirm that a condition may
# also be on a key attribute. We demonstrate this through a useful DynamoDB
# idiom for creating an item unless an item already exists with the same key,
# by using a "<>" (not equal) condition.
def test_update_item_condition_key_ne(test_table_s):
    p = random_string()
    assert not 'Item' in test_table_s.get_item(Key={'p': p}, ConsistentRead=True)
    # Create an empty item with key p, but only an item with p exists yet.
    # Note how when the item does not exist, the <> (not equal) test succeeds
    # (we already tested that in test_update_condition_ne())
    test_table_s.update_item(Key={'p': p},
        ConditionExpression='p <> :p',
        ExpressionAttributeValues={':p': p})
    assert 'Item' in test_table_s.get_item(Key={'p': p}, ConsistentRead=True)
    # If we do the same again, the item does exist, and the <> condition will
    # fail.
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            ConditionExpression='p <> :p',
            ExpressionAttributeValues={':p': p})

# Another example of a condition on the key, again an idiom for creating an
# item if no item already has that key. This time, using the
# attribute_not_exists() instead of the <> (not equal) operator in the test
# above.
def test_update_item_condition_key_attribute_not_exists(test_table_s):
    p = random_string()
    assert not 'Item' in test_table_s.get_item(Key={'p': p}, ConsistentRead=True)
    # Create an empty item with key p, but only an item with p exists yet.
    # Note how when the item does not exist, attribute_not_exists() succeeds
    test_table_s.update_item(Key={'p': p},
        ConditionExpression='attribute_not_exists(p)')
    assert 'Item' in test_table_s.get_item(Key={'p': p}, ConsistentRead=True)
    # If we do the same again, the item does exist, and the
    # attribute_not_exists() condition will fail.
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            ConditionExpression='attribute_not_exists(p)')
