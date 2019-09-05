# Copyright 2019 ScyllaDB
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

# Tests for the "Expected" parameter used to make certain operations (PutItem,
# UpdateItem and DeleteItem) conditional on the existing attribute values.
# "Expected" is the older version of ConditionExpression parameter, which
# is tested by the separate test_condition_expression.py.

import pytest
from botocore.exceptions import ClientError
from util import random_string

# Most of the tests in this file check that the "Expected" parameter works for
# the UpdateItem operation. It should also work the same for the PutItem and
# DeleteItem operations, and we'll make a small effort verifying that at
# the end of the file.

# Somewhat pedanticly, DynamoDB forbids using old-style Expected together
# with new-style UpdateExpression... Expected can only be used with
# AttributeUpdates (and for UpdateExpression, ConditionExpression should be
# used).
def test_update_expression_and_expected(test_table_s):
    p = random_string()
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET a = :val1',
        ExpressionAttributeValues={':val1': 1})
    with pytest.raises(ClientError, match='ValidationException.*UpdateExpression'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET a = :val1',
            ExpressionAttributeValues={':val1': 2},
            Expected={'a': {'ComparisonOperator': 'EQ',
                            'AttributeValueList': [1]}}
        )

# The following string of tests test the various types of Expected conditions
# on a single attribute. This condition is defined using ComparisonOperator
# (there are many types of those!) or by Value or Exists, and we need to check
# all these types of conditions.
#
# In each case we have tests for the "true" case of the condition, meaning
# that the condition evaluates to true and the update is supposed to happen,
# and the "false" case, where the condition evaluates to false, so the update
# doesn't happen and we get a ConditionalCheckFailedException instead.

def test_update_expected_1_eq_true(test_table_s):
    p = random_string()
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Value': 1, 'Action': 'PUT'}})
    # Case where expected and update are on the same attribute:
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Value': 2, 'Action': 'PUT'}},
        Expected={'a': {'ComparisonOperator': 'EQ',
                        'AttributeValueList': [1]}}
    )
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': 2}
    # Case where expected and update are on different attribute:
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'b': {'Value': 3, 'Action': 'PUT'}},
        Expected={'a': {'ComparisonOperator': 'EQ',
                        'AttributeValueList': [2]}}
    )
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': 2, 'b': 3}
    # For EQ, AttributeValueList must have a single element
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'b': {'Value': 3, 'Action': 'PUT'}},
            Expected={'a': {'ComparisonOperator': 'EQ',
                            'AttributeValueList': [2, 3]}}
        )

def test_update_expected_1_eq_false(test_table_s):
    p = random_string()
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Value': 1, 'Action': 'PUT'}})
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'b': {'Value': 3, 'Action': 'PUT'}},
            Expected={'a': {'ComparisonOperator': 'EQ',
                            'AttributeValueList': [2]}}
        )
    # If the compared value has a different type, it results in the
    # condition failing normally (it's not a validation error).
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'b': {'Value': 3, 'Action': 'PUT'}},
            Expected={'a': {'ComparisonOperator': 'EQ',
                            'AttributeValueList': ['dog']}}
        )
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': 1}

def test_update_expected_1_begins_with_true(test_table_s):
    p = random_string()
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Value': 'hello', 'Action': 'PUT'}})
    # Case where expected and update are on different attribute:
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'b': {'Value': 3, 'Action': 'PUT'}},
        Expected={'a': {'ComparisonOperator': 'BEGINS_WITH',
                        'AttributeValueList': ['hell']}}
    )
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': 'hello', 'b': 3}
    # For BEGIN_WITH, AttributeValueList must have a single element
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'b': {'Value': 3, 'Action': 'PUT'}},
            Expected={'a': {'ComparisonOperator': 'EQ',
                            'AttributeValueList': ['hell', 'heaven']}}
        )

def test_update_expected_1_begins_with_false(test_table_s):
    p = random_string()
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Value': 'hello', 'Action': 'PUT'}})
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'b': {'Value': 3, 'Action': 'PUT'}},
            Expected={'a': {'ComparisonOperator': 'EQ',
                            'AttributeValueList': ['dog']}}
        )
    # Although BEGINS_WITH requires String or Binary type, giving it a
    # number results not with a ValidationException but rather a
    # failed condition (ConditionalCheckFailedException)
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'b': {'Value': 3, 'Action': 'PUT'}},
            Expected={'a': {'ComparisonOperator': 'EQ',
                            'AttributeValueList': [3]}}
        )
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': 'hello'}

# FIXME: need to test many more ComparisonOperator options... See full list in
# description in https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/LegacyConditionalParameters.Expected.html

# Instead of ComparisonOperator and AttributeValueList, one can specify either
# Value or Exists:
def test_update_expected_1_value_true(test_table_s):
    p = random_string()
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Value': 1, 'Action': 'PUT'}})
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'b': {'Value': 2, 'Action': 'PUT'}},
        Expected={'a': {'Value': 1}}
    )
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': 1, 'b': 2}

def test_update_expected_1_value_false(test_table_s):
    p = random_string()
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Value': 1, 'Action': 'PUT'}})
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'b': {'Value': 2, 'Action': 'PUT'}},
            Expected={'a': {'Value': 2}}
        )
    # If the expected attribute is completely missing, the condition also fails
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'b': {'Value': 2, 'Action': 'PUT'}},
            Expected={'z': {'Value': 1}}
        )
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': 1}

def test_update_expected_1_exists_true(test_table_s):
    p = random_string()
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Value': 1, 'Action': 'PUT'}})
    # Surprisingly, the "Exists: True" cannot be used to confirm that the
    # attribute had *any* old value (use the NOT_NULL comparison operator
    # for that). It can only be used together with "Value", and in that case
    # doesn't mean a thing.
    # Only "Exists: False" has an interesting meaning.
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'b': {'Value': 2, 'Action': 'PUT'}},
            Expected={'a': {'Exists': True}}
        )
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'c': {'Value': 3, 'Action': 'PUT'}},
        Expected={'a': {'Exists': True, 'Value': 1}}
    )
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'d': {'Value': 4, 'Action': 'PUT'}},
        Expected={'z': {'Exists': False}}
    )
    # Exists: False cannot be used together with a Value:
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'c': {'Value': 3, 'Action': 'PUT'}},
            Expected={'a': {'Exists': False, 'Value': 1}}
        )
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': 1, 'c': 3, 'd': 4}

def test_update_expected_1_exists_false(test_table_s):
    p = random_string()
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Value': 1, 'Action': 'PUT'}})
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'b': {'Value': 2, 'Action': 'PUT'}},
            Expected={'a': {'Exists': False}}
        )
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'b': {'Value': 2, 'Action': 'PUT'}},
            Expected={'a': {'Exists': True, 'Value': 2}}
        )
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': 1}

# Test that it's not allowed to combine ComparisonOperator and Exists or Value
def test_update_expected_operator_clash(test_table_s):
    p = random_string()
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Value': 1, 'Action': 'PUT'}})
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'b': {'Value': 2, 'Action': 'PUT'}},
            Expected={'a': {'Exists': False, 'ComparisonOperator': 'EQ', 'AttributeValueList': [3]}})
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'b': {'Value': 2, 'Action': 'PUT'}},
            Expected={'a': {'Value': 3, 'ComparisonOperator': 'EQ', 'AttributeValueList': [3]}})

# All the previous tests involved a single condition on a single attribute.
# The following tests involving multiple conditions on multiple attributes.
# ConditionalOperator defaults to AND, and can also be set to OR.

def test_update_expected_multi_true(test_table_s):
    p = random_string()
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Value': 1, 'Action': 'PUT'},
                          'b': {'Value': 2, 'Action': 'PUT'}})
    # Test several conditions with default "AND" operator
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'z': {'Value': 3, 'Action': 'PUT'}},
        Expected={'a': {'Exists': True, 'Value': 1},
                  'b': {'ComparisonOperator': 'EQ', 'AttributeValueList': [2]},
                  'c': {'Exists': False}})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': 1, 'b': 2, 'z': 3}
    # Same with explicit "AND" operator
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'z': {'Value': 4, 'Action': 'PUT'}},
        Expected={'a': {'Exists': True, 'Value': 1},
                  'b': {'ComparisonOperator': 'EQ', 'AttributeValueList': [2]},
                  'c': {'Exists': False}},
        ConditionalOperator="AND")
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': 1, 'b': 2, 'z': 4}
    # With "OR" operator, it's enough that just one conditions is true
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'z': {'Value': 5, 'Action': 'PUT'}},
        Expected={'a': {'Exists': True, 'Value': 74},
                  'b': {'ComparisonOperator': 'EQ', 'AttributeValueList': [999]},
                  'c': {'Exists': False}},
        ConditionalOperator="OR")
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': 1, 'b': 2, 'z': 5}

def test_update_expected_multi_false(test_table_s):
    p = random_string()
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Value': 1, 'Action': 'PUT'},
                          'b': {'Value': 2, 'Action': 'PUT'},
                          'c': {'Value': 3, 'Action': 'PUT'}})
    # Test several conditions, one of them false, with default "AND" operator
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'z': {'Value': 3, 'Action': 'PUT'}},
            Expected={'a': {'Exists': True, 'Value': 1},
                      'b': {'ComparisonOperator': 'EQ', 'AttributeValueList': [3]},
                      'd': {'Exists': False}})
    # Same with explicit "AND" operator
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'z': {'Value': 4, 'Action': 'PUT'}},
            Expected={'a': {'Exists': True, 'Value': 1},
                      'b': {'ComparisonOperator': 'EQ', 'AttributeValueList': [3]},
                      'd': {'Exists': False}},
            ConditionalOperator="AND")
    # With "OR" operator, all the conditions need to be false to fail
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'z': {'Value': 5, 'Action': 'PUT'}},
            Expected={'a': {'Exists': True, 'Value': 74},
                      'b': {'ComparisonOperator': 'EQ', 'AttributeValueList': [999]},
                      'c': {'Exists': False}},
            ConditionalOperator='OR')
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': 1, 'b': 2, 'c': 3}

# Verify the behaviour of an empty Expected parameter:
def test_update_expected_empty(test_table_s):
    p = random_string()
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Value': 1, 'Action': 'PUT'}})
    # An empty Expected array results in a successful update:
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'z': {'Value': 3, 'Action': 'PUT'}},
        Expected={})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': 1, 'z': 3}
    # Trying with ConditionalOperator complains that you can't have
    # ConditionalOperator without Expected (despite Expected existing, though empty).
    with pytest.raises(ClientError, match='ValidationException.*ConditionalOperator'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'z': {'Value': 4, 'Action': 'PUT'}},
            Expected={}, ConditionalOperator='OR')
    with pytest.raises(ClientError, match='ValidationException.*ConditionalOperator'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'z': {'Value': 4, 'Action': 'PUT'}},
            Expected={}, ConditionalOperator='AND')

# All of the above tests tested "Expected" with the UpdateItem operation.
# We now want to test that it works also with the PutItem and DeleteItems
# operations. We don't need to check again all the different sub-cases tested
# above - we can assume that exactly the same code gets used to test the
# expected value. So we just need one test for each operation, to verify that
# this code actually gets called.

def test_delete_item_expected(test_table_s):
    p = random_string()
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Value': 1, 'Action': 'PUT'}})
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.delete_item(Key={'p': p}, Expected={'a': {'Value': 2}})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': 1}
    test_table_s.delete_item(Key={'p': p}, Expected={'a': {'Value': 1}})
    assert not 'Item' in test_table_s.get_item(Key={'p': p}, ConsistentRead=True)

def test_put_item_expected(test_table_s):
    p = random_string()
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Value': 1, 'Action': 'PUT'}})
    test_table_s.put_item(Item={'p': p, 'a': 2}, Expected={'a': {'Value': 1}})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': 2}
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.put_item(Item={'p': p, 'a': 3}, Expected={'a': {'Value': 1}})
