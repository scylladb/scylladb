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

# Tests for Expected with ComparisonOperator = "EQ":
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

# Check that set equality is checked correctly. Unlike string equality (for
# example), it cannot be done with just naive string comparison of the JSON
# representation, and we need to allow for any order.
def test_update_expected_1_eq_set(test_table_s):
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
    # Now finally check that "Expected"'s equality check knows the equality too.
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'b': {'Value': 3, 'Action': 'PUT'}},
        Expected={'a': {'ComparisonOperator': 'EQ',
                        'AttributeValueList': [set(['chinchilla', 'cat', 'dog', 'mouse'])]}}
    )
    assert 'b' in test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']

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

# Tests for Expected with ComparisonOperator = "NE":
def test_update_expected_1_ne_true(test_table_s):
    p = random_string()
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Value': 1, 'Action': 'PUT'}})
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'b': {'Value': 3, 'Action': 'PUT'}},
        Expected={'a': {'ComparisonOperator': 'NE',
                        'AttributeValueList': [2]}}
    )
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': 1, 'b': 3}
    # For NE, AttributeValueList must have a single element
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'b': {'Value': 3, 'Action': 'PUT'}},
            Expected={'a': {'ComparisonOperator': 'NE',
                            'AttributeValueList': [2, 3]}}
        )
    # If the types are different, this is considered "not equal":
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'b': {'Value': 4, 'Action': 'PUT'}},
        Expected={'a': {'ComparisonOperator': 'NE',
                        'AttributeValueList': ["1"]}}
    )
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': 1, 'b': 4}
    # If the attribute does not exist at all, this is also considered "not equal":
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'b': {'Value': 5, 'Action': 'PUT'}},
        Expected={'q': {'ComparisonOperator': 'NE',
                        'AttributeValueList': [1]}}
    )
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': 1, 'b': 5}

def test_update_expected_1_ne_false(test_table_s):
    p = random_string()
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Value': 1, 'Action': 'PUT'}})
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'b': {'Value': 3, 'Action': 'PUT'}},
            Expected={'a': {'ComparisonOperator': 'NE',
                            'AttributeValueList': [1]}}
        )

# Tests for Expected with ComparisonOperator = "LE":
def test_update_expected_1_le(test_table_s):
    p = random_string()
    # LE should work for string, number, and binary type
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Value': 1, 'Action': 'PUT'},
                          'b': {'Value': 'cat', 'Action': 'PUT'},
                          'c': {'Value': bytearray('cat', 'utf-8'), 'Action': 'PUT'}})
    # true cases:
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'z': {'Value': 2, 'Action': 'PUT'}},
        Expected={'a': {'ComparisonOperator': 'LE',
                        'AttributeValueList': [2]}}
    )
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 2
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'z': {'Value': 3, 'Action': 'PUT'}},
        Expected={'a': {'ComparisonOperator': 'LE',
                        'AttributeValueList': [1]}}
    )
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 3
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'z': {'Value': 4, 'Action': 'PUT'}},
        Expected={'b': {'ComparisonOperator': 'LE',
                        'AttributeValueList': ['dog']}}
    )
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 4
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'z': {'Value': 5, 'Action': 'PUT'}},
        Expected={'c': {'ComparisonOperator': 'LE',
                        'AttributeValueList': [bytearray('dog', 'utf-8')]}}
    )
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 5
    # false cases:
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'z': {'Value': 17, 'Action': 'PUT'}},
            Expected={'a': {'ComparisonOperator': 'LE',
                            'AttributeValueList': [0]}}
        )
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'z': {'Value': 17, 'Action': 'PUT'}},
            Expected={'b': {'ComparisonOperator': 'LE',
                            'AttributeValueList': ['aardvark']}}
        )
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'z': {'Value': 17, 'Action': 'PUT'}},
            Expected={'c': {'ComparisonOperator': 'LE',
                            'AttributeValueList': [bytearray('aardvark', 'utf-8')]}}
        )
    # If the types are different, this is also considered false
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'z': {'Value': 17, 'Action': 'PUT'}},
            Expected={'a': {'ComparisonOperator': 'LE',
                            'AttributeValueList': ["1"]}}
        )
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 5
    # For LE, AttributeValueList must have a single element
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'b': {'Value': 3, 'Action': 'PUT'}},
            Expected={'a': {'ComparisonOperator': 'LE',
                            'AttributeValueList': [2, 3]}}
        )

# Tests for Expected with ComparisonOperator = "LT":
def test_update_expected_1_lt(test_table_s):
    p = random_string()
    # LT should work for string, number, and binary type
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Value': 1, 'Action': 'PUT'},
                          'b': {'Value': 'cat', 'Action': 'PUT'},
                          'c': {'Value': bytearray('cat', 'utf-8'), 'Action': 'PUT'}})
    # true cases:
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'z': {'Value': 2, 'Action': 'PUT'}},
        Expected={'a': {'ComparisonOperator': 'LT',
                        'AttributeValueList': [2]}}
    )
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 2
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'z': {'Value': 4, 'Action': 'PUT'}},
        Expected={'b': {'ComparisonOperator': 'LT',
                        'AttributeValueList': ['dog']}}
    )
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 4
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'z': {'Value': 5, 'Action': 'PUT'}},
        Expected={'c': {'ComparisonOperator': 'LT',
                        'AttributeValueList': [bytearray('dog', 'utf-8')]}}
    )
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 5
    # false cases:
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'z': {'Value': 17, 'Action': 'PUT'}},
            Expected={'a': {'ComparisonOperator': 'LT',
                            'AttributeValueList': [1]}}
        )
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'z': {'Value': 17, 'Action': 'PUT'}},
            Expected={'a': {'ComparisonOperator': 'LT',
                            'AttributeValueList': [0]}}
        )
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'z': {'Value': 17, 'Action': 'PUT'}},
            Expected={'b': {'ComparisonOperator': 'LT',
                            'AttributeValueList': ['aardvark']}}
        )
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'z': {'Value': 17, 'Action': 'PUT'}},
            Expected={'c': {'ComparisonOperator': 'LT',
                            'AttributeValueList': [bytearray('aardvark', 'utf-8')]}}
        )
    # If the types are different, this is also considered false
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'z': {'Value': 17, 'Action': 'PUT'}},
            Expected={'a': {'ComparisonOperator': 'LT',
                            'AttributeValueList': ["1"]}}
        )
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 5
    # For LT, AttributeValueList must have a single element
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'b': {'Value': 3, 'Action': 'PUT'}},
            Expected={'a': {'ComparisonOperator': 'LT',
                            'AttributeValueList': [2, 3]}}
        )

# Tests for Expected with ComparisonOperator = "GE":
def test_update_expected_1_ge(test_table_s):
    p = random_string()
    # GE should work for string, number, and binary type
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Value': 1, 'Action': 'PUT'},
                          'b': {'Value': 'cat', 'Action': 'PUT'},
                          'c': {'Value': bytearray('cat', 'utf-8'), 'Action': 'PUT'}})
    # true cases:
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'z': {'Value': 2, 'Action': 'PUT'}},
        Expected={'a': {'ComparisonOperator': 'GE',
                        'AttributeValueList': [0]}}
    )
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 2
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'z': {'Value': 3, 'Action': 'PUT'}},
        Expected={'a': {'ComparisonOperator': 'GE',
                        'AttributeValueList': [1]}}
    )
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 3
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'z': {'Value': 4, 'Action': 'PUT'}},
        Expected={'b': {'ComparisonOperator': 'GE',
                        'AttributeValueList': ['aardvark']}}
    )
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 4
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'z': {'Value': 5, 'Action': 'PUT'}},
        Expected={'c': {'ComparisonOperator': 'GE',
                        'AttributeValueList': [bytearray('aardvark', 'utf-8')]}}
    )
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 5
    # false cases:
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'z': {'Value': 17, 'Action': 'PUT'}},
            Expected={'a': {'ComparisonOperator': 'GE',
                            'AttributeValueList': [3]}}
        )
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'z': {'Value': 17, 'Action': 'PUT'}},
            Expected={'b': {'ComparisonOperator': 'GE',
                            'AttributeValueList': ['dog']}}
        )
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'z': {'Value': 17, 'Action': 'PUT'}},
            Expected={'c': {'ComparisonOperator': 'GE',
                            'AttributeValueList': [bytearray('dog', 'utf-8')]}}
        )
    # If the types are different, this is also considered false
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'z': {'Value': 17, 'Action': 'PUT'}},
            Expected={'a': {'ComparisonOperator': 'GE',
                            'AttributeValueList': ["1"]}}
        )
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 5
    # For GE, AttributeValueList must have a single element
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'b': {'Value': 3, 'Action': 'PUT'}},
            Expected={'a': {'ComparisonOperator': 'GE',
                            'AttributeValueList': [2, 3]}}
        )

# Tests for Expected with ComparisonOperator = "GT":
def test_update_expected_1_gt(test_table_s):
    p = random_string()
    # GT should work for string, number, and binary type
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Value': 1, 'Action': 'PUT'},
                          'b': {'Value': 'cat', 'Action': 'PUT'},
                          'c': {'Value': bytearray('cat', 'utf-8'), 'Action': 'PUT'}})
    # true cases:
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'z': {'Value': 2, 'Action': 'PUT'}},
        Expected={'a': {'ComparisonOperator': 'GT',
                        'AttributeValueList': [0]}}
    )
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 2
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'z': {'Value': 4, 'Action': 'PUT'}},
        Expected={'b': {'ComparisonOperator': 'GT',
                        'AttributeValueList': ['aardvark']}}
    )
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 4
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'z': {'Value': 5, 'Action': 'PUT'}},
        Expected={'c': {'ComparisonOperator': 'GT',
                        'AttributeValueList': [bytearray('aardvark', 'utf-8')]}}
    )
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 5
    # false cases:
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'z': {'Value': 17, 'Action': 'PUT'}},
            Expected={'a': {'ComparisonOperator': 'GT',
                            'AttributeValueList': [3]}}
        )
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'z': {'Value': 17, 'Action': 'PUT'}},
            Expected={'a': {'ComparisonOperator': 'GT',
                            'AttributeValueList': [1]}}
        )
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'z': {'Value': 17, 'Action': 'PUT'}},
            Expected={'b': {'ComparisonOperator': 'GT',
                            'AttributeValueList': ['dog']}}
        )
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'z': {'Value': 17, 'Action': 'PUT'}},
            Expected={'c': {'ComparisonOperator': 'GT',
                            'AttributeValueList': [bytearray('dog', 'utf-8')]}}
        )
    # If the types are different, this is also considered false
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'z': {'Value': 17, 'Action': 'PUT'}},
            Expected={'a': {'ComparisonOperator': 'GT',
                            'AttributeValueList': ["1"]}}
        )
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 5
    # For GE, AttributeValueList must have a single element
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'b': {'Value': 3, 'Action': 'PUT'}},
            Expected={'a': {'ComparisonOperator': 'GT',
                            'AttributeValueList': [2, 3]}}
        )

# Tests for Expected with ComparisonOperator = "NOT_NULL":
def test_update_expected_1_not_null(test_table_s):
    # Note that despite its name, the "NOT_NULL" comparison operator doesn't check if
    # the attribute has the type "NULL", or an empty value. Rather it is explicitly
    # documented to check if the attribute exists at all.
    p = random_string()
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Value': 1, 'Action': 'PUT'},
                          'b': {'Value': 'cat', 'Action': 'PUT'},
                          'c': {'Value': None, 'Action': 'PUT'}})
    # true cases:
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'z': {'Value': 2, 'Action': 'PUT'}},
        Expected={'a': {'ComparisonOperator': 'NOT_NULL', 'AttributeValueList': []}}
    )
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 2
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'z': {'Value': 3, 'Action': 'PUT'}},
        Expected={'b': {'ComparisonOperator': 'NOT_NULL', 'AttributeValueList': []}}
    )
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 3
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'z': {'Value': 4, 'Action': 'PUT'}},
        Expected={'c': {'ComparisonOperator': 'NOT_NULL', 'AttributeValueList': []}}
    )
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 4
    # false cases:
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'z': {'Value': 17, 'Action': 'PUT'}},
            Expected={'q': {'ComparisonOperator': 'NOT_NULL', 'AttributeValueList': []}}
        )
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 4
    # For NOT_NULL, AttributeValueList must be empty
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'b': {'Value': 17, 'Action': 'PUT'}},
            Expected={'a': {'ComparisonOperator': 'NOT_NULL', 'AttributeValueList': [2]}}
        )

# Tests for Expected with ComparisonOperator = "NULL":
def test_update_expected_1_null(test_table_s):
    # Note that despite its name, the "NULL" comparison operator doesn't check if
    # the attribute has the type "NULL", or an empty value. Rather it is explicitly
    # documented to check if the attribute exists at all.
    p = random_string()
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Value': 1, 'Action': 'PUT'},
                          'b': {'Value': 'cat', 'Action': 'PUT'},
                          'c': {'Value': None, 'Action': 'PUT'}})
    # true cases:
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'z': {'Value': 2, 'Action': 'PUT'}},
        Expected={'q': {'ComparisonOperator': 'NULL', 'AttributeValueList': []}}
    )
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 2
    # false cases:
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'z': {'Value': 2, 'Action': 'PUT'}},
            Expected={'a': {'ComparisonOperator': 'NULL', 'AttributeValueList': []}}
        )
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'z': {'Value': 3, 'Action': 'PUT'}},
            Expected={'b': {'ComparisonOperator': 'NULL', 'AttributeValueList': []}}
        )
        assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 3
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'z': {'Value': 4, 'Action': 'PUT'}},
            Expected={'c': {'ComparisonOperator': 'NULL', 'AttributeValueList': []}}
        )
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 2
    # For NULL, AttributeValueList must be empty
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'b': {'Value': 17, 'Action': 'PUT'}},
            Expected={'a': {'ComparisonOperator': 'NULL', 'AttributeValueList': [2]}}
        )

# Tests for Expected with ComparisonOperator = "CONTAINS":
def test_update_expected_1_contains(test_table_s):
    # true cases. CONTAINS can be used for two unrelated things: check substrings
    # (in string or binary) and membership (in set or list).
    p = random_string()
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Value': 'hello', 'Action': 'PUT'},
                          'b': {'Value': set([2, 4, 7]), 'Action': 'PUT'},
                          'c': {'Value': [2, 4, 7], 'Action': 'PUT'},
                          'd': {'Value': bytearray('hi there', 'utf-8'), 'Action': 'PUT'}})
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'z': {'Value': 2, 'Action': 'PUT'}},
        Expected={'a': {'ComparisonOperator': 'CONTAINS', 'AttributeValueList': ['ell']}}
    )
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 2
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'z': {'Value': 3, 'Action': 'PUT'}},
        Expected={'b': {'ComparisonOperator': 'CONTAINS', 'AttributeValueList': [4]}}
    )
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 3
    # The CONTAINS documentation uses confusing wording on whether it works
    # only on sets, or also on lists. In fact, it does work on lists:
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'z': {'Value': 4, 'Action': 'PUT'}},
        Expected={'c': {'ComparisonOperator': 'CONTAINS', 'AttributeValueList': [4]}}
    )
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 4
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'z': {'Value': 5, 'Action': 'PUT'}},
        Expected={'d': {'ComparisonOperator': 'CONTAINS', 'AttributeValueList': [bytearray('here', 'utf-8')]}}
    )
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 5
    # false cases:
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'z': {'Value': 2, 'Action': 'PUT'}},
            Expected={'a': {'ComparisonOperator': 'CONTAINS', 'AttributeValueList': ['dog']}}
        )
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'z': {'Value': 2, 'Action': 'PUT'}},
            Expected={'a': {'ComparisonOperator': 'CONTAINS', 'AttributeValueList': [1]}}
        )
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'z': {'Value': 2, 'Action': 'PUT'}},
            Expected={'b': {'ComparisonOperator': 'CONTAINS', 'AttributeValueList': [1]}}
        )
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'z': {'Value': 2, 'Action': 'PUT'}},
            Expected={'c': {'ComparisonOperator': 'CONTAINS', 'AttributeValueList': [1]}}
        )
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'z': {'Value': 2, 'Action': 'PUT'}},
            Expected={'q': {'ComparisonOperator': 'CONTAINS', 'AttributeValueList': [1]}}
        )
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'z': {'Value': 2, 'Action': 'PUT'}},
            Expected={'d': {'ComparisonOperator': 'CONTAINS', 'AttributeValueList': [bytearray('dog', 'utf-8')]}}
        )
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 5
    # For CONTAINS, AttributeValueList must have just one item, and it must be
    # a string, number or binary
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'z': {'Value': 17, 'Action': 'PUT'}},
            Expected={'a': {'ComparisonOperator': 'CONTAINS', 'AttributeValueList': [2, 3]}}
        )
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'z': {'Value': 17, 'Action': 'PUT'}},
            Expected={'a': {'ComparisonOperator': 'CONTAINS', 'AttributeValueList': []}}
        )
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'z': {'Value': 17, 'Action': 'PUT'}},
            Expected={'a': {'ComparisonOperator': 'CONTAINS', 'AttributeValueList': [[1]]}}
        )

# Tests for Expected with ComparisonOperator = "NOT_CONTAINS":
def test_update_expected_1_not_contains(test_table_s):
    # true cases. NOT_CONTAINS can be used for two unrelated things: check substrings
    # (in string or binary) and membership (in set or list).
    p = random_string()
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Value': 'hello', 'Action': 'PUT'},
                          'b': {'Value': set([2, 4, 7]), 'Action': 'PUT'},
                          'c': {'Value': [2, 4, 7], 'Action': 'PUT'},
                          'd': {'Value': bytearray('hi there', 'utf-8'), 'Action': 'PUT'}})

    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'z': {'Value': 2, 'Action': 'PUT'}},
        Expected={'a': {'ComparisonOperator': 'NOT_CONTAINS', 'AttributeValueList': ['dog']}}
    )
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 2
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'z': {'Value': 3, 'Action': 'PUT'}},
        Expected={'a': {'ComparisonOperator': 'NOT_CONTAINS', 'AttributeValueList': [1]}}
    )
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 3
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'z': {'Value': 4, 'Action': 'PUT'}},
        Expected={'b': {'ComparisonOperator': 'NOT_CONTAINS', 'AttributeValueList': [1]}}
    )
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 4
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'z': {'Value': 5, 'Action': 'PUT'}},
        Expected={'c': {'ComparisonOperator': 'NOT_CONTAINS', 'AttributeValueList': [1]}}
    )
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 5
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'z': {'Value': 7, 'Action': 'PUT'}},
        Expected={'d': {'ComparisonOperator': 'NOT_CONTAINS', 'AttributeValueList': [bytearray('dog', 'utf-8')]}}
    )
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 7

    # false cases:
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'z': {'Value': 2, 'Action': 'PUT'}},
            Expected={'a': {'ComparisonOperator': 'NOT_CONTAINS', 'AttributeValueList': ['ell']}}
        )
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'z': {'Value': 3, 'Action': 'PUT'}},
            Expected={'b': {'ComparisonOperator': 'NOT_CONTAINS', 'AttributeValueList': [4]}}
        )
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'z': {'Value': 4, 'Action': 'PUT'}},
            Expected={'c': {'ComparisonOperator': 'NOT_CONTAINS', 'AttributeValueList': [4]}}
        )
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'z': {'Value': 5, 'Action': 'PUT'}},
            Expected={'d': {'ComparisonOperator': 'NOT_CONTAINS', 'AttributeValueList': [bytearray('here', 'utf-8')]}}
        )
    # Surprisingly, if an attribute does not exist at all, NOT_CONTAINS
    # fails, rather than succeeding. This is surprising because it means in
    # this case both CONTAINS and NOT_CONTAINS are false, and because "NE" does not
    # behave this way (if the attribute does not exist, NE succeeds).
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'z': {'Value': 17, 'Action': 'PUT'}},
            Expected={'q': {'ComparisonOperator': 'NOT_CONTAINS', 'AttributeValueList': [1]}}
        )
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 7
    # For NOT_CONTAINS, AttributeValueList must have just one item, and it must be
    # a string, number or binary
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'z': {'Value': 17, 'Action': 'PUT'}},
            Expected={'a': {'ComparisonOperator': 'NOT_CONTAINS', 'AttributeValueList': [2, 3]}}
        )
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'z': {'Value': 17, 'Action': 'PUT'}},
            Expected={'a': {'ComparisonOperator': 'NOT_CONTAINS', 'AttributeValueList': []}}
        )
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'z': {'Value': 17, 'Action': 'PUT'}},
            Expected={'a': {'ComparisonOperator': 'NOT_CONTAINS', 'AttributeValueList': [[1]]}}
        )

# Tests for Expected with ComparisonOperator = "BEGINS_WITH":
def test_update_expected_1_begins_with_true(test_table_s):
    p = random_string()
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Value': 'hello', 'Action': 'PUT'},
                          'd': {'Value': bytearray('hi there', 'utf-8'), 'Action': 'PUT'}})
    # Case where expected and update are on different attribute:
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'b': {'Value': 3, 'Action': 'PUT'}},
        Expected={'a': {'ComparisonOperator': 'BEGINS_WITH',
                        'AttributeValueList': ['hell']}}
    )
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['b'] == 3
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'b': {'Value': 4, 'Action': 'PUT'}},
        Expected={'d': {'ComparisonOperator': 'BEGINS_WITH',
                        'AttributeValueList': [bytearray('hi', 'utf-8')]}}
    )
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['b'] == 4
    # For BEGINS_WITH, AttributeValueList must have a single element
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'b': {'Value': 3, 'Action': 'PUT'}},
            Expected={'a': {'ComparisonOperator': 'BEGINS_WITH',
                            'AttributeValueList': ['hell', 'heaven']}}
        )

def test_update_expected_1_begins_with_false(test_table_s):
    p = random_string()
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Value': 'hello', 'Action': 'PUT'},
                          'x': {'Value': 3, 'Action': 'PUT'}})
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'b': {'Value': 3, 'Action': 'PUT'}},
            Expected={'a': {'ComparisonOperator': 'BEGINS_WITH',
                            'AttributeValueList': ['dog']}}
        )
    # BEGINS_WITH requires String or Binary operand, giving it a number
    # results with a ValidationException (not a normal failed condition):
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'b': {'Value': 3, 'Action': 'PUT'}},
            Expected={'a': {'ComparisonOperator': 'BEGINS_WITH',
                            'AttributeValueList': [3]}}
        )
    # However, if we try to compare the attribute to a String or Binary, and
    # the attribute value itself is a number, this is just a failed condition:
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'b': {'Value': 3, 'Action': 'PUT'}},
            Expected={'x': {'ComparisonOperator': 'BEGINS_WITH',
                            'AttributeValueList': ['dog']}}
        )
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': 'hello', 'x': 3}

# Tests for Expected with ComparisonOperator = "IN":
def test_update_expected_1_in(test_table_s):
    # Some copies of "IN"'s documentation are outright wrong: "IN" checks
    # whether the attribute value is in the give list of values. It does NOT
    # do the opposite - testing whether certain items are in a set attribute.
    p = random_string()
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Value': set([2, 4, 7]), 'Action': 'PUT'},
                          'c': {'Value': 3, 'Action': 'PUT'}})
    # true cases:
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'z': {'Value': 2, 'Action': 'PUT'}},
        Expected={'c': {'ComparisonOperator': 'IN', 'AttributeValueList': [2, 3, 8]}}
    )
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 2
    # false cases:
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'z': {'Value': 2, 'Action': 'PUT'}},
            Expected={'c': {'ComparisonOperator': 'IN', 'AttributeValueList': [1, 2, 4]}}
        )
    # a bunch of wrong interpretations of what the heck that "IN" does :-(
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'z': {'Value': 4, 'Action': 'PUT'}},
            Expected={'a': {'ComparisonOperator': 'IN', 'AttributeValueList': [2]}}
        )
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'z': {'Value': 3, 'Action': 'PUT'}},
            Expected={'a': {'ComparisonOperator': 'IN', 'AttributeValueList': [1, 2, 4, 7, 8]}}
        )
    # Strangely, all the items in AttributeValueList must be of the same type,
    # we can't check if an item is either the number 3 or the string 'dog',
    # although allowing this case as well would have been easy:
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'z': {'Value': 2, 'Action': 'PUT'}},
            Expected={'c': {'ComparisonOperator': 'IN', 'AttributeValueList': [3, 'dog']}}
        )
    # Empty list is not allowed
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'z': {'Value': 2, 'Action': 'PUT'}},
            Expected={'c': {'ComparisonOperator': 'IN', 'AttributeValueList': []}}
        )
    # Non-scalar attribute values are not allowed
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'z': {'Value': 5, 'Action': 'PUT'}},
            Expected={'c': {'ComparisonOperator': 'IN', 'AttributeValueList': [[1], [2]]}}
        )

# Tests for Expected with ComparisonOperator = "BETWEEN":
def test_update_expected_1_between(test_table_s):
    p = random_string()
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Value': 2, 'Action': 'PUT'},
                          'b': {'Value': 'cat', 'Action': 'PUT'},
                          'c': {'Value': bytearray('cat', 'utf-8'), 'Action': 'PUT'},
                          'd': {'Value': set([2, 4, 7]), 'Action': 'PUT'}})
    # true cases:
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'z': {'Value': 2, 'Action': 'PUT'}},
        Expected={'a': {'ComparisonOperator': 'BETWEEN', 'AttributeValueList': [1, 3]}}
    )
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 2
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'z': {'Value': 3, 'Action': 'PUT'}},
        Expected={'a': {'ComparisonOperator': 'BETWEEN', 'AttributeValueList': [1, 2]}}
    )
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 3
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'z': {'Value': 4, 'Action': 'PUT'}},
        Expected={'a': {'ComparisonOperator': 'BETWEEN', 'AttributeValueList': [2, 3]}}
    )
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 4
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'z': {'Value': 5, 'Action': 'PUT'}},
        Expected={'b': {'ComparisonOperator': 'BETWEEN', 'AttributeValueList': ['aardvark', 'dog']}}
    )
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 5
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'z': {'Value': 6, 'Action': 'PUT'}},
        Expected={'c': {'ComparisonOperator': 'BETWEEN', 'AttributeValueList': [bytearray('aardvark', 'utf-8'), bytearray('dog', 'utf-8')]}}
    )
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 6
    # false cases:
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'z': {'Value': 2, 'Action': 'PUT'}},
            Expected={'a': {'ComparisonOperator': 'BETWEEN', 'AttributeValueList': [0, 1]}}
        )
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'z': {'Value': 2, 'Action': 'PUT'}},
            Expected={'a': {'ComparisonOperator': 'BETWEEN', 'AttributeValueList': ['cat', 'dog']}}
        )
    with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'z': {'Value': 2, 'Action': 'PUT'}},
            Expected={'q': {'ComparisonOperator': 'BETWEEN', 'AttributeValueList': [0, 100]}})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 6
    # The given AttributeValueList array must contain exactly two items of the
    # same type, and in the right order. Any other input is considered a validation
    # error:
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'z': {'Value': 2, 'Action': 'PUT'}},
            Expected={'a': {'ComparisonOperator': 'BETWEEN', 'AttributeValueList': []}})
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'z': {'Value': 2, 'Action': 'PUT'}},
            Expected={'a': {'ComparisonOperator': 'BETWEEN', 'AttributeValueList': [2, 3, 4]}})
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'z': {'Value': 2, 'Action': 'PUT'}},
            Expected={'a': {'ComparisonOperator': 'BETWEEN', 'AttributeValueList': [4, 3]}})
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'z': {'Value': 2, 'Action': 'PUT'}},
            Expected={'b': {'ComparisonOperator': 'BETWEEN', 'AttributeValueList': ['dog', 'aardvark']}})
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'z': {'Value': 2, 'Action': 'PUT'}},
            Expected={'a': {'ComparisonOperator': 'BETWEEN', 'AttributeValueList': [4, 'dog']}})
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'z': {'Value': 2, 'Action': 'PUT'}},
            Expected={'d': {'ComparisonOperator': 'BETWEEN', 'AttributeValueList': [set([1]), set([2])]}})

##############################################################################
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
