# Copyright 2020 ScyllaDB
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

# Tests for the KeyConditionExpression parameter of the Query operation.
# KeyConditionExpression is a newer version of the older "KeyCondition"
# syntax. That older syntax is tested in test_query.py.

import pytest
from botocore.exceptions import ClientError, ParamValidationError
import random
from util import random_string, random_bytes, full_query, multiset

# test_table_sn_with_sorted_partition is just the regular test_table_sn, with
# a partition inserted with many items. The table, the partition key, and the
# items are returned - the items are sorted (by column 'c'), so they can be
# easily used to test various range queries. This fixture is useful for
# writing many small query tests which read the same input data without
# needing to re-insert data for every test, so overall the test suite is
# faster.
@pytest.fixture(scope="session")
def test_table_sn_with_sorted_partition(test_table_sn):
    p = random_string()
    items = [{'p': p, 'c': i, 'a': random_string()} for i in range(12)]
    with test_table_sn.batch_writer() as batch:
        for item in items:
            batch.put_item(item)
        # Add another partition just to make sure that a query of just
        # partition p can't just do a whole table scan and still succeed
        batch.put_item({'p': random_string(), 'c': 123, 'a': random_string()})
    yield test_table_sn, p, items

@pytest.fixture(scope="session")
def test_table_ss_with_sorted_partition(test_table):
    p = random_string()
    items = [{'p': p, 'c': str(i).zfill(3), 'a': random_string()} for i in range(12)]
    with test_table.batch_writer() as batch:
        for item in items:
            batch.put_item(item)
        batch.put_item({'p': random_string(), 'c': '123', 'a': random_string()})
    yield test_table, p, items

@pytest.fixture(scope="session")
def test_table_sb_with_sorted_partition(test_table_sb):
    p = random_string()
    items = [{'p': p, 'c': bytearray(str(i).zfill(3), 'ascii'), 'a': random_string()} for i in range(12)]
    with test_table_sb.batch_writer() as batch:
        for item in items:
            batch.put_item(item)
        batch.put_item({'p': random_string(), 'c': bytearray('123', 'ascii'), 'a': random_string()})
    yield test_table_sb, p, items


# A key condition with just a partition key, returning the entire partition
def test_key_condition_expression_partition(test_table_sn_with_sorted_partition):
    table, p, items = test_table_sn_with_sorted_partition
    got_items = full_query(table, KeyConditionExpression='p=:p', ExpressionAttributeValues={':p': p})
    # Both items and got_items are already sorted, so simple equality check is enough
    assert(got_items == items)

# As usual, whitespace in expression is ignored
def test_key_condition_expression_whitespace(test_table_sn_with_sorted_partition):
    table, p, items = test_table_sn_with_sorted_partition
    got_items = full_query(table, KeyConditionExpression=' p  =   :p ', ExpressionAttributeValues={':p': p})
    # Both items and got_items are already sorted, so simple equality check is enough
    assert(got_items == items)

# The KeyConditionExpression may only have one condition in each key column.
# A condition of "p=:p AND p=:p", while logically equivalent to just "p=:p",
# is not allowed.
def test_key_condition_expression_and_same(test_table_sn_with_sorted_partition):
    table, p, items = test_table_sn_with_sorted_partition
    with pytest.raises(ClientError, match='ValidationException.*one'):
        full_query(table, KeyConditionExpression='p=:p AND p=:p', ExpressionAttributeValues={':p': p})

# As usual, if we have an expression referring to a missing attribute value,
# we get an error:
def test_key_condition_expression_partition_missing_val(test_table_sn_with_sorted_partition):
    table, p, items = test_table_sn_with_sorted_partition
    with pytest.raises(ClientError, match='ValidationException.*:p'):
        full_query(table, KeyConditionExpression='p=:p')

# As usual, if we have an expression with unused values, we get an error:
def test_key_condition_expression_partition_unused_val(test_table_sn_with_sorted_partition):
    table, p, items = test_table_sn_with_sorted_partition
    with pytest.raises(ClientError, match='ValidationException.*qq'):
        full_query(table, KeyConditionExpression='p=:p', ExpressionAttributeValues={':p': p, ':qq': 3})

# The condition of the partition key must be an equality. Test that other
# operators such as "<" are not supported. They are not supported because
# partition keys are not sorted in the database, so there is no efficient
# way to implement such a query (and, anyway, it would be a Scan, not a
# Query).
def test_key_condition_expression_partition_eq_only(test_table_sn_with_sorted_partition):
    table, p, items = test_table_sn_with_sorted_partition
    with pytest.raises(ClientError, match='ValidationException.*supported'):
        full_query(table, KeyConditionExpression='p<:p', ExpressionAttributeValues={':p': p})

# The KeyConditionExpression must define the partition key p, it can't be
# just on the sort key c.
def test_key_condition_expression_no_partition(test_table_sn_with_sorted_partition):
    table, p, items = test_table_sn_with_sorted_partition
    with pytest.raises(ClientError, match='ValidationException.* p'):
        full_query(table, KeyConditionExpression='c=:c', ExpressionAttributeValues={':c': 3})

# The following tests test the various condition operators on the sort key,
# for a *numeric* sort key:

# Test the "=" operator on a numeric sort key:
def test_key_condition_expression_num_eq(test_table_sn_with_sorted_partition):
    table, p, items = test_table_sn_with_sorted_partition
    got_items = full_query(table, KeyConditionExpression='p=:p AND c=:c',
        ExpressionAttributeValues={':p': p, ':c': 3})
    expected_items = [item for item in items if item['c'] == 3]
    assert(got_items == expected_items)

# Test the "<" operator on a numeric sort key:
def test_key_condition_expression_num_lt(test_table_sn_with_sorted_partition):
    table, p, items = test_table_sn_with_sorted_partition
    got_items = full_query(table, KeyConditionExpression='p=:p AND c<:c',
        ExpressionAttributeValues={':p': p, ':c': 5})
    expected_items = [item for item in items if item['c'] < 5]
    assert(got_items == expected_items)

# The attribute name does not necessarily need to appear on the left side
# of the operator, it can also appear on the right side! Note how for
# "<", reversing the order obviously reverses the operator in the condition.
def test_key_condition_expression_num_eq_reverse(test_table_sn_with_sorted_partition):
    table, p, items = test_table_sn_with_sorted_partition
    got_items = full_query(table, KeyConditionExpression=':p=p AND :c=c',
        ExpressionAttributeValues={':p': p, ':c': 5})
    expected_items = [item for item in items if item['c'] == 5]
    assert(got_items == expected_items)

def test_key_condition_expression_num_lt_reverse(test_table_sn_with_sorted_partition):
    table, p, items = test_table_sn_with_sorted_partition
    got_items = full_query(table, KeyConditionExpression=':p=p AND :c<c',
        ExpressionAttributeValues={':p': p, ':c': 5})
    expected_items = [item for item in items if item['c'] > 5]
    assert(got_items == expected_items)

# Test the "<=" operator on a numeric sort key:
def test_key_condition_expression_num_le(test_table_sn_with_sorted_partition):
    table, p, items = test_table_sn_with_sorted_partition
    got_items = full_query(table, KeyConditionExpression='p=:p AND c<=:c',
        ExpressionAttributeValues={':p': p, ':c': 5})
    expected_items = [item for item in items if item['c'] <= 5]
    assert(got_items == expected_items)

# Test the ">" operator on a numeric sort key:
def test_key_condition_expression_num_gt(test_table_sn_with_sorted_partition):
    table, p, items = test_table_sn_with_sorted_partition
    got_items = full_query(table, KeyConditionExpression='p=:p AND c>:c',
        ExpressionAttributeValues={':p': p, ':c': 5})
    expected_items = [item for item in items if item['c'] > 5]
    assert(got_items == expected_items)

# Test the ">=" operator on a numeric sort key:
def test_key_condition_expression_num_ge(test_table_sn_with_sorted_partition):
    table, p, items = test_table_sn_with_sorted_partition
    got_items = full_query(table, KeyConditionExpression='p=:p AND c>=:c',
        ExpressionAttributeValues={':p': p, ':c': 5})
    expected_items = [item for item in items if item['c'] >= 5]
    assert(got_items == expected_items)

# Test the "BETWEEN/AND" ternary operator on a numeric sort key:
def test_key_condition_expression_num_between(test_table_sn_with_sorted_partition):
    table, p, items = test_table_sn_with_sorted_partition
    got_items = full_query(table, KeyConditionExpression='p=:p AND c BETWEEN :c1 AND :c2',
        ExpressionAttributeValues={':p': p, ':c1': 4, ':c2': 7})
    expected_items = [item for item in items if item['c'] >= 4 and item['c'] <= 7]
    assert(got_items == expected_items)

# Test that the BETWEEN and AND keywords (with its two different meanings!)
# are case-insensitive.
def test_key_condition_expression_case_insensitive(test_table_sn_with_sorted_partition):
    table, p, items = test_table_sn_with_sorted_partition
    got_items = full_query(table, KeyConditionExpression='p=:p AnD c BeTwEeN :c1 aNd :c2',
        ExpressionAttributeValues={':p': p, ':c1': 4, ':c2': 7})
    expected_items = [item for item in items if item['c'] >= 4 and item['c'] <= 7]
    assert(got_items == expected_items)

# The begins_with function does *not* work on a numeric sort key (it only
# works on strings or bytes - we'll check this later):
def test_key_condition_expression_num_begins(test_table_sn_with_sorted_partition):
    table, p, items = test_table_sn_with_sorted_partition
    with pytest.raises(ClientError, match='ValidationException.*begins_with'):
        full_query(table, KeyConditionExpression='p=:p AND begins_with(c, :c)',
            ExpressionAttributeValues={':p': p, ':c': 3})

# begins_with() is the only supported function in KeyConditionExpression.
# Unknown functions like dog() or functions supported in other expression
# types but not here (like attribute_exists()) result in errors.
def test_key_condition_expression_unknown(test_table_sn_with_sorted_partition):
    table, p, items = test_table_sn_with_sorted_partition
    with pytest.raises(ClientError, match='ValidationException.*dog'):
        full_query(table, KeyConditionExpression='p=:p AND dog(c, :c)',
            ExpressionAttributeValues={':p': p, ':c': 3})
    with pytest.raises(ClientError, match='ValidationException.*attribute_exists'):
        full_query(table, KeyConditionExpression='p=:p AND attribute_exists(c)',
            ExpressionAttributeValues={':p': p})

# As we already tested above for the partition key, it is not allowed to
# have multiple conditions on the same key column - here we test this for the
# sort key. This is why the separate BETWEEN operator exists...
def test_key_condition_expression_multi(test_table_sn_with_sorted_partition):
    table, p, items = test_table_sn_with_sorted_partition
    with pytest.raises(ClientError, match='ValidationException'):
        full_query(table, KeyConditionExpression='p=:p AND c>=:c1 AND c>=:c1',
            ExpressionAttributeValues={':p': p, ':c1': 3})
    # DynamoDB's produces a somewhat confusing error message for the
    # following. Before complaining on too many conditions on c, it ignores
    # the second - and then complains that the attribute value c2 isn't used.
    # But the details are less important, as long as we consider this an
    # error and use the word "syntax" in the error message, like Dynamo.
    with pytest.raises(ClientError, match='ValidationException.*yntax'):
        full_query(table, KeyConditionExpression='p=:p AND c>=:c1 AND c<=:c2',
            ExpressionAttributeValues={':p': p, ':c1': 3, 'c2': 7})

# Although the syntax for KeyConditionExpression is only a subset of that
# of ConditionExpression, it turns out that DynamoDB actually use the same
# parser for both. The implications of this for KeyConditionExpression
# parsing are interesting. In some cases, we simply see different error
# messages - e.g., unsupported operators (IN and <>) are not considered
# syntax errors but generate a slightly different error. The same thing
# is true for the OR and NOT keywords, which generate special errors.
# A slightly more interesting result of reusing the ConditionExpression
# parser is that parentheses, although they are quite useless for
# KeyConditionExpression (when there is always just one or two conditions
# ANDed together), *are* allowed in KeyConditionExpression.
def test_key_condition_expression_parser(test_table_sn_with_sorted_partition):
    table, p, items = test_table_sn_with_sorted_partition
    # The operators "<>" and "IN" are parsed, but not allowed:
    with pytest.raises(ClientError, match='ValidationException.*operator'):
        full_query(table, KeyConditionExpression='p=:p AND c<>:c',
            ExpressionAttributeValues={':p': p, ':c': 5})
    with pytest.raises(ClientError, match='ValidationException.*operator'):
        full_query(table, KeyConditionExpression='p=:p AND c IN (:c)',
            ExpressionAttributeValues={':p': p, ':c': 5})
    # The "OR" or "NOT" operators are parsed, but not allowed:
    with pytest.raises(ClientError, match='ValidationException.*OR'):
        full_query(table, KeyConditionExpression='c=:c OR p=:p',
            ExpressionAttributeValues={':p': p, ':c': 3})
    with pytest.raises(ClientError, match='ValidationException.*NOT'):
        full_query(table, KeyConditionExpression='NOT c=:c AND p=:p',
            ExpressionAttributeValues={':p': p, ':c': 3})
    # Unnecessary parentheses are allowed around the entire expression,
    # and on each primitive condition in it:
    got_items = full_query(table, KeyConditionExpression='((c=:c) AND (p=:p))',
        ExpressionAttributeValues={':p': p, ':c': 3})
    expected_items = [item for item in items if item['c'] == 3]
    assert(got_items == expected_items)
    # Strangely, although one pair of unnecesary parentheses are allowed
    # in each level, DynamoDB forbids more than one - it refuses to accept
    # the expression ((c=:c) AND ((p=:p))) with one too many redundant levels
    # of parentheses. However, we chose not to implement this extra check
    # in Alternator, so the following test is commented out:
    #with pytest.raises(ClientError, match='ValidationException'):
    #    full_query(table, KeyConditionExpression='((c=:c) AND ((p=:p)))',
    #        ExpressionAttributeValues={':p': p, ':c': 3})

# A simple case of syntax error - unknown operator "!=".
def test_key_condition_expression_syntax(test_table_sn_with_sorted_partition):
    table, p, items = test_table_sn_with_sorted_partition
    with pytest.raises(ClientError, match='ValidationException.*yntax'):
        full_query(table, KeyConditionExpression='p=:p AND c!=:c',
            ExpressionAttributeValues={':p': p, ':c': 3})

# Unsurprisingly, an empty KeyConditionExpression is an error
def test_key_condition_expression_empty(test_table_sn_with_sorted_partition):
    table, p, items = test_table_sn_with_sorted_partition
    with pytest.raises(ClientError, match='ValidationException.*empty'):
        full_query(table, KeyConditionExpression='')

# It is not allowed to use a non-key column in KeyConditionExpression
def test_key_condition_expression_non_key(test_table_sn_with_sorted_partition):
    table, p, items = test_table_sn_with_sorted_partition
    # The specific errors returned by DynamoDB in this case are somewhat
    # peculiar: If we have three conditions - on both key columns and on
    # a non-key columns - the error is that only two are allowed.
    # But if we just have two conditions, the error says that it expects
    # the second condition to be on the schema's sort key.
    # Alternator's parser doesn't need to give exactly these peculiar
    # errors, though.
    with pytest.raises(ClientError, match='ValidationException'):
        full_query(table, KeyConditionExpression='p=:p AND c<:c AND a<:a',
            ExpressionAttributeValues={':p': p, ':c': 3, ':a': 3})
    with pytest.raises(ClientError, match='ValidationException.*key'):
        full_query(table, KeyConditionExpression='p=:p AND a<:a',
            ExpressionAttributeValues={':p': p, ':a': 3})
    # DynamoDB's parser also, as usual, understands nested attribute names
    # in expressions, but doesn't allow them in condition expressions.
    with pytest.raises(ClientError, match='ValidationException.*nested'):
        full_query(table, KeyConditionExpression='p=:p AND a.b<:a',
            ExpressionAttributeValues={':p': p, ':a': 3})

# A condition needs to compare a column to a constant - we can't compare
# two columns, nor two constants
def test_key_condition_expression_bad_compare(test_table_sn_with_sorted_partition):
    table, p, items = test_table_sn_with_sorted_partition
    with pytest.raises(ClientError, match='ValidationException'):
        full_query(table, KeyConditionExpression='p=c')
    with pytest.raises(ClientError, match='ValidationException'):
        full_query(table, KeyConditionExpression='p=a')
    with pytest.raises(ClientError, match='ValidationException'):
        full_query(table, KeyConditionExpression=':p=:a',
            ExpressionAttributeValues={':p': p, ':a': 3})

# Nor can a "condition" be just a single key or constant:
def test_key_condition_expression_bad_value(test_table_sn_with_sorted_partition):
    table, p, items = test_table_sn_with_sorted_partition
    with pytest.raises(ClientError, match='ValidationException'):
        full_query(table, KeyConditionExpression='p')
    with pytest.raises(ClientError, match='ValidationException'):
        full_query(table, KeyConditionExpression=':a',
            ExpressionAttributeValues={':a': 3})

# In all tests above the condition had p first, s second. Verify that this
# order can be reversed.
def test_key_condition_expression_order(test_table_sn_with_sorted_partition):
    table, p, items = test_table_sn_with_sorted_partition
    got_items = full_query(table, KeyConditionExpression='c=:c AND p=:p',
        ExpressionAttributeValues={':p': p, ':c': 3})
    expected_items = [item for item in items if item['c'] == 3]
    assert(got_items == expected_items)

# All tests above had the attribute names written explicitly in the expression.
# Try the same with attribute name references (#something):
def test_key_condition_expression_name_ref(test_table_sn_with_sorted_partition):
    table, p, items = test_table_sn_with_sorted_partition
    got_items = full_query(table, KeyConditionExpression='#name1=:p AND #name2=:c',
        ExpressionAttributeValues={':p': p, ':c': 3},
        ExpressionAttributeNames={'#name1': 'p', '#name2': 'c'})
    expected_items = [item for item in items if item['c'] == 3]
    assert(got_items == expected_items)

# Missing or unused attribute name references cause errors:
def test_key_condition_expression_name_ref_error(test_table_sn_with_sorted_partition):
    table, p, items = test_table_sn_with_sorted_partition
    with pytest.raises(ClientError, match='ValidationException.*name2'):
        full_query(table, KeyConditionExpression='#name1=:p AND #name2=:c',
            ExpressionAttributeValues={':p': p, ':c': 3},
            ExpressionAttributeNames={'#name1': 'p'})
    with pytest.raises(ClientError, match='ValidationException.*name2'):
        full_query(table, KeyConditionExpression='#name1=:p',
            ExpressionAttributeValues={':p': p},
            ExpressionAttributeNames={'#name1': 'p', '#name2': 'c'})

# The condition knows the key attributes' types from the schema, and should
# refuse to compare one to a value with the wrong type, resulting in a
# ValidationException
def test_key_condition_expression_wrong_type(test_table_sn_with_sorted_partition):
    table, p, items = test_table_sn_with_sorted_partition
    with pytest.raises(ClientError, match='ValidationException.*type'):
        full_query(table, KeyConditionExpression='p=:num',
            ExpressionAttributeValues={':num': 3})
    with pytest.raises(ClientError, match='ValidationException.*type'):
        full_query(table, KeyConditionExpression='p=:p AND c=:str',
            ExpressionAttributeValues={':p': p, ':str': 'hello'})

# The following tests test the various condition operators on the sort key,
# for a *string* sort key:

# Test the "=" operator on a string sort key:
def test_key_condition_expression_str_eq(test_table_ss_with_sorted_partition):
    table, p, items = test_table_ss_with_sorted_partition
    got_items = full_query(table, KeyConditionExpression='p=:p AND c=:c',
        ExpressionAttributeValues={':p': p, ':c': '003'})
    expected_items = [item for item in items if item['c'] == '003']
    assert(got_items == expected_items)

# Test the "<" operator on a string sort key:
def test_key_condition_expression_str_lt(test_table_ss_with_sorted_partition):
    table, p, items = test_table_ss_with_sorted_partition
    got_items = full_query(table, KeyConditionExpression='p=:p AND c<:c',
        ExpressionAttributeValues={':p': p, ':c': '005'})
    expected_items = [item for item in items if item['c'] < '005']
    assert(got_items == expected_items)

# Test the "<=" operator on a string sort key:
def test_key_condition_expression_str_le(test_table_ss_with_sorted_partition):
    table, p, items = test_table_ss_with_sorted_partition
    got_items = full_query(table, KeyConditionExpression='p=:p AND c<=:c',
        ExpressionAttributeValues={':p': p, ':c': '005'})
    expected_items = [item for item in items if item['c'] <= '005']
    assert(got_items == expected_items)

# Test the ">" operator on a string sort key:
def test_key_condition_expression_str_gt(test_table_ss_with_sorted_partition):
    table, p, items = test_table_ss_with_sorted_partition
    got_items = full_query(table, KeyConditionExpression='p=:p AND c>:c',
        ExpressionAttributeValues={':p': p, ':c': '005'})
    expected_items = [item for item in items if item['c'] > '005']
    assert(got_items == expected_items)

# Test the ">=" operator on a string sort key:
def test_key_condition_expression_str_ge(test_table_ss_with_sorted_partition):
    table, p, items = test_table_ss_with_sorted_partition
    got_items = full_query(table, KeyConditionExpression='p=:p AND c>=:c',
        ExpressionAttributeValues={':p': p, ':c': '005'})
    expected_items = [item for item in items if item['c'] >= '005']
    assert(got_items == expected_items)

# Test the "BETWEEN/AND" ternary operator on a string sort key:
def test_key_condition_expression_str_between(test_table_ss_with_sorted_partition):
    table, p, items = test_table_ss_with_sorted_partition
    got_items = full_query(table, KeyConditionExpression='p=:p AND c BETWEEN :c1 AND :c2',
        ExpressionAttributeValues={':p': p, ':c1': '004', ':c2': '007'})
    expected_items = [item for item in items if item['c'] >= '004' and item['c'] <= '007']
    assert(got_items == expected_items)

# Test the begins_with function on a string sort key:
def test_key_condition_expression_str_begins(test_table_ss_with_sorted_partition):
    table, p, items = test_table_ss_with_sorted_partition
    got_items = full_query(table, KeyConditionExpression='p=:p AND begins_with(c,:c)',
        ExpressionAttributeValues={':p': p, ':c': '00'})
    expected_items = [item for item in items if item['c'].startswith('00')]
    assert(got_items == expected_items)

# The function name begins_with is case-sensitive - it doesn't work with
# other capitalization. Note that above we already tested the general case
# of an unknown function (e.g., dog) - this is really just another example.
def test_key_condition_expression_str_begins_case(test_table_ss_with_sorted_partition):
    table, p, items = test_table_ss_with_sorted_partition
    with pytest.raises(ClientError, match='ValidationException.*BEGINS_WITH'):
        full_query(table, KeyConditionExpression='p=:p AND BEGINS_WITH(c,:c)',
            ExpressionAttributeValues={':p': p, ':c': '00'})

# The following tests test the various condition operators on the sort key,
# for a *bytes* sort key:

# Test the "=" operator on a bytes sort key:
def test_key_condition_expression_bytes_eq(test_table_sb_with_sorted_partition):
    table, p, items = test_table_sb_with_sorted_partition
    got_items = full_query(table, KeyConditionExpression='p=:p AND c=:c',
        ExpressionAttributeValues={':p': p, ':c': bytearray('003', 'ascii')})
    expected_items = [item for item in items if item['c'] == bytearray('003', 'ascii')]
    assert(got_items == expected_items)

# Test the "<" operator on a bytes sort key:
def test_key_condition_expression_bytes_lt(test_table_sb_with_sorted_partition):
    table, p, items = test_table_sb_with_sorted_partition
    got_items = full_query(table, KeyConditionExpression='p=:p AND c<:c',
        ExpressionAttributeValues={':p': p, ':c': bytearray('005', 'ascii')})
    expected_items = [item for item in items if item['c'] < bytearray('005', 'ascii')]
    assert(got_items == expected_items)

# Test the "<=" operator on a bytes sort key:
def test_key_condition_expression_bytes_le(test_table_sb_with_sorted_partition):
    table, p, items = test_table_sb_with_sorted_partition
    got_items = full_query(table, KeyConditionExpression='p=:p AND c<=:c',
        ExpressionAttributeValues={':p': p, ':c': bytearray('005', 'ascii')})
    expected_items = [item for item in items if item['c'] <= bytearray('005', 'ascii')]
    assert(got_items == expected_items)

# Test the ">" operator on a bytes sort key:
def test_key_condition_expression_bytes_gt(test_table_sb_with_sorted_partition):
    table, p, items = test_table_sb_with_sorted_partition
    got_items = full_query(table, KeyConditionExpression='p=:p AND c>:c',
        ExpressionAttributeValues={':p': p, ':c': bytearray('005', 'ascii')})
    expected_items = [item for item in items if item['c'] > bytearray('005', 'ascii')]
    assert(got_items == expected_items)

# Test the ">=" operator on a string sort key:
def test_key_condition_expression_bytes_ge(test_table_sb_with_sorted_partition):
    table, p, items = test_table_sb_with_sorted_partition
    got_items = full_query(table, KeyConditionExpression='p=:p AND c>=:c',
        ExpressionAttributeValues={':p': p, ':c': bytearray('005', 'ascii')})
    expected_items = [item for item in items if item['c'] >= bytearray('005', 'ascii')]
    assert(got_items == expected_items)

# Test the "BETWEEN/AND" ternary operator on a bytes sort key:
def test_key_condition_expression_bytes_between(test_table_sb_with_sorted_partition):
    table, p, items = test_table_sb_with_sorted_partition
    got_items = full_query(table, KeyConditionExpression='p=:p AND c BETWEEN :c1 AND :c2',
        ExpressionAttributeValues={':p': p, ':c1': bytearray('004', 'ascii'), ':c2': bytearray('007', 'ascii')})
    expected_items = [item for item in items if item['c'] >= bytearray('004', 'ascii') and item['c'] <= bytearray('007', 'ascii')]
    assert(got_items == expected_items)

# Test the begins_with function on a bytes sort key:
def test_key_condition_expression_bytes_begins(test_table_sb_with_sorted_partition):
    table, p, items = test_table_sb_with_sorted_partition
    got_items = full_query(table, KeyConditionExpression='p=:p AND begins_with(c,:c)',
        ExpressionAttributeValues={':p': p, ':c': bytearray('00', 'ascii')})
    expected_items = [item for item in items if item['c'].startswith(bytearray('00', 'ascii'))]
    assert(got_items == expected_items)

# Query and KeyConditionExpress works also on a table with just a partition
# key and no sort key, although obviously it isn't very useful (for such
# tables, Query is just an elaborate way to do a GetItem).
# would have been more efficient):
def test_key_condition_expression_hash_only(test_table_s):
    p = random_string()
    item = {'p': p, 'val': 'hello'}
    test_table_s.put_item(Item=item)
    got_items = full_query(test_table_s, KeyConditionExpression='p=:p', ExpressionAttributeValues={':p': p})
    assert(got_items == [item])

# A Query cannot specify both KeyConditions and KeyConditionExpression
def test_key_condition_expression_and_conditions(test_table_sn_with_sorted_partition):
    table, p, items = test_table_sn_with_sorted_partition
    with pytest.raises(ClientError, match='ValidationException.*both'):
        full_query(table,
            KeyConditionExpression='p=:p',
            ExpressionAttributeValues={':p': p},
            KeyConditions={'c' : {'AttributeValueList': [3],
                'ComparisonOperator': 'GT'}}
            )

# The following is an older test we had, which test one arbitrary use case
# for KeyConditionExpression. It uses filled_test_table (the one we also
# use in test_scan.py) instead of the fixtures defined in this file.
# The only interesting thing about this test is that instead of creating
# the KeyConditionExpression string, it uses boto3's "Key" condition builder.
# That shouldn't make any difference, however - that builder also builds a
# string.
from boto3.dynamodb.conditions import Key
def test_query_key_condition_expression(dynamodb, filled_test_table):
    test_table, items = filled_test_table
    paginator = dynamodb.meta.client.get_paginator('query')
    got_items = []
    for page in paginator.paginate(TableName=test_table.name, KeyConditionExpression=Key("p").eq("long") & Key("c").lt("12")):
        got_items += page['Items']
    assert multiset([item for item in items if item['p'] == 'long' and item['c'] < '12']) == multiset(got_items)
