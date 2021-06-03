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

# Tests for the UpdateItem operations with an UpdateExpression parameter

import random
import string
import pytest
from botocore.exceptions import ClientError
from util import random_string

# The simplest test of using UpdateExpression to set a top-level attribute,
# instead of the older AttributeUpdates parameter.
# Checks only one "SET" action in an UpdateExpression.
def test_update_expression_set(test_table_s):
    p = random_string()
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET b = :val1',
        ExpressionAttributeValues={':val1': 4})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'b': 4}

# An empty UpdateExpression is NOT allowed, and generates a "The expression
# can not be empty" error. This contrasts with an empty AttributeUpdates which
# is allowed, and results in the creation of an empty item if it didn't exist
# yet (see test_empty_update()).
def test_update_expression_empty(test_table_s):
    p = random_string()
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p}, UpdateExpression='')

# A basic test with multiple SET actions in one expression
def test_update_expression_set_multi(test_table_s):
    p = random_string()
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET x = :val1, y = :val1',
        ExpressionAttributeValues={':val1': 4})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'x': 4, 'y': 4}

# SET can be used to copy an existing attribute to a new one
def test_update_expression_set_copy(test_table_s):
    p = random_string()
    test_table_s.put_item(Item={'p': p, 'a': 'hello'})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': 'hello'}
    test_table_s.update_item(Key={'p': p}, UpdateExpression='SET b = a')
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': 'hello', 'b': 'hello'}
    # Copying an non-existing attribute generates an error
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p}, UpdateExpression='SET c = z')
    # It turns out that attributes to be copied are read before the SET
    # starts to write, so "SET x = :val1, y = x" does not work...
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p}, UpdateExpression='SET x = :val1, y = x', ExpressionAttributeValues={':val1': 4})
    # SET z=z does nothing if z exists, or fails if it doesn't
    test_table_s.update_item(Key={'p': p}, UpdateExpression='SET a = a')
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': 'hello', 'b': 'hello'}
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p}, UpdateExpression='SET z = z')
    # We can also use name references in either LHS or RHS of SET, e.g.,
    # SET #one = #two. We need to also take the references used in the RHS
    # when we want to complain about unused names in ExpressionAttributeNames.
    test_table_s.update_item(Key={'p': p}, UpdateExpression='SET #one = #two',
         ExpressionAttributeNames={'#one': 'c', '#two': 'a'})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': 'hello', 'b': 'hello', 'c': 'hello'}
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p}, UpdateExpression='SET #one = #two',
             ExpressionAttributeNames={'#one': 'c', '#two': 'a', '#three': 'z'})

# Test for read-before-write action where the value to be read is nested inside a - operator
def test_update_expression_set_nested_copy(test_table_s):
    p = random_string()
    test_table_s.update_item(Key={'p': p}, UpdateExpression='SET #n = :two',
         ExpressionAttributeNames={'#n': 'n'}, ExpressionAttributeValues={':two': 2})
    test_table_s.update_item(Key={'p': p}, UpdateExpression='SET #nn = :seven - #n',
         ExpressionAttributeNames={'#nn': 'nn', '#n': 'n'}, ExpressionAttributeValues={':seven': 7})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'n': 2, 'nn': 5}

    test_table_s.update_item(Key={'p': p}, UpdateExpression='SET #nnn = :nnn',
         ExpressionAttributeNames={'#nnn': 'nnn'}, ExpressionAttributeValues={':nnn': [2,4]})
    test_table_s.update_item(Key={'p': p}, UpdateExpression='SET #nnnn = list_append(:val1, #nnn)',
         ExpressionAttributeNames={'#nnnn': 'nnnn', '#nnn': 'nnn'}, ExpressionAttributeValues={':val1': [1,3]})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'n': 2, 'nn': 5, 'nnn': [2,4], 'nnnn': [1,3,2,4]}

# Test for getting a key value with read-before-write
def test_update_expression_set_key(test_table_sn):
    p = random_string()
    test_table_sn.update_item(Key={'p': p, 'c': 7});
    test_table_sn.update_item(Key={'p': p, 'c': 7}, UpdateExpression='SET #n = #p',
         ExpressionAttributeNames={'#n': 'n', '#p': 'p'})
    test_table_sn.update_item(Key={'p': p, 'c': 7}, UpdateExpression='SET #nn = #c + #c',
         ExpressionAttributeNames={'#nn': 'nn', '#c': 'c'})
    assert test_table_sn.get_item(Key={'p': p, 'c': 7}, ConsistentRead=True)['Item'] == {'p': p, 'c': 7, 'n': p, 'nn': 14}

# Simple test for the "REMOVE" action
def test_update_expression_remove(test_table_s):
    p = random_string()
    test_table_s.put_item(Item={'p': p, 'a': 'hello', 'b': 'hi'})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': 'hello', 'b': 'hi'}
    test_table_s.update_item(Key={'p': p}, UpdateExpression='REMOVE a')
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'b': 'hi'}

# Demonstrate that although all DynamoDB examples give UpdateExpression
# action names in uppercase - e.g., "SET", it can actually be any case.
def test_update_expression_action_case(test_table_s):
    p = random_string()
    test_table_s.update_item(Key={'p': p}, UpdateExpression='SET b = :val1', ExpressionAttributeValues={':val1': 3})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'b': 3}
    test_table_s.update_item(Key={'p': p}, UpdateExpression='set b = :val1', ExpressionAttributeValues={':val1': 4})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'b': 4}
    test_table_s.update_item(Key={'p': p}, UpdateExpression='sEt b = :val1', ExpressionAttributeValues={':val1': 5})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'b': 5}

# Demonstrate that whitespace is ignored in UpdateExpression parsing.
def test_update_expression_action_whitespace(test_table_s):
    p = random_string()
    test_table_s.update_item(Key={'p': p}, UpdateExpression='set b = :val1', ExpressionAttributeValues={':val1': 4})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'b': 4}
    test_table_s.update_item(Key={'p': p}, UpdateExpression='  set   b=:val1  ', ExpressionAttributeValues={':val1': 5})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'b': 5}

# In UpdateExpression, the attribute name can appear directly in the expression
# (without a "#placeholder" notation) only if it is a single "token" as
# determined by DynamoDB's lexical analyzer rules: Such token is composed of
# alphanumeric characters whose first character must be alphabetic. Other
# names cause the parser to see multiple tokens, and produce syntax errors.
def test_update_expression_name_token(test_table_s):
    p = random_string()
    # Alphanumeric names starting with an alphabetical character work
    test_table_s.update_item(Key={'p': p}, UpdateExpression='SET alnum = :val1', ExpressionAttributeValues={':val1': 1})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['alnum'] == 1
    test_table_s.update_item(Key={'p': p}, UpdateExpression='SET Alpha_Numeric_123 = :val1', ExpressionAttributeValues={':val1': 2})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['Alpha_Numeric_123'] == 2
    test_table_s.update_item(Key={'p': p}, UpdateExpression='SET A123_ = :val1', ExpressionAttributeValues={':val1': 3})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['A123_'] == 3
    # But alphanumeric names cannot start with underscore or digits.
    # DynamoDB's lexical analyzer doesn't recognize them, and produces
    # a ValidationException looking like:
    #   Invalid UpdateExpression: Syntax error; token: "_", near: "SET _123"
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p}, UpdateExpression='SET _123 = :val1', ExpressionAttributeValues={':val1': 3})
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p}, UpdateExpression='SET _abc = :val1', ExpressionAttributeValues={':val1': 3})
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p}, UpdateExpression='SET 123a = :val1', ExpressionAttributeValues={':val1': 3})
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p}, UpdateExpression='SET 123 = :val1', ExpressionAttributeValues={':val1': 3})
    # Various other non-alpha-numeric characters, split a token and NOT allowed
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p}, UpdateExpression='SET hi-there = :val1', ExpressionAttributeValues={':val1': 3})
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p}, UpdateExpression='SET hi$there = :val1', ExpressionAttributeValues={':val1': 3})
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p}, UpdateExpression='SET "hithere" = :val1', ExpressionAttributeValues={':val1': 3})
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p}, UpdateExpression='SET !hithere = :val1', ExpressionAttributeValues={':val1': 3})

    # In addition to the literal names, DynamoDB also allows references to any
    # name, using the "#reference" syntax. It turns out the reference name is
    # also a token following the rules as above, with one interesting point:
    # since "#" already started the token, the next character may be any
    # alphanumeric and doesn't need to be only alphabetical.
    # Note that the reference target - the actual attribute name - can include
    # absolutely any characters, and we use silly_name below as an example
    silly_name = '3can include any character!.#='
    test_table_s.update_item(Key={'p': p}, UpdateExpression='SET #Alpha_Numeric_123 = :val1', ExpressionAttributeValues={':val1': 4}, ExpressionAttributeNames={'#Alpha_Numeric_123': silly_name})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'][silly_name] == 4
    test_table_s.update_item(Key={'p': p}, UpdateExpression='SET #123a = :val1', ExpressionAttributeValues={':val1': 5}, ExpressionAttributeNames={'#123a': silly_name})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'][silly_name] == 5
    test_table_s.update_item(Key={'p': p}, UpdateExpression='SET #123 = :val1', ExpressionAttributeValues={':val1': 6}, ExpressionAttributeNames={'#123': silly_name})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'][silly_name] == 6
    test_table_s.update_item(Key={'p': p}, UpdateExpression='SET #_ = :val1', ExpressionAttributeValues={':val1': 7}, ExpressionAttributeNames={'#_': silly_name})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'][silly_name] == 7
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p}, UpdateExpression='SET #hi-there = :val1', ExpressionAttributeValues={':val1': 7}, ExpressionAttributeNames={'#hi-there': silly_name})
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p}, UpdateExpression='SET #!hi = :val1', ExpressionAttributeValues={':val1': 7}, ExpressionAttributeNames={'#!hi': silly_name})
    # Just a "#" is not enough as a token. Interestingly, DynamoDB will
    # find the bad name in ExpressionAttributeNames before it actually tries
    # to parse UpdateExpression, but we can verify the parse fails too by
    # using a valid but irrelevant name in ExpressionAttributeNames:
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p}, UpdateExpression='SET # = :val1', ExpressionAttributeValues={':val1': 7}, ExpressionAttributeNames={'#': silly_name})
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p}, UpdateExpression='SET # = :val1', ExpressionAttributeValues={':val1': 7}, ExpressionAttributeNames={'#a': silly_name})

    # There is also the value references, ":reference", for the right-hand
    # side of an assignment. These have similar naming rules like "#reference".
    test_table_s.update_item(Key={'p': p}, UpdateExpression='SET a = :Alpha_Numeric_123', ExpressionAttributeValues={':Alpha_Numeric_123': 8})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['a'] == 8
    test_table_s.update_item(Key={'p': p}, UpdateExpression='SET a = :123a', ExpressionAttributeValues={':123a': 9})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['a'] == 9
    test_table_s.update_item(Key={'p': p}, UpdateExpression='SET a = :123', ExpressionAttributeValues={':123': 10})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['a'] == 10
    test_table_s.update_item(Key={'p': p}, UpdateExpression='SET a = :_', ExpressionAttributeValues={':_': 11})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['a'] == 11
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p}, UpdateExpression='SET a = :hi!there', ExpressionAttributeValues={':hi!there': 12})
    # Just a ":" is not enough as a token.
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p}, UpdateExpression='SET a = :', ExpressionAttributeValues={':': 7})
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p}, UpdateExpression='SET a = :', ExpressionAttributeValues={':a': 7})
    # Trying to use a :reference on the left-hand side of an assignment will
    # not work. In DynamoDB, it's a different type of token (and generates
    # syntax error).
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p}, UpdateExpression='SET :a = :b', ExpressionAttributeValues={':a': 1, ':b': 2})

# Multiple actions are allowed in one expression, but actions are divided
# into clauses (SET, REMOVE, DELETE, ADD) and each of those can only appear
# once.
def test_update_expression_multi(test_table_s):
    p = random_string()
    # We can have two SET actions in one SET clause:
    test_table_s.update_item(Key={'p': p}, UpdateExpression='SET a = :val1, b = :val2', ExpressionAttributeValues={':val1': 1, ':val2': 2})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': 1, 'b': 2}
    # But not two SET clauses - we get error "The "SET" section can only be used once in an update expression"
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p}, UpdateExpression='SET a = :val1 SET b = :val2', ExpressionAttributeValues={':val1': 1, ':val2': 2})
    # We can have a REMOVE and a SET clause (note no comma between clauses):
    test_table_s.update_item(Key={'p': p}, UpdateExpression='REMOVE a SET b = :val2', ExpressionAttributeValues={':val2': 3})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'b': 3}
    test_table_s.update_item(Key={'p': p}, UpdateExpression='SET c = :val2 REMOVE b', ExpressionAttributeValues={':val2': 3})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'c': 3}
    # The same clause (e.g., SET) cannot be used twice, even if interleaved with something else
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p}, UpdateExpression='SET a = :val1 REMOVE a SET b = :val2', ExpressionAttributeValues={':val1': 1, ':val2': 2})

# Trying to modify the same item twice in the same update is forbidden.
# For "SET a=:v REMOVE a" DynamoDB says: "Invalid UpdateExpression: Two
# document paths overlap with each other; must remove or rewrite one of
# these paths; path one: [a], path two: [a]". 
# It is actually good for Scylla that such updates are forbidden, because had
# we allowed "SET a=:v REMOVE a" the result would be surprising - because data
# wins over a delete with the same timestamp, so "a" would be set despite the
# REMOVE command appearing later in the command line.
def test_update_expression_multi_overlap(test_table_s):
    p = random_string()
    test_table_s.put_item(Item={'p': p, 'a': 'hello'})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': 'hello'}
    # Neither "REMOVE a SET a = :v" nor "SET a = :v REMOVE a" are allowed:
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p}, UpdateExpression='REMOVE a SET a = :v', ExpressionAttributeValues={':v': 'hi'})
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p}, UpdateExpression='SET a = :v REMOVE a', ExpressionAttributeValues={':v': 'yo'})
    # It's also not allowed to set a twice in the same clause
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p}, UpdateExpression='SET a = :v1, a = :v2', ExpressionAttributeValues={':v1': 'yo', ':v2': 'he'})
    # Obviously, the paths are compared after the name references are evaluated
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p}, UpdateExpression='SET #a1 = :v1, #a2 = :v2', ExpressionAttributeValues={':v1': 'yo', ':v2': 'he'}, ExpressionAttributeNames={'#a1': 'a', '#a2': 'a'})

# The problem isn't just with identical paths - we can't modify two paths that
# "overlap" in the sense that one is the ancestor of the other.
def test_update_expression_multi_overlap_nested(test_table_s):
    p = random_string()
    # Note that the overlap checks happen before checking the actual content
    # of the item, so it doesn't matter that the item we want to modify
    # doesn't even exist or have the structure referenced by the paths below.
    for expr in ['SET a = :val1, a.b = :val2',
                 'SET a.b = :val1, a = :val2',
                 'SET a.b = :val1, a.b = :val2',
                 'SET a.b = :val1, a.b.c = :val2',
                 'SET a.b.c = :val1, a.b = :val2',
                 'SET a.b.c = :val1, a.b.c = :val2',
                 'SET a.b = :val1, a.b.c.d.e = :val2',
                 'SET a.b.c.d.e = :val1, a.b = :val2',
                 'SET a = :val1, a[1] = :val2',
                 'SET a[1] = :val1, a = :val2',
                 'SET a[1] = :val1, a[1] = :val2',
                 'SET a[1][1] = :val1, a[1] = :val2',
                 'SET a[1] = :val1, a[1][1] = :val2',
                 'SET a[1][1] = :val1, a[1][1] = :val2',
                 'SET a[1][1][1][1] = :val1, a[1][1] = :val2',
                 'SET a[1][1] = :val1, a[1][1][1][1] = :val2',
                 'SET a[1][1][1][1] = :val1, a[1][1][1][1] = :val2',
                ]:
        print(expr)
        with pytest.raises(ClientError, match='ValidationException.*overlap'):
            test_table_s.update_item(Key={'p': p}, UpdateExpression=expr,
                ExpressionAttributeValues={':val1': 2, ':val2': 'there'})
    # Obviously this test can trivially pass if overlap checks wrongly labels
    # everything as an overlap. So the test test_update_expression_multi_nested
    # below is important - it confirms that we can do multiple modifications
    # to the same item when they do not overlap.

# Besides the concept of "overlapping" paths tested above, DynamoDB also has
# the concept of "conflicting" paths - e.g., attempting to set both a.b and
# a[1] together doesn't make sense.
def test_update_expression_multi_conflict_nested(test_table_s):
    p = random_string()
    for expr in ['SET a.b = :val1, a[1] = :val2',
                 'SET a.b.c = :val1, a.b[2] = :val2',
                ]:
        print(expr)
        with pytest.raises(ClientError, match='ValidationException.*conflict'):
            test_table_s.update_item(Key={'p': p}, UpdateExpression=expr,
                ExpressionAttributeValues={':val1': 2, ':val2': 'there'})

# We can do several non-overlapping modifications to the same top-level
# attribute and to different top-level attributes in the same update
# expression.
def test_update_expression_multi_nested(test_table_s):
    p = random_string()
    test_table_s.put_item(Item={'p': p, 'a': {'x': 3, 'y': 4, 'c': {'y': 3}}, 'b': {'x': 1, 'y': 2}})
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET a.b = :val1, a.c.d = :val2, b.x = :val3 REMOVE a.x, b.y',
        ExpressionAttributeValues={':val1': 10, ':val2': 'dog', ':val3': 17})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] ==  {
        'p': p,
        'a': {'y': 4, 'b': 10, 'c': {'y': 3, 'd': 'dog'}},
        'b': {'x': 17}}

# In the previous test we saw that *modifying* the same item twice in the same
# update is forbidden; But it is allowed to *read* an item in the same update
# that also modifies it, and we check this here.
def test_update_expression_multi_with_copy(test_table_s):
    p = random_string()
    test_table_s.put_item(Item={'p': p, 'a': 'hello'})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': 'hello'}
    # "REMOVE a SET b = a" works: as noted in test_update_expression_set_copy()
    # the value of 'a' is read before the actual REMOVE operation happens.
    test_table_s.update_item(Key={'p': p}, UpdateExpression='REMOVE a SET b = a')
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'b': 'hello'}
    test_table_s.update_item(Key={'p': p}, UpdateExpression='SET c = b REMOVE b')
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'c': 'hello'}


# Test case where a :val1 is referenced, without being defined
def test_update_expression_set_missing_value(test_table_s):
    p = random_string()
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET b = :val1',
            ExpressionAttributeValues={':val2': 4})
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET b = :val1')

# It is forbidden for ExpressionAttributeValues to contain values not used
# by the expression. DynamoDB produces an error like: "Value provided in
# ExpressionAttributeValues unused in expressions: keys: {:val1}"
def test_update_expression_spurious_value(test_table_s):
    p = random_string()
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p}, UpdateExpression='SET a = :val1',
            ExpressionAttributeValues={':val1': 3, ':val2': 4})

# Test case where a #name is referenced, without being defined
def test_update_expression_set_missing_name(test_table_s):
    p = random_string()
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET #name = :val1',
            ExpressionAttributeValues={':val2': 4},
            ExpressionAttributeNames={'#wrongname': 'hello'})
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET #name = :val1',
            ExpressionAttributeValues={':val2': 4})

# It is forbidden for ExpressionAttributeNames to contain names not used
# by the expression. DynamoDB produces an error like: "Value provided in
# ExpressionAttributeNames unused in expressions: keys: {#b}"
def test_update_expression_spurious_name(test_table_s):
    p = random_string()
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p}, UpdateExpression='SET #a = :val1',
            ExpressionAttributeNames={'#a': 'hello', '#b': 'hi'},
            ExpressionAttributeValues={':val1': 3, ':val2': 4})

# Test that the key attributes (hash key or sort key) cannot be modified
# by an update
def test_update_expression_cannot_modify_key(test_table):
    p = random_string()
    c = random_string()
    with pytest.raises(ClientError, match='ValidationException.*key'):
        test_table.update_item(Key={'p': p, 'c': c},
            UpdateExpression='SET p = :val1', ExpressionAttributeValues={':val1': 4})
    with pytest.raises(ClientError, match='ValidationException.*key'):
        test_table.update_item(Key={'p': p, 'c': c},
            UpdateExpression='SET c = :val1', ExpressionAttributeValues={':val1': 4})
    with pytest.raises(ClientError, match='ValidationException.*key'):
        test_table.update_item(Key={'p': p, 'c': c}, UpdateExpression='REMOVE p')
    with pytest.raises(ClientError, match='ValidationException.*key'):
        test_table.update_item(Key={'p': p, 'c': c}, UpdateExpression='REMOVE c')
    with pytest.raises(ClientError, match='ValidationException.*key'):
        test_table.update_item(Key={'p': p, 'c': c},
            UpdateExpression='ADD p :val1', ExpressionAttributeValues={':val1': 4})
    with pytest.raises(ClientError, match='ValidationException.*key'):
        test_table.update_item(Key={'p': p, 'c': c},
            UpdateExpression='ADD c :val1', ExpressionAttributeValues={':val1': 4})
    with pytest.raises(ClientError, match='ValidationException.*key'):
        test_table.update_item(Key={'p': p, 'c': c},
            UpdateExpression='DELETE p :val1', ExpressionAttributeValues={':val1': set(['cat', 'mouse'])})
    with pytest.raises(ClientError, match='ValidationException.*key'):
        test_table.update_item(Key={'p': p, 'c': c},
            UpdateExpression='DELETE c :val1', ExpressionAttributeValues={':val1': set(['cat', 'mouse'])})
    # As sanity check, verify we *can* modify a non-key column
    test_table.update_item(Key={'p': p, 'c': c}, UpdateExpression='SET a = :val1', ExpressionAttributeValues={':val1': 4})
    assert test_table.get_item(Key={'p': p, 'c': c}, ConsistentRead=True)['Item'] == {'p': p, 'c': c, 'a': 4}
    test_table.update_item(Key={'p': p, 'c': c}, UpdateExpression='REMOVE a')
    assert test_table.get_item(Key={'p': p, 'c': c}, ConsistentRead=True)['Item'] == {'p': p, 'c': c}

# Test that trying to start an expression with some nonsense like HELLO
# instead of SET, REMOVE, ADD or DELETE, fails.
def test_update_expression_non_existent_clause(test_table_s):
    p = random_string()
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='HELLO b = :val1',
            ExpressionAttributeValues={':val1': 4})

# Test support for "SET a = :val1 + :val2", "SET a = :val1 - :val2"
# Only exactly these combinations work - e.g., it's a syntax error to
# try to add three. Trying to add a string fails.
def test_update_expression_plus_basic(test_table_s):
    p = random_string()
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET b = :val1 + :val2',
        ExpressionAttributeValues={':val1': 4, ':val2': 3})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'b': 7}
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET b = :val1 - :val2',
        ExpressionAttributeValues={':val1': 5, ':val2': 2})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'b': 3}
    # Only the addition of exactly two values is supported!
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET b = :val1 + :val2 + :val3',
            ExpressionAttributeValues={':val1': 4, ':val2': 3, ':val3': 2})
    # Only numeric values can be added - other things like strings or lists
    # cannot be added, and we get an error like "Incorrect operand type for
    # operator or function; operator or function: +, operand type: S".
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET b = :val1 + :val2',
            ExpressionAttributeValues={':val1': 'dog', ':val2': 3})
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET b = :val1 + :val2',
            ExpressionAttributeValues={':val1': ['a', 'b'], ':val2': ['1', '2']})

# Test support for "SET a = b + :val2" et al., i.e., a version of the
# above test_update_expression_plus_basic with read before write.
def test_update_expression_plus_rmw(test_table_s):
    p = random_string()
    test_table_s.put_item(Item={'p': p, 'a': 2})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['a'] == 2
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET a = a + :val1',
        ExpressionAttributeValues={':val1': 3})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['a'] == 5
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET a = :val1 + a',
        ExpressionAttributeValues={':val1': 4})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['a'] == 9
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET b = :val1 + a',
        ExpressionAttributeValues={':val1': 1})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['b'] == 10
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET a = b + a')
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['a'] == 19

# Test the list_append() function in SET, for the most basic use case of
# concatenating two value references. Because this is the first test of
# functions in SET, we also test some generic features of how functions
# are parsed.
def test_update_expression_list_append_basic(test_table_s):
    p = random_string()
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET a = list_append(:val1, :val2)',
        ExpressionAttributeValues={':val1': [4, 'hello'], ':val2': ['hi', 7]})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': [4, 'hello', 'hi', 7]}
    # Unlike the operation name "SET", function names are case-sensitive!
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET a = LIST_APPEND(:val1, :val2)',
            ExpressionAttributeValues={':val1': [4, 'hello'], ':val2': ['hi', 7]})
    # As usual, spaces are ignored by the parser
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET a = list_append(:val1, :val2)',
        ExpressionAttributeValues={':val1': ['a'], ':val2': ['b']})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': ['a', 'b']}
    # The list_append function only allows two parameters. The parser can
    # correctly parse fewer or more, but then an error is generated: "Invalid
    # UpdateExpression: Incorrect number of operands for operator or function;
    # operator or function: list_append, number of operands: 1".
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET a = list_append(:val1)',
            ExpressionAttributeValues={':val1': ['a']})
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET a = list_append(:val1, :val2, :val3)',
            ExpressionAttributeValues={':val1': [4, 'hello'], ':val2': [7], ':val3': ['a']})
    # If list_append is used on value which isn't a list, we get
    # error: "Invalid UpdateExpression: Incorrect operand type for operator
    # or function; operator or function: list_append, operand type: S"
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET a = list_append(:val1, :val2)',
            ExpressionAttributeValues={':val1': [4, 'hello'], ':val2': 'hi'})

# Additional list_append() tests, also using attribute paths as parameters
# (i.e., read-modify-write).
def test_update_expression_list_append(test_table_s):
    p = random_string()
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET a = :val1',
        ExpressionAttributeValues={':val1': ['hi', 2]})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['a'] ==['hi', 2]
    # Often, list_append is used to append items to a list attribute
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET a = list_append(a, :val1)',
        ExpressionAttributeValues={':val1': [4, 'hello']})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['a'] == ['hi', 2, 4, 'hello']
    # But it can also be used to just concatenate in other ways:
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET a = list_append(:val1, a)',
        ExpressionAttributeValues={':val1': ['dog']})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['a'] == ['dog', 'hi', 2, 4, 'hello']
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET b = list_append(a, :val1)',
        ExpressionAttributeValues={':val1': ['cat']})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['b'] == ['dog', 'hi', 2, 4, 'hello', 'cat']
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET c = list_append(a, b)')
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['c'] == ['dog', 'hi', 2, 4, 'hello', 'dog', 'hi', 2, 4, 'hello', 'cat']
    # As usual, #references are allowed instead of inline names:
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET #name1 = list_append(#name2,:val1)',
        ExpressionAttributeValues={':val1': [8]},
        ExpressionAttributeNames={'#name1': 'a', '#name2': 'a'})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['a'] == ['dog', 'hi', 2, 4, 'hello', 8]

# Test the "if_not_exists" function in SET
# The test also checks additional features of function-call parsing.
def test_update_expression_if_not_exists(test_table_s):
    p = random_string()
    # Since attribute a doesn't exist, set it:
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET a = if_not_exists(a, :val1)',
        ExpressionAttributeValues={':val1': 2})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['a'] == 2
    # Now the attribute does exist, so set does nothing:
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET a = if_not_exists(a, :val1)',
        ExpressionAttributeValues={':val1': 3})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['a'] == 2
    # if_not_exists can also be used to check one attribute and set another,
    # but note that if_not_exists(a, :val) means a's value if it exists,
    # otherwise :val!
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET b = if_not_exists(c, :val1)',
        ExpressionAttributeValues={':val1': 4})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['b'] == 4
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['a'] == 2
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET b = if_not_exists(c, :val1)',
        ExpressionAttributeValues={':val1': 5})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['b'] == 5
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET b = if_not_exists(a, :val1)',
        ExpressionAttributeValues={':val1': 6})
    # note how because 'a' does exist, its value is copied, overwriting b's
    # value:
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['b'] == 2
    # The parser expects function parameters to be value references, paths,
    # or nested call to functions. Other crap will cause syntax errors:
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET b = if_not_exists(non@sense, :val1)',
            ExpressionAttributeValues={':val1': 6})
    # if_not_exists() requires that the first parameter be a path. However,
    # the parser doesn't know this, and allows for a function parameter
    # also a value reference or a function call. If try one of these other
    # things the parser succeeds, but we get a later error, looking like:
    # "Invalid UpdateExpression: Operator or function requires a document
    # path; operator or function: if_not_exists"
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET b = if_not_exists(if_not_exists(a, :val2), :val1)',
            ExpressionAttributeValues={':val1': 6, ':val2': 3})
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET b = if_not_exists(:val2, :val1)',
            ExpressionAttributeValues={':val1': 6, ':val2': 3})
    # Surprisingly, if the wrong argument is a :val value reference, the
    # parser first tries to look it up in ExpressionAttributeValues (and
    # fails if it's missing), before realizing any value reference would be
    # wrong... So the following fails like the above does - but with a
    # different error message (which we do not check here): "Invalid
    # UpdateExpression: An expression attribute value used in expression
    # is not defined; attribute value: :val2"
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET b = if_not_exists(:val2, :val1)',
            ExpressionAttributeValues={':val1': 6})

# When the expression parser parses a function call f(value, value), each
# value may itself be a function call - ad infinitum. So expressions like
# list_append(if_not_exists(a, :val1), :val2) are legal and so is deeper
# nesting.
@pytest.mark.xfail(reason="for unknown reason, DynamoDB does not allow nesting list_append")
def test_update_expression_function_nesting(test_table_s):
    p = random_string()
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET a = list_append(if_not_exists(a, :val1), :val2)',
            ExpressionAttributeValues={':val1': ['a', 'b'], ':val2': ['cat', 'dog']})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['a'] == ['a', 'b', 'cat', 'dog']
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET a = list_append(if_not_exists(a, :val1), :val2)',
            ExpressionAttributeValues={':val1': ['a', 'b'], ':val2': ['1', '2']})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['a'] == ['a', 'b', 'cat', 'dog', '1', '2']
    # I don't understand why the following expression isn't accepted, but it
    # isn't! It produces a "Invalid UpdateExpression: The function is not
    # allowed to be used this way in an expression; function: list_append".
    # I don't know how to explain it. In any case, the *parsing* works -
    # this is not a syntax error - the failure is in some verification later.
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET a = list_append(list_append(:val1, :val2), :val3)',
                ExpressionAttributeValues={':val1': ['a'], ':val2': ['1'], ':val3': ['hi']})
    # Ditto, the following passes the parser but fails some later check with
    # the same error message as above.
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET a = list_append(list_append(list_append(:val1, :val2), :val3), :val4)',
                ExpressionAttributeValues={':val1': ['a'], ':val2': ['1'], ':val3': ['hi'], ':val4': ['yo']})

# Verify how in SET expressions, "+" (or "-") nests with functions.
# We discover that f(x)+f(y) works but f(x+y) does NOT (results in a syntax
# error on the "+"). This means that the parser has two separate rules:
# 1.  set_action: SET path = value + value
# 2.  value: VALREF | NAME | NAME (value, ...)
def test_update_expression_function_plus_nesting(test_table_s):
    p = random_string()
    # As explained above, this - with "+" outside the expression, works:
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET b = if_not_exists(b, :val1)+:val2',
            ExpressionAttributeValues={':val1': 2, ':val2': 3})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['b'] == 5
    # ...but this - with the "+" inside an expression parameter, is a syntax
    # error:
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET c = if_not_exists(c, :val1+:val2)',
                ExpressionAttributeValues={':val1': 5, ':val2': 4})

# This test tries to use an undefined function "f". This, obviously, fails,
# but where we to actually print the error we would see "Invalid
# UpdateExpression: Invalid function name; function: f". Not a syntax error.
# This means that the parser accepts any alphanumeric name as a function
# name, and only later use of this function fails because it's not one of
# the supported file.
def test_update_expression_unknown_function(test_table_s):
    p = random_string()
    with pytest.raises(ClientError, match='ValidationException.*f'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET a = f(b,c,d)')
    with pytest.raises(ClientError, match='ValidationException.*f123_hi'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET a = f123_hi(b,c,d)')
    # Just like unreferenced column names parsed by the DynamoDB parser,
    # function names must also start with an alphabetic character. Trying
    # to use _f as a function name will result with an actual syntax error,
    # on the "_" token.
    with pytest.raises(ClientError, match='ValidationException.*yntax error'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET a = _f(b,c,d)')

# Test "ADD" operation for numbers
def test_update_expression_add_numbers(test_table_s):
    p = random_string()
    test_table_s.put_item(Item={'p': p, 'a': 3, 'b': 'hi'})
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='ADD a :val1',
        ExpressionAttributeValues={':val1': 4})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['a'] == 7
    # If the value to be added isn't a number, we get an error like "Invalid
    # UpdateExpression: Incorrect operand type for operator or function;
    # operator: ADD, operand type: STRING".
    with pytest.raises(ClientError, match='ValidationException.*type'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='ADD a :val1',
            ExpressionAttributeValues={':val1': 'hello'})
    # Similarly, if the attribute we're adding to isn't a number, we get an
    # error like "An operand in the update expression has an incorrect data
    # type"
    with pytest.raises(ClientError, match='ValidationException.*type'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='ADD b :val1',
            ExpressionAttributeValues={':val1': 1})

# In test_update_expression_add_numbers() above we tested ADDing a number to
# an existing number. The following test check that ADD can be used to
# create a *new* number, as if it was added to zero.
def test_update_expression_add_numbers_new(test_table_s):
    # Test that "ADD" can create a new number attribute:
    p = random_string()
    test_table_s.put_item(Item={'p': p, 'a': 'hello'})
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='ADD b :val1',
        ExpressionAttributeValues={':val1': 7})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['b'] == 7
    # Test that "ADD" can create an entirely new item:
    p = random_string()
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='ADD b :val1',
        ExpressionAttributeValues={':val1': 8})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['b'] == 8

# Test "ADD" operation for sets
def test_update_expression_add_sets(test_table_s):
    p = random_string()
    test_table_s.put_item(Item={'p': p, 'a': set(['dog', 'cat', 'mouse']), 'b': 'hi'})
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='ADD a :val1',
        ExpressionAttributeValues={':val1': set(['pig'])})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['a'] == set(['dog', 'cat', 'mouse', 'pig'])

    # TODO: right now this test won't detect duplicated values in the returned result,
    # because boto3 parses a set out of the returned JSON anyway. This check should leverage
    # lower level API (if exists) to ensure that the JSON contains no duplicates
    # in the set representation. It has been verified manually.
    test_table_s.put_item(Item={'p': p, 'a': set(['beaver', 'lynx', 'coati']), 'b': 'hi'})
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='ADD a :val1',
        ExpressionAttributeValues={':val1': set(['coati', 'beaver', 'badger'])})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['a'] == set(['beaver', 'badger', 'lynx', 'coati'])

    # The value to be added needs to be a set of the same type - it can't
    # be a single element or anything else. If the value has the wrong type,
    # we get an error like "Invalid UpdateExpression: Incorrect operand type
    # for operator or function; operator: ADD, operand type: STRING".
    with pytest.raises(ClientError, match='ValidationException.*type'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='ADD a :val1',
            ExpressionAttributeValues={':val1': 'hello'})

# In test_update_expression_add_sets() above we tested ADDing elements to an
# existing set. The following test checks that ADD can be used to create a
# *new* set, by adding its first item.
def test_update_expression_add_sets_new(test_table_s):
    # Test that "ADD" can create a new set attribute:
    p = random_string()
    test_table_s.put_item(Item={'p': p, 'a': 'hello'})
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='ADD b :val1',
        ExpressionAttributeValues={':val1': set(['dog'])})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['b'] == set(['dog'])
    # Test that "ADD" can create an entirely new item:
    p = random_string()
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='ADD b :val1',
        ExpressionAttributeValues={':val1': set(['cat'])})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['b'] == set(['cat'])

# Test "DELETE" operation for sets
def test_update_expression_delete_sets(test_table_s):
    p = random_string()
    test_table_s.put_item(Item={'p': p, 'a': set(['dog', 'cat', 'mouse']), 'b': 'hi'})
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='DELETE a :val1',
        ExpressionAttributeValues={':val1': set(['cat', 'mouse'])})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['a'] == set(['dog'])
    # Deleting an element not present in the set is not an error - it just
    # does nothing
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='DELETE a :val1',
        ExpressionAttributeValues={':val1': set(['pig'])})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['a'] == set(['dog'])
    # Deleting all the elements cannot leave an empty set (which isn't
    # supported). Rather, it deletes the attribute altogether:
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='DELETE a :val1',
        ExpressionAttributeValues={':val1': set(['dog'])})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'b': 'hi'}
    # Deleting elements from a non-existent attribute is allowed, and
    # simply does nothing:
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='DELETE a :val1',
        ExpressionAttributeValues={':val1': set(['dog'])})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'b': 'hi'}
    # An empty set parameter is not allowed
    with pytest.raises(ClientError, match='ValidationException.*empty'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='DELETE a :val1',
            ExpressionAttributeValues={':val1': set([])})
    # The value to be deleted must be a set of the same type - it can't
    # be a single element or anything else. If the value has the wrong type,
    # we get an error like "Invalid UpdateExpression: Incorrect operand type
    # for operator or function; operator: DELETE, operand type: STRING".
    test_table_s.put_item(Item={'p': p, 'a': set(['dog', 'cat', 'mouse']), 'b': 'hi'})
    with pytest.raises(ClientError, match='ValidationException.*type'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='DELETE a :val1',
            ExpressionAttributeValues={':val1': 'hello'})

######## Tests for paths and nested attribute updates:

# A dot inside a name in ExpressionAttributeNames is a literal dot, and
# results in a top-level attribute with an actual dot in its name - not
# a nested attribute path.
def test_update_expression_dot_in_name(test_table_s):
    p = random_string()
    test_table_s.update_item(Key={'p': p}, UpdateExpression='SET #a = :val1',
        ExpressionAttributeValues={':val1': 3},
        ExpressionAttributeNames={'#a': 'a.b'})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a.b': 3}


# Below we have several tests of what happens when a nested attribute is
# on the left-hand side of an assignment, but an every simpler case of
# nested attributes is having one on the right hand side of an assignment:
def test_update_expression_nested_attribute_rhs(test_table_s):
    p = random_string()
    test_table_s.put_item(Item={'p': p, 'a': {'b': 3, 'c': {'x': 7, 'y': 8}}, 'd': 5})
    test_table_s.update_item(Key={'p': p}, UpdateExpression='SET z = a.c.x')
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['z'] == 7

# A basic test for direct update of a nested attribute: One of the top-level
# attributes is itself a document, and we update only one of that document's
# nested attributes.
def test_update_expression_nested_attribute_dot(test_table_s):
    p = random_string()
    test_table_s.put_item(Item={'p': p, 'a': {'b': 3, 'c': 4}, 'd': 5})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': {'b': 3, 'c': 4}, 'd': 5}
    test_table_s.update_item(Key={'p': p}, UpdateExpression='SET a.c = :val1',
        ExpressionAttributeValues={':val1': 7})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': {'b': 3, 'c': 7}, 'd': 5}
    # Of course we can also add new nested attributes, not just modify
    # existing ones:
    test_table_s.update_item(Key={'p': p}, UpdateExpression='SET a.d = :val1',
        ExpressionAttributeValues={':val1': 3})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': {'b': 3, 'c': 7, 'd': 3}, 'd': 5}

# Similar test, for a list: one of the top-level attributes is a list, we
# can update one of its items.
def test_update_expression_nested_attribute_index(test_table_s):
    p = random_string()
    test_table_s.put_item(Item={'p': p, 'a': ['one', 'two', 'three']})
    test_table_s.update_item(Key={'p': p}, UpdateExpression='SET a[1] = :val1',
        ExpressionAttributeValues={':val1': 'hello'})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': ['one', 'hello', 'three']}

# An index into a list must be an actual integer - DynamoDB does not support
# a value reference (:xyz) to be used as a index. This is the same test as the
# above test_update_expression_nested_attribute_index() - we just try to use
# the a reference :xyz for the index 1. And it's considered a syntax error
def test_update_expression_nested_attribute_index_reference(test_table_s):
    p = random_string()
    test_table_s.put_item(Item={'p': p, 'a': ['one', 'two', 'three']})
    with pytest.raises(ClientError, match='ValidationException.*yntax'):
        test_table_s.update_item(Key={'p': p}, UpdateExpression='SET a[:xyz] = :val1',
            ExpressionAttributeValues={':val1': 'hello', ':xyz': 1})

# In the previous test we saw that a value reference (:xyz) can't be used
# as an index. Here we see that a name reference (#xyz) also can't be used
# as an index. It too is a syntax error - it doesn't even matter if we
# define this #xyz or not.
def test_update_expression_nested_attribute_index_reference_name(test_table_s):
    p = random_string()
    test_table_s.put_item(Item={'p': p, 'a': ['one', 'two', 'three']})
    with pytest.raises(ClientError, match='ValidationException.*yntax'):
        test_table_s.update_item(Key={'p': p}, UpdateExpression='SET a[#xyz] = :val1',
            ExpressionAttributeValues={':val1': 'hello'})

# Test that just like happens in top-level attributes, also in nested
# attributes, setting them replaces the old value - potentially an entire
# nested document, by the whole value (which may have a different type)
def test_update_expression_nested_different_type(test_table_s):
    p = random_string()
    test_table_s.put_item(Item={'p': p, 'a': {'b': 3, 'c': {'one': 1, 'two': 2}}})
    test_table_s.update_item(Key={'p': p}, UpdateExpression='SET a.c = :val1',
        ExpressionAttributeValues={':val1': 7})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': {'b': 3, 'c': 7}}

# Yet another test of a nested attribute update. This one uses deeper
# level of nesting (dots and indexes), adds #name references to the mix.
def test_update_expression_nested_deep(test_table_s):
    p = random_string()
    test_table_s.put_item(Item={'p': p, 'a': {'b': 3, 'c': ['hi', {'x': {'y': [3, 5, 7]}}]}})
    test_table_s.update_item(Key={'p': p}, UpdateExpression='SET a.c[1].#name.y[1] = :val1',
        ExpressionAttributeValues={':val1': 9}, ExpressionAttributeNames={'#name': 'x'})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['a'] ==  {'b': 3, 'c': ['hi', {'x': {'y': [3, 9, 7]}}]}
    # A deep path can also appear on the right-hand-side of an assignment
    test_table_s.update_item(Key={'p': p}, UpdateExpression='SET a.z = a.c[1].#name.y[1]',
        ExpressionAttributeNames={'#name': 'x'})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['a']['z'] ==  9

# A REMOVE operation can be used to remove nested attributes, and also
# individual list items.
def test_update_expression_nested_remove(test_table_s):
    p = random_string()
    test_table_s.put_item(Item={'p': p, 'a': {'b': 3, 'c': ['hi', {'x': {'y': [3, 5, 7]}, 'q': 2}]}})
    test_table_s.update_item(Key={'p': p}, UpdateExpression='REMOVE a.c[1].x.y[1], a.c[1].q')
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['a'] ==  {'b': 3, 'c': ['hi', {'x': {'y': [3, 7]}}]}

# Removing a list item beyond the end of the list (e.g., REMOVE a[17] when
# the list only has three items) is silently ignored.
def test_update_expression_nested_remove_list_item_after_end(test_table_s):
    p = random_string()
    test_table_s.put_item(Item={'p': p, 'a': [4, 5, 6]})
    test_table_s.update_item(Key={'p': p}, UpdateExpression='REMOVE a[17]')

# If we remove a[1] and then change a[3], the index "3" refers to the position
# *before* the first removal.
def test_update_expression_nested_remove_list_item_original_number(test_table_s):
    p = random_string()
    test_table_s.put_item(Item={'p': p, 'a': [2, 3, 4, 5, 6, 7]})
    test_table_s.update_item(Key={'p': p}, UpdateExpression='REMOVE a[1] SET a[3] = :val',
        ExpressionAttributeValues={':val': 17})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['a'] ==  [2, 4, 17, 6, 7]
    # The order of the operations doesn't matter
    test_table_s.put_item(Item={'p': p, 'a': [2, 3, 4, 5, 6, 7]})
    test_table_s.update_item(Key={'p': p}, UpdateExpression='SET a[3] = :val REMOVE a[1]',
        ExpressionAttributeValues={':val': 17})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['a'] ==  [2, 4, 17, 6, 7]

# DynamoDB allows an empty map. So removing the only member from a map leaves
# behind an empty map.
def test_update_expression_nested_remove_singleton_map(test_table_s):
    p = random_string()
    test_table_s.put_item(Item={'p': p, 'a': {'b': 1}})
    test_table_s.update_item(Key={'p': p}, UpdateExpression='REMOVE a.b')
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['a'] == {}

# DynamoDB allows an empty list. So removing the only member from a list leaves
# behind an empty list.
def test_update_expression_nested_remove_singleton_list(test_table_s):
    p = random_string()
    test_table_s.put_item(Item={'p': p, 'a': [1]})
    test_table_s.update_item(Key={'p': p}, UpdateExpression='REMOVE a[0]')
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['a'] == []

# The DynamoDB documentation specifies: "When you use SET to update a list
# element, the contents of that element are replaced with the new data that
# you specify. If the element does not already exist, SET will append the
# new element at the end of the list."
# So if we take a three-element list a[7], and set a[7], the new element
# will be put at the end of the list, not position 7 specifically.
def test_nested_attribute_update_array_out_of_bounds(test_table_s):
    p = random_string()
    test_table_s.put_item(Item={'p': p, 'a': ['one', 'two', 'three']})
    test_table_s.update_item(Key={'p': p}, UpdateExpression='SET a[7] = :val1',
        ExpressionAttributeValues={':val1': 'hello'})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': ['one', 'two', 'three', 'hello']}
    # The DynamoDB documentation also says: "If you add multiple elements
    # in a single SET operation, the elements are sorted in order by element
    # number.
    test_table_s.update_item(Key={'p': p}, UpdateExpression='SET a[84] = :val1, a[37] = :val2, a[17] = :val3, a[50] = :val4',
        ExpressionAttributeValues={':val1': 'a1', ':val2': 'a2', ':val3': 'a3', ':val4': 'a4'})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': ['one', 'two', 'three', 'hello', 'a3', 'a2', 'a4', 'a1']}

# Test what happens if we try to write to a.b, which would only make sense if
# a were a nested document, but a doesn't exist, or exists and is NOT a nested
# document but rather a scalar or list or something.
# DynamoDB actually detects this case and prints an error:
#   ClientError: An error occurred (ValidationException) when calling the
#   UpdateItem operation: The document path provided in the update expression
#   is invalid for update
def test_nested_attribute_update_bad_path_dot(test_table_s):
    p = random_string()
    test_table_s.put_item(Item={'p': p, 'a': 'hello', 'b': ['hi']})
    with pytest.raises(ClientError, match='ValidationException.*path'):
        test_table_s.update_item(Key={'p': p}, UpdateExpression='SET a.c = :val1',
            ExpressionAttributeValues={':val1': 7})
    with pytest.raises(ClientError, match='ValidationException.*path'):
        test_table_s.update_item(Key={'p': p}, UpdateExpression='SET b.c = :val1',
            ExpressionAttributeValues={':val1': 7})
    with pytest.raises(ClientError, match='ValidationException.*path'):
        test_table_s.update_item(Key={'p': p}, UpdateExpression='SET c.c = :val1',
            ExpressionAttributeValues={':val1': 7})
    # Same errors for "remove" operation.
    with pytest.raises(ClientError, match='ValidationException.*path'):
        test_table_s.update_item(Key={'p': p}, UpdateExpression='REMOVE a.c')
    with pytest.raises(ClientError, match='ValidationException.*path'):
        test_table_s.update_item(Key={'p': p}, UpdateExpression='REMOVE b.c')
    with pytest.raises(ClientError, match='ValidationException.*path'):
        test_table_s.update_item(Key={'p': p}, UpdateExpression='REMOVE c.c')
    # Same error when the item doesn't exist at all:
    p = random_string()
    with pytest.raises(ClientError, match='ValidationException.*path'):
        test_table_s.update_item(Key={'p': p}, UpdateExpression='SET c.c = :val1',
            ExpressionAttributeValues={':val1': 7})
    # HOWEVER, although it *is* allowed to remove a random path from a non-
    # existent item. I don't know why. See the next test -
    # test_nested_attribute_remove_from_missing_item

# Though in the above test (test_nested_attribute_update_bad_path_dot) we
# showed that DynamoDB does not allow REMOVE x.y if attribute x doesn't
# exist - and generates a ValidationException, it turns out that if the
# entire item doesn't exist, then a REMOVE x.y is silently ignored.
# I don't understand why they did this.
@pytest.mark.xfail(reason="for unknown reason, DynamoDB allows REMOVE x.y when item doesn't exist")
def test_nested_attribute_remove_from_missing_item(test_table_s):
    p = random_string()
    test_table_s.update_item(Key={'p': p}, UpdateExpression='REMOVE x.y')
    test_table_s.update_item(Key={'p': p}, UpdateExpression='REMOVE x[0]')

# Similarly for other types of bad paths - using [0] on something which
# doesn't exist or isn't an array.
def test_nested_attribute_update_bad_path_array(test_table_s):
    p = random_string()
    test_table_s.put_item(Item={'p': p, 'a': 'hello'})
    with pytest.raises(ClientError, match='ValidationException.*path'):
        test_table_s.update_item(Key={'p': p}, UpdateExpression='SET a[0] = :val1',
            ExpressionAttributeValues={':val1': 7})
    with pytest.raises(ClientError, match='ValidationException.*path'):
        test_table_s.update_item(Key={'p': p}, UpdateExpression='SET b[0] = :val1',
            ExpressionAttributeValues={':val1': 7})
    # Same errors for "remove" operation.
    with pytest.raises(ClientError, match='ValidationException.*path'):
        test_table_s.update_item(Key={'p': p}, UpdateExpression='REMOVE a[0]')
    with pytest.raises(ClientError, match='ValidationException.*path'):
        test_table_s.update_item(Key={'p': p}, UpdateExpression='REMOVE b[0]')
    # Same error when the item doesn't exist at all:
    p = random_string()
    with pytest.raises(ClientError, match='ValidationException.*path'):
        test_table_s.update_item(Key={'p': p}, UpdateExpression='SET b[0] = :val1',
            ExpressionAttributeValues={':val1': 7})
    # HOWEVER, although it *is* allowed to remove a random path from a non-
    # existent item. I don't know why... See test_nested_attribute_remove_from_missing_item

# DynamoDB Does not allow empty sets.
# Trying to ask UpdateItem to put one of these in an attribute should be
# forbidden. Empty lists and maps *are* allowed.
# Note that in test_item.py::test_update_item_empty_attribute we checked
# this with the AttributeUpdates syntax. Here we check the same with the
# UpdateExpression syntax.
def test_update_expression_empty_attribute(test_table_s):
    p = random_string()
    # Empty sets are *not* allowed
    with pytest.raises(ClientError, match='ValidationException.*empty'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET a = :v',
            ExpressionAttributeValues={':v': set()})
    assert not 'Item' in test_table_s.get_item(Key={'p': p}, ConsistentRead=True)
    # But empty lists, maps, strings and binary blobs *are* allowed:
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET d = :v1, e = :v2, f = :v3, g = :v4',
        ExpressionAttributeValues={':v1': [], ':v2': {}, ':v3': '', ':v4': b''})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'd': [], 'e': {}, 'f': '', 'g': b''}

# Verify which kind of update operations require a read-before-write (a.k.a
# read-modify-write, or RMW). We test this by using a table configured with
# "forbid_rmw" isolation mode and checking which writes succeed or pass.
# This is a Scylla-only test (the test_table_s_forbid_rmw implies scylla_only).
def test_update_expression_when_rmw(test_table_s_forbid_rmw):
    table = test_table_s_forbid_rmw
    p = random_string()
    # A write with a RHS (right-hand side) being a constant from the query
    # doesn't need RMW:
    table.update_item(Key={'p': p},
        UpdateExpression='SET a = :val',
        ExpressionAttributeValues={':val': 3})
    # But if the LHS (left-hand side) of the assignment is a document path,
    # it *does* need RMW:
    with pytest.raises(ClientError, match='ValidationException.*write isolation policy'):
        table.update_item(Key={'p': p},
            UpdateExpression='SET a.b = :val',
            ExpressionAttributeValues={':val': 3})
    assert table.get_item(Key={'p': p}, ConsistentRead=True)['Item']['a'] == 3
    # A write with a path in the RHS of a SET also needs RMW
    with pytest.raises(ClientError, match='ValidationException.*write isolation policy'):
        table.update_item(Key={'p': p},
            UpdateExpression='SET b = a')
