# Tests for the UpdateItem operations with an UpdateExpression parameter

import random
import string

import pytest
from botocore.exceptions import ClientError

def random_string(length=10, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for x in range(length))

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
@pytest.mark.xfail(reason="attribute copy (read-before-write) not yet implemented")
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

# In the previous test we saw that *modifying* the same item twice in the same
# update is forbidden; But it is allowed to *read* an item in the same update
# that also modifies it, and we check this here.
@pytest.mark.xfail(reason="attribute copy (read-before-write) not yet implemented")
def test_update_expression_multi_with_copy(test_table_s):
    p = random_string()
    test_table_s.put_item(Item={'p': p, 'a': 'hello'})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': 'hello'}
    # "REMOVE a SET b = a" works: as noted in test_update_expression_set_copy()
    # the value of 'a' is read before the actual REMOVE operation happens.
    test_table_s.update_item(Key={'p': p}, UpdateExpression='REMOVE a SET b = a')
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'b': 'hello'}
    test_table_s.update_item(Key={'p': p}, UpdateExpression='SET c = b remove b')
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

# Test that the key attributes (hash key or sort key) cannot be modified
# by an update
def test_update_expression_cannot_modify_key(test_table):
    p = random_string()
    c = random_string()
    with pytest.raises(ClientError, match='ValidationException'):
        test_table.update_item(Key={'p': p, 'c': c},
            UpdateExpression='SET p = :val1', ExpressionAttributeValues={':val1': 4})
    with pytest.raises(ClientError, match='ValidationException'):
        test_table.update_item(Key={'p': p, 'c': c},
            UpdateExpression='SET c = :val1', ExpressionAttributeValues={':val1': 4})
    with pytest.raises(ClientError, match='ValidationException'):
        test_table.update_item(Key={'p': p, 'c': c}, UpdateExpression='REMOVE p')
    with pytest.raises(ClientError, match='ValidationException'):
        test_table.update_item(Key={'p': p, 'c': c}, UpdateExpression='REMOVE c')
    # As sanity check, verify we *can* modify a non-key column
    test_table.update_item(Key={'p': p, 'c': c}, UpdateExpression='SET a = :val1', ExpressionAttributeValues={':val1': 4})
    assert test_table.get_item(Key={'p': p, 'c': c}, ConsistentRead=True)['Item'] == {'p': p, 'c': c, 'a': 4}
    test_table.update_item(Key={'p': p, 'c': c}, UpdateExpression='REMOVE a')
    assert test_table.get_item(Key={'p': p, 'c': c}, ConsistentRead=True)['Item'] == {'p': p, 'c': c}

# Test that trying to start an expression with some nonsense like HELLO
# instead of SET, REMOVE, ADD or DELETE, fails.
def test_update_expression_non_existant_clause(test_table_s):
    p = random_string()
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='HELLO b = :val1',
            ExpressionAttributeValues={':val1': 4})
