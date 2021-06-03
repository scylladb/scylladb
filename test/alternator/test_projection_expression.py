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

# Tests for the various operations (GetItem, Query, Scan) with a
# ProjectionExpression parameter.
#
# ProjectionExpression is an expension of the legacy AttributesToGet
# parameter. Both parameters request that only a subset of the attributes
# be fetched for each item, instead of all of them. But while AttributesToGet
# was limited to top-level attributes, ProjectionExpression can request also
# nested attributes.

import pytest
from botocore.exceptions import ClientError
from util import random_string, full_scan, full_query, multiset

# Basic test for ProjectionExpression, requesting only top-level attributes.
# Result should include the selected attributes only - if one wants the key
# attributes as well, one needs to select them explicitly. When no key
# attributes are selected, an item may have *none* of the selected
# attributes, and returned as an empty item.
def test_projection_expression_toplevel(test_table):
    p = random_string()
    c = random_string()
    item = {'p': p, 'c': c, 'a': 'hello', 'b': 'hi'}
    test_table.put_item(Item=item)
    for wanted in [ ['a'],             # only non-key attribute
                    ['c', 'a'],        # a key attribute (sort key) and non-key
                    ['p', 'c'],        # entire key
                    ['nonexistent']    # Our item doesn't have this
                   ]:
        got_item = test_table.get_item(Key={'p': p, 'c': c}, ProjectionExpression=",".join(wanted), ConsistentRead=True)['Item']
        expected_item = {k: item[k] for k in wanted if k in item}
        assert expected_item == got_item

# Various simple tests for ProjectionExpression's syntax, using only top-evel
# attributes.
def test_projection_expression_toplevel_syntax(test_table_s):
    p = random_string()
    test_table_s.put_item(Item={'p': p, 'a': 'hello', 'b': 'hi'})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression='a')['Item'] == {'a': 'hello'}
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression='#name', ExpressionAttributeNames={'#name': 'a'})['Item'] == {'a': 'hello'}
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression='a,b')['Item'] == {'a': 'hello', 'b': 'hi'}
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression=' a  ,   b  ')['Item'] == {'a': 'hello', 'b': 'hi'}
    # Missing or unused names in ExpressionAttributeNames are errors:
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression='#name', ExpressionAttributeNames={'#wrong': 'a'})['Item'] == {'a': 'hello'}
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression='#name', ExpressionAttributeNames={'#name': 'a', '#unused': 'b'})['Item'] == {'a': 'hello'}
    # It is not allowed to fetch the same top-level attribute twice (or in
    # general, list two overlapping attributes). We get an error like
    # "Invalid ProjectionExpression: Two document paths overlap with each
    # other; must remove or rewrite one of these paths; path one: [a], path
    # two: [a]".
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression='a,a')['Item']
    # A comma with nothing after it is a syntax error:
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression='a,')['Item']
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression=',a')['Item']
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression='a,,b')['Item']
    # An empty ProjectionExpression is not allowed. DynamoDB recognizes its
    # syntax, but then writes: "Invalid ProjectionExpression: The expression
    # can not be empty".
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression='')['Item']

# The following two tests are similar to test_projection_expression_toplevel()
# which tested the GetItem operation - but these test Scan and Query.
# Both test ProjectionExpression with only top-level attributes.
def test_projection_expression_scan(filled_test_table):
    table, items = filled_test_table
    for wanted in [ ['another'],       # only non-key attributes (one item doesn't have it!)
                    ['c', 'another'],  # a key attribute (sort key) and non-key
                    ['p', 'c'],        # entire key
                    ['nonexistent']    # none of the items have this attribute!
                   ]:
        got_items = full_scan(table,  ProjectionExpression=",".join(wanted))
        expected_items = [{k: x[k] for k in wanted if k in x} for x in items]
        assert multiset(expected_items) == multiset(got_items)

def test_projection_expression_query(test_table):
    p = random_string()
    items = [{'p': p, 'c': str(i), 'a': str(i*10), 'b': str(i*100) } for i in range(10)]
    with test_table.batch_writer() as batch:
        for item in items:
            batch.put_item(item)
    for wanted in [ ['a'],             # only non-key attributes
                    ['c', 'a'],        # a key attribute (sort key) and non-key
                    ['p', 'c'],        # entire key
                    ['nonexistent']    # none of the items have this attribute!
                   ]:
        got_items = full_query(test_table, KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}}, ProjectionExpression=",".join(wanted))
        expected_items = [{k: x[k] for k in wanted if k in x} for x in items]
        assert multiset(expected_items) == multiset(got_items)

# The previous tests all fetched only top-level attributes. They could all
# be written using AttributesToGet instead of ProjectionExpression (and,
# in fact, we do have similar tests with AttributesToGet in other files),
# but the previous test checked that the alternative syntax works correctly.
# The following test checks fetching more elaborate attribute paths from
# nested documents.
def test_projection_expression_path(test_table_s):
    p = random_string()
    test_table_s.put_item(Item={
        'p': p,
        'a': {'b': [2, 4, {'x': 'hi', 'y': 'yo'}], 'c': 5},
        'b': 'hello' 
        })
    # Fetching the entire nested document "a" works, of course:
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression='a')['Item'] == {'a': {'b': [2, 4, {'x': 'hi', 'y': 'yo'}], 'c': 5}}
    # If we fetch a.b, we get only the content of b - but it's still inside
    # the a dictionary:
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression='a.b')['Item'] == {'a': {'b': [2, 4, {'x': 'hi', 'y': 'yo'}]}}
    # Similarly, fetching a.b[0] gives us a one-element array in a dictionary.
    # Note that [0] is the first element of an array.
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression='a.b[0]')['Item'] == {'a': {'b': [2]}}
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression='a.b[2]')['Item'] == {'a': {'b': [{'x': 'hi', 'y': 'yo'}]}}
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression='a.b[2].y')['Item'] == {'a': {'b': [{'y': 'yo'}]}}
    # Trying to read any sort of non-existent attribute returns an empty item.
    # This includes a non-existing top-level attribute, an attempt to read
    # beyond the end of an array or a non-existent member of a dictionary, as
    # well as paths which begin with a non-existent prefix.
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression='x')['Item'] == {}
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression='a.b[3]')['Item'] == {}
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression='a.x')['Item'] == {}
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression='a.x.y')['Item'] == {}
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression='a.b[3].x')['Item'] == {}
    # Similarly, indexing a dictionary as an array, or array as dictionary, or
    # integer as either, yields an empty item.
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression='a.b.x')['Item'] == {}
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression='a[0]')['Item'] == {}
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression='a.b[0].x')['Item'] == {}
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression='a.b[0][0]')['Item'] == {}
    # We can read multiple paths - the result are merged into one object
    # structured the same was as in the original item:
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression='a.b[0],a.b[1]')['Item'] == {'a': {'b': [2, 4]}}
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression='a.b[0],a.c')['Item'] == {'a': {'b': [2], 'c': 5}}
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression='a.c,b')['Item'] == {'a': {'c': 5}, 'b': 'hello'}
    # If some of the paths are not available, they are silently ignored (just
    # like they returned an empty item when used alone earlier)
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression='a.x, a.b[0], x, a.b[3].x')['Item'] == {'a': {'b': [2]}}

    # It is not allowed to read the same path multiple times. The error from
    # DynamoDB looks like: "Invalid ProjectionExpression: Two document paths
    # overlap with each other; must remove or rewrite one of these paths;
    # path one: [a, b, [0]], path two: [a, b, [0]]".
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression='a.b[0],a.b[0]')['Item']
    # Two paths are considered to "overlap" if the content of one path
    # contains the content of the second path. So requesting both "a" and
    # "a.b[0]" is not allowed.
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression='a,a.b[0]')['Item']

    # Above we noted that asking for to project a non-existent attribute in an
    # existing item yields an empty Item object. However, if the item does not
    # exist at all, the Item object will be missing entirely:
    p = random_string()
    assert not 'Item' in test_table_s.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression='x')
    assert not 'Item' in test_table_s.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression='a.x')
    assert not 'Item' in test_table_s.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression='a[0]')

# Above in test_projection_expression_toplevel_syntax() we tested how
# name references (#name) work in top-level attributes. In the following
# two tests we test how they work in more elaborate paths:
# 1. Multiple path components can make multiple references, e.g., "#a.#b"
# 2. Conversely, a single reference, e.g., "#a", is always a single path
#    component. Even if "#a" is "a.b", this refers to the literal attribute
#    "a.b" - with a dot in its name - and not to the b element in a.
def test_projection_expression_path_references(test_table_s):
    p = random_string()
    test_table_s.put_item(Item={'p': p, 'a': {'b': 1, 'c': 2}, 'b': 'hi'})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression='a.b')['Item'] == {'a': {'b': 1}}
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression='#n1.b', ExpressionAttributeNames={'#n1': 'a'})['Item'] == {'a': {'b': 1}}
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression='a.#n2', ExpressionAttributeNames={'#n2': 'b'})['Item'] == {'a': {'b': 1}}
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression='#n1.#n2', ExpressionAttributeNames={'#n1': 'a', '#n2': 'b'})['Item'] == {'a': {'b': 1}}
    # Missing or unused names in ExpressionAttributeNames are errors:
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression='a.#n2', ExpressionAttributeNames={'#wrong': 'b'})
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression='a.#n2', ExpressionAttributeNames={'#n2': 'b', '#unused': 'x'})

def test_projection_expression_path_dot(test_table_s):
    p = random_string()
    test_table_s.put_item(Item={'p': p, 'a.b': 'hi', 'a': {'b': 'yo', 'c': 'jo'}})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression='a.b')['Item'] == {'a': {'b': 'yo'}}
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression='#name', ExpressionAttributeNames={'#name': 'a.b'})['Item'] == {'a.b': 'hi'}

# DynamoDB does not allow "overlapping" paths to be listed in
# ProjectionExpression. This includes both identical paths, and paths where
# one is a sub-path of the other - e.g. "a.b" and "a.b.c". As we already saw
# above, paths with just a common *prefix* - e.g., "a.b, a.c" - are fine.
def test_projection_expression_path_overlap(test_table_s):
    # The overlap is tested symbolically, on the given paths, without any
    # relation to what the item contains, or whether it even exists. So we
    # don't even need to create an item for this test. We still need a
    # key for the GetItem call :-)
    p = random_string()
    for expr in ['a, a',
                 'a.b, a.b',
                 'a[1], a[1]',
                 'a, a.b',
                 'a.b, a',
                 'a.b, a.b[2]',
                 'a.b, a.b.c',
                 'a, a.b[2].c',
                 'a.b.d, a.b',
                 'a.b.d.e, a.b',
                 'a.b, a.b.d',
                 'a.b, a.b.d.e',
                ]:
        with pytest.raises(ClientError, match='ValidationException.* overlap'):
            print(expr)
            test_table_s.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression=expr)
    # The checks above can be easily passed by an over-zealos "overlap" check
    # which declares everything an overlap :-) Let's also check some non-
    # overlap cases - which shouldn't be declared an overlap.
    for expr in ['a, b',
                 'a.b, a.c',
                 'a.b.d, a.b.e',
                 'a[1], a[2]',
                 'a.b, a.c[2]',
                ]:
        print(expr)
        test_table_s.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression=expr)

# In addition to not allowing "overlapping" paths, DynamoDB also does not
# allow "conflicting" paths: It does not allow giving both a.b and a[1] in a
# single ProjectionExpression. It gives the error:
#  "Invalid ProjectionExpression: Two document paths conflict with each other;
#   must remove or rewrite one of these paths; path one: [a, b], path two:
#   [a, [1]]".
# The reasoning is that asking for both in one request makes no sense because
# no item will ever be able to fulfill both.
def test_projection_expression_path_conflict(test_table_s):
    # The conflict is tested symbolically, on the given paths, without any
    # relation to what the item contains, or whether it even exists. So we
    # don't even need to create an item for this test. We still need a
    # key for the GetItem call :-)
    p = random_string()
    for expr in ['a.b, a[1]',
                 'a[1], a.b',
                 'a.b[1], a.b.c',
                 'a.b.c, a.b[1]',
                ]:
        with pytest.raises(ClientError, match='ValidationException.* conflict'):
            test_table_s.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression=expr)
    # The checks above can be easily passed by an over-zealos "conflict" check
    # which declares everything a conflict :-) Let's also check some non-
    # conflict cases - which shouldn't be declared a conflict.
    for expr in ['a.b, a.c',
                 'a.b, a.c[1]',
                ]:
        test_table_s.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression=expr)

# Above we nested paths in ProjectionExpression, but just for the GetItem
# request. Let's verify they also work in Query and Scan requests:
def test_query_projection_expression_path(test_table):
    p = random_string()
    items = [{'p': p, 'c': str(i), 'a': {'x': str(i*10), 'y': 'hi'}, 'b': 'hello' } for i in range(10)]
    with test_table.batch_writer() as batch:
        for item in items:
            batch.put_item(item)
    got_items = full_query(test_table, KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}}, ProjectionExpression="a.x")
    expected_items = [{'a': {'x': x['a']['x']}} for x in items]
    assert multiset(expected_items) == multiset(got_items)

def test_scan_projection_expression_path(test_table):
    # This test is similar to test_query_projection_expression_path above,
    # but uses a scan instead of a query. The scan will generate unrelated
    # partitions created by other tests (hopefully not too many...) that we
    # need to ignore. We also need to ask for "p" too, so we can filter by it.
    p = random_string()
    items = [{'p': p, 'c': str(i), 'a': {'x': str(i*10), 'y': 'hi'}, 'b': 'hello' } for i in range(10)]
    with test_table.batch_writer() as batch:
        for item in items:
            batch.put_item(item)
    got_items = [ x for x in full_scan(test_table, ProjectionExpression="p, a.x") if x['p'] == p]
    expected_items = [{'p': p, 'a': {'x': x['a']['x']}} for x in items]
    assert multiset(expected_items) == multiset(got_items)

# BatchGetItem also supports ProjectionExpression, let's test that it
# applies to all items, and that it correctly suports document paths as well.
def test_batch_get_item_projection_expression_path(test_table_s):
    items = [{'p': random_string(), 'a': {'b': random_string(), 'x': 'hi'}, 'c': random_string()} for i in range(3)]
    with test_table_s.batch_writer() as batch:
        for item in items:
            batch.put_item(item)
    got_items = test_table_s.meta.client.batch_get_item(
        RequestItems = {test_table_s.name: {
            'Keys': [{'p': item['p']} for item in items],
            'ProjectionExpression': 'a.b',
            'ConsistentRead': True}})['Responses'][test_table_s.name]
    expected_items = [{'a': {'b': item['a']['b']}} for item in items]
    assert multiset(got_items) == multiset(expected_items)

# It is not allowed to use both ProjectionExpression and its older cousin,
# AttributesToGet, together. If trying to do this, DynamoDB produces an error
# like "Can not use both expression and non-expression parameters in the same
# request: Non-expression parameters: {AttributesToGet} Expression
# parameters: {ProjectionExpression}
def test_projection_expression_and_attributes_to_get(test_table_s):
    p = random_string()
    test_table_s.put_item(Item={'p': p, 'a': 'hello', 'b': 'hi'})
    with pytest.raises(ClientError, match='ValidationException.*both'):
        test_table_s.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression='a', AttributesToGet=['b'])['Item']
    with pytest.raises(ClientError, match='ValidationException.*both'):
        full_scan(test_table_s,  ProjectionExpression='a', AttributesToGet=['a'])
    with pytest.raises(ClientError, match='ValidationException.*both'):
        full_query(test_table_s, KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}}, ProjectionExpression='a', AttributesToGet=['a'])

# above in test_projection_expression_toplevel_syntax among other things
# we noted how spurious entries in ExpressionAttributeNames, not needed
# the the ProjectionExpression, cause an error. Sometimes we have two
# expressions in the same request, for example, both a ProjectionExpression
# and a KeyConditionExpression. It's only an error if a name is not
# needed by both of these expressions
def test_projection_expression_and_key_condition_expression(test_table_s):
    p = random_string()
    test_table_s.put_item(Item={'p': p, 'a': 'hello', 'b': 'hi'})
    got_items = full_query(test_table_s,
        KeyConditionExpression='#name1 = :val1',
        ProjectionExpression='#name2',
        ExpressionAttributeNames={'#name1': 'p', '#name2': 'a'},
        ExpressionAttributeValues={':val1': p});
    assert got_items == [{'a': 'hello'}]

# Test whether the nesting depth of an a path in a projection expression
# is limited. If the implementation is done using recursion, it is goood
# practice to limit it and not crash the server. According to the DynamoDB
# documentation, DynamoDB supports nested attributes up to 32 levels deep:
# https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html#limits-attributes-nested-depth
# There is no reason why Alternator should not use exactly the same limit
# as is officially documented by DynamoDB.
def test_projection_expression_path_nesting_levels(test_table_s):
    p = random_string()
    # 32 nesting levels (including the top-level attribute) work
    test_table_s.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression='a'+('.b'*31))
    # 33 nesting levels do not. DynamoDB gives an error: "Invalid
    # ProjectionExpression: The document path has too many nesting levels;
    # nesting levels: 33".
    with pytest.raises(ClientError, match='ValidationException.*nesting levels'):
        test_table_s.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression='a'+('.b'*32))
