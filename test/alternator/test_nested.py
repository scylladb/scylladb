# Copyright 2019-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

# Test for operations on items with *nested* attributes.

from test.alternator.util import random_string

# Test that we can write a top-level attribute that is a nested document, and
# read it back correctly.
def test_nested_document_attribute_write(test_table_s):
    nested_value = {
        'a': 3,
        'b': {'c': 'hello', 'd': ['hi', 'there', {'x': 'y'}, '42']},
    }
    p = random_string()
    test_table_s.put_item(Item={'p': p, 'a': nested_value})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': nested_value}

# Test that if we have a top-level attribute that is a nested document (i.e.,
# a dictionary), updating this attribute will replace it entirely by a new
# nested document - not merge into the old content with the new content.
def test_nested_document_attribute_overwrite(test_table_s):
    p = random_string()
    test_table_s.put_item(Item={'p': p, 'a': {'b': 3, 'c': 4}, 'd': 5})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': {'b': 3, 'c': 4}, 'd': 5}
    test_table_s.update_item(Key={'p': p}, AttributeUpdates={'a': {'Value': {'c': 5}, 'Action': 'PUT'}})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': {'c': 5}, 'd': 5}

# Moreover, we can overwrite an entire nested document by, say, a string,
# and that's also fine.
def test_nested_document_attribute_overwrite_2(test_table_s):
    p = random_string()
    test_table_s.put_item(Item={'p': p, 'a': {'b': 3, 'c': 4}, 'd': 5})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': {'b': 3, 'c': 4}, 'd': 5}
    test_table_s.update_item(Key={'p': p}, AttributeUpdates={'a': {'Value': 'hi', 'Action': 'PUT'}})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': 'hi', 'd': 5}

# Verify that AttributeUpdates cannot be used to update a nested attribute -
# trying to use a dot in the name of the attribute, will just create one with
# an actual dot in its name.
def test_attribute_updates_dot(test_table_s):
    p = random_string()
    test_table_s.update_item(Key={'p': p}, AttributeUpdates={'a.b': {'Value': 3, 'Action': 'PUT'}})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a.b': 3}
