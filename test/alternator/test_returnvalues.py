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

# Tests for the ReturnValues parameter for the different update operations
# (PutItem, UpdateItem, DeleteItem).

import pytest
from botocore.exceptions import ClientError
from util import random_string

# Test trivial support for the ReturnValues parameter in PutItem, UpdateItem
# and DeleteItem - test that "NONE" works (and changes nothing), while a
# completely unsupported value gives an error.
# This test is useful to check that before the ReturnValues parameter is fully
# implemented, it returns an error when a still-unsupported ReturnValues
# option is attempted in the request - instead of simply being ignored.
def test_trivial_returnvalues(test_table_s):
    # PutItem:
    p = random_string()
    test_table_s.put_item(Item={'p': p, 'a': 'hi'})
    ret=test_table_s.put_item(Item={'p': p, 'a': 'hello'}, ReturnValues='NONE')
    assert not 'Attributes' in ret
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.put_item(Item={'p': p, 'a': 'hello'}, ReturnValues='DOG')
    # UpdateItem:
    p = random_string()
    test_table_s.put_item(Item={'p': p, 'a': 'hi', 'b': 'dog'})
    ret=test_table_s.update_item(Key={'p': p}, ReturnValues='NONE',
        UpdateExpression='SET b = :val',
        ExpressionAttributeValues={':val': 'cat'})
    assert not 'Attributes' in ret
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p}, ReturnValues='DOG',
            UpdateExpression='SET a = a + :val',
            ExpressionAttributeValues={':val': 1})
    # DeleteItem:
    p = random_string()
    test_table_s.put_item(Item={'p': p, 'a': 'hi'})
    ret=test_table_s.delete_item(Key={'p': p}, ReturnValues='NONE')
    assert not 'Attributes' in ret
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.delete_item(Key={'p': p}, ReturnValues='DOG')

# Test the ReturnValues parameter on a PutItem operation. Only two settings
# are supported for this parameter for this operation: NONE (the default)
# and ALL_OLD.
def test_put_item_returnvalues(test_table_s):
    # By default, the previous value of an item is not returned:
    p = random_string()
    test_table_s.put_item(Item={'p': p, 'a': 'hi'})
    ret=test_table_s.put_item(Item={'p': p, 'a': 'hello'})
    assert not 'Attributes' in ret
    # Using ReturnValues=NONE is the same:
    p = random_string()
    test_table_s.put_item(Item={'p': p, 'a': 'hi'})
    ret=test_table_s.put_item(Item={'p': p, 'a': 'hello'}, ReturnValues='NONE')
    assert not 'Attributes' in ret
    # With ReturnValues=ALL_OLD, the old value of the item is returned
    # in an "Attributes" attribute:
    p = random_string()
    test_table_s.put_item(Item={'p': p, 'a': 'hi'})
    ret=test_table_s.put_item(Item={'p': p, 'a': 'hello'}, ReturnValues='ALL_OLD')
    assert ret['Attributes'] == {'p': p, 'a': 'hi'}
    # If the item does not previously exist, "Attributes" is not returned
    # at all:
    p = random_string()
    ret=test_table_s.put_item(Item={'p': p, 'a': 'hello'}, ReturnValues='ALL_OLD')
    assert not 'Attributes' in ret
    # Other ReturnValue options - UPDATED_OLD, ALL_NEW, UPDATED_NEW,
    # are supported by other operations but not by PutItem:
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.put_item(Item={'p': p, 'a': 'hello'}, ReturnValues='UPDATED_OLD')
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.put_item(Item={'p': p, 'a': 'hello'}, ReturnValues='ALL_NEW')
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.put_item(Item={'p': p, 'a': 'hello'}, ReturnValues='UPDATED_NEW')
    # Also, obviously, a non-supported setting "DOG" also returns in error:
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.put_item(Item={'p': p, 'a': 'hello'}, ReturnValues='DOG')
    # The ReturnValues value is case sensitive, so while "NONE" is supported
    # (and tested above), "none" isn't:
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.put_item(Item={'p': p, 'a': 'hello'}, ReturnValues='none')

# Test the ReturnValues parameter on a DeleteItem operation. Only two settings
# are supported for this parameter for this operation: NONE (the default)
# and ALL_OLD.
def test_delete_item_returnvalues(test_table_s):
    # By default, the previous value of an item is not returned:
    p = random_string()
    test_table_s.put_item(Item={'p': p, 'a': 'hi'})
    ret=test_table_s.delete_item(Key={'p': p})
    assert not 'Attributes' in ret
    # Using ReturnValues=NONE is the same:
    p = random_string()
    test_table_s.put_item(Item={'p': p, 'a': 'hi'})
    ret=test_table_s.delete_item(Key={'p': p}, ReturnValues='NONE')
    assert not 'Attributes' in ret
    # With ReturnValues=ALL_OLD, the old value of the item is returned
    # in an "Attributes" attribute:
    p = random_string()
    test_table_s.put_item(Item={'p': p, 'a': 'hi'})
    ret=test_table_s.delete_item(Key={'p': p}, ReturnValues='ALL_OLD')
    assert ret['Attributes'] == {'p': p, 'a': 'hi'}
    # If the item does not previously exist, "Attributes" is not returned
    # at all:
    p = random_string()
    ret=test_table_s.delete_item(Key={'p': p}, ReturnValues='ALL_OLD')
    assert not 'Attributes' in ret
    # Other ReturnValue options - UPDATED_OLD, ALL_NEW, UPDATED_NEW,
    # are supported by other operations but not by PutItem:
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.delete_item(Key={'p': p}, ReturnValues='UPDATE_OLD')
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.delete_item(Key={'p': p}, ReturnValues='ALL_NEW')
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.delete_item(Key={'p': p}, ReturnValues='UPDATE_NEW')
    # Also, obviously, a non-supported setting "DOG" also returns in error:
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.delete_item(Key={'p': p}, ReturnValues='DOG')
    # The ReturnValues value is case sensitive, so while "NONE" is supported
    # (and tested above), "none" isn't:
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.delete_item(Key={'p': p}, ReturnValues='none')

# Test the ReturnValues parameter on a UpdateItem operation. All five
# settings are supported for this parameter for this operation: NONE
# (the default), ALL_OLD, UPDATED_OLD, ALL_NEW and UPDATED_NEW.
# We test them in separate tests to allow for this feature to be
# implemented incrementally.

def test_update_item_returnvalues_none(test_table_s):
    # By default, the previous value of an item is not returned:
    p = random_string()
    test_table_s.put_item(Item={'p': p, 'a': 'hi', 'b': 'dog'})
    ret=test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET b = :val',
        ExpressionAttributeValues={':val': 'cat'})
    assert not 'Attributes' in ret

    # Using ReturnValues=NONE is the same:
    p = random_string()
    test_table_s.put_item(Item={'p': p, 'a': 'hi', 'b': 'dog'})
    ret=test_table_s.update_item(Key={'p': p}, ReturnValues='NONE',
        UpdateExpression='SET b = :val',
        ExpressionAttributeValues={':val': 'cat'})
    assert not 'Attributes' in ret

    # The ReturnValues value is case sensitive, so while "NONE" is supported
    # (and tested above), "none" isn't:
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p}, ReturnValues='none',
            UpdateExpression='SET a = :val',
            ExpressionAttributeValues={':val': 1})

    # A non-supported setting "DOG" also returns in error:
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p}, ReturnValues='DOG',
            UpdateExpression='SET a = :val',
            ExpressionAttributeValues={':val': 1})

def test_update_item_returnvalues_all_old(test_table_s):
    # With ReturnValues=ALL_OLD, the entire old value of the item (even
    # attributes we did not modify) is returned in an "Attributes" attribute:
    p = random_string()
    test_table_s.put_item(Item={'p': p, 'a': 'hi', 'b': 'dog'})
    ret=test_table_s.update_item(Key={'p': p}, ReturnValues='ALL_OLD',
        UpdateExpression='SET b = :val',
        ExpressionAttributeValues={':val': 'cat'})
    assert ret['Attributes'] == {'p': p, 'a': 'hi', 'b': 'dog'}

    # If the item does not previously exist, "Attributes" is not returned
    # at all:
    p = random_string()
    ret=test_table_s.update_item(Key={'p': p}, ReturnValues='ALL_OLD',
        UpdateExpression='SET b = :val',
        ExpressionAttributeValues={':val': 'cat'})
    assert not 'Attributes' in ret

def test_update_item_returnvalues_updated_old(test_table_s):
    # With ReturnValues=UPDATED_OLD, only the overwritten attributes of the
    # old item are returned in an "Attributes" attribute:
    p = random_string()
    test_table_s.put_item(Item={'p': p, 'a': 'hi', 'b': 'dog'})
    ret=test_table_s.update_item(Key={'p': p}, ReturnValues='UPDATED_OLD',
        UpdateExpression='SET b = :val, c = :val2',
        ExpressionAttributeValues={':val': 'cat', ':val2': 'hello'})
    assert ret['Attributes'] == {'b': 'dog'}

    # Even if an update overwrites an attribute by the same value again,
    # this is considered an update, and the old value (identical to the
    # new one) is returned:
    ret=test_table_s.update_item(Key={'p': p}, ReturnValues='UPDATED_OLD',
        UpdateExpression='SET b = :val',
        ExpressionAttributeValues={':val': 'cat'})
    assert ret['Attributes'] == {'b': 'cat'}

    # Deleting an attribute also counts as overwriting it, of course:
    ret=test_table_s.update_item(Key={'p': p}, ReturnValues='UPDATED_OLD',
        UpdateExpression='REMOVE b')
    assert ret['Attributes'] == {'b': 'cat'}

    # If we write to an attribute that didn't exist before, we do not
    # get Attributes at all:
    ret=test_table_s.update_item(Key={'p': p}, ReturnValues='UPDATED_OLD',
        UpdateExpression='SET b = :val',
        ExpressionAttributeValues={':val': 'cat'})
    assert not 'Attributes' in ret

    # However, if we write to two attributes, one which previously existed
    # and one didn't, we get back only the one which previously existed:
    ret=test_table_s.update_item(Key={'p': p}, ReturnValues='UPDATED_OLD',
        UpdateExpression='SET b = :val, d = :val2',
        ExpressionAttributeValues={':val': 'dog', ':val2': 'lion'})
    assert ret['Attributes'] == {'b': 'cat'}

    # Of course if we write to two attributes which previously existed,
    # we get both of them back
    ret=test_table_s.update_item(Key={'p': p}, ReturnValues='UPDATED_OLD',
        UpdateExpression='SET b = :val, d = :val2',
        ExpressionAttributeValues={':val': 'cat', ':val2': 'tiger'})
    assert ret['Attributes'] == {'b': 'dog', 'd': 'lion'}

    # If we write absolutely nothing (the only way to do this is with the
    # old AttributeUpdates syntax), we don't get Attributes back.
    ret=test_table_s.update_item(Key={'p': p}, ReturnValues='UPDATED_OLD',
        AttributeUpdates={})
    assert not 'Attributes' in ret

def test_update_item_returnvalues_all_new(test_table_s):
    # With ReturnValues=ALL_NEW, the entire new value of the item (including
    # old attributes we did not modify) is returned:
    p = random_string()
    test_table_s.put_item(Item={'p': p, 'a': 'hi', 'b': 'dog'})
    ret=test_table_s.update_item(Key={'p': p}, ReturnValues='ALL_NEW',
        UpdateExpression='SET b = :val',
        ExpressionAttributeValues={':val': 'cat'})
    assert ret['Attributes'] == {'p': p, 'a': 'hi', 'b': 'cat'}

    # Verify that if a column is deleted, it is *not* returned:
    ret=test_table_s.update_item(Key={'p': p}, ReturnValues='ALL_NEW',
        UpdateExpression='REMOVE b')
    assert ret['Attributes'] == {'p': p, 'a': 'hi'}

    # If the item did not previously exist, that's still fine, we get the
    # new value of the item:
    p = random_string()
    ret=test_table_s.update_item(Key={'p': p}, ReturnValues='ALL_NEW',
        UpdateExpression='SET b = :val',
        ExpressionAttributeValues={':val': 'cat'})
    assert ret['Attributes'] == {'p': p, 'b': 'cat'}

    # A more interesting question is what happens if the item did not
    # previously exist, and all the update does is *delete* an attribute,
    # which never existed. In this case, the item is not created (see also
    # test_update_item_non_existent()), and "Attributes" is not returned,
    p = random_string()
    ret=test_table_s.update_item(Key={'p': p}, ReturnValues='ALL_NEW',
        UpdateExpression='REMOVE b')
    assert not 'Attributes' in ret
    ret=test_table_s.get_item(Key={'p': p}, ConsistentRead=True)
    assert not 'Item' in ret

    # If we write absolutely nothing (the only way to do this is with the
    # old AttributeUpdates syntax), we get an empty item (just the key)
    # if it didn't yet exist (and now it does), or the old item if it did.
    ret=test_table_s.update_item(Key={'p': p}, ReturnValues='ALL_NEW',
        AttributeUpdates={})
    assert ret['Attributes'] == {'p': p}
    test_table_s.put_item(Item={'p': p, 'a': 'hi'})
    ret=test_table_s.update_item(Key={'p': p}, ReturnValues='ALL_NEW',
        AttributeUpdates={})
    assert ret['Attributes'] == {'p': p, 'a': 'hi'}

def test_update_item_returnvalues_updated_new(test_table_s):
    # With ReturnValues=UPDATED_NEW, only the new value of the updated
    # attributes are returned. Note that "updated attributes" means
    # the newly set attributes - it doesn't require that these attributes
    # have any previous values.
    p = random_string()
    test_table_s.put_item(Item={'p': p, 'a': 'hi', 'b': 'dog'})
    ret=test_table_s.update_item(Key={'p': p}, ReturnValues='UPDATED_NEW',
        UpdateExpression='SET b = :val, c = :val2',
        ExpressionAttributeValues={':val': 'cat', ':val2': 'hello'})
    assert ret['Attributes'] == {'b': 'cat', 'c': 'hello'}

    # Deleting an attribute also counts as overwriting it, but the deleted
    # column is not returned in the response - so it's empty in this case.
    ret=test_table_s.update_item(Key={'p': p}, ReturnValues='UPDATED_NEW',
        UpdateExpression='REMOVE b')
    assert not 'Attributes' in ret

    # Verify If we write to multiple attributes, we get them all back,
    # regardless of whether they previously existed or not (and again,
    # deleted columns aren't returned):
    ret=test_table_s.update_item(Key={'p': p}, ReturnValues='UPDATED_NEW',
        UpdateExpression='SET a = :val, b = :val2 REMOVE c',
        ExpressionAttributeValues={':val': 'dog', ':val2': 'tiger'})
    assert ret['Attributes'] == {'a': 'dog', 'b': 'tiger'}

    # In the above examples, UPDATED_NEW is not useful because it just
    # returns the new values we already know from the request... UPDATED_NEW
    # becomes more useful in read-modify-write operations:
    p = random_string()
    test_table_s.put_item(Item={'p': p, 'a': 1})
    ret=test_table_s.update_item(Key={'p': p}, ReturnValues='UPDATED_NEW',
        UpdateExpression='SET a = a + :val',
        ExpressionAttributeValues={':val': 1})
    assert ret['Attributes'] == {'a': 2}

    # UPDATED_NEW only returns non-key attributes. Even if the operation
    # caused a new item to be created, the new key attributes aren't
    # returned.
    p = random_string()
    ret=test_table_s.update_item(Key={'p': p}, ReturnValues='UPDATED_NEW',
        UpdateExpression='SET a = :val',
        ExpressionAttributeValues={':val': 1})
    assert ret['Attributes'] == {'a': 1}

    # If we write absolutely nothing (the only way to do this is with the
    # old AttributeUpdates syntax), we don't get back any Attributes.
    # Not even if the item didn't previously exist and this write caused
    # it to be created.
    p = random_string()
    ret=test_table_s.update_item(Key={'p': p}, ReturnValues='UPDATED_NEW',
        AttributeUpdates={})
    assert not 'Attributes' in ret
    test_table_s.put_item(Item={'p': p, 'a': 'hi'})
    ret=test_table_s.update_item(Key={'p': p}, ReturnValues='UPDATED_NEW',
        AttributeUpdates={})
    assert not 'Attributes' in ret

# Test the ReturnValues from an UpdateItem directly modifying a *nested*
# attribute, in the relevant ReturnValue modes:
def test_update_item_returnvalues_nested(test_table_s):
    p = random_string()
    test_table_s.put_item(Item={'p': p, 'a': {'b': 'dog', 'c': [1, 2, 3]}, 'd': 'cat'})
    ret=test_table_s.update_item(Key={'p': p}, ReturnValues='ALL_OLD',
        UpdateExpression='SET a.b = :val',
        ExpressionAttributeValues={':val': 'hi'})
    assert ret['Attributes'] == {'p': p, 'a': {'b': 'dog', 'c': [1, 2, 3]}, 'd': 'cat'}
    # UPDATED_OLD and UPDATED_NEW return *only* the nested attribute, not
    # the entire top-level attribute. It still needs to return it nested
    # within its the appropriate hierarchy, but not containing the entire
    # top-level attribute.
    ret=test_table_s.update_item(Key={'p': p}, ReturnValues='UPDATED_OLD',
        UpdateExpression='SET a.b = :val',
        ExpressionAttributeValues={':val': 'yo'})
    assert ret['Attributes'] == {'a': {'b': 'hi'}}
    ret=test_table_s.update_item(Key={'p': p}, ReturnValues='UPDATED_OLD',
        UpdateExpression='SET a.c[1] = :val',
        ExpressionAttributeValues={':val': 7})
    assert ret['Attributes'] == {'a': {'c': [2]}}
    ret=test_table_s.update_item(Key={'p': p}, ReturnValues='UPDATED_OLD',
        UpdateExpression='SET a.c[1] = :val, a.b = :val2',
        ExpressionAttributeValues={':val': 8, ':val2': 'dog'})
    assert ret['Attributes'] == {'a': {'b': 'yo', 'c': [7]}}
    ret=test_table_s.update_item(Key={'p': p}, ReturnValues='UPDATED_NEW',
        UpdateExpression='SET a.b = :val',
        ExpressionAttributeValues={':val': 'hello'})
    assert ret['Attributes'] == {'a': {'b': 'hello'}}
    ret=test_table_s.update_item(Key={'p': p}, ReturnValues='UPDATED_NEW',
        UpdateExpression='SET a.c[1] = :val',
        ExpressionAttributeValues={':val': 4})
    assert ret['Attributes'] == {'a': {'c': [4]}}
    ret=test_table_s.update_item(Key={'p': p}, ReturnValues='UPDATED_NEW',
        UpdateExpression='SET a.c[1] = :val, a.b = :val2',
        ExpressionAttributeValues={':val': 3, ':val2': 'dog'})
    assert ret['Attributes'] == {'a': {'b': 'dog', 'c': [3]}}
    # Although ALL_NEW mode returns the entire item and doesn't need to
    # know how to project a nested attribute, we still need to check that
    # it can return the post-update value of the attribute within the entire
    # item.
    ret=test_table_s.update_item(Key={'p': p}, ReturnValues='ALL_NEW',
        UpdateExpression='SET a.b = :val',
        ExpressionAttributeValues={':val': 'hi'})
    assert ret['Attributes'] == {'p': p, 'a': {'b': 'hi', 'c': [1, 3, 3]}, 'd': 'cat' }
    ret=test_table_s.update_item(Key={'p': p}, ReturnValues='ALL_NEW',
        UpdateExpression='SET a.c[1] = :val',
        ExpressionAttributeValues={':val': 4})
    assert ret['Attributes'] == {'p': p, 'a': {'b': 'hi', 'c': [1, 4, 3]}, 'd': 'cat' }
    ret=test_table_s.update_item(Key={'p': p}, ReturnValues='ALL_NEW',
        UpdateExpression='SET a.c[1] = :val, a.b = :val2',
        ExpressionAttributeValues={':val': 3, ':val2': 'dog'})
    assert ret['Attributes'] == {'p': p, 'a': {'b': 'dog', 'c': [1, 3, 3]}, 'd': 'cat' }
    # Test with REMOVE, and one of them doing nothing (so shouldn't be in UPDATED_OLD)
    ret=test_table_s.update_item(Key={'p': p}, ReturnValues='UPDATED_OLD',
        UpdateExpression='REMOVE a.c[1], a.c[3]')
    assert ret['Attributes'] == {'a': {'c': [3]}}  # a.c[3] did not exist
    # When adding a new sub-attribute, UPDATED_OLD does not return anything,
    # but UPDATED_NEW does:
    ret=test_table_s.update_item(Key={'p': p}, ReturnValues='UPDATED_OLD',
        UpdateExpression='SET a.x1 = :val',
        ExpressionAttributeValues={':val': 8})
    assert not 'Attributes' in ret
    ret=test_table_s.update_item(Key={'p': p}, ReturnValues='UPDATED_NEW',
        UpdateExpression='SET a.x2 = :val',
        ExpressionAttributeValues={':val': 8})
    assert ret['Attributes'] == {'a': {'x2': 8}}
    # Nevertheless, there is one strange exception - although setting an array
    # element *beyond* its end (e.g., a.c[100]) does add a new array item, it
    # is *not* returned by UPDATED_NEW. I am not sure DynamoDB did this
    # deliberately (is it a bug or a feature?), but it simplifies our
    # implementation as well: after setting a.c[100], a.c[100] is not actually
    # set (instead, maybe a.c[4] was set) so UPDATED_NEW returning a.c[100]
    # returns nothing.
    ret=test_table_s.update_item(Key={'p': p}, ReturnValues='UPDATED_NEW',
        UpdateExpression='SET a.c[100] = :val',
        ExpressionAttributeValues={':val': 70})
    assert not 'Attributes' in ret
    # When removing an item, it shouldn't appear in UPDATED_NEW. Again there
    # a strange exception - which I'm not sure if we should consider it a
    # DynamoDB bug or feature - but it simplifies our own implementation as
    # well: if we remove a.c[1], the new item *will* have a new a.c[1] (the
    # previous a.c[2] is shifted back), so this value is returned. This is
    # very odd, but this is what DynamoDB does, so we should too...
    ret=test_table_s.update_item(Key={'p': p}, ReturnValues='UPDATED_NEW',
        UpdateExpression='REMOVE a.c[1]')
    assert ret['Attributes'] == {'a': {'c': [70]}}
