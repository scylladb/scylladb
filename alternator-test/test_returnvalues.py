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
@pytest.mark.xfail(reason="ReturnValues not supported")
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
@pytest.mark.xfail(reason="ReturnValues not supported")
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
@pytest.mark.xfail(reason="ReturnValues not supported")
def test_update_item_returnvalues(test_table_s):
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

    # With ReturnValues=ALL_OLD, the entire old value of the item (even
    # attributes we did not modify) is returned in an "Attributes" attribute:
    p = random_string()
    test_table_s.put_item(Item={'p': p, 'a': 'hi', 'b': 'dog'})
    ret=test_table_s.update_item(Key={'p': p}, ReturnValues='ALL_OLD',
        UpdateExpression='SET b = :val',
        ExpressionAttributeValues={':val': 'cat'})
    assert ret['Attributes'] == {'p': p, 'a': 'hi', 'b': 'dog'}

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

    # With ReturnValues=ALL_NEW, the entire new value of the item (including
    # old attributes we did not modify) is returned:
    p = random_string()
    test_table_s.put_item(Item={'p': p, 'a': 'hi', 'b': 'dog'})
    ret=test_table_s.update_item(Key={'p': p}, ReturnValues='ALL_NEW',
        UpdateExpression='SET b = :val',
        ExpressionAttributeValues={':val': 'cat'})
    assert ret['Attributes'] == {'p': p, 'a': 'hi', 'b': 'cat'}

    # With ReturnValues=UPDATED_NEW, only the new value of the updated
    # attributes are returned. Note that "updated attributes" means
    # the newly set attributes - it doesn't require that these attributes
    # have any previous values
    p = random_string()
    test_table_s.put_item(Item={'p': p, 'a': 'hi', 'b': 'dog'})
    ret=test_table_s.update_item(Key={'p': p}, ReturnValues='UPDATED_NEW',
        UpdateExpression='SET b = :val, c = :val2',
        ExpressionAttributeValues={':val': 'cat', ':val2': 'hello'})
    assert ret['Attributes'] == {'b': 'cat', 'c': 'hello'}
    # Deleting an attribute also counts as overwriting it, but the delete
    # column is not returned in the response - so it's empty in this case.
    ret=test_table_s.update_item(Key={'p': p}, ReturnValues='UPDATED_NEW',
        UpdateExpression='REMOVE b')
    assert not 'Attributes' in ret
    # In the above examples, UPDATED_NEW is not useful because it just
    # returns the new values we already know from the request... UPDATED_NEW
    # becomes more useful in read-modify-write operations:
    p = random_string()
    test_table_s.put_item(Item={'p': p, 'a': 1})
    ret=test_table_s.update_item(Key={'p': p}, ReturnValues='UPDATED_NEW',
        UpdateExpression='SET a = a + :val',
        ExpressionAttributeValues={':val': 1})
    assert ret['Attributes'] == {'a': 2}

    # A non-supported setting "DOG" also returns in error:
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p}, ReturnValues='DOG',
            UpdateExpression='SET a = a + :val',
            ExpressionAttributeValues={':val': 1})
    # The ReturnValues value is case sensitive, so while "NONE" is supported
    # (and tested above), "none" isn't:
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p}, ReturnValues='none',
            UpdateExpression='SET a = a + :val',
            ExpressionAttributeValues={':val': 1})
