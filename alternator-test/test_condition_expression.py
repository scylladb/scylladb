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

# Tests for the ConditionExpression parameter

import pytest
from botocore.exceptions import ClientError
from util import random_string

# Test that ConditionExpression works as expected
@pytest.mark.xfail(reason="ConditionExpression not yet implemented")
def test_update_condition_expression(test_table_s):
    p = random_string()
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET b = :val1',
        ExpressionAttributeValues={':val1': 4})
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET b = :val1',
        ConditionExpression='b = :oldval',
        ExpressionAttributeValues={':val1': 6, ':oldval': 4})
    with pytest.raises(ClientError, match='ConditionalCheckFailedException.*'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET b = :val1',
            ConditionExpression='b = :oldval',
            ExpressionAttributeValues={':val1': 8, ':oldval': 4})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'b': 6}
