# Copyright 2020-present ScyllaDB
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

# Tests for the number type. Numbers in DynamoDB have an unusual definition -
# they are a floating-point type with 38 decimal digits of precision and
# decimal exponent in the range -130 to +125. The *decimal* definition allows
# this type to accurately represent integers (with magnitude up to the allowed
# exponent) or decimal fractions up to the supported precision.
# Because of this unusual definition, none of the C++ types can accurately
# hold DynamoDB numbers - and Alternator currently uses the arbitrary-
# precision "big_decimal" type to hold them.
#
# The tests here try to verify two things:
#   1. That Alternator's number type supports the full precision and magnitude
#      that DynamoDB's number type supports. We don't want to see precision
#      or magnitude lost when storing and retrieving numbers, or when doing
#      calculations on them.
#   2. That Alternator's number type does not have *better* precision or
#      magnitude than DynamoDB does. If it did, users may be tempted to rely
#      on that implementation detail.
#
# We have additional tests in other files that numbers can be stored,
# retrieved, calculated (add and subtract), and sorted (when a sort key
# is a number). The tests in this file focus just on the precision and
# magnitude that the number type can store.

import pytest
from botocore.exceptions import ClientError
from decimal import Decimal
from util import random_string

# Monkey-patch the boto3 library to stop doing its own error-checking on
# numbers. This works around a bug https://github.com/boto/boto3/issues/2500
# of incorrect checking of responses, and we also need to get boto3 to not do
# its own error checking of requests, to allow us to check the server's
# handling of such errors.
import boto3.dynamodb.types
import decimal
boto3.dynamodb.types.DYNAMODB_CONTEXT = decimal.Context(prec=100)

# Test that numbers of allowed magnitudes - between to 1e-130 and 1e125 -
# can be stored and successfully retrieved unchanged.
def test_number_magnitude_allowed(test_table_s):
    p = random_string()
    for num in [Decimal("1e10"), Decimal("1e100"), Decimal("1e125"),
                Decimal("9.99999999e125"), Decimal("1e-100"),
                Decimal("1e-130")]:
        for sign in [False, True]:
            if sign:
                num = -num
            test_table_s.update_item(Key={'p': p},
                UpdateExpression='SET a = :val',
                ExpressionAttributeValues={':val': num})
            assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['a'] == num

# Test that numbers of too big (or small) a magnitude cannot be stored.
@pytest.mark.xfail(reason="Number type allows too much magnitude and precision")
def test_number_magnitude_not_allowed(test_table_s):
    p = random_string()
    for num in [Decimal("1e126"), Decimal("11e125")]:
        with pytest.raises(ClientError, match='ValidationException.*overflow'):
            test_table_s.update_item(Key={'p': p},
                UpdateExpression='SET a = :val',
                ExpressionAttributeValues={':val': num})
    for num in [Decimal("1e-131"), Decimal("0.9e-130")]:
        print(num)
        with pytest.raises(ClientError, match='ValidationException.*underflow'):
            test_table_s.update_item(Key={'p': p},
                UpdateExpression='SET a = :val',
                ExpressionAttributeValues={':val': num})

# Check that numbers up to the specified precision (38 decimal digits) can
# be stored and retrieved unchanged.
def test_number_precision_allowed(test_table_s):
    p = random_string()
    for num in [Decimal("3.1415926535897932384626433832795028841"),
                Decimal("314159265358979323846.26433832795028841"),
                Decimal("31415926535897932384626433832795028841e30")]:
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET a = :val',
            ExpressionAttributeValues={':val': num})
        assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['a'] == num

# Check that numbers with more significant digits than supported (38 decimal
# digits) cannot be stored.
@pytest.mark.xfail(reason="Number type allows too much magnitude and precision")
def test_number_precision_not_allowed(test_table_s):
    p = random_string()
    for num in [Decimal("3.14159265358979323846264338327950288419"),
                Decimal("314159265358979323846.264338327950288419"),
                Decimal("314159265358979323846264338327950288419e30")]:
        with pytest.raises(ClientError, match='ValidationException.*significant'):
            test_table_s.update_item(Key={'p': p},
                UpdateExpression='SET a = :val',
                ExpressionAttributeValues={':val': num})

# While most of the Alternator code just saves high-precision numbers
# unchanged, the "+" and "-" operations need to calculate with them, and
# we should check the calculation isn't done with some lower-precision
# representation, e.g., double
def test_update_expression_plus_precision(test_table_s):
    p = random_string()
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET b = :val1 + :val2',
        ExpressionAttributeValues={':val1': Decimal("1"), ':val2': Decimal("10000000000000000000000")})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'b': Decimal("10000000000000000000001")}
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET b = :val2 - :val1',
        ExpressionAttributeValues={':val1': Decimal("1"), ':val2': Decimal("10000000000000000000000")})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'b': Decimal("9999999999999999999999")}

# Some additions or subtractions can result in overflow to the allowed range,
# causing the update to fail: 9e125 + 9e125 = 1.8e126 which overflows.
@pytest.mark.xfail(reason="Number type allows too much magnitude and precision")
def test_update_expression_plus_overflow(test_table_s):
    p = random_string()
    with pytest.raises(ClientError, match='ValidationException.*overflow'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET b = :val1 + :val2',
            ExpressionAttributeValues={':val1': Decimal("9e125"), ':val2': Decimal("9e125")})
    with pytest.raises(ClientError, match='ValidationException.*overflow'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET b = :val1 - :val2',
            ExpressionAttributeValues={':val1': Decimal("9e125"), ':val2': Decimal("-9e125")})

# Similarly, addition or subtraction can also result in unsupported precision
# and causing the update to fail: For example, 1e50 + 1 cannot be represented
# in 38 digits of precision.
@pytest.mark.xfail(reason="Number type allows too much magnitude and precision")
def test_update_expression_plus_imprecise(test_table_s):
    p = random_string()
    # Strangely, DynamoDB says that the error is: "Number overflow. Attempting
    # to store a number with magnitude larger than supported range". This is
    # clearly the wrong error message...
    with pytest.raises(ClientError, match='ValidationException.*number'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET b = :val1 - :val2',
            ExpressionAttributeValues={':val1': Decimal("1e50"), ':val2': Decimal("1")})
    with pytest.raises(ClientError, match='ValidationException.*number'):
        test_table_s.update_item(Key={'p': p},
            UpdateExpression='SET b = :val1 + :val2',
            ExpressionAttributeValues={':val1': Decimal("1e50"), ':val2': Decimal("1")})
