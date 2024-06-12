# Copyright 2020-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

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

import decimal
from decimal import Decimal

# Monkey-patch the boto3 library to stop doing its own error-checking on
# numbers. This works around a bug https://github.com/boto/boto3/issues/2500
# of incorrect checking of responses, and we also need to get boto3 to not do
# its own error checking of requests, to allow us to check the server's
# handling of such errors.
import boto3.dynamodb.types
import pytest
from botocore.exceptions import ClientError

from test.alternator.util import random_string, client_no_transform

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

# DynamoDB limits the magnitude of the numbers (the exponent can be between
# -130 and 125). If we neglect to limit the magnitude, it can allow a user
# to request an addition operation between two numbers of wildly different
# magnitudes, that requires an unlimited amount of memory and CPU time - i.e.,
# a DoS attack. The attacker can cause a std::bad_alloc, large allocations,
# and very long scheduler stall, all with a very short request.
# When we had issue #6794 we had to skip this test, because it took a very
# long time and/or crashes Scylla.
def test_number_magnitude_not_allowed_dos(test_table_s):
    p = random_string()
    # Python's "Decimal" type and the way it's used by the Boto3 library
    # has its own limitations, so we need to bypass them with the wrapper
    # client_no_transform(), and pass numbers directly to the protocol as
    # strings.
    a = "1.0"
    b = "1.0e100000000"
    with client_no_transform(test_table_s.meta.client) as client:
        with pytest.raises(ClientError, match='ValidationException.*overflow'):
            client.update_item(TableName=test_table_s.name,
                Key={'p': {'S': p}},
                UpdateExpression='SET x = :a + :b',
                ExpressionAttributeValues={':a': {'N': a}, ':b': {'N': b}})

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
def test_number_precision_not_allowed(test_table_s):
    p = random_string()
    for num in [Decimal("3.14159265358979323846264338327950288419"),
                Decimal("314159265358979323846.264338327950288419"),
                Decimal("314159265358979323846264338327950288419e30")]:
        with pytest.raises(ClientError, match='ValidationException.*significant'):
            test_table_s.update_item(Key={'p': p},
                UpdateExpression='SET a = :val',
                ExpressionAttributeValues={':val': num})

# The above tests checked the legal magnitudes and precisions of non-key
# columns, and the following tests do the same for a numeric key column.
# Because different code paths are involved for serializing and storing
# key and non-key columns, it's important to check this case as well.
def test_number_magnitude_key(test_table_sn):
    p = random_string()
    # Legal magnitudes are allowed:
    for num in [Decimal("1e10"), Decimal("1e100"), Decimal("1e125"),
                Decimal("9.99999999e125"), Decimal("1e-100"),
                Decimal("1e-130")]:
        for sign in [False, True]:
            if sign:
                num = -num
            x = random_string()
            test_table_sn.update_item(Key={'p': p, 'c': num},
                UpdateExpression='SET a = :val',
                ExpressionAttributeValues={':val': x})
            assert test_table_sn.get_item(Key={'p': p, 'c': num}, ConsistentRead=True)['Item']['a'] == x
    # Illegal magnitudes are not allowed:
    x = random_string()
    for num in [Decimal("1e126"), Decimal("11e125")]:
        for sign in [False, True]:
            if sign:
                num = -num
            with pytest.raises(ClientError, match='ValidationException.*overflow'):
                test_table_sn.update_item(Key={'p': p, 'c': num},
                    UpdateExpression='SET a = :val',
                    ExpressionAttributeValues={':val': x})
    for num in [Decimal("1e-131"), Decimal("0.9e-130")]:
        for sign in [False, True]:
            if sign:
                num = -num
            with pytest.raises(ClientError, match='ValidationException.*underflow'):
                test_table_sn.update_item(Key={'p': p, 'c': num},
                    UpdateExpression='SET a = :val',
                    ExpressionAttributeValues={':val': x})

def test_number_precision_key(test_table_sn):
    p = random_string()
    # Legal precision is allowed:
    for num in [Decimal("3.1415926535897932384626433832795028841"),
                Decimal("314159265358979323846.26433832795028841"),
                Decimal("31415926535897932384626433832795028841e30")]:
        x = random_string()
        test_table_sn.update_item(Key={'p': p, 'c': num},
            UpdateExpression='SET a = :val',
            ExpressionAttributeValues={':val': x})
        assert test_table_sn.get_item(Key={'p': p, 'c': num}, ConsistentRead=True)['Item']['a'] == x
    # Illegal precision is not allowed:
    x = random_string()
    for num in [Decimal("3.14159265358979323846264338327950288419"),
                Decimal("314159265358979323846.264338327950288419"),
                Decimal("314159265358979323846264338327950288419e30")]:
        with pytest.raises(ClientError, match='ValidationException.*significant'):
            test_table_sn.update_item(Key={'p': p, 'c': num},
                UpdateExpression='SET a = :val',
                ExpressionAttributeValues={':val': x})

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
    # Validate that the individual operands aren't too large - the only
    # problem was the sum
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET b = :val1 + :val2',
        ExpressionAttributeValues={':val1': Decimal("9e125"), ':val2': Decimal("9e124")})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['b'] == Decimal("9.9e125")

# Similarly, addition or subtraction can also result in unsupported precision
# and causing the update to fail: For example, 1e50 + 1 cannot be represented
# in 38 digits of precision.
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

# Test that invalid strings cannot be stored as numbers and produce the
# expected error. This includes random non-numeric strings (e.g., "dog"),
# various syntax errors, and also the strings "NaN" and "Infinity", which
# although may be legal numbers in other systems (including Python), are
# not supported by DynamoDB. Spurious spaces are also not allowed.
def test_invalid_numbers(test_table_s):
    p = random_string()
    # We cannot write this test using boto3's high-level API because it
    # reformats and validates the numeric parameter before sending it to
    # the server, but we can test this using the client_no_transform trick.
    # Note that client_no_transform, the number 3 should be passed as
    # {'N': '3'}.
    with client_no_transform(test_table_s.meta.client) as client:
        for s in ['NaN', 'Infinity', '-Infinity', '-NaN', 'dog', '-dog', ' 1', '1 ']:
            with pytest.raises(ClientError, match='ValidationException.*numeric'):
                client.update_item(TableName=test_table_s.name,
                    Key={'p': {'S': p}},
                    UpdateExpression='SET a = :val',
                    ExpressionAttributeValues={':val': {'N': s}})
        # As a sanity check, check that *allowed* numbers are fine:
        for s in ['3', '-7.1234', '-17e5', '-17.4E37', '+3', '.123', '0001.23', '1e+5', '0e1000']:
            client.update_item(TableName=test_table_s.name,
                Key={'p': {'S': p}},
                UpdateExpression='SET a = :val',
                ExpressionAttributeValues={':val': {'N': s}})

# In DynamoDB's JSON format, a number value is represented as map with key
# "N" and the value is a *string* containing the number. E.g., {"N": "123"}.
# Using a string instead of a number in the JSON is important to guarantee
# the full range of DynamoDB's floating point even if the JSON libraries
# do not understand them. But can a user use a number in the JSON anyway?
# E.g., would {"N": 123} work as a number value? It turns out that the
# answer is no - it doesn't work. Let's check that:
def test_number_in_json(test_table_s):
    # We must use client_no_transform() to build the JSON encoding
    # ourselves instead of boto3 doing it automatically for us.
    with client_no_transform(test_table_s.meta.client) as client:
        p = random_string()
        # Alternator reads numeric inputs in several code paths which may
        # handle errors differently, so let's verify several of them.
        # It turns out that all code paths call the same validate_value()
        # function, so result in the same error.
        with pytest.raises(ClientError, match='SerializationException'):
            client.update_item(TableName=test_table_s.name,
                Key={'p': {'S': p}},
                UpdateExpression='SET a = :val',
                # Note that we're passing a number 123 here, not a string
                # '123', and that is wrong.
                ExpressionAttributeValues={':val': {'N': 123}})
        with pytest.raises(ClientError, match='SerializationException'):
            client.update_item(TableName=test_table_s.name,
                Key={'p': {'S': p}},
                UpdateExpression='SET a = :vgood',
                ConditionExpression='a < :vbad',
                ExpressionAttributeValues={':vgood': {'N': '1'}, ':vbad': {'N': 123}})
