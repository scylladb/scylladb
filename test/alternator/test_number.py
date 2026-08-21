# Copyright 2020-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

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

import boto3.dynamodb.types
import pytest
from botocore.exceptions import ClientError

from test.alternator.util import random_string, client_no_transform

# Monkey-patch the boto3 library to stop doing its own error-checking on
# numbers. This works around a bug https://github.com/boto/boto3/issues/2500
# of incorrect checking of responses, and we also need to get boto3 to not do
# its own error checking of requests, to allow us to check the server's
# handling of such errors.
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

# Zero can be written as 0e126. Should this be allowed - since the number
# is zero, which is allowed - or forbidden because nominally the exponent
# is 126? In my opinion the former interpretation is the correct one,
# since the nominal exponent in the scientific notation input isn't what
# matters, but rather the actual magnitude: E.g., consider 0.1e126 is allowed
# despite having a nominal exponent 126 - because its actual magnitude is 126.
# At first glance, it appears that DynamoDB seems to follow the latter
# interpretation, and forbids 0e126 (and similar). Which sounds like a
# legitimate decision - except it is NOT followed consistently - while
# DynamoDB forbids 0e126, it allows 0.0e126! That is inconsistent, and I
# consider it a DynamoDB bug, so Alternator follows the first interpretation
# (both 0e126 and 0.0e126 are allowed), and I'm marking this test as a
# dynamodb_bug.
def test_number_magnitude_not_allowed_zero(test_table_s, dynamodb_bug):
    p = random_string()
    # 0e125 is allowed, obviously
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET a = :val',
        ExpressionAttributeValues={':val': Decimal("0e125")})
    # To cement our understanding of nominal exponent vs. actual magnitude,
    # confirm that 0.1e126 is allowed - it's actual magnitude is 125,
    # which is allowed.
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET a = :val',
        ExpressionAttributeValues={':val': Decimal("0.1e126")})
    # 0.0e126 is still just zero and has actual magnitude 0, despite the
    # nominal exponent 126, so is also allowed
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET a = :val',
        ExpressionAttributeValues={':val': Decimal("0.0e126")})
    # If 0.0e126 is allowed, obviously 0e126 should also be allowed, and
    # Alternator allows it as this test confirms - but DynamoDB doesn't and
    # this test fails here on DynamoDB. I consider this a DynamoDB bug,
    # hence the "dynamodb_bug" tag on this test.
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET a = :val',
        ExpressionAttributeValues={':val': Decimal("0e126")})
    # Verify that the 0e126 that we wrote above is really just a regular zero
    assert 0 == test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['a']
    # Similarly, 0e-131 should be allowed (it's, again, just zero), and
    # DynamoDB has a bug causing it to be forbidden.
    test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET a = :val',
        ExpressionAttributeValues={':val': Decimal("0e-131")})

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
        for s in ['3', '-7.1234', '-17e5', '-17.4E37', '+3', '.123', '0001.23', '1e+5']:
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

# Verify that Number RANGE (sort) keys use value-based comparison: different
# string representations of the same number ("1000" vs "1e3") are treated as
# the same sort key. A second PutItem with a different representation
# overwrites the first, and GetItem with any representation
# finds the item.
#
# Additionally, verify that the sort key is returned in canonical
# (normalized) form regardless of what representation was used to write it.
#
# This is consistent with DynamoDB behaviour. We use client_no_transform()
# to send exact string representations and avoid boto3's own Decimal
# normalization.
def test_number_range_key_representation(test_table_sn):
    p = random_string()
    with client_no_transform(test_table_sn.meta.client) as client:
        # Write an item with sort key "1000".
        client.put_item(TableName=test_table_sn.name,
            Item={'p': {'S': p}, 'c': {'N': '1000'}, 'v': {'S': 'first'}})
        # Sanity: reading with the same representation works.
        got = client.get_item(TableName=test_table_sn.name,
            Key={'p': {'S': p}, 'c': {'N': '1000'}},
            ConsistentRead=True)
        assert got['Item']['v']['S'] == 'first'
        # Reading with a different representation of the same value should
        # also find the item (RANGE key comparison is value-based).
        got2 = client.get_item(TableName=test_table_sn.name,
            Key={'p': {'S': p}, 'c': {'N': '1e3'}},
            ConsistentRead=True)
        assert got2['Item']['v']['S'] == 'first'
        # Overwrite with a different representation.
        client.put_item(TableName=test_table_sn.name,
            Item={'p': {'S': p}, 'c': {'N': '1e3'}, 'v': {'S': 'second'}})
        # There should be exactly one item in this partition (overwritten,
        # not a second item).
        result = client.query(TableName=test_table_sn.name,
            KeyConditionExpression='p = :p',
            ExpressionAttributeValues={':p': {'S': p}},
            ConsistentRead=True)
        assert result['Count'] == 1
        assert result['Items'][0]['v']['S'] == 'second'
        # All representations of the same value find the single item.
        # Includes exponent notation, trailing fractional zero, leading
        # zeros, and explicit plus sign.
        for n in ['1000', '1e3', '1E+3', '1000.0', '001000', '+1000']:
            got3 = client.get_item(TableName=test_table_sn.name,
                Key={'p': {'S': p}, 'c': {'N': n}},
                ConsistentRead=True)
            assert got3['Item']['v']['S'] == 'second'
            # The returned sort key should be in canonical form ("1000")
            # regardless of what representation was used for the lookup.
            assert got3['Item']['c']['N'] == '1000'

# Verify that Number HASH (partition) keys are normalized: different string
# representations of the same number ("1000" vs "1e3") should refer to the
# same item, just like in DynamoDB.  Also verify that the returned key is
# in canonical (normalized) form.
#
# DynamoDB normalizes Numbers on write, so "1e3" and "1000" are the same
# partition key. Alternator currently does NOT normalize — different
# representations produce different (scale, unscaled) byte pairs, different
# Murmur3 tokens, and end up in different partitions.
# Reproduces SCYLLADB-1575
@pytest.mark.xfail(reason="SCYLLADB-1575")
def test_number_hash_key_representation(test_table_n):
    with client_no_transform(test_table_n.meta.client) as client:
        # Write an item with HASH key "1000".
        client.put_item(TableName=test_table_n.name,
            Item={'p': {'N': '1000'}, 'v': {'S': 'first'}})
        # Sanity: reading with the same representation works.
        got = client.get_item(TableName=test_table_n.name,
            Key={'p': {'N': '1000'}},
            ConsistentRead=True)
        assert got['Item']['v']['S'] == 'first'
        # The returned key should be in canonical form.
        assert got['Item']['p']['N'] == '1000'
        # In DynamoDB, reading with a different representation of the
        # same number should find the same item. In Alternator, this
        # currently fails (SCYLLADB-1575: returns no item) because the
        # representations serialize to different bytes → different
        # tokens → different partitions.
        got2 = client.get_item(TableName=test_table_n.name,
            Key={'p': {'N': '1e3'}},
            ConsistentRead=True)
        assert 'Item' in got2
        assert got2['Item']['v']['S'] == 'first'
        # Even when looked up via '1e3', the returned key should be
        # in canonical form ('1000'), not the lookup representation.
        assert got2['Item']['p']['N'] == '1000'
        # Writing with a different representation should overwrite, not
        # create a second item.
        client.put_item(TableName=test_table_n.name,
            Item={'p': {'N': '1e3'}, 'v': {'S': 'second'}})
        got3 = client.get_item(TableName=test_table_n.name,
            Key={'p': {'N': '1000'}},
            ConsistentRead=True)
        assert got3['Item']['v']['S'] == 'second'
        got4 = client.get_item(TableName=test_table_n.name,
            Key={'p': {'N': '1e3'}},
            ConsistentRead=True)
        assert got4['Item']['v']['S'] == 'second'
        # All these representations should find the same item and
        # return the key in canonical form.
        for n in ['1000', '1e3', '1E+3', '1000.0', '001000', '+1000']:
            got5 = client.get_item(TableName=test_table_n.name,
                Key={'p': {'N': n}},
                ConsistentRead=True)
            assert got5['Item']['v']['S'] == 'second'
            assert got5['Item']['p']['N'] == '1000'

# DynamoDB normalizes Number values on write: it strips leading zeros,
# trailing fractional zeros, converts exponent notation to plain decimal
# form (within the representable range), and removes explicit plus signs.
# The following table lists (input_string, expected_canonical_output)
# pairs that document DynamoDB's exact normalization rules.
_NORMALIZATION_CASES = [
    # Basic integers — no change expected.
    ('1', '1'),
    ('123', '123'),
    ('-5', '-5'),
    ('0', '0'),
    ('10', '10'),
    ('100', '100'),
    ('1000', '1000'),

    # Leading zeros — stripped.
    ('007', '7'),
    ('001.23', '1.23'),
    ('00', '0'),

    # Explicit plus sign — stripped.
    ('+3', '3'),
    ('+0', '0'),
    ('+1.5', '1.5'),

    # Trailing fractional zeros — stripped.
    ('1.0', '1'),
    ('1.00', '1'),
    ('1.10', '1.1'),
    ('100.000', '100'),
    ('0.0', '0'),
    ('0.10', '0.1'),

    # Exponent notation — expanded to plain decimal.
    ('1e3', '1000'),
    ('1E3', '1000'),
    ('1e+3', '1000'),
    ('1E+3', '1000'),
    ('5e1', '50'),
    ('-3e2', '-300'),
    ('1.5e2', '150'),
    ('1.23e4', '12300'),

    # Negative exponent — expanded to plain decimal.
    ('1e-3', '0.001'),
    ('5e-1', '0.5'),
    ('123e-2', '1.23'),
    ('1.23e-1', '0.123'),

    # Exponent + trailing zeros — both normalized.
    ('1.0e3', '1000'),
    ('1.00e2', '100'),
    ('1.0e-1', '0.1'),

    # Negative zero — sign stripped.
    ('-0', '0'),
    ('-0.0', '0'),

    # Zero with exponent — simplified to '0'.
    ('0e5', '0'),
    ('0.0e3', '0'),
    ('0e-5', '0'),

    # Fractional without leading zero — leading zero added.
    ('.5', '0.5'),
    ('.123', '0.123'),
    ('-.5', '-0.5'),

    # Large magnitude within DynamoDB bounds.
    # The normalized form is always plain decimal, never scientific notation.
    ('1e20', '100000000000000000000'),
    ('9.9e10', '99000000000'),
    ('1e125', '1' + '0' * 125),

    # Small values — expanded to plain decimal.
    ('1e-20', '0.00000000000000000001'),

    # Values that are already in canonical form — no change.
    ('3.14159', '3.14159'),
    ('-273.15', '-273.15'),
    ('0.001', '0.001'),
]

# Verify that DynamoDB returns Number attribute values in canonical
# (normalized) form regardless of what string representation was used
# to write them. This test probes the exact normalization rules listed
# in _NORMALIZATION_CASES.
#
# Uses client_no_transform() to send exact Number strings and inspect
# the exact strings returned, bypassing boto3's own Decimal normalization.
def test_number_output_normalization(test_table_s):
    with client_no_transform(test_table_s.meta.client) as client:
        failures = []
        for i, (input_num, expected) in enumerate(_NORMALIZATION_CASES):
            key = {'S': random_string()}
            client.put_item(TableName=test_table_s.name,
                Item={'p': key, 'a': {'N': input_num}})
            got = client.get_item(TableName=test_table_s.name,
                Key={'p': key},
                ConsistentRead=True)
            actual = got['Item']['a']['N']
            if actual != expected:
                failures.append(
                    f'  {input_num!r}: expected {expected!r}, got {actual!r}')
        assert not failures, \
            'Number output normalization mismatches:\n' + '\n'.join(failures)

# DynamoDB normalizes Number values inside Number Sets (NS), so different
# string representations of the same number are treated as the same set
# element.
# Alternator stores NS elements as JSON strings and uses string
# comparison (rjson::single_value_comp on kStringType), so "1" and "1.0"
# are treated as different elements.
# This single test checks ADD (union), DELETE (difference), and
# ConditionExpression equality sequentially.
# Reproduces SCYLLADB-1575.
@pytest.mark.xfail(reason="SCYLLADB-1575")
def test_number_set_normalization(test_table_s):
    p = random_string()
    with client_no_transform(test_table_s.meta.client) as client:
        tn = test_table_s.name
        key = {'S': p}

        # --- ADD: adding a different representation of an existing element
        # should not increase the set size.
        client.put_item(TableName=tn,
            Item={'p': key, 'ns': {'NS': ['1', '2', '3']}})
        client.update_item(TableName=tn,
            Key={'p': key},
            UpdateExpression='ADD ns :v',
            ExpressionAttributeValues={':v': {'NS': ['1.0', '2.00']}})
        got = client.get_item(TableName=tn, Key={'p': key},
            ConsistentRead=True)['Item']
        ns = got['ns']['NS']
        # DynamoDB: still 3 elements (1.0 == 1, 2.00 == 2).
        # Alternator bug (SCYLLADB-1575): 5 elements ("1", "1.0", "2", "2.00", "3").
        assert len(ns) == 3

        # --- DELETE: removing by a different representation should work.
        client.put_item(TableName=tn,
            Item={'p': key, 'ns': {'NS': ['10.0', '20', '30']}})
        client.update_item(TableName=tn,
            Key={'p': key},
            UpdateExpression='DELETE ns :v',
            ExpressionAttributeValues={':v': {'NS': ['10', '2e1']}})
        got = client.get_item(TableName=tn, Key={'p': key},
            ConsistentRead=True)['Item']
        ns = got['ns']['NS']
        # DynamoDB: only "30" remains (10 == 10.0, 2e1 == 20).
        # Alternator bug (SCYLLADB-1575): still all 3 ("10.0", "20", "30").
        assert len(ns) == 1

        # --- ConditionExpression EQ: sets with same values but different
        # representations should be equal.
        client.put_item(TableName=tn,
            Item={'p': key, 'ns': {'NS': ['1', '2']}})
        # Condition: ns = {1.0, 2.00}  — should pass (same numbers).
        # Alternator bug (SCYLLADB-1575): fails because "1" != "1.0" in string comparison.
        client.update_item(TableName=tn,
            Key={'p': key},
            UpdateExpression='SET #x = :x',
            ConditionExpression='ns = :ns',
            ExpressionAttributeNames={'#x': 'flag'},
            ExpressionAttributeValues={
                ':x': {'S': 'passed'},
                ':ns': {'NS': ['1.0', '2.00']},
            })
        got = client.get_item(TableName=tn, Key={'p': key},
            ConsistentRead=True)['Item']
        assert got.get('flag', {}).get('S') == 'passed', \
            'ConditionExpression: NS equality with different representations failed'
