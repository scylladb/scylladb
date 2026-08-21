# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# Tests for the new vector search feature added to DynamoDB in August 2026.
#
# In Alternator, for this feature to work an vector store must be running
# and configured in Scylla - use test/alternator/run with the "--vs" option
# to run a vector store alongside Scylla. Tests that need a fully functioning
# vector store (as opposed to simple syntax checking) are marked with the
# needs_vector_store fixture, and will be skipped if the vector store is not
# running.

import pytest
import time
import json
import struct
import decimal
from packaging.version import Version
from decimal import Decimal
from contextlib import contextmanager
from functools import cache

from botocore.exceptions import ClientError
import boto3.dynamodb.types
import botocore

from test.pylib.skip_types import skip_env
from .util import random_string, new_test_table, unique_table_name, scylla_config_read, scylla_config_write, client_no_transform, is_aws, manual_request
from .test_streams import wait_for_status_active

# Support for the new DynamoDB vector search API was added in Botocore 1.43.64
# All tests in this file cannot run with older versions of the SDK, and will
# be skipped in that case.
@pytest.fixture(scope='function', autouse=True)
def all_tests_need_vector_search_in_botocore(dynamodb):
    if (Version(botocore.__version__) < Version('1.43.64')):
        skip_env("Botocore version 1.43.64 or above required to run this test")

# Monkey-patch the boto3 library to stop doing its own error-checking on
# numbers. This works around a bug https://github.com/boto/boto3/issues/2500
# of incorrect checking of responses, and we also need to get boto3 to not do
# its own error checking of requests, to allow us to check the server's
# handling of such errors.
# This is needed at least for test_numeric_list_precision_range().
boto3.dynamodb.types.DYNAMODB_CONTEXT = decimal.Context(prec=100)

# Helper function to check a vector store is configured in Scylla
# with the --vector-store-primary-uri option. This can be done, for
# example, by running test/alternator/run with the option "--vs".
# This function needs some table as a parameter; calling it again
# for the same table will use a cached result.
# If the test is running on AWS DynamoDB, we always return true - the
# functionality equivalent to the vector store is always available there.
@cache
def vector_store_configured(table_vs):
    if is_aws(table_vs):
        return True
    # Issue a trial query to detect whether Scylla was started with a vector
    # store URI. If we get an error message "Vector Store is disabled", it
    # means the vector store is not configured. If we get any other error or
    # success - it means the vector store is configured (but might not be
    # ready yet - individual tests will use their own retry loops).
    try:
        table_vs.query(IndexName='vind',
            VectorSearch={'QueryVector': [0, 0, 0]},
            Limit=1)
    except ClientError as e:
        if 'Vector Store is disabled' in e.response['Error']['Message']:
            return False
    return True

# Fixture to skip a test if the vector store is not configured.
# It is assumed that if Scylla is configured to use the vector store, then
# the reverse is also true - the vector store is configured to use Scylla,
# so we can check the end-to-end functionality.
@pytest.fixture(scope="function")
def needs_vector_store(table_vs):
    if not vector_store_configured(table_vs):
        skip_env('Vector Store is not configured (run with --vs)')

# Simple test table with a vector index on a 3-dimensional vector column v
# Please note that because this is a shared table, tests that perform
# global queries on it, not filtering to a specific partition, may get
# results from other tests - so such tests will need to create their own
# table instead of using this shared one.
# If vector store is configured, we wait for the vector index to become
# active before yielding the table, so tests that use this fixture can
# read from it immediately.
@pytest.fixture(scope="module")
def table_vs(dynamodb):
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
            VectorIndexes=[
                {'IndexName': 'vind',
                 'VectorAttribute': {'AttributeName': 'v'},
                 'Dimensions': 3,
                 'DistanceFunction': 'COSINE',
                 'Projection': {'ProjectionType': 'KEYS_ONLY'}
                }
            ]) as table:
        if vector_store_configured(table):
            wait_for_vector_index_active(table, 'vind')
        yield table

# A simple test for the vector type. In the DynamoDB API, there is no special
# "vector" type, a vector is simply a list of a known size that contains only
# numbers. Alternator also has a more efficient vector type which is tested
# below, but here we check the regular number array, and this is just a
# trivial test that an array of numbers can be written and read (without
# trying anything with vector search).
def test_numeric_list_value(dynamodb, test_table_s):
    p = random_string()
    v = [Decimal("0"), Decimal("1.2"), Decimal("-2.3"), Decimal("1.2e10")]
    test_table_s.put_item(Item={'p': p, 'v': v})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['v'] == v

# One of the downsides of using a standard list of standard numbers to
# represent a vector is that DynamoDB keeps these numbers with high precision
# (38 decimal digits) which wastes storage and not really needed for vector
# search. Let's verify that indeed, high-precision numbers are stored in a
# list of numbers (we have similar tests for a single number in test_number.py).
def test_numeric_list_precision_range(test_table_s):
    p = random_string()
    v = [Decimal("3.1415926535897932384626433832795028841"),
         Decimal("9.99999999e125")]
    test_table_s.put_item(Item={'p': p, 'v': v})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['v'] == v

# Test CreateTable creating a new table with a basic vector index. This test
# doesn't check that the vector index actually works - we'll do this in
# separate tests below. It just tests that the new CreateTable parameter
# "VectorIndexes" isn't rejected or otherwise fails.
# This test doesn't cover all the different parameters inside VectorIndexes,
# we'll cover those in separate tests.
def test_createtable_vectorindexes(dynamodb):
    with new_test_table(dynamodb,
            KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
            AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }],
            VectorIndexes=[
                {   'IndexName': 'hello',
                    'VectorAttribute': {'AttributeName': 'v'},
                    'Dimensions': 4,
                    'DistanceFunction': 'COSINE',
                    'Projection': {'ProjectionType': 'KEYS_ONLY'},
                }]) as table:
        pass

# Test that in CreateTable's VectorIndexes, all of the following parameters
# are mandatory: IndexName, VectorAttribute, Dimensions, DistanceFunction
# and Projection. None of these have defaults, and if any one of these is
# missing, we get a ValidationException.
# Note: in new_dynamodb_session in conftest.py, we used
# parameter_validation=False by default, so boto3 doesn't do the validation
# of missing parameters for us, which is good, because it allows us to send
# requests with missing fields and see the server catch that error.
def test_createtable_vectorindexes_missing_fields(dynamodb):
    good_params = {
           'IndexName': 'hello',
           'VectorAttribute': {'AttributeName': 'v'},
           'Dimensions': 4,
           'DistanceFunction': 'COSINE',
           'Projection': {'ProjectionType': 'KEYS_ONLY'},
           }
    for key in good_params:
        bad = dict(good_params)
        del bad[key]
        # The error message should mention the missing parameter's name.
        # Strangely, DynamoDB lowercases the first character of the parameter
        # name, so we use (?i) to ignore case in the match.
        with pytest.raises(ClientError, match=f'(?i)ValidationException.*{key}'):
            with new_test_table(dynamodb,
                KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
                AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }],
                VectorIndexes=[bad]) as table:
                pass

# Test that DistanceFunction can be COSINE, DOT_PRODUCT or EUCLIDEAN.
# An unknown name like 'dog' or lower-case 'cosine' are rejected (the
# names are case-sensitive).
def test_createtable_vectorindexes_distancefunction(dynamodb):
    # The three documented values are all accepted:
    for good in ['COSINE', 'DOT_PRODUCT', 'EUCLIDEAN']:
        with new_test_table(dynamodb,
                KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
                AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }],
                VectorIndexes=[
                    {   'IndexName': 'hello',
                        'VectorAttribute': {'AttributeName': 'v'},
                        'Dimensions': 4,
                        'DistanceFunction': good,
                        'Projection': {'ProjectionType': 'KEYS_ONLY'},
                    }]) as table:
            pass
    # An unrecognized name, or a recognized name in the wrong case, is
    # rejected with a ValidationException:
    for bad in ['dog', 'cosine', 'Cosine']:
        with pytest.raises(ClientError, match='(?i)ValidationException.*DistanceFunction'):
            with new_test_table(dynamodb,
                    KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
                    AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }],
                    VectorIndexes=[
                        {   'IndexName': 'hello',
                            'VectorAttribute': {'AttributeName': 'v'},
                            'Dimensions': 4,
                            'DistanceFunction': bad,
                            'Projection': {'ProjectionType': 'KEYS_ONLY'},
                        }]) as table:
                pass

# Check that we are not allowed to create two VectorIndexes with the same
# name.
def test_createtable_vectorindexes_same_name(dynamodb):
    with pytest.raises(ClientError, match='ValidationException.*Duplicate.*hello'):
        with new_test_table(dynamodb,
            KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
            AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }],
            VectorIndexes=[
                {   'IndexName': 'hello',
                    'VectorAttribute': {'AttributeName': 'v'},
                    'Dimensions': 4,
                    'DistanceFunction': 'COSINE',
                    'Projection': {'ProjectionType': 'KEYS_ONLY'},
                },
                {   'IndexName': 'hello',
                    'VectorAttribute': {'AttributeName': 'x'},
                    'Dimensions': 7,
                    'DistanceFunction': 'COSINE',
                    'Projection': {'ProjectionType': 'KEYS_ONLY'},
                }
            ]) as table:
            pass

# Check that we are not allowed to create a VectorIndexes with the same name
# as the name of another type of index - GSI or an LSI.
def test_createtable_vectorindexes_same_name_gsi(dynamodb):
    with pytest.raises(ClientError, match='ValidationException.*Duplicate.*hello'):
        with new_test_table(dynamodb,
            KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
            AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }],
            GlobalSecondaryIndexes=[
                {   'IndexName': 'hello',
                    'KeySchema': [{ 'AttributeName': 'p', 'KeyType': 'HASH' }],
                    'Projection': { 'ProjectionType': 'ALL' }
                }],
            VectorIndexes=[
                {   'IndexName': 'hello',
                    'VectorAttribute': {'AttributeName': 'x'},
                    'Dimensions': 7,
                    'DistanceFunction': 'COSINE',
                    'Projection': {'ProjectionType': 'KEYS_ONLY'}
                }]
            ) as table:
            pass

def test_createtable_vectorindexes_same_name_lsi(dynamodb):
    with pytest.raises(ClientError, match='ValidationException.*Duplicate.*hello'):
        with new_test_table(dynamodb,
            KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' },
                        { 'AttributeName': 'c', 'KeyType': 'RANGE' }],
            AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' },
                                  { 'AttributeName': 'c', 'AttributeType': 'S' },
                                  { 'AttributeName': 'x', 'AttributeType': 'S' }],
            LocalSecondaryIndexes=[
                {   'IndexName': 'hello',
                    'KeySchema': [{ 'AttributeName': 'p', 'KeyType': 'HASH' },
                                  { 'AttributeName': 'x', 'KeyType': 'RANGE' }],
                    'Projection': { 'ProjectionType': 'ALL' }
                }],
            VectorIndexes=[
                {   'IndexName': 'hello',
                    'VectorAttribute': {'AttributeName': 'x'},
                    'Dimensions': 7,
                    'DistanceFunction': 'COSINE',
                    'Projection': {'ProjectionType': 'KEYS_ONLY'}
                }]
            ) as table:
            pass


# In DynamoDB, a vector index's IndexName follows the same naming rules as
# table names and names of other indexes - name length from 3 to 255 and match
# the regex [a-zA-Z0-9._-]+.
#
# In Alternator, the maximum allowed length for IndexName is a bit lower - 192
# (see max_table_name_length). So we split this test into two: One that passes
# on both DynamoDB and Alternator, and one for IndexName of length 255 that
# passes DynamoDB but fails on Alternator.
# Note that Alternator's IndexName length limit is similar, but not identical,
# to the rules for IndexName for GSI/LSI (see test_gsi.py and test_lsi.py) -
# there, Alternator doesn't put the limit on the length of the GSI/LSI's
# IndexName, but puts a limit (222) on the sum of the table's name and
# GSI/LSI's name.
def test_createtable_vectorindexes_indexname_rules(dynamodb):
    # Forbidden names: shorter than 3 characters, longer than 255
    # characters, or containing characters outside [a-zA-Z0-9._-].
    # These names should be rejected
    for bad_name in ['xy', 'x'*256, 'hello$world', 'hello world']:
        with pytest.raises(ClientError, match='(?i)ValidationException.*IndexName'):
            with new_test_table(dynamodb,
                KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
                AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }],
                VectorIndexes=[
                    {   'IndexName': bad_name,
                        'VectorAttribute': {'AttributeName': 'v'},
                        'Dimensions': 74,
                        'DistanceFunction': 'COSINE',
                        'Projection': {'ProjectionType': 'KEYS_ONLY'}
                    }]
            ) as table:
                pass
    # Allowed names: exactly 3 characters, 192 characters, and using
    # all characters from [a-zA-Z0-9._-].
    # This test is slightly slower than usual, because three tables and
    # indexes will be successfully created and then immediately deleted.
    for good_name in ['xyz', 'x'*192,
                      'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-']:
        with new_test_table(dynamodb,
            KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
            AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }],
            VectorIndexes=[
                {   'IndexName': good_name,
                    'VectorAttribute': {'AttributeName': 'v'},
                    'Dimensions': 74,
                    'DistanceFunction': 'COSINE',
                    'Projection': {'ProjectionType': 'KEYS_ONLY'}
            }]
        ) as table:
            pass

@pytest.mark.xfail(reason='Alternator limits IndexName length to 192, DynamoDB allows 255')
def test_createtable_vectorindexes_indexname_255(dynamodb):
    good_name = 'x'*255
    with new_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
        AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }],
        VectorIndexes=[
            {   'IndexName': good_name,
                'VectorAttribute': {'AttributeName': 'v'},
                'Dimensions': 74,
                'DistanceFunction': 'COSINE',
                'Projection': {'ProjectionType': 'KEYS_ONLY'}
        }]
    ) as table:
        pass

# Both DynamoDB and Alternator have an upper limit for the allowed number
# of dimensions, but this limit is different. We consider the fact we have
# a different limit acceptable.
def max_vector_dimensions(dynamodb):
    if is_aws(dynamodb):
        return 4096
    else:
        # In ScyllaDB, the limit is cql3::cql3_type::MAX_VECTOR_DIMENSION
        return 16000

# Check what values are allowed for the "Dimensions" property in CreateTable's
# VectorIndexes's VectorAttribute.
def test_createtable_vectorindexes_dimensions_rules(dynamodb):
    vector_index = {
        'IndexName': 'vector_index',
        'VectorAttribute': {'AttributeName': 'v'},
        'DistanceFunction': 'COSINE',
        'Projection': {'ProjectionType': 'KEYS_ONLY'},
        # 'Dimensions' is missing and will be added below for each case
    }
    # A non-numeric "Dimensions" (e.g., a string) is rejected with a
    # SerializationException:
    vector_index['Dimensions'] = 'hello'
    with pytest.raises(ClientError, match='SerializationException'):
        with new_test_table(dynamodb,
            KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
            AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }],
            VectorIndexes=[vector_index]) as table:
            pass
    # Curiously, if Dimensions is set to a non-whole value (e.g., 6.7) it's
    # allowed - the number is silently truncated to 6 and accepted. This
    # doesn't make too much sense, but it is what DynamoDB does.
    vector_index['Dimensions'] = 6.7
    with new_test_table(dynamodb,
            KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
            AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }],
            VectorIndexes=[vector_index]) as table:
        desc = table.meta.client.describe_table(TableName=table.name)
        vector_indexes = desc['Table']['VectorIndexes']
        assert len(vector_indexes) == 1
        assert vector_indexes[0]['Dimensions'] == 6

    max_dimensions = max_vector_dimensions(dynamodb)
    # Forbidden dimensions: negative, zero, and above max_dimensions.
    # These are rejected with ValidationException.
    for bad_dimensions in [-17, 0, max_dimensions+1]:
        vector_index['Dimensions'] = bad_dimensions
        with pytest.raises(ClientError, match='(?i)ValidationException.*Dimensions'):
            with new_test_table(dynamodb,
                KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
                AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }],
                VectorIndexes=[vector_index]) as table:
                pass
    # Allowed dimensions: 1, max_dimensions:
    for good_dimensions in [1, max_dimensions]:
        vector_index['Dimensions'] = good_dimensions
        with new_test_table(dynamodb,
            KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
            AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }],
            VectorIndexes=[vector_index]) as table:
            pass

# Check that the "AttributeName" property in CreateTable's VectorIndexes's
# VectorAttribute must not be a key column (of the base table or any of its
# GSIs or LSIs). This is because key columns have a declared type, which
# can't be a vector (a list), so making such a column the key of a vector
# index makes no sense.
def test_createtable_vectorindexes_attributename_key(dynamodb):
    # Forbidden AttributeName: base-table keys (hash and range), GSI keys,
    # LSI keys:
    for bad_attr in ['p', 'c', 'x', 'y', 'z']:
        with pytest.raises(ClientError, match='ValidationException.*VectorAttribute'):
            with new_test_table(dynamodb,
                KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' },
                            { 'AttributeName': 'c', 'KeyType': 'RANGE' }],
                AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' },
                                      { 'AttributeName': 'c', 'AttributeType': 'S' },
                                      { 'AttributeName': 'x', 'AttributeType': 'S' },
                                      { 'AttributeName': 'y', 'AttributeType': 'S' },
                                      { 'AttributeName': 'z', 'AttributeType': 'S' },
                    ],
                VectorIndexes=[
                    {   'IndexName': 'vector_index',
                        'VectorAttribute': {'AttributeName': bad_attr}, 'Dimensions': 42,
                        'DistanceFunction': 'COSINE',
                        'Projection': {'ProjectionType': 'KEYS_ONLY'}
                    }],
                GlobalSecondaryIndexes=[
                    {   'IndexName': 'gsi',
                        'KeySchema': [{ 'AttributeName': 'x', 'KeyType': 'HASH' },
                                      { 'AttributeName': 'y', 'KeyType': 'RANGE' }],
                        'Projection': { 'ProjectionType': 'ALL' }
                    }],
                LocalSecondaryIndexes=[
                    {   'IndexName': 'lsi',
                        'KeySchema': [{ 'AttributeName': 'p', 'KeyType': 'HASH' },
                                      { 'AttributeName': 'z', 'KeyType': 'RANGE' }],
                        'Projection': { 'ProjectionType': 'ALL' }
                    }],
            ) as table:
                pass

# Check that the "AttributeName" property in CreateTable's VectorIndexes's
# VectorAttribute is an attribute name, limited exactly like ordinary (non-
# key) attributes to 65535 (DYNAMODB_NONKEY_ATTR_NAME_SIZE_MAX) bytes.
# Note that there is no limitation on which characters are allowed, so we
# don't check that.
def test_createtable_vectorindexes_attributename_len(dynamodb):
    # Forbidden AttributeName: empty string, string over 65535
    for bad_attr in ['', 'x'*65536]:
        with pytest.raises(ClientError, match='(?i)ValidationException.*AttributeName'):
            with new_test_table(dynamodb,
                KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
                AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }],
                VectorIndexes=[
                    {   'IndexName': 'vector_index',
                        'VectorAttribute': {'AttributeName': bad_attr}, 'Dimensions': 42,
                        'DistanceFunction': 'COSINE',
                        'Projection': {'ProjectionType': 'KEYS_ONLY'}
                    }]
            ) as table:
                pass

# Test that we can add two different vector indexes on the same table
# in CreateTable, but they must be on different attributes.
def test_createtable_vectorindexes_multiple(dynamodb):
    # Can create two vector indexes on two different attributes:
    with new_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
        AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }],
        VectorIndexes=[
            {   'IndexName': 'ind1',
                'VectorAttribute': {'AttributeName': 'x'},
                'Dimensions': 42,
                'DistanceFunction': 'COSINE',
                'Projection': {'ProjectionType': 'KEYS_ONLY'}
            },
            {   'IndexName': 'ind2',
                'VectorAttribute': {'AttributeName': 'y'},
                'Dimensions': 17,
                'DistanceFunction': 'COSINE',
                'Projection': {'ProjectionType': 'KEYS_ONLY'}
            },
        ]) as table:
        pass
    # We can't create two vector indexes on the same attribute is they have
    # a different "Dimensions". This is because Alternator wants to enforce
    # that inserted vectors have the right length, and can't enforce two
    # conflicting requirements on the same attribute.
    with pytest.raises(ClientError, match='(?i)ValidationException.*Dimensions'):
        with new_test_table(dynamodb,
            KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
            AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }],
            VectorIndexes=[
                {   'IndexName': 'ind1',
                    'VectorAttribute': {'AttributeName': 'x'},
                    'Dimensions': 42,
                    'DistanceFunction': 'COSINE',
                    'Projection': {'ProjectionType': 'KEYS_ONLY'}
                },
                {   'IndexName': 'ind2',
                    'VectorAttribute': {'AttributeName': 'x'},
                    'Dimensions': 17,
                    'DistanceFunction': 'COSINE',
                    'Projection': {'ProjectionType': 'KEYS_ONLY'}
                },
            ]) as table:
            pass
    # But we *can* create two vector indexes on the same attribute if they
    # have the same "Dimensions". These vector indexes can differ in other
    # parameters, like DistanceFunction or Projection, which can make this
    # case useful.
    with new_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
        AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }],
        VectorIndexes=[
            {   'IndexName': 'ind1',
                'VectorAttribute': {'AttributeName': 'x'},
                'Dimensions': 42,
                'DistanceFunction': 'COSINE',
                'Projection': {'ProjectionType': 'KEYS_ONLY'}
            },
            {   'IndexName': 'ind2',
                'VectorAttribute': {'AttributeName': 'x'},
                'Dimensions': 42,
                'DistanceFunction': 'DOT_PRODUCT',
                'Projection': {'ProjectionType': 'KEYS_ONLY'}
            },
        ]) as table:
        pass
    # We can also create to completely identical vector indexes on the same
    # attribute. It is silly and wasteful to maintain two identical indexes,
    # but allowed.
    with new_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
        AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }],
        VectorIndexes=[
            {   'IndexName': 'ind1',
                'VectorAttribute': {'AttributeName': 'x'},
                'Dimensions': 42,
                'DistanceFunction': 'COSINE',
                'Projection': {'ProjectionType': 'KEYS_ONLY'}
            },
            {   'IndexName': 'ind2',
                'VectorAttribute': {'AttributeName': 'x'},
                'Dimensions': 42,
                'DistanceFunction': 'COSINE',
                'Projection': {'ProjectionType': 'KEYS_ONLY'}
            },
        ]) as table:
        # Verify with DescribeTable that both vector indexes were really
        # created:
        desc = table.meta.client.describe_table(TableName=table.name)
        vector_indexes = desc['Table']['VectorIndexes']
        assert {v['IndexName'] for v in vector_indexes} == {'ind1', 'ind2'}

# Test that vector indexes are correctly listed in DescribeTable:
def test_describetable_vectorindexes(dynamodb):
    with new_test_table(dynamodb,
            KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
            AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }],
            VectorIndexes=[
                {   'IndexName': 'ind1',
                    'VectorAttribute': {'AttributeName': 'x'},
                    'Dimensions': 42,
                    'DistanceFunction': 'COSINE',
                    'Projection': {'ProjectionType': 'KEYS_ONLY'}
                },
                {   'IndexName': 'ind2',
                    'VectorAttribute': {'AttributeName': 'y'},
                    'Dimensions': 17,
                    'DistanceFunction': 'COSINE',
                    'Projection': {'ProjectionType': 'KEYS_ONLY'}
                },
            ]) as table:
        desc = table.meta.client.describe_table(TableName=table.name)
        assert 'Table' in desc
        assert 'VectorIndexes' in desc['Table']
        vector_indexes = desc['Table']['VectorIndexes']
        assert len(vector_indexes) == 2
        for vec in vector_indexes:
            assert vec['IndexName'] == 'ind1' or vec['IndexName'] == 'ind2'
            if vec['IndexName'] == 'ind1':
                assert vec['VectorAttribute'] == {'AttributeName': 'x'}
                assert vec['Dimensions'] == 42
            else: # vec['IndexName'] == 'ind2':
                assert vec['VectorAttribute'] == {'AttributeName': 'y'}
                assert vec['Dimensions'] == 17
            assert vec['DistanceFunction'] == 'COSINE'
            assert vec['Projection'] == {'ProjectionType': 'KEYS_ONLY'}
            # Like a GSI's IndexArn, each vector index also gets its own ARN.
            assert 'IndexArn' in vec

# In addition to the basic listing of a vector index in DescribeTable tested
# above, in this test we check additional fields that should appear in each
# vector index's description. This needs to be a separate (rather than
# folded into test_describetable_vectorindexes above) xfail test, just like
# test_gsi_describe_fields() in test_gsi.py, because Alternator will not
# have these fields initially.
@pytest.mark.xfail(reason="Alternator does not yet report IndexSizeBytes/ItemCount for vector indexes")
def test_describetable_vectorindexes_describe_fields(table_vs):
    desc = table_vs.meta.client.describe_table(TableName=table_vs.name)
    vector_indexes = desc['Table']['VectorIndexes']
    assert len(vector_indexes) == 1
    vec = vector_indexes[0]
    assert 'IndexSizeBytes' in vec    # actual size depends on content
    assert 'ItemCount' in vec

# Test that like DescribeTable, CreateTable also returns the VectorIndexes
# definition its response
def test_createtable_vectorindexes_returned(dynamodb):
    # To look at the response of CreateTable, we need to use the "client"
    # interface, not the usual higher-level "resource" interface that we
    # usually use in tests - because that doesn't return the actual response.
    client = dynamodb.meta.client
    table_name = unique_table_name()
    resp = client.create_table(
        TableName=table_name,
        BillingMode='PAY_PER_REQUEST',
        KeySchema=[{ 'AttributeName': 'p', 'KeyType': 'HASH' }],
        AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }],
        VectorIndexes=[{
            'IndexName': 'ind',
            'VectorAttribute': {'AttributeName': 'x'},
            'Dimensions': 42,
            'DistanceFunction': 'COSINE',
            'Projection': {'ProjectionType': 'KEYS_ONLY'}
        }])
    try:
        assert 'TableDescription' in resp
        assert 'VectorIndexes' in resp['TableDescription']
        vector_indexes = resp['TableDescription']['VectorIndexes']
        assert len(vector_indexes) == 1
        vec = vector_indexes[0]
        assert vec['IndexName'] == 'ind'
        assert vec['VectorAttribute'] == {'AttributeName': 'x'}
        assert vec['Dimensions'] == 42
        assert vec['DistanceFunction'] == 'COSINE'
        assert vec['Projection'] == {'ProjectionType': 'KEYS_ONLY'}
    finally:
        # We must wait for the table to become ACTIVE before deleting it -
        # In DynamoDB CreateTable is asynchronous, the table is initially in
        # CREATING state, and we can't delete it until it's ACTIVE.
        wait_for_status_active(dynamodb.Table(table_name))
        client.delete_table(TableName=table_name)

# Basic test for UpdateTable successfully adding a vector index
def test_updatetable_vectorindex_create(dynamodb):
    with new_test_table(dynamodb,
            KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
            AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }]) as table:
        # There are no vector indexes yet:
        desc = table.meta.client.describe_table(TableName=table.name)
        assert 'Table' in desc
        assert 'VectorIndexes' not in desc['Table']
        # Add a vector index with UpdateTable
        table.update(VectorIndexUpdates=[{'Create':
            { 'IndexName': 'hello',
              'VectorAttribute': {'AttributeName': 'x'},
              'Dimensions': 17,
              'DistanceFunction': 'COSINE',
              'Projection': {'ProjectionType': 'KEYS_ONLY'}
            }}])
        # Now describe_table should see the new vector index:
        desc = table.meta.client.describe_table(TableName=table.name)
        assert 'Table' in desc
        assert 'VectorIndexes' in desc['Table']
        vector_indexes = desc['Table']['VectorIndexes']
        assert len(vector_indexes) == 1
        vec = vector_indexes[0]
        assert vec['IndexName'] == 'hello'
        assert vec['VectorAttribute'] == {'AttributeName': 'x'}
        assert vec['Dimensions'] == 17
        assert vec['DistanceFunction'] == 'COSINE'
        assert vec['Projection'] == {'ProjectionType': 'KEYS_ONLY'}
        # In DynamoDB the UpdateTable operation is asynchronous, and before
        # it finishes all its background work we will not be allowed to delete
        # the table when we exit this code block. So we must wait for the both
        # the UpdateTable to complete (wait_for_status_active()) and for the
        # index backfilling to finish (wait_for_vector_index_active()).
        # However, on DynamoDB backfilling takes a ridiculously long amount of
        # time (close to 20 minutes!), so a much faster alternative is to delete
        # the vector index first, which will cancel the backfilling and allow
        # us to delete the table immediately after that.
        wait_for_status_active(table)
        table.update(VectorIndexUpdates=[{'Delete': { 'IndexName': 'hello' }}])
        wait_for_status_active(table)

# Basic test for UpdateTable successfully removing a vector index
def test_updatetable_vectorindex_delete(dynamodb):
    with new_test_table(dynamodb,
            KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
            AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }],
            VectorIndexes=[{
                'IndexName': 'hello',
                'VectorAttribute': {'AttributeName': 'x'},
                'Dimensions': 42,
                'DistanceFunction': 'COSINE',
                'Projection': {'ProjectionType': 'KEYS_ONLY'}
            }]) as table:
        # There should be one vector index now:
        desc = table.meta.client.describe_table(TableName=table.name)
        assert 'Table' in desc
        assert 'VectorIndexes' in desc['Table']
        assert len(desc['Table']['VectorIndexes']) == 1
        # Delete the vector index with UpdateTable
        table.update(VectorIndexUpdates=[
            {'Delete': { 'IndexName': 'hello' }}])
        wait_for_status_active(table)
        # Now describe_table should see no vector index:
        desc = table.meta.client.describe_table(TableName=table.name)
        assert 'Table' in desc
        assert 'VectorIndexes' not in desc['Table']

# UpdateTable can't remove a vector index that doesn't exist. We get a
# ResourceNotFoundException.
def test_updatetable_vectorindex_delete_nonexistent(dynamodb):
    with pytest.raises(ClientError, match='ResourceNotFoundException'):
        with new_test_table(dynamodb,
                KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
                AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }]) as table:
            table.update(VectorIndexUpdates=[
                {'Delete': { 'IndexName': 'nonexistent' }}])

# Test that VectorIndexUpdates only allows "Create" and "Delete" actions -
# unlike GlobalSecondaryIndexUpdates which also has "Update" - vector
# indexes do not support Update.
def test_updatetable_vectorindex_update_not_allowed(dynamodb):
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
            VectorIndexes=[{
                'IndexName': 'vind',
                'VectorAttribute': {'AttributeName': 'v'},
                'Dimensions': 3,
                'DistanceFunction': 'COSINE',
                'Projection': {'ProjectionType': 'KEYS_ONLY'},
            }]) as table:
        client = table.meta.client
        # Botocore's service model only knows "Create" and "Delete" as
        # members of a VectorIndexUpdate, so an unrecognized key like
        # "Update" or "Dog" would normally be silently dropped during
        # request serialization, rather than reaching the server at all.
        # Just like test_gsi_updatetable_errors's "Dog" trick for
        # GlobalSecondaryIndexUpdates, we monkey-patch the service model to
        # make it recognize these action names too (reusing "Delete"'s
        # shape, which is just an IndexName), so we can confirm the *server*
        # rejects them.
        service_model = client.meta.service_model
        client.meta.service_model._instance_cache = {}  # clear cached shapes
        shape_resolver = service_model._shape_resolver
        shape = shape_resolver._shape_map['VectorIndexUpdate']
        # If "Update" (or our made-up "Dog") were ever to legitimately become
        # a real member of VectorIndexUpdate, this test's premise is no
        # longer true, and blindly overwriting/restoring it below could mask
        # that real change. Fail loudly instead.
        assert 'Update' not in shape['members'] and 'Dog' not in shape['members']
        original_members = dict(shape['members'])
        shape['members']['Update'] = shape['members']['Delete']
        shape['members']['Dog'] = shape['members']['Delete']
        try:
            for bad_action in ['Update', 'Dog']:
                with pytest.raises(ClientError, match='ValidationException.*VectorIndexUpdate'):
                    client.update_table(
                        TableName=table.name,
                        VectorIndexUpdates=[{bad_action: {'IndexName': 'vind'}}])
        finally:
            shape['members'] = original_members
            client.meta.service_model._instance_cache = {}

# Test that in UpdateTable's Create operation, a IndexName and VectorAttribute
# are required. Inside the VectorAttribute, a AttributeName and Dimensions
# are required. With any of those fields missing we get a ValidationException.
# This test is similar to test_createtable_vectorindexes_missing_fields()
# above, but for UpdateTable instead of CreateTable.
def test_updatetable_vectorindex_missing_fields(dynamodb):
    with new_test_table(dynamodb,
            KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
            AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }]) as table:
        good_params = {
               'IndexName': 'hello',
               'VectorAttribute': {'AttributeName': 'v'},
               'Dimensions': 4,
               'DistanceFunction': 'COSINE',
               'Projection': {'ProjectionType': 'KEYS_ONLY'},
               }
        for key in good_params:
            bad = dict(good_params)
            del bad[key]
            with pytest.raises(ClientError, match=f'(?i)ValidationException.*{key}'):
                table.update(VectorIndexUpdates=[{'Create': bad}])

# Test that when adding a vector index with UpdateTable,
# 1. Its name cannot be the same as an existing vector index or GSI or LSI
# 2. Its attribute cannot be a key column (of base, GSI or LSI) or the
#    attribute on an existing vector index
def test_updatetable_vectorindex_taken_name_or_attribute(dynamodb):
    # We create a table with vector index, GSI and LSI, so we can check
    # all the desired cases on a single table.
    with new_test_table(dynamodb,
        KeySchema=[
            { 'AttributeName': 'p', 'KeyType': 'HASH' },
            { 'AttributeName': 'c', 'KeyType': 'RANGE' }],
        AttributeDefinitions=[
            { 'AttributeName': 'p', 'AttributeType': 'S' },
            { 'AttributeName': 'c', 'AttributeType': 'S' },
            { 'AttributeName': 'x', 'AttributeType': 'S' },
            { 'AttributeName': 'y', 'AttributeType': 'S' },
            { 'AttributeName': 'z', 'AttributeType': 'S' }],
        VectorIndexes=[
            {   'IndexName': 'vec',
                'VectorAttribute': {'AttributeName': 'v'},
                'Dimensions': 13,
                'DistanceFunction': 'COSINE',
                'Projection': {'ProjectionType': 'KEYS_ONLY'}
            }],
        GlobalSecondaryIndexes=[
            { 'IndexName': 'gsi',
              'KeySchema': [
                  { 'AttributeName': 'x', 'KeyType': 'HASH' },
                  { 'AttributeName': 'y', 'KeyType': 'RANGE' }],
              'Projection': { 'ProjectionType': 'ALL' }}],
        LocalSecondaryIndexes=[
            { 'IndexName': 'lsi',
              'KeySchema': [
                  { 'AttributeName': 'p', 'KeyType': 'HASH' },
                  { 'AttributeName': 'z', 'KeyType': 'RANGE' }],
              'Projection': { 'ProjectionType': 'ALL' }
            }],
        ) as table:
        # IndexName already in use:
        for bad_name in ['vec', 'gsi', 'lsi']:
            with pytest.raises(ClientError, match='ValidationException.*already exists'):
                table.update(VectorIndexUpdates=[{'Create':
                    {   'IndexName': bad_name,
                        'VectorAttribute': {'AttributeName': 'xyz'},
                        'Dimensions': 17,
                        'DistanceFunction': 'COSINE',
                        'Projection': {'ProjectionType': 'KEYS_ONLY'}
                    }}])
        # AttributeName already a key column (of the base or index)
        # of a different type (the vector index of 'v' also mismatches
        # because it has different Dimensions defined):
        for bad_attr in ['p', 'c', 'x', 'y', 'z', 'v']:
            with pytest.raises(ClientError, match='ValidationException.*VectorAttribute'):
                table.update(VectorIndexUpdates=[{'Create':
                    {   'IndexName': 'newind',
                        'VectorAttribute': {'AttributeName': bad_attr},
                        'Dimensions': 17,
                        'DistanceFunction': 'COSINE',
                        'Projection': {'ProjectionType': 'KEYS_ONLY'}
                    }}])

# In test_updatetable_vectorindex_taken_name_or_attribute() above we tested
# that we can't add a vector index with the same name as an existing GSI or
# LSI. Here we check that the reverse also holds - we can't add a GSI with
# the same name as an existing vector index.
def test_updatetable_gsi_same_name_as_vector_index(dynamodb):
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
            VectorIndexes=[
                {'IndexName': 'vec',
                 'VectorAttribute': {'AttributeName': 'v'},
                 'Dimensions': 3,
                 'DistanceFunction': 'COSINE',
                 'Projection': {'ProjectionType': 'KEYS_ONLY'}
                }
            ]) as table:
        with pytest.raises(ClientError, match='ValidationException.*already exists'):
            table.meta.client.update_table(
                TableName=table.name,
                AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
                GlobalSecondaryIndexUpdates=[{'Create': {
                    'IndexName': 'vec',
                    'KeySchema': [{'AttributeName': 'p', 'KeyType': 'HASH'}],
                    'Projection': {'ProjectionType': 'ALL'}
                }}])

# Similarly, we can't add a GSI on an attribute that's already used as a
# vector index attribute.
def test_updatetable_gsi_key_is_vector_attribute(dynamodb):
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
            VectorIndexes=[
                {'IndexName': 'vec',
                 'VectorAttribute': {'AttributeName': 'v'},
                 'Dimensions': 3,
                 'DistanceFunction': 'COSINE',
                 'Projection': {'ProjectionType': 'KEYS_ONLY'}
                }
            ]) as table:
        # The attribute 'v' is already a vector index target - it cannot
        # become the hash key of a new GSI.
        with pytest.raises(ClientError, match='ValidationException.*attribute'):
            table.meta.client.update_table(
                TableName=table.name,
                AttributeDefinitions=[{'AttributeName': 'v', 'AttributeType': 'S'}],
                GlobalSecondaryIndexUpdates=[{'Create': {
                    'IndexName': 'gsi',
                    'KeySchema': [{'AttributeName': 'v', 'KeyType': 'HASH'}],
                    'Projection': {'ProjectionType': 'ALL'}
                }}])

# Similar to test_createtable_vectorindexes_indexname_rules() above, verify
# that also for UpdateTable creating a new vector index, the new IndexName
# must have length from 3 to 255 and match the regex [a-zA-Z0-9._-]+.
#
# In Alternator, the maximum allowed length for IndexName is a bit lower -
# 192 (see max_table_name_length). So just like for the CreateTable case, we
# split this test into two: one that passes on both DynamoDB and Alternator,
# and one for IndexName of length 255 that passes on DynamoDB but fails on
# Alternator.
def test_updatetable_vectorindex_indexname_rules(dynamodb):
    with new_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
        AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }]) as table:
        # Forbidden names: shorter than 3 characters, longer than 255
        # characters, or containing characters outside [a-zA-Z0-9._-].
        # These names should be rejected
        for bad_name in ['xy', 'x'*256, 'hello$world', 'hello world']:
            with pytest.raises(ClientError, match='(?i)ValidationException.*IndexName'):
                table.update(VectorIndexUpdates=[{'Create':
                    { 'IndexName': bad_name,
                      'VectorAttribute': {'AttributeName': 'x'},
                      'Dimensions': 17,
                      'DistanceFunction': 'COSINE',
                      'Projection': {'ProjectionType': 'KEYS_ONLY'}
                    }}])
        # Allowed names: exactly 3 characters, 192 characters, and using
        # all characters from [a-zA-Z0-9._-].
        for good_name in ['xyz', 'x'*192,
                          'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-']:
            table.update(VectorIndexUpdates=[{'Create':
                { 'IndexName': good_name,
                  'VectorAttribute': {'AttributeName': 'x'},
                  'Dimensions': 17,
                  'DistanceFunction': 'COSINE',
                  'Projection': {'ProjectionType': 'KEYS_ONLY'}
                }}])
            wait_for_status_active(table)
            table.update(VectorIndexUpdates=[{'Delete': { 'IndexName': good_name }}])
            wait_for_status_active(table)

@pytest.mark.xfail(reason='Alternator limits IndexName length to 192, DynamoDB allows 255')
def test_updatetable_vectorindex_indexname_255(dynamodb):
    good_name = 'x'*255
    with new_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
        AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }]) as table:
        table.update(VectorIndexUpdates=[{'Create':
            { 'IndexName': good_name,
              'VectorAttribute': {'AttributeName': 'x'},
              'Dimensions': 17,
              'DistanceFunction': 'COSINE',
              'Projection': {'ProjectionType': 'KEYS_ONLY'}
            }}])
        wait_for_status_active(table)
        # Before we can delete the table we need to either wait for the vector
        # index backfilling to finish with wait_for_vector_index_active(),
        # which takes as much as 20 minutes (!) on DynamoDB, or, a MUCH faster
        # alternative is to delete the vector index first:
        table.update(VectorIndexUpdates=[{'Delete': { 'IndexName': good_name }}])
        wait_for_status_active(table)

# Similar to test_createtable_vectorindexes_dimensions_rules() above for
# CreateTable, here we want to verify that also for UpdateTable that creates a
# new vector index, a vector index's Dimensions has the same limitations.
def test_updatetable_vectorindex_dimensions_rules(dynamodb):
    with new_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
        AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }]) as table:
        vector_index = {
            'IndexName': 'ind',
            'VectorAttribute': {'AttributeName': 'x'},
            'DistanceFunction': 'COSINE',
            'Projection': {'ProjectionType': 'KEYS_ONLY'},
            # 'Dimensions' is missing and will be added below for each case
        }
        # A non-numeric "Dimensions" (e.g., a string) is rejected with a
        # SerializationException:
        vector_index['Dimensions'] = 'hello'
        with pytest.raises(ClientError, match='SerializationException'):
            table.update(VectorIndexUpdates=[{'Create': vector_index}])
        # Curiously, if Dimensions is set to a non-whole value (e.g., 6.7)
        # it's allowed - the number is silently truncated to 6 and accepted.
        # This doesn't make too much sense, but it is what DynamoDB does.
        vector_index['Dimensions'] = 6.7
        table.update(VectorIndexUpdates=[{'Create': vector_index}])
        wait_for_status_active(table)
        desc = table.meta.client.describe_table(TableName=table.name)
        vector_indexes = desc['Table']['VectorIndexes']
        assert len(vector_indexes) == 1
        assert vector_indexes[0]['Dimensions'] == 6
        table.update(VectorIndexUpdates=[{'Delete': {'IndexName': 'ind'}}])
        wait_for_status_active(table)

        max_dimensions = max_vector_dimensions(dynamodb)
        # Forbidden dimensions: negative, zero, and above max_dimensions.
        # These are rejected with ValidationException.
        for bad_dimensions in [-17, 0, max_dimensions+1]:
            vector_index['Dimensions'] = bad_dimensions
            with pytest.raises(ClientError, match='(?i)ValidationException.*Dimensions'):
                table.update(VectorIndexUpdates=[{'Create': vector_index}])
        # Allowed dimensions: 1, max_dimensions:
        for good_dimensions in [1, max_dimensions]:
            vector_index['Dimensions'] = good_dimensions
            table.update(VectorIndexUpdates=[{'Create': vector_index}])
            wait_for_status_active(table)
            table.update(VectorIndexUpdates=[{'Delete': {'IndexName': 'ind'}}])
            wait_for_status_active(table)

# Similar to test_createtable_vectorindexes_attributename_len() above,
# verify that also for UpdateTable create a new vector index, a vector
# index's attribute name must have between 1 and 65535 bytes.
# Note that we also checked above that it can't be one of the existing keys
# (of base table, GSI or LSI), or an already indexed vector column. Here we
# only test the allowed length limits.
def test_updatetable_vectorindex_attributename_len(dynamodb):
    with new_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
        AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }]) as table:
        for bad_attr in ['', 'x'*65536]:
            with pytest.raises(ClientError, match='(?i)ValidationException.*AttributeName'):
                table.update(VectorIndexUpdates=[{'Create':
                    { 'IndexName': 'ind',
                      'VectorAttribute': {'AttributeName': bad_attr},
                      'Dimensions': 17,
                      'DistanceFunction': 'COSINE',
                      'Projection': {'ProjectionType': 'KEYS_ONLY'}
                    }}])

# DynamoDB traditionally limited UpdateTable to only one GSI operation (Create
# or Delete), and placed the same limit on VectorIndexUpdates: Even though
# it's an array, it must have exactly one element. Let's validate this
# limitation is enforced (if one day we decide to lift this limitation in
# Alternator, we can delete or change this test).
def test_updatetable_vectorindex_just_one_update(dynamodb):
    with new_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
        AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }]) as table:
        # Zero operations aren't allowed - it's treated just like a missing
        # VectorIndexUpdates, and therefore a do-nothing UpdateTable which
        # is not allowed.
        with pytest.raises(ClientError, match='ValidationException.* one'):
            table.update(VectorIndexUpdates=[])
        # Two "Create" aren't allowed.
        # Again following DynamoDB's lead on GSI, interestingly in this case
        # the error is LimitExceededException, not ValidationException.
        with pytest.raises(ClientError, match='LimitExceededException'):
            table.update(VectorIndexUpdates=[
                {'Create': {'IndexName': 'ind1', 'VectorAttribute': {'AttributeName': 'x'}, 'Dimensions': 17, 'DistanceFunction': 'COSINE', 'Projection': {'ProjectionType': 'KEYS_ONLY'}}},
                {'Create': {'IndexName': 'ind2', 'VectorAttribute': {'AttributeName': 'y'}, 'Dimensions': 17, 'DistanceFunction': 'COSINE', 'Projection': {'ProjectionType': 'KEYS_ONLY'}}}])
        # Two "Delete" aren't allowed (they are rejected even before noticing
        # that the indexes we ask to delete don't exist).
        with pytest.raises(ClientError, match='LimitExceededException'):
            table.update(VectorIndexUpdates=[
                {'Delete': {'IndexName': 'ind1'}},
                {'Delete': {'IndexName': 'ind2'}}])
        # Also one "Delete" and one "Create" isn't allowed
        with pytest.raises(ClientError, match='LimitExceededException'):
            table.update(VectorIndexUpdates=[
                {'Create': {'IndexName': 'ind1', 'VectorAttribute': {'AttributeName': 'x'}, 'Dimensions': 17, 'DistanceFunction': 'COSINE', 'Projection': {'ProjectionType': 'KEYS_ONLY'}}},
                {'Delete': {'IndexName': 'ind2'}}])

# Also, it's not allowed to have in one UpdateTable request both a
# VectorIndexUpdates and a GlobalSecondaryIndexUpdates. There is no real
# reason why we can't support this, but since we already don't allow adding
# (or deleting) more than one GSI or more than one vector index in the same
# operation, it makes sense to disallow having both. If one day we decide to
# allow both in the same request, we can delete this test.
def test_updatetable_vector_and_gsi_same_request(dynamodb):
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
        with pytest.raises(ClientError, match='LimitExceededException'):
            table.meta.client.update_table(
                TableName=table.name,
                AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
                VectorIndexUpdates=[{'Create': {
                    'IndexName': 'vec',
                    'VectorAttribute': {'AttributeName': 'v'},
                    'Dimensions': 3,
                    'DistanceFunction': 'COSINE',
                    'Projection': {'ProjectionType': 'KEYS_ONLY'}
                }}],
                GlobalSecondaryIndexUpdates=[{'Create': {
                    'IndexName': 'gsi',
                    'KeySchema': [{'AttributeName': 'p', 'KeyType': 'HASH'}],
                    'Projection': {'ProjectionType': 'ALL'}
                }}])

# Test that PutItem still works as expected on a table with a vector index
# created by CreateTable or UpdateTable. This test just checks that having
# an index does not cause writes to fail - it does not try to use the index.
# We have two versions of this test - one that creates the vector index in
# CreateTable, and one that does it via UpdateTable.
@pytest.mark.parametrize('via_update', [False, True])
def test_putitem_vectorindex(dynamodb, via_update):
    vector_index = {
        'IndexName': 'vec',
        'VectorAttribute': {'AttributeName': 'v'},
        'Dimensions': 3,
        'DistanceFunction': 'COSINE',
        'Projection': {'ProjectionType': 'KEYS_ONLY'},
    }
    # If via_update, create the table without a vector index, and add it
    # later with UpdateTable. Otherwise, create it right away in CreateTable.
    extra_kwargs = {} if via_update else {'VectorIndexes': [vector_index]}
    with new_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
        AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }],
        **extra_kwargs
        ) as table:
        if via_update:
            table.update(VectorIndexUpdates=[{'Create': vector_index}])
            wait_for_status_active(table)
            # Don't wait for the backfilling to complete (this can take a
            # huge amount of time on DynamoDB, even on an empty table).
            # We don't intend to read from the index anyway.
        p = random_string()
        item = {'p': p, 'v': [1,2,3]}
        table.put_item(Item=item)
        # Not only should put_item() not fail, it should also succeed in
        # writing the item (so we can read it back with get_item). Note that
        # this test does NOT try to read from the vector index - it just reads
        # from the base table.
        assert item == table.get_item(Key={'p': p}, ConsistentRead=True)['Item']
        if via_update:
            # We didn't wait for the index backfilling to complete, so rather
            # than wait for it now (this can take 20 minutes on DynamoDB) so
            # we will be allowed to delete the table, it is faster to delete
            # the index.
            table.update(VectorIndexUpdates=[{'Delete': {'IndexName': 'vec'}}])
            wait_for_status_active(table)

# Start testing the SearchVectors operation. As of this writing, boto3 did
# not yet add support for this operation in the higher-level "resource layer"
# and only added it in the "client layer". So we can't use
# table.search_vectors() - we need to use the somewhat uglier
# table.meta.client.search_vectors(TableName=table.name, ...).

# Test that a SearchVectors without an IndexName is rejected.
def test_searchvectors_missing_indexname(table_vs):
    client = table_vs.meta.client
    with pytest.raises(ClientError, match='ValidationException.*IndexName'):
        client.search_vectors(TableName=table_vs.name, SearchVector=[1, 2, 3], TopK=1)

# Test that a SearchVectors with an IndexName which does not refer to a valid
# vector index is rejected with a ValidationException. Note that it doesn't
# really matter if IndexName refers to a garbage name or to a real GSI/LSI -
# the code just checks if it's a known vector index name.
def test_searchvectors_wrong_indexname(table_vs):
    client = table_vs.meta.client
    with pytest.raises(ClientError, match='ValidationException.*nonexistent'):
        client.search_vectors(TableName=table_vs.name, IndexName='nonexistent', SearchVector=[1, 2, 3], TopK=1)

# Test that a Query on a vector index is rejected (the separate SearchVectors
# operation should be used instead). A Query expects IndexName to point to a
# an LSI or GSI - which it doesn't. But rather than reporting unhelpfully
# that an index by that name doesn't exist, we want to report that this index
# does exist - but has the wrong type.
def test_query_vectorindex_rejected(table_vs):
    with pytest.raises(ClientError, match='ValidationException.*index type'):
        table_vs.query(
            IndexName='vind',
            KeyConditionExpression='p = :p',
            ExpressionAttributeValues={':p': 'x'},
        )

# Similarly, Scan on a vector index is rejected.
def test_scan_vectorindex_rejected(table_vs):
    with pytest.raises(ClientError, match='ValidationException.*index type'):
        table_vs.scan(
            IndexName='vind',
        )

# Test that a SearchVectors that is missing the required SearchVector field
# is rejected with a ValidationException.
def test_searchvectors_missing_searchvector(table_vs):
    client = table_vs.meta.client
    with pytest.raises(ClientError, match='ValidationException.*SearchVector'):
        client.search_vectors(
            TableName=table_vs.name,
            IndexName='vind',
            TopK=1)

# Context manager to override one already-serialized request parameter to
# an arbitrary raw JSON value, just before the request is signed and sent.
# This is useful for testing malformed inputs that boto3 refuses if passed
# normally through its serializer - e.g., a string where boto3 expects a list.
# It's a simpler alternative to manual_request(): the rest of the request is
# built normally, only the named parameter's value is swapped out.
@contextmanager
def override_param(client, operation_name, param_name, value):
    def inject(request, **kwargs):
        body = json.loads(request.body)
        body[param_name] = value
        request.data = json.dumps(body).encode('utf-8')
    event_name = f'before-sign.dynamodb.{operation_name}'
    client.meta.events.register(event_name, inject)
    try:
        yield
    finally:
        client.meta.events.unregister(event_name, inject)

# Test that SearchVectors' SearchVector must be a list of numbers and must
# have the exact length defined as Dimensions of the vector index - which
# in table_vs is 3.
def test_searchvectors_searchvector_bad(table_vs):
    client = table_vs.meta.client
    # A non-list SearchVector, such as a string, is rejected.
    # Boto3 will catch an attempt to pass a string for SearchVector, so we
    # need to use override_param() to cause boto3 to send this string,
    # so we can test how the server handles it.
    with override_param(client, 'SearchVectors', 'SearchVector', 'not a list'):
        with pytest.raises(ClientError, match='SerializationException'):
            client.search_vectors(
                TableName=table_vs.name,
                IndexName='vind',
                TopK=1,
                # SearchVector is set in override_param() above.
            )
    # A list of the right length but with non-numeric elements
    # should be rejected:
    with pytest.raises(ClientError, match='ValidationException.*number'):
        client.search_vectors(
            TableName=table_vs.name,
            IndexName='vind',
            SearchVector=[1, 'b', 3],
            TopK=1
        )
    # DynamoDB's error message in the check above (with 'b') is: "All values
    # in the search vector must be a 32-bit floating-point number attribute"
    # Indeed, the DynamoDB documentation confirms that each SearchVector
    # element must be "a 32-bit IEEE-754 floating point number". A number
    # with excessive *range* (too big or small to fit in a 32-bit float) is
    # indeed rejected:
    with pytest.raises(ClientError, match='ValidationException.*number'):
        client.search_vectors(
            TableName=table_vs.name,
            IndexName='vind',
            SearchVector=[1, Decimal('1e39'), 3],  # 32-bit float max is ~3e38
            TopK=1
        )
    # But excessive *precision* (more significant digits than a 32-bit 
    # can hold) is accepted - presumably silently truncated.
    client.search_vectors(
        TableName=table_vs.name,
        IndexName='vind',
        SearchVector=[1, Decimal('3.1415926535897932384626433832795028841'), 3],
        TopK=1
    )
    # A numeric list but with the wrong length is rejected:
    with pytest.raises(ClientError, match='ValidationException.*dimension'):
        client.search_vectors(
            TableName=table_vs.name,
            IndexName='vind',
            SearchVector=[1, 2],
            TopK=1
        )
    with pytest.raises(ClientError, match='ValidationException.*dimension'):
        client.search_vectors(
            TableName=table_vs.name,
            IndexName='vind',
            SearchVector=[1, 2, 3, 4],
            TopK=1
        )

# Tests for the SearchVectors request's TopK parameter, which determines how
# many nearest neighbors to return. This TopK is different from Query's "Limit" -
# Limit in Query is optional, and used for the pagination of results - while
# TopK is mandatory, and vector search does not support pagination.

# Test that TopK is mandatory in a SearchVectors request, and must be a
# positive integer:
def test_searchvectors_topk_bad(table_vs):
    client = table_vs.meta.client
    # TopK cannot be missing:
    with pytest.raises(ClientError, match='ValidationException.*TopK'):
        client.search_vectors(
            TableName=table_vs.name,
            IndexName='vind',
            SearchVector=[1, 2, 3],
        )
    # TopK must be a positive integer:
    for bad_topk in ['hello', 1.5, 0, -3]:
        with pytest.raises(ClientError, match='ValidationException.*TopK|SerializationException'):
            client.search_vectors(
                TableName=table_vs.name,
                IndexName='vind',
                SearchVector=[1, 2, 3],
                TopK=bad_topk
            )

# Both DynamoDB and Alternator have an upper limit for the allowed TopK in
# vector search, but this limit is different. We consider the fact we have
# a different limit acceptable.
def max_topk(dynamodb):
    if is_aws(dynamodb):
        return 100
    else:
        # In Alternator, this is max_vector_search_limit in
        # alternator/executor_read.cc, analogous to CQL's max_ann_query_limit
        # defined in cql3/statements/select_statement.hh.
        return 1000

# Test that SearchVectors does not allow a TopK above max_topk(). This
# limit also exists in CQL (max_ann_query_limit). TopK needs to be limited
# because vector search does not support pagination so a very large TopK
# would result in a very large single response page..
def test_searchvectors_topk_too_large(table_vs):
    client = table_vs.meta.client
    with pytest.raises(ClientError, match='ValidationException.*TopK'):
        client.search_vectors(
            TableName=table_vs.name,
            IndexName='vind',
            SearchVector=[1, 2, 3],
            TopK=max_topk(table_vs)+1,
        )

# Test that a SearchVectors request with TopK=max_topk()
# (the maximum allowed value) succeeds - it should not be rejected.
# The query may return fewer results than the limit (the table_vs fixture
# has few items, possibly none), but must not fail with a validation error.
def test_searchvectors_topk_at_max(table_vs, needs_vector_store):
    client = table_vs.meta.client
    client.search_vectors(
        TableName=table_vs.name,
        IndexName='vind',
        SearchVector=[1, 2, 3],
        TopK=max_topk(table_vs),
    )

# SearchVectors is *not* the same as Query, and does not support the same
# parameters. In particular, it does not support "Limit", "ConsistentRead",
# "ExclusiveStartKey", or "ScanIndexForward". However, DynamoDB does not
# generate an error if one of these is added to SearchVectors - it is silently
# ignored. Only boto3 guards against them. This test checks that indeed,
# boto3 rejects all of these parameters to SearchVectors. It's not a very
# useful test - it checks boto3, not the server, but it's useful as a
# reminder of the fact that these parameters are not supported (and if one
# day DynamoDB is updated to support one of them and boto3 is updated to
# allow it to - we'll notice.
def test_searchvectors_unsupported_query_params(table_vs):
    client = table_vs.meta.client
    for param, value in [
        ('Limit', 10),
        ('ConsistentRead', True),
        ('ExclusiveStartKey', {'p': 'x'}),
        ('ScanIndexForward', True),
    ]:
        with pytest.raises(KeyError, match=param):
            client.search_vectors(
                TableName=table_vs.name,
                IndexName='vind',
                SearchVector=[1, 2, 3],
                TopK=1,
                **{param: value}
            )

# Test that SearchVectors with ExpressionAttributeValues but no
# SearchConditionExpression at all (so none of ExpressionAttributeValues's
# entries can be used) is rejected.
def test_searchvectors_expression_attribute_values_without_expression(table_vs):
    client = table_vs.meta.client
    with pytest.raises(ClientError, match='ValidationException.*ExpressionAttributeValues'):
        client.search_vectors(
            TableName=table_vs.name,
            IndexName='vind',
            SearchVector=[1, 2, 3],
            TopK=1,
            ExpressionAttributeValues={':val1': 'a'},
        )

# Test that SearchVectors is rejected if ExpressionAttributeValues has an
# unused element, even when the SearchConditionExpression does use *some*
# of ExpressionAttributeValues's entries. Here ':val1' is used by the
# SearchConditionExpression, but ':val2' remains unused, and should still
# be flagged. We need a table whose vector index has a SearchSchema element
# of our own (table_vs's 'vind' index doesn't have one), so we have
# something meaningful to reference in the SearchConditionExpression - we
# just reuse the base table's own HASH key 'p' as the vector index's
# SearchSchema INLINE_FILTER element.
def test_searchvectors_unused_expression_attribute_values(dynamodb):
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
            VectorIndexes=[
                {'IndexName': 'vind',
                 'VectorAttribute': {'AttributeName': 'v'},
                 'Dimensions': 3,
                 'DistanceFunction': 'COSINE',
                 'Projection': {'ProjectionType': 'KEYS_ONLY'},
                 'SearchSchema': [
                     {'AttributeName': 'p', 'SearchSchemaElementType': 'INLINE_FILTER'}
                 ],
                }
            ]) as table:
        client = table.meta.client
        with pytest.raises(ClientError, match='ValidationException.*val2'):
            client.search_vectors(
                TableName=table.name,
                IndexName='vind',
                SearchVector=[1, 2, 3],
                TopK=1,
                SearchConditionExpression='p = :val1',
                ExpressionAttributeValues={':val1': 'a', ':val2': 'b'},
            )

# Test that SearchVectors with ExpressionAttributeNames but no expression at
# all (so none of ExpressionAttributeNames's entries can be used) is
# rejected.
def test_searchvectors_expression_attribute_names_without_expression(table_vs):
    client = table_vs.meta.client
    with pytest.raises(ClientError, match='ValidationException.*ExpressionAttributeNames'):
        client.search_vectors(
            TableName=table_vs.name,
            IndexName='vind',
            SearchVector=[1, 2, 3],
            TopK=1,
            ExpressionAttributeNames={'#name1': 'x'},
        )

# Test that SearchVectors is rejected if ExpressionAttributeNames has an
# unused element, even when the ProjectionExpression does use *some* of
# ExpressionAttributeNames's entries. Here '#name1' is used by the
# ProjectionExpression, but '#name2' remains unused, and should still be
# flagged.
def test_searchvectors_projectionexpression_unused_expression_attribute_names(table_vs):
    client = table_vs.meta.client
    with pytest.raises(ClientError, match='ValidationException.*name2'):
        client.search_vectors(
            TableName=table_vs.name,
            IndexName='vind',
            SearchVector=[1, 2, 3],
            TopK=1,
            ProjectionExpression='#name1',
            ExpressionAttributeNames={'#name1': 'x', '#name2': 'y'},
        )

# Timeout (in seconds) used by the retry loops in tests that wait for the
# vector store to index data. Centralized here so it can be adjusted easily.
VECTOR_STORE_TIMEOUT = 20

# Repeatedly calls client.search_vectors(**kwargs) until condition(result) is
# true, or `timeout` seconds elapse. While waiting, ClientError is tolerated
# (this can happen if the caller did not wait_for_vector_index_active() and
# the is still). But if the deadline is reached without condition() ever being
# satisfied, we fail() with `message` (a string, or a callable taking the last
# result - possibly None, if every attempt raised - and returning a string),
# with the last exception (if any) appended. This way a persistent,
# non-transient error (e.g. a typo in a SearchConditionExpression) is not
# silently swallowed.
def wait_for_search_vectors(client, condition, message, timeout=VECTOR_STORE_TIMEOUT, sleep=0.1, **kwargs):
    deadline = time.monotonic() + timeout
    last_exception = None
    result = None
    while True:
        try:
            result = client.search_vectors(**kwargs)
            if condition(result):
                return result
        except ClientError as e:
            last_exception = e
        if time.monotonic() > deadline:
            msg = message(result) if callable(message) else message
            if last_exception is not None:
                msg += f' (last error: {last_exception})'
            pytest.fail(msg)
        time.sleep(sleep)

# Test that a SearchVectors request returns the nearest-neighbour item.
# The vector store is eventually consistent: after put_item the ANN index
# takes time to reflect the new item, so we retry until it appears.
# A private table is used to avoid other tests' data interfering with the
# TopK=1 result. Data is inserted before the index is created so the
# vector store picks by prefill scan rather than CDC.
def test_searchvectors_prefill(dynamodb, needs_vector_store):
    if is_aws(dynamodb):
        # Any test using prefill is extremely slow on DynamoDB, often taking
        # as much as 20 minutes to index even a table with a single item :-(
        timeout = 1800
        sleep = 3
    else:
        timeout = VECTOR_STORE_TIMEOUT
        sleep = 0.1
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
        client = table.meta.client
        p = random_string()
        table.put_item(Item={'p': p, 'v': [1, 0, 0]})
        table.update(VectorIndexUpdates=[{'Create':
            {'IndexName': 'vind',
             'VectorAttribute': {'AttributeName': 'v'},
             'Dimensions': 3,
             'DistanceFunction': 'COSINE',
             'Projection': {'ProjectionType': 'KEYS_ONLY'}}}])
        wait_for_search_vectors(client,
            condition=lambda result: result.get('SearchResults') and result['SearchResults'][0]['Item']['p'] == p,
            message='Timed out waiting for vector store to return the expected item',
            timeout=timeout, sleep=sleep,
            TableName=table.name, IndexName='vind', SearchVector=[1, 0, 0], TopK=1)

# Same as test_searchvectors_prefill but for a table with a clustering key,
# which exercises the separate code path in query_vector() for hash+range
# tables.
def test_searchvectors_with_ck_prefill(dynamodb, needs_vector_store):
    if is_aws(dynamodb):
        # Any test using prefill is extremely slow on DynamoDB, often taking
        # as much as 20 minutes to index even a table with a single item :-(
        timeout = 1800
        sleep = 3
    else:
        timeout = VECTOR_STORE_TIMEOUT
        sleep = 0.1
    with new_test_table(dynamodb,
            KeySchema=[
                {'AttributeName': 'p', 'KeyType': 'HASH'},
                {'AttributeName': 'c', 'KeyType': 'RANGE'}],
            AttributeDefinitions=[
                {'AttributeName': 'p', 'AttributeType': 'S'},
                {'AttributeName': 'c', 'AttributeType': 'S'}]) as table:
        client = table.meta.client
        p = random_string()
        c = random_string()
        table.put_item(Item={'p': p, 'c': c, 'v': [1, 0, 0]})
        table.update(VectorIndexUpdates=[{'Create':
            {'IndexName': 'vind',
             'VectorAttribute': {'AttributeName': 'v'},
             'Dimensions': 3,
             'DistanceFunction': 'COSINE',
             'Projection': {'ProjectionType': 'KEYS_ONLY'}}}])
        wait_for_search_vectors(client,
            condition=lambda result: result.get('SearchResults') and result['SearchResults'][0]['Item']['p'] == p
                and result['SearchResults'][0]['Item']['c'] == c,
            message='Timed out waiting for vector store to return the expected item',
            timeout=timeout, sleep=sleep,
            TableName=table.name, IndexName='vind', SearchVector=[1, 0, 0], TopK=1)

# Utility function for waiting until the given vector index is ACTIVE, which
# means that when this function returns, we are guaranteed that:
#  1. Queries on this index will succeed.
#  2. The prefill scan of the existing table data has completed, so all items
#     that existed in the table before the index was created have been indexed.
# This function uses DescribeTable and waits for the index's IndexStatus to
# become "ACTIVE". This is more elgant than waiting for an actual Query to
# succeed, and also doesn't require knowing the dimensions of this index to
# attempt a real Query.
def wait_for_vector_index_active(table, index_name):
    # On DynamoDB, just like adding a GSI (see wait_for_gsi() in util.py),
    # backfilling a new vector index added to an existing table can take an
    # absurdly long amount of time, even for a tiny table - much longer than
    # what it would take to create a new table with an empty index.
    # We measured times close to 20 minutes!
    # So we need a very long timeout when is_aws(table).
    if is_aws(table):
        timeout = 1800
        delay = 3
    else:
        timeout = VECTOR_STORE_TIMEOUT
        delay = 0.1
    deadline = time.monotonic() + timeout
    while True:
        desc = table.meta.client.describe_table(TableName=table.name)
        for vi in desc.get('Table', {}).get('VectorIndexes', []):
            if vi['IndexName'] == index_name and vi['IndexStatus'] == 'ACTIVE':
                return
        if time.monotonic() > deadline:
            pytest.fail(f'Timed out waiting for vector index "{index_name}" to become ACTIVE')
        time.sleep(delay)

# Test that wait_for_vector_index_active(), waiting for IndexStatus==ACTIVE,
# indeed reliably waits for the index to be ready. A Query issued immediately
# after wait_for_vector_index_active() returns should succeed without any
# retry loop, and also returns the prefilled data.
def test_wait_for_vector_index_active(dynamodb, needs_vector_store):
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
        client = table.meta.client
        p = random_string()
        table.put_item(Item={'p': p, 'v': [1, 0, 0]})
        table.update(VectorIndexUpdates=[{'Create':
            {'IndexName': 'vind',
             'VectorAttribute': {'AttributeName': 'v'},
             'Dimensions': 3,
             'DistanceFunction': 'COSINE',
             'Projection': {'ProjectionType': 'KEYS_ONLY'}}}])
        wait_for_vector_index_active(table, 'vind')
        # The index is now ACTIVE: the prefill scan has completed and the
        # item we inserted is guaranteed to be indexed. Call SearchVectors
        # without catching exceptions or retrying.
        result = client.search_vectors(
            TableName=table.name,
            IndexName='vind',
            SearchVector=[1, 0, 0],
            TopK=1
        )
        results = result.get('SearchResults')
        assert results and results[0]['Item']['p'] == p

# Test that when a table and vector index is created CreateTable, the
# "Backfilling" flag never appears in DescribeTable calls at any point.
# Note that users should still wait for the Index's status to change
# from CREATING to ACTIVE (like our wait_for_vector_index_active() does).
def test_describetable_vectorindex_backfilling_createtable(dynamodb):
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
            VectorIndexes=[{
                'IndexName': 'vind',
                'VectorAttribute': {'AttributeName': 'v'},
                'Dimensions': 3,
                'DistanceFunction': 'COSINE',
                'Projection': {'ProjectionType': 'KEYS_ONLY'},
            }]) as table:
        client = table.meta.client
        timeout = 1800 if is_aws(dynamodb) else VECTOR_STORE_TIMEOUT
        deadline = time.monotonic() + timeout
        while True:
            desc = client.describe_table(TableName=table.name)
            vi = desc['Table']['VectorIndexes'][0]
            assert 'Backfilling' not in vi
            if vi['IndexStatus'] == 'ACTIVE':
                break
            if time.monotonic() > deadline:
                pytest.fail('Timed out waiting for vector index to become ACTIVE')
            time.sleep(0.01 if not is_aws(dynamodb) else 3)

# Test that when a vector index is added to an *existing* table via
# UpdateTable, the "Backfilling" flag may appears in DescribeTable,
# while IndexStatus is CREATING. When IndexStatus reaches ACTIVE, the
# "Backfilling" flag must no longer be reported.
# We poll tightly right after issuing the UpdateTable, to try to catch
# the index while it's actively backfilling and confirm Backfilling is then
# true - the documentation promises this is possible even for a table with
# very little data: "Index construction drives backfill duration, not the
# number of items in the base table. Even a table with very few items can
# take a substantial amount of time to finish backfilling.". In practice,
# backfilling on DynamoDB often takes as much as 20 minutes.
def test_describetable_vectorindex_backfilling_updatetable(dynamodb, needs_vector_store):
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
        client = table.meta.client
        p = random_string()
        table.put_item(Item={'p': p, 'v': [1, 0, 0]})
        table.update(VectorIndexUpdates=[{'Create':
            {'IndexName': 'vind',
             'VectorAttribute': {'AttributeName': 'v'},
             'Dimensions': 3,
             'DistanceFunction': 'COSINE',
             'Projection': {'ProjectionType': 'KEYS_ONLY'}}}])
        timeout = 1800 if is_aws(dynamodb) else VECTOR_STORE_TIMEOUT
        sleep = 0.01 if not is_aws(dynamodb) else 3
        deadline = time.monotonic() + timeout
        seen_backfilling_true = False
        while True:
            desc = client.describe_table(TableName=table.name)
            vi = desc['Table']['VectorIndexes'][0]  # only one vector index in this test
            if vi['IndexStatus'] == 'CREATING':
                # While CREATING, Backfilling is either absent/false (still
                # provisioning the index) or true (actively backfilling) -
                # never anything else.
                assert vi.get('Backfilling') in (None, False, True)
                if vi.get('Backfilling') is True:
                    seen_backfilling_true = True
            else:
                assert vi['IndexStatus'] == 'ACTIVE'
                # Once ACTIVE, Backfilling must no longer be reported at all.
                assert 'Backfilling' not in vi
                break
            if time.monotonic() > deadline:
                pytest.fail('Timed out waiting for vector index to become ACTIVE')
            time.sleep(sleep)
        # TODO: this test may be flaky on Alternator if we can't catch backfilling
        # "in the act", we may need to "inject" flowness into backfilling, or skip
        # this test. On DynamoDB, it will definitely pass because of the extreme
        # slowness of backfilling - often close to 20 minutes.
        assert seen_backfilling_true

# The tests test_query_vector_prefill and test_query_vector_with_ck_prefill
# used string keys in the indexed table. In theory, there shouldn't be any
# difference in the vector store's behavior if the keys are of a different
# type (in addition to string, they can be numeric or binary). But in
# practice, the factor store does handle different key types differently,
# and this test used to fail before this was fixed.
# To save a bit of time, we don't test all combinations of hash and range
# key types but test each type at least once as a hash key and a range key.
@pytest.mark.parametrize('hash_type,range_type', [
    ('N', None), ('B', None), ('S', 'N'),  ('S', 'B'),
], ids=[
    'N', 'B', 'SN', 'SB'])
def test_searchvectors_prefill_key_types(dynamodb, needs_vector_store, hash_type, range_type):
    key_schema = [{'AttributeName': 'p', 'KeyType': 'HASH'}]
    attr_defs = [{'AttributeName': 'p', 'AttributeType': hash_type}]
    if range_type is not None:
        key_schema.append({'AttributeName': 'c', 'KeyType': 'RANGE'})
        attr_defs.append({'AttributeName': 'c', 'AttributeType': range_type})
    key = {'S': 'hello', 'N': Decimal('42'), 'B': b'hello'}
    with new_test_table(dynamodb, KeySchema=key_schema,
                            AttributeDefinitions=attr_defs) as table:
        client = table.meta.client
        p = key[hash_type]
        item = {'p': p, 'v': [1, 0, 0]}
        if range_type is not None:
            c = key[range_type]
            item['c'] = c
        table.put_item(Item=item)
        table.update(VectorIndexUpdates=[{'Create':
            {'IndexName': 'vind',
             'VectorAttribute': {'AttributeName': 'v'},
             'Dimensions': 3,
             'DistanceFunction': 'COSINE',
             'Projection': {'ProjectionType': 'KEYS_ONLY'}}}])
        wait_for_vector_index_active(table, 'vind')
        result = client.search_vectors(
            TableName=table.name,
            IndexName='vind',
            SearchVector=[1, 0, 0],
            TopK=1)
        results = result.get('SearchResults', [])
        assert len(results) == 1 and results[0]['Item']['p'] == p
        if range_type is not None:
            assert results[0]['Item']['c'] == c

# Same as test_query_vector_prefill but whereas in test_query_vector_prefill
# the vector store reads the indexed data by scanning the table, here the
# vector index is created first and only later the data is written, so the
# vector store is expected to pick it up via CDC.
def test_searchvectors_cdc(dynamodb, needs_vector_store):
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
            VectorIndexes=[
                {'IndexName': 'vind',
                 'VectorAttribute': {'AttributeName': 'v'},
                 'Dimensions': 3,
                 'DistanceFunction': 'COSINE',
                 'Projection': {'ProjectionType': 'KEYS_ONLY'}}
            ]) as table:
        client = table.meta.client
        # Wait until the vector store is ready (prefill of the empty table
        # has completed), to ensure the subsequent write is picked up via CDC.
        wait_for_vector_index_active(table, 'vind')
        # Now write the item. It should reach the vector store via CDC.
        p = random_string()
        table.put_item(Item={'p': p, 'v': [1, 0, 0]})
        # Retry SearchVectors until the newly written item appears in the results.
        wait_for_search_vectors(client,
            condition=lambda result: result.get('SearchResults') and result['SearchResults'][0]['Item']['p'] == p,
            message='Timed out waiting for vector store to return the expected item via CDC',
            TableName=table.name, IndexName='vind', SearchVector=[1, 0, 0], TopK=1)

# Similar test to test_searchvectors_cdc, where an item is written after the
# vector index is created, but here the item is written using LWT (using a
# ConditionExpression that causes the request to be a read-modify-write
# operation so need to use LWT for most write isolation modes). This is
# important to test because LWT has different code path for recognizing we
# need to write to the CDC log).
def test_searchvectors_cdc_lwt(dynamodb, needs_vector_store):
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
            VectorIndexes=[
                {'IndexName': 'vind',
                 'VectorAttribute': {'AttributeName': 'v'},
                 'Dimensions': 3,
                 'DistanceFunction': 'COSINE',
                 'Projection': {'ProjectionType': 'KEYS_ONLY'}}
            ]) as table:
        client = table.meta.client
        wait_for_vector_index_active(table, 'vind')
        # Write the item, with a ConditionExpression to guarantee LWT.
        p = random_string()
        table.put_item(Item={'p': p, 'v': [1, 0, 0]},
            ConditionExpression='attribute_not_exists(p)')
        result = wait_for_search_vectors(client,
            condition=lambda result: len(result.get('SearchResults', [])) > 0,
            message='Timed out waiting for vector store index an item via CDC',
            TableName=table.name, IndexName='vind', SearchVector=[1, 0, 0], TopK=1)
        results = result['SearchResults']
        assert len(results) == 1 and results[0]['Item']['p'] == p

# Similar test to test_searchvectors_cdc, where an item is written after the
# vector index is created, but here two items are written using BatchWriteItem.
def test_searchvectors_cdc_batchwriteitem(dynamodb, needs_vector_store):
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
            VectorIndexes=[
                {'IndexName': 'vind',
                 'VectorAttribute': {'AttributeName': 'v'},
                 'Dimensions': 3,
                 'DistanceFunction': 'COSINE',
                 'Projection': {'ProjectionType': 'KEYS_ONLY'}}
            ]) as table:
        client = table.meta.client
        wait_for_vector_index_active(table, 'vind')
        # Write two items via a single BatchWriteItem. They should reach
        # the vector store via CDC.
        p1 = random_string()
        p2 = random_string()
        with table.batch_writer() as batch:
            batch.put_item(Item={'p': p1, 'v': [1, 0, 0]})
            batch.put_item(Item={'p': p2, 'v': [0, 1, 0]})
        # Retry SearchVectors until any 2 results appear, then assert (just
        # once, outside the loop) that they are the expected items.
        result = wait_for_search_vectors(client,
            condition=lambda result: len(result.get('SearchResults', [])) == 2,
            message=lambda result: f'Timed out waiting for 2 items to appear via CDC, '
                f'got {len(result.get("SearchResults", [])) if result else 0}',
            TableName=table.name, IndexName='vind', SearchVector=[1, 0, 0], TopK=2)
        assert {item['Item']['p'] for item in result['SearchResults']} == {p1, p2}

# Similar test to test_searchvectors_cdc, where an item is written after the
# vector index is created, but here two items are written using TransactWriteItems.
def test_searchvectors_cdc_transactwriteitems(dynamodb, needs_vector_store):
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
            VectorIndexes=[
                {'IndexName': 'vind',
                 'VectorAttribute': {'AttributeName': 'v'},
                 'Dimensions': 3,
                 'DistanceFunction': 'COSINE',
                 'Projection': {'ProjectionType': 'KEYS_ONLY'}}
            ]) as table:
        client = table.meta.client
        wait_for_vector_index_active(table, 'vind')
        # Now write two items via a single TransactWriteItems. They should
        # reach the vector store via CDC.
        p1 = random_string()
        p2 = random_string()
        client.transact_write_items(TransactItems=[
            {'Put': {'TableName': table.name, 'Item': {'p': p1, 'v': [1, 0, 0]}}},
            {'Put': {'TableName': table.name, 'Item': {'p': p2, 'v': [0, 1, 0]}}},
        ])
        # Retry SearchVectors until any 2 results appear, then assert (just
        # once, outside the loop) that they are the expected items - if a bug
        # makes the wrong items appear, we'll find out immediately instead of
        # looping until the timeout.
        result = wait_for_search_vectors(client,
            condition=lambda result: len(result.get('SearchResults', [])) == 2,
            message=lambda result: f'Timed out waiting for 2 items to appear via CDC, '
                f'got {len(result.get("SearchResults", [])) if result else 0}',
            TableName=table.name, IndexName='vind', SearchVector=[1, 0, 0], TopK=2)
        assert {item['Item']['p'] for item in result['SearchResults']} == {p1, p2}

# Similar to test_searchvectors_cdc, this test also checks that SearchVectors
# finds data inserted after the index was created. But this test adds a
# twist: before creating the index, we insert a malformed value for the
# vector attribute (a string or wrong-length vector). We check that this
# malformed value is ignored by the initial prefill scan, but should not
# prevent a later write with a well-formed vector from being indexed and
# returned by SearchVectors.
@pytest.mark.parametrize('use_update_item', [False, True], ids=['put_item', 'update_item'])
@pytest.mark.parametrize('malformed', ['garbage', [1,2]], ids=['string','wrong_length'])
def test_searchvectors_cdc_malformed_prefill(dynamodb, needs_vector_store, malformed, use_update_item):
    if is_aws(dynamodb):
        # Any test using prefill is extremely slow on DynamoDB, often taking
        # as much as 20 minutes to index even a table with a single item :-(
        timeout = 1800
        sleep = 3
    else:
        timeout = VECTOR_STORE_TIMEOUT
        sleep = 0.1
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
        client = table.meta.client
        # Vector index is not yet enabled, so we can insert a string as the
        # value of v, without validation.
        p1 = random_string()
        table.put_item(Item={'p': p1, 'v': malformed})
        # Insert another item with a proper vector
        p2 = random_string()
        table.put_item(Item={'p': p2, 'v': [1, 0, 0]})
        # Now create the vector index. The prefill scan will encounter the
        # malformed item and must silently ignore it.
        table.update(VectorIndexUpdates=[{'Create':
            {'IndexName': 'vind',
             'VectorAttribute': {'AttributeName': 'v'},
             'Dimensions': 3,
             'DistanceFunction': 'COSINE',
             'Projection': {'ProjectionType': 'KEYS_ONLY'}}}])
        # Wait for the prefill scan to complete (index becomes ACTIVE).
        wait_for_vector_index_active(table, 'vind')
        # At this point only p2 should be indexed and returned by SearchVectors
        result = client.search_vectors(TableName=table.name, IndexName='vind',
                                        SearchVector=[1, 0, 0], TopK=10)
        results = result['SearchResults']
        assert len(results) == 1 and results[0]['Item']['p'] == p2
        # Now replace the value of p1 by a properly formed vector. It should
        # be eventually picked up by CDC and indexed by the vector index:
        if use_update_item:
            table.update_item(Key={'p': p1},
                UpdateExpression='SET v = :v',
                ExpressionAttributeValues={':v': [1, Decimal("0.1"), 0]})
        else:
            table.put_item(Item={'p': p1, 'v': [1, Decimal("0.1"), 0]})
        get_ps = lambda result: {item['Item']['p'] for item in result['SearchResults']}
        wait_for_search_vectors(client,
            condition=lambda result: get_ps(result) == {p2, p1},
            message=lambda result: f'Timed out waiting for both items via CDC, got {get_ps(result) if result else None}',
            timeout=timeout, sleep=sleep,
            TableName=table.name, IndexName='vind', SearchVector=[1, 0, 0], TopK=10)

# Test like test_searchvectors_prefill, but with a search returning multiple
# results. This helps us verify that:
#  1. "TopK" determines the number of results.
#  2. The query_vector() code correctly handles the need to read and
#     return multiple items.
#  3. The multiple results are correctly sorted by distance (nearest first).
def test_searchvectors_multiple_results(dynamodb, needs_vector_store):
    if is_aws(dynamodb):
        # Any test using prefill is extremely slow on DynamoDB, often taking
        # as much as 20 minutes to index even a table with a single item :-(
        timeout = 1800
        sleep = 3
    else:
        timeout = VECTOR_STORE_TIMEOUT
        sleep = 0.1
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
        client = table.meta.client
        # Insert 4 items at known cosine distances from the query vector [1, 0, 0]:
        #   p1 at [1, 0, 0]     - cosine distance 0   (closest, identical direction)
        #   p2 at [1, 0.1, 0]   - cosine distance ~0.005 (2nd, slightly off-axis)
        #   p3 at [0, 1, 0]     - cosine distance 1 (3rd, orthogonal)
        #   p4 at [-1, 0, 0]    - cosine distance 2 (farthest, opposite direction)
        # Data is inserted before the vector index is created so the vector
        # store picks it up via scan rather than CDC, which finishes faster.
        p1, p2, p3, p4 = random_string(), random_string(), random_string(), random_string()
        table.put_item(Item={'p': p1, 'v': [Decimal("1"),   Decimal("0"),   Decimal("0")]})
        table.put_item(Item={'p': p2, 'v': [Decimal("1"),   Decimal("0.1"), Decimal("0")]})
        table.put_item(Item={'p': p3, 'v': [Decimal("0"),   Decimal("1"),   Decimal("0")]})
        table.put_item(Item={'p': p4, 'v': [Decimal("-1"),  Decimal("0"),   Decimal("0")]})
        table.update(VectorIndexUpdates=[{'Create':
            {'IndexName': 'vind',
             'VectorAttribute': {'AttributeName': 'v'},
             'Dimensions': 3,
             'DistanceFunction': 'COSINE',
             'Projection': {'ProjectionType': 'KEYS_ONLY'}}}])
        expected_order = [p1, p2, p3]
        get_got = lambda result: [r['Item']['p'] for r in result.get('SearchResults', []) if r['Item']['p'] in {p1, p2, p3, p4}]
        wait_for_search_vectors(client,
            condition=lambda result: get_got(result) == expected_order,
            message=lambda result: f'Timed out waiting for correct ordered results; '
                f'last got: {get_got(result) if result else None}, expected {expected_order}',
            timeout=timeout, sleep=sleep,
            TableName=table.name, IndexName='vind',
            SearchVector=[Decimal("1"), Decimal("0"), Decimal("0")], TopK=3)

# Same as test_searchvectors_multiple_results but for a table with a
# clustering key, to exercise the hash+range code path in query_vector().
def test_searchvectors_with_ck_multiple_results(dynamodb, needs_vector_store):
    if is_aws(dynamodb):
        # Any test using prefill is extremely slow on DynamoDB, often taking
        # as much as 20 minutes to index even a table with a single item :-(
        timeout = 1800
        sleep = 3
    else:
        timeout = VECTOR_STORE_TIMEOUT
        sleep = 0.1
    with new_test_table(dynamodb,
            KeySchema=[
                {'AttributeName': 'p', 'KeyType': 'HASH'},
                {'AttributeName': 'c', 'KeyType': 'RANGE'}],
            AttributeDefinitions=[
                {'AttributeName': 'p', 'AttributeType': 'S'},
                {'AttributeName': 'c', 'AttributeType': 'S'}]) as table:
        client = table.meta.client
        p1, p2, p3, p4 = random_string(), random_string(), random_string(), random_string()
        c1, c2, c3, c4 = random_string(), random_string(), random_string(), random_string()
        table.put_item(Item={'p': p1, 'c': c1, 'v': [Decimal("1"),   Decimal("0"),   Decimal("0")]})
        table.put_item(Item={'p': p2, 'c': c2, 'v': [Decimal("1"),   Decimal("0.1"), Decimal("0")]})
        table.put_item(Item={'p': p3, 'c': c3, 'v': [Decimal("0"),   Decimal("1"),   Decimal("0")]})
        table.put_item(Item={'p': p4, 'c': c4, 'v': [Decimal("-1"),  Decimal("0"),   Decimal("0")]})
        table.update(VectorIndexUpdates=[{'Create':
            {'IndexName': 'vind',
             'VectorAttribute': {'AttributeName': 'v'},
             'Dimensions': 3,
             'DistanceFunction': 'COSINE',
             'Projection': {'ProjectionType': 'KEYS_ONLY'}}}])
        expected_order = [(p1, c1), (p2, c2), (p3, c3)]
        pcs = {(p1, c1), (p2, c2), (p3, c3), (p4, c4)}
        get_got = lambda result: [(r['Item']['p'], r['Item']['c']) for r in result.get('SearchResults', []) if (r['Item']['p'], r['Item']['c']) in pcs]
        wait_for_search_vectors(client,
            condition=lambda result: get_got(result) == expected_order,
            message=lambda result: f'Timed out waiting for correct ordered results; '
                f'last got: {get_got(result) if result else None}, expected {expected_order}',
            timeout=timeout, sleep=sleep,
            TableName=table.name, IndexName='vind',
            SearchVector=[Decimal("1"), Decimal("0"), Decimal("0")], TopK=3)

# Test that PutItem rejects a vector attribute value that is invalid for
# the declared vector index on that attribute. The index on table_vs declares
# attribute 'v' as a 3-dimensional vector, so putting a non-list, a list of
# wrong length, a list with non-numeric elements, or a list containing a
# number that cannot be represented as a float must all be rejected.
#
# Note that this write rejection feature is nice to have (and mirrors what
# happens in GSI where writes with the wrong type for the indexed column
# are rejected), but was not really necessary: We could have allowed writes
# with the wrong type, and items with a wrong type would simply be ignored
# by the vector index and not returned in vector search results.
def test_putitem_vectorindex_bad_vector(table_vs):
    p = random_string()
    # Not a list - should be rejected:
    with pytest.raises(ClientError, match='ValidationException'):
        table_vs.put_item(Item={'p': p, 'v': 'not a list'})
    # A single number scalar (N), instead of a list of numbers - should be
    # rejected:
    with pytest.raises(ClientError, match='ValidationException'):
        table_vs.put_item(Item={'p': p, 'v': 5})
    # A number set (NS), instead of a list of numbers - should be rejected:
    with pytest.raises(ClientError, match='ValidationException'):
        table_vs.put_item(Item={'p': p, 'v': {1, 2, 3}})
    # A list of the wrong length - should be rejected:
    with pytest.raises(ClientError, match='ValidationException'):
        table_vs.put_item(Item={'p': p, 'v': [1, 2]})
    with pytest.raises(ClientError, match='ValidationException'):
        table_vs.put_item(Item={'p': p, 'v': [1, 2, 3, 4]})
    # A list of the right length but with a non-numeric element - should be rejected:
    with pytest.raises(ClientError, match='ValidationException'):
        table_vs.put_item(Item={'p': p, 'v': [1, 'hello', 3]})
    # A list whose numeric elements can't be represented as a 32-bit float
    # (value out of float range) - should be rejected:
    with pytest.raises(ClientError, match='ValidationException'):
        table_vs.put_item(Item={'p': p, 'v': [1, Decimal('1e100'), 3]})

# Same as test_putitem_vectorindex_bad_vector but using UpdateItem.
def test_updateitem_vectorindex_bad_vector(table_vs):
    p = random_string()
    # Not a list - should be rejected:
    with pytest.raises(ClientError, match='ValidationException'):
        table_vs.update_item(Key={'p': p},
            UpdateExpression='SET v = :val',
            ExpressionAttributeValues={':val': 'not a list'})
    # A single number scalar (N), instead of a list of numbers - should be
    # rejected:
    with pytest.raises(ClientError, match='ValidationException'):
        table_vs.update_item(Key={'p': p},
            UpdateExpression='SET v = :val',
            ExpressionAttributeValues={':val': 5})
    # A number set (NS), instead of a list of numbers - should be rejected:
    with pytest.raises(ClientError, match='ValidationException'):
        table_vs.update_item(Key={'p': p},
            UpdateExpression='SET v = :val',
            ExpressionAttributeValues={':val': {1, 2, 3}})
    # A list of the wrong length - should be rejected:
    with pytest.raises(ClientError, match='ValidationException'):
        table_vs.update_item(Key={'p': p},
            UpdateExpression='SET v = :val',
            ExpressionAttributeValues={':val': [1, 2]})
    with pytest.raises(ClientError, match='ValidationException'):
        table_vs.update_item(Key={'p': p},
            UpdateExpression='SET v = :val',
            ExpressionAttributeValues={':val': [1, 2, 3, 4]})
    # A list of the right length but with a non-numeric element - should be rejected:
    with pytest.raises(ClientError, match='ValidationException'):
        table_vs.update_item(Key={'p': p},
            UpdateExpression='SET v = :val',
            ExpressionAttributeValues={':val': [1, 'hello', 3]})
    # A list whose numeric elements can't be represented as a 32-bit float
    # (value out of float range) - should be rejected:
    with pytest.raises(ClientError, match='ValidationException'):
        table_vs.update_item(Key={'p': p},
            UpdateExpression='SET v = :val',
            ExpressionAttributeValues={':val': [1, Decimal('1e100'), 3]})

# Same as test_putitem_vectorindex_bad_vector but using BatchWriteItem.
def test_batchwriteitem_vectorindex_bad_vector(table_vs):
    p = random_string()
    # Not a list - should be rejected:
    with pytest.raises(ClientError, match='ValidationException'):
        with table_vs.batch_writer() as batch:
            batch.put_item(Item={'p': p, 'v': 'not a list'})
    # A single number scalar (N), instead of a list of numbers - should be
    # rejected:
    with pytest.raises(ClientError, match='ValidationException'):
        with table_vs.batch_writer() as batch:
            batch.put_item(Item={'p': p, 'v': 5})
    # A number set (NS), instead of a list of numbers - should be rejected:
    with pytest.raises(ClientError, match='ValidationException'):
        with table_vs.batch_writer() as batch:
            batch.put_item(Item={'p': p, 'v': {1, 2, 3}})
    # A list of the wrong length - should be rejected:
    with pytest.raises(ClientError, match='ValidationException'):
        with table_vs.batch_writer() as batch:
            batch.put_item(Item={'p': p, 'v': [1, 2]})
    with pytest.raises(ClientError, match='ValidationException'):
        with table_vs.batch_writer() as batch:
            batch.put_item(Item={'p': p, 'v': [1, 2, 3, 4]})
    # A list of the right length but with a non-numeric element - should be rejected:
    with pytest.raises(ClientError, match='ValidationException'):
        with table_vs.batch_writer() as batch:
            batch.put_item(Item={'p': p, 'v': [1, 'hello', 3]})
    # A list whose numeric elements can't be represented as a 32-bit float
    # (value out of float range) - should be rejected:
    with pytest.raises(ClientError, match='ValidationException'):
        with table_vs.batch_writer() as batch:
            batch.put_item(Item={'p': p, 'v': [1, Decimal('1e100'), 3]})

# If one item in the batch is valid and another is invalid, the entire
# batch should be rejected and neither item should be inserted:
def test_batchwriteitem_vectorindex_bad_and_good(table_vs):
    p_good = random_string()
    p_bad = random_string()
    with pytest.raises(ClientError, match='ValidationException'):
        with table_vs.batch_writer() as batch:
            batch.put_item(Item={'p': p_good, 'v': [1, 2, 3]})
            batch.put_item(Item={'p': p_bad,  'v': 'not a list'})
    assert 'Item' not in table_vs.get_item(Key={'p': p_good}, ConsistentRead=True)
    assert 'Item' not in table_vs.get_item(Key={'p': p_bad},  ConsistentRead=True)

# Test that DeleteItem removes the item from the vector index.
# Two variants are tested via parametrize:
# - without clustering key (no_ck): deleting the only item in a partition
#   generates a partition tombstone in CDC
# - with clustering key (with_ck): deleting a row generates a row tombstone
#   in CDC, which is a different code path
@pytest.mark.parametrize('with_ck', [False, True], ids=['no_ck', 'with_ck'])
def test_deleteitem_vectorindex(dynamodb, needs_vector_store, with_ck):
    key_schema = [{'AttributeName': 'p', 'KeyType': 'HASH'}]
    attr_defs = [{'AttributeName': 'p', 'AttributeType': 'S'}]
    if with_ck:
        key_schema.append({'AttributeName': 'c', 'KeyType': 'RANGE'})
        attr_defs.append({'AttributeName': 'c', 'AttributeType': 'S'})
    with new_test_table(dynamodb,
            KeySchema=key_schema,
            AttributeDefinitions=attr_defs,
            VectorIndexes=[
                {'IndexName': 'vind',
                 'VectorAttribute': {'AttributeName': 'v'},
                 'Dimensions': 3,
                 'DistanceFunction': 'COSINE',
                 'Projection': {'ProjectionType': 'KEYS_ONLY'}}
            ]) as table:
        client = table.meta.client
        # Wait until the vector store is ready (empty table prefill done).
        wait_for_vector_index_active(table, 'vind')
        # Write the item and wait for it to appear in the vector index.
        p = random_string()
        item = {'p': p, 'v': [1, 0, 0]}
        key = {'p': p}
        if with_ck:
            c = random_string()
            item['c'] = c
            key['c'] = c
        table.put_item(Item=item)
        result = wait_for_search_vectors(client,
            condition=lambda result: len(result.get('SearchResults', [])) > 0,
            message='Timed out waiting for item to appear in vector index',
            TableName=table.name, IndexName='vind', SearchVector=[1, 0, 0], TopK=1)
        assert result['SearchResults'][0]['Item']['p'] == p
        if with_ck:
            assert result['SearchResults'][0]['Item']['c'] == c
        # Delete the item and wait for it to disappear from the vector index.
        table.delete_item(Key=key)
        wait_for_search_vectors(client,
            condition=lambda result: len(result.get('SearchResults', [])) == 0,
            message='Timed out waiting for deleted item to disappear from vector index',
            TableName=table.name, IndexName='vind', SearchVector=[1, 0, 0], TopK=1)

# Test that PutItem or UpdateItem on an existing item replaces its vector in
# the index: the old vector is removed and the new one is indexed. Two items
# are inserted so we can verify the ordering changes after the replace.
@pytest.mark.parametrize('use_update_item', [False, True], ids=['put_item', 'update_item'])
def test_replace_vector_vectorindex(dynamodb, needs_vector_store, use_update_item):
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
        client = table.meta.client
        p1 = random_string()
        p2 = random_string()
        # Insert two different items and index them.
        table.put_item(Item={'p': p1, 'v': [1, 0, 0]})
        table.put_item(Item={'p': p2, 'v': [0, 1, 0]})
        table.update(VectorIndexUpdates=[{'Create':
            {'IndexName': 'vind',
             'VectorAttribute': {'AttributeName': 'v'},
             'Dimensions': 3,
             'DistanceFunction': 'COSINE',
             'Projection': {'ProjectionType': 'KEYS_ONLY'}}}])
        wait_for_vector_index_active(table, 'vind')
        # Initially, search [1, 0, 0] returns p1 first, p2 second.
        result = client.search_vectors(TableName=table.name, IndexName='vind',
                                        SearchVector=[1, 0, 0], TopK=2)
        assert [r['Item']['p'] for r in result['SearchResults']] == [p1, p2]
        # Replace p1's vector with [-1, 0, 0] (opposite direction, now farthest
        # from [1, 0, 0]), using either PutItem or UpdateItem.
        if use_update_item:
            table.update_item(Key={'p': p1},
                UpdateExpression='SET v = :v',
                ExpressionAttributeValues={':v': [-1, 0, 0]})
        else:
            table.put_item(Item={'p': p1, 'v': [-1, 0, 0]})
        # Wait until the index reflects the change: p2 should now come before p1
        # in a search for [1, 0, 0].
        get_got = lambda result: [r['Item']['p'] for r in result.get('SearchResults', [])]
        wait_for_search_vectors(client,
            condition=lambda result: get_got(result) == [p2, p1],
            message=lambda result: f'Timed out waiting for index to reflect replaced vector; '
                f'last order: {get_got(result) if result else None}',
            TableName=table.name, IndexName='vind', SearchVector=[1, 0, 0], TopK=2)

# Test that UpdateItem modifying non-vector attributes does not affect the
# vector index: the item remains discoverable by its unchanged vector.
# If the non-vector attribute 'x' being modified is projected into the
# index, the updated value of 'x' is also returned via ProjectionExpression.
@pytest.mark.parametrize('with_projection', [False, True], ids=['no_projection', 'with_projection'])
def test_updateitem_nonvector_vectorindex(dynamodb, needs_vector_store, with_projection):
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
        client = table.meta.client
        p = random_string()
        table.put_item(Item={'p': p, 'v': [1, 0, 0], 'x': 'before'})
        # Project 'x' into the index, in addition to the key and vector
        # attributes, so it can be requested back below.
        if with_projection:
            projection = {'ProjectionType': 'INCLUDE', 'NonKeyAttributes': ['x']}
        else:
            projection = {'ProjectionType': 'KEYS_ONLY'}
        table.update(VectorIndexUpdates=[{'Create':
            {'IndexName': 'vind',
             'VectorAttribute': {'AttributeName': 'v'},
             'Dimensions': 3,
             'DistanceFunction': 'COSINE',
             'Projection': projection}}])
        wait_for_vector_index_active(table, 'vind')
        # UpdateItem to change a non-vector attribute.
        table.update_item(Key={'p': p},
            UpdateExpression='SET x = :newx',
            ExpressionAttributeValues={':newx': 'after'})
        # The item should still be findable by its vector, which didn't change.
        # On DynamoDB, updating the projected 'x' attribute in the index is
        # not instantaneous, so we need to retry until the change propagates.
        search_kwargs = {'TableName': table.name, 'IndexName': 'vind', 'SearchVector': [1, 0, 0], 'TopK': 1}
        if with_projection:
            search_kwargs['ProjectionExpression'] = 'p, v, x'
        def matches(result):
            results = result['SearchResults']
            if len(results) != 1:
                return False
            if with_projection:
                return results[0]['Item'] == {'p': p, 'v': [1, 0, 0], 'x': 'after'}
            return results[0]['Item']['p'] == p
        wait_for_search_vectors(client, condition=matches,
            message='Timed out waiting for updated attribute to be reflected in vector index',
            **search_kwargs)

# Test that UpdateItem removing the vector attribute (but not the item itself)
# causes the item to be removed from the vector index. The item should still
# exist in the base table (readable via GetItem), but must no longer appear
# in vector search results.
def test_updateitem_remove_vector_vectorindex(dynamodb, needs_vector_store):
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
        client = table.meta.client
        p = random_string()
        table.put_item(Item={'p': p, 'v': [1, 0, 0], 'x': 'hello'})
        table.update(VectorIndexUpdates=[{'Create':
            {'IndexName': 'vind',
             'VectorAttribute': {'AttributeName': 'v'},
             'Dimensions': 3,
             'DistanceFunction': 'COSINE',
             'Projection': {'ProjectionType': 'KEYS_ONLY'}}}])
        wait_for_vector_index_active(table, 'vind')
        result = client.search_vectors(TableName=table.name, IndexName='vind',
                                        SearchVector=[1, 0, 0], TopK=1)
        results = result['SearchResults']
        # Verify the item was indexed
        assert len(results) == 1 and results[0]['Item'] == {'p': p}
        # Remove only the vector attribute, leaving the rest of the item intact.
        table.update_item(Key={'p': p}, UpdateExpression='REMOVE v')
        # The item must eventually disappear from the vector index.
        wait_for_search_vectors(client,
            condition=lambda result: not result['SearchResults'],
            message='Timed out waiting for item to disappear from vector index after vector attribute removal',
            TableName=table.name, IndexName='vind', SearchVector=[1, 0, 0], TopK=1)
        # The item itself must still exist in the base table, just without 'v'.
        result = table.get_item(Key={'p': p}, ConsistentRead=True)
        assert result['Item'] == {'p': p, 'x': 'hello'}

# Test vector index with TTL together. A table is created without TTL enabled,
# data is inserted with expiration time set to the past (but expiration not
# yet enabled), and the item should still appear in vector search. Then TTL
# expiration is enabled and the item should disappear from the vector search
# once TTL deletes it and the deletion propagates via CDC.
# This test is skipped if alternator_ttl_period_in_seconds is not set to a
# low value because otherwise it would take too long to run. On DynamoDB,
# we have no control over the TTL latency, and this test can take a very long
# time.
# Two code paths are tested via parametrize:
# - without clustering key (no_ck): partition deletions in CDC.
# - with clustering key (with_ck): row deletions in CDC.
@pytest.mark.parametrize('have_ck', [False, True], ids=['no_ck', 'with_ck'])
def test_vector_with_ttl(dynamodb, needs_vector_store, have_ck):
    if is_aws(dynamodb):
        period = 1800
    else:
        period = scylla_config_read(dynamodb, 'alternator_ttl_period_in_seconds')
        if period is None or float(period) > 1:
            skip_env('need alternator_ttl_period_in_seconds <= 1 to run this test quickly')
    key_schema = [{'AttributeName': 'p', 'KeyType': 'HASH'}]
    attr_defs = [{'AttributeName': 'p', 'AttributeType': 'S'}]
    if have_ck:
        key_schema.append({'AttributeName': 'c', 'KeyType': 'RANGE'})
        attr_defs.append({'AttributeName': 'c', 'AttributeType': 'S'})
    with new_test_table(dynamodb,
            KeySchema=key_schema,
            AttributeDefinitions=attr_defs,
            VectorIndexes=[
                {'IndexName': 'vind',
                 'VectorAttribute': {'AttributeName': 'v'},
                 'Dimensions': 3,
                 'DistanceFunction': 'COSINE',
                 'Projection': {'ProjectionType': 'KEYS_ONLY'}}
            ]) as table:
        client = table.meta.client
        # Wait until the vector store is ready (prefill of the empty table
        # has completed), to ensure the rest of the test doesn't need to
        # the vector store not yet being up (we'll still need to wait for
        # specific data to be indexed, but the index itself will be ready)
        wait_for_vector_index_active(table, 'vind')
        p = random_string()
        item = {'p': p, 'expiration': int(time.time()) - 60, 'v': [1, 0, 0]}
        if have_ck:
            c = random_string()
            item['c'] = c
        # Insert an item with 'expiration' set to the past, before TTL is enabled.
        # The item should still be visible (and indexed) because TTL is not yet
        # configured on this table.
        table.put_item(Item=item)
        # Wait for the item to appear in vector search. Since TTL is not yet
        # enabled, the item must be visible despite its past expiration time.
        result = wait_for_search_vectors(client,
            condition=lambda result: len(result.get('SearchResults', [])) > 0,
            message='Timed out waiting for item to appear in vector search before TTL was enabled',
            TableName=table.name, IndexName='vind', SearchVector=[1, 0, 0], TopK=1)
        assert result['SearchResults'][0]['Item']['p'] == p
        # Now enable TTL on the 'expiration' attribute. The item has its
        # expiration in the past, so TTL should delete it quickly.
        client.update_time_to_live(
            TableName=table.name,
            TimeToLiveSpecification={'AttributeName': 'expiration', 'Enabled': True})
        # Wait for the item to disappear from vector search. TTL deletes the
        # item from the database, and the deletion propagates to the vector
        # store via CDC. SearchVectors, unlike Query, always returns only
        # attributes projected into the index (no ALL_PROJECTED_ATTRIBUTES
        # needed - that's the default), so these results come directly from
        # the vector store, not the base table.
        wait_for_search_vectors(client,
            condition=lambda result: len(result['SearchResults']) == 0,
            message='Timed out waiting for TTL-expired item to disappear from vector search',
            timeout=VECTOR_STORE_TIMEOUT + float(period),
            TableName=table.name, IndexName='vind', SearchVector=[1, 0, 0], TopK=1)

# Test that invalid Projection parameter values are rejected for both
# CreateTable and UpdateTable's vector index creation.
def test_vector_projection_bad(dynamodb):
    bad_projections = [
        # 'not_an_object',   # We can't check this with boto3
        {'ProjectionType': 'GARBAGE'},
        {},  # missing ProjectionType
    ]
    for bad_projection in bad_projections:
        with pytest.raises(ClientError, match='(?i)ValidationException.*Projection'):
            with new_test_table(dynamodb,
                    KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
                    AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
                    VectorIndexes=[{
                        'IndexName': 'vind',
                        'VectorAttribute': {'AttributeName': 'v'},
                        'Dimensions': 3,
                        'DistanceFunction': 'COSINE',
                        'Projection': bad_projection,
                    }]) as table:
                pass
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
        for bad_projection in bad_projections:
            with pytest.raises(ClientError, match='(?i)ValidationException.*Projection'):
                table.update(VectorIndexUpdates=[{'Create': {
                    'IndexName': 'vind',
                    'VectorAttribute': {'AttributeName': 'v'},
                    'Dimensions': 3,
                    'DistanceFunction': 'COSINE',
                    'Projection': bad_projection,
                }}])

# As we saw in test_item.py::test_attribute_allowed_chars in the DynamoDB API
# attribute names can contain any characters whatsoever, including quotes,
# spaces, and even null bytes. Test that such crazy attribute names can be
# used as vector attributes in vector indexes, and that a vector index with
# such an attribute can be created and used successfully.
def test_vector_attribute_allowed_chars(dynamodb, needs_vector_store):
    # To check both scan-based prefill and CDC-based indexing, we create the
    # table without a vector index and then add the vector index. Data that
    # we added before creating the index needs scan, and data added later
    # needs CDC. We want to ensure that both work correctly with such
    # attribute names.
    attribute_name = 'v with spaces and .-+-&*!#@$%^()\\ \' "quotes" and \0 null byte'
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
        client = table.meta.client
        p1 = random_string()
        table.put_item(Item={'p': p1, attribute_name: [1, 0, 0]})
        table.update(VectorIndexUpdates=[{'Create':
            {'IndexName': 'vind',
             'VectorAttribute': {'AttributeName': attribute_name},
             'Dimensions': 3,
             'DistanceFunction': 'COSINE',
             'Projection': {'ProjectionType': 'KEYS_ONLY'}}}])
        wait_for_vector_index_active(table, 'vind')
        # The previous item was indexed by a scan. Now let's add another item
        # which will get indexed by CDC.
        p2 = random_string()
        table.put_item(Item={'p': p2, attribute_name: [0, 0, 1]})
        # Wait until the CDC-indexed update (v=[0, 0, 1]) is reflected in the
        # vector search results.
        get_got = lambda result: [r['Item']['p'] for r in result.get('SearchResults', [])]
        wait_for_search_vectors(client,
            condition=lambda result: get_got(result) == [p2, p1],
            message=lambda result: f'Timed out waiting for items to appear in vector search; '
                f'last got: {get_got(result) if result else None}',
            TableName=table.name, IndexName='vind', SearchVector=[0, 0, 1], TopK=2)

# Test that garbage values (like "dog" or "Inf") for the "N"-typed numbers
# are not allowed as vector attribute values given as a list of numbers.
# They should be rejected with a validation error both before the index is
# created (this test) and after (the next test), because such values are not
# allowed as "N" variables - regardless of vector search.
# This test (the "before") doesn't need vector search and can also run on
# DynamoDB. It reproduces issue #8070 - where Alternator validates number
# values, but forget to validate numbers when they are inside a list.
@pytest.mark.xfail(reason='issue #8070 - Alternator did not validate "N" values inside lists')
def test_putitem_vector_bad_number_string_before(test_table_s):
    p = random_string()
    # boto3 normally validates number strings before sending them to the
    # server, so we need client_no_transform to bypass that validation and
    # let the server reject the bad values itself.
    with client_no_transform(test_table_s.meta.client) as client:
        for bad_num in ['dog', 'Inf', 'NaN', 'Infinity', '-Infinity']:
            with pytest.raises(ClientError, match='ValidationException'):
                client.put_item(
                    TableName=test_table_s.name,
                    Item={
                        'p': {'S': p},
                        'v': {'L': [{'N': '1'}, {'N': bad_num}, {'N': '0'}]},
                    })

def test_putitem_vector_bad_number_string_after(table_vs):
    p = random_string()
    # After the vector index is created, invalid "N" strings in a list
    # must be rejected - they remain invalid DynamoDB numbers.
    with client_no_transform(table_vs.meta.client) as client:
        for bad_num in ['dog', 'Inf', 'NaN', 'Infinity', '-Infinity']:
            with pytest.raises(ClientError, match='ValidationException'):
                client.put_item(
                    TableName=table_vs.name,
                    Item={
                        'p': {'S': p},
                        'v': {'L': [{'N': '1'}, {'N': bad_num}, {'N': '0'}]},
                    })

# Test that a SearchVectors request with a vector with a non-numeric "N"
# element, like "dog" or "Inf", is rejected with a validation error. Note
# that this path does not convert the numbers to Alternator's internal type
# ("decimal") so the validation path is different, so we need to check it.
def test_searchvectors_searchvector_bad_number_string(table_vs, needs_vector_store):
    # boto3 validates number strings before sending them, so we use
    # client_no_transform to bypass that and let the server reject them.
    with client_no_transform(table_vs.meta.client) as client:
        for bad_num in ['dog', 'Inf', 'NaN', 'Infinity', '-Infinity']:
            print(bad_num)
            with pytest.raises(ClientError, match='ValidationException.*number'):
                client.search_vectors(
                    TableName=table_vs.name,
                    IndexName='vind',
                    SearchVector=[{'N': '1'}, {'N': bad_num}, {'N': '0'}],
                    TopK=1,
                )

# Test that when creating a vector index via UpdateTable, a mandatory
# DistanceFunction must be specified. The valid values are EUCLIDEAN,
# COSINE, DOT_PRODUCT; an invalid value, or a missing DistanceFunction,
# should both be rejected. DescribeTable should return the DistanceFunction
# that was set.
# Same as test_createtable_vectorindexes_distancefunction() above, but for
# UpdateTable instead of CreateTable.
def test_updatetable_vectorindex_distancefunction(dynamodb):
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
        # A bad DistanceFunction should be rejected:
        with pytest.raises(ClientError, match='(?i)ValidationException.*DistanceFunction'):
            table.update(VectorIndexUpdates=[{'Create':
                {'IndexName': 'ind',
                 'VectorAttribute': {'AttributeName': 'v'},
                 'Dimensions': 3,
                 'DistanceFunction': 'BAD_FUNCTION',
                 'Projection': {'ProjectionType': 'KEYS_ONLY'}}}])
        # Each of the valid DistanceFunction values should be accepted and
        # returned by DescribeTable. We use a different attribute name for
        # each to avoid conflicts.
        for df in ['EUCLIDEAN', 'COSINE', 'DOT_PRODUCT']:
            table.update(VectorIndexUpdates=[{'Create':
                {'IndexName': f'ind_{df}',
                 'VectorAttribute': {'AttributeName': f'v_{df}'},
                 'Dimensions': 3,
                 'DistanceFunction': df,
                 'Projection': {'ProjectionType': 'KEYS_ONLY'}}}])
            wait_for_status_active(table)
            desc = table.meta.client.describe_table(TableName=table.name)
            indexes = {vi['IndexName']: vi for vi in desc['Table']['VectorIndexes']}
            assert f'ind_{df}' in indexes
            assert indexes[f'ind_{df}']['DistanceFunction'] == df
            # DynamoDB only allows one online index build per table at a
            # time ("Subscriber limit exceeded"), so we must delete this
            # index (and wait for that to finish too) before creating the
            # next one in the next iteration.
            table.update(VectorIndexUpdates=[{'Delete': {'IndexName': f'ind_{df}'}}])
            wait_for_status_active(table)
        # DistanceFunction is mandatory - it cannot be omitted:
        with pytest.raises(ClientError, match='(?i)ValidationException.*DistanceFunction'):
            table.update(VectorIndexUpdates=[{'Create':
                {'IndexName': 'ind_missing',
                 'VectorAttribute': {'AttributeName': 'v_missing'},
                 'Dimensions': 3,
                 'Projection': {'ProjectionType': 'KEYS_ONLY'}}}])

# Test that the different DistanceFunction values (EUCLIDEAN, COSINE,
# DOT_PRODUCT) chosen during CreateTable actually work as expected in
# subsequent SearchVectors requests, returning different result orders for
# the same search vector.
def test_createtable_searchvectors_distancefunction(dynamodb, needs_vector_store):
    # Choose 3 items whose nearest-neighbor under search vector [1, 0, 0]
    # differs depending on the distance function:
    #
    #   p_small = [0.5, 0, 0]   - perfect direction, small magnitude
    #   p_big   = [2, 0.01, 0]  - large magnitude, nearly perfect direction
    #   p_close = [1, 0.3, 0]   - moderate magnitude, clearly off direction
    #
    # Under COSINE  (angle only, higher=closer):  p_small is nearest (cosine=1.0)
    # Under DOT_PRODUCT (inner product, higher=closer): p_big is nearest (dot=2)
    # Under EUCLIDEAN (L2 distance, lower=closer): p_close is nearest (dist=0.3)
    p_small = random_string()
    p_big   = random_string()
    p_close = random_string()
    for df, expected_p in [('COSINE', p_small),
                            ('DOT_PRODUCT', p_big),
                            ('EUCLIDEAN', p_close)]:
        with new_test_table(dynamodb,
                KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
                AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
            client = table.meta.client
            table.put_item(Item={'p': p_small, 'v': [Decimal("0.5"), Decimal("0"),    Decimal("0")]})
            table.put_item(Item={'p': p_big,   'v': [Decimal("2"),   Decimal("0.01"), Decimal("0")]})
            table.put_item(Item={'p': p_close, 'v': [Decimal("1"),   Decimal("0.3"),  Decimal("0")]})
            table.update(VectorIndexUpdates=[{'Create': {
                'IndexName': 'vind',
                'VectorAttribute': {'AttributeName': 'v'},
                'Dimensions': 3,
                'DistanceFunction': df,
                'Projection': {'ProjectionType': 'KEYS_ONLY'},
            }}])
            wait_for_vector_index_active(table, 'vind')
            result = client.search_vectors(
                TableName=table.name,
                IndexName='vind',
                SearchVector=[Decimal("1"), Decimal("0"), Decimal("0")],
                TopK=1,
            )
            results = result['SearchResults']
            assert len(results) == 1, \
                f'Expected 1 result for DistanceFunction={df}'
            assert results[0]['Item']['p'] == expected_p, \
                f'For DistanceFunction={df}, expected nearest item {expected_p}, ' \
                f'got {results[0]["Item"]["p"]}'

# NYH CONTINUE HERE

# Test the "Score" field in the search results returned by SearchVectors.
# The score is returned per item, and its meaning depends on the
# DistanceFunction of the vector index:
# COSINE - Scores range from 0 (identical) to 2 (opposite). 
#          Scores are returned in ascending order - the nearest neighbor has
#          the lowest distance score, and returned first.
# EUCLIDEAN - Scores represent the Euclidean distance between vectors.
#             Scores are returned in ascending order.
# DOT_PRODUCT - Contrary to the other distance functions, here scores are
#          returned in *descending* order. The score measures similarity
#          (higher is better match), not distance (lower is better match).
#
# The following three tests verify that for each of the three DistanceFunction
# choices, the scores have the right order (ascending or decending), and that
# the scores match expected values for known vectors.

def test_searchvectors_score_cosine(dynamodb, needs_vector_store):
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
        client = table.meta.client
        # Insert 4 items at known COSINE distance to the search vector [1, 0, 0].
        # COSINE distance is 1 minus cosine similarity, ranging from 0
        # (identical direction) to 2 (opposite direction):
        #   p1 at [1, 0, 0]   cos_sim=1.0    -> distance 0.0   (identical)
        #   p2 at [1, 0.1, 0] cos_sim~=0.995 -> distance ~ 0.005
        #   p3 at [0, 1, 0]   cos_sim=0.0    -> distance 1.0   (orthogonal)
        #   p4 at [-1, 0, 0]  cos_sim=-1.0   -> distance 2.0   (opposite)
        p1, p2, p3, p4 = random_string(), random_string(), random_string(), random_string()
        table.put_item(Item={'p': p1, 'v': [Decimal("1.0"),  Decimal("0.0"), Decimal("0.0")]})
        table.put_item(Item={'p': p2, 'v': [Decimal("1.0"),  Decimal("0.1"), Decimal("0.0")]})
        table.put_item(Item={'p': p3, 'v': [Decimal("0.0"),  Decimal("1.0"), Decimal("0.0")]})
        table.put_item(Item={'p': p4, 'v': [Decimal("-1.0"), Decimal("0.0"), Decimal("0.0")]})
        table.update(VectorIndexUpdates=[{'Create':
            {'IndexName': 'vind',
             'VectorAttribute': {'AttributeName': 'v'},
             'Dimensions': 3,
             'DistanceFunction': 'COSINE',
             'Projection': {'ProjectionType': 'KEYS_ONLY'},
            }}])
        wait_for_vector_index_active(table, 'vind')
        result = client.search_vectors(
            TableName=table.name,
            IndexName='vind',
            SearchVector=[Decimal("1.0"), Decimal("0.0"), Decimal("0.0")],
            TopK=4)
        results = result['SearchResults']
        assert [r['Item']['p'] for r in results] == [p1, p2, p3, p4]
        scores = [r['Score'] for r in results]
        assert len(scores) == 4
        # Scores must be in ascending order (nearest neighbor has the
        # lowest "distance").
        assert scores == sorted(scores)
        # Map item key to its position in the result list.
        pos = {r['Item']['p']: i for i, r in enumerate(results)}
        # Known distance values for three easy cases:
        assert scores[pos[p1]] == pytest.approx(0.0, abs=1e-5)
        assert scores[pos[p3]] == pytest.approx(1.0, abs=1e-5)
        assert scores[pos[p4]] == pytest.approx(2.0, abs=1e-5)

def test_searchvectors_score_euclidean(dynamodb, needs_vector_store):
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
        client = table.meta.client
        # Insert 4 items at known EUCLIDEAN (L2) distance to the search
        # vector [1, 0, 0]. Under EUCLIDEAN, the Score is the raw L2
        # distance itself (lower = more similar), not a transformed
        # similarity value:
        #   p1 at [1, 0, 0]   -> distance 0.0   (identical)
        #   p2 at [1, 0.1, 0] -> distance 0.1
        #   p3 at [2, 0, 0]   -> distance 1.0
        #   p4 at [-1, 0, 0]  -> distance 2.0
        p1, p2, p3, p4 = random_string(), random_string(), random_string(), random_string()
        table.put_item(Item={'p': p1, 'v': [Decimal("1.0"),  Decimal("0.0"), Decimal("0.0")]})
        table.put_item(Item={'p': p2, 'v': [Decimal("1.0"),  Decimal("0.1"), Decimal("0.0")]})
        table.put_item(Item={'p': p3, 'v': [Decimal("2.0"),  Decimal("0.0"), Decimal("0.0")]})
        table.put_item(Item={'p': p4, 'v': [Decimal("-1.0"), Decimal("0.0"), Decimal("0.0")]})
        table.update(VectorIndexUpdates=[{'Create':
            {'IndexName': 'vind',
             'VectorAttribute': {'AttributeName': 'v'},
             'Dimensions': 3,
             'DistanceFunction': 'EUCLIDEAN',
             'Projection': {'ProjectionType': 'KEYS_ONLY'}}}])
        wait_for_vector_index_active(table, 'vind')
        result = client.search_vectors(
            TableName=table.name,
            IndexName='vind',
            SearchVector=[Decimal("1.0"), Decimal("0.0"), Decimal("0.0")],
            TopK=4)
        results = result['SearchResults']
        assert [r['Item']['p'] for r in results] == [p1, p2, p3, p4]
        scores = [r['Score'] for r in results]
        assert len(scores) == 4
        # Scores must be in ascending order (nearest neighbor has the
        # lowest distance).
        assert scores == sorted(scores)
        # Map item key to its position in the result list.
        pos = {r['Item']['p']: i for i, r in enumerate(results)}
        # Known distance values for three easy cases:
        assert scores[pos[p1]] == pytest.approx(0.0, abs=1e-5)
        assert scores[pos[p3]] == pytest.approx(1.0, abs=1e-5)
        assert scores[pos[p4]] == pytest.approx(2.0, abs=1e-5)

def test_searchvectors_score_dot_product(dynamodb, needs_vector_store):
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
        client = table.meta.client
        # Insert 4 items at known DOT_PRODUCT to the search vector [1, 0, 0].
        # Under DOT_PRODUCT, the Score is the raw dot product itself (higher
        # = more similar), and results are returned in descending order -
        # the opposite of COSINE and EUCLIDEAN, per DynamoDB's documentation:
        #   p1 at [2, 0, 0]   -> dot product 2   (most similar)
        #   p2 at [1, 0.1, 0] -> dot product 1
        #   p3 at [0, 1, 0]   -> dot product 0
        #   p4 at [-1, 0, 0]  -> dot product -1  (least similar)
        p1, p2, p3, p4 = random_string(), random_string(), random_string(), random_string()
        table.put_item(Item={'p': p1, 'v': [Decimal("2.0"),  Decimal("0.0"), Decimal("0.0")]})
        table.put_item(Item={'p': p2, 'v': [Decimal("1.0"),  Decimal("0.1"), Decimal("0.0")]})
        table.put_item(Item={'p': p3, 'v': [Decimal("0.0"),  Decimal("1.0"), Decimal("0.0")]})
        table.put_item(Item={'p': p4, 'v': [Decimal("-1.0"), Decimal("0.0"), Decimal("0.0")]})
        table.update(VectorIndexUpdates=[{'Create':
            {'IndexName': 'vind',
             'VectorAttribute': {'AttributeName': 'v'},
             'Dimensions': 3,
             'DistanceFunction': 'DOT_PRODUCT',
             'Projection': {'ProjectionType': 'KEYS_ONLY'}}}])
        wait_for_vector_index_active(table, 'vind')
        result = client.search_vectors(
            TableName=table.name,
            IndexName='vind',
            SearchVector=[Decimal("1.0"), Decimal("0.0"), Decimal("0.0")],
            TopK=4)
        results = result['SearchResults']
        assert [r['Item']['p'] for r in results] == [p1, p2, p3, p4]
        scores = [r['Score'] for r in results]
        assert len(scores) == 4
        # Scores must be in descending order (nearest neighbor has the
        # highest dot product) - unlike COSINE and EUCLIDEAN.
        assert scores == sorted(scores, reverse=True)
        # Map item key to its position in the result list.
        pos = {r['Item']['p']: i for i, r in enumerate(results)}
        # Known dot-product values for three easy cases:
        assert scores[pos[p1]] == pytest.approx(2.0, abs=1e-5)
        assert scores[pos[p3]] == pytest.approx(0.0, abs=1e-5)
        assert scores[pos[p4]] == pytest.approx(-1.0, abs=1e-5)

# The DOT_PRODUCT distance function is not bounded if vectors are not
# normalized (have an arbitrary magnitude, not 1.0). It is possible that the
# Score returned by SearchVectors could be beyond the range of 32-bit float
# even for valid float32 vectors. In this case, the implementation should
# not cause an error or drop this item - it should return the item correctly
# with a very high (even if not mathematically accurate) Score.
def test_searchvectors_score_dot_product_overflow(dynamodb, needs_vector_store):
    # Two float32 vectors with very large - but valid - magnitude BIG,
    # have a dot product of BIG^2, which overflows float32 to +infinity.
    BIG = 1e38 # Valid 32-bit number, but close to the maximum
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
        client = table.meta.client
        p = random_string()
        table.put_item(Item={'p': p, 'v': [Decimal(str(BIG)), Decimal("0.0"), Decimal("0.0")]})
        table.update(VectorIndexUpdates=[{'Create':
            {'IndexName': 'vind',
             'VectorAttribute': {'AttributeName': 'v'},
             'Dimensions': 3,
             'DistanceFunction': 'DOT_PRODUCT',
             'Projection': {'ProjectionType': 'KEYS_ONLY'}}}])
        wait_for_vector_index_active(table, 'vind')
        result = client.search_vectors(
            TableName=table.name,
            IndexName='vind',
            SearchVector=[Decimal(str(BIG)), Decimal("0.0"), Decimal("0.0")],
            TopK=1)
        results = result['SearchResults']
        assert len(results) == 1 and results[0]['Item']['p'] == p
        # The dot product BIG * BIG overflows float32 to infinity.
        # Since JSON can't represent infinity and must return some number,
        # we don't really care what it returns, as long as it's very large.
        # Let's just verify it's larger than BIG itself.
        assert results[0]['Score'] >= BIG

# Test that the DOT_PRODUCT distance function correctly orders three items:
# one very highly similar (large positive dot product overflowing 32 bits),
# one mildly similar (dot product 0), and one very highly dissimilar
# (large negative overflowing 32 bits).
# SearchVectors should return them in descending score order.
# We also check the Scores themselves: the highly similar item should have
# a score much larger than BIG, the mildly similar item exactly 0, and the
# highly dissimilar item should have a large negative score.
#
# This test used to reproduce a bug in the vector store: When the Score
# overflowed the 32-bit calculation, it returned the same value "null" for
# both +infinity and -infinity.
def test_searchvectors_score_dot_product_overflow2(dynamodb, needs_vector_store):
    BIG = 1e38  # Near FLT_MAX; dot product BIG * BIG overflows float32
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
        client = table.meta.client
        p_high = random_string()
        p_mid  = random_string()
        p_low  = random_string()
        table.put_item(Item={'p': p_high, 'v': [Decimal(str(BIG)),  Decimal("0.0"), Decimal("0.0")]})
        table.put_item(Item={'p': p_mid,  'v': [Decimal("0.0"),     Decimal("0.0"), Decimal("0.0")]})
        table.put_item(Item={'p': p_low,  'v': [Decimal(str(-BIG)), Decimal("0.0"), Decimal("0.0")]})
        table.update(VectorIndexUpdates=[{'Create':
            {'IndexName': 'vind',
             'VectorAttribute': {'AttributeName': 'v'},
             'Dimensions': 3,
             'DistanceFunction': 'DOT_PRODUCT',
             'Projection': {'ProjectionType': 'KEYS_ONLY'}}}])
        wait_for_vector_index_active(table, 'vind')
        result = client.search_vectors(
            TableName=table.name,
            IndexName='vind',
            SearchVector=[Decimal(str(BIG)), Decimal("0.0"), Decimal("0.0")],
            TopK=3)
        results = result['SearchResults']
        assert len(results) == 3
        # Results must be in descending score order (highest dot product first).
        assert [r['Item']['p'] for r in results] == [p_high, p_mid, p_low]
        scores = [r['Score'] for r in results]
        # The highly similar item's score should be large and positive
        assert scores[0] > BIG
        # The mildly similar item's dot product with [BIG, 0, 0] is exactly 0.
        assert scores[1] == pytest.approx(0.0, abs=1e-5)
        # The highly dissimilar item's score should be large and negative.
        assert scores[2] < -BIG

# In virtually all the tests above, we used vectors of dimension 3 as an
# example. But dimensions up to max_vector_dimensions() are allowed, so
# let's have at least one test that actually uses the maximum dimension to
# check that it works end-to-end - indexing (via either prefill or CDC) and
# searching.
@pytest.mark.parametrize('via_cdc', [False, True], ids=['prefill', 'cdc'])
def test_searchvectors_max_dimension(dynamodb, needs_vector_store, via_cdc):
    dim = max_vector_dimensions(dynamodb)
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
            **({} if not via_cdc else
               {'VectorIndexes': [{'IndexName': 'vind',
                                   'VectorAttribute': {'AttributeName': 'v'},
                                   'Dimensions': dim,
                                   'DistanceFunction': 'COSINE',
                                   'Projection': {'ProjectionType': 'KEYS_ONLY'}}]}
            )) as table:
        client = table.meta.client
        if via_cdc:
            # The index was already created in new_test_table above. Wait for
            # it to become ACTIVE so that subsequent writes are picked up via
            # CDC rather than prefill.
            wait_for_vector_index_active(table, 'vind')
        # Build a search vector: all zeros except the first element which is
        # 1. The item we insert has exactly this vector, so it is the
        # nearest neighbor of the search.
        search_vec = [Decimal("1.0")] + [Decimal("0.0")] * (dim - 1)
        p = random_string()
        table.put_item(Item={'p': p, 'v': search_vec})
        if not via_cdc:
            # For the prefill case the index is created after the data, so
            # we create it now and wait for the prefill scan to complete.
            table.update(VectorIndexUpdates=[{'Create':
                {'IndexName': 'vind',
                 'VectorAttribute': {'AttributeName': 'v'},
                 'Dimensions': dim,
                 'DistanceFunction': 'COSINE',
                 'Projection': {'ProjectionType': 'KEYS_ONLY'}}}])
            wait_for_vector_index_active(table, 'vind')
        # Retry SearchVectors until the item appears in the results.
        wait_for_search_vectors(client,
            condition=lambda result: result.get('SearchResults') and result['SearchResults'][0]['Item']['p'] == p,
            message=f'Timed out waiting for dim={dim} item to appear via '
                    f'{"CDC" if via_cdc else "prefill"}',
            TableName=table.name, IndexName='vind', SearchVector=search_vec, TopK=1)

# Until now, tests used 'Projection': {'ProjectionType': 'KEYS_ONLY'} almost
# exclusively. Let's begin now to fully test the different projection options.

# Check that the allowed ProjectionType are KEYS_ONLY, ALL and INCLUDE, and
# these names are case-sensitive. Check that NonKeyAttributes can only be
# added together with ProjectionType=INCLUDE (which cannot be missing).
# This test checks CreateTable (the same rules should also apply in
# UpdateTable).
def test_vectorindexes_projectiontype_values(dynamodb):
    vector_index = {
        'IndexName': 'vind',
        'VectorAttribute': {'AttributeName': 'v'},
        'Dimensions': 3,
        'DistanceFunction': 'COSINE',
        # 'Projection' is set below for each case
    }
    # ALL and KEYS_ONLY are accepted:
    for good in ['KEYS_ONLY', 'ALL']:
        vector_index['Projection'] = {'ProjectionType': good}
        with new_test_table(dynamodb,
                KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
                AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
                VectorIndexes=[vector_index]) as table:
            pass
    # INCLUDE with NonKeyAttributes is accepted:
    vector_index['Projection'] = {'ProjectionType': 'INCLUDE', 'NonKeyAttributes': ['x']}
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
            VectorIndexes=[vector_index]) as table:
        pass
    # An unrecognized name, or a recognized name in the wrong case, is
    # rejected with a ValidationException:
    for bad in ['keys_only', 'all', 'include', 'Keys_Only', 'garbage']:
        vector_index['Projection'] = {'ProjectionType': bad}
        with pytest.raises(ClientError, match='(?i)ValidationException.*ProjectionType'):
            with new_test_table(dynamodb,
                    KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
                    AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
                    VectorIndexes=[vector_index]) as table:
                pass
    # NonKeyAttributes is only allowed together with ProjectionType=INCLUDE -
    # ProjectionType can't be missing or be one of the other types:
    for bad_type in [None, 'KEYS_ONLY', 'ALL']:
        if bad_type is None:
            del vector_index['Projection']
        else:
            vector_index['Projection'] = {'ProjectionType': bad_type, 'NonKeyAttributes': ['x']}
        with pytest.raises(ClientError, match='(?i)ValidationException.*(NonKeyAttributes|Projection)'):
            with new_test_table(dynamodb,
                    KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
                    AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
                    VectorIndexes=[vector_index]) as table:
                pass
    # ProjectionType=INCLUDE requires NonKeyAttributes - it cannot be missing:
    vector_index['Projection'] = {'ProjectionType': 'INCLUDE'}
    with pytest.raises(ClientError, match='(?i)ValidationException.*NonKeyAttributes'):
        with new_test_table(dynamodb,
                KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
                AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
                VectorIndexes=[vector_index]) as table:
            pass

# Test what NonKeyAttributes (with ProjectionType=INCLUDE) may contain.
# It cannot be an empty list. The documentation says that only 20 attributes
# can be projected, but the vector attribute is also counted as "projected"
# (even though test_searchvectors_projected_vector_reduced_precision shows it
# is not *really* projected - it is reconstructed), leaving only 19 attributes
# allowed in NonKeyAttributes. Note that although the key columns are also
# projected (for real), they do not count against the projection limit.
# Each attribute name should be a legal attribute name.
# Despite the name "NonKeyAttributes", key columns *may* also be listed (but
# it has no effect, because key attributes are always projected anyway).
def test_vectorindexes_projection_nonkeyattributes(dynamodb):
    vector_index = {
        'IndexName': 'vind',
        'VectorAttribute': {'AttributeName': 'v'},
        'Dimensions': 3,
        'DistanceFunction': 'COSINE',
        # 'Projection' is set below for each case
    }
    # NonKeyAttributes cannot be an empty list:
    vector_index['Projection'] = {'ProjectionType': 'INCLUDE', 'NonKeyAttributes': []}
    with pytest.raises(ClientError, match='(?i)ValidationException.*NonKeyAttributes'):
        with new_test_table(dynamodb,
                KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
                AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
                VectorIndexes=[vector_index]) as table:
            pass
    # As the comment above explains, only 19 attributes can be listed in
    # NonKeyAttributes. To check that the key columns aren't counted against
    # this limit, we run the following checks (19 succeeds, 20 fails) with
    # two different key schemas - one with just a HASH key and the other
    # with HASH+RANGE - and see that in both cases the limit is the same 19
    # attributes:
    for key_schema, attribute_definitions in [
            ([{'AttributeName': 'p', 'KeyType': 'HASH'}],
             [{'AttributeName': 'p', 'AttributeType': 'S'}]),
            ([{'AttributeName': 'p', 'KeyType': 'HASH'},
              {'AttributeName': 'c', 'KeyType': 'RANGE'}],
             [{'AttributeName': 'p', 'AttributeType': 'S'},
              {'AttributeName': 'c', 'AttributeType': 'S'}]),
            ]:
        # 19 attributes in NonKeyAttributes is accepted (regardless of the
        # number of key columns):
        vector_index['Projection'] = {'ProjectionType': 'INCLUDE',
            'NonKeyAttributes': [f'attr{i}' for i in range(19)]}
        with new_test_table(dynamodb,
                KeySchema=key_schema, AttributeDefinitions=attribute_definitions,
                VectorIndexes=[vector_index]) as table:
            pass
        # And 20 non-key attribute names is above the limit, still
        # regardless of the number of key columns:
        vector_index['Projection'] = {'ProjectionType': 'INCLUDE',
            'NonKeyAttributes': [f'attr{i}' for i in range(20)]}
        with pytest.raises(ClientError, match='(?i)ValidationException.*projected attributes'):
            with new_test_table(dynamodb,
                    KeySchema=key_schema, AttributeDefinitions=attribute_definitions,
                    VectorIndexes=[vector_index]) as table:
                pass
    # Each attribute name must be a legal attribute name. For example, an
    # empty string is not a legal attribute name, and is rejected:
    vector_index['Projection'] = {'ProjectionType': 'INCLUDE', 'NonKeyAttributes': ['']}
    with pytest.raises(ClientError, match='(?i)ValidationException.*NonKeyAttributes'):
        with new_test_table(dynamodb,
                KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
                AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
                VectorIndexes=[vector_index]) as table:
            pass
    # Despite the name "NonKeyAttributes", the base table's own key columns
    # (hash and range) may be listed - it has no effect, because key
    # attributes are always projected anyway. We noticed the same thing for
    # GSIs in test_gsi_projection_include_keyattributes.
    vector_index['Projection'] = {'ProjectionType': 'INCLUDE', 'NonKeyAttributes': ['p']}
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
            VectorIndexes=[vector_index]) as table:
        pass
    vector_index['Projection'] = {'ProjectionType': 'INCLUDE', 'NonKeyAttributes': ['c']}
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'},
                       {'AttributeName': 'c', 'KeyType': 'RANGE'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'},
                                  {'AttributeName': 'c', 'AttributeType': 'S'}],
            VectorIndexes=[vector_index]) as table:
        pass

# Now check that the three different ProjectionType values (KEYS_ONLY, ALL,
# INCLUDE) actually work end-to-end - that the SearchVectors results contain
# the expected attributes.
#
# The item has two non-key attributes, 'x' and 'y'. Depending on the index's
# Projection, SearchVectors (which, unlike Query, has no "Select" parameter
# and always returns the attributes projected into the index) is expected to
# return:
#   KEYS_ONLY: just the key attribute 'p'.
#   ALL: the entire item.
#   INCLUDE with NonKeyAttributes=['x']: the key attribute 'p' plus 'x' (but
#     not 'y', which wasn't listed).
#
# There is one subtlety for "ALL" which we'll also check: Although the entire
# item is projected into the index, SearchVectors does not return by default
# one of these columns - the vector attribute itself. The DynamoDB
# documentation explains why: "Vector data is large, and you typically don't
# need it in the response. The results include the other projected attributes
# and the Score value. To include the vector attribute, request it with a
# ProjectionExpression. ". We will check below that indeed the vector
# attribute is not returned by default, but can be requested with a
# ProjectionExpression.
@pytest.mark.parametrize('projection_type', ['KEYS_ONLY', 'ALL', 'INCLUDE'])
def test_vectorindexes_projectiontype_end_to_end(dynamodb, needs_vector_store, projection_type):
    projection = {'ProjectionType': projection_type}
    if projection_type == 'INCLUDE':
        projection['NonKeyAttributes'] = ['x']
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
            VectorIndexes=[{
                'IndexName': 'vind',
                'VectorAttribute': {'AttributeName': 'v'},
                'Dimensions': 3,
                'DistanceFunction': 'COSINE',
                'Projection': projection,
            }]) as table:
        wait_for_vector_index_active(table, 'vind')
        client = table.meta.client
        p = random_string()
        table.put_item(Item={'p': p, 'v': [1, 0, 0], 'x': 'hello', 'y': 'world'})
        if projection_type == 'KEYS_ONLY':
            expected_item = {'p': p}
        elif projection_type == 'ALL':
            # As explained above, the entire item except the vector attribute
            # 'v' is returned. We'll check below that 'v' can still be
            # requested ProjectionExpression (i.e., it is projected into the
            # index but not returned by default).
            expected_item = {'p': p, 'x': 'hello', 'y': 'world'}
        else: # projection_type=INCLUDE with NonKeyAttributes=['x']
            expected_item = {'p': p, 'x': 'hello'}
        result = wait_for_search_vectors(client,
            condition=lambda result: result.get('SearchResults'),
            message=f'Timed out waiting for projection_type={projection_type} item to appear',
            TableName=table.name, IndexName='vind', SearchVector=[1, 0, 0], TopK=1)
        assert result['SearchResults'][0]['Item'] == expected_item
        if projection_type == 'ALL':
            # Confirm that 'v' is indeed projected into the index (like every
            # other attribute, with ProjectionType=ALL) - it's just not
            # returned by default. Requesting it explicitly with a
            # ProjectionExpression does return it. No retry loop is needed
            # here: the item was already confirmed to be indexed above.
            result = client.search_vectors(
                TableName=table.name,
                IndexName='vind',
                SearchVector=[1, 0, 0],
                TopK=1,
                ProjectionExpression='p, v, x, y',
            )
            assert result['SearchResults'][0]['Item'] == {'p': p, 'v': [1, 0, 0], 'x': 'hello', 'y': 'world'}

# Test how ProjectionExpression in SearchVector interacts with the three
# different ProjectionType values (KEYS_ONLY, ALL, INCLUDE) defined in the
# vector index. KEYS_ONLY allows projection only the key attributes (hash
# or range), ALL allows projection of all attributes, and INCLUDE allows
# projection of the specified attributes.
@pytest.mark.parametrize('projection_type', ['KEYS_ONLY', 'ALL', 'INCLUDE'])
def test_searchvectors_projectionexpression_vs_projectiontype(dynamodb, needs_vector_store, projection_type):
    projection = {'ProjectionType': projection_type}
    if projection_type == 'INCLUDE':
        projection['NonKeyAttributes'] = ['x']
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
            VectorIndexes=[{
                'IndexName': 'vind',
                'VectorAttribute': {'AttributeName': 'v'},
                'Dimensions': 3,
                'DistanceFunction': 'COSINE',
                'Projection': projection,
            }]) as table:
        wait_for_vector_index_active(table, 'vind')
        client = table.meta.client
        p = random_string()
        table.put_item(Item={'p': p, 'v': [1, 0, 0], 'x': 'hello', 'y': 'world'})
        # Wait until the item is indexed:
        wait_for_search_vectors(client,
            condition=lambda result: result.get('SearchResults'),
            message='Timed out waiting for item to appear',
            TableName=table.name, IndexName='vind', SearchVector=[1, 0, 0], TopK=1)
        # 'p' (the key) and 'v' (the vector attribute) are always projected,
        # regardless of projection_type, so ProjectionExpression can always
        # retrieve them explicitly:
        # NOTE: It's surprising that the vector attribute 'v' is always
        # projected into the index, even for KEYS_ONLY. If the original
        # vector contains high-precision numbers, the vector index would want
        # to truncate them to float32, rather than project the original high-
        # precision numbers... We'll test this issue in a separate test
        # below.
        result = client.search_vectors(
            TableName=table.name, IndexName='vind',
            SearchVector=[1, 0, 0], TopK=1,
            ProjectionExpression='p, v')
        assert result['SearchResults'][0]['Item'] == {'p': p, 'v': [1, 0, 0]}
        # 'x' is projected under ALL (all attributes) and under INCLUDE
        # (since it's listed in NonKeyAttributes), but not under KEYS_ONLY.
        # Requesting an attribute that isn't projected is *not* an error -
        # despite the documentation's "Only projected attributes can be
        # returned" wording, it's simply omitted from the returned Item,
        # the same way ProjectionExpression silently omits an attribute
        # that an item doesn't have (test_projection_expression_path).
        result = client.search_vectors(
            TableName=table.name, IndexName='vind',
            SearchVector=[1, 0, 0], TopK=1,
            ProjectionExpression='x')
        if projection_type == 'KEYS_ONLY':
            assert result['SearchResults'][0]['Item'] == {}
        else:
            assert result['SearchResults'][0]['Item'] == {'x': 'hello'}
        # 'y' is projected only under ALL - not under KEYS_ONLY, and not
        # under INCLUDE either (whose NonKeyAttributes lists only 'x'). As
        # above, requesting it anyway is not an error - it's just omitted:
        result = client.search_vectors(
            TableName=table.name, IndexName='vind',
            SearchVector=[1, 0, 0], TopK=1,
            ProjectionExpression='y')
        if projection_type == 'ALL':
            assert result['SearchResults'][0]['Item'] == {'y': 'world'}
        else:
            assert result['SearchResults'][0]['Item'] == {}

# In the test above (test_searchvectors_projectionexpression_vs_projectiontype)
# we saw that the vector attribute is always projected into the index, even
# for KEYS_ONLY. In this test we check if the original vector is *really*
# projected into the index - and the surprising result is that it isn't...
# Instead what *really* happs is that the vector index stores a version of the
# original vector truncated to float32 precision, and when the vector
# attribute is requested explicitly with ProjectionExpression (as we tested
# earlier, it is NOT returned by ProjectionType=ALL!), the vector index
# reconstructs it from the 32-bit float version and returns that - not the
# original high-precision vector.
# This test proves this surprising (and undocumented) behavior.
def test_searchvectors_projected_vector_reduced_precision(dynamodb, needs_vector_store):
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
            VectorIndexes=[{
                'IndexName': 'vind',
                'VectorAttribute': {'AttributeName': 'v'},
                'Dimensions': 3,
                'DistanceFunction': 'COSINE',
                'Projection': {'ProjectionType': 'KEYS_ONLY'},
            }]) as table:
        wait_for_vector_index_active(table, 'vind')
        client = table.meta.client
        p = random_string()
        # A value with many more significant digits than a 32-bit float can
        # hold.
        v = [Decimal("1"), Decimal("3.1415926535897932384626433832795028841"), Decimal("3")]
        table.put_item(Item={'p': p, 'v': v})
        # Verify that the high-precision decimal was really stored with full
        # precision in the base table (not truncated to float32)
        result = table.get_item(Key={'p': p})
        assert result['Item']['v'] == v
        # Wait until the item is indexed, and ask ProjectionExpression to
        # return the vector. We'll then see it's a reconstructed, truncated,
        # version of v, not identical to the original v.
        result = wait_for_search_vectors(client,
            condition=lambda result: result.get('SearchResults'),
            message='Timed out waiting for item to appear',
            TableName=table.name, IndexName='vind', SearchVector=[1, 0, 0], TopK=1,
            ProjectionExpression='p, v')
        got_v = result['SearchResults'][0]['Item']['v']
        # Check that got_v is *not* identical to v, but they are identical
        # when truncated to float32 precision.
        assert got_v != v
        to_float32 = lambda vec: [struct.unpack('f', struct.pack('f', float(x)))[0] for x in vec]
        assert to_float32(got_v) == to_float32(v)

# Check the allowed values for SearchSchema in CreateTable's VectorIndexes.
# If present, it must be a non-empty array (an empty array is rejected and not
# equivalent to omitting SearchSchema entirely). Each element must have an
# AttributeName that is a legal attribute name, and a SearchSchemaElementType
# which must be 'HASH' or 'INLINE_FILTER' (case-sensitive). There can be at
# most one 'HASH' element (there can also be none), and any number of
# 'INLINE_FILTER' elements.
# Just like a GSI or LSI's key attributes, every attribute referenced by a
# SearchSchema element must be declared in AttributeDefinitions - and,
# conversely, AttributeDefinitions may not include an attribute that isn't
# used anywhere (not as a table, GSI or LSI key, and not in any SearchSchema).
# This is why below, each case below is careful to declare in
# AttributeDefinitions exactly the attributes ('p', and 'x', 'y' and/or 'z')
# that this specific case's SearchSchema (if any) actually uses - no more,
# no less.
def test_createtable_vectorindexes_searchschema_values(dynamodb):
    vector_index = {
        'IndexName': 'vind',
        'VectorAttribute': {'AttributeName': 'v'},
        'Dimensions': 3,
        'DistanceFunction': 'COSINE',
        'Projection': {'ProjectionType': 'KEYS_ONLY'},
        # 'SearchSchema' is set (or unset) below for each case
    }
    key_schema = [{'AttributeName': 'p', 'KeyType': 'HASH'}]
    # Pieces for AttributeDefinitions, to be used in various combinations below:
    p_attr = {'AttributeName': 'p', 'AttributeType': 'S'}
    x_attr = {'AttributeName': 'x', 'AttributeType': 'S'}
    y_attr = {'AttributeName': 'y', 'AttributeType': 'S'}
    z_attr = {'AttributeName': 'z', 'AttributeType': 'S'}
    # SearchSchema is optional - leaving it out is fine. Since nothing
    # in this case uses 'x' or 'y', AttributeDefinitions only declares 'p':
    with new_test_table(dynamodb,
            KeySchema=key_schema, AttributeDefinitions=[p_attr],
            VectorIndexes=[vector_index]) as table:
        pass
    # An empty SearchSchema is NOT equivalent to omitting it, and not allowed.
    vector_index['SearchSchema'] = []
    with pytest.raises(ClientError, match='(?i)ValidationException.*searchSchema'):
        with new_test_table(dynamodb,
                KeySchema=key_schema, AttributeDefinitions=[p_attr],
                VectorIndexes=[vector_index]) as table:
            pass
    # A single HASH element is fine:
    vector_index['SearchSchema'] = [{'AttributeName': 'x', 'SearchSchemaElementType': 'HASH'}]
    with new_test_table(dynamodb,
            KeySchema=key_schema, AttributeDefinitions=[p_attr, x_attr],
            VectorIndexes=[vector_index]) as table:
        pass
    # Any number of INLINE_FILTER elements, and no HASH at all, is fine:
    vector_index['SearchSchema'] = [
        {'AttributeName': 'x', 'SearchSchemaElementType': 'INLINE_FILTER'},
        {'AttributeName': 'y', 'SearchSchemaElementType': 'INLINE_FILTER'},
    ]
    with new_test_table(dynamodb,
            KeySchema=key_schema, AttributeDefinitions=[p_attr, x_attr, y_attr],
            VectorIndexes=[vector_index]) as table:
        pass
    # A mix of one HASH element and several INLINE_FILTER elements is fine:
    vector_index['SearchSchema'] = [
        {'AttributeName': 'x', 'SearchSchemaElementType': 'HASH'},
        {'AttributeName': 'y', 'SearchSchemaElementType': 'INLINE_FILTER'},
        {'AttributeName': 'z', 'SearchSchemaElementType': 'INLINE_FILTER'},
    ]
    with new_test_table(dynamodb,
            KeySchema=key_schema, AttributeDefinitions=[p_attr, x_attr, y_attr, z_attr],
            VectorIndexes=[vector_index]) as table:
        pass
    # The order of the elements in SearchSchema doesn't matter - unlike
    # KeySchema, where HASH must come before RANGE, there is no requirement
    # here that the HASH element (if any) appear first. The same combination
    # as above, with the HASH element listed last, is also fine:
    vector_index['SearchSchema'] = [
        {'AttributeName': 'y', 'SearchSchemaElementType': 'INLINE_FILTER'},
        {'AttributeName': 'z', 'SearchSchemaElementType': 'INLINE_FILTER'},
        {'AttributeName': 'x', 'SearchSchemaElementType': 'HASH'},
    ]
    with new_test_table(dynamodb,
            KeySchema=key_schema, AttributeDefinitions=[p_attr, x_attr, y_attr, z_attr],
            VectorIndexes=[vector_index]) as table:
        pass
    # Defining more than one HASH element in SearchSchema is not allowed:
    vector_index['SearchSchema'] = [
        {'AttributeName': 'x', 'SearchSchemaElementType': 'HASH'},
        {'AttributeName': 'y', 'SearchSchemaElementType': 'HASH'},
    ]
    with pytest.raises(ClientError, match='(?i)ValidationException.*SearchSchema'):
        with new_test_table(dynamodb,
                KeySchema=key_schema, AttributeDefinitions=[p_attr, x_attr, y_attr],
                VectorIndexes=[vector_index]) as table:
            pass
    # SearchSchemaElementType must be exactly 'HASH' or 'INLINE_FILTER' -
    # this check is case-sensitive, and no other value is allowed:
    for bad_type in ['hash', 'Hash', 'inline_filter', 'Inline_Filter', 'RANGE', 'garbage']:
        vector_index['SearchSchema'] = [{'AttributeName': 'x', 'SearchSchemaElementType': bad_type}]
        with pytest.raises(ClientError, match='(?i)ValidationException.*SearchSchemaElementType'):
            with new_test_table(dynamodb,
                    KeySchema=key_schema, AttributeDefinitions=[p_attr, x_attr],
                    VectorIndexes=[vector_index]) as table:
                pass
    # AttributeName must be a legal attribute name - e.g., an empty string is
    # not a legal attribute name, and is rejected. We also declare it (under
    # its illegal empty name) in AttributeDefinitions, so that the failure we
    # catch below is specifically about the illegal name, not about a missing
    # AttributeDefinitions entry (which we test separately below):
    vector_index['SearchSchema'] = [{'AttributeName': '', 'SearchSchemaElementType': 'INLINE_FILTER'}]
    with pytest.raises(ClientError, match='(?i)ValidationException.*AttributeName'):
        with new_test_table(dynamodb,
                KeySchema=key_schema,
                AttributeDefinitions=[p_attr, {'AttributeName': '', 'AttributeType': 'S'}],
                VectorIndexes=[vector_index]) as table:
            pass
    # A SearchSchema element referencing an attribute ('x') that isn't
    # declared in AttributeDefinitions is rejected - just like a GSI or LSI's
    # key attribute must be declared:
    vector_index['SearchSchema'] = [{'AttributeName': 'x', 'SearchSchemaElementType': 'INLINE_FILTER'}]
    with pytest.raises(ClientError, match='(?i)ValidationException.*attribute definitions'):
        with new_test_table(dynamodb,
                KeySchema=key_schema, AttributeDefinitions=[p_attr],  # 'x' is missing!
                VectorIndexes=[vector_index]) as table:
            pass
    # Conversely, if AttributeDefinitions includes an attribute ('y') that
    # isn't used anywhere - not as a table, GSI or LSI key, and not
    # referenced by any vector index's SearchSchema - CreateTable is
    # rejected. This isn't specific to vector indexes: it's DynamoDB's
    # general rule that every declared attribute must be used by something.
    vector_index['SearchSchema'] = [{'AttributeName': 'x', 'SearchSchemaElementType': 'HASH'}]
    with pytest.raises(ClientError, match='(?i)ValidationException.*[Aa]ttributeDefinitions'):
        with new_test_table(dynamodb,
                KeySchema=key_schema, AttributeDefinitions=[p_attr, x_attr, y_attr],  # 'y' is unused!
                VectorIndexes=[vector_index]) as table:
            pass

# Test that after a vector index is created with a SearchSchema that defines
# (in AttributeDefinitions) specific types for some attributes (e.g. S or N),
# it is no longer possible to write different types into those attributes.
# We check this for both HASH and INLINE_FILTER attributes, and for all three
# types allowed for these attributes: S(tring), N(umber) and B(inary) - the
# same three types allowed for a table, GSI or LSI's key attributes.
@pytest.mark.parametrize('attribute_type', ['S', 'N', 'B'])
def test_vectorindexes_searchschema_type_verification(dynamodb, attribute_type):
    # A value of the correct type, and one of a different (wrong) type, for
    # each of the three allowed types:
    good_values = {'S': 'hello', 'N': Decimal('42'), 'B': b'hello'}
    good_value = good_values[attribute_type]
    bad_value = good_values['N' if attribute_type != 'N' else 'S']
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[
                {'AttributeName': 'p', 'AttributeType': 'S'},
                {'AttributeName': 'x', 'AttributeType': attribute_type},
                {'AttributeName': 'y', 'AttributeType': attribute_type},
            ],
            VectorIndexes=[{
                'IndexName': 'vind',
                'VectorAttribute': {'AttributeName': 'v'},
                'Dimensions': 3,
                'DistanceFunction': 'COSINE',
                'Projection': {'ProjectionType': 'KEYS_ONLY'},
                'SearchSchema': [
                    {'AttributeName': 'x', 'SearchSchemaElementType': 'HASH'},
                    {'AttributeName': 'y', 'SearchSchemaElementType': 'INLINE_FILTER'},
                ],
            }]) as table:
        # 'x' is the vector index's HASH (partition key) attribute, declared
        # in AttributeDefinitions as type attribute_type. Writing a value of
        # a different type into it is rejected with a type mismatch, just
        # like it would be for a GSI or LSI key attribute:
        p = random_string()
        with pytest.raises(ClientError, match='ValidationException.*mismatch'):
            table.put_item(Item={'p': p, 'x': bad_value})
        assert 'Item' not in table.get_item(Key={'p': p}, ConsistentRead=True)
        # Same check for 'y' - the INLINE_FILTER attribute, also declared as
        # type attribute_type:
        with pytest.raises(ClientError, match='ValidationException.*mismatch'):
            table.put_item(Item={'p': p, 'y': bad_value})
        assert 'Item' not in table.get_item(Key={'p': p}, ConsistentRead=True)
        # For comparison, writing the correct (declared) type for both
        # attributes succeeds:
        item = {'p': p, 'x': good_value, 'y': good_value}
        table.put_item(Item=item)
        assert table.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == item

# Test that a certain attribute is chosen a SearchSchema's HASH attribute with
# string type, not only are you not allowed to insert non-string values (this
# is tested in test_vectorindexes_searchschema_type_verification) additionally
# it is forbidden to insert an *empty* string value.
# Conversely, in a INLINE_FILTER attribute, empty string values are allowed.
def test_vectorindexes_searchschema_empty_string_value(dynamodb):
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[
                {'AttributeName': 'p', 'AttributeType': 'S'},
                {'AttributeName': 'x', 'AttributeType': 'S'},
                {'AttributeName': 'y', 'AttributeType': 'S'},
            ],
            VectorIndexes=[{
                'IndexName': 'vind',
                'VectorAttribute': {'AttributeName': 'v'},
                'Dimensions': 3,
                'DistanceFunction': 'COSINE',
                'Projection': {'ProjectionType': 'KEYS_ONLY'},
                'SearchSchema': [
                    {'AttributeName': 'x', 'SearchSchemaElementType': 'HASH'},
                    {'AttributeName': 'y', 'SearchSchemaElementType': 'INLINE_FILTER'},
                ],
            }]) as table:
        p = random_string()
        # Writing an empty string into 'x' - the HASH (partition key)
        # attribute - is rejected, just like it would be for a table, GSI or
        # LSI key attribute (see test_item.py::test_key_empty_string_value):
        with pytest.raises(ClientError, match='ValidationException.*empty string'):
            table.put_item(Item={'p': p, 'x': ''})
        assert 'Item' not in table.get_item(Key={'p': p}, ConsistentRead=True)
        # But an empty string *is* allowed for 'y' - the INLINE_FILTER
        # attribute - since it isn't a key column:
        item = {'p': p, 'y': ''}
        table.put_item(Item=item)
        assert table.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == item


# Test which types can be used in AttributeDefinitions for a vector index's HASH
# and INLINE_FILTER attributes.
# Just like a table, GSI or LSI's key attributes (see test_forbidden_key_types
# and test_gsi_invalid_key_types), only S(tring), N(umber) and B(inary) are
# allowed - all other types are rejected. We don't check here that these three
# types *are* allowed - that will be checked by other tests that actually use
# them end-to-end.
def test_vectorindexes_searchschema_invalid_types(dynamodb):
    # The following are all the types that DynamoDB supports, as documented in
    # https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Programming.LowLevelAPI.html
    # (except S, N and B) - also the non-existent type "junk" yields the same
    # error message.
    for bad_type in ['BOOL', 'NULL', 'M', 'L', 'SS', 'NS', 'BS', 'junk']:
        for element_type in ['HASH', 'INLINE_FILTER']:
            with pytest.raises(ClientError, match=f"ValidationException.*'{bad_type}'"):
                with new_test_table(dynamodb,
                    KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
                    AttributeDefinitions=[
                        {'AttributeName': 'p', 'AttributeType': 'S'},
                        {'AttributeName': 'x', 'AttributeType': bad_type},
                    ],
                    VectorIndexes=[{
                        'IndexName': 'vind',
                        'VectorAttribute': {'AttributeName': 'v'},
                        'Dimensions': 3,
                        'DistanceFunction': 'COSINE',
                        'Projection': {'ProjectionType': 'KEYS_ONLY'},
                        'SearchSchema': [
                            {'AttributeName': 'x', 'SearchSchemaElementType': element_type},
                        ],
                    }]) as table:
                    pass

# Test how an attribute defined as INLINE_FILTER in the SearchSchema can then
# be used in SearchVectors searches in SearchConditionExpression to filter
# the results. This SearchConditionExpression is optional - the same index
# can be searched with or without filtering. Only the "=" operator is
# supported for the SearchConditionExpression.
def test_searchvectors_inline_filter(dynamodb, needs_vector_store):
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[
                {'AttributeName': 'p', 'AttributeType': 'S'},
                {'AttributeName': 'x', 'AttributeType': 'S'},
            ],
            VectorIndexes=[{
                'IndexName': 'vind',
                'VectorAttribute': {'AttributeName': 'v'},
                'Dimensions': 3,
                'DistanceFunction': 'COSINE',
                'Projection': {'ProjectionType': 'ALL'},
                'SearchSchema': [
                    {'AttributeName': 'x', 'SearchSchemaElementType': 'INLINE_FILTER'},
                ],
            }]) as table:
        wait_for_vector_index_active(table, 'vind')
        client = table.meta.client
        # Two items with the same vector (so both are equally good matches
        # for the search below), but different values of the inline-filter
        # attribute 'x'.
        p1 = random_string()
        p2 = random_string()
        table.put_item(Item={'p': p1, 'v': [1, 0, 0], 'x': 'foo'})
        table.put_item(Item={'p': p2, 'v': [1, 0, 0], 'x': 'bar'})
        # Without a SearchConditionExpression at all, both items are
        # candidates - retry until both appear in a TopK=2 search (this is
        # also how we confirm that both items have been indexed, before we
        # move on to the filtered searches below which don't need retrying).
        get_ps = lambda result: {item['Item']['p'] for item in result.get('SearchResults', [])}
        wait_for_search_vectors(client,
            condition=lambda result: get_ps(result) == {p1, p2},
            message=lambda result: f'Timed out waiting for both items to appear, got {get_ps(result) if result else None}',
            TableName=table.name, IndexName='vind', SearchVector=[1, 0, 0], TopK=2)
        # A SearchConditionExpression filtering on x='foo' returns only p1:
        result = client.search_vectors(
            TableName=table.name, IndexName='vind',
            SearchVector=[1, 0, 0], TopK=2,
            SearchConditionExpression='x = :val',
            ExpressionAttributeValues={':val': 'foo'})
        assert [item['Item']['p'] for item in result['SearchResults']] == [p1]
        # Similarly, filtering on x='bar' returns only p2:
        result = client.search_vectors(
            TableName=table.name, IndexName='vind',
            SearchVector=[1, 0, 0], TopK=2,
            SearchConditionExpression='x = :val',
            ExpressionAttributeValues={':val': 'bar'})
        assert [item['Item']['p'] for item in result['SearchResults']] == [p2]
        # A filter matching neither item's x value returns no results at all:
        result = client.search_vectors(
            TableName=table.name, IndexName='vind',
            SearchVector=[1, 0, 0], TopK=2,
            SearchConditionExpression='x = :val',
            ExpressionAttributeValues={':val': 'nonexistent'})
        assert result['SearchResults'] == []
        # Only the "=" operator is supported in SearchConditionExpression -
        # other operators (e.g. "<>") are rejected with a ValidationException:
        with pytest.raises(ClientError, match='ValidationException'):
            client.search_vectors(
                TableName=table.name, IndexName='vind',
                SearchVector=[1, 0, 0], TopK=2,
                SearchConditionExpression='x <> :val',
                ExpressionAttributeValues={':val': 'foo'})

# Test how an attribute defined as HASH in the SearchSchema can then be used
# in SearchVectors searches in SearchConditionExpression to filter the
# results. HASH is different from INLINE_FILTER in that it must be filtered
# in every request (with SearchConditionExpression) - it is not his optional.
# This is because it is implemented (as its name suggest, analogous to HASH
# keys in normal tables) as physical splitting of the index into multiple
# separate partitions. It is what ScyllaDB calls a "local index".
def test_searchvectors_hash_filter(dynamodb, needs_vector_store):
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[
                {'AttributeName': 'p', 'AttributeType': 'S'},
                {'AttributeName': 'x', 'AttributeType': 'S'},
            ],
            VectorIndexes=[{
                'IndexName': 'vind',
                'VectorAttribute': {'AttributeName': 'v'},
                'Dimensions': 3,
                'DistanceFunction': 'COSINE',
                'Projection': {'ProjectionType': 'ALL'},
                'SearchSchema': [
                    {'AttributeName': 'x', 'SearchSchemaElementType': 'HASH'},
                ],
            }]) as table:
        wait_for_vector_index_active(table, 'vind')
        client = table.meta.client
        # Two items with the same vector (so both are equally good matches),
        # but different values of the HASH partition-key attribute 'x'.
        p1 = random_string()
        p2 = random_string()
        x1 = random_string()
        x2 = random_string()
        table.put_item(Item={'p': p1, 'v': [1, 0, 0], 'x': x1})
        table.put_item(Item={'p': p2, 'v': [1, 0, 0], 'x': x2})
        # Searching without a SearchConditionExpression at all is rejected -
        # unlike INLINE_FILTER, the HASH element must always be given a value:
        with pytest.raises(ClientError, match='ValidationException'):
            client.search_vectors(
                TableName=table.name, IndexName='vind',
                SearchVector=[1, 0, 0], TopK=2)
        # Retry until p1 appears when searching its own partition (x=x1),
        # and separately until p2 appears when searching its own partition
        # (x=x2). These are two separate items, so even though they were
        # written one right after the other, they could be indexed at
        # slightly different times - each needs its own retry loop. Once
        # both succeed, indexing is confirmed complete for both, so the
        # further checks below don't need their own retry loops.
        for p, x in [(p1, x1), (p2, x2)]:
            get_ps = lambda result: [item['Item']['p'] for item in result.get('SearchResults', [])]
            wait_for_search_vectors(client,
                condition=lambda result: get_ps(result) == [p],
                message=lambda result: f'Timed out waiting for {p} to appear in its own partition, '
                    f'got {get_ps(result) if result else None}',
                TableName=table.name, IndexName='vind', SearchVector=[1, 0, 0], TopK=2,
                SearchConditionExpression='x = :val', ExpressionAttributeValues={':val': x})
        # Searching x1's partition never returns p2, even though p2's vector
        # is just as good a match - it's stored in a different partition:
        result = client.search_vectors(
            TableName=table.name, IndexName='vind',
            SearchVector=[1, 0, 0], TopK=2,
            SearchConditionExpression='x = :val',
            ExpressionAttributeValues={':val': x1})
        assert [item['Item']['p'] for item in result['SearchResults']] == [p1]
        # And searching x2's partition returns only p2:
        result = client.search_vectors(
            TableName=table.name, IndexName='vind',
            SearchVector=[1, 0, 0], TopK=2,
            SearchConditionExpression='x = :val',
            ExpressionAttributeValues={':val': x2})
        assert [item['Item']['p'] for item in result['SearchResults']] == [p2]
        # A partition value that doesn't match any item's 'x' returns no
        # results (not an error - the partition is simply empty):
        result = client.search_vectors(
            TableName=table.name, IndexName='vind',
            SearchVector=[1, 0, 0], TopK=2,
            SearchConditionExpression='x = :val',
            ExpressionAttributeValues={':val': random_string()})
        assert result['SearchResults'] == []

# Test a more complex SearchConditionExpression that combines a HASH element and
# two INLINE_FILTER elements, and see it all works together.
def test_searchvectors_hash_and_inline_filters(dynamodb, needs_vector_store):
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[
                {'AttributeName': 'p', 'AttributeType': 'S'},
                {'AttributeName': 'x', 'AttributeType': 'S'},
                {'AttributeName': 'y', 'AttributeType': 'S'},
                {'AttributeName': 'z', 'AttributeType': 'S'},
            ],
            VectorIndexes=[{
                'IndexName': 'vind',
                'VectorAttribute': {'AttributeName': 'v'},
                'Dimensions': 3,
                'DistanceFunction': 'COSINE',
                'Projection': {'ProjectionType': 'ALL'},
                'SearchSchema': [
                    {'AttributeName': 'x', 'SearchSchemaElementType': 'HASH'},
                    {'AttributeName': 'y', 'SearchSchemaElementType': 'INLINE_FILTER'},
                    {'AttributeName': 'z', 'SearchSchemaElementType': 'INLINE_FILTER'},
                ],
            }]) as table:
        wait_for_vector_index_active(table, 'vind')
        client = table.meta.client
        x1 = random_string()
        x2 = random_string()
        y1 = random_string()
        y2 = random_string()
        z1 = random_string()
        z2 = random_string()
        # All items share the same vector, so they're all equally good
        # matches - only the SearchConditionExpression should distinguish
        # between them. pA is the item we'll be searching for below; pB, pC
        # and pD each differ from pA in exactly one of z, y and x (the
        # partition), respectively. Note that pD happens to share pA's y
        # and z values - it's only its partition (x) that's different.
        pA = random_string()
        pB = random_string()
        pC = random_string()
        pD = random_string()
        table.put_item(Item={'p': pA, 'v': [1, 0, 0], 'x': x1, 'y': y1, 'z': z1})
        table.put_item(Item={'p': pB, 'v': [1, 0, 0], 'x': x1, 'y': y1, 'z': z2})
        table.put_item(Item={'p': pC, 'v': [1, 0, 0], 'x': x1, 'y': y2, 'z': z1})
        table.put_item(Item={'p': pD, 'v': [1, 0, 0], 'x': x2, 'y': y1, 'z': z1})
        # Retry (separately for each partition, as different items can be
        # indexed at slightly different times) until all of that partition's
        # items are indexed:
        for x, expected in [(x1, {pA, pB, pC}), (x2, {pD})]:
            get_ps = lambda result: {item['Item']['p'] for item in result.get('SearchResults', [])}
            wait_for_search_vectors(client,
                condition=lambda result: get_ps(result) == expected,
                message=lambda result: f'Timed out waiting for partition x={x} to contain {expected}, '
                    f'got {get_ps(result) if result else None}',
                TableName=table.name, IndexName='vind', SearchVector=[1, 0, 0], TopK=4,
                SearchConditionExpression='x = :x', ExpressionAttributeValues={':x': x})
        # Combining the HASH element with both INLINE_FILTER elements in a
        # single SearchConditionExpression: only pA matches all three:
        result = client.search_vectors(
            TableName=table.name, IndexName='vind',
            SearchVector=[1, 0, 0], TopK=4,
            SearchConditionExpression='x = :x AND y = :y AND z = :z',
            ExpressionAttributeValues={':x': x1, ':y': y1, ':z': z1})
        assert [item['Item']['p'] for item in result['SearchResults']] == [pA]
        # Changing just the 'z' INLINE_FILTER value to something that
        # matches nothing returns no results, even though 'x' and 'y' still
        # match pA and pB:
        result = client.search_vectors(
            TableName=table.name, IndexName='vind',
            SearchVector=[1, 0, 0], TopK=4,
            SearchConditionExpression='x = :x AND y = :y AND z = :z',
            ExpressionAttributeValues={':x': x1, ':y': y1, ':z': random_string()})
        assert result['SearchResults'] == []
        # Changing the HASH value to x2 scopes the search to the other
        # partition - where the very same y=y1, z=z1 filter values now match
        # pD instead of pA:
        result = client.search_vectors(
            TableName=table.name, IndexName='vind',
            SearchVector=[1, 0, 0], TopK=4,
            SearchConditionExpression='x = :x AND y = :y AND z = :z',
            ExpressionAttributeValues={':x': x2, ':y': y1, ':z': z1})
        assert [item['Item']['p'] for item in result['SearchResults']] == [pD]

# Test that when an item is added to the table which is missing an attribute
# that is defined as a HASH in the vector index's SearchSchema, that item is
# not returned in any vector search results. Conversely, if an item is missing
# an attribute that is defined as an INLINE_FILTER in the SearchSchema, it can
# still be returned in a vector search result, if not filtering on this
# specific attribute.
# The DynamoDB documentation explicitly warns about this case:
#   "If your vector index defines a partition key in the SearchSchema and you
#    write an item without that attribute (or remove it with UpdateItem), the
#    write succeeds on the base table but the item is silently excluded from
#    the vector index. It will not appear in SearchVectors results even though
#    the base table item and its vector embedding still exist. Make sure that
#    every item you want to be searchable contains the vector index partition
#    key attribute."
def test_searchvectors_missing_searchschema_attribute(dynamodb, needs_vector_store):
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[
                {'AttributeName': 'p', 'AttributeType': 'S'},
                {'AttributeName': 'x', 'AttributeType': 'S'},
                {'AttributeName': 'y', 'AttributeType': 'S'},
            ],
            VectorIndexes=[{
                'IndexName': 'vind',
                'VectorAttribute': {'AttributeName': 'v'},
                'Dimensions': 3,
                'DistanceFunction': 'COSINE',
                'Projection': {'ProjectionType': 'ALL'},
                'SearchSchema': [
                    {'AttributeName': 'x', 'SearchSchemaElementType': 'HASH'},
                    {'AttributeName': 'y', 'SearchSchemaElementType': 'INLINE_FILTER'},
                ],
            }]) as table:
        wait_for_vector_index_active(table, 'vind')
        client = table.meta.client
        xval = random_string()
        yval = random_string()
        # All three items share the same vector, so they'd all be equally
        # good matches, if they were indexed and matched the search's
        # partition:
        p_full = random_string()      # has both 'x' and 'y'
        p_no_hash = random_string()   # missing 'x' - the HASH attribute
        p_no_filter = random_string() # missing 'y' - the INLINE_FILTER attribute
        table.put_item(Item={'p': p_full, 'v': [1, 0, 0], 'x': xval, 'y': yval})
        table.put_item(Item={'p': p_no_hash, 'v': [1, 0, 0], 'y': yval})
        table.put_item(Item={'p': p_no_filter, 'v': [1, 0, 0], 'x': xval})
        # Retry, searching partition x=xval without filtering on 'y', until
        # both p_full and p_no_filter appear. p_no_filter is missing the
        # INLINE_FILTER attribute 'y' entirely, but that doesn't exclude it
        # from the index - it's still found here, since we're not filtering
        # on 'y'. p_no_hash, on the other hand, is missing the HASH
        # attribute 'x' itself, so it can never be part of (or found in) any
        # partition, including this one - once the loop below stabilizes on
        # the 2 expected items, we assert p_no_hash is never among them.
        get_ps = lambda result: {item['Item']['p'] for item in result.get('SearchResults', [])}
        result = wait_for_search_vectors(client,
            condition=lambda result: get_ps(result) == {p_full, p_no_filter},
            message=lambda result: f'Timed out waiting for p_full and p_no_filter to appear, '
                f'got {get_ps(result) if result else None}',
            TableName=table.name, IndexName='vind', SearchVector=[1, 0, 0], TopK=3,
            SearchConditionExpression='x = :x', ExpressionAttributeValues={':x': xval})
        assert p_no_hash not in get_ps(result)
        # Now also filter on y=yval: p_no_filter (which has no 'y' at all)
        # no longer matches, and only p_full remains:
        result = client.search_vectors(
            TableName=table.name, IndexName='vind',
            SearchVector=[1, 0, 0], TopK=3,
            SearchConditionExpression='x = :x AND y = :y',
            ExpressionAttributeValues={':x': xval, ':y': yval})
        assert [item['Item']['p'] for item in result['SearchResults']] == [p_full]

# Like other expressions tested in other test files, SearchConditionExpression
# also verifies that there are no unused AttributeNames or AttributeValues.
def test_searchvectors_searchconditionexpression_unused_names_and_values(dynamodb):
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[
                {'AttributeName': 'p', 'AttributeType': 'S'},
                {'AttributeName': 'x', 'AttributeType': 'S'},
            ],
            VectorIndexes=[{
                'IndexName': 'vind',
                'VectorAttribute': {'AttributeName': 'v'},
                'Dimensions': 3,
                'DistanceFunction': 'COSINE',
                'Projection': {'ProjectionType': 'KEYS_ONLY'},
                'SearchSchema': [
                    {'AttributeName': 'x', 'SearchSchemaElementType': 'INLINE_FILTER'},
                ],
            }]) as table:
        wait_for_vector_index_active(table, 'vind')
        client = table.meta.client
        # An ExpressionAttributeNames entry ('#unused') that isn't
        # referenced anywhere in SearchConditionExpression is rejected, even
        # though '#x' (the other entry) is used:
        with pytest.raises(ClientError, match='ValidationException.*unused'):
            client.search_vectors(
                TableName=table.name, IndexName='vind',
                SearchVector=[1, 2, 3], TopK=1,
                SearchConditionExpression='#x = :val',
                ExpressionAttributeNames={'#x': 'x', '#unused': 'y'},
                ExpressionAttributeValues={':val': 'a'},
            )
        # Similarly, an ExpressionAttributeValues entry (':unused') that
        # isn't referenced anywhere in SearchConditionExpression is
        # rejected, even though ':val' (the other entry) is used:
        with pytest.raises(ClientError, match='ValidationException.*unused'):
            client.search_vectors(
                TableName=table.name, IndexName='vind',
                SearchVector=[1, 2, 3], TopK=1,
                SearchConditionExpression='#x = :val',
                ExpressionAttributeNames={'#x': 'x'},
                ExpressionAttributeValues={':val': 'a', ':unused': 'b'},
            )

# Above we tested that when an item is added to the table, it is indexed and
# can be found by a vector search and also filtered by its filterable columns
# (HASH or INLINE_FILTER). This test checks that when a filterable columns of
# these two types is modified - or removed - the item is (eventually) no
# longer returned by a vector search that filters on the original value of
# that column.
def test_searchvectors_searchschema_attribute_modified(dynamodb, needs_vector_store):
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[
                {'AttributeName': 'p', 'AttributeType': 'S'},
                {'AttributeName': 'x', 'AttributeType': 'S'},
                {'AttributeName': 'y', 'AttributeType': 'S'},
            ],
            VectorIndexes=[{
                'IndexName': 'vind',
                'VectorAttribute': {'AttributeName': 'v'},
                'Dimensions': 3,
                'DistanceFunction': 'COSINE',
                'Projection': {'ProjectionType': 'ALL'},
                'SearchSchema': [
                    {'AttributeName': 'x', 'SearchSchemaElementType': 'HASH'},
                    {'AttributeName': 'y', 'SearchSchemaElementType': 'INLINE_FILTER'},
                ],
            }]) as table:
        wait_for_vector_index_active(table, 'vind')
        client = table.meta.client
        p = random_string()
        x_old, x_new = random_string(), random_string()
        y_old, y_new = random_string(), random_string()

        # True if item p is found by a search on partition x, optionally
        # (if y is given) also filtered on y:
        def found(x, y=None):
            if y is None:
                condition = 'x = :x'
                values = {':x': x}
            else:
                condition = 'x = :x AND y = :y'
                values = {':x': x, ':y': y}
            result = client.search_vectors(
                TableName=table.name, IndexName='vind',
                SearchVector=[1, 0, 0], TopK=1,
                SearchConditionExpression=condition,
                ExpressionAttributeValues=values)
            return any(item['Item']['p'] == p for item in result.get('SearchResults', []))

        def wait_until(cond, message):
            deadline = time.monotonic() + VECTOR_STORE_TIMEOUT
            while not cond():
                if time.monotonic() > deadline:
                    pytest.fail(message)
                time.sleep(0.1)

        table.put_item(Item={'p': p, 'v': [1, 0, 0], 'x': x_old, 'y': y_old})
        # Wait until the item is initially indexed and found:
        wait_until(lambda: found(x_old, y_old),
                   'Timed out waiting for item to be initially indexed')

        # Modify the HASH attribute 'x'. Eventually, a search still filtering
        # on the *old* x value should no longer find the item - it has moved
        # to a different partition:
        table.update_item(Key={'p': p}, UpdateExpression='SET x = :x',
            ExpressionAttributeValues={':x': x_new})
        wait_until(lambda: not found(x_old, y_old),
                   "Timed out waiting for item to disappear after changing its HASH attribute")
        # Confirm it can now be found under its new x value, before moving on:
        wait_until(lambda: found(x_new, y_old),
                   "Timed out waiting for item to appear under its new HASH value")

        # Modify the INLINE_FILTER attribute 'y'. Eventually, a search still
        # filtering on the *old* y value should no longer find the item:
        table.update_item(Key={'p': p}, UpdateExpression='SET y = :y',
            ExpressionAttributeValues={':y': y_new})
        wait_until(lambda: not found(x_new, y_old),
                   "Timed out waiting for item to disappear after changing its INLINE_FILTER attribute")
        wait_until(lambda: found(x_new, y_new),
                   "Timed out waiting for item to appear under its new INLINE_FILTER value")

        # Remove the INLINE_FILTER attribute 'y' entirely. Eventually, a
        # search still filtering on the old y value should no longer find
        # the item (even though, as tested elsewhere, the item remains in
        # the index and would be found if we didn't filter on 'y' at all):
        table.update_item(Key={'p': p}, UpdateExpression='REMOVE y')
        wait_until(lambda: not found(x_new, y_new),
                   "Timed out waiting for item to disappear after removing its INLINE_FILTER attribute")

        # Remove the HASH attribute 'x' entirely. Per the DynamoDB
        # documentation this "silently de-indexes" the item: eventually, a
        # search filtering on the old x value (with no 'y' condition at all)
        # should no longer find the item.
        table.update_item(Key={'p': p}, UpdateExpression='REMOVE x')
        wait_until(lambda: not found(x_new),
                   "Timed out waiting for item to disappear after removing its HASH attribute")


# TODO: check ReturnConsumedCapacity.
# - SearchVectors' ConsumedCapacity.VectorSearchRequestBytes and write ops'
#   ConsumedCapacity.VectorIndexes[name].VectorWriteRequestBytes are never
#   checked - not even that they're present and roughly reasonable (e.g.
#   nonzero after a write that touches a vector attribute, and metered at a
#   1 KB minimum per request per the documentation).
# TODO: It is documented that Vector indexes require on-demand (PAY_PER_REQUEST) capacity mode. Attempting to create a table with PROVISIONED billing and a vector index, or adding a vector index to an existing table with PROVISIONED billing should be refused. This test will not be interesting for Alternator, as we don't really support PROVISIONED anyway.
# TODO: test enabling vector index and Alternator Streams together, and
# checking that Alternator Streams works as expected. Also we may need to
# do something to avoid vector search's favorite parameters like TTL and
# post-changes to take control - or vice versa we may get CDC which isn't
# good enough for vector search.
# Note that today, Alternator Streams only works with vnodes while vector
# search doesn't work with vnodes - so we can't actually check this
# combination! But we must check it when Alternator Streams finally supports
# tablets.

################################################################################
# Checks for Alternator extensions over DynamoDB's vector search
################################################################################

# Test that if a table is created to use vnodes instead of the modern default
# of tablets, then it can't use a vector index because vector index is
# officially supported only with tablets.
# When we finally remove vnode support from the code, this test should be
# deleted.
def test_createtable_vectorindexes_vnodes_forbidden(vs):
    with pytest.raises(ClientError, match='ValidationException.*vnodes'):
        with new_test_table(vs,
            # set system:initial_tablets to a non-number to disable tablets:
            Tags=[{'Key': 'system:initial_tablets', 'Value': 'none'}],
            KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
            AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }],
            VectorIndexes=[
                {   'IndexName': 'hello',
                    'VectorAttribute': {'AttributeName': 'x', 'Dimensions': 7}
                }]
            ) as table:
            pass

# Test that if a table is created to use vnodes instead of the modern default
# of tablets, then one can't add to it a vector index because vector index is
# officially supported only with tablets. This is the UpdateTable version
# of a similar test for CreateTable above.
# When we finally remove vnode support from the code, this test should be
# deleted.
def test_updatetable_vectorindex_vnodes_forbidden(vs):
    with new_test_table(vs,
        # set system:initial_tablets to a non-number to disable tablets:
        Tags=[{'Key': 'system:initial_tablets', 'Value': 'none'}],
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
        AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }]) as table:
        with pytest.raises(ClientError, match='ValidationException.*vnodes'):
            table.update(VectorIndexUpdates=[{'Create':
                { 'IndexName': 'ind',
                  'VectorAttribute': {'AttributeName': 'x', 'Dimensions': 17 }
                }}])

# The context manager unconfigured_vector_store() temporarily (for the
# duration of the "with" block) un-configures the vector store in Scylla -
# the vector_store_primary_uri configuration option. This allows testing the
# behavior when the vector store is not configured, even if we are testing
# on a setup where it is configured.
@contextmanager
def unconfigured_vector_store(vs):
    # As mentioned in issue #28225, we can't write an empty string to the
    # configuration due to a bug. But luckily, we can write any garbage which
    # isn't a valid URI, and this will be considered unconfigured.
    # We also can't restore an empty configuration due to #28225.
    # When #28225 is fixed, this entire function can be simplified to just:
    #    with scylla_config_temporary_string(vs, 'vector_store_primary_uri', ''):
    #        yield
    # Instead we need to use the following mess:
    original_value = scylla_config_read(vs, 'vector_store_primary_uri')
    if original_value == '""':
        # nothing to do, or to restore
        yield
        return
    assert original_value.startswith('"') and original_value.endswith('"')
    original_value = original_value[1:-1]
    scylla_config_write(vs, 'vector_store_primary_uri', 'garbage')
    try:
        yield
    finally:
        scylla_config_write(vs, 'vector_store_primary_uri', original_value)

# If the vector store is not configured, then Query with VectorSearch is
# rejected with a ValidationException saying "Vector Store is disabled".
def test_query_vector_store_disabled(vs, table_vs):
    with unconfigured_vector_store(vs):
        with pytest.raises(ClientError, match='ValidationException.*Vector Store is disabled'):
            table_vs.query(IndexName='vind', VectorSearch={'QueryVector': [0, 0, 0]},
                           Limit=1)

# Test that even if the vector store is not configured, it is possible to
# create a vector index on the table - but DescribeTable will always show
# that it is CREATING, not ACTIVE.
# I'm not convinced it is a good idea to allow create vector indexes if
# the vector store isn't even configured in Scylla, but currently we do
# allow it.
def test_vectorindex_status_without_vector_store(vs):
    with unconfigured_vector_store(vs):
        with new_test_table(vs,
                KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
                AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
                VectorIndexes=[
                    {'IndexName': 'vind',
                     'VectorAttribute': {'AttributeName': 'v', 'Dimensions': 3}}
                ]) as table:
            desc = table.meta.client.describe_table(TableName=table.name)
            vector_indexes = desc['Table']['VectorIndexes']
            assert len(vector_indexes) == 1
            assert vector_indexes[0]['IndexName'] == 'vind'
            assert vector_indexes[0]['IndexStatus'] == 'CREATING'

# We want to be able to run these tests using an unmodified boto3 library -
# which doesn't understand the new parameters that Alternator added to
# CreateTable, Query, and so on, and moreover will strip unexpected fields
# in Alternator's responses.
# So the following fixture "vs" is a DynamoDB API connection, similar to our
# usual "dynamodb" fixture, but modified to allow our new vector-search
# parameters in the requests and responses.
#
# Users can use exactly the same code to get vector search support in boto3,
# but the more "official" way would be to modify botocore's JSON configuration
# file, botocore/data/dynamodb/2012-08-10/service-2.json.

# add_vs_to_client() is a context manager that modifies the given boto3
# client to accept the new vector-search parameters in requests and responses,
# and also monkey-patches the global TypeSerializer/TypeDeserializer so that
# the Vector type is serialized as FLOAT32VECTOR.
# On exit, only the global TypeSerializer/TypeDeserializer patches are
# restored. The per-client service-model mutations (new shapes, added
# members, AttributeValue.FLOAT32VECTOR, etc.) are not reverted.
@contextmanager
def add_vs_to_client(client):
    # All the new parameter "shapes" that we will use below for the
    # new parameters of the different operations:
    new_shapes = {
        # For CreateTable (and also DescribeTable's output)
        'VectorIndexes': {
            'type': 'list',
            'member': {'shape': 'VectorIndex'},
        },
        'VectorIndex': {
            'type': 'structure',
            'members': {
                'IndexName': {'shape': 'String'},
                'VectorAttribute': {'shape': 'VectorAttribute'},
                'Projection': {'shape': 'Projection'},
                'SimilarityFunction': {'shape': 'String'},
                # The following two fields are only returned in DescribeTable's
                # output, not accepted in CreateTable's input.
                'IndexStatus': {'shape': 'String'},
                'Backfilling': {'shape': 'BooleanObject'},
            },
            'required': ['IndexName', 'VectorAttribute'],
        },
        'VectorAttribute': {
            'type': 'structure',
            'members': {
                'AttributeName': {'shape': 'String'},
                'Dimensions': {'shape': 'Integer'},
            },
            'required': ['AttributeName', 'Dimensions'],
        },
        # For UpdateTable:
        'VectorIndexUpdates': {
            'type': 'list',
            'member': {'shape': 'VectorIndexUpdate'},
        },
        'VectorIndexUpdate': {
            'type': 'structure',
            'members': {
                'Create': {'shape': 'CreateVectorIndexAction'},
                'Delete': {'shape': 'DeleteVectorIndexAction'},
            }
        },
        'CreateVectorIndexAction': {
            'type': 'structure',
            'members': {
                'IndexName': {'shape': 'String'},
                'VectorAttribute': {'shape': 'VectorAttribute'},
                'Projection': {'shape': 'Projection'},
                'SimilarityFunction': {'shape': 'String'},
            },
            'required': ['IndexName', 'VectorAttribute'],
        },
        'DeleteVectorIndexAction': {
            'type': 'structure',
            'members': {
                'IndexName': {'shape': 'String'},
            },
            'required': ['IndexName'],
        },
        # For Query:
        'VectorSearch': {
            'type': 'structure',
            'members': {
                'QueryVector': {'shape': 'AttributeValue'},
                'ReturnScores': {'shape': 'String'},
            },
            'required': ['QueryVector'],
        },
        # For VectorSearch.ReturnScores response:
        'Score': {'type': 'double'},
        'ScoresList': {
            'type': 'list',
            'member': {'shape': 'Score'},
        },
        # For the 'FLOAT32VECTOR' (optimized vector) type: a list of raw JSON
        # numbers.
        'Float32VectorElement': {'type': 'double'},
        'Float32VectorAttributeValue': {
            'type': 'list',
            'member': {'shape': 'Float32VectorElement'},
        },
    }
    # Register the new shapes:
    service_model = client.meta.service_model
    shape_resolver = service_model._shape_resolver
    for shape_name, shape_def in new_shapes.items():
        shape_resolver._shape_map[shape_name] = shape_def
        # Evict any cached shapes for these names
        shape_resolver._shape_cache.pop(shape_name, None)

    # Add a VectorIndexes parameter to CreateTable
    create_table_op = service_model.operation_model('CreateTable')
    input_shape = create_table_op.input_shape
    input_shape._shape_model['members']['VectorIndexes'] = {
        'shape': 'VectorIndexes'
    }
    input_shape._cache.pop('members', None)

    # Add VectorIndexUpdates parameter to UpdateTable
    update_table_op = service_model.operation_model('UpdateTable')
    input_shape = update_table_op.input_shape
    input_shape._shape_model['members']['VectorIndexUpdates'] = {
        'shape': 'VectorIndexUpdates'
    }
    input_shape._cache.pop('members', None)

    # Add a VectorSearch parameter to Query
    query_op = service_model.operation_model('Query')
    input_shape = query_op.input_shape
    input_shape._shape_model['members']['VectorSearch'] = {
        'shape': 'VectorSearch'
    }
    input_shape._cache.pop('members', None)

    # Add Scores list to Query output
    query_output_shape = query_op.output_shape
    query_output_shape._shape_model['members']['Scores'] = {'shape': 'ScoresList'}
    query_output_shape._cache.pop('members', None)

    # Add a VectorIndexes field to "TableDescription", the shape returned
    # by DescribeTable and also CreateTable
    output_shape = shape_resolver.get_shape_by_name('TableDescription')
    output_shape._shape_model['members']['VectorIndexes'] = {
        'shape': 'VectorIndexes'
    }
    output_shape._cache.pop('members', None)
    shape_resolver._shape_cache.pop('TableDescription', None)

    # Add FLOAT32VECTOR (the new optimized vector type) to the AttributeValue
    # shape, so that boto3 will accept and pass through the FLOAT32VECTOR type
    # in requests and responses. FLOAT32VECTOR holds a list of floating-point
    # numbers.
    attribute_value_shape = shape_resolver.get_shape_by_name('AttributeValue')
    attribute_value_shape._shape_model['members']['FLOAT32VECTOR'] = {'shape': 'Float32VectorAttributeValue'}
    attribute_value_shape._cache.pop('members', None)
    shape_resolver._shape_cache.pop('AttributeValue', None)

    # Monkey-patch boto3 resource's TypeSerializer so that values of type
    # "Vector" (a class defined below) are serialized into the JSON request as
    # {"FLOAT32VECTOR": [1.0, ...]} (JSON numbers) instead of the standard
    # list encoding {"L": [{"N": "1.0"}, ...]}. This allows the high-level
    # resource interface (table.put_item etc.) to send Vector attributes
    # without needing client_no_transform.
    _orig_serialize = boto3.dynamodb.types.TypeSerializer.serialize
    def _serialize_with_vector(self, value):
        if isinstance(value, Vector):
            return {'FLOAT32VECTOR': list(value)}
        return _orig_serialize(self, value)
    boto3.dynamodb.types.TypeSerializer.serialize = _serialize_with_vector
    _sentinel = object()
    _orig_deserialize_float32vector = getattr(boto3.dynamodb.types.TypeDeserializer, '_deserialize_float32vector', _sentinel)
    boto3.dynamodb.types.TypeDeserializer._deserialize_float32vector = lambda self, value: Vector(value)
    try:
        yield
    finally:
        boto3.dynamodb.types.TypeSerializer.serialize = _orig_serialize
        if _orig_deserialize_float32vector is _sentinel:
            del boto3.dynamodb.types.TypeDeserializer._deserialize_float32vector
        else:
            boto3.dynamodb.types.TypeDeserializer._deserialize_float32vector = _orig_deserialize_float32vector


@pytest.fixture(scope="module")
def vs(new_dynamodb_session, dynamodb):
    if is_aws(dynamodb):
        skip_env('Scylla-only: vector search extensions not available on DynamoDB')
    resource = new_dynamodb_session()
    with add_vs_to_client(resource.meta.client):
        yield resource

# Use the Vector(list) type for test values that are meant to be stored as
# optimized vectors (array of floats instead of JSON list of numbers).
# The serialization monkey-patching in the vs fixture will cause this list
# to be serialized and sent to Alternator as
# {'FLOAT32VECTOR': [1.0, 2.0, ...]}} instead of the standard list-of-numbers
# {'L': [{'N': '1.0'}, ...]}.
class Vector(list):
    pass

################################### FLOAT32VECTOR ##############################
# Tests for the optimized vector type, "FLOAT32VECTOR". This is a new type not
# supported by DynamoDB. It knows all elements are numbers and only guarantees
# 32-bit floating point precision, so allows Scylla to store the vector much
# more efficiently, using 32-bit floats instead of textual JSON representation.

# Check that we can write and then read back a "FLOAT32VECTOR" top-level
# attribute. We use manual_request() to bypass boto3's serializer entirely,
# because boto3 does not know the "FLOAT32VECTOR" type and would reject it
# before sending. Scylla shouldn't reject this value - in the worst case it
# could store the attribute as a JSON string
# {"FLOAT32VECTOR": [1.0, 2.0, 3.0]} - but ideally it should understand the
# "FLOAT32VECTOR" type and store it as a native array of floats.
#
# Writing tests with "manual_request" is ugly. So in the next tests we will
# check the same thing with progressively more convenient ways to write the
# test.
def test_put_and_get_toplevel_v_manual_request(test_table_s):
    p = random_string()
    v = [1.0, 2.0, 3.0]
    manual_request(test_table_s, 'PutItem', json.dumps({
        'TableName': test_table_s.name,
        'Item': {'p': {'S': p}, 'v': {'FLOAT32VECTOR': v}},
    }))
    result = manual_request(test_table_s, 'GetItem', json.dumps({
        'TableName': test_table_s.name,
        'Key': {'p': {'S': p}},
        'ConsistentRead': True,
    }))
    assert 'Item' in result
    assert result['Item']['p'] == {'S': p}
    assert result['Item']['v'] == {'FLOAT32VECTOR': v}

# Same as test_put_and_get_toplevel_v_manual_request, but using the vs
# fixture (which patches the 'FLOAT32VECTOR' shape into boto3's
# AttributeValue) and client_no_transform, so boto3 can serialize and
# deserialize the 'FLOAT32VECTOR' type.
#
# Writing tests with "client_no_transform" is still a bit ugly, so the
# next test will do the same thing again even more conveniently.
def test_put_and_get_toplevel_v_client_no_transform(test_table_s, vs):
    p = random_string()
    v = [1.0, 2.0, 3.0]
    with client_no_transform(vs.meta.client) as client:
        client.put_item(
            TableName=test_table_s.name,
            Item={'p': {'S': p}, 'v': {'FLOAT32VECTOR': v}},
        )
        result = client.get_item(
            TableName=test_table_s.name,
            Key={'p': {'S': p}},
            ConsistentRead=True,
        )
    assert 'Item' in result
    assert result['Item']['p'] == {'S': p}
    assert result['Item']['v'] == {'FLOAT32VECTOR': v}

# Finally is the same test again, using the new Vector(...) instead of
# ugly hacks like manual_request and client_no_transform. This finally
# looks like a good enough API to give to users, and is the approach
# we'll use below for the rest of the tests for the optimized vector type.
# Note: we must use a table from the vs fixture (here table_vs) and not an
# ordinary table (like test_table_s), because the FLOAT32VECTOR type is only
# patched into the AttributeValue shape of the vs client. An ordinary table's
# client would fail when botocore encounters the unknown FLOAT32VECTOR member.
def test_put_and_get_toplevel_v(table_vs):
    p = random_string()
    v = Vector([1.0, 2.0, 3.0])
    table_vs.put_item(Item={'p': p, 'v': v})
    result = table_vs.get_item(Key={'p': p}, ConsistentRead=True)
    assert 'Item' in result
    assert result['Item']['p'] == p
    assert result['Item']['v'] == v

# Test that on a table with vector index enabled, we can't insert a vector
# with a floating-point value that doesn't fit in 32 bits.
def test_vector_float32_range(table_vs):
    p = random_string()
    # 1e100 and -1e100 are finite doubles but become infinite as 32-bit float
    with pytest.raises(ClientError, match='ValidationException.*32-bit'):
        table_vs.put_item(Item={'p': p, 'v': Vector([1.0, 1e100, 3.0])})
    with pytest.raises(ClientError, match='ValidationException.*32-bit'):
        table_vs.put_item(Item={'p': p, 'v': Vector([1.0, -1e100, 3.0])})

# Actually, the limitation that vector components must be finite (like all
# numbers in JSON) when saved as 32-bit floats doesn't require a vector
# index to be enabled. It should be enforced for any FLOAT32VECTOR attribute,
# even if it's not indexed.
def test_vector_float32_range_no_index(test_table_s, vs):
    p = random_string()
    # test_table_s has no vector index on it - but we should still reject
    # a "FLOAT32VECTOR" attribute value whose elements overflow 32-bit float.
    with pytest.raises(ClientError, match='ValidationException.*32-bit'):
        vs.meta.client.put_item(TableName=test_table_s.name,
            Item={'p': p, 'v': Vector([1.0, 1e100, 3.0])})
    with pytest.raises(ClientError, match='ValidationException.*32-bit'):
        vs.meta.client.put_item(TableName=test_table_s.name,
            Item={'p': p, 'v': Vector([1.0, -1e100, 3.0])})

# A floating-point numbers with more significant digits than a 32-bit float
# allows is allowed, but silently truncated to 32-bit precision. It is *not*
# rejected. Check that we can read it back with some loss of precision,
# below the 32-bit float epsilon.
def test_vector_float32_precision(table_vs):
    p = random_string()
    # FLT_EPSILON is the difference between 1.0 and the next representable
    # 32-bit float greater than 1.0. Any value between 1 and 1+FLT_EPSILON
    # will be indistinguishable from 1.0 when truncated to 32-bit float
    # precision.
    FLT_EPSILON = 1.1920928955078125e-7
    # x = 1.0 + FLT_EPSILON/2 is distinguishable from 1.0 in Python's
    # double-precision (64-bit) floating-point, but indistinguishable from
    # 1.0 when Alternator will save it as 32-bit and read it back.
    x = 1.0 + FLT_EPSILON / 2
    table_vs.put_item(Item={'p': p, 'v': Vector([1.0, x, 3.0])})
    result = table_vs.get_item(Key={'p': p}, ConsistentRead=True)
    v = result['Item']['v']
    assert isinstance(v, Vector)
    # The middle value should be truncated to 32-bit float precision, so it
    # should be equal to 1.0 within the 32-bit float epsilon (but not equal
    # to the original value which had more precision).
    assert abs(v[1] - 1.0) < FLT_EPSILON

# Continue the test above (test_vector_float32_precision) to confirm that
# the vector components are really truncated to 32-bit precision and not
# wastefully stored with higher precision.
# Importantly, this test proves that the vector value is stored in an
# *optimized* way, and validates its main benefit over the unoptimized
# list-of-numbers approach.
def test_vector_float32_optimized(table_vs):
    p = random_string()
    FLT_EPSILON = 1.1920928955078125e-7
    x = 1.0 + FLT_EPSILON / 2
    table_vs.put_item(Item={'p': p, 'v': Vector([1.0, x, 3.0])})
    result = table_vs.get_item(Key={'p': p}, ConsistentRead=True)
    v = result['Item']['v']
    # The middle value should be truncated to exactly 1.0.
    assert v[1] == 1.0

# Test more directly (using CQL) that the vector is stored in the underlying
# table in an optimized way - and also exactly how it is encoded. It's
# important that we don't unintentionally change this encoding, because the
# vector store needs to know how to read it.
# This test is similar to the tests in test_encoding.py.
def test_vector_encoding(table_vs, cql):
    p = random_string()
    # We pick example values that have an accurate representation in
    # 32-bit float, so we know exactly what we expect to be stored.
    table_vs.put_item(Item={'p': p, 'v': Vector([1.0, 2.5, -3.25])})
    ks = 'alternator_' + table_vs.name
    cf = table_vs.name
    rows = list(cql.execute(
        f'SELECT ":attrs" FROM "{ks}"."{cf}" WHERE p = \'{p}\''))
    assert len(rows) == 1
    attrs = rows[0][0]
    assert 'v' in attrs
    # The 'v' attribute should be encoded by a single byte 5
    # (alternator_type::FLOAT32VECTOR) followed directly by the 3 float32
    # values 1.0, 2.5, -3.25 in big-endian binary. No explicit length field.
    ALTERNATOR_TYPE_FLOAT32VECTOR = 5
    v = attrs['v']
    assert isinstance(v, bytes)
    assert v[0] == ALTERNATOR_TYPE_FLOAT32VECTOR
    N = 3
    assert len(v) == 1 + N * 4
    # We can check that the values are correct by unpacking them as big-endian
    # float32 values.
    values = struct.unpack('>' + 'f' * N, v[1:])
    assert values == (1.0, 2.5, -3.25)

# Test that we can use a "vector" attribute as a non top-level attribute.
# It might be stored unoptimized as a JSON value ({"FLOAT32VECTOR": [...]}) but it
# should still work and be retrievable and searchable.
def test_put_and_get_nested_vector_value(table_vs):
    p = random_string()
    # Store a Vector nested inside a map attribute, not as a top-level attribute.
    item = {'p': p, 'nested': {'v': Vector([1.0, 2.0, 3.0])}}
    table_vs.put_item(Item=item)
    result = table_vs.get_item(Key={'p': p}, ConsistentRead=True)
    assert 'Item' in result
    # Because the vector elements are whole numbers, we expect them to be
    # returned without loss of precision, whether or not they were stored
    # in an optimized way or not (which we don't want to assert in this
    # test). So we can check that the returned item is exactly equal to the
    # original item we put, including the nested Vector.
    assert result['Item'] == item

# When an attribute does not yet have a vector index on it, it is possible
# to write to it vectors of any length (even the zero length). But when
# the attribute does have an vector index, writes with the wrong length are
# rejected.
def test_vector_float32vector_any_length_without_index(test_table_s, vs):
    p = random_string()
    for length in [0, 1, 3, 42]:
        # Without a vector index, FLOAT32VECTOR vectors of any length
        # (including empty) are allowed:
        vs.meta.client.put_item(TableName=test_table_s.name,
            Item={'p': p, 'v': Vector([1.0 for i in range(length)])})

def test_vector_float32vector_wrong_length_with_index(table_vs):
    p = random_string()
    # table_vs has a vector index on 'v' with Dimensions=3, so only length-3
    # FLOAT32VECTOR vectors are accepted. Other lengths should be rejected.
    for bad_length in [0, 1, 2, 4, 42]:
        with pytest.raises(ClientError, match='ValidationException.*exactly 3'):
            table_vs.put_item(Item={'p': p, 'v': Vector([1.0 for i in range(bad_length)])})

# Test a vector-search Query when some of the vector attributes are written
# using the optimized "FLOAT32VECTOR" type. The optimized vector type is
# recommended, but not mandatory - users can also use a list of numbers
# ("L" of "N"), so to confirm this, this test writes one vector with an
# optimized type and one with an unoptimized type, and checks that both are
# visible in the vector search results.
def test_query_vector_float32vector_and_lon(vs, needs_vector_store):
    with new_test_table(vs,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
        p_v = random_string()  # written with the optimized FLOAT32VECTOR type
        p_l = random_string()  # written with the standard L-of-N type
        table.put_item(Item={'p': p_v, 'v': Vector([1.0, 0.0, 0.0])})
        table.put_item(Item={'p': p_l, 'v': [Decimal("1"), Decimal("0"), Decimal("0")]})
        table.update(VectorIndexUpdates=[{'Create':
            {'IndexName': 'vind',
             'VectorAttribute': {'AttributeName': 'v', 'Dimensions': 3}}}])
        # wait_for_vector_index_active() ensures the prefill scan is complete,
        # so both items are guaranteed to be indexed.
        wait_for_vector_index_active(table, 'vind')

        result = table.query(
            IndexName='vind',
            VectorSearch={'QueryVector': [Decimal("1"), Decimal("0"), Decimal("0")]},
            Limit=2,
        )
        assert {item['p'] for item in result.get('Items', [])} == {p_v, p_l}

        # QueryVector can also be given as a "FLOAT32VECTOR" type if we want,
        # instead of a list of numbers. Verify that this really works:
        result = table.query(
            IndexName='vind',
            VectorSearch={'QueryVector': Vector([1, 0, 0])},
            Limit=2,
        )
        assert {item['p'] for item in result.get('Items', [])} == {p_v, p_l}

# In test_query_vector_float32vector_and_lon we verified that "FLOAT32VECTOR"
# and "L"-of-"N" vectors are both indexed for the prefill case (the items were
# written before the index was created). For completeness, we should also
# check that they are also read correctly when written after the index is
# created - i.e. when noticed with CDC.
def test_query_vector_float32vector_and_lon_cdc(vs, needs_vector_store):
    with new_test_table(vs,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
            VectorIndexes=[{'IndexName': 'vind',
                            'VectorAttribute': {'AttributeName': 'v', 'Dimensions': 3}}]) as table:
        # Wait until the vector store is ready (prefill of the empty table
        # has completed), to ensure our writes are picked up via CDC, not
        # prefill.
        wait_for_vector_index_active(table, 'vind')
        p_v = random_string()  # written with the optimized FLOAT32VECTOR type
        p_l = random_string()  # written with the standard L-of-N type
        table.put_item(Item={'p': p_v, 'v': Vector([1.0, 0.0, 0.0])})
        table.put_item(Item={'p': p_l, 'v': [Decimal("1"), Decimal("0"), Decimal("0")]})
        # Retry the query until both items appear in the vector search results.
        deadline = time.monotonic() + VECTOR_STORE_TIMEOUT
        while True:
            result = table.query(
                IndexName='vind',
                VectorSearch={'QueryVector': [Decimal("1"), Decimal("0"), Decimal("0")]},
                Limit=2,
            )
            if {item['p'] for item in result.get('Items', [])} == {p_v, p_l}:
                break
            if time.monotonic() > deadline:
                pytest.fail('Timed out waiting for V-type and L-type items to appear via CDC')
            time.sleep(0.1)

##############################################################################
# Tests for vector search pre-filtering via KeyConditionExpression.
#
# KeyConditionExpression in a vector search query is sent to the vector store
# as a pre-filter: only nearest neighbors from the matching set are returned.
# Only projected attributes (currently key columns) may be referenced.
# Operators supported: =, <, <=, >, >=, IN, BETWEEN. OR and NOT are rejected.
##############################################################################

# Test that the old-style KeyConditions (non-expression API) is rejected for
# vector search queries (just like QueryFilter is rejected).
def test_query_vectorsearch_key_conditions_rejected(table_vs):
    with pytest.raises(ClientError, match='ValidationException.*KeyConditions'):
        table_vs.query(
            IndexName='vind',
            VectorSearch={'QueryVector': [0, 0, 1]},
            Limit=1,
            KeyConditions={'p': {'AttributeValueList': [{'S': 'x'}], 'ComparisonOperator': 'EQ'}},
        )

# Test that KeyConditionExpression referencing a non-projected attribute is
# rejected with a ValidationException.
def test_query_vectorsearch_key_condition_nonprojected(table_vs):
    # The table 'table_vs' has only 'p' as a key column. 'v' (the vector
    # attribute) and 'x' (a regular attribute) are not projected.
    for bad_attr in ['v', 'x']:
        with pytest.raises(ClientError, match='ValidationException'):
            table_vs.query(
                IndexName='vind',
                VectorSearch={'QueryVector': [0, 0, 1]},
                Limit=1,
                KeyConditionExpression=f'{bad_attr} = :val',
                ExpressionAttributeValues={':val': 'anything'},
            )

# Test that NOT in a KeyConditionExpression is rejected for vector search
# pre-filtering.
def test_query_vectorsearch_key_condition_not_rejected(table_vs):
    with pytest.raises(ClientError, match='ValidationException'):
        table_vs.query(
            IndexName='vind',
            VectorSearch={'QueryVector': [0, 0, 1]},
            Limit=1,
            KeyConditionExpression='NOT p = :p',
            ExpressionAttributeValues={':p': 'x'},
        )

# Test that OR in a KeyConditionExpression is rejected for vector search
# pre-filtering. This is because the vector store currently only supports
# AND among a list of conditions given to it in the pre-filter.
def test_query_vectorsearch_key_condition_or_rejected(table_vs):
    with pytest.raises(ClientError, match='ValidationException'):
        table_vs.query(
            IndexName='vind',
            VectorSearch={'QueryVector': [0, 0, 1]},
            Limit=1,
            KeyConditionExpression='p = :p1 OR p = :p2',
            ExpressionAttributeValues={':p1': 'x', ':p2': 'y'},
        )

# Test that an unsupported operator (<>) in a KeyConditionExpression is
# rejected for vector search pre-filtering. The vector store does not
# currently support a not-equal operator.
def test_query_vectorsearch_key_condition_ne_rejected(table_vs):
    with pytest.raises(ClientError, match='ValidationException'):
        table_vs.query(
            IndexName='vind',
            VectorSearch={'QueryVector': [0, 0, 1]},
            Limit=1,
            KeyConditionExpression='p <> :p',
            ExpressionAttributeValues={':p': 'x'},
        )

# Test that a boolean function call (e.g. attribute_exists()) in a
# KeyConditionExpression is rejected. attribute_exists() is valid in a
# FilterExpression but not in a vector search KeyConditionExpression
# because the vector store can't support it.
def test_query_vectorsearch_key_condition_function_rejected(table_vs):
    with pytest.raises(ClientError, match='ValidationException'):
        table_vs.query(
            IndexName='vind',
            VectorSearch={'QueryVector': [0, 0, 1]},
            Limit=1,
            KeyConditionExpression='attribute_exists(p)',
        )

# Test that using an unsupported value type (e.g. a list) in a
# KeyConditionExpression pre-filter is rejected with a ValidationException.
# Only S (string) and N (number) are valid types for pre-filter comparisons.
# Note that we can't even pass N when the attribute is a key column of type
# S - this will be tested below (test_query_vectorsearch_prefilter_type_mismatch).
def test_query_vectorsearch_prefilter_bad_value_type(table_vs):
    for bad_val in [
        ['hello', 'world'],      # List -> DynamoDB L type
        {'key': 'val'},          # Map -> DynamoDB M type
        b'hello',                # bytes -> DynamoDB B type
    ]:
        with pytest.raises(ClientError, match='ValidationException.*KeyConditionExpression'):
            table_vs.query(
                IndexName='vind',
                VectorSearch={'QueryVector': [0, 0, 1]},
                Limit=1,
                KeyConditionExpression='p = :p',
                ExpressionAttributeValues={':p': bad_val},
            )

# Test that passing a constant whose DynamoDB type does not match the key
# column's declared type is rejected with a ValidationException.  table_vs
# has a string (S) partition key 'p', so passing a numeric (N) value should
# be rejected for all comparison operators.
def test_query_vectorsearch_prefilter_type_mismatch(table_vs):
    num_val = Decimal('42')
    for kce, vals in [
        ('p = :v',                {':v': num_val}),
        ('p < :v',                {':v': num_val}),
        ('p <= :v',               {':v': num_val}),
        ('p > :v',                {':v': num_val}),
        ('p >= :v',               {':v': num_val}),
        ('p IN (:v)',             {':v': num_val}),
        ('p BETWEEN :lo AND :hi', {':lo': num_val, ':hi': num_val}),
    ]:
        # This test doesn't request a vector store to be configured (no
        # needs_vector_store fixture) so following query() can't succeed;
        # But we expect to get the specific error "Type mismatch" before
        # the query is even gets to checking if there is a vector store.
        with pytest.raises(ClientError, match='ValidationException.*Type mismatch'):
            table_vs.query(
                IndexName='vind',
                VectorSearch={'QueryVector': [0, 0, 1]},
                Limit=1,
                KeyConditionExpression=kce,
                ExpressionAttributeValues=vals,
            )

# Test that using a nested attribute path (e.g. "a.b") in a
# KeyConditionExpression pre-filter is rejected with a ValidationException.
# Only top-level attribute names are supported; nested paths would require
# the vector store to understand DynamoDB's nested document model.
def test_query_vectorsearch_key_condition_nested_attr_rejected(table_vs):
    for kce, vals in [
        ('a.b = :v',             {':v': 'x'}),
        ('a.b < :v',             {':v': 'x'}),
        ('a.b IN (:v)',          {':v': 'x'}),
        ('a.b BETWEEN :lo AND :hi', {':lo': 'x', ':hi': 'z'}),
    ]:
        with pytest.raises(ClientError, match='ValidationException.*nested'):
            table_vs.query(
                IndexName='vind',
                VectorSearch={'QueryVector': [0, 0, 1]},
                Limit=1,
                KeyConditionExpression=kce,
                ExpressionAttributeValues=vals,
            )

# Success-path test: KeyConditionExpression pre-filter is applied by the vector
# store before ANN, so the result always contains exactly Limit items (if that
# many matching items exist) regardless of how many non-matching items are
# nearer to the query vector.
#
# Setup: 4 items in the table. 2 "keep" items have vectors far from the query
# ([0,0,1]); 2 "drop" items have vectors very close to the query. With
# Limit=2:
#   - Without pre-filtering: ANN returns the 2 "drop" items (nearest), then a
#     post-filter would keep 0 items.
#   - With pre-filtering (KeyConditionExpression p IN ('keep1','keep2')): ANN
#     only considers "keep" items and returns both -> Count=2, ScannedCount=2.
def test_query_vectorsearch_prefilter_in(vs, needs_vector_store):
    with new_test_table(vs,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
        p_keep1, p_keep2 = random_string(), random_string()
        p_drop1, p_drop2 = random_string(), random_string()
        # "drop" items are nearest to the query vector [0, 0, 1].
        # "keep" items are farther away.
        table.put_item(Item={'p': p_drop1, 'v': Vector([0, 0, 1])})
        table.put_item(Item={'p': p_drop2, 'v': Vector([0, 0.1, 1])})
        table.put_item(Item={'p': p_keep1, 'v': Vector([1, 0, 0])})
        table.put_item(Item={'p': p_keep2, 'v': Vector([0.9, 0, 0])})
        table.update(VectorIndexUpdates=[{'Create':
            {'IndexName': 'vind',
             'VectorAttribute': {'AttributeName': 'v', 'Dimensions': 3}}}])
        wait_for_vector_index_active(table, 'vind')
        # With Limit=2 and a pre-filter restricting to keep1/keep2, we expect
        # exactly 2 results (both keep items), even though the 2 nearest overall
        # items are the drop items.
        result = table.query(
            IndexName='vind',
            VectorSearch={'QueryVector': Vector([0, 0, 1])},
            Limit=2,
            KeyConditionExpression='p IN (:k1, :k2)',
            ExpressionAttributeValues={':k1': p_keep1, ':k2': p_keep2},
        )
        assert result['Count'] == 2 and result['ScannedCount'] == 2
        assert {item['p'] for item in result['Items']} == {p_keep1, p_keep2}
        # Let's contrast the above pre-filter with how post-filter works:
        # without a filter, Limit=2 returns the 2 nearest items - the "drop"
        # items. A post-FilterExpression on 'p' asking for the keep items
        # would then return 0 results. This demonstrates why pre-filtering is
        # important.
        result = table.query(
            IndexName='vind',
            VectorSearch={'QueryVector': Vector([0, 0, 1])},
            Limit=2,
        )
        assert {item['p'] for item in result['Items']} == {p_drop1, p_drop2}
        result = table.query(
            IndexName='vind',
            VectorSearch={'QueryVector': Vector([0, 0, 1])},
            Limit=2,
            FilterExpression='p IN (:k1, :k2)',
            ExpressionAttributeValues={':k1': p_keep1, ':k2': p_keep2},
        )
        assert result['Count'] == 0
        assert result['ScannedCount'] == 2

# Success-path test: KeyConditionExpression with equality on the partition key.
# Similar to test_query_vectorsearch_prefilter_in but uses a single = condition.
# With Limit=1, the pre-filter restricts the ANN to a single partition key, so
# exactly 1 result is returned even though closer items exist.
def test_query_vectorsearch_prefilter_equality(vs, needs_vector_store):
    with new_test_table(vs,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
        p_keep = random_string()
        p_drop = random_string()
        table.put_item(Item={'p': p_drop, 'v': Vector([0, 0, 1])})   # nearest to query
        table.put_item(Item={'p': p_keep, 'v': Vector([1, 0, 0])})   # farther
        table.update(VectorIndexUpdates=[{'Create':
            {'IndexName': 'vind',
             'VectorAttribute': {'AttributeName': 'v', 'Dimensions': 3}}}])
        wait_for_vector_index_active(table, 'vind')
        result = table.query(
            IndexName='vind',
            VectorSearch={'QueryVector': Vector([0, 0, 1])},
            Limit=1,
            KeyConditionExpression='p = :p',
            ExpressionAttributeValues={':p': p_keep},
        )
        # The pre-filter restricts to p_keep only, so we get exactly 1 result
        # even though p_drop is the nearest neighbor overall.
        assert result['Count'] == 1 and result['Items'][0]['p'] == p_keep
        assert result['ScannedCount'] == 1

# Success-path test: KeyConditionExpression with a string inequality (<)
# pre-filter. String comparisons in the vector store are lexicographic.
# Also tests that when the constant is on the left-hand side of the
# comparison (e.g. ':threshold > p'), the operator is correctly reversed
# (treated as 'p < :threshold').
def test_query_vectorsearch_prefilter_string_inequality(vs, needs_vector_store):
    with new_test_table(vs,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
        # All 'z_'-prefixed keys are lexicographically > 'm';
        # the 'a_'-prefixed key is lexicographically < 'm'.
        p_drop = 'z_' + random_string()   # nearest to query, key > 'm'
        p_hi   = 'z_' + random_string()   # farther from query, key > 'm'
        p_lo   = 'a_' + random_string()   # farther from query, key < 'm'
        table.put_item(Item={'p': p_drop, 'v': Vector([0, 0, 1])})   # nearest to query
        table.put_item(Item={'p': p_hi,   'v': Vector([1, 0, 0])})   # farther, high key
        table.put_item(Item={'p': p_lo,   'v': Vector([0.9, 0, 0])}) # farther, low key
        table.update(VectorIndexUpdates=[{'Create':
            {'IndexName': 'vind',
             'VectorAttribute': {'AttributeName': 'v', 'Dimensions': 3}}}])
        wait_for_vector_index_active(table, 'vind')
        # Lexicographic inequality: p < 'm' keeps only p_lo ('a_...' < 'm').
        # p_drop ('z_...') is nearest overall but is excluded by the pre-filter.
        result = table.query(
            IndexName='vind',
            VectorSearch={'QueryVector': Vector([0, 0, 1])},
            Limit=1,
            KeyConditionExpression='p < :threshold',
            ExpressionAttributeValues={':threshold': 'm'},
        )
        assert result['Count'] == 1 and result['Items'][0]['p'] == p_lo
        assert result['ScannedCount'] == 1
        # Same inequality with the constant on the left-hand side: ':threshold > p'
        # is equivalent to 'p < :threshold' (the operator is reversed internally).
        result = table.query(
            IndexName='vind',
            VectorSearch={'QueryVector': Vector([0, 0, 1])},
            Limit=1,
            KeyConditionExpression=':threshold > p',
            ExpressionAttributeValues={':threshold': 'm'},
        )
        assert result['Count'] == 1 and result['Items'][0]['p'] == p_lo
        assert result['ScannedCount'] == 1

# Success-path test: KeyConditionExpression with a AND on two conditions on
# two projected attributes (the partition key and sort key). One of the
# conditions is a BETWEEN condition on a number attribute.
def test_query_vectorsearch_prefilter_and_number_between(vs, needs_vector_store):
    with new_test_table(vs,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'},
                       {'AttributeName': 'c', 'KeyType': 'RANGE'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'},
                                   {'AttributeName': 'c', 'AttributeType': 'N'}]) as table:
        p = random_string()
        # c=1 and c=2 are "keep" items (c in [1,2]); c=3 and c=4 are "drop".
        # "drop" items are nearest to the query vector.
        table.put_item(Item={'p': p, 'c': 3, 'v': Vector([0, 0, 1])})
        table.put_item(Item={'p': p, 'c': 4, 'v': Vector([0, 0.1, 1])})
        table.put_item(Item={'p': p, 'c': 1, 'v': Vector([1, 0, 0])})
        table.put_item(Item={'p': p, 'c': 2, 'v': Vector([0.9, 0, 0])})
        table.update(VectorIndexUpdates=[{'Create':
            {'IndexName': 'vind',
             'VectorAttribute': {'AttributeName': 'v', 'Dimensions': 3}}}])
        wait_for_vector_index_active(table, 'vind')
        result = table.query(
            IndexName='vind',
            VectorSearch={'QueryVector': Vector([0, 0, 1])},
            Limit=2,
            KeyConditionExpression='p = :p AND c BETWEEN :lo AND :hi',
            ExpressionAttributeValues={':p': p, ':lo': 1, ':hi': 2},
        )
        assert result['Count'] == 2 and result['ScannedCount'] == 2
        assert {item['c'] for item in result['Items']} == {Decimal(1), Decimal(2)}

# Test KeyConditionExpression pre-filter with a numeric sort key value that
# cannot be represented exactly as a 64-bit double.
# 2^53 + 1 = 9007199254740993 is the classic example: as a double it rounds to
# 9007199254740992 (= 2^53). If the pre-filter RHS were converted through double,
# the equality condition 'c = 9007199254740993' would silently become
# 'c = 9007199254740992', which matches the wrong item (c_drop) and the ANN
# search would return c_drop (the nearest vector) rather than c_keep.
# By keeping the value as a decimal string the comparison is exact.
def test_query_vectorsearch_prefilter_number_big_decimal(vs, needs_vector_store):
    with new_test_table(vs,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'},
                       {'AttributeName': 'c', 'KeyType': 'RANGE'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'},
                                   {'AttributeName': 'c', 'AttributeType': 'N'}]) as table:
        p = random_string()
        c_drop = Decimal('9007199254740992')  # 2^53, representable as double, nearest to query
        c_keep = Decimal('9007199254740993')  # 2^53+1, NOT representable as double (rounds to c_drop)
        table.put_item(Item={'p': p, 'c': c_drop, 'v': Vector([0, 0, 1])})  # nearest to query
        table.put_item(Item={'p': p, 'c': c_keep, 'v': Vector([1, 0, 0])})  # farther from query
        table.update(VectorIndexUpdates=[{'Create':
            {'IndexName': 'vind',
             'VectorAttribute': {'AttributeName': 'v', 'Dimensions': 3}}}])
        wait_for_vector_index_active(table, 'vind')
        # Pre-filter c = c_keep. If the value were converted through double it
        # would become c_drop's value (9007199254740992), and ANN would return
        # c_drop (the nearest item) instead of c_keep.
        result = table.query(
            IndexName='vind',
            VectorSearch={'QueryVector': Vector([0, 0, 1])},
            Limit=1,
            KeyConditionExpression='p = :p AND c = :c',
            ExpressionAttributeValues={':p': p, ':c': c_keep},
        )
        assert result['Count'] == 1 and result['ScannedCount'] == 1
        assert result['Items'][0]['c'] == c_keep

####################################### SELECT ##############################
# Test that a vector search Query returns, with Select='ALL_ATTRIBUTES', the
# full item content correctly (all attributes, correct key values) in the
# expected order - for multiple results. Two variants are tested via
# parametrize, to exercise two separate code paths in query_vector(), for
# tables with and without clustering keys:
# - no_ck: table with just a hash key
# - with_ck: table with a hash key and a range key
@pytest.mark.parametrize('have_ck', [False, True], ids=['no_ck', 'with_ck'])
def test_query_vector_full_items(vs, needs_vector_store, have_ck):
    key_schema = [{'AttributeName': 'p', 'KeyType': 'HASH'}]
    attr_defs = [{'AttributeName': 'p', 'AttributeType': 'S'}]
    if have_ck:
        key_schema.append({'AttributeName': 'c', 'KeyType': 'RANGE'})
        attr_defs.append({'AttributeName': 'c', 'AttributeType': 'S'})
    with new_test_table(vs,
            KeySchema=key_schema,
            AttributeDefinitions=attr_defs) as table:
        # Build 3 items, each with distinct key(s), a vector, and extra attributes.
        # A 4th item is inserted but should not appear with Limit=3.
        if have_ck:
            # deliberately use just two different p values, so some of the
            # returned items have the same p but different c, to exercise yet
            # another potentially different code path:
            p1 = random_string()
            p2 = random_string()
            ps = [p1, p1, p2, p2]
        else:
            ps = [random_string() for _ in range(4)]
        vectors = [
            [Decimal("1"),  Decimal("0"),   Decimal("0")],   # closest to query
            [Decimal("1"),  Decimal("0.1"), Decimal("0")],   # 2nd
            [Decimal("0"),  Decimal("1"),   Decimal("0")],   # 3rd
            [Decimal("-1"), Decimal("0"),   Decimal("0")],   # farthest, excluded
        ]
        items = []
        for i, (p, v) in enumerate(zip(ps, vectors)):
            item = {'p': p, 'v': v, 'x': f'attr_{i}', 'y': Decimal(str(i * 10))}
            if have_ck:
                item['c'] = random_string()
            items.append(item)
            table.put_item(Item=item)
        table.update(VectorIndexUpdates=[{'Create':
            {'IndexName': 'vind',
             'VectorAttribute': {'AttributeName': 'v', 'Dimensions': 3}}}])
        # The 3 nearest items in expected distance order (closest first).
        expected_items = items[:3]
        # Wait until the returned items match the expected list exactly,
        # verifying both the full content of each item and their order.
        deadline = time.monotonic() + VECTOR_STORE_TIMEOUT
        while True:
            try:
                result = table.query(
                    IndexName='vind',
                    VectorSearch={'QueryVector': [Decimal("1"), Decimal("0"), Decimal("0")]},
                    Limit=3,
                    Select='ALL_ATTRIBUTES'
                )
                if result.get('Items') == expected_items:
                    break
            except ClientError:
                pass
            if time.monotonic() > deadline:
                pytest.fail('Timed out waiting for vector store to return the expected items')
            time.sleep(0.1)

# Test support for "Select" parameter in vector search Query.
# We test all valid Select values and their effects on the returned items,
# as well as validation errors for invalid combinations.
# The first part tests validation errors (no vector store needed), and
# the second part tests correct results (needs vector store).
def test_query_vectorsearch_select_bad(table_vs):
    # Unknown Select value
    with pytest.raises(ClientError, match='ValidationException.*Select'):
        table_vs.query(IndexName='vind',
            VectorSearch={'QueryVector': [1, 2, 3]}, Limit=1,
            Select='GARBAGE')
    # Select=SPECIFIC_ATTRIBUTES without ProjectionExpression or AttributesToGet
    with pytest.raises(ClientError, match='ValidationException.*SPECIFIC_ATTRIBUTES'):
        table_vs.query(IndexName='vind',
            VectorSearch={'QueryVector': [1, 2, 3]}, Limit=1,
            Select='SPECIFIC_ATTRIBUTES')
    # ProjectionExpression with Select=ALL_ATTRIBUTES is not allowed
    with pytest.raises(ClientError, match='ValidationException.*SPECIFIC_ATTRIBUTES'):
        table_vs.query(IndexName='vind',
            VectorSearch={'QueryVector': [1, 2, 3]}, Limit=1,
            Select='ALL_ATTRIBUTES', ProjectionExpression='p')
    # ProjectionExpression with Select=COUNT is not allowed
    with pytest.raises(ClientError, match='ValidationException.*SPECIFIC_ATTRIBUTES'):
        table_vs.query(IndexName='vind',
            VectorSearch={'QueryVector': [1, 2, 3]}, Limit=1,
            Select='COUNT', ProjectionExpression='p')
    # ProjectionExpression with Select=ALL_PROJECTED_ATTRIBUTES is not allowed
    with pytest.raises(ClientError, match='ValidationException.*SPECIFIC_ATTRIBUTES'):
        table_vs.query(IndexName='vind',
            VectorSearch={'QueryVector': [1, 2, 3]}, Limit=1,
            Select='ALL_PROJECTED_ATTRIBUTES', ProjectionExpression='p')

# Test that a vector index created with Projection={'ProjectionType': 'KEYS_ONLY'}
# (via CreateTable or UpdateTable) works correctly:
# - The ProjectionType=KEYS_ONLY is accepted
# - Select=ALL_PROJECTED_ATTRIBUTES returns only the primary key attributes
# - Select=ALL_ATTRIBUTES returns all attributes
# ProjectionType=KEYS_ONLY matches the default vector index behavior, so it
# doesn't change results but must be accepted as a valid parameter.
@pytest.mark.parametrize('via_update', [False, True], ids=['createtable', 'updatetable'])
def test_vector_projection_keys_only(vs, needs_vector_store, via_update):
    if via_update:
        ctx = new_test_table(vs,
                KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
                AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}])
    else:
        ctx = new_test_table(vs,
                KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
                AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
                VectorIndexes=[{
                    'IndexName': 'vind',
                    'VectorAttribute': {'AttributeName': 'v', 'Dimensions': 3},
                    'Projection': {'ProjectionType': 'KEYS_ONLY'},
                }])
    with ctx as table:
        if via_update:
            table.update(VectorIndexUpdates=[{'Create': {
                'IndexName': 'vind',
                'VectorAttribute': {'AttributeName': 'v', 'Dimensions': 3},
                'Projection': {'ProjectionType': 'KEYS_ONLY'},
            }}])
        p = random_string()
        table.put_item(Item={'p': p, 'v': [1, 0, 0], 'x': 'hello'})
        wait_for_vector_index_active(table, 'vind')
        # Select=ALL_PROJECTED_ATTRIBUTES returns only the primary key.
        result = table.query(
            IndexName='vind',
            VectorSearch={'QueryVector': [1, 0, 0]},
            Limit=1,
            Select='ALL_PROJECTED_ATTRIBUTES')
        assert result['Items'] == [{'p': p}]
        # Select=ALL_ATTRIBUTES returns the full item.
        result = table.query(
            IndexName='vind',
            VectorSearch={'QueryVector': [1, 0, 0]},
            Limit=1,
            Select='ALL_ATTRIBUTES')
        assert result['Items'] == [{'p': p, 'v': [1, 0, 0], 'x': 'hello'}]

####################################### FilterExpression #####################
# Test FilterExpression for post-filtering vector search results: After Limit
# results are found by the vector index and the full items are retrieved
# from the base table, items which do not match the given FilterExpression are
# removed. This means that fewer than Limit results may be returned. This
# matches DynamoDB's general Query behavior where the filtering is applied after
# Limit.
# Two Select values are tested (via parametrize):
# ALL_ATTRIBUTES: the matching items are returned in the Items list.
# COUNT: no items are returned, but the implementation still needs to retrieve
#        full items (or at least the attributes needed by the filter) and
#        count how many among the Limit candidates matched the filter.
# ScannedCount (number of pre-filtering results) and Count (number of post-
# filtering results) are returned in both cases and checked.
@pytest.mark.parametrize('select', ['ALL_ATTRIBUTES', 'COUNT'])
def test_query_vectorsearch_filter_expression(vs, needs_vector_store, select):
    with new_test_table(vs,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
        # Insert all 5 items before the vector index is created so the vector
        # store picks them up via prefill scan (faster than CDC).
        # p_far is the furthest item and will not be among the 4 nearest
        # neighbors returned with Limit=4.
        p_keep1, p_keep2 = random_string(), random_string()
        p_drop1, p_drop2 = random_string(), random_string()
        p_far = random_string()
        table.put_item(Item={'p': p_keep1, 'v': [1, 0, 0],             'x': 'keep'})
        table.put_item(Item={'p': p_drop1, 'v': [1, Decimal("0.1"), 0], 'x': 'drop'})
        table.put_item(Item={'p': p_keep2, 'v': [1, Decimal("0.2"), 0], 'x': 'keep'})
        table.put_item(Item={'p': p_drop2, 'v': [1, Decimal("0.3"), 0], 'x': 'drop'})
        table.put_item(Item={'p': p_far,   'v': [1, Decimal("0.4"), 0], 'x': 'keep'})
        nearest_ps = {p_keep1, p_keep2, p_drop1, p_drop2} # 4 nearest neighbors
        keep_ps = {p_keep1, p_keep2} # x='keep' items among 4 nearest neighbors
        table.update(VectorIndexUpdates=[{'Create':
            {'IndexName': 'vind',
             'VectorAttribute': {'AttributeName': 'v', 'Dimensions': 3}}}])
        # Wait until nearest 4 items (nearest_ps) are visible in a query
        # without a filter.
        deadline = time.monotonic() + VECTOR_STORE_TIMEOUT
        while True:
            try:
                result = table.query(
                    IndexName='vind',
                    VectorSearch={'QueryVector': [1, 0, 0]},
                    Limit=4,
                )
                if {item['p'] for item in result.get('Items', [])} == nearest_ps:
                    break
            except ClientError:
                pass
            if time.monotonic() > deadline:
                pytest.fail('Timed out waiting for all items to be indexed')
            time.sleep(0.1)
        # Query with a FilterExpression that matches 2 of the 4 nearest
        # candidates (Limit=4). We expect Count=2 and ScannedCount=4. Note
        # that even though p_far also has x=keep, it was not among the 4
        # nearest neighbors - so it will not be included.
        result = table.query(
            IndexName='vind',
            VectorSearch={'QueryVector': [1, 0, 0]},
            Limit=4,
            Select=select,
            FilterExpression='x = :want',
            ExpressionAttributeValues={':want': 'keep'},
        )
        assert result['Count'] == 2
        assert result['ScannedCount'] == 4
        if select == 'COUNT':
            assert 'Items' not in result
        else:
            assert {item['p'] for item in result['Items']} == keep_ps

# Test FilterExpression for post-filtering vector search results with
# Select=SPECIFIC_ATTRIBUTES. Here the full items are not returned, but still
# need to be retrieved from the base table - including attributes which are
# needed by the filter but not returned in the final results.
def test_query_vectorsearch_filter_expression_specific_attributes(vs, needs_vector_store):
    with new_test_table(vs,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
        # Same 5-item setup as test_query_vectorsearch_filter_expression.
        # p_far is the furthest and won't be among the 4 nearest with Limit=4.
        p_keep1, p_keep2 = random_string(), random_string()
        p_drop1, p_drop2 = random_string(), random_string()
        p_far = random_string()
        table.put_item(Item={'p': p_keep1, 'v': [1, 0, 0],             'x': 'keep'})
        table.put_item(Item={'p': p_drop1, 'v': [1, Decimal("0.1"), 0], 'x': 'drop'})
        table.put_item(Item={'p': p_keep2, 'v': [1, Decimal("0.2"), 0], 'x': 'keep'})
        table.put_item(Item={'p': p_drop2, 'v': [1, Decimal("0.3"), 0], 'x': 'drop'})
        table.put_item(Item={'p': p_far,   'v': [1, Decimal("0.4"), 0], 'x': 'keep'})
        nearest_ps = {p_keep1, p_keep2, p_drop1, p_drop2}
        table.update(VectorIndexUpdates=[{'Create':
            {'IndexName': 'vind',
             'VectorAttribute': {'AttributeName': 'v', 'Dimensions': 3}}}])
        # Wait until the 4 nearest items are visible without a filter.
        deadline = time.monotonic() + VECTOR_STORE_TIMEOUT
        while True:
            try:
                result = table.query(
                    IndexName='vind',
                    VectorSearch={'QueryVector': [1, 0, 0]},
                    Limit=4,
                )
                if {item['p'] for item in result.get('Items', [])} == nearest_ps:
                    break
            except ClientError:
                pass
            if time.monotonic() > deadline:
                pytest.fail('Timed out waiting for all items to be indexed')
            time.sleep(0.1)
        # Query with Select=SPECIFIC_ATTRIBUTES projecting only 'p', but
        # FilterExpression uses 'x' which is NOT in the projection. The
        # implementation must still retrieve 'x' from the base table to
        # evaluate the filter, even though 'x' is not returned to the caller.
        result = table.query(
            IndexName='vind',
            VectorSearch={'QueryVector': [1, 0, 0]},
            Limit=4,
            Select='SPECIFIC_ATTRIBUTES',
            ProjectionExpression='p',
            FilterExpression='x = :want',
            ExpressionAttributeValues={':want': 'keep'},
        )
        assert result['Count'] == 2
        assert result['ScannedCount'] == 4
        # Items should contain only 'p' (the projected attribute), not 'x'
        # (the filter attribute that was not projected).
        assert result['Items'] == [{'p': p_keep1}, {'p': p_keep2}]

# Test FilterExpression with Select=SPECIFIC_ATTRIBUTES and a nested
# ProjectionExpression (e.g. 'x.a'). Only the requested sub-attribute
# should be returned, not the entire top-level attribute.
def test_query_vectorsearch_filter_expression_nested_projection(vs, needs_vector_store):
    with new_test_table(vs,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
        p = random_string()
        # Item has a nested attribute 'x' with sub-attributes 'a' and 'b'.
        # The FilterExpression uses 'y', which is not in the projection.
        table.put_item(Item={'p': p, 'v': [1, 0, 0], 'x': {'a': 'keep', 'b': 'drop'}, 'y': 'pass'})
        table.update(VectorIndexUpdates=[{'Create':
            {'IndexName': 'vind',
             'VectorAttribute': {'AttributeName': 'v', 'Dimensions': 3}}}])
        wait_for_vector_index_active(table, 'vind')
        # ProjectionExpression requests only the nested attribute 'x.a' (and 'p').
        # FilterExpression uses 'y', which is not in the projection at all.
        # The result should contain only 'p' and 'x': {'a': 'keep'} - the
        # 'b' sub-attribute of 'x' must not appear, and 'y' must not appear.
        result = table.query(
            IndexName='vind',
            VectorSearch={'QueryVector': [1, 0, 0]},
            Limit=1,
            Select='SPECIFIC_ATTRIBUTES',
            ProjectionExpression='p, x.a',
            FilterExpression='y = :want',
            ExpressionAttributeValues={':want': 'pass'},
        )
        assert result['Count'] == 1
        assert result['ScannedCount'] == 1
        assert result['Items'] == [{'p': p, 'x': {'a': 'keep'}}]



################################################################################
# Gaps found by AI comparing the tests above (excluding the Alternator-
# extension tests below the "Checks for Alternator extensions" divider, which
# use the "vs" fixture and don't reflect real DynamoDB) against the official
# DynamoDB vector-search documentation. None of these are implemented yet -
# this is a list of things to write tests for.
#
# CreateTable / UpdateTable level:
# - "A CreateTable request can define multiple vector indexes at once, up to
#   the per-table limit of five. Exceeding that limit fails with
#   ValidationException: One or more parameter values were invalid: VectorIndex
#   count exceeds the per-table limit of 5." test_createtable_vectorindexes_multiple
#   only ever creates 2 vector indexes - neither the "5 succeeds" nor the
#   "6 fails" boundary is tested.
# - Creating/deleting a vector index while another vector-index (or GSI/LSI)
#   build is still in progress on the *same table* should fail with
#   LimitExceededException ("Subscriber limit exceeded: Only 1 online index can
#   be created or deleted simultaneously per table") - and this limit is
#   explicitly documented as *shared* between vector indexes and GSIs. We only
#   test that a single UpdateTable call can't request two Creates/Deletes at
#   once (test_updatetable_vectorindex_just_one_update); we never test two
#   *separate* UpdateTable calls where the second one is issued before the
#   first index has finished backfilling.
# - DeleteTable while a vector index is still CREATING/backfilling should fail
#   with ResourceInUseException ("Cannot delete table while indexes are being
#   created, updated, or deleted."). Every test that creates a vector index
#   carefully works around this (by deleting the index first, or waiting for
#   it to become ACTIVE) - the rejection itself is never directly tested.

#
# SearchSchema / inline filters:
# - "Number of inline filters per vector index: 18" (a documented, non-
#   adjustable quota). We test that *some* number of INLINE_FILTER elements is
#   accepted, and that at most one HASH is allowed, but never test the 18/19
#   boundary for INLINE_FILTER count.
# - Writing an empty string into the vector index's HASH (partition key)
#   attribute should be rejected ("cannot contain an empty string value").
#   test_vectorindexes_searchschema_type_verification tests a wrong *type* for
#   HASH/INLINE_FILTER attributes, but not an empty string of the *correct*
#   type.
#
# DescribeTable / index lifecycle:
#
# Cross-feature interactions that are documented but likely impractical to
# test in this kind of test file (would need a second region, a huge base
# table, DAX, or S3 import/export infrastructure) - listed here mainly so we
# don't forget they exist and are untested:
# - Global tables: replication of the vector index definition to new replica
#   Regions, independent per-Region backfilling, and cross-Region eventual
#   consistency of SearchVectors results (even under MRSC).
# - Point-in-time recovery / on-demand backup and restore: the vector index is
#   rebuilt (not copied byte-for-byte) from the restored base table data, and
#   goes through backfilling again.
# - Table export to S3 (includes the raw vector attribute) and import from S3
#   (vector index is populated as items are written during the import).
# - DAX does not support SearchVectors at all.
# - The base-table-size allowlisting threshold (600 GB) for creating a new
#   vector index on an existing table.
# - We never test that PartiQL (ExecuteStatement) can't be used to search a
#   vector index ("Vector indexes are not accessible through PartiQL").
################################################################################
