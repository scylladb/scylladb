# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# Tests for the vector search feature. This is an Alternator extension
# that does not exist on DynamoDB, so most tests in this file are skipped
# when running against DynamoDB (a few tests that don't use the "vs" fixture
# can still run on DynamoDB).

import pytest
import time
import decimal
from decimal import Decimal
from contextlib import contextmanager
from functools import cache

from botocore.exceptions import ClientError
import boto3.dynamodb.types

from .util import random_string, new_test_table, unique_table_name, scylla_config_read, scylla_config_write, client_no_transform, is_aws
from test.pylib.skip_types import skip_env

# Monkey-patch the boto3 library to stop doing its own error-checking on
# numbers. This works around a bug https://github.com/boto/boto3/issues/2500
# of incorrect checking of responses, and we also need to get boto3 to not do
# its own error checking of requests, to allow us to check the server's
# handling of such errors.
# This is needed at least for test_numeric_list_precision_range().
boto3.dynamodb.types.DYNAMODB_CONTEXT = decimal.Context(prec=100)

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
@pytest.fixture(scope="module")
def vs(new_dynamodb_session, dynamodb):
    if is_aws(dynamodb):
        skip_env('Scylla-only: vector search extensions not available on DynamoDB')
    resource = new_dynamodb_session()
    client = resource.meta.client
    # Patch the client to support the new APIs:
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
            },
            'required': ['QueryVector'],
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

    # Add a VectorIndexes field to "TableDescription", the shape returned
    # by DescribeTable and also CreateTable
    output_shape = shape_resolver.get_shape_by_name('TableDescription')
    output_shape._shape_model['members']['VectorIndexes'] = {
        'shape': 'VectorIndexes'
    }
    output_shape._cache.pop('members', None)
    shape_resolver._shape_cache.pop('TableDescription', None)

    yield resource

# A simple test for the vector type. In vector search, a vector is simply
# an array of known size that contains only numbers. In the DynamoDB API,
# there is no special "vector" type, it's just an regular "list" type,
# and the indexing code may later require that it contain only numbers or
# have a specific length. When this test was written, this vector is stored
# inefficiently as a JSON string with ASCII representation of numbers, but
# in the future, we may decide to recognize such numeric-only lists and
# store them on disk in an optimized way - and still this test will need
# to continue passing.
def test_vector_value(dynamodb, test_table_s):
    p = random_string()
    v = [Decimal("0"), Decimal("1.2"), Decimal("-2.3"), Decimal("1.2e10")]
    test_table_s.put_item(Item={'p': p, 'v': v})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['v'] == v

# Even if we will have an optimized storage format for a numeric-only list,
# we will still need to use some form of "decimal" type (variable precision
# based on decimal digits) to support DynamoDB's full numeric precision and
# range (38 decimal digits, exponent up to 125) even in a list. This test
# confirms that Alternator indeed allows that full precision and range
# (which is different from any hardware floating-point type) inside lists.
# See similar tests but for a single number in test_number.py.
def test_numeric_list_precision_range(test_table_s):
    p = random_string()
    v = [Decimal("3.1415926535897932384626433832795028841"),
         Decimal("9.99999999e125")]
    test_table_s.put_item(Item={'p': p, 'v': v})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['v'] == v

# Test CreateTable creating a new table with a basic vector index. This test
# doesn't check that the vector index actually works - we'll do this in
# separate tests below. It just tests that the new Alternator-only
# CreateTable parameter "VectorIndexes" isn't rejected or otherwise fails.
# This test also doesn't cover all the different parameters inside
# VectorIndexes.
def test_createtable_vectorindexes(vs):
    with new_test_table(vs,
            KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
            AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }],
            VectorIndexes=[
                {   'IndexName': 'hello',
                    'VectorAttribute': {'AttributeName': 'v', 'Dimensions': 4}
                }]) as table:
        pass

# Test that in CreateTable's VectorIndexes, a IndexName and VectorAttribute
# is required. Inside the VectorAttribute, a AttributeName and Dimensions
# are required. With any of those fields missing we get a ValidationException.
def test_createtable_vectorindexes_missing_fields(vs):
    # Note: in new_dynamodb_session in conftest.py, we used
    # parameter_validation=False by default, so boto3 doesn't do the
    # validation of missing parameters for us, which is good, because
    # it allows us to send requests with missing fields and see the server
    # catch that error.
    for bad in bad_vector_indexes:
        with pytest.raises(ClientError, match='ValidationException'):
            with new_test_table(vs,
                KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
                AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }],
                VectorIndexes=[bad]) as table:
                pass

bad_vector_indexes = [
    # everything missing:
    {},
    # VectorAttribute missing:
    {'IndexName': 'hello'},
    # IndexName missing:
    {'VectorAttribute': {'AttributeName': 'v', 'Dimensions': 4}},
    # VectorAttribute missing parts:
    {'IndexName': 'hello', 'VectorAttribute': {'Dimensions': 4}},
    {'IndexName': 'hello', 'VectorAttribute': {'AttributeName': 'v'}},
]

# Check that we are not allowed to create two VectorIndexes with the same
# name.
def test_createtable_vectorindexes_same_name(vs):
    with pytest.raises(ClientError, match='ValidationException.*Duplicate.*hello'):
        with new_test_table(vs,
            KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
            AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }],
            VectorIndexes=[
                {   'IndexName': 'hello',
                    'VectorAttribute': {'AttributeName': 'v', 'Dimensions': 4}
                },
                {   'IndexName': 'hello',
                    'VectorAttribute': {'AttributeName': 'x', 'Dimensions': 7}
                }
            ]) as table:
            pass

# Check that we are not allowed to a VectorIndexes with the same name as
# the name of another type of index - GSI or an LSI.
def test_createtable_vectorindexes_same_name_gsi(vs):
    with pytest.raises(ClientError, match='ValidationException.*Duplicate.*hello'):
        with new_test_table(vs,
            KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
            AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }],
            GlobalSecondaryIndexes=[
                {   'IndexName': 'hello',
                    'KeySchema': [{ 'AttributeName': 'p', 'KeyType': 'HASH' }],
                    'Projection': { 'ProjectionType': 'ALL' }
                }],
            VectorIndexes=[
                {   'IndexName': 'hello',
                    'VectorAttribute': {'AttributeName': 'x', 'Dimensions': 7}
                }]
            ) as table:
            pass

def test_createtable_vectorindexes_same_name_lsi(vs):
    with pytest.raises(ClientError, match='ValidationException.*Duplicate.*hello'):
        with new_test_table(vs,
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
                    'VectorAttribute': {'AttributeName': 'x', 'Dimensions': 7}
                }]
            ) as table:
            pass

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

# Verify that a vector index's IndexName follows the same naming rules as
# table names - name length from 3 up to 192 (max_table_name_length) and
# match the regex [a-zA-Z0-9._-]+.
# Note that these rules are similar, but not identical, to the rules for
# IndexName for GSI/LSI (tested in test_gsi.py and test_lsi.py) - there,
# Alternator doesn't put the limit on the length of the GSI/LSI's IndexName,
# but puts a limit (222) on the sum of the table's name and GSI/LSI's name.
def test_createtable_vectorindexes_indexname_rules(vs):
    # Forbidden names: shorter than 3 characters, longer than 192
    # characters, or containing characters outside [a-zA-Z0-9._-].
    # These names should be rejected
    for bad_name in ['xy', 'x'*193, 'hello$world', 'hello world']:
        with pytest.raises(ClientError, match='ValidationException.*IndexName'):
            with new_test_table(vs,
                KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
                AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }],
                VectorIndexes=[
                    {   'IndexName': bad_name,
                        'VectorAttribute': {'AttributeName': 'v', 'Dimensions': 74 }
                }]
            ) as table:
                pass
    # Allowed names: exactly 3 characters, 192 characters, and using
    # all characters from [a-zA-Z0-9._-].
    # This test is slightly slower than usual, because three tables and
    # indexes will be successfully created and then immediately deleted.
    for good_name in ['xyz', 'x'*192,
                      'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-']:
        with new_test_table(vs,
            KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
            AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }],
            VectorIndexes=[
                {   'IndexName': good_name,
                    'VectorAttribute': {'AttributeName': 'v', 'Dimensions': 74 }
            }]
        ) as table:
            pass

# Check that the "Dimensions" property in CreateTable's VectorIndexes's
# VectorAttribute must be an integer between 1 and 16000 (MAX_VECTOR_DIMENSION
# in the code).
MAX_VECTOR_DIMENSION = 16000
def test_createtable_vectorindexes_dimensions_rules(vs):
    # Forbidden dimensions: non-integer, negative, zero, and above
    # MAX_VECTOR_DIMENSION. These dimensions should be rejected.
    for bad_dimensions in ['hello', 1.2, -17, 0, MAX_VECTOR_DIMENSION+1]:
        with pytest.raises(ClientError, match='ValidationException.*Dimensions'):
            with new_test_table(vs,
                KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
                AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }],
                VectorIndexes=[
                    {   'IndexName': 'vector_index',
                        'VectorAttribute': {'AttributeName': 'v', 'Dimensions': bad_dimensions }
                }]
            ) as table:
                pass
    # Allowed dimensions: 1, MAX_VECTOR_DIMENSION:
    for good_dimensions in [1, MAX_VECTOR_DIMENSION]:
        with new_test_table(vs,
            KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
            AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }],
            VectorIndexes=[
                {   'IndexName': 'vector_index',
                    'VectorAttribute': {'AttributeName': 'v', 'Dimensions': good_dimensions }
            }]
        ) as table:
            pass

# Check that the "AttributeName" property in CreateTable's VectorIndexes's
# VectorAttribute must not be a key column (of the base table or any of its
# GSIs or LSIs). This is because key columns have a declared type, which
# can't be a vector (a list), so making such a column the key of a vector
# index makes no sense.
def test_createtable_vectorindexes_attributename_key(vs):
    # Forbidden AttributeName: base-table keys (hash and range), GSI keys,
    # LSI keys:
    for bad_attr in ['p', 'c', 'x', 'y', 'z']:
        with pytest.raises(ClientError, match='ValidationException.*AttributeName'):
            with new_test_table(vs,
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
                        'VectorAttribute': {'AttributeName': bad_attr, 'Dimensions': 42 }
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
def test_createtable_vectorindexes_attributename_len(vs):
    # Forbidden AttributeName: empty string, string over 65535
    for bad_attr in ['', 'x'*65536]:
        with pytest.raises(ClientError, match='ValidationException.*AttributeName'):
            with new_test_table(vs,
                KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
                AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }],
                VectorIndexes=[
                    {   'IndexName': 'vector_index',
                        'VectorAttribute': {'AttributeName': bad_attr, 'Dimensions': 42 }
                    }]
            ) as table:
                pass

# Test that we can add two different vector indexes on the same table
# in CreateTable, but they must be on different attributes.
def test_createtable_vectorindexes_multiple(vs):
    # Can create two vector indexes on two different attributes:
    with new_test_table(vs,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
        AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }],
        VectorIndexes=[
            {   'IndexName': 'ind1',
                'VectorAttribute': {'AttributeName': 'x', 'Dimensions': 42 }
            },
            {   'IndexName': 'ind2',
                'VectorAttribute': {'AttributeName': 'y', 'Dimensions': 17 }
            },
        ]) as table:
        pass
    # But can't create two vector indexes on the same attribute.
    # Why don't we allow that? If the two indexes were to request different
    # "dimensions", Alternator would not know which vector length to enforce
    # when inserting values. But for simplicity (and avoiding wasted space
    # and work) we decided not to allow two indexes on the same attribute,
    # in any case.
    with pytest.raises(ClientError, match='ValidationException.*Duplicate'):
        with new_test_table(vs,
            KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
            AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }],
            VectorIndexes=[
                {   'IndexName': 'ind1',
                    'VectorAttribute': {'AttributeName': 'x', 'Dimensions': 42 }
                },
                {   'IndexName': 'ind2',
                    'VectorAttribute': {'AttributeName': 'x', 'Dimensions': 42 }
                },
            ]) as table:
            pass

# Test that vector indexes are correctly listed in DescribeTable:
def test_describetable_vectorindexes(vs):
    with new_test_table(vs,
            KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
            AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }],
            VectorIndexes=[
                {   'IndexName': 'ind1',
                    'VectorAttribute': {'AttributeName': 'x', 'Dimensions': 42 }
                },
                {   'IndexName': 'ind2',
                    'VectorAttribute': {'AttributeName': 'y', 'Dimensions': 17 }
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
                assert vec['VectorAttribute'] == {'AttributeName': 'x', 'Dimensions': 42}
            else: # vec['IndexName'] == 'ind2':
                assert vec['VectorAttribute'] == {'AttributeName': 'y', 'Dimensions': 17}
            assert vec['Projection'] == {'ProjectionType': 'KEYS_ONLY'}

# Test that like DescribeTable, CreateTable also returns the VectorIndexes
# definition its response
def test_createtable_vectorindexes_returned(vs):
    # To look at the response of CreateTable, we need to use the "client"
    # interface, not the usual higher-level "resource" interface that we
    # usually use in tests - because that doesn't return the actual response.
    client = vs.meta.client
    table_name = unique_table_name()
    resp = client.create_table(
        TableName=table_name,
        BillingMode='PAY_PER_REQUEST',
        KeySchema=[{ 'AttributeName': 'p', 'KeyType': 'HASH' }],
        AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }],
        VectorIndexes=[{
            'IndexName': 'ind',
            'VectorAttribute': {'AttributeName': 'x', 'Dimensions': 42 },
        }])
    try:
        assert 'TableDescription' in resp
        assert 'VectorIndexes' in resp['TableDescription']
        vector_indexes = resp['TableDescription']['VectorIndexes']
        assert len(vector_indexes) == 1
        vec = vector_indexes[0]
        assert vec['IndexName'] == 'ind'
        assert vec['VectorAttribute'] == {'AttributeName': 'x', 'Dimensions': 42}
        # Note that today, CreateTable just echoes back the parameters it got
        # doesn't add default parameters, so we don't expect to see, for
        # example, a "Projection" field in the response because we didn't send
        # one. We may change this decision in the future.
        #assert vec['Projection'] == {'ProjectionType': 'KEYS_ONLY'}
    finally:
        # In principle, we need to wait for the table to become ACTIVE before
        # deleting it. But this test only runs on Alternator, where
        # CreateTable is synchronous anyway, so we don't bother to add a
        # waiting loop.
        client.delete_table(TableName=table_name)

# Basic test for UpdateTable successfully adding a vector index
def test_updatetable_vectorindex_create(vs):
    with new_test_table(vs,
            KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
            AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }]) as table:
        # There are no vector indexes yet:
        desc = table.meta.client.describe_table(TableName=table.name)
        assert 'Table' in desc
        assert 'VectorIndexes' not in desc['Table']
        # Add a vector index with UpdateTable
        table.update(VectorIndexUpdates=[{'Create':
            { 'IndexName': 'hello',
              'VectorAttribute': {'AttributeName': 'x', 'Dimensions': 17 }
            }}])
        # Now describe_table should see the new vector index:
        desc = table.meta.client.describe_table(TableName=table.name)
        assert 'Table' in desc
        assert 'VectorIndexes' in desc['Table']
        vector_indexes = desc['Table']['VectorIndexes']
        assert len(vector_indexes) == 1
        vec = vector_indexes[0]
        assert vec['IndexName'] == 'hello'
        assert vec['VectorAttribute'] == {'AttributeName': 'x', 'Dimensions': 17}

# Basic test for UpdateTable successfully removing a vector index
def test_updatetable_vectorindex_delete(vs):
    with new_test_table(vs,
            KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
            AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }],
            VectorIndexes=[{
                'IndexName': 'hello',
                'VectorAttribute': {'AttributeName': 'x', 'Dimensions': 42 }
            }]) as table:
        # There should be one vector index now:
        desc = table.meta.client.describe_table(TableName=table.name)
        assert 'Table' in desc
        assert 'VectorIndexes' in desc['Table']
        assert len(desc['Table']['VectorIndexes']) == 1
        # Delete the vector index with UpdateTable
        table.update(VectorIndexUpdates=[
            {'Delete': { 'IndexName': 'hello' }}])
        # Now describe_table should see no vector index:
        desc = table.meta.client.describe_table(TableName=table.name)
        assert 'Table' in desc
        assert 'VectorIndexes' not in desc['Table']

# UpdateTable can't remove a vector index that doesn't exist. We get a
# ResourceNotFoundException.
def test_updatetable_vectorindex_delete_nonexistent(vs):
    with pytest.raises(ClientError, match='ResourceNotFoundException'):
        with new_test_table(vs,
                KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
                AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }]) as table:
            table.update(VectorIndexUpdates=[
                {'Delete': { 'IndexName': 'nonexistent' }}])

# Test that in UpdateTable's Create operation, a IndexName and VectorAttribute
# are required. Inside the VectorAttribute, a AttributeName and Dimensions
# are required. With any of those fields missing we get a ValidationException.
def test_updatetable_vectorindex_missing_fields(vs):
    with new_test_table(vs,
            KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
            AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }]) as table:
        # Note: in new_dynamodb_session in conftest.py, we used
        # parameter_validation=False by default, so boto3 doesn't do the
        # validation of missing parameters for us, which is good, because
        # it allows us to send requests with missing fields and see the server
        # catch that error.
        for bad in bad_vector_indexes:
            with pytest.raises(ClientError, match='ValidationException.*VectorIndexUpdates'):
                table.update(VectorIndexUpdates=[{'Create': bad}])

# Test that when adding a vector index with UpdateTable,
# 1. Its name cannot be the same as an existing vector index or GSI or LSI
# 2. Its attribute cannot be a key column (of base, GSI or LSI) or the
#    attribute on an existing vector index
def test_updatetable_vectorindex_taken_name_or_attribute(vs):
    # We create a table with vector index, GSI and LSI, so we can check
    # all the desired cases on a single table.
    with new_test_table(vs,
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
            { 'IndexName': 'vec',
              'VectorAttribute': {'AttributeName': 'v', 'Dimensions': 13 }}],
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
                    { 'IndexName': bad_name,
                      'VectorAttribute': {'AttributeName': 'xyz', 'Dimensions': 17 }
                    }}])
        # AttributeName already in use:
        for bad_attr in ['p', 'c', 'x', 'y', 'z', 'v']:
            with pytest.raises(ClientError, match='ValidationException.*AttributeName'):
                table.update(VectorIndexUpdates=[{'Create':
                    { 'IndexName': 'newind',
                      'VectorAttribute': {'AttributeName': bad_attr, 'Dimensions': 17 }
                    }}])

# In test_updatetable_vectorindex_taken_name_or_attribute() above we tested
# that we can't add a vector index with the same name as an existing GSI or
# LSI. Here we check that the reverse also holds - we can't add a GSI with
# the same name as an existing vector index.
def test_updatetable_gsi_same_name_as_vector_index(vs):
    with new_test_table(vs,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
            VectorIndexes=[
                {'IndexName': 'vec',
                 'VectorAttribute': {'AttributeName': 'v', 'Dimensions': 3}}
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
def test_updatetable_gsi_key_is_vector_attribute(vs):
    with new_test_table(vs,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
            VectorIndexes=[
                {'IndexName': 'vec',
                 'VectorAttribute': {'AttributeName': 'v', 'Dimensions': 3}}
            ]) as table:
        # The attribute 'v' is already a vector index target - it cannot
        # become the hash key of a new GSI.
        with pytest.raises(ClientError, match='ValidationException.*AttributeDefinitions'):
            table.meta.client.update_table(
                TableName=table.name,
                AttributeDefinitions=[{'AttributeName': 'v', 'AttributeType': 'S'}],
                GlobalSecondaryIndexUpdates=[{'Create': {
                    'IndexName': 'gsi',
                    'KeySchema': [{'AttributeName': 'v', 'KeyType': 'HASH'}],
                    'Projection': {'ProjectionType': 'ALL'}
                }}])

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

# Similar to an above test for the CreateTable case, verify that also for
# UpdateTable create a new vector index, a vector index's IndexName must
# have length from 3 up to 192 (max_table_name_length) and match the regex
# [a-zA-Z0-9._-]+.
def test_updatetable_vectorindex_indexname_bad(vs):
    with new_test_table(vs,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
        AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }]) as table:
        # Forbidden names: shorter than 3 characters, longer than 192
        # characters, or containing characters outside [a-zA-Z0-9._-].
        # These names should be rejected
        for bad_name in ['xy', 'x'*193, 'hello$world', 'hello world']:
            with pytest.raises(ClientError, match='ValidationException.*IndexName'):
                table.update(VectorIndexUpdates=[{'Create':
                    { 'IndexName': bad_name,
                      'VectorAttribute': {'AttributeName': 'x', 'Dimensions': 17 }
                    }}])

# Similar to an above test for the CreateTable case, verify that also for
# UpdateTable create a new vector index, a vector index's Dimensions must
# be an integer between 1 and MAX_VECTOR_DIMENSION
def test_updatetable_vectorindex_dimensions_bad(vs):
    with new_test_table(vs,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
        AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }]) as table:
        for bad_dimensions in ['hello', 1.2, -17, 0, MAX_VECTOR_DIMENSION+1]:
            with pytest.raises(ClientError, match='ValidationException.*Dimensions'):
                table.update(VectorIndexUpdates=[{'Create':
                    { 'IndexName': 'ind',
                      'VectorAttribute': {'AttributeName': 'x', 'Dimensions': bad_dimensions }
                    }}])

# Similar to an above test for the CreateTable case, verify that also for
# UpdateTable create a new vector index, a vector index's attribute must
# have between 1 and 65535 bytes.
# Note that we also checked above that it can't be one of the existing keys
# (of base table, GSI or LSI), or an already indexed vector column. Here we
# only test the allowed length limits.
def test_updatetable_vectorindex_attributename_bad_len(vs):
    with new_test_table(vs,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
        AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }]) as table:
        for bad_attr in ['', 'x'*65536]:
            with pytest.raises(ClientError, match='ValidationException.*AttributeName'):
                table.update(VectorIndexUpdates=[{'Create':
                    { 'IndexName': 'ind',
                      'VectorAttribute': {'AttributeName': bad_attr, 'Dimensions': 17 }
                    }}])

# DynamoDB currently limits UpdateTable to only one GSI operation (Create
# or Delete), so we placed the same limit on VectorIndexUpdates - even though
# it's an array, it must have exactly one element. Let's validate this
# limitation is enforced - but if one day we decide to lift it, we can
# and delete this test.
def test_updatetable_vectorindex_just_one_update(vs):
    with new_test_table(vs,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
        AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }]) as table:
        # Zero operations aren't allowed - it's treated just like a missing
        # VectorIndexUpdates, and therefore a do-nothing UpdateTable which
        # is not allowed.
        with pytest.raises(ClientError, match='ValidationException.*requires one'):
            table.update(VectorIndexUpdates=[])
        # Two "Create" aren't allowed.
        # Again following DynamoDB's lead on GSI, interestingly in this case
        # the error is LimitExceededException, not ValidationException.
        with pytest.raises(ClientError, match='LimitExceededException.*allows one'):
            table.update(VectorIndexUpdates=[
                {'Create': {'IndexName': 'ind1', 'VectorAttribute': {'AttributeName': 'x', 'Dimensions': 17 }}},
                {'Create': {'IndexName': 'ind2', 'VectorAttribute': {'AttributeName': 'y', 'Dimensions': 17 }}}])
        # Two "Delete" aren't allowed (they are rejected even before noticing
        # that the indexes we ask to delete don't exist).
        with pytest.raises(ClientError, match='LimitExceededException.*allows one'):
            table.update(VectorIndexUpdates=[
                {'Delete': {'IndexName': 'ind1'}},
                {'Delete': {'IndexName': 'ind2'}}])
        # Also one "Delete" and one "Create" isn't allowed
        with pytest.raises(ClientError, match='LimitExceededException.*allows one'):
            table.update(VectorIndexUpdates=[
                {'Create': {'IndexName': 'ind1', 'VectorAttribute': {'AttributeName': 'x', 'Dimensions': 17 }}},
                {'Delete': {'IndexName': 'ind2'}}])

# Also, it's not allowed to have in one UpdateTable request both a
# VectorIndexUpdates and a GlobalSecondaryIndexUpdates. There is no real
# reason why we can't support this, but since we already don't allow adding
# (or deleting) more than one GSI or more than one vector index in the same
# operation, it makes sense to disallow having both. If one day we decide to
# allow both in the same request, we can delete this test.
def test_updatetable_vector_and_gsi_same_request(vs):
    with new_test_table(vs,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
        with pytest.raises(ClientError, match='LimitExceededException'):
            table.meta.client.update_table(
                TableName=table.name,
                AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
                VectorIndexUpdates=[{'Create': {
                    'IndexName': 'vec',
                    'VectorAttribute': {'AttributeName': 'v', 'Dimensions': 3}
                }}],
                GlobalSecondaryIndexUpdates=[{'Create': {
                    'IndexName': 'gsi',
                    'KeySchema': [{'AttributeName': 'p', 'KeyType': 'HASH'}],
                    'Projection': {'ProjectionType': 'ALL'}
                }}])

# Test that PutItem still works as expected on a table with a vector index
# created by CreateTable or UpdateTable. It might not work if we set up CDC
# in a broken way that breaks writes.
def test_putitem_vectorindex_createtable(vs):
    with new_test_table(vs,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
        AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }],
        VectorIndexes=[
            { 'IndexName': 'vec',
              'VectorAttribute': {'AttributeName': 'v', 'Dimensions': 3 }}]
        ) as table:
        p = random_string()
        item = {'p': p, 'v': [1,2,3]}
        table.put_item(Item=item)
        assert item == table.get_item(Key={'p': p}, ConsistentRead=True)['Item']

def test_putitem_vectorindex_updatetable(vs):
    # Create the table without a vector index, and add it later:
    with new_test_table(vs,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
        AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }]) as table:
        table.update(VectorIndexUpdates=[
            {'Create': {'IndexName': 'ind', 'VectorAttribute': {'AttributeName': 'v', 'Dimensions': 3 }}}])
        # In general we may need to wait here until the vector index is
        # ACTIVE, but currently in Alternator we don't need to wait.
        p = random_string()
        item = {'p': p, 'v': [1,2,3]}
        table.put_item(Item=item)
        assert item == table.get_item(Key={'p': p}, ConsistentRead=True)['Item']

# Simple test table with a vector index on a 3-dimensional vector column v
# Please note that because this is a shared table, tests that perform
# global queries on it, not filtering to a specific partition, may get
# results from other tests - so such tests will need to create their own
# table instead of using this shared one.
@pytest.fixture(scope="module")
def table_vs(vs):
    with new_test_table(vs,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
            VectorIndexes=[
                {'IndexName': 'vind',
                 'VectorAttribute': {'AttributeName': 'v', 'Dimensions': 3}}
            ]) as table:
        yield table

# Test that a Query with a VectorSearch parameter without an IndexName
# is rejected with a ValidationException.
def test_query_vectorsearch_missing_indexname(table_vs):
    with pytest.raises(ClientError, match='ValidationException.*IndexName'):
        table_vs.query(VectorSearch={'QueryVector': [1, 2, 3]})

# Test that a Query with a VectorSearch parameter with an IndexName
# which does not refer to a valid vector index is rejected with a
# ValidationException. Note that it doesn't really matter if IndexName
# refers to a garbage name or to a real GSI/LSI - the code just checks
# if it's a known vector index name.
def test_query_vectorsearch_wrong_indexname(table_vs):
    with pytest.raises(ClientError, match='ValidationException.*is not a vector index'):
        table_vs.query(IndexName='nonexistent',
                       VectorSearch={'QueryVector': [1, 2, 3]})

# Test that a Query on a vector index without a VectorSearch parameter is
# rejected. When VectorSearch isn't specified, the code expects a base table,
# LSI or GSI - which it won't find. But rather than reporting unhelpfully
# that an index by that name doesn't exist, we want to report that this index
# does exist - and is a vector index - so VectorSearch must be specified.
def test_query_vectorindex_no_vectorsearch(table_vs):
    with pytest.raises(ClientError, match='ValidationException.*VectorSearch'):
        table_vs.query(
            IndexName='vind',
            KeyConditionExpression='p = :p',
            ExpressionAttributeValues={':p': 'x'},
        )

# Test that a Query with a VectorSearch parameter that is missing the
# required QueryVector field is rejected with a ValidationException.
def test_query_vectorsearch_missing_queryvector(table_vs):
    with pytest.raises(ClientError, match='ValidationException.*QueryVector'):
        table_vs.query(
            IndexName='vind',
            VectorSearch={},
        )

# Test that QueryVector must be a list of numbers, automatically (thanks to
# boto3) using the DynamoDB encoding {"L": [{"N": "1"}, ...]}, and must
# have the exact length defined as Dimensions of the vector index - which
# in table_vs is 3.
def test_query_vectorsearch_queryvector_bad(table_vs):
    # A non-list QueryVector is rejected:
    with pytest.raises(ClientError, match='ValidationException.*list of numbers'):
        table_vs.query(IndexName='vind',
            VectorSearch={'QueryVector': 'not a list'},
        )
    # A list of the right length but with non-numeric elements
    # should be rejected:
    with pytest.raises(ClientError, match='ValidationException.*only numbers'):
        table_vs.query(IndexName='vind',
            VectorSearch={'QueryVector': [1, 'b', 3]}
        )
    # A numeric list but with the wrong length is rejected:
    with pytest.raises(ClientError, match='ValidationException.*length'):
        table_vs.query(IndexName='vind',
            VectorSearch={'QueryVector': [1, 2]}
        )
    with pytest.raises(ClientError, match='ValidationException.*length'):
        table_vs.query(IndexName='vind',
            VectorSearch={'QueryVector': [1, 2, 3, 4]}
        )

# Test that a Query with VectorSearch requires a Limit parameter, which
# determines how many nearest neighbors to return. This is somewhat
# different from the usual meaning of "Limit" in a query which is
# optional, and used for pagination of results. Vector search does
# not currently support pagination.
def test_query_vectorsearch_limit_bad(table_vs):
    # Limit cannot be missing:
    with pytest.raises(ClientError, match='ValidationException.*Limit'):
        table_vs.query(
            IndexName='vind',
            VectorSearch={'QueryVector': [1, 2, 3]},
        )
    # Limit must be a positive integer:
    for bad_limit in ['hello', 1.5, 0, -3]:
        with pytest.raises(ClientError, match='ValidationException.*Limit'):
            table_vs.query(
                IndexName='vind',
                VectorSearch={'QueryVector': [1, 2, 3]},
                Limit=bad_limit
            )

# Test that a Query with VectorSearch does not support ConsistentRead=True,
# just like queries on a GSI.
def test_query_vectorsearch_consistent_read(table_vs):
    with pytest.raises(ClientError, match='ValidationException.*Consistent'):
        table_vs.query(
            IndexName='vind',
            VectorSearch={'QueryVector': [1, 2, 3]},
            Limit=10,
            ConsistentRead=True)

# Test that a Query with VectorSearch does not support pagination via
# ExclusiveStartKey.
def test_query_vectorsearch_exclusive_start_key(table_vs):
    with pytest.raises(ClientError, match='ValidationException.*ExclusiveStartKey'):
        table_vs.query(
            IndexName='vind',
            VectorSearch={'QueryVector': [1, 2, 3]},
            Limit=10,
            ExclusiveStartKey={'p': 'somekey'},
        )

# Test that a Query with VectorSearch does not support ScanIndexForward.
# The ordering of vector search results is determined by vector distance,
# not by the sort key, so ScanIndexForward makes no sense and is rejected.
def test_query_vectorsearch_scan_index_forward(table_vs):
    with pytest.raises(ClientError, match='ValidationException.*ScanIndexForward'):
        table_vs.query(
            IndexName='vind',
            VectorSearch={'QueryVector': [1, 2, 3]},
            Limit=10,
            ScanIndexForward=True,
        )
    with pytest.raises(ClientError, match='ValidationException.*ScanIndexForward'):
        table_vs.query(
            IndexName='vind',
            VectorSearch={'QueryVector': [1, 2, 3]},
            Limit=10,
            ScanIndexForward=False,
        )

# Test that a Query with VectorSearch and an unused element in
# ExpressionAttributeValues is rejected.
def test_query_vectorsearch_unused_expression_attribute_values(table_vs):
    with pytest.raises(ClientError, match='ValidationException.*val2'):
        table_vs.query(
            IndexName='vind',
            VectorSearch={'QueryVector': [1, 2, 3]},
            Limit=1,
            ExpressionAttributeValues={':val2': 'a'},
        )

# Test that a Query with VectorSearch and an unused element in
# ExpressionAttributeNames is rejected.
def test_query_vectorsearch_unused_expression_attribute_names(table_vs):
    with pytest.raises(ClientError, match='ValidationException.*name2'):
        table_vs.query(
            IndexName='vind',
            VectorSearch={'QueryVector': [1, 2, 3]},
            Limit=1,
            ProjectionExpression='#name1',
            ExpressionAttributeNames={'#name1': 'x', '#name2': 'y'},
        )

# Helper function to check a vector store is configured in Scylla
# with the --vector-store-primary-uri option. This can be done, for
# example, by running test/alternator/run with the option "--vs".
# This function needs some table as a parameter; calling it again
# for the same table will use a cached result.
@cache
def vector_store_configured(table_vs):
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
@pytest.fixture(scope="module")
def needs_vector_store(table_vs):
    if not vector_store_configured(table_vs):
        skip_env('Vector Store is not configured (run with --vs)')

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

# Timeout (in seconds) used by the retry loops in tests that wait for the
# vector store to index data. Centralized here so it can be adjusted easily.
VECTOR_STORE_TIMEOUT = 20

# Test that a vector search Query returns the nearest-neighbour item.
# The vector store is eventually consistent: after put_item the ANN index
# takes time to reflect the new item, so we retry until it appears.
# A private table is used to avoid other tests' data interfering with the
# Limit=1 result. Data is inserted before the index is created so the
# vector store picks it up faster, by prefill scan rather than CDC.
def test_query_vector_prefill(vs, needs_vector_store):
    with new_test_table(vs,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
        p = random_string()
        table.put_item(Item={'p': p, 'v': [1, 0, 0]})
        table.update(VectorIndexUpdates=[{'Create':
            {'IndexName': 'vind',
             'VectorAttribute': {'AttributeName': 'v', 'Dimensions': 3}}}])
        deadline = time.monotonic() + VECTOR_STORE_TIMEOUT
        while True:
            try:
                result = table.query(
                    IndexName='vind',
                    VectorSearch={'QueryVector': [1, 0, 0]},
                    Limit=1
                )
                if result.get('Items') and result['Items'][0]['p'] == p:
                    break
            except ClientError:
                pass
            if time.monotonic() > deadline:
                pytest.fail('Timed out waiting for vector store to return the expected item')
            time.sleep(0.1)

# Same as test_query_vector_prefill but for a table with a clustering key, which
# exercises the separate code path in query_vector() for hash+range tables.
def test_query_vector_with_ck_prefill(vs, needs_vector_store):
    with new_test_table(vs,
            KeySchema=[
                {'AttributeName': 'p', 'KeyType': 'HASH'},
                {'AttributeName': 'c', 'KeyType': 'RANGE'}],
            AttributeDefinitions=[
                {'AttributeName': 'p', 'AttributeType': 'S'},
                {'AttributeName': 'c', 'AttributeType': 'S'}]) as table:
        p = random_string()
        c = random_string()
        table.put_item(Item={'p': p, 'c': c, 'v': [1, 0, 0]})
        table.update(VectorIndexUpdates=[{'Create':
            {'IndexName': 'vind',
             'VectorAttribute': {'AttributeName': 'v', 'Dimensions': 3}}}])
        deadline = time.monotonic() + VECTOR_STORE_TIMEOUT
        while True:
            try:
                result = table.query(
                    IndexName='vind',
                    VectorSearch={'QueryVector': [1, 0, 0]},
                    Limit=1
                )
                items = result.get('Items', [])
                if items and items[0]['p'] == p and items[0]['c'] == c:
                    break
            except ClientError:
                pass
            if time.monotonic() > deadline:
                pytest.fail('Timed out waiting for vector store to return the expected item')
            time.sleep(0.1)

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
    deadline = time.monotonic() + VECTOR_STORE_TIMEOUT
    while True:
        desc = table.meta.client.describe_table(TableName=table.name)
        for vi in desc.get('Table', {}).get('VectorIndexes', []):
            if vi['IndexName'] == index_name and vi['IndexStatus'] == 'ACTIVE':
                return
        if time.monotonic() > deadline:
            pytest.fail(f'Timed out waiting for vector index "{index_name}" to become ACTIVE')
        time.sleep(0.1)

# Test that wait_for_vector_index_active(), waiting for IndexStatus==ACTIVE,
# indeed reliably waits for the index to be ready. A Query issued immediately
# after wait_for_vector_index_active() returns should succeed without any
# retry loop, and also returns the prefilled data.
def test_wait_for_vector_index_active(vs, needs_vector_store):
    with new_test_table(vs,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
        p = random_string()
        table.put_item(Item={'p': p, 'v': [1, 0, 0]})
        table.update(VectorIndexUpdates=[{'Create':
            {'IndexName': 'vind',
             'VectorAttribute': {'AttributeName': 'v', 'Dimensions': 3}}}])
        wait_for_vector_index_active(table, 'vind')
        # The index is now ACTIVE: the prefill scan has completed and the
        # item we inserted is guaranteed to be indexed. Query without catching
        # exceptions or retrying.
        result = table.query(
            IndexName='vind',
            VectorSearch={'QueryVector': [1, 0, 0]},
            Limit=1
        )
        assert result.get('Items') and result['Items'][0]['p'] == p

# The tests test_query_vector_prefill and test_query_vector_with_ck_prefill
# used string keys in the indexed table. In theory, there shouldn't be any
# difference in the vector store's behavior if the keys are of a different
# type (in addition to string, they can be numeric or binary). But in
# practice, the factor store does handle different key types differently,
# and this test used to fail before this was fixed.
# To save a bit of time, we don't test all combinations of hash and range
# key types but test each type at least once as a hash key and a range key.
@pytest.mark.skip_bug(reason="Bug in vector store for non-string keys, fails very slowly so let's skip")
@pytest.mark.parametrize('hash_type,range_type', [
    ('N', None), ('B', None), ('S', 'N'),  ('S', 'B'),
], ids=[
    'N', 'B', 'SN', 'SB'])
def test_query_vector_prefill_key_types(vs, needs_vector_store, hash_type, range_type):
    key_schema = [{'AttributeName': 'p', 'KeyType': 'HASH'}]
    attr_defs = [{'AttributeName': 'p', 'AttributeType': hash_type}]
    if range_type is not None:
        key_schema.append({'AttributeName': 'c', 'KeyType': 'RANGE'})
        attr_defs.append({'AttributeName': 'c', 'AttributeType': range_type})
    key = {'S': 'hello', 'N': Decimal('42'), 'B': b'hello'}
    with new_test_table(vs, KeySchema=key_schema,
                            AttributeDefinitions=attr_defs) as table:
        p = key[hash_type]
        item = {'p': p, 'v': [1, 0, 0]}
        if range_type is not None:
            c = key[range_type]
            item['c'] = c
        table.put_item(Item=item)
        table.update(VectorIndexUpdates=[{'Create':
            {'IndexName': 'vind',
             'VectorAttribute': {'AttributeName': 'v', 'Dimensions': 3}}}])
        wait_for_vector_index_active(table, 'vind')
        result = table.query(IndexName='vind',
            VectorSearch={'QueryVector': [1, 0, 0]}, Limit=1)
        assert len(result['Items']) == 1 and result['Items'] == [item]

# Same as test_query_vector_prefill but whereas in test_query_vector_prefill
# the vector store reads the indexed data by scanning the table, here the
# vector index is created first and only later the data is written, so the
# vector store is expected to pick it up via CDC.
def test_query_vector_cdc(vs, needs_vector_store):
    with new_test_table(vs,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
            VectorIndexes=[
                {'IndexName': 'vind',
                 'VectorAttribute': {'AttributeName': 'v', 'Dimensions': 3}}
            ]) as table:
        # Wait until the vector store is ready (prefill of the empty table
        # has completed), to ensure the subsequent write is picked up via CDC.
        wait_for_vector_index_active(table, 'vind')
        # Now write the item. It should reach the vector store via CDC.
        p = random_string()
        table.put_item(Item={'p': p, 'v': [1, 0, 0]})
        # Retry the query until the newly written item appears in the results.
        deadline = time.monotonic() + VECTOR_STORE_TIMEOUT
        while True:
            try:
                result = table.query(
                    IndexName='vind',
                    VectorSearch={'QueryVector': [1, 0, 0]},
                    Limit=1
                )
                if result.get('Items') and result['Items'][0]['p'] == p:
                    break
            except ClientError:
                pass
            if time.monotonic() > deadline:
                pytest.fail('Timed out waiting for vector store to return the expected item via CDC')
            time.sleep(0.1)

# Similar test to test_query_vector_cdc, where an item is written after the
# vector index is created, but here the item is written using LWT (using a
# ConditionExpression that causes the request to be a read-modify-write
# operation so need to use LWT for most write isolation modes). This is
# important to test because LWT has different code path for recognizing we
# need to write to the CDC log).
def test_query_vector_cdc_lwt(vs, needs_vector_store):
    with new_test_table(vs,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
            VectorIndexes=[
                {'IndexName': 'vind',
                 'VectorAttribute': {'AttributeName': 'v', 'Dimensions': 3}}
            ]) as table:
        wait_for_vector_index_active(table, 'vind')
        # Write the item, with a ConditionExpression to guarantee LWT.
        p = random_string()
        table.put_item(Item={'p': p, 'v': [1, 0, 0]},
            ConditionExpression='attribute_not_exists(p)')
        deadline = time.monotonic() + VECTOR_STORE_TIMEOUT
        while True:
            result = table.query(IndexName='vind',
                    VectorSearch={'QueryVector': [1, 0, 0]}, Limit=1)
            if len(result['Items']) > 0:
                break
            if time.monotonic() > deadline:
                pytest.fail('Timed out waiting for vector store index an item via CDC')
            time.sleep(0.1)
        assert len(result['Items']) == 1 and result['Items'][0]['p'] == p


# Similar to test_query_vector_cdc, this test also that a vector search Query
# find data inserted after the index was created. But this test adds a twist:
# before creating the index, we insert a malformed value for the vector
# attribute (a string). We check that this malformed is ignored by the initial
# prefill scan, but should not prevent a later write with a well-formed vector
# from being indexed and returned by queries.
@pytest.mark.parametrize('malformed', ['garbage', [1,2]], ids=['string','wrong_length'])
def test_query_vector_cdc_malformed_prefill(vs, needs_vector_store, malformed):
    with new_test_table(vs,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
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
             'VectorAttribute': {'AttributeName': 'v', 'Dimensions': 3}}}])
        # Wait for the prefill scan to complete (index becomes ACTIVE).
        wait_for_vector_index_active(table, 'vind')
        # At this point only p2 should be indexed and returned by a query
        result = table.query(IndexName='vind', VectorSearch={'QueryVector': [1, 0, 0]}, Limit=10)
        assert len(result['Items']) == 1 and result['Items'][0]['p'] == p2
        # Now replace the value of p1 by a properly formed vector. It should
        # be eventually picked up by CDC and indexed by the vector index:
        table.put_item(Item={'p': p1, 'v': [1, Decimal("0.1"), 0]})
        deadline = time.monotonic() + VECTOR_STORE_TIMEOUT
        while True:
            result = table.query(IndexName='vind', VectorSearch={'QueryVector': [1, 0, 0]},
                                 Limit=10)
            if len(result['Items']) == 2 and {item['p'] for item in result['Items']} == {p2, p1}:
                break
            if time.monotonic() > deadline:
                assert len(result['Items']) == 2 and {item['p'] for item in result['Items']} == {p2, p1}
                break
            time.sleep(0.1)

# Test like test_query_vector_prefill, but with a query returning multiple
# results. This helps us verify that:
#  1. "Limits" determines the number of results.
#  2. The query_vector() code correctly handles the need to read and
#     return multiple items.
#  3. The multiple results are correctly sorted by distance (nearest first).
def test_query_vector_multiple_results(vs, needs_vector_store):
    with new_test_table(vs,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
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
             'VectorAttribute': {'AttributeName': 'v', 'Dimensions': 3}}}])
        expected_order = [p1, p2, p3]
        deadline = time.monotonic() + VECTOR_STORE_TIMEOUT
        while True:
            try:
                result = table.query(
                    IndexName='vind',
                    VectorSearch={'QueryVector': [Decimal("1"), Decimal("0"), Decimal("0")]},
                    Limit=3
                )
                items = result.get('Items', [])
                got = [item['p'] for item in items if item['p'] in {p1, p2, p3, p4}]
                if got == expected_order:
                    break
            except ClientError:
                pass
            if time.monotonic() > deadline:
                pytest.fail(f'Timed out waiting for correct ordered results; last got: {got}, expected {expected_order}')
            time.sleep(0.1)

# Same as test_query_vector_multiple_results but for a table with a
# clustering key, to exercise the hash+range code path in query_vector().
def test_query_vector_with_ck_multiple_results(vs, needs_vector_store):
    with new_test_table(vs,
            KeySchema=[
                {'AttributeName': 'p', 'KeyType': 'HASH'},
                {'AttributeName': 'c', 'KeyType': 'RANGE'}],
            AttributeDefinitions=[
                {'AttributeName': 'p', 'AttributeType': 'S'},
                {'AttributeName': 'c', 'AttributeType': 'S'}]) as table:
        p1, p2, p3, p4 = random_string(), random_string(), random_string(), random_string()
        c1, c2, c3, c4 = random_string(), random_string(), random_string(), random_string()
        table.put_item(Item={'p': p1, 'c': c1, 'v': [Decimal("1"),   Decimal("0"),   Decimal("0")]})
        table.put_item(Item={'p': p2, 'c': c2, 'v': [Decimal("1"),   Decimal("0.1"), Decimal("0")]})
        table.put_item(Item={'p': p3, 'c': c3, 'v': [Decimal("0"),   Decimal("1"),   Decimal("0")]})
        table.put_item(Item={'p': p4, 'c': c4, 'v': [Decimal("-1"),  Decimal("0"),   Decimal("0")]})
        table.update(VectorIndexUpdates=[{'Create':
            {'IndexName': 'vind',
             'VectorAttribute': {'AttributeName': 'v', 'Dimensions': 3}}}])
        expected_order = [(p1, c1), (p2, c2), (p3, c3)]
        deadline = time.monotonic() + VECTOR_STORE_TIMEOUT
        while True:
            try:
                result = table.query(
                    IndexName='vind',
                    VectorSearch={'QueryVector': [Decimal("1"), Decimal("0"), Decimal("0")]},
                    Limit=3
                )
                items = result.get('Items', [])
                pcs = {(p1, c1), (p2, c2), (p3, c3), (p4, c4)}
                got = [(item['p'], item['c']) for item in items if (item['p'], item['c']) in pcs]
                if got == expected_order:
                    break
            except ClientError:
                pass
            if time.monotonic() > deadline:
                pytest.fail(f'Timed out waiting for correct ordered results; last got: {got}, expected {expected_order}')
            time.sleep(0.1)

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

# Test that DeleteItem removes the item from the vector index.
# Two variants are tested via parametrize:
# - without clustering key (no_ck): deleting the only item in a partition
#   generates a partition tombstone in CDC
# - with clustering key (with_ck): deleting a row generates a row tombstone
#   in CDC, which is a different code path
@pytest.mark.parametrize('with_ck', [False, True], ids=['no_ck', 'with_ck'])
def test_deleteitem_vectorindex(vs, needs_vector_store, with_ck):
    key_schema = [{'AttributeName': 'p', 'KeyType': 'HASH'}]
    attr_defs = [{'AttributeName': 'p', 'AttributeType': 'S'}]
    if with_ck:
        key_schema.append({'AttributeName': 'c', 'KeyType': 'RANGE'})
        attr_defs.append({'AttributeName': 'c', 'AttributeType': 'S'})
    with new_test_table(vs,
            KeySchema=key_schema,
            AttributeDefinitions=attr_defs,
            VectorIndexes=[
                {'IndexName': 'vind',
                 'VectorAttribute': {'AttributeName': 'v', 'Dimensions': 3}}
            ]) as table:
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
        deadline = time.monotonic() + VECTOR_STORE_TIMEOUT
        while True:
            result = table.query(IndexName='vind',
                                 VectorSearch={'QueryVector': [1, 0, 0]},
                                 Select='ALL_ATTRIBUTES',
                                 Limit=1)
            if len(result['Items']) > 0:
                assert result['Items'][0] == item
                break
            if time.monotonic() > deadline:
                pytest.fail('Timed out waiting for item to appear in vector index')
            time.sleep(0.1)
        # Delete the item and wait for it to disappear from the vector index.
        table.delete_item(Key=key)
        deadline = time.monotonic() + VECTOR_STORE_TIMEOUT
        while True:
            result = table.query(IndexName='vind',
                                 VectorSearch={'QueryVector': [1, 0, 0]},
                                 Limit=1)
            if len(result.get('Items', [])) == 0:
                break
            if time.monotonic() > deadline:
                pytest.fail('Timed out waiting for deleted item to disappear from vector index')
            time.sleep(0.1)

# Test vector index with Alternator TTL together. A table is created without
# TTL enabled, data is inserted with expiration time set to the past (but
# expiration not yet enabled), and the item should still appear in vector
# search. Then TTL expiration is enabled and the item should disappear from
# the vector search once TTL deletes it and the deletion propagates via CDC.
# This test is skipped if alternator_ttl_period_in_seconds is not set to a
# low value because otherwise it would take too long to run.
# Two code paths are tested via parametrize:
# - without clustering key (no_ck): partition deletions in CDC.
# - with clustering key (with_ck): row deletions in CDC.
@pytest.mark.parametrize('have_ck', [False, True], ids=['no_ck', 'with_ck'])
def test_vector_with_ttl(vs, needs_vector_store, have_ck):
    period = scylla_config_read(vs, 'alternator_ttl_period_in_seconds')
    if period is None or float(period) > 1:
        skip_env('need alternator_ttl_period_in_seconds <= 1 to run this test quickly')
    key_schema = [{'AttributeName': 'p', 'KeyType': 'HASH'}]
    attr_defs = [{'AttributeName': 'p', 'AttributeType': 'S'}]
    if have_ck:
        key_schema.append({'AttributeName': 'c', 'KeyType': 'RANGE'})
        attr_defs.append({'AttributeName': 'c', 'AttributeType': 'S'})
    with new_test_table(vs,
            KeySchema=key_schema,
            AttributeDefinitions=attr_defs,
            VectorIndexes=[
                {'IndexName': 'vind',
                 'VectorAttribute': {'AttributeName': 'v', 'Dimensions': 3}}
            ]) as table:
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
        deadline = time.monotonic() + VECTOR_STORE_TIMEOUT
        while True:
            result = table.query(IndexName='vind',
                                 VectorSearch={'QueryVector': [1, 0, 0]},
                                 Limit=1)
            if len(result.get('Items', [])) > 0:
                assert result['Items'][0]['p'] == p
                break
            if time.monotonic() > deadline:
                pytest.fail('Timed out waiting for item to appear in vector search before TTL was enabled')
            time.sleep(0.1)
        # Now enable TTL on the 'expiration' attribute. The item has its
        # expiration in the past, so TTL should delete it quickly.
        table.meta.client.update_time_to_live(
            TableName=table.name,
            TimeToLiveSpecification={'AttributeName': 'expiration', 'Enabled': True})
        # Wait for the item to disappear from vector search. TTL deletes the
        # item from the database, and the deletion propagates to the vector
        # store via CDC.
        deadline = time.monotonic() + VECTOR_STORE_TIMEOUT + float(period)
        while True:
            result = table.query(
                IndexName='vind',
                VectorSearch={'QueryVector': [1, 0, 0]},
                Select='ALL_PROJECTED_ATTRIBUTES',
                Limit=1
            )
            if len(result['Items']) == 0:
                break
            if time.monotonic() > deadline:
                pytest.fail('Timed out waiting for TTL-expired item to disappear from vector search')
            time.sleep(0.1)
        # Since we used Select='ALL_PROJECTED_ATTRIBUTES', the loop above
        # already confirms the vector store removed the item (the results
        # come directly from the vector store, not the base table).
        assert result['Items'] == []

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

def test_query_vectorsearch_select(vs, needs_vector_store):
    with new_test_table(vs,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
        p = random_string()
        # Insert data before creating the vector index so the vector store
        # picks it up via prefill scan rather than CDC (faster).
        table.put_item(Item={'p': p, 'v': [1, 0, 0], 'x': 'hello', 'y': 'world'})
        table.update(VectorIndexUpdates=[{'Create':
            {'IndexName': 'vind',
             'VectorAttribute': {'AttributeName': 'v', 'Dimensions': 3}}}])
        # Wait for the item to appear in vector search.
        deadline = time.monotonic() + VECTOR_STORE_TIMEOUT
        while True:
            try:
                result = table.query(
                    IndexName='vind',
                    VectorSearch={'QueryVector': [1, 0, 0]},
                    Limit=1)
                if result.get('Items') and result['Items'][0]['p'] == p:
                    break
            except ClientError:
                pass
            if time.monotonic() > deadline:
                pytest.fail('Timed out waiting for item to be indexed')
            time.sleep(0.1)
        # ALL_PROJECTED_ATTRIBUTES (default when no Select): returns only the
        # primary key attributes.
        result = table.query(IndexName='vind',
            VectorSearch={'QueryVector': [1, 0, 0]}, Limit=1)
        assert result['Items'] == [{'p': p}]
        # Explicit Select=ALL_PROJECTED_ATTRIBUTES:
        result = table.query(IndexName='vind',
            VectorSearch={'QueryVector': [1, 0, 0]}, Limit=1,
            Select='ALL_PROJECTED_ATTRIBUTES')
        assert result['Items'] == [{'p': p}]
        # Select=ALL_ATTRIBUTES: returns the full item.
        result = table.query(IndexName='vind',
            VectorSearch={'QueryVector': [1, 0, 0]}, Limit=1,
            Select='ALL_ATTRIBUTES')
        assert result['Items'] == [{'p': p, 'v': [1, 0, 0], 'x': 'hello', 'y': 'world'}]
        # Select=SPECIFIC_ATTRIBUTES with ProjectionExpression: returns only
        # the specified attributes.
        result = table.query(IndexName='vind',
            VectorSearch={'QueryVector': [1, 0, 0]}, Limit=1,
            Select='SPECIFIC_ATTRIBUTES', ProjectionExpression='p, x')
        assert result['Items'] == [{'p': p, 'x': 'hello'}]
        # ProjectionExpression without Select: defaults to SPECIFIC_ATTRIBUTES.
        result = table.query(IndexName='vind',
            VectorSearch={'QueryVector': [1, 0, 0]}, Limit=1,
            ProjectionExpression='p, y')
        assert result['Items'] == [{'p': p, 'y': 'world'}]
        # Can also use ProjectionExpression with ExpressionAttributeNames:
        result = table.query(IndexName='vind',
            VectorSearch={'QueryVector': [1, 0, 0]}, Limit=1,
            ProjectionExpression='#name1, #name2',
            ExpressionAttributeNames={'#name1': 'p', '#name2': 'x'})
        assert result['Items'] == [{'p': p, 'x': 'hello'}]
        # Select=SPECIFIC_ATTRIBUTES with AttributesToGet: returns only the
        # specified attributes.
        result = table.query(IndexName='vind',
            VectorSearch={'QueryVector': [1, 0, 0]}, Limit=1,
            Select='SPECIFIC_ATTRIBUTES', AttributesToGet=['p', 'x'])
        assert result['Items'] == [{'p': p, 'x': 'hello'}]
        # AttributesToGet without Select: defaults to SPECIFIC_ATTRIBUTES.
        result = table.query(IndexName='vind',
            VectorSearch={'QueryVector': [1, 0, 0]}, Limit=1,
            AttributesToGet=['p', 'y'])
        assert result['Items'] == [{'p': p, 'y': 'world'}]
        # Select=COUNT: returns only the count, no items list.
        result = table.query(IndexName='vind',
            VectorSearch={'QueryVector': [1, 0, 0]}, Limit=1,
            Select='COUNT')
        assert 'Items' not in result
        assert result['Count'] == 1

# Test that invalid Projection parameter values are rejected for both
# CreateTable and UpdateTable's vector index creation.
def test_vector_projection_bad(vs):
    bad_projections = [
        # 'not_an_object',   # We can't check this with boto3
        {'ProjectionType': 'GARBAGE'},
        {},  # missing ProjectionType
    ]
    for bad_projection in bad_projections:
        with pytest.raises(ClientError, match='ValidationException.*Projection'):
            with new_test_table(vs,
                    KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
                    AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
                    VectorIndexes=[{
                        'IndexName': 'vind',
                        'VectorAttribute': {'AttributeName': 'v', 'Dimensions': 3},
                        'Projection': bad_projection,
                    }]) as table:
                pass
    with new_test_table(vs,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
        for bad_projection in bad_projections:
            with pytest.raises(ClientError, match='ValidationException.*Projection'):
                table.update(VectorIndexUpdates=[{'Create': {
                    'IndexName': 'vind',
                    'VectorAttribute': {'AttributeName': 'v', 'Dimensions': 3},
                    'Projection': bad_projection,
                }}])

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

# As we saw in test_item.py::test_attribute_allowed_chars in the DynamoDB API
# attribute names can contain any characters whatsoever, including quotes,
# spaces, and even null bytes. Test that such crazy attribute names can be
# used as vector attributes in vector indexes, and that a vector index with
# such an attribute can be created and used successfully.
def test_vector_attribute_allowed_chars(vs, needs_vector_store):
    # To check both scan-based prefill and CDC-based indexing, we create the
    # table without a vector index and then add the vector index. Data that
    # we added before creating the index needs scan, and data added later
    # needs CDC. We want to ensure that both work correctly with such
    # attribute names.
    attribute_name = 'v with spaces and .-+-&*!#@$%^()\\ \' "quotes" and \0 null byte'
    with new_test_table(vs,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
        p1 = random_string()
        table.put_item(Item={'p': p1, attribute_name: [1, 0, 0]})
        table.update(VectorIndexUpdates=[{'Create':
            {'IndexName': 'vind',
             'VectorAttribute': {'AttributeName': attribute_name, 'Dimensions': 3}}}])
        wait_for_vector_index_active(table, 'vind')
        # The previous item was indexed by a scan. Now let's add another item
        # which will get indexed by CDC.
        p2 = random_string()
        table.put_item(Item={'p': p2, attribute_name: [0, 0, 1]})
        # Wait until the CDC-indexed update (v=[0, 0, 1]) is reflected in the
        # vector search results.
        deadline = time.monotonic() + VECTOR_STORE_TIMEOUT
        while True:
            result = table.query(IndexName='vind',
                VectorSearch={'QueryVector': [0, 0, 1]}, Limit=2)
            if 'Items' in result and len(result['Items']) == 2 and result['Items'][0]['p'] == p2 and result['Items'][1]['p'] == p1:
                break
            if time.monotonic() > deadline:
                pytest.fail('Timed out waiting for items to appear in vector search')
            time.sleep(0.1)

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

# Test that a Query with a vector with a non-numeric "N" element, like "dog"
# or "Inf", is rejected with a validation error. Note that the Query path
# does not convert the numbers to Alternator's internal type ("decimal") so
# the validation path is different, so we need to check it.
def test_query_vectorsearch_queryvector_bad_number_string(table_vs, needs_vector_store):
    # boto3 validates number strings before sending them, so we use
    # client_no_transform to bypass that and let the server reject them.
    with client_no_transform(table_vs.meta.client) as client:
        for bad_num in ['dog', 'Inf', 'NaN', 'Infinity', '-Infinity']:
            print(bad_num)
            with pytest.raises(ClientError, match='ValidationException.*not a valid number'):
                client.query(
                    TableName=table_vs.name,
                    IndexName='vind',
                    VectorSearch={'QueryVector': {'L': [{'N': '1'}, {'N': bad_num}, {'N': '0'}]}},
                    Limit=1,
                )

##############################################################################
# CONTINUE HERE - MAKE A DECISION! PRE-FILTERING:
# Tests *pre-filtering* for filtering on projected attributes, which can be (if
# we continue the implementation) pushed to the vector store or right now -
# key columns.
# We need to decide: In DynamoDB pre-filtering is done with
# KeyConditionExpression NOT FilterExpression.
# KeyConditionExpression is the traditional approach in Query, but is a bit
# weird because these aren't really "keys" (even though in CQL CREATE TABLE
# syntax we pretend they are - and today, they really are keys).
# Do we want to force the user to put these pre-filtering in KeyConditionExpression
# instead of FilterExpression, and only allow FilterExpression for post-filtering
# that must be done in Scylla?
# Alternatively, we could put everything in FilterExpression. This is less
# with DynamoDB's usual semantics but simpler for users.
############################################################################

# WRITE TEST:
# Test that if the FilterExpression happens to only use projected attributes
# (by default this means key attributes) - or if we decide (see above) that
# it's KeyconditionExpression, then it can be, and is, sent to the vector
# store and performed there. We can check that this happens by noticing that
# we get a full LIMIT of results, and not less.

# WRITE TEST:
# Test FilterExpression for post-filtering vector search results with
# Select=ALL_PROJECTED_ATTRIBUTES where the filter only needs projected
# attributes (currently those are key attributes). Here it is important
# for efficency that the vector index applies the filter and we do not need
# to retrive the full items from the base table at all. We can verify that
# this code path was reached by checking that we got back LIMIT results, and
# not fewer.

# TODO: Like test_vector_projection_keys_only, write additional tests for
# ProjectionType=INCLUDE with NonKeyAttributes and ProjectionType=ALL.
# We don't yet support this feature, so I didn't bother to write such a
# test yet, but I can write an xfailing test because we know what we
# expect ALL_PROJECTED_ATTRIBUTES to return in that case.

# Test enabling both vector index and Alternator Streams together on the same
# table, and checking that both work as expected: New items appear in vector
# search results and also in stream records.
# This test focuses on streams with KEYS_ONLY view type, below we have
# additional tests for other types - NEW_IMAGE and OLD_IMAGE.
# Beyond just testing that the two features can coexist, we also want to
# test different combinations and orders of how these features can be enabled
# and later disabled, to check that at any time the situation with the
# feature(s) enabled and the underlying CDC implementation is exactly as
# we expect.
# Three enablement orderings are tested via parametrize:
# - both_at_creation: both vector index and Streams enabled in CreateTable.
# - stream_then_vector: Streams enabled first, then vector index added.
# - vector_then_stream: vector index enabled first, then Streams added.
#   Note: a fourth way - adding both vector index and Streams by one
#   UpdateTable request - is not allowed. This is checked in the next test
#   (test_updatetable_vector_and_streams_same_request).
# Additionally, we test two cases where we disable one of the features after
# creating both, to check that the other continues working correctly:
# - both_at_creation_then_disable_stream
# - both_at_creation_then_disable_vector
@pytest.mark.parametrize('setup_mode', [
    'both_at_creation',
    'stream_then_vector',
    'vector_then_stream',
    'both_at_creation_then_disable_stream',
    'both_at_creation_then_disable_vector',
])
def test_vector_with_streams(vs, needs_vector_store, dynamodbstreams, cql, setup_mode):
    vector_index_spec = {
        'IndexName': 'vind',
        'VectorAttribute': {'AttributeName': 'v', 'Dimensions': 3},
    }
    # Note that the stream may ask for KEYS_ONLY, but the vector store's CDC
    # needs deltas - both need to coexist on the same table.
    stream_spec = {'StreamEnabled': True, 'StreamViewType': 'KEYS_ONLY'}

    create_kwargs = dict(
        KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
    )
    if setup_mode.startswith('both_at_creation'):
        create_kwargs['VectorIndexes'] = [vector_index_spec]
        create_kwargs['StreamSpecification'] = stream_spec

    with new_test_table(vs, **create_kwargs) as table:
        if setup_mode == 'stream_then_vector':
            # Enable Streams first, then add the vector index.
            table.update(StreamSpecification=stream_spec)
            table.update(VectorIndexUpdates=[{'Create': vector_index_spec}])
        elif setup_mode == 'vector_then_stream':
            # Enable the vector index first, then add the streams.
            table.update(VectorIndexUpdates=[{'Create': vector_index_spec}])
            table.update(StreamSpecification=stream_spec)

        # Wait for the vector index to finish its initial prefill scan before
        # writing, to ensure the later write is picked up via CDC, not prefill.
        wait_for_vector_index_active(table, 'vind')

        # Wait for the stream to become ENABLED:
        streams_resp = dynamodbstreams.list_streams(TableName=table.name)
        assert streams_resp['Streams']
        arn = streams_resp['Streams'][0]['StreamArn']
        stream_deadline = time.monotonic() + 60
        while True:
            desc = dynamodbstreams.describe_stream(StreamArn=arn)['StreamDescription']
            if desc.get('StreamStatus') == 'ENABLED':
                break
            if time.monotonic() > stream_deadline:
                pytest.fail('Timed out waiting for stream to become ENABLED')
            time.sleep(0.5)
        # Collect all shard "LATEST" iterators, handling multi-page DescribeStream.
        iterators = []
        while True:
            for shard in desc['Shards']:
                shard_id = shard['ShardId']
                iterators.append(dynamodbstreams.get_shard_iterator(
                    StreamArn=arn, ShardId=shard_id,
                    ShardIteratorType='LATEST')['ShardIterator'])
            if 'LastEvaluatedShardId' not in desc:
                break
            desc = dynamodbstreams.describe_stream(
                StreamArn=arn,
                ExclusiveStartShardId=desc['LastEvaluatedShardId'])['StreamDescription']

        # Confirm that DescribeTable and DescribeStream say that the vector
        # index and stream are both enabled, with the expected KEYS_ONLY.
        table_desc = table.meta.client.describe_table(TableName=table.name)['Table']
        assert 'VectorIndexes' in table_desc
        vector_indexes = table_desc.get('VectorIndexes')
        assert len(vector_indexes) == 1
        assert vector_indexes[0]['IndexName'] == 'vind'
        assert 'StreamSpecification' in table_desc
        stream_spec_desc = table_desc['StreamSpecification']
        assert stream_spec_desc['StreamEnabled'] == True
        assert stream_spec_desc['StreamViewType'] == 'KEYS_ONLY'

        # Confirm with CQL DESCRIBE TABLE that the CDC is configured with
        # delta=full, which is required for the vector index to work correctly
        # (unless post-image is enabled, which it isn't)
        ks = f'alternator_{table.name}'
        create_stmt = cql.execute(f'DESCRIBE TABLE "{ks}"."{table.name}"').one().create_statement
        assert "'delta': 'full'" in create_stmt
        assert "'preimage': 'false'" in create_stmt
        assert "'postimage': 'false'" in create_stmt

        # Write an item with a vector. Both the vector store (via CDC) and
        # Alternator Streams (also via CDC) should pick this up.
        p = random_string()
        table.put_item(Item={'p': p, 'v': [1, 0, 0]})

        # 1. Wait for the item to appear in vector search results.
        deadline = time.monotonic() + VECTOR_STORE_TIMEOUT
        while True:
            result = table.query(
                IndexName='vind',
                VectorSearch={'QueryVector': [1, 0, 0]},
                Limit=1,
                Select='ALL_PROJECTED_ATTRIBUTES',
            )
            if result.get('Items') and result['Items'][0]['p'] == p:
                break
            if time.monotonic() > deadline:
                pytest.fail('Timed out waiting for item to appear in vector search results')
            time.sleep(0.1)

        # 2. Wait for the INSERT record to appear in Alternator Streams.
        # We poll all shards until we see the record for key p.
        # The retry loop isn't really necessary because if we found the
        # item in vector search, we know it already appeared in CDC from where
        # the vector index read.
        deadline = time.monotonic() + VECTOR_STORE_TIMEOUT
        found_in_stream = False
        while not found_in_stream:
            new_iterators = []
            for it in iterators:
                response = dynamodbstreams.get_records(ShardIterator=it)
                if 'NextShardIterator' in response:
                    new_iterators.append(response['NextShardIterator'])
                for record in response.get('Records', []):
                    keys = record['dynamodb']['Keys']
                    if keys.get('p', {}).get('S') == p and record['eventName'] == 'INSERT':
                        found_in_stream = True
                        # KEYS_ONLY stream must not include NewImage or OldImage
                        # (which would contain the full item with 'v' etc.).
                        assert 'NewImage' not in record['dynamodb']
                        assert 'OldImage' not in record['dynamodb']
            iterators = new_iterators
            if not found_in_stream:
                if time.monotonic() > deadline:
                    pytest.fail('Timed out waiting for INSERT record to appear in Alternator Streams')
                time.sleep(0.1)

        if setup_mode == 'both_at_creation_then_disable_stream':
            # Check that if we disable Streams now, DescribeTable/ListStreams
            # say the stream no longer  exists, but DescribeTable says the
            # vector index is still enabled, and moreover the vector index
            # still works - if we write a new vector, we can eventually find
            # it in a vector search.
            table.update(StreamSpecification={'StreamEnabled': False})
            # DescribeTable says stream is disabled, vector index is enabled
            table_desc = table.meta.client.describe_table(TableName=table.name)['Table']
            assert 'StreamSpecification' not in table_desc
            assert 'VectorIndexes' in table_desc
            assert len(table_desc['VectorIndexes']) == 1
            assert table_desc['VectorIndexes'][0]['IndexName'] == 'vind'
            # DescribeStream on the old stream ARN says the stream is gone
            desc = dynamodbstreams.describe_stream(StreamArn=arn)['StreamDescription']
            assert desc['StreamStatus'] == 'DISABLED'
            # ListStreams must no longer lists streams for this table.
            list_resp = dynamodbstreams.list_streams(TableName=table.name)
            assert not list_resp.get('Streams')
            # Write a new item and verify the vector index still picks it up
            p2 = random_string()
            table.put_item(Item={'p': p2, 'v': [0, 1, 0]})
            deadline = time.monotonic() + VECTOR_STORE_TIMEOUT
            while True:
                result = table.query(
                    IndexName='vind',
                    VectorSearch={'QueryVector': [0, 1, 0]},
                    Limit=1,
                    Select='ALL_PROJECTED_ATTRIBUTES',
                )
                if result.get('Items') and result['Items'][0]['p'] == p2:
                    break
                if time.monotonic() > deadline:
                    pytest.fail('Timed out waiting for new item to appear in vector search after stream disabled')
                time.sleep(0.1)
        if setup_mode == 'both_at_creation_then_disable_vector':
            # Delete the vector index. The stream should remain enabled.
            table.update(VectorIndexUpdates=[{'Delete': {'IndexName': 'vind'}}])
            # DescribeTable must no longer report VectorIndexes, but must
            # still report StreamSpecification with KEYS_ONLY.
            table_desc = table.meta.client.describe_table(TableName=table.name)['Table']
            assert not table_desc.get('VectorIndexes')
            assert 'StreamSpecification' in table_desc
            assert table_desc['StreamSpecification']['StreamEnabled'] == True
            assert table_desc['StreamSpecification']['StreamViewType'] == 'KEYS_ONLY'
            # ListStreams must still list this table's stream, with the same ARN.
            list_resp = dynamodbstreams.list_streams(TableName=table.name)
            assert list_resp.get('Streams')
            assert list_resp['Streams'][0]['StreamArn'] == arn

            # Now that there's no longer a vector index, we no longer need
            # delta=full which we needed while the vector index existed (and
            # we confirmed above was set up). At this point, we need just what
            # the Alternator KEYS_ONLY stream needs - this is delta=keys.
            # So let's verify that we've gone back to delta=keys, or we'll be
            # wasting performance.
            ks = f'alternator_{table.name}'
            create_stmt = cql.execute(f'DESCRIBE TABLE "{ks}"."{table.name}"').one().create_statement
            assert "'delta': 'keys'" in create_stmt
            assert "'preimage': 'false'" in create_stmt
            assert "'postimage': 'false'" in create_stmt

            # Write a new item and verify it still appears in the stream.
            # Not only is Streams still enabled, we can actually continue to
            # use the same iterators we had before to see new records.
            p2 = random_string()
            table.put_item(Item={'p': p2, 'v': [0, 1, 0]})
            deadline = time.monotonic() + VECTOR_STORE_TIMEOUT
            found_in_stream = False
            while not found_in_stream:
                new_iterators = []
                for it in iterators:
                    response = dynamodbstreams.get_records(ShardIterator=it)
                    if 'NextShardIterator' in response:
                        new_iterators.append(response['NextShardIterator'])
                    for record in response.get('Records', []):
                        keys = record['dynamodb']['Keys']
                        if keys.get('p', {}).get('S') == p2 and record['eventName'] == 'INSERT':
                            found_in_stream = True
                            assert 'NewImage' not in record['dynamodb']
                            assert 'OldImage' not in record['dynamodb']
                iterators = new_iterators
                if not found_in_stream:
                    if time.monotonic() > deadline:
                        pytest.fail('Timed out waiting for INSERT record in stream after vector index deleted')
                    time.sleep(0.1)

# Test that you can't change the Streams setup *and* vector indexes in one
# UpdateTable call. We have the same test for streams and GSIs, in
# test_gsi_updatetable.py::test_gsi_updatetable_combined_with_streams.
def test_updatetable_vector_and_streams_same_request(vs):
    with new_test_table(vs,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
        vector_ops = [
            {'Create': {'IndexName': 'vind', 'VectorAttribute': {'AttributeName': 'v', 'Dimensions': 3}}},
            {'Delete': {'IndexName': 'nonexistent'}},
        ]
        stream_specs = [
            {'StreamEnabled': True, 'StreamViewType': 'KEYS_ONLY'},
            {'StreamEnabled': False},
        ]
        for vector_op in vector_ops:
            for stream_spec in stream_specs:
                with pytest.raises(ClientError, match='ValidationException.*cannot create or delete index while changing stream status'):
                    table.update(
                        VectorIndexUpdates=[vector_op],
                        StreamSpecification=stream_spec)

# Test special interactions of Streams with NEW_IMAGE, and vector index:
# * When enabling vector index when streams with NEW_IMAGE was already
#   enabled, things work - the vector search functions, stream returns new
#   images as requested. DescribeStream knows we're in NEW_IMAGE.
# * Enabling the vector index did not enable delta=full mode because NEW_IMAGE
#   (postimage) is enough, and adding delta=full on top would be wasteful.
# * After disabling the stream, the post-images are no longer needed, but
#   CDC should continue to be enabled and switch to delta=full mode (the vector
#   index default), and the vector index continues to get updated.
@pytest.mark.parametrize('setup_mode', [
    'stream_then_vector_then_disable_stream',
    'stream_then_vector_then_disable_vector',
])
def test_vector_with_new_image_stream(vs, needs_vector_store, dynamodbstreams, cql, setup_mode):
    stream_spec = {'StreamEnabled': True, 'StreamViewType': 'NEW_IMAGE'}
    vector_index_spec = {
        'IndexName': 'vind',
        'VectorAttribute': {'AttributeName': 'v', 'Dimensions': 3},
    }

    # Create the table with a NEW_IMAGE stream, then add the vector index.
    with new_test_table(vs,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
            StreamSpecification=stream_spec) as table:
        table.update(VectorIndexUpdates=[{'Create': vector_index_spec}])
        wait_for_vector_index_active(table, 'vind')

        # Wait for the stream to become ENABLED.
        streams_resp = dynamodbstreams.list_streams(TableName=table.name)
        assert streams_resp['Streams']
        arn = streams_resp['Streams'][0]['StreamArn']
        stream_deadline = time.monotonic() + 60
        while True:
            desc = dynamodbstreams.describe_stream(StreamArn=arn)['StreamDescription']
            if desc.get('StreamStatus') == 'ENABLED':
                break
            if time.monotonic() > stream_deadline:
                pytest.fail('Timed out waiting for stream to become ENABLED')
            time.sleep(0.1)

        # DescribeStream and DescribeTable must both report NEW_IMAGE
        assert desc['StreamViewType'] == 'NEW_IMAGE'
        table_desc = table.meta.client.describe_table(TableName=table.name)['Table']
        assert table_desc['StreamSpecification']['StreamEnabled'] == True
        assert table_desc['StreamSpecification']['StreamViewType'] == 'NEW_IMAGE'

        # Check, using CQL DESCRIBE TABLE, that CDC is configured with
        # postimage=true (for NEW_IMAGE) but delta=keys, not full - because
        # the vector store can use the post image directly, so upgrading to
        # delta=full is wasteful.
        ks = f'alternator_{table.name}'
        create_stmt = cql.execute(f'DESCRIBE TABLE "{ks}"."{table.name}"').one().create_statement
        assert "'postimage': 'true'" in create_stmt
        assert "'delta': 'keys'" in create_stmt

        # Collect all shard LATEST iterators, that we'll use to read from
        # the stream.
        iterators = []
        while True:
            for shard in desc['Shards']:
                iterators.append(dynamodbstreams.get_shard_iterator(
                    StreamArn=arn, ShardId=shard['ShardId'],
                    ShardIteratorType='LATEST')['ShardIterator'])
            if 'LastEvaluatedShardId' not in desc:
                break
            desc = dynamodbstreams.describe_stream(
                StreamArn=arn,
                ExclusiveStartShardId=desc['LastEvaluatedShardId'])['StreamDescription']

        # Write an item. Both vector store and stream should pick it up.
        p1 = random_string()
        item = {'p': p1, 'v': [1, 0, 0], 'x': 'hello'}
        table.put_item(Item=item)

        # Wait for the item to appear in vector search results.
        deadline = time.monotonic() + VECTOR_STORE_TIMEOUT
        while True:
            result = table.query(IndexName='vind',
                VectorSearch={'QueryVector': [1, 0, 0]}, Limit=1)
            if result.get('Items') and result['Items'][0]['p'] == p1:
                break
            if time.monotonic() > deadline:
                pytest.fail('Timed out waiting for item to appear in vector search')
            time.sleep(0.1)

        # Wait for the INSERT record to appear in the stream.
        # The stream must include NewImage (because StreamViewType=NEW_IMAGE)
        deadline = time.monotonic() + VECTOR_STORE_TIMEOUT
        found_in_stream = False
        while not found_in_stream:
            new_iterators = []
            for it in iterators:
                response = dynamodbstreams.get_records(ShardIterator=it)
                if 'NextShardIterator' in response:
                    new_iterators.append(response['NextShardIterator'])
                for record in response.get('Records', []):
                    keys = record['dynamodb']['Keys']
                    if keys.get('p', {}).get('S') == p1 and record['eventName'] == 'INSERT':
                        found_in_stream = True
                        # NEW_IMAGE stream must include NewImage with the full item.
                        assert 'NewImage' in record['dynamodb']
                        deserializer = boto3.dynamodb.types.TypeDeserializer()
                        new_image = {x:deserializer.deserialize(y) for (x,y) in record['dynamodb']['NewImage'].items()}
                        assert new_image == item
                        # No OldImage - we only asked for NEW_IMAGE and in
                        # any case there was no previous item in this test.
                        assert 'OldImage' not in record['dynamodb']
            iterators = new_iterators
            if not found_in_stream:
                if time.monotonic() > deadline:
                    pytest.fail('Timed out waiting for INSERT record in stream')
                time.sleep(0.1)

        if setup_mode == 'stream_then_vector_then_disable_stream':
            # Now disable the stream. The vector index will remain enabled.
            table.update(StreamSpecification={'StreamEnabled': False})

            # Verify that DescribeTable and DescribeStream report the stream is
            # really disabled, and DescribeTable still reports the vector index
            # is enabled.
            table_desc = table.meta.client.describe_table(TableName=table.name)['Table']
            # Currently, when vector index is enabled, our implementation drops
            # StreamSpecification entirely, it doesn't have it with enabled = false.
            assert 'StreamSpecification' not in table_desc
            assert 'VectorIndexes' in table_desc
            assert len(table_desc['VectorIndexes']) == 1
            assert table_desc['VectorIndexes'][0]['IndexName'] == 'vind'
            # If we try to describe the old ARN of the stream, it should say it's disabled:
            desc = dynamodbstreams.describe_stream(StreamArn=arn)['StreamDescription']
            assert desc['StreamStatus'] == 'DISABLED'
            # Also check that ListStreams for this table doesn't return this stream
            # (it's disabled, so it shouldn't appear as an active stream).
            list_resp = dynamodbstreams.list_streams(TableName=table.name)
            assert not list_resp.get('Streams')

            # Check that CDC is still enabled, but post-image is off and
            # delta=full (the vector index default):
            create_stmt = cql.execute(f'DESCRIBE TABLE "{ks}"."{table.name}"').one().create_statement
            assert "'postimage': 'false'" in create_stmt
            assert "'delta': 'full'" in create_stmt

            # Write a second item and verify the vector store still indexes it,
            # so the remaining CDC is good enough for the vector store.
            p2 = random_string()
            table.put_item(Item={'p': p2, 'v': [0, 1, 0]})
            deadline = time.monotonic() + VECTOR_STORE_TIMEOUT
            while True:
                result = table.query(IndexName='vind',
                    VectorSearch={'QueryVector': [0, 1, 0]}, Limit=1)
                if result.get('Items') and result['Items'][0]['p'] == p2:
                    break
                if time.monotonic() > deadline:
                    pytest.fail('Timed out waiting for item to appear in vector search after stream disabled')
                time.sleep(0.1)
        if setup_mode == 'stream_then_vector_then_disable_vector':
            # Now disable the vector index. The stream will remain enabled.
            table.update(VectorIndexUpdates=[{'Delete': {'IndexName': 'vind'}}])

            # Verify that DescribeTable and DescribeStream report the stream is
            # still enabled, and DescribeTable no longer reports the vector index
            # as enabled.
            table_desc = table.meta.client.describe_table(TableName=table.name)['Table']
            assert table_desc['StreamSpecification']['StreamEnabled'] == True
            assert table_desc['StreamSpecification']['StreamViewType'] == 'NEW_IMAGE'
            assert 'VectorIndexes' not in table_desc
            # The old ARN of the stream is still usable and listed in ListStreams:
            desc = dynamodbstreams.describe_stream(StreamArn=arn)['StreamDescription']
            assert desc['StreamStatus'] == 'ENABLED'
            list_resp = dynamodbstreams.list_streams(TableName=table.name)
            assert 'Streams' in list_resp
            assert len(list_resp['Streams']) == 1
            assert list_resp['Streams'][0]['StreamArn'] == arn
            # Verify that CDC is still enabled, still has postimage and
            # delta=keys which the stream still needs
            create_stmt = cql.execute(f'DESCRIBE TABLE "{ks}"."{table.name}"').one().create_statement
            assert "'postimage': 'true'" in create_stmt
            assert "'delta': 'keys'" in create_stmt
            # Write a second item and verify the stream still receives it.
            # We continue with the same iterators we had before, which should
            # still work because the stream is still enabled.
            p2 = random_string()
            item = {'p': p2, 'x': 'hello'}
            table.put_item(Item=item)
            deadline = time.monotonic() + VECTOR_STORE_TIMEOUT
            found_in_stream = False
            while not found_in_stream:
                new_iterators = []
                for it in iterators:
                    response = dynamodbstreams.get_records(ShardIterator=it)
                    if 'NextShardIterator' in response:
                        new_iterators.append(response['NextShardIterator'])
                    for record in response.get('Records', []):
                        keys = record['dynamodb']['Keys']
                        if keys.get('p', {}).get('S') == p2 and record['eventName'] == 'INSERT':
                            found_in_stream = True
                            # NEW_IMAGE stream must still include NewImage with the full item.
                            assert 'NewImage' in record['dynamodb']
                            deserializer = boto3.dynamodb.types.TypeDeserializer()
                            new_image = {x: deserializer.deserialize(y) for (x, y) in record['dynamodb']['NewImage'].items()}
                            assert new_image == item
                iterators = new_iterators
                if not found_in_stream:
                    if time.monotonic() > deadline:
                        pytest.fail('Timed out waiting for INSERT record in stream after vector disabled')
                    time.sleep(0.1)

# Test that enabling a vector index doesn't confuse Alternator to report that
# this table has streams enabled. The vector index enables CDC internally
# but this must not be mistaken for a user-facing stream: DescribeTable must
# not show StreamSpecification, and ListStreams should not list any streams
# for this table..
def test_vectorindex_no_spurious_stream(table_vs, dynamodbstreams):
    # DescribeTable must not report StreamSpecification for a table that only
    # has a vector index (and no user-requested Alternator Stream).
    table_desc = table_vs.meta.client.describe_table(TableName=table_vs.name)['Table']
    assert 'StreamSpecification' not in table_desc
    # ListStreams must not list any streams return this table.
    list_resp = dynamodbstreams.list_streams(TableName=table_vs.name)
    assert not list_resp.get('Streams')

# Test special interactions of Streams with OLD_IMAGE, and vector index:
# * When enabling a vector index after a stream with OLD_IMAGE was already
#   enabled, things work - the vector search functions, stream returns old
#   images as requested.
# * DescribeTable, DescribeStream, and ListStreams know this stream exists
#   and has OLD_IMAGE. This is despite the implementation detail that the
#   vector index enabled delta=full on CDC (because pre-images aren't enough
#   for it).
def test_vector_with_old_image_stream(vs, needs_vector_store, dynamodbstreams, cql):
    stream_spec = {'StreamEnabled': True, 'StreamViewType': 'OLD_IMAGE'}
    vector_index_spec = {
        'IndexName': 'vind',
        'VectorAttribute': {'AttributeName': 'v', 'Dimensions': 3},
    }

    # Create the table with an OLD_IMAGE stream, then add the vector index.
    with new_test_table(vs,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
            StreamSpecification=stream_spec) as table:
        table.update(VectorIndexUpdates=[{'Create': vector_index_spec}])
        wait_for_vector_index_active(table, 'vind')

        # ListStreams must list this table's stream
        streams_resp = dynamodbstreams.list_streams(TableName=table.name)
        assert streams_resp['Streams']
        assert len(streams_resp['Streams']) == 1
        arn = streams_resp['Streams'][0]['StreamArn']

        # Wait for the stream to become ENABLED.
        stream_deadline = time.monotonic() + 60
        while True:
            desc = dynamodbstreams.describe_stream(StreamArn=arn)['StreamDescription']
            if desc.get('StreamStatus') == 'ENABLED':
                break
            if time.monotonic() > stream_deadline:
                pytest.fail('Timed out waiting for stream to become ENABLED')
            time.sleep(0.5)

        # DescribeStream and DescribeTable must both report OLD_IMAGE.
        assert desc['StreamViewType'] == 'OLD_IMAGE'
        table_desc = table.meta.client.describe_table(TableName=table.name)['Table']
        assert table_desc['StreamSpecification']['StreamEnabled'] == True
        assert table_desc['StreamSpecification']['StreamViewType'] == 'OLD_IMAGE'

        # Verify (using CQL DESCRIBE TABLE) that CDC is configured with preimage=true,
        # postimage=false and delta=full (which the vector index needs when
        # postimage=false)
        ks = f'alternator_{table.name}'
        create_stmt = cql.execute(f'DESCRIBE TABLE "{ks}"."{table.name}"').one().create_statement
        assert "'preimage': 'full'" in create_stmt
        assert "'postimage': 'false'" in create_stmt
        assert "'delta': 'full'" in create_stmt

        # Collect all shard LATEST iterators to prepare to read from the stream.
        iterators = []
        while True:
            for shard in desc['Shards']:
                iterators.append(dynamodbstreams.get_shard_iterator(
                    StreamArn=arn, ShardId=shard['ShardId'],
                    ShardIteratorType='LATEST')['ShardIterator'])
            if 'LastEvaluatedShardId' not in desc:
                break
            desc = dynamodbstreams.describe_stream(
                StreamArn=arn,
                ExclusiveStartShardId=desc['LastEvaluatedShardId'])['StreamDescription']

        # Write a first version of an item and wait for it to appear in vector search.
        p = random_string()
        item1 = {'p': p, 'v': [1, 0, 0], 'x': 'version1'}
        table.put_item(Item=item1)

        # Overwrite the item with a new version. The stream should eventually
        # produce a record record with OldImage=item1 (the previous version)
        # and no NewImage.
        item2 = {'p': p, 'v': [1, 0, 0], 'x': 'version2'}
        table.put_item(Item=item2)

        deadline = time.monotonic() + VECTOR_STORE_TIMEOUT
        found_oldimage = False
        while not found_oldimage:
            new_iterators = []
            for it in iterators:
                response = dynamodbstreams.get_records(ShardIterator=it)
                if 'NextShardIterator' in response:
                    new_iterators.append(response['NextShardIterator'])
                for record in response.get('Records', []):
                    keys = record['dynamodb']['Keys']
                    if keys.get('p', {}).get('S') == p:
                        # OLD_IMAGE stream must include OldImage with the previous item.
                        if 'OldImage' in record['dynamodb']:
                            found_oldimage = True
                            deserializer = boto3.dynamodb.types.TypeDeserializer()
                            old_image = {x: deserializer.deserialize(y) for (x, y) in record['dynamodb']['OldImage'].items()}
                            assert old_image == item1
                            # OLD_IMAGE must not include NewImage.
                            assert 'NewImage' not in record['dynamodb']
            iterators = new_iterators
            if not found_oldimage:
                if time.monotonic() > deadline:
                    pytest.fail('Timed out waiting for MODIFY record in Alternator Streams')
                time.sleep(0.1)
