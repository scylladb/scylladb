# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# Tests for the vector search feature. This is an Alternator extension
# that does not exist on DynamoDB, so most tests in this file are skipped
# when running against DynamoDB (a few tests that don't use the "vs" fixture
# can still run on DynamoDB).

import pytest
import time
import json
import struct
import decimal
from decimal import Decimal
from contextlib import contextmanager
from functools import cache

from botocore.exceptions import ClientError
import boto3.dynamodb.types

from test.pylib.skip_types import skip_env
from .util import random_string, new_test_table, unique_table_name, scylla_config_read, scylla_config_write, client_no_transform, is_aws, manual_request

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
    boto3.dynamodb.types.TypeDeserializer._deserialize_float32vector = lambda self, value: Vector(value)

    yield resource

    # Restore the original serialize method and remove the deserializer patch.
    boto3.dynamodb.types.TypeSerializer.serialize = _orig_serialize
    del boto3.dynamodb.types.TypeDeserializer._deserialize_float32vector

# Use the Vector(list) type for test values that are meant to be stored as
# optimized vectors (array of floats instead of JSON list of numbers).
# The serialization monkey-patching in the vs fixture will cause this list
# to be serialized and sent to Alternator as
# {'FLOAT32VECTOR': [1.0, 2.0, ...]}} instead of the standard list-of-numbers
# {'L': [{'N': '1.0'}, ...]}.
class Vector(list):
    pass


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

# Similarly, it's not allowed to combine VectorIndexUpdates with
# StreamSpecification in the same UpdateTable request. This mirrors
# the same restriction on GlobalSecondaryIndexUpdates + StreamSpecification
# ("You cannot create or delete index while changing stream status").
def test_updatetable_vector_and_stream_same_request(vs):
    with new_test_table(vs,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
        with pytest.raises(ClientError, match='ValidationException'):
            table.meta.client.update_table(
                TableName=table.name,
                VectorIndexUpdates=[{'Create': {
                    'IndexName': 'vec',
                    'VectorAttribute': {'AttributeName': 'v', 'Dimensions': 3}
                }}],
                StreamSpecification={'StreamEnabled': True, 'StreamViewType': 'NEW_AND_OLD_IMAGES'})

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
            VectorSearch={'QueryVector': [1, 0, 0]}, Limit=1,
            Select='ALL_ATTRIBUTES')
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

# Test that when creating a vector index via UpdateTable, an optional
# SimilarityFunction can be specified. The valid values are EUCLIDEAN,
# COSINE, DOT_PRODUCT, and an invalid value should be rejected.
# DescribeTable should return the SimilarityFunction that was set.
def test_updatetable_vectorindex_similarity_function(vs):
    with new_test_table(vs,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
        # A bad SimilarityFunction should be rejected:
        with pytest.raises(ClientError, match='ValidationException.*SimilarityFunction'):
            table.update(VectorIndexUpdates=[{'Create':
                {'IndexName': 'ind',
                 'VectorAttribute': {'AttributeName': 'v', 'Dimensions': 3},
                 'SimilarityFunction': 'BAD_FUNCTION'}}])
        # Each of the valid SimilarityFunction values should be accepted and
        # returned by DescribeTable. We use a different attribute name for
        # each to avoid conflicts.
        for sf in ['EUCLIDEAN', 'COSINE', 'DOT_PRODUCT']:
            table.update(VectorIndexUpdates=[{'Create':
                {'IndexName': f'ind_{sf}',
                 'VectorAttribute': {'AttributeName': f'v_{sf}', 'Dimensions': 3},
                 'SimilarityFunction': sf}}])
            desc = table.meta.client.describe_table(TableName=table.name)
            indexes = {vi['IndexName']: vi for vi in desc['Table']['VectorIndexes']}
            assert f'ind_{sf}' in indexes
            assert indexes[f'ind_{sf}']['SimilarityFunction'] == sf
        # Without an explicit SimilarityFunction, a default ("COSINE") is
        # used and DescribeTable should return it:
        table.update(VectorIndexUpdates=[{'Create':
            {'IndexName': 'ind_default',
             'VectorAttribute': {'AttributeName': 'v_default', 'Dimensions': 3}}}])
        desc = table.meta.client.describe_table(TableName=table.name)
        indexes = {vi['IndexName']: vi for vi in desc['Table']['VectorIndexes']}
        assert 'ind_default' in indexes
        assert indexes['ind_default']['SimilarityFunction'] == 'COSINE'

# Same as test_updatetable_vectorindex_similarity_function() above, but
# for CreateTable instead of UpdateTable.
def test_createtable_vectorindex_similarity_function(vs):
    # A bad SimilarityFunction should be rejected:
    with pytest.raises(ClientError, match='ValidationException.*SimilarityFunction'):
        with new_test_table(vs,
                KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
                AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
                VectorIndexes=[{'IndexName': 'ind',
                                'VectorAttribute': {'AttributeName': 'v', 'Dimensions': 3},
                                'SimilarityFunction': 'BAD_FUNCTION'}]) as table:
            pass
    # Each of the valid SimilarityFunction values should be accepted and
    # returned by DescribeTable.
    for sf in ['EUCLIDEAN', 'COSINE', 'DOT_PRODUCT']:
        with new_test_table(vs,
                KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
                AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
                VectorIndexes=[{'IndexName': 'ind',
                                'VectorAttribute': {'AttributeName': 'v', 'Dimensions': 3},
                                'SimilarityFunction': sf}]) as table:
            desc = table.meta.client.describe_table(TableName=table.name)
            indexes = {vi['IndexName']: vi for vi in desc['Table']['VectorIndexes']}
            assert indexes['ind']['SimilarityFunction'] == sf
    # Without an explicit SimilarityFunction, COSINE is the default and
    # DescribeTable should return it:
    with new_test_table(vs,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
            VectorIndexes=[{'IndexName': 'ind',
                            'VectorAttribute': {'AttributeName': 'v', 'Dimensions': 3}}]) as table:
        desc = table.meta.client.describe_table(TableName=table.name)
        indexes = {vi['IndexName']: vi for vi in desc['Table']['VectorIndexes']}
        assert indexes['ind']['SimilarityFunction'] == 'COSINE'

# Test that the different SimilarityFunction values (EUCLIDEAN, COSINE,
# DOT_PRODUCT) chosen during CreateTable actually work as expected in
# subsequent vector-search queries, returning different result orders for
# the same query vector.
def test_createtable_query_similarity_function(vs, needs_vector_store):
    # Choose 3 items whose nearest-neighbor under query [1, 0, 0] differs
    # depending on the similarity function:
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
    for sf, expected_p in [('COSINE', p_small),
                            ('DOT_PRODUCT', p_big),
                            ('EUCLIDEAN', p_close)]:
        with new_test_table(vs,
                KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
                AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
            table.put_item(Item={'p': p_small, 'v': [Decimal("0.5"), Decimal("0"),    Decimal("0")]})
            table.put_item(Item={'p': p_big,   'v': [Decimal("2"),   Decimal("0.01"), Decimal("0")]})
            table.put_item(Item={'p': p_close, 'v': [Decimal("1"),   Decimal("0.3"),  Decimal("0")]})
            table.update(VectorIndexUpdates=[{'Create': {
                'IndexName': 'vind',
                'VectorAttribute': {'AttributeName': 'v', 'Dimensions': 3},
                'SimilarityFunction': sf,
            }}])
            wait_for_vector_index_active(table, 'vind')
            result = table.query(
                IndexName='vind',
                VectorSearch={'QueryVector': [Decimal("1"), Decimal("0"), Decimal("0")]},
                Limit=1,
                Select='ALL_PROJECTED_ATTRIBUTES',
            )
            assert len(result['Items']) == 1, \
                f'Expected 1 result for SimilarityFunction={sf}'
            assert result['Items'][0]['p'] == expected_p, \
                f'For SimilarityFunction={sf}, expected nearest item {expected_p}, ' \
                f'got {result["Items"][0]["p"]}'

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

# Test that VectorSearch.ReturnScores rejects unknown values and the
# SIMILARITY+COUNT combination. No vector store needed.
def test_query_vectorsearch_return_similarity_bad(table_vs):
    # Unknown value is rejected:
    with pytest.raises(ClientError, match='ValidationException.*ReturnScores'):
        table_vs.query(IndexName='vind',
            VectorSearch={'QueryVector': [1, 2, 3], 'ReturnScores': 'GARBAGE'}, Limit=1)
    # SIMILARITY with Select=COUNT is rejected:
    with pytest.raises(ClientError, match='ValidationException.*COUNT'):
        table_vs.query(IndexName='vind',
            VectorSearch={'QueryVector': [1, 2, 3], 'ReturnScores': 'SIMILARITY'}, Limit=1,
            Select='COUNT')

# Test for VectorSearch.ReturnScores - if set to NONE (the default),
# a "Scores" field is missing in the response, but with SIMILARITY,
# a "Scores" field is present and contains the similarity values for
# each returned item. We verify the length matches Items, the scores are in
# descending order, and exact scores match expected COSINE similarity values
# for known vectors.
def test_query_vectorsearch_return_similarity(vs, needs_vector_store):
    with new_test_table(vs,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
        # Insert 4 items at known COSINE similarity to query vector [1, 0, 0]:
        #   p1 at [1, 0, 0]   cos=1.0    -> similarity 1.0 (identical)
        #   p2 at [1, 0.1, 0] cos~=0.995 -> similarity ~ 0.9975
        #   p3 at [0, 1, 0]   cos=0.0    -> similarity 0.5 (orthogonal)
        #   p4 at [-1, 0, 0]  cos=-1.0   -> similarity 0.0 (opposite)
        p1, p2, p3, p4 = random_string(), random_string(), random_string(), random_string()
        table.put_item(Item={'p': p1, 'v': Vector([1.0,  0.0,  0.0])})
        table.put_item(Item={'p': p2, 'v': Vector([1.0,  0.1,  0.0])})
        table.put_item(Item={'p': p3, 'v': Vector([0.0,  1.0,  0.0])})
        table.put_item(Item={'p': p4, 'v': Vector([-1.0, 0.0,  0.0])})
        table.update(VectorIndexUpdates=[{'Create':
            {'IndexName': 'vind',
             'VectorAttribute': {'AttributeName': 'v', 'Dimensions': 3},
             'SimilarityFunction': 'COSINE'}}])
        wait_for_vector_index_active(table, 'vind')
        # Without ReturnScores (or with NONE), no Scores in response.
        result = table.query(
            IndexName='vind',
            VectorSearch={'QueryVector': Vector([1.0, 0.0, 0.0])},
            Limit=4)
        assert 'Scores' not in result
        result = table.query(
            IndexName='vind',
            VectorSearch={'QueryVector': Vector([1.0, 0.0, 0.0]), 'ReturnScores': 'NONE'},
            Limit=4)
        assert 'Scores' not in result
        # With SIMILARITY, Scores is present, same length as Items, and
        # in descending order (nearest neighbor has the highest score).
        result = table.query(
            IndexName='vind',
            VectorSearch={'QueryVector': Vector([1.0, 0.0, 0.0]), 'ReturnScores': 'SIMILARITY'},
            Limit=4)
        assert [item['p'] for item in result['Items']] == [p1, p2, p3, p4]
        assert 'Scores' in result
        sims = result['Scores']
        assert len(sims) == 4
        # Scores must be in descending order.
        assert sims == sorted(sims, reverse=True)
        # Map item key to its position in the result list.
        pos = {item['p']: i for i, item in enumerate(result['Items'])}
        # Known similarity values for three easy cases:
        assert sims[pos[p1]] == pytest.approx(1.0, abs=1e-5)
        assert sims[pos[p3]] == pytest.approx(0.5, abs=1e-5)
        assert sims[pos[p4]] == pytest.approx(0.0, abs=1e-5)
        # Use FilterExpression to only leave p1 and p3 in the results, and
        # verify their similarity values are still correct and in the right
        # order.
        result = table.query(
            IndexName='vind',
            VectorSearch={'QueryVector': Vector([1.0, 0.0, 0.0]), 'ReturnScores': 'SIMILARITY'},
            Limit=4,
            FilterExpression='p = :p1 OR p = :p3',
            ExpressionAttributeValues={':p1': p1, ':p3': p3},
        )
        assert [item['p'] for item in result['Items']] == [p1, p3]
        assert result['ScannedCount'] == 4
        assert result['Count'] == 2
        sims = result['Scores']
        assert len(sims) == 2
        assert sims[0] == pytest.approx(1.0, abs=1e-5)
        assert sims[1] == pytest.approx(0.5, abs=1e-5)

# Another ReturnScores=SIMILARITY test, for a different similarity
# function (EUCLIDEAN).
def test_query_vectorsearch_return_similarity2(vs, needs_vector_store):
    with new_test_table(vs,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
        p = random_string()
        table.put_item(Item={'p': p, 'v': Vector([2.0, 0.0, 0.0])})
        table.update(VectorIndexUpdates=[{'Create':
            {'IndexName': 'vind',
             'VectorAttribute': {'AttributeName': 'v', 'Dimensions': 3},
             'SimilarityFunction': 'EUCLIDEAN'}}])
        wait_for_vector_index_active(table, 'vind')
        result = table.query(
            IndexName='vind',
            VectorSearch={'QueryVector': Vector([1.0, 0.0, 0.0]), 'ReturnScores': 'SIMILARITY'},
            Limit=1)
        assert len(result['Items']) == 1 and result['Items'][0]['p'] == p
        # The EUCLIDEAN similarity is defined as 1/(1+d) where d is the L2
        # distance. The L2 distance between our query vector and single item
        # is 1.0, so similarity is 1/(1+1) = 0.5.
        assert result['Scores'] == [pytest.approx(0.5, abs=1e-5)]

# The DOT_PRODUCT similarity function is not bounded if vectors are not
# normalized (have an arbitrary magnitude, not 1.0). It is possible that the
# similarity value returned with ReturnScores=SIMILARITY could be beyond
# the range of 32-bit float even for valid float32 vectors. In this case, the
# implementation should not cause an error or drop this item - it should
# return the item correctly with a very high (even if not mathematically
# accurate) similarity score.
def test_query_vectorsearch_return_similarity_dot_product_overflow(vs, needs_vector_store):
    # Two float32 vectors with very large - but valid - magnitude BIG,
    # have a dot product of BIG^2, which overflows float32 to +infinity.
    BIG = 1e38 # Valid 32-bit number, but close to the maximum
    with new_test_table(vs,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
        p = random_string()
        table.put_item(Item={'p': p, 'v': Vector([BIG, 0.0, 0.0])})
        table.update(VectorIndexUpdates=[{'Create':
            {'IndexName': 'vind',
             'VectorAttribute': {'AttributeName': 'v', 'Dimensions': 3},
             'SimilarityFunction': 'DOT_PRODUCT'}}])
        wait_for_vector_index_active(table, 'vind')
        result = table.query(
            IndexName='vind',
            VectorSearch={'QueryVector': Vector([BIG, 0.0, 0.0]),
                          'ReturnScores': 'SIMILARITY'},
            Limit=1,
            Select='ALL_ATTRIBUTES')
        assert len(result['Items']) == 1 and result['Items'][0]['p'] == p
        assert 'Scores' in result
        # The dot product BIG * BIG overflows float32 to infinity.
        # Since JSON can't represent infinity and must return some number,
        # we don't really care what it returns, as long as it's very large.
        # Let's just verify it's larger than BIG itself.
        assert result['Scores'][0] >= BIG

# Test that DOT_PRODUCT similarity correctly orders three items:
# one very highly similar (large positive dot product overflowing 32 bits),
# one mildly similar (small positive), and one very highly dissimilar
# (large negative overflowing 32 bits).
# The vector store should return them in descending score order.
# We also check the Scores themselves: the highly similar item should have
# a score much larger than 1, the mildly similar item around 1, and the
# highly dissimilar item should have a large negative score.
#
# This test used to reproduce a bug in the vector store: When the similarity score
# overflowed the 32-bit calculation, it returned the same value "null" for both
# +infinity and -infinity.
def test_query_vectorsearch_return_similarity_dot_product_overflow2(vs, needs_vector_store):
    BIG = 1e38  # Near FLT_MAX; dot product BIG * BIG overflows float32
    with new_test_table(vs,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
        p_high = random_string()
        p_mid  = random_string()
        p_low  = random_string()
        table.put_item(Item={'p': p_high, 'v': Vector([BIG,  0.0, 0.0])})
        table.put_item(Item={'p': p_mid,  'v': Vector([0.0,  0.0, 0.0])})
        table.put_item(Item={'p': p_low,  'v': Vector([-BIG, 0.0, 0.0])})
        table.update(VectorIndexUpdates=[{'Create':
            {'IndexName': 'vind',
             'VectorAttribute': {'AttributeName': 'v', 'Dimensions': 3},
             'SimilarityFunction': 'DOT_PRODUCT'}}])
        wait_for_vector_index_active(table, 'vind')
        result = table.query(
            IndexName='vind',
            VectorSearch={'QueryVector': Vector([BIG, 0.0, 0.0]),
                          'ReturnScores': 'SIMILARITY'},
            Limit=3,
            Select='ALL_ATTRIBUTES')
        items = result['Items']
        scores = result['Scores']
        assert len(items) == 3
        assert len(scores) == 3
        # Results must be in descending score order (highest dot product first).
        assert [item['p'] for item in items] == [p_high, p_mid, p_low]
        # The highly similar item's score should be large and positive
        assert scores[0] > BIG
        # The mildly similar item's dot product is 0, the "similarity"
        # converts it to 0.5 (since similarity is defined as (dot+1)/2
        # to map from [-1,1] to [0,1] if vectors are normalized).
        assert scores[1] == pytest.approx(0.5, abs=1e-5)
        # The highly dissimilar item's score should be large and negative.
        assert scores[2] < -BIG

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

# TODO: test enabling vector index and Alternator Streams together, and
# checking that Alternator Streams works as expected. Also we may need to
# do something to avoid vector search's favorite parameters like TTL and
# post-changes to take control - or vice versa we may get CDC which isn't
# good enough for vector search.
# Note that today, Alternator Streams only works with vnodes while vector
# search doesn't work with vnodes - so we can't actually check this
# combination! But we must check it when Alternator Streams finally supports
# tablets.
