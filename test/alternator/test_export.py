# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

import base64
import collections
import requests
import pytest
import boto3
import time
import uuid
import json
import hashlib
import datetime
import gzip
import os

from botocore.exceptions import ClientError
from contextlib import contextmanager

from cassandra.cluster import ConsistencyLevel
from cassandra.query import SimpleStatement
from test.pylib.manager_client import ManagerClient
from test.alternator.util import create_test_table, is_aws, random_string

# NOTE: tests here use `pytest.mark.xfail(reason="Not yet implemented on Scylla")` as xfail marker, even tho the tests
# will pass, when `--aws` is used.

# This creates table with string partition key and no sort key.
# We define it here instead of reusing `conftest.test_table_s_for_export_only`, so:
#  - we're not influenced by other tests using the same table (which in some weird cases could end with performance issues like exporting a lot of data)
#  - we control our usage of the function here (hence the name `_for_export_only`). The tests here are fine doing random things to the table,
#    and tests using `test_table_s_for_export_only` table don't care about it's content as well. We made sure
#    worst case scenario won't result in exporting a lot of data.
@pytest.fixture(scope='module')
def test_table_s_for_export_only(dynamodb):
    table = create_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }, ],
        AttributeDefinitions=[ { 'AttributeName': 'p', 'AttributeType': 'S' } ])
    yield table
    table.delete()

# Helper to get the table ARN from a table object.
def get_table_arn(table):
    desc = table.meta.client.describe_table(TableName=table.name)
    return desc['Table']['TableArn']

# Helper to create a unique S3 bucket name.
def unique_bucket_name():
    return f"alternator-export-test-{uuid.uuid4().hex[:12]}"


# Create an S3 client using the same endpoint configuration as the DynamoDB
# fixture where possible. On AWS, the default S3 client is used. On Scylla,
# we will use MinIo (or something similar capable of pretending S3).
# The code has branch to handle both cases, but the MinIo path is not coverted as
# Scylla implementation as not ready - it's here as a placeholder.
def make_s3_client(dynamodb):
    if is_aws(dynamodb):
        return boto3.client('s3')
    # Placeholder for MinIo configuration for local Scylla testing.
    assert False, "MinIo S3 client configuration for local Scylla testing is not implemented yet"


# Context manager that creates a uniquely-named S3 bucket and deletes it (including all objects) on exit.
@contextmanager
def new_s3_bucket(s3_client, region=None):
    bucket_name = unique_bucket_name()
    kwargs = {'Bucket': bucket_name}
    # us-east-1 does not accept a LocationConstraint - in other words if you want `us-east-1` bucket, you need to omit `LocationConstraint` entirely,
    # in all other cases supply `LocationConstraint` - this is AWS quirk.
    if region and region != 'us-east-1':
        kwargs['CreateBucketConfiguration'] = {'LocationConstraint': region}
    s3_client.create_bucket(**kwargs)
    try:
        yield bucket_name
    finally:
        # Delete all objects before deleting the bucket
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket_name):
            if 'Contents' in page:
                s3_client.delete_objects(
                    Bucket=bucket_name,
                    Delete={'Objects': [{'Key': obj['Key']} for obj in page['Contents']]}
                )
        s3_client.delete_bucket(Bucket=bucket_name)


# Helper: enable PITR on a table (required for ExportTableToPointInTime on
# DynamoDB). Returns the client used.
def enable_pitr(table, timeout=120):
    client = table.meta.client

    # we don't need to call update_continuous_backups on scylladb, because current implementation will work without it
    # and not calling it allows us to avoid implementing the call itself
    # Note: you can call `is_aws` with table, because access pattern internally matches `dynamodb` object.
    if not is_aws(table):
        return client

    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            client.update_continuous_backups(
                TableName=table.name,
                PointInTimeRecoverySpecification={'PointInTimeRecoveryEnabled': True}
            )
            break
        except ClientError as e:
            # After table creation there's a delay until PITR can be enabled (and then another delay
            # until PITR is actually active) - the first delay is notified to the user by ContinuousBackupsUnavailableException
            # exception. So we catch it here and try again.
            if e.response['Error']['Code'] == 'ContinuousBackupsUnavailableException':
                time.sleep(0.1)
            else:
                raise
    # Wait until PITR is actually active — there is a propagation delay on AWS.
    while time.time() < deadline:
        resp = client.describe_continuous_backups(TableName=table.name)
        pitr = resp['ContinuousBackupsDescription'].get('PointInTimeRecoveryDescription', {})
        if pitr.get('PointInTimeRecoveryStatus') == 'ENABLED':
            return client
        time.sleep(1)
    raise TimeoutError(f"PITR did not become ENABLED on {table.name} within {timeout}s")


# Helper: wait for an export to reach a terminal status (COMPLETED or FAILED).
# My actual tests show export takes approximately 10-12 minutes.
def wait_for_export(client, export_arn, timeout=3000):
    deadline = time.time() + timeout
    while time.time() < deadline:
        desc = client.describe_export(ExportArn=export_arn)['ExportDescription']
        status = desc['ExportStatus']
        if status in ('COMPLETED', 'FAILED'):
            return desc
        time.sleep(5)
    raise TimeoutError(f"Export {export_arn} did not complete within {timeout}s")


# Test that ExportTableToPointInTime accepts valid parameters.
def test_export_table_basic(test_table_s_for_export_only):
    client = enable_pitr(test_table_s_for_export_only)
    table_arn = client.describe_table(TableName=test_table_s_for_export_only.name)['Table']['TableArn']
    client_token = random_string(20)

    response = client.export_table_to_point_in_time(
        TableArn=table_arn,
        S3Bucket='my-test-bucket',
        S3Prefix='exports/test',
        ExportFormat='DYNAMODB_JSON',
        ClientToken=client_token,
    )

    assert 'ExportDescription' in response
    export_desc = response['ExportDescription']
    assert export_desc['ExportStatus'] == 'IN_PROGRESS'
    assert export_desc['S3Bucket'] == 'my-test-bucket'
    assert export_desc['S3Prefix'] == 'exports/test'
    assert export_desc['ExportFormat'] == 'DYNAMODB_JSON'
    assert export_desc['ClientToken'] == client_token
    assert export_desc['TableArn'] == table_arn


# Test that ExportTableToPointInTime reports empty S3Prefix (is optional, but must be not empty string if provided).
def test_export_table_s3_prefix_empty(test_table_s_for_export_only):
    client = enable_pitr(test_table_s_for_export_only)
    table_arn = client.describe_table(TableName=test_table_s_for_export_only.name)['Table']['TableArn']

    with pytest.raises(ClientError, match='ValidationException.*[sS]3Prefix'):
        response = client.export_table_to_point_in_time(
            TableArn=table_arn,
            S3Bucket='my-test-bucket',
            S3Prefix='',
            ExportFormat='DYNAMODB_JSON',
        )


# Test that ExportTableToPointInTime reports empty ClientToken (is optional, but must be not empty string if provided).
def test_export_table_client_token_empty(test_table_s_for_export_only):
    client = enable_pitr(test_table_s_for_export_only)
    table_arn = client.describe_table(TableName=test_table_s_for_export_only.name)['Table']['TableArn']

    with pytest.raises(ClientError, match='ValidationException.*[cC]lientToken'):
        response = client.export_table_to_point_in_time(
            TableArn=table_arn,
            S3Bucket='my-test-bucket',
            ExportFormat='DYNAMODB_JSON',
            ClientToken='',
        )


# Test that not supported format is rejected (only DYNAMODB_JSON is supported).
def test_export_table_unsupported_format(dynamodb, test_table_s_for_export_only):
    client = enable_pitr(test_table_s_for_export_only)
    table_arn = client.describe_table(TableName=test_table_s_for_export_only.name)['Table']['TableArn']

    with pytest.raises(ClientError, match='ValidationException.*[eE]xportFormat'):
        client.export_table_to_point_in_time(
            TableArn=table_arn,
            S3Bucket='my-bucket',
            ExportFormat='QWERTY',
        )


# Test that export format being empty string is rejected.
def test_export_table_empty_format(dynamodb, test_table_s_for_export_only):
    client = enable_pitr(test_table_s_for_export_only)
    table_arn = client.describe_table(TableName=test_table_s_for_export_only.name)['Table']['TableArn']

    with pytest.raises(ClientError, match='ValidationException.*[eE]xportFormat'):
        client.export_table_to_point_in_time(
            TableArn=table_arn,
            S3Bucket='my-bucket',
            ExportFormat='',
        )


# Test that non-DYNAMODB_JSON format (ION) is rejected.
def test_export_table_unsupported_format_ion(dynamodb, test_table_s_for_export_only, scylla_only):
    client = enable_pitr(test_table_s_for_export_only)
    table_arn = client.describe_table(TableName=test_table_s_for_export_only.name)['Table']['TableArn']

    with pytest.raises(ClientError, match='ValidationException.*[eE]xportFormat'):
        client.export_table_to_point_in_time(
            TableArn=table_arn,
            S3Bucket='my-bucket',
            ExportFormat='ION',
        )


# Test that empty string as export type is rejected
def test_export_table_export_type_empty(dynamodb, test_table_s_for_export_only):
    client = enable_pitr(test_table_s_for_export_only)
    table_arn = client.describe_table(TableName=test_table_s_for_export_only.name)['Table']['TableArn']

    with pytest.raises(ClientError, match='ValidationException.*[eE]xportType'):
        client.export_table_to_point_in_time(
            TableArn=table_arn,
            S3Bucket='my-bucket',
            ExportType='',
        )


# Test that not supported export type is rejected.
def test_export_table_export_type_unsupported(dynamodb, test_table_s_for_export_only):
    client = enable_pitr(test_table_s_for_export_only)
    table_arn = client.describe_table(TableName=test_table_s_for_export_only.name)['Table']['TableArn']

    with pytest.raises(ClientError, match='ValidationException.*[eE]xportType'):
        client.export_table_to_point_in_time(
            TableArn=table_arn,
            S3Bucket='my-bucket',
            ExportType='QWERTY',
        )


# Test that incremental export is rejected.
def test_export_table_unsupported_incremental(dynamodb, test_table_s_for_export_only, scylla_only):
    client = enable_pitr(test_table_s_for_export_only)
    table_arn = client.describe_table(TableName=test_table_s_for_export_only.name)['Table']['TableArn']
    
    with pytest.raises(ClientError, match='ValidationException.*[eE]xportType'):
        client.export_table_to_point_in_time(
            TableArn=table_arn,
            S3Bucket='my-bucket',
            ExportType='INCREMENTAL_EXPORT',
        )


# Test that IncrementalExportSpecification is rejected.
def test_export_table_unsupported_incremental_spec(dynamodb, test_table_s_for_export_only, scylla_only):
    client = enable_pitr(test_table_s_for_export_only)
    table_arn = client.describe_table(TableName=test_table_s_for_export_only.name)['Table']['TableArn']

    with pytest.raises(ClientError, match='ValidationException.*[iI]ncrementalExportSpecification'):
        client.export_table_to_point_in_time(
            TableArn=table_arn,
            S3Bucket='my-bucket',
            IncrementalExportSpecification={
                'ExportFromTime': int(time.time()) - 3600,
                'ExportToTime': int(time.time()),
                'ExportViewType': 'NEW_IMAGE',
            },
        )


# Test that ExportTime far in the past is rejected.
def test_export_table_invalid_export_time(test_table_s_for_export_only):
    client = enable_pitr(test_table_s_for_export_only)
    table_arn = client.describe_table(TableName=test_table_s_for_export_only.name)['Table']['TableArn']

    # ExportTime before table construction should be rejected (we use 7 days)
    # Expected error message from DynamoDB:
    #    An error occurred (InvalidExportTimeException) when calling the ExportTableToPointInTime operation: Export Time is invalid
    with pytest.raises(ClientError, match='InvalidExportTimeException.*Export ?Time'):
        client.export_table_to_point_in_time(
            TableArn=table_arn,
            S3Bucket='my-bucket',
            ExportTime=int(time.time()) - 3600 * 24 * 7, # 7 days, because the table might have been created some time ago
        )


# Test that missing ExportTime is accepted
def test_export_table_missing_export_time(test_table_s_for_export_only):
    client = enable_pitr(test_table_s_for_export_only)
    table_arn = client.describe_table(TableName=test_table_s_for_export_only.name)['Table']['TableArn']

    client.export_table_to_point_in_time(
        TableArn=table_arn,
        S3Bucket='my-bucket',
    )

# Test that client token is accepted and returned in response.
def test_export_table_client_token(test_table_s_for_export_only):
    client = enable_pitr(test_table_s_for_export_only)
    table_arn = client.describe_table(TableName=test_table_s_for_export_only.name)['Table']['TableArn']

    response = client.export_table_to_point_in_time(
        TableArn=table_arn,
        S3Bucket='my-bucket',
        ClientToken='unique-idempotency-token',
    )

    assert response['ExportDescription']['ClientToken'] == 'unique-idempotency-token'


# Test that ExportTime close to now is accepted.
def test_export_table_export_time_now(test_table_s_for_export_only):
    client = enable_pitr(test_table_s_for_export_only)
    table_arn = client.describe_table(TableName=test_table_s_for_export_only.name)['Table']['TableArn']

    response = client.export_table_to_point_in_time(
        TableArn=table_arn,
        S3Bucket='my-bucket',
        ExportTime=int(time.time()),
    )
    assert response['ExportDescription']['ExportStatus'] == 'IN_PROGRESS'


# Test that missing TableArn is rejected.
def test_export_table_missing_table_arn(test_table_s_for_export_only):
    client = enable_pitr(test_table_s_for_export_only)

    with pytest.raises(ClientError, match='ValidationException.*[tT]ableArn'):
        client.export_table_to_point_in_time(
            TableArn='',
            S3Bucket='my-bucket',
        )


# Test that missing S3Bucket is rejected.
def test_export_table_missing_s3_bucket(test_table_s_for_export_only):
    client = enable_pitr(test_table_s_for_export_only)
    table_arn = client.describe_table(TableName=test_table_s_for_export_only.name)['Table']['TableArn']

    with pytest.raises(ClientError, match='ValidationException.*[sS]3Bucket'):
        client.export_table_to_point_in_time(
            TableArn=table_arn,
            S3Bucket='',
        )

# Test that missing S3Bucket is rejected.
@pytest.mark.xfail(reason="Not yet implemented on Scylla - needs DescribeExport to work")
def test_export_table_nonexistant_s3_bucket(test_table_s_for_export_only):
    client = enable_pitr(test_table_s_for_export_only)
    table_arn = client.describe_table(TableName=test_table_s_for_export_only.name)['Table']['TableArn']
    fake_bucket = unique_bucket_name()

    # this one starts on Amazon, so we follow the suit
    desc = client.export_table_to_point_in_time(
        TableArn=table_arn,
        S3Bucket=fake_bucket,
    )
    final = wait_for_export(client, desc['ExportDescription']['ExportArn'])
    assert final['ExportStatus'] == 'FAILED'
    assert final['FailureCode'] == 'S3NoSuchBucket'
    assert 'bucket does not exist' in final['FailureMessage']

# Test that the internal system-distributed tables for alternator export to S3 exist and are queryable.
@pytest.mark.parametrize("table_name", ['alternator_export_to_s3_exports', 'alternator_export_to_s3_client_tokens'])
def test_export_to_s3_checks_if_internal_tables_exist(cql, table_name):
    statement = SimpleStatement(f"SELECT * FROM system_distributed.{table_name} LIMIT 1", consistency_level = ConsistencyLevel.ONE)
    # we don't care about the results, we just want to make sure the query doesn't throw an exception (ie it reads anything even if table is empty)
    cql.execute(statement)
