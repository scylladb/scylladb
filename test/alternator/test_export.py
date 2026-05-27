# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# Tests for the DynamoDB ExportTableToPointInTime, DescribeExport, and
# ListExports APIs (part of SCYLLADB-1939).

import time
import uuid

import pytest
from botocore.exceptions import ClientError

from test.alternator.util import create_test_table, is_aws, random_string
from test.pylib.skip_types import skip_env

# NOTE: tests here use `pytest.mark.xfail(reason="Not yet implemented on Scylla")` as xfail marker, even though the tests
# will pass, when `--aws` is used.

# This creates an empty table with string partition key and no sort key. We do
# not use the shared test_table_s fixture because exports may take longer for a
# table with data written by other tests.
@pytest.fixture(scope='module')
def test_table_s_for_export_only(dynamodb):
    table = create_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }, ],
        AttributeDefinitions=[ { 'AttributeName': 'p', 'AttributeType': 'S' } ])
    enable_pitr(table)
    yield table
    table.delete()

# Helper to create a unique S3 bucket name.
def unique_bucket_name():
    return f"alternator-export-test-{uuid.uuid4().hex[:12]}"


# Helper: enable PITR on a table (required for ExportTableToPointInTime on
# DynamoDB). Returns the client used.
def enable_pitr(table, timeout=120):
    client = table.meta.client

    # We don't need to call update_continuous_backups on ScyllaDB, because current implementation will work without it
    # and not calling it allows us to avoid implementing the call itself.
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
    # Wait until PITR is actually active on DynamoDB.
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
        time.sleep(0.25)
    raise TimeoutError(f"Export {export_arn} did not complete within {timeout}s")


@pytest.fixture(scope='module')
def veryslow_on_aws(dynamodb, request):
    if is_aws(dynamodb) and not request.config.getoption('runveryslow'):
        skip_env('need --runveryslow option to run')


# Test that ExportTableToPointInTime accepts valid parameters.
# for scylla test we don't want to pass real S3 bucket (as we don't yet have a minio infrastructure to supply one for scylla and we want to
# avoid creating real S3 buckets if possible) - thus we're making this test `scylla_only`.
# In future it will be updated to use a minio bucket and `scylla_only` marker will be removed.
def test_export_table_basic(test_table_s_for_export_only, scylla_only):
    client = test_table_s_for_export_only.meta.client
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
    assert export_desc['ExportArn'].startswith(f"arn:aws:dynamodb:")


# Test that ExportTableToPointInTime reports empty S3Prefix (is optional, but must be not empty string if provided).
def test_export_table_s3_prefix_empty(test_table_s_for_export_only):
    client = test_table_s_for_export_only.meta.client
    table_arn = client.describe_table(TableName=test_table_s_for_export_only.name)['Table']['TableArn']

    with pytest.raises(ClientError, match='ValidationException.*[sS]3Prefix'):
         client.export_table_to_point_in_time(
            TableArn=table_arn,
            S3Bucket='my-test-bucket',
            S3Prefix='',
            ExportFormat='DYNAMODB_JSON',
        )


# Test that ExportTableToPointInTime reports empty ClientToken (is optional, but must be not empty string if provided).
def test_export_table_client_token_empty(test_table_s_for_export_only):
    client = test_table_s_for_export_only.meta.client
    table_arn = client.describe_table(TableName=test_table_s_for_export_only.name)['Table']['TableArn']

    with pytest.raises(ClientError, match='ValidationException.*[cC]lientToken'):
         client.export_table_to_point_in_time(
            TableArn=table_arn,
            S3Bucket='my-test-bucket',
            ExportFormat='DYNAMODB_JSON',
            ClientToken='',
        )


# Test that not supported format is rejected (only DYNAMODB_JSON is supported).
def test_export_table_unsupported_format(dynamodb, test_table_s_for_export_only):
    client = test_table_s_for_export_only.meta.client
    table_arn = client.describe_table(TableName=test_table_s_for_export_only.name)['Table']['TableArn']

    with pytest.raises(ClientError, match='ValidationException.*[eE]xportFormat'):
        client.export_table_to_point_in_time(
            TableArn=table_arn,
            S3Bucket='my-bucket',
            ExportFormat='QWERTY',
        )


# Test that export format being empty string is rejected.
def test_export_table_empty_format(dynamodb, test_table_s_for_export_only):
    client = test_table_s_for_export_only.meta.client
    table_arn = client.describe_table(TableName=test_table_s_for_export_only.name)['Table']['TableArn']

    with pytest.raises(ClientError, match='ValidationException.*[eE]xportFormat'):
        client.export_table_to_point_in_time(
            TableArn=table_arn,
            S3Bucket='my-bucket',
            ExportFormat='',
        )


# Test that non-DYNAMODB_JSON format (ION) is rejected.
def test_export_table_unsupported_format_ion(dynamodb, test_table_s_for_export_only, scylla_only):
    client = test_table_s_for_export_only.meta.client
    table_arn = client.describe_table(TableName=test_table_s_for_export_only.name)['Table']['TableArn']

    with pytest.raises(ClientError, match='ValidationException.*[eE]xportFormat'):
        client.export_table_to_point_in_time(
            TableArn=table_arn,
            S3Bucket='my-bucket',
            ExportFormat='ION',
        )


# Test that empty string as export type is rejected
def test_export_table_export_type_empty(dynamodb, test_table_s_for_export_only):
    client = test_table_s_for_export_only.meta.client
    table_arn = client.describe_table(TableName=test_table_s_for_export_only.name)['Table']['TableArn']

    with pytest.raises(ClientError, match='ValidationException.*[eE]xportType'):
        client.export_table_to_point_in_time(
            TableArn=table_arn,
            S3Bucket='my-bucket',
            ExportType='',
        )


# Test that not supported export type is rejected.
def test_export_table_export_type_unsupported(dynamodb, test_table_s_for_export_only):
    client = test_table_s_for_export_only.meta.client
    table_arn = client.describe_table(TableName=test_table_s_for_export_only.name)['Table']['TableArn']

    with pytest.raises(ClientError, match='ValidationException.*[eE]xportType'):
        client.export_table_to_point_in_time(
            TableArn=table_arn,
            S3Bucket='my-bucket',
            ExportType='QWERTY',
        )


# Test that incremental export is rejected.
def test_export_table_unsupported_incremental(dynamodb, test_table_s_for_export_only, scylla_only):
    client = test_table_s_for_export_only.meta.client
    table_arn = client.describe_table(TableName=test_table_s_for_export_only.name)['Table']['TableArn']

    with pytest.raises(ClientError, match='ValidationException.*[eE]xportType'):
        client.export_table_to_point_in_time(
            TableArn=table_arn,
            S3Bucket='my-bucket',
            ExportType='INCREMENTAL_EXPORT',
        )


# Test that IncrementalExportSpecification is rejected.
def test_export_table_unsupported_incremental_spec(dynamodb, test_table_s_for_export_only, scylla_only):
    client = test_table_s_for_export_only.meta.client
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


@pytest.mark.parametrize('unsupported_parameter, value', [
    ('S3BucketOwner', '123456789012'),
    ('S3SseAlgorithm', 'AES256'),
    ('S3SseKmsKeyId', 'test-key-id'),
])
def test_export_table_unsupported_s3_options(test_table_s_for_export_only, scylla_only, unsupported_parameter, value):
    client = test_table_s_for_export_only.meta.client
    table_arn = client.describe_table(TableName=test_table_s_for_export_only.name)['Table']['TableArn']

    with pytest.raises(ClientError, match=f'ValidationException.*{unsupported_parameter}'):
        client.export_table_to_point_in_time(
            TableArn=table_arn,
            S3Bucket='my-bucket',
            **{unsupported_parameter: value},
        )


# Test that ExportTime far in the past is rejected.
def test_export_table_invalid_export_time(test_table_s_for_export_only):
    client = test_table_s_for_export_only.meta.client
    table_arn = client.describe_table(TableName=test_table_s_for_export_only.name)['Table']['TableArn']

    # This is a new test table, so it did not exist 7 days ago and this export time is invalid.
    with pytest.raises(ClientError, match='InvalidExportTimeException.*Export ?Time'):
        client.export_table_to_point_in_time(
            TableArn=table_arn,
            S3Bucket='my-bucket',
            ExportTime=int(time.time()) - 3600 * 24 * 7,
        )


# Test that missing ExportTime is accepted
def test_export_table_missing_export_time(test_table_s_for_export_only):
    client = test_table_s_for_export_only.meta.client
    table_arn = client.describe_table(TableName=test_table_s_for_export_only.name)['Table']['TableArn']

    client.export_table_to_point_in_time(
        TableArn=table_arn,
        S3Bucket='my-bucket',
    )

# Test that client token is accepted and returned in response.
def test_export_table_client_token(test_table_s_for_export_only):
    client = test_table_s_for_export_only.meta.client
    table_arn = client.describe_table(TableName=test_table_s_for_export_only.name)['Table']['TableArn']

    response = client.export_table_to_point_in_time(
        TableArn=table_arn,
        S3Bucket='my-bucket',
        ClientToken='unique-idempotency-token',
    )

    assert response['ExportDescription']['ClientToken'] == 'unique-idempotency-token'


# Test that client token is omitted from response when it is absent in request.
def test_export_table_without_client_token(test_table_s_for_export_only):
    client = test_table_s_for_export_only.meta.client
    table_arn = client.describe_table(TableName=test_table_s_for_export_only.name)['Table']['TableArn']

    response = client.export_table_to_point_in_time(
        TableArn=table_arn,
        S3Bucket='my-bucket',
    )

    # AWS, when export is called without client token, will return uniquely generated client token in response.
    # We check that we do that as well here.
    assert 'ClientToken' in response['ExportDescription']


# Test that ExportTime close to now is accepted.
def test_export_table_export_time_now(test_table_s_for_export_only):
    client = test_table_s_for_export_only.meta.client
    table_arn = client.describe_table(TableName=test_table_s_for_export_only.name)['Table']['TableArn']

    response = client.export_table_to_point_in_time(
        TableArn=table_arn,
        S3Bucket='my-bucket',
        ExportTime=int(time.time()),
    )
    assert response['ExportDescription']['ExportStatus'] == 'IN_PROGRESS'


# Test that missing TableArn is rejected.
def test_export_table_empty_table_arn(test_table_s_for_export_only):
    client = test_table_s_for_export_only.meta.client

    with pytest.raises(ClientError, match='ValidationException.*[tT]ableArn'):
        client.export_table_to_point_in_time(
            TableArn='',
            S3Bucket='my-bucket',
        )


# Test that missing S3Bucket is rejected.
def test_export_table_empty_s3_bucket(test_table_s_for_export_only):
    client = test_table_s_for_export_only.meta.client
    table_arn = client.describe_table(TableName=test_table_s_for_export_only.name)['Table']['TableArn']

    with pytest.raises(ClientError, match='ValidationException.*[sS]3Bucket'):
        client.export_table_to_point_in_time(
            TableArn=table_arn,
            S3Bucket='',
        )
