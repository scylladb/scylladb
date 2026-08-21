# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# Tests for the DynamoDB ExportTableToPointInTime (part of SCYLLADB-1939).

import time

import pytest
from botocore.exceptions import ClientError

from test.alternator.util import create_test_table, is_aws, random_string

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
                # This is run against AWS (see `if` above), which is slow, so no point in testing often
                time.sleep(2)
            else:
                raise
    # Wait until PITR is actually active on DynamoDB.
    while time.time() < deadline:
        resp = client.describe_continuous_backups(TableName=table.name)
        pitr = resp['ContinuousBackupsDescription'].get('PointInTimeRecoveryDescription', {})
        if pitr.get('PointInTimeRecoveryStatus') == 'ENABLED':
            return client
        # This is run against AWS (see `if` above), which is slow, so no point in testing often
        time.sleep(2)
    raise TimeoutError(f"PITR did not become ENABLED on {table.name} within {timeout}s")


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
    assert export_desc['ExportStatus'] == 'FAILED'
    assert export_desc['S3Bucket'] == 'my-test-bucket'
    assert export_desc['S3Prefix'] == 'exports/test'
    assert export_desc['ExportFormat'] == 'DYNAMODB_JSON'
    assert export_desc['ClientToken'] == client_token
    assert export_desc['TableArn'] == table_arn
    assert export_desc['ExportArn'].startswith(f"arn:aws:dynamodb:")


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


# Test that ExportTime close to now is accepted.
# This is a separate test for scylla only, as DynamoDB itself will reject ExportTime close to now.
# For performance reasons in test we don't want to follow the suit with it.
def test_export_table_export_time_now(test_table_s_for_export_only, scylla_only):
    client = test_table_s_for_export_only.meta.client
    table_arn = client.describe_table(TableName=test_table_s_for_export_only.name)['Table']['TableArn']

    response = client.export_table_to_point_in_time(
        TableArn=table_arn,
        S3Bucket='my-bucket',
        ExportTime=int(time.time()),
    )
    assert response['ExportDescription']['ExportStatus'] == 'FAILED'
