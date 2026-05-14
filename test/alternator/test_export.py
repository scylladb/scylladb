# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

import base64
import collections
import requests
# Tests for the DynamoDB ExportTableToPointInTime, DescribeExport, and
# ListExports APIs (part of SCYLLADB-1939).

import base64
import collections

import pytest
import boto3
import time
import uuid
import json
import hashlib
import datetime
import gzip
import os
import decimal

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
from test.alternator.util import create_test_table, is_aws, new_test_table

# NOTE: tests here use `pytest.mark.xfail(reason="Not yet implemented on Scylla")` as xfail marker as the implementation is ongoing.
# The tests will pass against AWS.

# This creates table with string partition key and no sort key. We define it here instead of reusing `conftest.test_table_s_for_export_only`
# because we want to control table's content - since the table is expected to be used only from this file we have approximate control over
# what will be inside.
@pytest.fixture(scope='module')
def test_table_s_for_export_only(dynamodb):
    table = create_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }, ],
        AttributeDefinitions=[ { 'AttributeName': 'p', 'AttributeType': 'S' } ])
    yield table
    table.delete()

@pytest.fixture(scope='module')
def test_table_s_2_for_export_only(dynamodb):
    table = create_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }, ],
        AttributeDefinitions=[ { 'AttributeName': 'p', 'AttributeType': 'S' } ])
    yield table
    table.delete()

# Returns True if we're running against AWS, but gets info based on the table object.
def is_aws_from_client(client):
    return client._endpoint.host.endswith('.amazonaws.com')

# Returns True if we're running against AWS, but gets info based on the table object.
def is_aws_from_table(table):
    return is_aws_from_client(table.meta.client)

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
# The code has branch to handle both cases, but the MinIo path is not covered as
# Scylla implementation is not ready - it's here as a placeholder.
def make_s3_client(dynamodb):
    if is_aws(dynamodb):
        return boto3.client('s3')
    # Placeholder for MinIo configuration for local Scylla testing.
    assert False, "MinIo S3 client configuration for local Scylla testing is not implemented yet"


# Context manager that creates a uniquely-named S3 bucket and deletes it (including all objects) on exit.
@contextmanager
def new_s3_bucket(s3_client):
    bucket_name = unique_bucket_name()
    region = s3_client.meta.region_name
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
    if not is_aws_from_table(table):
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
# My actual tests show export takes approximately 10-12 minutes on AWS hence hefty timeout.
def wait_for_export(client, export_arn):
    timeout = 60 * 60 if is_aws_from_client(client) else 60
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
# ---------------------------------------------------------------------------
# ExportTableToPointInTime tests
# ---------------------------------------------------------------------------

# Test that ExportTableToPointInTime starts an export and returns an
# ExportDescription with expected fields.
@pytest.mark.xfail(reason="Not yet implemented on Scylla")
def test_export_basic(dynamodb, test_table_s_for_export_only):
    table = test_table_s_for_export_only
    client = enable_pitr(table)
    table_arn = get_table_arn(table)
    s3 = make_s3_client(dynamodb)

    with new_s3_bucket(s3) as bucket:
        response = client.export_table_to_point_in_time(
            TableArn=table_arn,
            S3Bucket=bucket,
        )
        desc = response['ExportDescription']
        assert 'ExportArn' in desc
        assert desc['ExportStatus'] in ('IN_PROGRESS', 'COMPLETED')
        assert desc['TableArn'] == table_arn
        assert desc['S3Bucket'] == bucket
        assert desc['ExportFormat'] == 'DYNAMODB_JSON'  # default format is DYNAMODB_JSON
        assert desc['ExportType'] == 'FULL_EXPORT'  # default format is DYNAMODB_JSON
        # Wait for completion and verify
        final = wait_for_export(client, desc['ExportArn'])
        assert final['ExportStatus'] == 'COMPLETED'


# Test ExportTableToPointInTime with an S3 prefix.
@pytest.mark.xfail(reason="Not yet implemented on Scylla")
def test_export_with_prefix(dynamodb, test_table_s_for_export_only):
    table = test_table_s_for_export_only
    client = enable_pitr(table)
    table_arn = get_table_arn(table)
    s3 = make_s3_client(dynamodb)
    prefix = 'test-export-prefix/dir/dir2'

    with new_s3_bucket(s3) as bucket:
        response = client.export_table_to_point_in_time(
            TableArn=table_arn,
            S3Bucket=bucket,
            S3Prefix=prefix,
        )
        desc = response['ExportDescription']
        assert desc['S3Prefix'] == prefix
        final = wait_for_export(client, desc['ExportArn'])
        assert final['ExportStatus'] == 'COMPLETED'
        assert final['S3Prefix'] == prefix
        root_dir = fetch_all_exported_items(s3, bucket)
        def dump_dir(root_dir, prefix=''):
            for name, content in root_dir.items():
                print(f"{prefix}{name} {'(file)' if type(content) is bytes else '(dir)'}")
                if type(content) is dict:
                    dump_dir(content, prefix + '  ')
        dump_dir(root_dir)
        n = root_dir
        path_to_find1 = os.path.join(prefix, 'AWSDynamoDB')
        for p in path_to_find1.split('/'):
            n = n.get(p, None)
            assert type(n) is dict, root_dir
        # we need to skip unique-string part.
        assert len(n) == 1, root_dir
        n = next(iter(n.values()))
        assert type(n) is dict, root_dir
        n = n.get('_started', None)
        # we found _started file were we expected it, we assume rest of directory is fine.
        assert type(n) is bytes, root_dir

# Test that ExportTableToPointInTime with DYNAMODB_JSON format correctly parses option and completes.
@pytest.mark.xfail(reason="Not yet implemented on Scylla")
def test_export_dynamodb_json_format(dynamodb, test_table_s_for_export_only):
    table = test_table_s_for_export_only
    client = enable_pitr(table)
    table_arn = get_table_arn(table)
    s3 = make_s3_client(dynamodb)

    with new_s3_bucket(s3) as bucket:
        response = client.export_table_to_point_in_time(
            TableArn=table_arn,
            S3Bucket=bucket,
            ExportFormat='DYNAMODB_JSON',
        )
        desc = response['ExportDescription']
        assert desc.get('ExportFormat') == 'DYNAMODB_JSON'
        final = wait_for_export(client, desc['ExportArn'])
        assert final['ExportStatus'] == 'COMPLETED'


# Test that ExportTableToPointInTime with ION format correctly parses option and completes.
@pytest.mark.xfail(reason="Not yet implemented on Scylla")
def test_export_ion_format(dynamodb, test_table_s_for_export_only):
    table = test_table_s_for_export_only
    client = enable_pitr(table)
    table_arn = get_table_arn(table)
    s3 = make_s3_client(dynamodb)

    with new_s3_bucket(s3) as bucket:
        response = client.export_table_to_point_in_time(
            TableArn=table_arn,
            S3Bucket=bucket,
            ExportFormat='ION',
        )
        desc = response['ExportDescription']
        assert desc.get('ExportFormat') == 'ION'
        final = wait_for_export(client, desc['ExportArn'])
        assert final['ExportStatus'] == 'COMPLETED'


# Test that ExportTableToPointInTime with ION format correctly parses option and completes.
@pytest.mark.xfail(reason="Not yet implemented on Scylla")
def test_export_unsupported_format(dynamodb, test_table_s_for_export_only):
    table = test_table_s_for_export_only
    client = enable_pitr(table)
    table_arn = get_table_arn(table)
    s3 = make_s3_client(dynamodb)

    with new_s3_bucket(s3) as bucket:
        with pytest.raises(ClientError, match='ValidationException.*exportFormat.*failed'):
            response = client.export_table_to_point_in_time(
                TableArn=table_arn,
                S3Bucket=bucket,
                ExportFormat='QWERTY',
            )


# Test that ExportTableToPointInTime with ION format correctly parses option and completes.
@pytest.mark.xfail(reason="Not yet implemented on Scylla")
def test_export_missing_format(dynamodb, test_table_s_for_export_only):
    table = test_table_s_for_export_only
    client = enable_pitr(table)
    table_arn = get_table_arn(table)
    s3 = make_s3_client(dynamodb)

    with new_s3_bucket(s3) as bucket:
        with pytest.raises(ClientError, match='ValidationException.*exportFormat.*failed'):
            response = client.export_table_to_point_in_time(
                TableArn=table_arn,
                S3Bucket=bucket,
                ExportFormat='',
            )


# Test that an export of a table with data completes and reports a
# correct ItemCount. The `_full` test will do a full S3 download and verify all exported data.
@pytest.mark.xfail(reason="Not yet implemented on Scylla")
def test_export_with_data(dynamodb):
    with new_test_table(dynamodb,
        KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
    ) as table:
        # Insert some items
        num_items = 10
        for i in range(num_items):
            table.put_item(Item={'p': f'item{i}', 'data': f'value{i}'})

        client = enable_pitr(table)
        table_arn = get_table_arn(table)
        s3 = make_s3_client(dynamodb)

        with new_s3_bucket(s3) as bucket:
            response = client.export_table_to_point_in_time(
                TableArn=table_arn,
                S3Bucket=bucket,
            )
            final = wait_for_export(client, response['ExportDescription']['ExportArn'])
            assert final['ExportStatus'] == 'COMPLETED'
            assert final['ItemCount'] == num_items


def fetch_all_exported_items(s3, bucket):
    result = {}
    response = s3.list_objects_v2(Bucket=bucket)
    objects = response.get('Contents', [])
    while response.get('IsTruncated'):
        response = s3.list_objects_v2(Bucket=bucket,
                                      ContinuationToken=response['NextContinuationToken'])
        objects.extend(response.get('Contents', []))
    for obj in objects:
        key = obj['Key']
        # Strip the prefix to get the relative path
        rel_path = key.lstrip('/')
        parts = rel_path.split('/')
        node = result
        for part in parts[:-1]:
            if part not in node:
                node[part] = {}
            node = node[part]
        # Leaf: fetch content as bytes
        content = s3.get_object(Bucket=bucket, Key=key)['Body'].read()
        node[parts[-1]] = content
    return result

# Test that an export of a table with data completes using DYNAMODB_JSON format.
# Test will fetch S3 files and verify that they contain expected items and that manifest files are correct.
@pytest.mark.xfail(reason="Not yet implemented on Scylla")
def test_export_with_data_json_full(dynamodb):
    with new_test_table(dynamodb,
        KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
    ) as table:
        # Insert some items
        inserted_items_as_text = []

        num_items = 10
        for i in range(num_items):
            table.put_item(Item={'p': f'item{i}', 
                                 'd01': f'value{i}',
                                 'd02': (i + 10),
                                 'd03': b"qwerty" + str(i).encode('utf-8'),
                                 'd04': { 'a' + str(i), 'b' + str(i), 'c' + str(i) },
                                 'd05': { decimal.Decimal(1.5 + i), decimal.Decimal(2.5 + i), decimal.Decimal(3.875 + i) },
                                 'd06': { b'qwe' + str(i).encode('utf-8'), b'rty' + str(i).encode('utf-8'), b'abc' + str(i).encode('utf-8') },
                                 'd07': { i:i - 10, i + 10: i - 20 },
                                 'd08': { 'nested' + str(i): { 'v1' + str(i): 'v2' + str(i), 'v3' + str(i): 'v4' + str(i) } },
                                 'd09': [ i + 5, i + 10, i + 15 ],
                                 'd10': None,
                                 'd11': bool(i & 1 == 0),
                                 })
        
            # Note: sets are not ordered, so for text-based comparison we will sort them here.
            # The same fields loaded from export files will be sorted as well.
            item = {"Item":{
                'p': { 'S':f'item{i}' },
                'd01': { 'S':f'value{i}' },
                'd02': { 'N':f'{i + 10}' },
                'd03': { 'B': base64.b64encode(b'qwerty' + str(i).encode('utf-8')).decode('utf-8') },
                'd04': { 'SS': list(sorted(( 'a' + str(i), 'b' + str(i), 'c' + str(i) )) ) },
                'd05': { 'NS': list(sorted(( str(decimal.Decimal(1.5 + i)), str(decimal.Decimal(2.5 + i)), str(decimal.Decimal(3.875 + i)) )) ) },
                'd06': { 'BS': list(sorted(( 
                    base64.b64encode(b'qwe' + str(i).encode('utf-8')).decode('utf-8'), 
                    base64.b64encode(b'rty' + str(i).encode('utf-8')).decode('utf-8'), 
                    base64.b64encode(b'abc' + str(i).encode('utf-8')).decode('utf-8')
                )) ) },
                'd07': { 'M': { str(i): { 'N': str(i - 10) }, str(i + 10): { 'N': str(i - 20) } } },
                'd08': { 'M': { 'nested' + str(i): { 'M': { 'v1' + str(i): { 'S': 'v2' + str(i) }, 'v3' + str(i): { 'S': 'v4' + str(i) } } } } },
                'd09': { 'L': [ { 'N': str(i + 5) }, { 'N': str(i + 10) }, { 'N': str(i + 15) } ] },
                'd10': { 'NULL': True },
                'd11': { 'BOOL': bool(i & 1 == 0) },
            }}

            #  we insert items as text, because text lines can be sorted.
            inserted_items_as_text.append(json.dumps(item, indent=None, sort_keys=True))

        client = enable_pitr(table)
        table_arn = get_table_arn(table)
        s3 = make_s3_client(dynamodb)

        with new_s3_bucket(s3) as bucket:
            response = client.export_table_to_point_in_time(
                TableArn=table_arn,
                S3Bucket=bucket,
            )
            final = wait_for_export(client, response['ExportDescription']['ExportArn'])
            assert final['ExportStatus'] == 'COMPLETED'
            assert final['ItemCount'] == num_items

            # Download all exported files as a in-memory dictionary representing a directory structure.
            root_dir = downloaded_root_dir = fetch_all_exported_items(s3, bucket)

            def get_content_by_path(pth):
                if pth.startswith('/'):
                    pth = pth[1:]
                parts = pth.split('/')
                node = downloaded_root_dir
                for part in parts:
                    assert part in node, f"Path {pth} not found in exported files"
                    node = node[part]
                assert type(node) is bytes, f"Path {pth} is expected to be a file but is a {type(node)}"
                return node

            # We're expecting this directory structure:
            # AWSDynamoDB/<some-unique-str>/
            #                    data/
            #                         <unique-string>.json.gz # note - files might be empty
            #                         <unique-string>.json.gz
            #                         <unique-string>.json.gz
            #                         <unique-string>.json.gz
            #                         ...
            #                    manifest-files.json
            #                    manifest-files.md5
            #                    manifest-summary.json
            #                    manifest-summary.md5
            #                    _started
            # Note: this test was run against real AWS export and this structure was observed.

            assert set(root_dir.keys()) == { 'AWSDynamoDB' }
            root_dir = root_dir['AWSDynamoDB']

            # unique-str - we just read files inside (this is used to distinguish different exports into the same path)
            assert len(root_dir) == 1
            root_dir = next(iter(root_dir.values()))

            # top level content directory (the one with `data` and `_started`)
            assert set(root_dir.keys()) == { 'data', 'manifest-files.json', 'manifest-files.md5', 'manifest-summary.json', 'manifest-summary.md5', '_started' }

            data_dir = root_dir['data']
            items = []

            def sort_field(obj, field_path):
                field_list = field_path.split('.')
                for f in field_list[:-1]:
                    obj = obj.get(f, None)
                    if type(obj) is not dict:
                        # we will return here - something is wrong as we can't find our object
                        # but the text comparison assert will fail as well and printed object there should be easier to decipher. 
                        return
                val = obj[field_list[-1]]
                if type(val) is list:
                    val = list(sorted(val))
                    obj[field_list[-1]] = val
            # there might be any number of files and some of them might be empty, we need to read them all to verify that they contain the expected items.
            # we also need to count items in each file to verify manifest file later on.
            item_count_map = collections.defaultdict(int)
            for data_file, data_file_content in data_dir.items():
                assert data_file.endswith('.json.gz')

                content = gzip.decompress(data_file_content).decode('utf-8')
                for line in content.splitlines():
                    line = line.strip()
                    js = json.loads(line)
                    # Note: sets are not ordered, so we will sort them here before text comparison.
                    sort_field(js, 'Item.d04.SS')
                    sort_field(js, 'Item.d05.NS')
                    sort_field(js, 'Item.d06.BS')

                    items.append(json.dumps(js, indent=None, sort_keys=True))
                    item_count_map[data_file] += 1
            # we assert with message here, as pytest will shorten output of `items` and `inserted_items_as_text` if the assert fails and even `-vv` sometimes doesn't help.
            assert set(items) == set(inserted_items_as_text), f"items1: {list(sorted(items))}\nitems2: {list(sorted(inserted_items_as_text))}"

            def calculate_md5(content):
                h = hashlib.md5()
                if type(content) is str:
                    content = content.encode('utf-8')
                h.update(content)
                return base64.b64encode(h.digest())
            
            # two manifest files in `data` directory each have their own md5 file, we need to verify those. 
            for path1, path2 in (
                ('manifest-files.json', 'manifest-files.md5'),
                ('manifest-summary.json', 'manifest-summary.md5'),
            ):
                expected_md5 = root_dir[path2].strip()
                calculated_md5 = calculate_md5(root_dir[path1])
                assert calculated_md5 == expected_md5, f"{calculated_md5} != {expected_md5} for file {path1}"
            
            manifest_files_json = root_dir['manifest-files.json'].decode('utf-8')
            total_real_item_count = 0
            for line in manifest_files_json.splitlines():
                js = json.loads(line.strip())
                assert 'itemCount' in js
                assert 'dataFileS3Key' in js
                assert 'etag' in js
                assert 'md5Checksum' in js

                # path to the data file seems to be relative from bucket root and should start with `AWSDynamoDB/`
                # the path itself is verified by the call to `get_content_by_path` later on.
                p = js['dataFileS3Key']
                assert p.startswith('AWSDynamoDB/')

                # we counted items in each file while reading them, now we verify that the count matches the manifest file.
                real_item_count = item_count_map[os.path.split(p)[1]]
                assert js['itemCount'] == real_item_count
                assert js['etag'] != ''
                total_real_item_count += real_item_count

                # there is md5 file for each data file, we verify that the md5 in manifest matches the calculated md5 of the downloaded file.
                md5_checksum = js['md5Checksum'].encode('utf-8')
                assert md5_checksum != ''
                calculated_md5 = calculate_md5(get_content_by_path(p))
                assert calculated_md5 == md5_checksum, f"{calculated_md5} != {md5_checksum} for file {p}"
            assert total_real_item_count == num_items

            manifest_summary_json = root_dir['manifest-summary.json'].decode('utf-8')
            manifest_summary = json.loads(manifest_summary_json)
            assert 'version' in manifest_summary
            assert 'exportArn' in manifest_summary
            assert manifest_summary['exportArn'] == final['ExportArn']
            assert 'startTime' in manifest_summary
            assert 'endTime' in manifest_summary

            start_time = datetime.datetime.fromisoformat(manifest_summary['startTime'])
            end_time = datetime.datetime.fromisoformat(manifest_summary['endTime'])
            assert start_time < end_time
            # This is only few items on test instance, so 1h seems reasonable.
            assert end_time - start_time < datetime.timedelta(hours=1)

            assert 'tableArn' in manifest_summary
            assert manifest_summary['tableArn'] == table_arn

            assert 'tableId' in manifest_summary
            assert 'exportTime' in manifest_summary

            export_time = datetime.datetime.fromisoformat(manifest_summary['exportTime'])

            # The export time should be close to the start time (certainly within a day, but let's be more strict and require it to be within an hour).
            # It's unclear if export_time must be before (or equal) to start_time, so we don't check it.
            assert start_time - export_time < datetime.timedelta(hours=1)

            assert 's3Bucket' in manifest_summary
            assert manifest_summary['s3Bucket'] == bucket

            assert 's3Prefix' in manifest_summary
            assert 's3SseAlgorithm' in manifest_summary
            assert 's3SseKmsKeyId' in manifest_summary
            assert 'manifestFilesS3Key' in manifest_summary
            assert get_content_by_path(manifest_summary['manifestFilesS3Key']) is root_dir['manifest-files.json']

            assert 'billedSizeBytes' in manifest_summary
            assert manifest_summary['billedSizeBytes'] >= 0

            assert 'itemCount' in manifest_summary
            assert manifest_summary['itemCount'] == num_items

            assert 'outputFormat' in manifest_summary
            assert manifest_summary['outputFormat'] == 'DYNAMODB_JSON'

# Test that sending the same ClientToken twice returns the same export.
@pytest.mark.xfail(reason="Not yet implemented on Scylla")
def test_export_client_token_idempotent(dynamodb, test_table_s_for_export_only):
    table = test_table_s_for_export_only
    client = enable_pitr(table)
    table_arn = get_table_arn(table)
    s3 = make_s3_client(dynamodb)
    token = str(uuid.uuid4())

    with new_s3_bucket(s3) as bucket:
        resp1 = client.export_table_to_point_in_time(
            TableArn=table_arn,
            S3Bucket=bucket,
            ClientToken=token,
        )
        resp2 = client.export_table_to_point_in_time(
            TableArn=table_arn,
            S3Bucket=bucket,
            ClientToken=token,
        )
        assert resp1['ExportDescription']['ExportArn'] == resp2['ExportDescription']['ExportArn']
        wait_for_export(client, resp1['ExportDescription']['ExportArn'])


# Following tests check idempotency of export with respect to ClientToken - they verify that
# if we change some parameter of the export but keep the same ClientToken,
# the second call will fail with ExportConflictException.
# Note: we postpone test for `S3SseKmsKeyId` (due to complexity of setting up KMS)
@pytest.mark.xfail(reason="Not yet implemented on Scylla")
def test_export_client_token_changed_bucket(dynamodb, test_table_s_for_export_only, test_table_s_2_for_export_only):
    client1 = enable_pitr(test_table_s_for_export_only)
    table_arn1 = get_table_arn(test_table_s_for_export_only)
    client2 = enable_pitr(test_table_s_2_for_export_only)
    table_arn2 = get_table_arn(test_table_s_2_for_export_only)
    s3 = make_s3_client(dynamodb)
    token = str(uuid.uuid4())

    with new_s3_bucket(s3) as bucket:
        client1.export_table_to_point_in_time(
            TableArn=table_arn1,
            S3Bucket=bucket,
            ClientToken=token,
        )
        with pytest.raises(ClientError, match='ExportConflictException.*Duplicate request detected'):
            client2.export_table_to_point_in_time(
                TableArn=table_arn2,
                S3Bucket=bucket + 'A',
                ClientToken=token,
            )

# Test that trying to export the table twice using the same client token but different export formats fails with ExportConflictException.
@pytest.mark.xfail(reason="Not yet implemented on Scylla")
def test_export_client_token_changed_export_format(dynamodb, test_table_s_for_export_only, test_table_s_2_for_export_only):
    client1 = enable_pitr(test_table_s_for_export_only)
    table_arn1 = get_table_arn(test_table_s_for_export_only)
    client2 = enable_pitr(test_table_s_2_for_export_only)
    table_arn2 = get_table_arn(test_table_s_2_for_export_only)
    s3 = make_s3_client(dynamodb)
    token = str(uuid.uuid4())

    with new_s3_bucket(s3) as bucket:
        client1.export_table_to_point_in_time(
            TableArn=table_arn1,
            S3Bucket=bucket,
            ExportFormat='DYNAMODB_JSON',
            ClientToken=token,
        )
        with pytest.raises(ClientError, match='ExportConflictException.*Duplicate request detected'):
            client2.export_table_to_point_in_time(
                TableArn=table_arn2,
                S3Bucket=bucket,
                ExportFormat='ION',
                ClientToken=token,
                )


# Test that trying to export the table twice using the same client token but different export times fails with ExportConflictException.
@pytest.mark.xfail(reason="Not yet implemented on Scylla")
def test_export_client_token_changed_export_time(dynamodb, test_table_s_for_export_only, test_table_s_2_for_export_only):
    client1 = enable_pitr(test_table_s_for_export_only)
    table_arn1 = get_table_arn(test_table_s_for_export_only)
    client2 = enable_pitr(test_table_s_2_for_export_only)
    table_arn2 = get_table_arn(test_table_s_2_for_export_only)
    s3 = make_s3_client(dynamodb)
    token = str(uuid.uuid4())
    export_time = int(datetime.datetime.now().timestamp())

    with new_s3_bucket(s3) as bucket:
        client1.export_table_to_point_in_time(
            TableArn=table_arn1,
            S3Bucket=bucket,
            ExportTime=export_time,
            ClientToken=token,
        )
        with pytest.raises(ClientError, match='ExportConflictException.*Duplicate request detected'):
            client2.export_table_to_point_in_time(
                TableArn=table_arn2,
                S3Bucket=bucket,
                ExportTime=export_time + 1,
                ClientToken=token,
                )


# Test that trying to export the table twice using the same client token but different bucket owners fails with ExportConflictException.
@pytest.mark.xfail(reason="Not yet implemented on Scylla")
def test_export_client_token_changed_bucket_owner(dynamodb, test_table_s_for_export_only, test_table_s_2_for_export_only):
    client1 = enable_pitr(test_table_s_for_export_only)
    table_arn1 = get_table_arn(test_table_s_for_export_only)
    client2 = enable_pitr(test_table_s_2_for_export_only)
    table_arn2 = get_table_arn(test_table_s_2_for_export_only)
    s3 = make_s3_client(dynamodb)
    token = str(uuid.uuid4())

    with new_s3_bucket(s3) as bucket:
        client1.export_table_to_point_in_time(
            TableArn=table_arn1,
            S3Bucket=bucket,
            ClientToken=token,
        )
        with pytest.raises(ClientError, match='ExportConflictException.*Duplicate request detected'):
            client2.export_table_to_point_in_time(
                TableArn=table_arn2,
                S3Bucket=bucket,
                ClientToken=token,
                S3BucketOwner='000000000000',
                )


# Test that trying to export the table twice using the same client token but different S3 prefixes fails with ExportConflictException.
@pytest.mark.xfail(reason="Not yet implemented on Scylla")
def test_export_client_token_changed_s3_prefix(dynamodb, test_table_s_for_export_only, test_table_s_2_for_export_only):
    client1 = enable_pitr(test_table_s_for_export_only)
    table_arn1 = get_table_arn(test_table_s_for_export_only)
    client2 = enable_pitr(test_table_s_2_for_export_only)
    table_arn2 = get_table_arn(test_table_s_2_for_export_only)
    s3 = make_s3_client(dynamodb)
    token = str(uuid.uuid4())

    with new_s3_bucket(s3) as bucket:
        client1.export_table_to_point_in_time(
            TableArn=table_arn1,
            S3Bucket=bucket,
            ClientToken=token,
        )
        with pytest.raises(ClientError, match='ExportConflictException.*Duplicate request detected'):
            client2.export_table_to_point_in_time(
                TableArn=table_arn2,
                S3Bucket=bucket,
                ClientToken=token,
                S3Prefix='s3-prefix',
                )


# Test that trying to export the table twice using the same client token but different S3 SSE algorithms fails with ExportConflictException.
@pytest.mark.xfail(reason="Not yet implemented on Scylla")
def test_export_client_token_changed_s3_sse_algo(dynamodb, test_table_s_for_export_only, test_table_s_2_for_export_only):
    client1 = enable_pitr(test_table_s_for_export_only)
    table_arn1 = get_table_arn(test_table_s_for_export_only)
    client2 = enable_pitr(test_table_s_2_for_export_only)
    table_arn2 = get_table_arn(test_table_s_2_for_export_only)
    s3 = make_s3_client(dynamodb)
    token = str(uuid.uuid4())

    with new_s3_bucket(s3) as bucket:
        client1.export_table_to_point_in_time(
            TableArn=table_arn1,
            S3Bucket=bucket,
            ClientToken=token,
        )
        with pytest.raises(ClientError, match='ExportConflictException.*Duplicate request detected'):
            client2.export_table_to_point_in_time(
                TableArn=table_arn2,
                S3Bucket=bucket,
                ClientToken=token,
                S3SseAlgorithm='AES256',
                )


# Test that trying to export the table with incremental export specifications times (ExportFromTime and ExportToTime) too close to each other -
# (less than 15 minutes) - should fail with InvalidExportTimeException
@pytest.mark.xfail(reason="Not yet implemented on Scylla")
def test_export_incremental_export_time_too_close(dynamodb, test_table_s_for_export_only):
    client1 = enable_pitr(test_table_s_for_export_only)
    table_arn1 = get_table_arn(test_table_s_for_export_only)
    s3 = make_s3_client(dynamodb)
    token = str(uuid.uuid4())
    export_time = int(datetime.datetime.now().timestamp())
    with new_s3_bucket(s3) as bucket:
        with pytest.raises(ClientError, match='InvalidExportTimeException.*Difference between export period'):
            client1.export_table_to_point_in_time(
                TableArn=table_arn1,
                S3Bucket=bucket,
                ClientToken=token,
                ExportType='INCREMENTAL_EXPORT',
                IncrementalExportSpecification={
                    'ExportFromTime': export_time - 2,
                    'ExportToTime': export_time,
                    'ExportViewType': 'NEW_IMAGE',
                }
            )
        

# Test that trying to export the table twice using the same client token but different incremental export specifications (ExportFromTime) fails with ExportConflictException.
@pytest.mark.xfail(reason="Not yet implemented on Scylla")
def test_export_client_token_changed_incremental_export_spec_from_time(dynamodb, test_table_s_for_export_only, test_table_s_2_for_export_only):
    client1 = enable_pitr(test_table_s_for_export_only)
    table_arn1 = get_table_arn(test_table_s_for_export_only)
    client2 = enable_pitr(test_table_s_2_for_export_only)
    table_arn2 = get_table_arn(test_table_s_2_for_export_only)
    s3 = make_s3_client(dynamodb)
    token = str(uuid.uuid4())
    export_time = int(datetime.datetime.now().timestamp())
    min_export_time_diff = 1

    if is_aws_from_client(client1):
        min_export_time_diff = 15 * 60
        # Amazon requires from and to times to be at least 15 minutes apart and we can't set to time in the future, so we need to wait.
        time.sleep(min_export_time_diff + 1)

    with new_s3_bucket(s3) as bucket:
        client1.export_table_to_point_in_time(
            TableArn=table_arn1,
            S3Bucket=bucket,
            ClientToken=token,
            ExportType='INCREMENTAL_EXPORT',
            IncrementalExportSpecification={
                'ExportFromTime': export_time,
                'ExportToTime': export_time + min_export_time_diff + 1,
                'ExportViewType': 'NEW_IMAGE',
            }
        )
        with pytest.raises(ClientError, match='ExportConflictException.*Duplicate request detected'):
            client2.export_table_to_point_in_time(
                TableArn=table_arn2,
                S3Bucket=bucket,
                ClientToken=token,
                ExportType='INCREMENTAL_EXPORT',
                IncrementalExportSpecification={
                    'ExportFromTime': export_time + 1,
                    'ExportToTime': export_time + min_export_time_diff + 1,
                    'ExportViewType': 'NEW_IMAGE',
                }
            )


# Test that trying to export the table twice using the same client token but different incremental export specifications (ExportToTime) fails with ExportConflictException.
@pytest.mark.xfail(reason="Not yet implemented on Scylla")
def test_export_client_token_changed_incremental_export_spec_to_time(dynamodb, test_table_s_for_export_only, test_table_s_2_for_export_only):
    client1 = enable_pitr(test_table_s_for_export_only)
    table_arn1 = get_table_arn(test_table_s_for_export_only)
    client2 = enable_pitr(test_table_s_2_for_export_only)
    table_arn2 = get_table_arn(test_table_s_2_for_export_only)
    s3 = make_s3_client(dynamodb)
    token = str(uuid.uuid4())
    export_time = int(datetime.datetime.now().timestamp())
    min_export_time_diff = 1

    # This is unsupported on scylla, so we don't sleep in that case and call and fail sooner.
    if is_aws_from_client(client1):
        min_export_time_diff = 15 * 60
        # Amazon requires from and to times to be at least 15 minutes apart and we can't set to time in the future, so we need to wait.
        time.sleep(min_export_time_diff + 1)

    with new_s3_bucket(s3) as bucket:
        resp1 = client1.export_table_to_point_in_time(
            TableArn=table_arn1,
            S3Bucket=bucket,
            ClientToken=token,
            ExportType='INCREMENTAL_EXPORT',
            IncrementalExportSpecification={
                'ExportFromTime': export_time,
                'ExportToTime': export_time + min_export_time_diff,
                'ExportViewType': 'NEW_IMAGE',
            }
        )
        with pytest.raises(ClientError, match='ExportConflictException.*Duplicate request detected'):
            client2.export_table_to_point_in_time(
                TableArn=table_arn2,
                S3Bucket=bucket,
                ClientToken=token,
                ExportType='INCREMENTAL_EXPORT',
                IncrementalExportSpecification={
                    'ExportFromTime': export_time,
                    'ExportToTime': export_time + min_export_time_diff + 1,
                    'ExportViewType': 'NEW_IMAGE',
                }
            )


# Test that trying to export the table twice using the same client token but different incremental export specifications (ExportViewType) fails with ExportConflictException.
@pytest.mark.xfail(reason="Not yet implemented on Scylla")
def test_export_client_token_changed_incremental_export_spec_view_type(dynamodb, test_table_s_for_export_only, test_table_s_2_for_export_only):
    client1 = enable_pitr(test_table_s_for_export_only)
    table_arn1 = get_table_arn(test_table_s_for_export_only)
    client2 = enable_pitr(test_table_s_2_for_export_only)
    table_arn2 = get_table_arn(test_table_s_2_for_export_only)
    s3 = make_s3_client(dynamodb)
    token = str(uuid.uuid4())
    export_time = int(datetime.datetime.now().timestamp())
    min_export_time_diff = 1

    # This is unsupported on scylla, so we don't sleep in that case and call and fail sooner.
    if is_aws_from_client(client1):
        min_export_time_diff = 15 * 60
        # Amazon requires from and to times to be at least 15 minutes apart and we can't set to time in the future, so we need to wait.
        time.sleep(min_export_time_diff + 1)

    with new_s3_bucket(s3) as bucket:
        client1.export_table_to_point_in_time(
            TableArn=table_arn1,
            S3Bucket=bucket,
            ClientToken=token,
            ExportType='INCREMENTAL_EXPORT',
            IncrementalExportSpecification={
                'ExportFromTime': export_time,
                'ExportToTime': export_time + min_export_time_diff,
                'ExportViewType': 'NEW_IMAGE',
            }
        )
        with pytest.raises(ClientError, match='ExportConflictException.*Duplicate request detected'):
            client2.export_table_to_point_in_time(
                TableArn=table_arn2,
                S3Bucket=bucket,
                ClientToken=token,
                ExportType='INCREMENTAL_EXPORT',
                IncrementalExportSpecification={
                    'ExportFromTime': export_time,
                    'ExportToTime': export_time + min_export_time_diff,
                    'ExportViewType': 'NEW_AND_OLD_IMAGES',
                }
            )


# Test that starts two exports on the same table (no ClientToken)
@pytest.mark.xfail(reason="Not yet implemented on Scylla")
def test_export_two_exports_without_client_token(dynamodb, test_table_s_for_export_only):
    table = test_table_s_for_export_only
    client = enable_pitr(table)
    table_arn = get_table_arn(table)
    s3 = make_s3_client(dynamodb)

    with new_s3_bucket(s3) as bucket:
        resp1 = client.export_table_to_point_in_time(
            TableArn=table_arn,
            S3Bucket=bucket
        )
        resp2 = client.export_table_to_point_in_time(
            TableArn=table_arn,
            S3Bucket=bucket
        )
        assert resp1['ExportDescription']['ExportArn'] != resp2['ExportDescription']['ExportArn']
        wait_for_export(client, resp1['ExportDescription']['ExportArn'])
        wait_for_export(client, resp2['ExportDescription']['ExportArn'])


# Test that trying to export from two tables using the same client token fails.
@pytest.mark.xfail(reason="Not yet implemented on Scylla")
def test_export_client_token_doesnt_work_on_two_tables(dynamodb, test_table_s_for_export_only, test_table_s_2_for_export_only):
    client1 = enable_pitr(test_table_s_for_export_only)
    table_arn1 = get_table_arn(test_table_s_for_export_only)
    client2 = enable_pitr(test_table_s_2_for_export_only)
    table_arn2 = get_table_arn(test_table_s_2_for_export_only)
    s3 = make_s3_client(dynamodb)
    token = str(uuid.uuid4())

    with new_s3_bucket(s3) as bucket:
        client1.export_table_to_point_in_time(
            TableArn=table_arn1,
            S3Bucket=bucket,
            ClientToken=token,
        )
        with pytest.raises(ClientError, match='ExportConflictException.*Duplicate request detected'):
            client2.export_table_to_point_in_time(
                TableArn=table_arn2,
                S3Bucket=bucket,
                ClientToken=token,
            )

# Test that exporting a nonexistent table returns TableNotFoundException.
@pytest.mark.xfail(reason="Not yet implemented on Scylla")
def test_export_nonexistent_table(dynamodb, test_table_s_for_export_only):
    client = dynamodb.meta.client
    s3 = make_s3_client(dynamodb)
    region = dynamodb.meta.client.meta.region_name
    table_arn = get_table_arn(test_table_s_for_export_only)
    account_id = table_arn.split(':')[4]
    fake_arn = f'arn:aws:dynamodb:{region}:{account_id}:table/nonexistent_table_xyz'

    with new_s3_bucket(s3) as bucket:
        with pytest.raises(ClientError, match='TableNotFoundException.*Table not found'):
            client.export_table_to_point_in_time(
                TableArn=fake_arn,
                S3Bucket=bucket,
            )


# Test that exporting to a nonexistent S3 bucket fails.
# This one is little different, as - it seems - DynamoDB actually accepts nonexistent bucket and starts the export,
# returns the FAILED and and error message later on.
@pytest.mark.xfail(reason="Not yet implemented on Scylla")
def test_export_nonexistent_bucket(dynamodb, test_table_s_for_export_only):
    table = test_table_s_for_export_only
    client = enable_pitr(table)
    table_arn = get_table_arn(table)
    fake_bucket = unique_bucket_name()

    response = client.export_table_to_point_in_time(
        TableArn=table_arn,
        S3Bucket=fake_bucket,
    )

    export_arn = response['ExportDescription']['ExportArn']

    desc = wait_for_export(client, export_arn)
    assert desc['ExportArn'] == export_arn
    assert desc['TableArn'] == table_arn
    assert desc['S3Bucket'] == fake_bucket
    assert desc['ExportStatus'] == 'FAILED'
    assert desc['FailureCode'] == 'S3NoSuchBucket'
    assert 'bucket does not exist' in desc['FailureMessage']

    for ex in iterate_over_exports(client, table_arn):
        if ex['ExportArn'] == export_arn:
            assert ex['ExportStatus'] == 'FAILED'
            break
    else:
        assert False, f"Export {export_arn} not found in list_exports"

# Test that exporting to a empty S3 bucket fails.
@pytest.mark.xfail(reason="Not yet implemented on Scylla")
def test_export_empty_bucket(dynamodb, test_table_s_for_export_only):
    table = test_table_s_for_export_only
    client = enable_pitr(table)
    table_arn = get_table_arn(table)

    with pytest.raises(ClientError, match='ValidationException.*s3Bucket.*failed'):
        response = client.export_table_to_point_in_time(
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

# Test that failed export that didn't start doesn't reserve ClientToken
# and thus the same ClientToken can be used again for a new export.
@pytest.mark.xfail(reason="Not yet implemented on Scylla")
def test_export_failed_does_not_reserve_client_token(dynamodb, test_table_s_for_export_only):
    table = test_table_s_for_export_only
    client = enable_pitr(table)
    table_arn = get_table_arn(table)
    s3 = make_s3_client(dynamodb)
    token = str(uuid.uuid4())

    with pytest.raises(ClientError, match='ValidationException.*s3Bucket.*failed'):
        response = client.export_table_to_point_in_time(
            TableArn=table_arn,
            S3Bucket='',
            ClientToken=token,
        )

    with new_s3_bucket(s3) as bucket:
        response = client.export_table_to_point_in_time(
            TableArn=table_arn,
            S3Bucket=bucket,
            ClientToken=token,
        )

# ---------------------------------------------------------------------------
# DescribeExport tests
# ---------------------------------------------------------------------------

# Test that DescribeExport returns the full ExportDescription for a
# known export.
@pytest.mark.xfail(reason="Not yet implemented on Scylla")
def test_describe_export(dynamodb, test_table_s_for_export_only):
    table = test_table_s_for_export_only
    client = enable_pitr(table)
    table_arn = get_table_arn(table)
    s3 = make_s3_client(dynamodb)

    with new_s3_bucket(s3) as bucket:
        response = client.export_table_to_point_in_time(
            TableArn=table_arn,
            S3Bucket=bucket,
        )
        export_arn = response['ExportDescription']['ExportArn']

        desc = client.describe_export(ExportArn=export_arn)['ExportDescription']
        assert desc['ExportArn'] == export_arn
        assert desc['TableArn'] == table_arn
        assert desc['S3Bucket'] == bucket
        assert desc['ExportStatus'] in ('IN_PROGRESS', 'COMPLETED')
        assert 'StartTime' in desc
        assert 'ExportFormat' in desc

        wait_for_export(client, export_arn)

        # this should return description with ExportStatus=COMPLETED and additional fields like EndTime, ExportManifest, ItemCount, BilledSizeBytes etc.
        desc = client.describe_export(ExportArn=export_arn)['ExportDescription']
        assert desc['ExportStatus'] == 'COMPLETED'
        assert desc['ExportArn'] == export_arn
        assert desc['TableArn'] == table_arn
        assert 'EndTime' in desc
        assert 'ExportManifest' in desc
        assert 'ItemCount' in desc
        assert 'BilledSizeBytes' in desc


# Test that DescribeExport with a non-existent ARN returns ValidationException.
@pytest.mark.xfail(reason="Not yet implemented on Scylla")
def test_describe_export_nonexistent(dynamodb, test_table_s_for_export_only):
    client = dynamodb.meta.client
    region = dynamodb.meta.client.meta.region_name
    table_arn = get_table_arn(test_table_s_for_export_only)
    account_id = table_arn.split(':')[4]
    fake_arn = f'arn:aws:dynamodb:{region}:{account_id}:table/nonexistent_table_xyz'
    with pytest.raises(ClientError, match='ValidationException.*Invalid Export ARN'):
        client.describe_export(ExportArn=fake_arn)



# Test that DescribeExport with a incorrect ARN returns ValidationException.
# Note: this one is a bit tricky - AWS probably doesn't check for `arn:` prefix and we fail
# on the same ValidationException as `test_describe_export_nonexistent` test.
@pytest.mark.xfail(reason="Not yet implemented on Scylla")
def test_describe_export_incorrect_arn_1(dynamodb):
    client = dynamodb.meta.client
    fake_arn = f'qwerty:aws:dynamodb:us-east-1:000000000000:table/nonexistent_table_xyz'
    with pytest.raises(ClientError, match='ValidationException.*Invalid Export ARN'):
        client.describe_export(ExportArn=fake_arn)


# Test that DescribeExport with a incorrect ARN returns AccessDeniedException.
@pytest.mark.xfail(reason="Not yet implemented on Scylla")
def test_describe_export_incorrect_arn_2(dynamodb):
    client = dynamodb.meta.client
    fake_arn = f'arn:qwerty:dynamodb:us-east-1:000000000000:table/nonexistent_table_xyz'
    with pytest.raises(ClientError, match='AccessDeniedException.*Access is denied'):
        client.describe_export(ExportArn=fake_arn)


# Test that DescribeExport with a incorrect ARN returns AccessDeniedException.
@pytest.mark.xfail(reason="Not yet implemented on Scylla")
def test_describe_export_incorrect_arn_3(dynamodb):
    client = dynamodb.meta.client
    fake_arn = f'arn:aws:qwerty:us-east-1:000000000000:table/nonexistent_table_xyz'
    with pytest.raises(ClientError, match='AccessDeniedException.*Access is denied'):
        client.describe_export(ExportArn=fake_arn)


# Test that DescribeExport with a incorrect ARN returns AccessDeniedException.
@pytest.mark.xfail(reason="Not yet implemented on Scylla")
def test_describe_export_incorrect_arn_4(dynamodb):
    client = dynamodb.meta.client
    fake_arn = f'arn:aws:dynamodb:qwerty:000000000000:table/nonexistent_table_xyz'
    with pytest.raises(ClientError, match='AccessDeniedException.*Access is denied'):
        client.describe_export(ExportArn=fake_arn)


# Test that DescribeExport with a incorrect ARN returns AccessDeniedException.
@pytest.mark.xfail(reason="Not yet implemented on Scylla")
def test_describe_export_incorrect_arn_5(dynamodb):
    client = dynamodb.meta.client
    fake_arn = f'arn:aws:dynamodb:us-east-1:qwerty:table/nonexistent_table_xyz'
    with pytest.raises(ClientError, match='AccessDeniedException.*Access is denied'):
        client.describe_export(ExportArn=fake_arn)


# ---------------------------------------------------------------------------
# ListExports tests
# ---------------------------------------------------------------------------

# Test that ListExports returns an ExportSummaries list (possibly empty)
# for a table that has no exports.
@pytest.mark.xfail(reason="Not yet implemented on Scylla")
def test_list_exports_empty(dynamodb):
    with new_test_table(dynamodb,
        KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
    ) as table:
        client = table.meta.client
        table_arn = get_table_arn(table)
        response = client.list_exports(TableArn=table_arn)
        assert 'ExportSummaries' in response
        # The list should be empty for a freshly-created table
        assert len(response['ExportSummaries']) == 0


def iterate_over_exports(client, table_arn = None, MaxResults = None):
    kwargs = {}
    if table_arn is not None:
        kwargs['TableArn'] = table_arn
    if MaxResults is not None:
        kwargs['MaxResults'] = MaxResults
    response = client.list_exports(**kwargs)
    if MaxResults is not None:
        assert len(response['ExportSummaries']) <= MaxResults
    yield from response['ExportSummaries']
    while 'NextToken' in response and response['NextToken']:
        kwargs['NextToken'] = response['NextToken']
        response = client.list_exports(**kwargs)
        if MaxResults is not None:
            assert len(response['ExportSummaries']) <= MaxResults
        yield from response['ExportSummaries']


# Test that after starting an export, ListExports includes it.
@pytest.mark.xfail(reason="Not yet implemented on Scylla")
def test_list_exports_contains_export(dynamodb, test_table_s_for_export_only):
    table = test_table_s_for_export_only
    client = enable_pitr(table)
    table_arn = get_table_arn(table)
    s3 = make_s3_client(dynamodb)

    with new_s3_bucket(s3) as bucket:
        response = client.export_table_to_point_in_time(
            TableArn=table_arn,
            S3Bucket=bucket,
        )
        export_arn = response['ExportDescription']['ExportArn']

        for e in iterate_over_exports(client, table_arn):
            if e['ExportArn'] == export_arn:
                break
        else:
            pytest.fail(f"Export {export_arn} not found in ListExports for table {table_arn}")

        wait_for_export(client, export_arn)


# Test that ListExports without a TableArn filter returns results.
@pytest.mark.xfail(reason="Not yet implemented on Scylla")
def test_list_exports_no_table_filter(dynamodb, test_table_s_for_export_only):
    table = test_table_s_for_export_only
    client = enable_pitr(table)
    table_arn = get_table_arn(table)
    s3 = make_s3_client(dynamodb)

    with new_s3_bucket(s3) as bucket:
        response = client.export_table_to_point_in_time(
            TableArn=table_arn,
            S3Bucket=bucket,
        )
        export_arn = response['ExportDescription']['ExportArn']

        # ListExports without TableArn should still include our export
        for e in iterate_over_exports(client):
            if e['ExportArn'] == export_arn:
                break
        else:
            pytest.fail(f"Export {export_arn} not found in ListExports without TableArn filter")

        wait_for_export(client, export_arn)


# Test that ListExports pagination via MaxResults and NextToken works.
@pytest.mark.xfail(reason="Not yet implemented on Scylla")
def test_list_exports_pagination(dynamodb):
    with new_test_table(dynamodb,
        KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
    ) as table:
        client = enable_pitr(table)
        table_arn = get_table_arn(table)

        export_arns = []
        # Start multiple exports to the same bucket with different prefixes
        # Note: we intentionally use the fake bucket, as it starts the export,
        # but fails it almost immediately, drastically reducing test runtime.
        # The exports will still be present in ListExports results (with status FAILED),
        # which is enough for testing pagination.
        fake_bucket = unique_bucket_name()
        for i in range(10):
            resp = client.export_table_to_point_in_time(
                TableArn=table_arn,
                S3Bucket=fake_bucket,
                S3Prefix=f'export-{i}',
                ClientToken=str(uuid.uuid4()),
            )
            export_arns.append(resp['ExportDescription']['ExportArn'])
        for export_arn in export_arns:
            wait_for_export(client, export_arn)

        # List with MaxResults=1 to force pagination
        all_arns = []
        for summary in iterate_over_exports(client, table_arn, MaxResults=1):
            assert 'ExportArn' in summary
            all_arns.append(summary['ExportArn'])

        # we're filtering by our newly created table, so we must get only our exports.
        assert set(export_arns) == set(all_arns)
        assert len(export_arns) == len(all_arns)
        # ARNs are supposed to be returned in reverse order by ARN itself (Amazon documentation).
        # ARNs must not repeat, so we use `<`.
        assert export_arns[0] < export_arns[1] < export_arns[2] < export_arns[3] < export_arns[4] < export_arns[5] < export_arns[6] < export_arns[7] < export_arns[8] < export_arns[9]


# Test that ExportSummary from ListExports contains the expected
# fields: ExportArn, ExportStatus, ExportType.
@pytest.mark.xfail(reason="Not yet implemented on Scylla")
def test_list_exports_summary_fields(dynamodb, test_table_s_for_export_only):
    table = test_table_s_for_export_only
    client = enable_pitr(table)
    table_arn = get_table_arn(table)
    s3 = make_s3_client(dynamodb)

    with new_s3_bucket(s3) as bucket:
        resp = client.export_table_to_point_in_time(
            TableArn=table_arn,
            S3Bucket=bucket,
        )
        export_arn = resp['ExportDescription']['ExportArn']
        wait_for_export(client, export_arn)

        for summary in iterate_over_exports(client, table_arn):
            if summary['ExportArn'] == export_arn:
                break
        else:
            pytest.fail(f"Export {export_arn} not found in ListExports for table {table_arn}")
        assert 'ExportArn' in summary
        assert 'ExportStatus' in summary
        assert summary['ExportStatus'] == 'COMPLETED'
        assert 'ExportType' in summary
        assert summary['ExportType'] == 'FULL_EXPORT'
