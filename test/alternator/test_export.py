# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

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

from test.alternator.util import create_test_table, is_aws, new_test_table

# NOTE: tests here use `pytest.mark.xfail(reason="Not yet implemented on Scylla")` as xfail marker as the implementation is ongoing.
# The tests will pass against AWS.


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

    # We need to enable PITR on AWS, but not on scylla (in fact the call itself in scylla is not handled and will fail).
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
# My actual tests show export takes approximately 10-12 minutes on AWS hence hefty timeout.
def wait_for_export(client, export_arn):
    timeout = 60 * 60 if is_aws(client) else 60
    deadline = time.time() + timeout
    while time.time() < deadline:
        desc = client.describe_export(ExportArn=export_arn)['ExportDescription']
        status = desc['ExportStatus']
        if status in ('COMPLETED', 'FAILED'):
            return desc
        time.sleep(5)
    raise TimeoutError(f"Export {export_arn} did not complete within {timeout}s")


# ---------------------------------------------------------------------------
# ExportTableToPointInTime tests
# ---------------------------------------------------------------------------

# Test that ExportTableToPointInTime starts an export and returns an
# ExportDescription with expected fields.
@pytest.mark.xfail(reason="Not yet implemented on Scylla")
def test_export_basic(dynamodb, test_table_s):
    table = test_table_s
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

        # Amazon returns an unique ClientToken in response, even if user did not supply one.
        assert desc['ClientToken']

        assert desc['ExportFormat'] == 'DYNAMODB_JSON'  # default format is DYNAMODB_JSON
        assert 'ExportType' not in desc # default is FULL_EXPORT, but AWS does not return it in the response for some reason.
        # Wait for completion and verify
        final = wait_for_export(client, desc['ExportArn'])
        assert final['ExportStatus'] == 'COMPLETED'
        assert 'ExportType' not in desc # default is FULL_EXPORT, but AWS does not return it in the response for some reason.


# Test that ExportTableToPointInTime starts an export and then table is deleted,
# after which ListExports should still return the export (with TableArn and TableId) and DescribeExport should still work.
# This was run against Amazon and it seems they just return IN_PROGRESS after deleting the table and the S3 bucket.
# We don't have to mark the progress as immediately FAILED in such situation.
@pytest.mark.xfail(reason="Not yet implemented on Scylla")
def test_export_start_and_delete_table(dynamodb):
    with new_test_table(dynamodb,
        KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
    ) as table:
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
            export_arn = desc['ExportArn']
            assert desc['ExportStatus'] in ('IN_PROGRESS', 'COMPLETED')
            assert desc['TableArn'] == table_arn
            assert desc['S3Bucket'] == bucket
            assert desc['ExportFormat'] == 'DYNAMODB_JSON'  # default format is DYNAMODB_JSON
            assert 'ExportType' not in desc # default is FULL_EXPORT, but AWS does not return it in the response for some reason.

    for ex in iterate_over_exports(client, table_arn):
        if ex['ExportArn'] == export_arn:
            assert ex['ExportStatus'] in ('FAILED', 'COMPLETED', 'IN_PROGRESS'), ex
            break
    else:
        assert False, f"Export {export_arn} not found in list_exports"

    desc = client.describe_export(ExportArn=export_arn)['ExportDescription']
    assert desc['ExportArn'] == export_arn
    assert desc['TableArn'] == table_arn
    assert desc['S3Bucket'] == bucket
    assert desc['ExportStatus'] in ('IN_PROGRESS', 'COMPLETED', 'IN_PROGRESS')
    assert 'StartTime' in desc
    assert 'ExportFormat' in desc


# Test that ExportTableToPointInTime starts an export and returns an
# ExportDescription with expected fields.
@pytest.mark.xfail(reason="Not yet implemented on Scylla")
def test_export_basic_with_export_type(dynamodb, test_table_s):
    table = test_table_s
    client = enable_pitr(table)
    table_arn = get_table_arn(table)
    s3 = make_s3_client(dynamodb)

    with new_s3_bucket(s3) as bucket:
        response = client.export_table_to_point_in_time(
            TableArn=table_arn,
            S3Bucket=bucket,
            ExportType='FULL_EXPORT'
        )
        desc = response['ExportDescription']
        assert 'ExportArn' in desc
        assert desc['ExportStatus'] in ('IN_PROGRESS', 'COMPLETED')
        assert desc['TableArn'] == table_arn
        assert desc['S3Bucket'] == bucket
        assert desc['ExportFormat'] == 'DYNAMODB_JSON'  # default format is DYNAMODB_JSON
        assert desc['ExportType'] == 'FULL_EXPORT'
        # Wait for completion and verify
        final = wait_for_export(client, desc['ExportArn'])
        assert final['ExportStatus'] == 'COMPLETED'
        assert desc['ExportType'] == 'FULL_EXPORT'

# Test that ExportTableToPointInTime with ION format correctly parses option and completes.
@pytest.mark.xfail(reason="Not yet implemented on Scylla")
def test_export_unsupported_format(dynamodb, test_table_s):
    table = test_table_s
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
def test_export_missing_format(dynamodb, test_table_s):
    table = test_table_s
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

# Test that an export of a table with data completes.
# Test will fetch S3 files and verify that they contain expected items and that manifest files are correct.
# Test for ION will skip S3 file content verifitcation for now until we get python ion library set up for testing -
#   directory content will still be verified and as will be manifest files.
# Test is parameterized to run with both supported export formats and with/without S3 prefix.
@pytest.mark.xfail(reason="Not yet implemented on Scylla")
@pytest.mark.parametrize('s3_prefix_value', [None, 'dira/dirb/dirc'])
@pytest.mark.parametrize('export_format_value', ['DYNAMODB_JSON', 'ION'])
def test_export_with_data_full(dynamodb, s3_prefix_value, export_format_value):
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
                                 'd09': [ i + 5, 'qwerty' ],
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
                'd09': { 'L': [ { 'N': str(i + 5) }, { 'S': 'qwerty' } ] },
                'd10': { 'NULL': True },
                'd11': { 'BOOL': bool(i & 1 == 0) },
            }}

            #  we insert items as text, because text lines can be sorted.
            inserted_items_as_text.append(json.dumps(item, indent=None, sort_keys=True))

        client = enable_pitr(table)
        table_arn = get_table_arn(table)
        s3 = make_s3_client(dynamodb)

        with new_s3_bucket(s3) as bucket:
            args = {}
            if s3_prefix_value is not None:
                args['S3Prefix'] = s3_prefix_value
            response = client.export_table_to_point_in_time(
                TableArn=table_arn,
                S3Bucket=bucket,
                ExportFormat=export_format_value,
                **args
            )
            final = wait_for_export(client, response['ExportDescription']['ExportArn'])
            assert final['ExportStatus'] == 'COMPLETED'
            assert final['ItemCount'] == num_items

            # Download all exported files as a in-memory dictionary representing a directory structure.
            root_dir = downloaded_root_dir = fetch_all_exported_items(s3, bucket)

            if s3_prefix_value:
                for p in s3_prefix_value.split('/'):
                    assert p in root_dir, downloaded_root_dir
                    root_dir = root_dir[p]
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
                # For now we validate content file only for json format.
                if export_format_value == 'DYNAMODB_JSON':
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
                else:
                    assert data_file.endswith('.ion.gz')


            # For now we validate content file only for json format.
            if export_format_value == 'DYNAMODB_JSON':
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

                # path to the data file seems to be relative from bucket root and should start with `<optionally s3_prefix_value/>AWSDynamoDB/`
                # the path itself is verified by the call to `get_content_by_path` later on.
                p = js['dataFileS3Key']
                assert p.startswith('AWSDynamoDB/' if s3_prefix_value is None else os.path.join(s3_prefix_value, 'AWSDynamoDB/'))

                # We counted items in each file while reading them, now we verify that the count matches the manifest file.
                # This is done only for json export format.
                if export_format_value == 'DYNAMODB_JSON':
                    real_item_count = item_count_map[os.path.split(p)[1]]
                    assert js['itemCount'] == real_item_count
                    total_real_item_count += real_item_count
                assert js['etag'] != ''

                # there is md5 file for each data file, we verify that the md5 in manifest matches the calculated md5 of the downloaded file.
                md5_checksum = js['md5Checksum'].encode('utf-8')
                assert md5_checksum != ''
                calculated_md5 = calculate_md5(get_content_by_path(p))
                assert calculated_md5 == md5_checksum, f"{calculated_md5} != {md5_checksum} for file {p}"

            # We count items only with json export format.
            if export_format_value == 'DYNAMODB_JSON':
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
            assert manifest_summary['outputFormat'] == export_format_value

# Test that sending the same ClientToken twice returns the same export.
@pytest.mark.xfail(reason="Not yet implemented on Scylla")
def test_export_client_token_idempotent(dynamodb, test_table_s):
    table = test_table_s
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
        assert resp1['ExportDescription']['ClientToken'] == resp2['ExportDescription']['ClientToken'] == token
        wait_for_export(client, resp1['ExportDescription']['ExportArn'])


# Following tests check idempotency of export with respect to ClientToken - they verify that
# if we change some parameter of the export but keep the same ClientToken,
# the second call will fail with ExportConflictException.
# Note: we postpone test for `S3SseKmsKeyId` (due to complexity of setting up KMS)
@pytest.mark.xfail(reason="Not yet implemented on Scylla")
def test_export_client_token_changed_bucket(dynamodb, test_table_s, test_table_s_2):
    client1 = enable_pitr(test_table_s)
    table_arn1 = get_table_arn(test_table_s)
    client2 = enable_pitr(test_table_s_2)
    table_arn2 = get_table_arn(test_table_s_2)
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
def test_export_client_token_changed_export_format(dynamodb, test_table_s, test_table_s_2):
    client1 = enable_pitr(test_table_s)
    table_arn1 = get_table_arn(test_table_s)
    client2 = enable_pitr(test_table_s_2)
    table_arn2 = get_table_arn(test_table_s_2)
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
def test_export_client_token_changed_export_time(dynamodb, test_table_s, test_table_s_2):
    client1 = enable_pitr(test_table_s)
    table_arn1 = get_table_arn(test_table_s)
    client2 = enable_pitr(test_table_s_2)
    table_arn2 = get_table_arn(test_table_s_2)
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
def test_export_client_token_changed_bucket_owner(dynamodb, test_table_s, test_table_s_2):
    client1 = enable_pitr(test_table_s)
    table_arn1 = get_table_arn(test_table_s)
    client2 = enable_pitr(test_table_s_2)
    table_arn2 = get_table_arn(test_table_s_2)
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
def test_export_client_token_changed_s3_prefix(dynamodb, test_table_s, test_table_s_2):
    client1 = enable_pitr(test_table_s)
    table_arn1 = get_table_arn(test_table_s)
    client2 = enable_pitr(test_table_s_2)
    table_arn2 = get_table_arn(test_table_s_2)
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
def test_export_client_token_changed_s3_sse_algo(dynamodb, test_table_s, test_table_s_2):
    client1 = enable_pitr(test_table_s)
    table_arn1 = get_table_arn(test_table_s)
    client2 = enable_pitr(test_table_s_2)
    table_arn2 = get_table_arn(test_table_s_2)
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
def test_export_incremental_export_time_too_close(dynamodb, test_table_s):
    client1 = enable_pitr(test_table_s)
    table_arn1 = get_table_arn(test_table_s)
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
def test_export_client_token_changed_incremental_export_spec_from_time(dynamodb, test_table_s, test_table_s_2):
    client1 = enable_pitr(test_table_s)
    table_arn1 = get_table_arn(test_table_s)
    client2 = enable_pitr(test_table_s_2)
    table_arn2 = get_table_arn(test_table_s_2)
    s3 = make_s3_client(dynamodb)
    token = str(uuid.uuid4())
    export_time = int(datetime.datetime.now().timestamp())
    min_export_time_diff = 1

    if is_aws(client1):
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
def test_export_client_token_changed_incremental_export_spec_to_time(dynamodb, test_table_s, test_table_s_2):
    client1 = enable_pitr(test_table_s)
    table_arn1 = get_table_arn(test_table_s)
    client2 = enable_pitr(test_table_s_2)
    table_arn2 = get_table_arn(test_table_s_2)
    s3 = make_s3_client(dynamodb)
    token = str(uuid.uuid4())
    export_time = int(datetime.datetime.now().timestamp())
    min_export_time_diff = 1

    # This is unsupported on scylla, so we don't sleep in that case and call and fail sooner.
    if is_aws(client1):
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
def test_export_client_token_changed_incremental_export_spec_view_type(dynamodb, test_table_s, test_table_s_2):
    client1 = enable_pitr(test_table_s)
    table_arn1 = get_table_arn(test_table_s)
    client2 = enable_pitr(test_table_s_2)
    table_arn2 = get_table_arn(test_table_s_2)
    s3 = make_s3_client(dynamodb)
    token = str(uuid.uuid4())
    export_time = int(datetime.datetime.now().timestamp())
    min_export_time_diff = 1

    # This is unsupported on scylla, so we don't sleep in that case and call and fail sooner.
    if is_aws(client1):
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
def test_export_two_exports_without_client_token(dynamodb, test_table_s):
    table = test_table_s
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
def test_export_client_token_doesnt_work_on_two_tables(dynamodb, test_table_s, test_table_s_2):
    client1 = enable_pitr(test_table_s)
    table_arn1 = get_table_arn(test_table_s)
    client2 = enable_pitr(test_table_s_2)
    table_arn2 = get_table_arn(test_table_s_2)
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
def test_export_nonexistent_table(dynamodb, test_table_s):
    client = dynamodb.meta.client
    s3 = make_s3_client(dynamodb)
    region = dynamodb.meta.client.meta.region_name
    table_arn = get_table_arn(test_table_s)
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
def test_export_nonexistent_bucket(dynamodb, test_table_s):
    table = test_table_s
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

# Test that exporting with S3Bucket field set to an emty string fails.
@pytest.mark.xfail(reason="Not yet implemented on Scylla")
def test_export_empty_bucket(dynamodb, test_table_s):
    table = test_table_s
    client = enable_pitr(table)
    table_arn = get_table_arn(table)

    with pytest.raises(ClientError, match='ValidationException.*s3Bucket.*failed'):
        response = client.export_table_to_point_in_time(
            TableArn=table_arn,
            S3Bucket='',
        )


# Test that failed export that didn't start doesn't reserve ClientToken
# and thus the same ClientToken can be used again for a new export.
@pytest.mark.xfail(reason="Not yet implemented on Scylla")
def test_export_failed_does_not_reserve_client_token(dynamodb, test_table_s):
    table = test_table_s
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

    # We don't care about export result here, we just want to verify that ClientToken can be passed again to start a new export
    # (the call did not raise an exception).
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
def test_describe_export(dynamodb, test_table_s):
    table = test_table_s
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
def test_describe_export_nonexistent(dynamodb, test_table_s):
    client = dynamodb.meta.client
    region = dynamodb.meta.client.meta.region_name
    table_arn = get_table_arn(test_table_s)
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
def test_list_exports_contains_export(dynamodb, test_table_s):
    table = test_table_s
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


# Test that ListExports without a TableArn filter returns results.
@pytest.mark.xfail(reason="Not yet implemented on Scylla")
def test_list_exports_no_table_filter(dynamodb, test_table_s):
    table = test_table_s
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


        #  - alternator/test_export.py:247 test_export_with_data_full[DYNAMODB_JSON-dira/dirb/dirc]
        #  - alternator/test_export.py:247 test_export_with_data_full[ION-None]
        #  - alternator/test_export.py:247 test_export_with_data_full[ION-dira/dirb/dirc]

# Test that ListExports pagination via MaxResults and NextToken works.
@pytest.mark.xfail(reason="Not yet implemented on Scylla")
def test_list_exports_pagination(dynamodb):
    max_tables = 10

    def table_created_callback(tables):
        export_arns = []
        # Start multiple exports to the same bucket with different prefixes
        # Note: we intentionally use the fake bucket, as it starts the export,
        # but fails it almost immediately, drastically reducing test runtime.
        # The exports will still be present in ListExports results (with status FAILED),
        # which is enough for testing pagination.
        for table in tables:
            client = enable_pitr(table)
            table_arn = get_table_arn(table)
            resp = client.export_table_to_point_in_time(
                TableArn=table_arn,
                S3Bucket=unique_bucket_name(),
                S3Prefix=f'prefix-{uuid.uuid4()}',
                ClientToken=str(uuid.uuid4()),
            )
            export_arns.append(resp['ExportDescription']['ExportArn'])

        # List with MaxResults=3 to force pagination (we have 10 exports, but MaxResults=3, so we will need at least 4 calls to get all results).
        # We use MaxResults>1 to make sure subblock of all results is returned sorted as well.
        all_arns = []
        for summary in iterate_over_exports(client, MaxResults=3):
            assert 'ExportArn' in summary
            all_arns.append(summary['ExportArn'])

        # we're not filtering, so we will get more stuff in the list, but at least all our exports should be there
        assert len(export_arns) <= len(all_arns)
        assert set(export_arns).issubset(set(all_arns))

        # ARNs are supposed to be returned in reverse order by ARN itself (Amazon documentation).
        # ARNs must not repeat, so we use `<`.
        for x in range(len(all_arns) - 1):
            assert all_arns[x] < all_arns[x + 1], all_arns

    def create_tables_recursively(tables):
        if len(tables) >= max_tables:
            return table_created_callback(tables)

        with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
        ) as table:
            create_tables_recursively(tables + [table])

    create_tables_recursively([])

# Test that ExportSummary from ListExports contains the expected
# fields: ExportArn, ExportStatus, ExportType.
@pytest.mark.xfail(reason="Not yet implemented on Scylla")
def test_list_exports_summary_fields(dynamodb, test_table_s):
    table = test_table_s
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
