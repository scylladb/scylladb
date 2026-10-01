# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# Tests for the DynamoDB ExportTableToPointInTime, DescribeExport, and
# ListExports APIs (part of SCYLLADB-1939).

import base64
import collections
import logging
from urllib import response

import pytest
import boto3
import time
import uuid
import json
import hashlib
import datetime
import gzip
import decimal
import os
import re
import requests

from botocore.exceptions import ClientError
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from contextlib import contextmanager, ExitStack

from test.alternator.util import get_table_arn, is_aws, manual_request, new_test_table, create_test_table, random_string, scylla_config_read, scylla_config_temporary, scylla_inject_error
from test.pylib.skip_types import skip_env

# Helper to check if table is deleted.
def is_table_deleted(dynamodb, table_name: str) -> bool:
    try:
        dynamodb.describe_table(TableName=table_name)
        return False  # table still exists (or is in DELETING/ACTIVE/etc.)
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            return True
        raise

# Bucket names allow neither uppercase letters nor underscores, so
# unique_table_name() cannot be used.
def unique_bucket_name():
    return f"alternator-export-test-{uuid.uuid4().hex[:12]}"

# An S3 client in the "dynamodb" fixture's region. DynamoDB allows a bucket
# in another region, but keeping it in the table's region saves cost and
# latency. Local testing uses the S3 server of the test framework
# (test/pylib/s3mock_server.py). The address and the credentials are in the
# environment. If there is no S3 server, or Scylla does not know it, the test
# is skipped. test/alternator/run starts a server only with "--s3".
def make_s3_client(dynamodb):
    if is_aws(dynamodb):
        return boto3.client('s3', region_name=dynamodb.meta.client.meta.region_name)
    if 'S3_SERVER_ADDRESS_FOR_TEST' not in os.environ:
        skip_env('No S3 server was started for this run (test/alternator/run needs the "--s3" option to start one)')
    # Scylla does the export, so it must have the server in
    # object_storage_endpoints.
    if not json.loads(scylla_config_read(dynamodb, 'object_storage_endpoints') or '{}'):
        skip_env('Scylla was not configured with any object_storage_endpoints')
    return boto3.client('s3',
        endpoint_url=f"http://{os.environ['S3_SERVER_ADDRESS_FOR_TEST']}:{os.environ['S3_SERVER_PORT_FOR_TEST']}",
        aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
        region_name='local',
        config=boto3.session.Config(signature_version='s3v4'))

# Create an S3 bucket and delete it, with its contents, on exit.
@contextmanager
def new_s3_bucket(s3_client, bucket_name=None):
    if bucket_name is None:
        bucket_name = unique_bucket_name()
    region = s3_client.meta.region_name
    kwargs: dict = {'Bucket': bucket_name}
    # us-east-1 does not accept a LocationConstraint - in other words if you want `us-east-1` bucket, you need to omit `LocationConstraint` entirely,
    # in all other cases supply `LocationConstraint` - this is AWS quirk.
    if region and region != 'us-east-1':
        kwargs['CreateBucketConfiguration'] = {'LocationConstraint': region}
    s3_client.create_bucket(**kwargs)
    try:
        yield bucket_name
    finally:
        # We try hard to clean up: a bucket left behind on AWS costs money.
        # DynamoDB cannot cancel an export. An export the test did not wait
        # for keeps writing, so delete_bucket would fail with BucketNotEmpty
        # and leak the bucket. Denying Put makes the export's next write
        # fail, so DynamoDB ends the export as FAILED and stops writing.
        # Delete/List stay allowed for our cleanup.
        # We do not clean up on S3Mock: the cleanup below sometimes takes more than 1 minute.
        # The S3Mock shutdown removes the data.
        if is_aws(s3_client):
            policy = {
                'Version': '2012-10-17',
                'Statement': [{
                    'Sid': 'BlockWrites',
                    'Effect': 'Deny',
                    'Principal': '*',
                    'Action': ['s3:PutObject', 's3:PutObjectAcl'],
                    'Resource': f'arn:aws:s3:::{bucket_name}/*',
                }],
            }
            try:
                s3_client.put_bucket_policy(Bucket=bucket_name, Policy=json.dumps(policy))
            except ClientError as ce:
                logging.error("Failed to block bucket writes on S3 for bucket %s: %s", bucket_name, ce)
                # Not yet fatal - fall through to the retry loop below.

            # Delete all objects, then the bucket.
            deadline = time.time() + 60 # 60-second timeout for bucket deletion
            while time.time() < deadline:
                paginator = s3_client.get_paginator('list_objects_v2')
                for page in paginator.paginate(Bucket=bucket_name):
                    if 'Contents' in page:
                        s3_client.delete_objects(
                            Bucket=bucket_name,
                            Delete={'Objects': [{'Key': obj['Key']} for obj in page['Contents']]}
                        )
                try:
                    s3_client.delete_bucket(Bucket=bucket_name)
                    break
                except ClientError as ce:
                    if ce.response['Error']['Code'] != 'BucketNotEmpty':
                        raise
                    time.sleep(2)
            else:
                assert False, f"Failed to delete S3 bucket {bucket_name} within the timeout of 1 minute"


# This creates an empty table with string partition key and no sort key. We do
# not use the shared test_table_s fixture because exports may take longer for a
# table with data written by other tests.
@pytest.fixture(scope='module')
def test_table_s_for_export_only(dynamodb):
    table = create_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }, ],
        AttributeDefinitions=[ { 'AttributeName': 'p', 'AttributeType': 'S' } ])
    try:
        enable_pitr(table)
    except Exception:
        table.delete()
        raise
    yield table
    table.delete()


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
                # This is run against AWS (see `if` above), which is slow, so no point in testing often
                time.sleep(2)
            else:
                raise
    else:
        raise TimeoutError(f"UpdateContinuousBackups did not succeed on {table.name} within {timeout}s")
    # Wait until PITR is actually active — there is a propagation delay on AWS.
    while time.time() < deadline:
        resp = client.describe_continuous_backups(TableName=table.name)
        pitr = resp['ContinuousBackupsDescription'].get('PointInTimeRecoveryDescription', {})
        if pitr.get('PointInTimeRecoveryStatus') == 'ENABLED':
            return client
        # This is run against AWS (see `if` above), which is slow, so no point in testing often
        time.sleep(2)
    raise TimeoutError(f"PITR did not become ENABLED on {table.name} within {timeout}s")


# Helper: wait for an export to reach a terminal status (COMPLETED or FAILED).
# My actual tests show export takes approximately 10-12 minutes on AWS hence hefty timeout.
def wait_for_export(client, export_arn):
    timeout = 60 * 60 if is_aws(client) else 60
    sleep_time = 5 if is_aws(client) else 0.1
    deadline = time.time() + timeout
    while time.time() < deadline:
        desc = client.describe_export(ExportArn=export_arn)['ExportDescription']
        status = desc['ExportStatus']
        if status in ('COMPLETED', 'FAILED'):
            return desc
        time.sleep(sleep_time)
    raise TimeoutError(f"Export {export_arn} did not complete within {timeout}s")


# ---------------------------------------------------------------------------
# ExportTableToPointInTime tests
# ---------------------------------------------------------------------------

# Test that ExportTableToPointInTime starts an export and returns an
# ExportDescription with expected fields.
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
        assert 'ExportType' not in final # default is FULL_EXPORT, but AWS does not return it in the response for some reason.


# Test that ExportTableToPointInTime starts an export and then table is deleted,
# after which ListExports will not return the export (with TableArn and TableId), but DescribeExport will work.
# Amazon seems to delete the table asynchronously - after the delete request, the table exists for some time and export still goes on
# (IN_PROGRESS status). Once the table gets actually deleted the export status (asynchronously) goes to FAILED / COMPLETED.
# Note, that it's theoritically possible in this test to complete an export before the table actually gets deleted.
def test_export_start_and_delete_table(dynamodb):
    s3 = make_s3_client(dynamodb)
    with new_s3_bucket(s3) as bucket:
        with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
        ) as table:
            client = enable_pitr(table)
            table_arn = get_table_arn(table)

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

        now = time.time()
        while time.time() < now + 60:
            if is_table_deleted(client, table.name):
                break
            time.sleep(2 if is_aws(client) else 0.1)
        else:
            assert False, f"Table {table.name} should be deleted after exiting the context manager"

        # For some reason Amazon doesn't list the export in ListExports.
        for ex in iterate_over_exports(client, table_arn):
            if ex['ExportArn'] == export_arn:
                assert False, f"Export {export_arn} found in list_exports"

        desc = client.describe_export(ExportArn=export_arn)['ExportDescription']
        assert desc['ExportArn'] == export_arn
        assert desc['TableArn'] == table_arn
        assert desc['S3Bucket'] == bucket
        # We might be fast enough here to still get IN_PROGRESS (rare, but happens)
        assert desc['ExportStatus'] in ('IN_PROGRESS', 'COMPLETED', 'FAILED')
        assert 'StartTime' in desc
        assert 'ExportFormat' in desc


# Test that ExportTableToPointInTime starts an export and returns an
# ExportDescription with expected fields. Wait for completion and verify that final description
# is also valid.
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
        assert final['ExportType'] == 'FULL_EXPORT'

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
# Test for ION will skip S3 file content verification for now until we get python ion library set up for testing -
#   directory content will still be verified and as will be manifest files.
# Test is parameterized to run with both supported export formats and with/without S3 prefix.
# A prefix that ends with '/' must not add an empty directory before "AWSDynamoDB".
# The empty table is tested only once, as DYNAMODB_JSON without a prefix.
@pytest.mark.parametrize('num_items,export_format_value,s3_prefix_value',
    [(10, f, p) for f in ['DYNAMODB_JSON', 'ION'] for p in [None, 'dira/dirb/dirc', 'dira/dirb/']] +
    [(0, 'DYNAMODB_JSON', None)])
def test_export_with_data_full(dynamodb, s3_prefix_value, export_format_value, num_items, request):
    if export_format_value == 'ION' and not is_aws(dynamodb):
        request.node.add_marker(pytest.mark.xfail(reason="ION export format not yet implemented on Scylla"))
    with new_test_table(dynamodb,
        KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
    ) as table:
        # Insert some items
        inserted_items_as_text = []

        for i in range(num_items):
            table.put_item(Item={'p': f'item{i}',
                                 'd01': f'value{i}',
                                 'd02': (i + 10),
                                 'd03': b"qwerty" + str(i).encode('utf-8'),
                                 'd04': { 'a' + str(i), 'b' + str(i), 'c' + str(i) },
                                 'd05': { decimal.Decimal(1.5 + i), decimal.Decimal(2.5 + i), decimal.Decimal(3.875 + i) },
                                 'd06': { b'qwe' + str(i).encode('utf-8'), b'rty' + str(i).encode('utf-8'), b'abc' + str(i).encode('utf-8') },
                                 'd07': { str(i):i - 10, str(i + 10): i - 20 },
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
            # Initialize an export
            args = {}
            if s3_prefix_value is not None:
                args['S3Prefix'] = s3_prefix_value
            response = client.export_table_to_point_in_time(
                TableArn=table_arn,
                S3Bucket=bucket,
                ExportFormat=export_format_value,
                **args
            )


            # Wait for a completion of the export (successful).
            final = wait_for_export(client, response['ExportDescription']['ExportArn'])

            assert final['ExportStatus'] == 'COMPLETED'
            assert final['ItemCount'] == num_items

            # Download all exported files as a in-memory dictionary representing a directory structure.
            root_dir = downloaded_root_dir = fetch_all_exported_items(s3, bucket)

            def simplify_downloaded_dir(dr):
                for k, v in dr.items():
                    if isinstance(v, dict):
                        simplify_downloaded_dir(v)
                    else:
                        dr[k] = ''

            if s3_prefix_value:
                # Every entry must start with `s3_prefix_value`. Skip empty parts (from a
                # trailing '/').
                for p in filter(None, s3_prefix_value.split('/')):
                    if p not in root_dir:
                        # Something is wrong, so we will print whole tree for easier debugging.
                        # since values in the tree contain a lot of data, which is not really useful,
                        # we update all values to an empty string.
                        simplify_downloaded_dir(downloaded_root_dir)
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

            # Validate downloaded directory structure.

            # Must start with `AWSDynamoDB`
            assert set(root_dir.keys()) == { 'AWSDynamoDB' }
            root_dir = root_dir['AWSDynamoDB']

            # The next directory is the export id: the part of the ExportArn after `/export/`.
            assert len(root_dir) == 1
            export_id = final['ExportArn'].rsplit('/export/', 1)[1]
            assert set(root_dir.keys()) == {export_id}, root_dir.keys()
            root_dir = root_dir[export_id]

            # Top level content directory (the one with `data` and `_started`)
            assert set(root_dir.keys()) == { 'data', 'manifest-files.json', 'manifest-files.md5', 'manifest-summary.json', 'manifest-summary.md5', '_started' }

            data_dir = root_dir['data']
            items = []

            # Helper to sort a field in a nested dictionary by a dot-separated path. This is used to sort sets (SS, NS, BS) before comparing exported items with inserted items.
            # AWS will return sets in a random order.
            def sort_field(obj, field_path):
                field_list = field_path.split('.')
                for f in field_list[:-1]:
                    obj = obj.get(f, None)
                    if type(obj) is not dict:
                        # We will return here - something is wrong as we can't find our object
                        # but the text comparison assert will fail as well and printed object there should be easier to decipher.
                        return
                val = obj[field_list[-1]]
                if type(val) is list:
                    val = list(sorted(val))
                    obj[field_list[-1]] = val

            # There might be any number of files and some of them might be empty, we need to read them all to verify that they contain the expected items.
            # We also need to count items in each file to verify manifest file later on.
            item_count_map = collections.defaultdict(int)
            for data_file, data_file_content in data_dir.items():
                # For now we validate content file only for json format.
                if export_format_value == 'DYNAMODB_JSON':
                    # Decipher and read file with items. Insert read items into `items` list and count them in `item_count_map`.
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


            # We've read all files with data content, so `items` should contain all items exported and `item_count_map` should contain count of items in each file
            # (that will be checked later).
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

            # Verify two main manifest files in `data` directory (each has their own md5 file).
            for path1, path2 in (
                ('manifest-files.json', 'manifest-files.md5'),
                ('manifest-summary.json', 'manifest-summary.md5'),
            ):
                expected_md5 = root_dir[path2].strip()
                calculated_md5 = calculate_md5(root_dir[path1])
                assert calculated_md5 == expected_md5, f"{calculated_md5} != {expected_md5} for file {path1}"

            # Verify main manifest files content.
            manifest_files_json = root_dir['manifest-files.json'].decode('utf-8')
            total_real_item_count = 0
            for line in manifest_files_json.splitlines():
                js = json.loads(line.strip())
                assert 'itemCount' in js
                assert 'dataFileS3Key' in js
                assert 'etag' in js
                assert 'md5Checksum' in js

                # Path to the data file seems to be relative from bucket root and should start with `<optionally s3_prefix_value/>AWSDynamoDB/`.
                # The path itself is verified by the call to `get_content_by_path` later on.
                p = js['dataFileS3Key']
                if s3_prefix_value is None:
                    assert p.startswith('AWSDynamoDB/')
                else:
                    t = s3_prefix_value + ('' if s3_prefix_value.endswith('/') else '/')
                    assert p.startswith(t + 'AWSDynamoDB/')

                # We counted items in each file while reading them, now we verify that the count matches the manifest file.
                # This is done only for json export format.
                if export_format_value == 'DYNAMODB_JSON':
                    real_item_count = item_count_map[p.split('/')[-1]]
                    assert js['itemCount'] == real_item_count
                    total_real_item_count += real_item_count
                # The etag in the manifest has to be the one S3 reports for the data file. The
                # file is uploaded in multiple parts, so its etag is not the md5 of the content
                # and Scylla cannot compute it on its own - see `s3_storage_sink` in
                # alternator/export.cc.
                s3_etag = s3.head_object(Bucket=bucket, Key=p)['ETag'].strip('"')
                assert js['etag'] == s3_etag, f"{js['etag']} != {s3_etag} for file {p}"

                # There is md5 file for each data file, we verify that the md5 in manifest matches the calculated md5 of the downloaded file.
                md5_checksum = js['md5Checksum'].encode('utf-8')
                assert md5_checksum
                calculated_md5 = calculate_md5(get_content_by_path(p))
                assert calculated_md5 == md5_checksum, f"{calculated_md5} != {md5_checksum} for file {p}"

            # Verify total item count in manifest files matches the number of items inserted into the table.
            # We count items only with json export format.
            if export_format_value == 'DYNAMODB_JSON':
                assert total_real_item_count == num_items

            # Read and verify manifest-summary.json file.
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
            assert get_content_by_path(manifest_summary['manifestFilesS3Key']) == root_dir['manifest-files.json']

            assert 'billedSizeBytes' in manifest_summary
            assert manifest_summary['billedSizeBytes'] >= 0

            assert 'itemCount' in manifest_summary
            assert manifest_summary['itemCount'] == num_items

            assert 'outputFormat' in manifest_summary
            assert manifest_summary['outputFormat'] == export_format_value

# Test that sending the same ClientToken twice returns the same export.
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


# Test that a retry finds its export after the source table was dropped. scylla_only, because
# DynamoDB deletes a table asynchronously, so the table can still exist at the retry.
def test_export_client_token_idempotent_after_table_deleted(dynamodb, scylla_only):
    s3 = make_s3_client(dynamodb)
    token = str(uuid.uuid4())

    with new_s3_bucket(s3) as bucket:
        with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
        ) as table:
            client = enable_pitr(table)
            request = {'TableArn': get_table_arn(table), 'S3Bucket': bucket, 'ClientToken': token}
            desc1 = client.export_table_to_point_in_time(**request)['ExportDescription']

        now = time.time()
        while not is_table_deleted(client, table.name):
            assert time.time() < now + 60, f"Table {table.name} should be deleted after exiting the context manager"
            time.sleep(0.1)

        desc2 = client.export_table_to_point_in_time(**request)['ExportDescription']
        assert desc2['ExportArn'] == desc1['ExportArn']
        assert desc2['ClientToken'] == token


# Test that a retry with the generated ClientToken of a request without a token returns the same
# export. We send the first request manually, because boto adds a ClientToken if there is none.
def test_export_generated_client_token_idempotent(dynamodb, test_table_s):
    table = test_table_s
    client = enable_pitr(table)
    table_arn = get_table_arn(table)
    s3 = make_s3_client(dynamodb)

    with new_s3_bucket(s3) as bucket:
        payload = f'{{"TableArn": "{table_arn}", "S3Bucket": "{bucket}"}}'
        desc1 = manual_request(dynamodb, 'ExportTableToPointInTime', payload)['ExportDescription']
        token = desc1['ClientToken']
        assert token
        desc2 = client.export_table_to_point_in_time(
            TableArn=table_arn,
            S3Bucket=bucket,
            ClientToken=token,
        )['ExportDescription']
        assert desc2['ExportArn'] == desc1['ExportArn']
        assert desc2['ClientToken'] == token


# Send two requests with the same client token and the same payload, but a different order of fields.
def test_export_client_token_idempotent_different_order_of_members(dynamodb, test_table_s):
    table = test_table_s
    client = enable_pitr(table)
    table_arn = get_table_arn(table)
    s3 = make_s3_client(dynamodb)
    token = str(uuid.uuid4())

    with new_s3_bucket(s3) as bucket:
        # We do not use json.dumps, so that the order of the fields is known.
        payload = f"""{{
            "TableArn": "{table_arn}",
            "S3Bucket": "{bucket}",
            "ClientToken": "{token}"
        }}"""
        resp1 = manual_request(dynamodb, 'ExportTableToPointInTime', payload)

        # Same request, different order
        payload = f"""{{
            "ClientToken": "{token}",
            "S3Bucket": "{bucket}",
            "TableArn": "{table_arn}"
        }}"""
        resp2 = manual_request(dynamodb, 'ExportTableToPointInTime', payload)
        assert resp1['ExportDescription']['ExportArn'] == resp2['ExportDescription']['ExportArn']
        assert resp1['ExportDescription']['ClientToken'] == resp2['ExportDescription']['ClientToken'] == token



# Following tests check idempotency of export with respect to ClientToken - they verify that
# if we change some parameter of the export but keep the same ClientToken,
# the second call will fail with ExportConflictException.
# Note: we postpone test for `S3SseKmsKeyId` (due to complexity of setting up KMS)

# Test that trying to export a different table reusing the same client token fails with ExportConflictException.
def test_export_client_token_changed_export_format(dynamodb, test_table_s, test_table_s_2, request):
    if not is_aws(dynamodb):
        # Scylla does not support ION, and checks the parameters before the ClientToken. Thus
        # the second request gets ValidationException.
        request.node.add_marker(pytest.mark.xfail(reason="ION export format not yet implemented on Scylla"))
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


# Test that trying to export the table twice using the same client token but changing some parameter value fails with ExportConflictException.
@pytest.mark.parametrize("param_values", [
    ('ExportFormat', 'DYNAMODB_JSON', 'ION'),
    ('S3Bucket', unique_bucket_name(), unique_bucket_name() + 'a' ),
    ('S3BucketOwner', None, '000000000000'),
    ('S3Prefix', None, 'prefix' ),
    ('S3SseAlgorithm', 'AES256', 'KMS'),
])
def test_export_client_token_some_value_changed(dynamodb, test_table_s, param_values, request):
    param_name, first_param_value, second_param_value = param_values

    # Scylla checks the parameters before the ClientToken. If Scylla does not support the changed
    # value, the second request gets ValidationException, not ExportConflictException.
    if param_name in ('ExportFormat', 'S3BucketOwner', 'S3SseAlgorithm') and not is_aws(dynamodb):
        request.node.add_marker(pytest.mark.xfail(
                reason=f"{param_name} value used here is not supported on Scylla"))

    client1 = enable_pitr(test_table_s)
    table_arn1 = get_table_arn(test_table_s)
    s3 = make_s3_client(dynamodb)
    token = str(uuid.uuid4())

    with new_s3_bucket(s3, bucket_name=(first_param_value if param_name == 'S3Bucket' else None)) as bucket:
        kwargs = {
            'S3Bucket': bucket,
        }
        kwargs[param_name] = first_param_value
        client1.export_table_to_point_in_time(
            TableArn=table_arn1,
            ClientToken=token,
            **{ k: v for k, v in kwargs.items() if v is not None }
        )
        with pytest.raises(ClientError, match='ExportConflictException.*Duplicate request detected'):
            kwargs = {
                'S3Bucket': bucket,
            }
            kwargs[param_name] = second_param_value
            client1.export_table_to_point_in_time(
                TableArn=table_arn1,
                ClientToken=token,
                **{ k: v for k, v in kwargs.items() if v is not None }
            )


# Test that setting fields to unsupported, weird values fail with consistent error.
# (<exception name>, <field name>, <error text>, <value>)
# exception name - Name of the exception, that should be raised
# field name - Name of the field that should be set to an invalid value
# error text - Text of the error message that should be raised
# value - value that should be set to the field. If None, the field will be not set at all.
# Both <exception name> and <error text> are must appear in the error message, first <exception name>, then (after possibly some text) <error text>.
# We can't add ExportArn field testing here - boto3 raises strange exception (looks like a bug in boto3).
@pytest.mark.parametrize('exception_field_error_and_value', [
    ('ValidationException', 'ExportType', '[eE]xportType', 'QWERTY'),
    ('ValidationException', 'ExportType', '[eE]xportType', ''),
    ('ValidationException', 'ClientToken', '[cC]lientToken', ''),
    ('ValidationException', 'ExportFormat', '[eE]xportFormat', ''),
    ('ValidationException', 'ExportFormat', '[eE]xportFormat', 'QWERTY'),
    ('InvalidExportTimeException', 'ExportTime', 'Export ?Time', -1),
    ('ValidationException', 'S3Prefix', '[sS]3Prefix', ''),
    ('ValidationException', 'S3Bucket', '[sS]3Bucket', ''),
    ('ValidationException', 'S3Bucket', '[sS]3Bucket', None),
    ('ValidationException', 'TableArn', '[tT]ableArn', None),
    ('ValidationException', 'TableArn', '[tT]ableArn', ''),
    ('ValidationException', 'TableArn', '[tT]ableArn', '\t'),
    ('ValidationException', 'TableArn', '[tT]ableArn', 'QWERTY'),
    ('ValidationException', 'S3BucketOwner', '[sS]3BucketOwner', ''),
    ('ValidationException', 'S3SseAlgorithm', '[sS]3SseAlgorithm', ''),
    ('ValidationException', 'S3SseAlgorithm', '[sS]3SseAlgorithm', 'QWERTY'),
])
def test_export_setting_fields_to_weird_values_or_skipping_fail(dynamodb, test_table_s, exception_field_error_and_value):
    client = enable_pitr(test_table_s)
    table_arn = get_table_arn(test_table_s)
    exception, field, error, value = exception_field_error_and_value
    with pytest.raises(ClientError, match=f'{exception}.*({error})'):
        kw = {
            'TableArn': table_arn,
            'S3Bucket': unique_bucket_name()
        }
        kw[field] = value
        client.export_table_to_point_in_time(**{ k:v for k, v in kw.items() if v is not None })


# S3Bucket and S3Prefix end up in the path of the requests which the export sends to S3, so a
# character which has a meaning in an HTTP request line - a space, a CR or a LF - must not be
# accepted: it would otherwise let the request add requests of its own. DynamoDB rejects such a
# bucket name as well, as it accepts only letters, digits, '.', '-' and '_' in it.
@pytest.mark.parametrize('bucket', [
    'alternator export test',
    'alternator-export-test\r\nGET /other HTTP/1.1\r\nHost: h\r\n\r\n',
    'alternator-export-test\n',
    'alternator/export/test',
])
def test_export_bad_s3_bucket(dynamodb, test_table_s, bucket):
    client = enable_pitr(test_table_s)
    table_arn = get_table_arn(test_table_s)
    with pytest.raises(ClientError, match='ValidationException.*[sS]3Bucket'):
        client.export_table_to_point_in_time(TableArn=table_arn, S3Bucket=bucket)


# The same for S3Prefix. DynamoDB takes any S3 key as a prefix, while Alternator accepts only the
# characters which S3 documents as safe in a key name, so this test is for Alternator only.
@pytest.mark.parametrize('prefix', [
    'alternator export test',
    'alternator-export-test\r\nGET /other HTTP/1.1\r\nHost: h\r\n\r\n',
    'alternator-export-test\n',
    'alternator?export=test',
])
def test_export_bad_s3_prefix(dynamodb, test_table_s, prefix):
    if is_aws(dynamodb):
        skip_env('DynamoDB accepts any S3 key as S3Prefix')
    client = enable_pitr(test_table_s)
    table_arn = get_table_arn(test_table_s)
    with pytest.raises(ClientError, match='ValidationException.*[sS]3Prefix'):
        client.export_table_to_point_in_time(TableArn=table_arn,
                S3Bucket=unique_bucket_name(), S3Prefix=prefix)


# The same as test_export_setting_fields_to_weird_values_or_skipping_fail but only for S3SseKmsKeyId field
# as it requires S3SseAlgorithm to be set
@pytest.mark.parametrize('exception_field_error_and_value', [
    ('ValidationException', 'S3SseKmsKeyId', 's3SseKmsKeyId', ''),
])
@pytest.mark.xfail(reason="S3SseAlgorithm and S3SseKmsKeyId options are not supported on scylla")
def test_export_setting_fields_to_weird_values_or_skipping_fail_for_S3SseKmsKeyId(dynamodb, test_table_s, exception_field_error_and_value):
    client = enable_pitr(test_table_s)
    table_arn = get_table_arn(test_table_s)
    exception, field, error, value = exception_field_error_and_value
    with pytest.raises(ClientError, match=f'{exception}.*{error}'):
        kw = {
            'TableArn': table_arn,
            'S3Bucket': 'my-bucket',
            'S3SseAlgorithm': 'KMS',
        }
        kw[field] = value
        client.export_table_to_point_in_time(**{ k:v for k, v in kw.items() if v is not None })


# Test that trying to export the table with incremental export specifications times (ExportFromTime and ExportToTime) too close to each other -
# (less than 15 minutes) - should fail with InvalidExportTimeException
@pytest.mark.xfail(reason="INCREMENTAL_EXPORT is not supported on scylla")
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


# Test that trying to export the table twice using the same client token but different IncrementalExportSpecification fails with ExportConflictException.
@pytest.mark.xfail(reason="INCREMENTAL_EXPORT is not supported on scylla")
def test_export_client_token_changed_incremental_export_spec_fields(dynamodb, test_table_s, test_table_s_2):
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
        # ExportFromTime is different
        with pytest.raises(ClientError, match='ExportConflictException.*Duplicate request detected'):
            client2.export_table_to_point_in_time(
                TableArn=table_arn1,
                S3Bucket=bucket,
                ClientToken=token,
                ExportType='INCREMENTAL_EXPORT',
                IncrementalExportSpecification={
                    'ExportFromTime': export_time + 1,
                    'ExportToTime': export_time + min_export_time_diff + 1,
                    'ExportViewType': 'NEW_IMAGE',
                }
            )

        # ExportToTime is different
        with pytest.raises(ClientError, match='ExportConflictException.*Duplicate request detected'):
            client2.export_table_to_point_in_time(
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

        # ExportViewType is different
        with pytest.raises(ClientError, match='ExportConflictException.*Duplicate request detected'):
            client2.export_table_to_point_in_time(
                TableArn=table_arn1,
                S3Bucket=bucket,
                ClientToken=token,
                ExportType='INCREMENTAL_EXPORT',
                IncrementalExportSpecification={
                    'ExportFromTime': export_time,
                    'ExportToTime': export_time + min_export_time_diff + 1,
                    'ExportViewType': 'NEW_AND_OLD_IMAGES',
                }
            )


# Test that starts two exports on the same table (no ClientToken)
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


# Test the limit of concurrent exports on a shard. A request above the limit gets
# LimitExceededException. With the limit set to zero, all requests are above the limit. A rejected
# request writes nothing, so its ClientToken can start an export later.
def test_export_concurrency_limit(dynamodb, scylla_only, test_table_s):
    client = enable_pitr(test_table_s)
    table_arn = get_table_arn(test_table_s)
    token = str(uuid.uuid4())
    # Skip if there is no S3 server: without an S3 endpoint, all exports are refused before
    # the limit check.
    make_s3_client(dynamodb)

    with scylla_config_temporary(dynamodb, 'alternator_max_concurrent_exports_per_shard', '0'):
        with pytest.raises(ClientError, match='LimitExceededException.*Too many exports'):
            client.export_table_to_point_in_time(
                TableArn=table_arn,
                S3Bucket=unique_bucket_name(),
                ClientToken=token,
            )

    # We only expect that this export is accepted. The bucket does not exist, so it fails fast.
    resp = client.export_table_to_point_in_time(
        TableArn=table_arn,
        S3Bucket=unique_bucket_name(),
        ClientToken=token,
    )
    assert resp['ExportDescription']['ExportStatus'] == 'IN_PROGRESS'
    # Wait for the export to finish, so that it does not hold a slot in a later test.
    assert wait_for_export(client, resp['ExportDescription']['ExportArn'])['ExportStatus'] == 'FAILED'


# Stops each export during its scan (in the item handler of run_export()), so that a test can
# keep a known number of exports running. The context manager yields two functions: one waits
# until `count` exports are stopped, the other releases them. The injection is entered once for
# each item, so each table must have one item. On exit, the injection is disabled, which
# releases all exports.
EXPORT_PAUSE_INJECTION = 'alternator_export_to_s3_pause_in_item_handler'

@contextmanager
def paused_exports(rest_api):
    def enter_count():
        response = requests.get(f'{rest_api}/v2/error_injection/injection/{EXPORT_PAUSE_INJECTION}/enters', timeout=30)
        response.raise_for_status()
        return int(response.json())
    def wait_until_paused(count, timeout=60):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if enter_count() >= count:
                return
            time.sleep(0.05)
        pytest.fail(f'{count} exports did not reach the pause within {timeout}s')
    # The exports share the messages, so one message releases all of them. A later export
    # does not stop.
    def release_paused():
        requests.post(f'{rest_api}/v2/error_injection/injection/{EXPORT_PAUSE_INJECTION}/message', timeout=30).raise_for_status()
    with scylla_inject_error(rest_api, EXPORT_PAUSE_INJECTION, one_shot=False):
        yield wait_until_paused, release_paused


# Test that a running export holds a slot of its shard and releases it when it ends. With a limit
# of one export per shard, start exports until one is rejected. Then release the stopped exports.
# After they complete, a new export must be accepted. If a slot leaks, the new export is rejected.
def test_export_paused_export_holds_its_slot(dynamodb, scylla_only, rest_api):
    s3 = make_s3_client(dynamodb)
    with new_test_table(dynamodb,
        KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
    ) as table:
        # One item, so that an export stops only once (see paused_exports()).
        table.put_item(Item={'p': 'item'})
        client = table.meta.client
        table_arn = get_table_arn(table)
        def start_export(bucket):
            return client.export_table_to_point_in_time(
                TableArn=table_arn,
                S3Bucket=bucket,
            )['ExportDescription']['ExportArn']

        with new_s3_bucket(s3) as bucket, \
             scylla_config_temporary(dynamodb, 'alternator_max_concurrent_exports_per_shard', '1'):
            with paused_exports(rest_api) as (wait_until_paused, release_paused):
                running = []
                # The shard of the connection serves the request. The requests below use one
                # connection, so usually the second request is rejected. The loop does not
                # require this.
                max_requests = 64
                for _ in range(max_requests):
                    try:
                        running.append(start_export(bucket))
                    except ClientError as e:
                        assert e.response['Error']['Code'] == 'LimitExceededException', e
                        break
                else:
                    pytest.fail(f'No export was rejected in {max_requests} requests, so no shard reached its limit of one')
                assert running
                wait_until_paused(len(running))
                release_paused()
                for export_arn in running:
                    assert wait_for_export(client, export_arn)['ExportStatus'] == 'COMPLETED'
            # The injection is disabled, so this export does not stop. If it is accepted, the
            # exports above released their slots. The fiber releases the slot shortly after it
            # writes COMPLETED, so retry until the deadline.
            deadline = time.time() + 30
            while True:
                try:
                    export_arn = start_export(bucket)
                    break
                except ClientError as e:
                    assert e.response['Error']['Code'] == 'LimitExceededException', e
                    assert time.time() < deadline, 'The slots of the completed exports were not given back within 30s'
                    time.sleep(0.05)
            assert wait_for_export(client, export_arn)['ExportStatus'] == 'COMPLETED'


# Test that trying to export from two tables using the same client token fails.
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
def test_export_nonexistent_table(dynamodb, test_table_s):
    client = dynamodb.meta.client
    fake_arn = get_table_arn(test_table_s) + '_nonexistent'

    with pytest.raises(ClientError, match='TableNotFoundException.*not found'):
        client.export_table_to_point_in_time(
            TableArn=fake_arn,
            S3Bucket=unique_bucket_name(),
        )


# Test that exporting to a nonexistent S3 bucket fails.
# This one is little different, as - it seems - DynamoDB actually accepts nonexistent bucket and starts the export,
# returns the FAILED and an error message later on.
def test_export_nonexistent_bucket(dynamodb, test_table_s):
    table = test_table_s
    client = enable_pitr(table)
    table_arn = get_table_arn(table)
    # Skip if there is no S3 server: then the export fails with a different
    # FailureCode.
    make_s3_client(dynamodb)
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

    assert_listed_with_status(client, table_arn, export_arn, 'FAILED')


# Test that a request that fails validation does not reserve its ClientToken. After it, ListExports
# shows no new export, and the corrected request with the same token starts an export.
@pytest.mark.parametrize('invalid', ['S3Bucket', 'ExportFormat', 'ExportTime'])
def test_export_failed_does_not_reserve_client_token(dynamodb, test_table_s, invalid):
    table = test_table_s
    client = enable_pitr(table)
    table_arn = get_table_arn(table)
    s3 = make_s3_client(dynamodb)
    token = str(uuid.uuid4())
    invalid_value = {
        'S3Bucket': '',
        'ExportFormat': 'QWERTY',
        'ExportTime': datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=10),
    }[invalid]

    with new_s3_bucket(s3) as bucket:
        request = {'TableArn': table_arn, 'S3Bucket': bucket, 'ClientToken': token}
        listed_before = {ex['ExportArn'] for ex in iterate_over_exports(client, table_arn)}

        with pytest.raises(ClientError, match='ValidationException|InvalidExportTimeException'):
            client.export_table_to_point_in_time(**{**request, invalid: invalid_value})

        # The rejected request did not write an export...
        assert {ex['ExportArn'] for ex in iterate_over_exports(client, table_arn)} == listed_before
        # ...and did not reserve its ClientToken, so the corrected request starts an export.
        desc = client.export_table_to_point_in_time(**request)['ExportDescription']
        assert desc['ClientToken'] == token
        assert desc['ExportArn'] not in listed_before


# Test that a retry gets IN_PROGRESS with the reserved ARN, if the ClientToken row exists but the
# export row does not (a different node inserted the ClientToken row but not yet the export row).
# To make this state, we delete the export row of a finished export.
def test_export_client_token_without_export_row(dynamodb, cql, test_table_s):
    table = test_table_s
    client = enable_pitr(table)
    table_arn = get_table_arn(table)
    s3 = make_s3_client(dynamodb)
    token = str(uuid.uuid4())

    with new_s3_bucket(s3) as bucket:
        request = {'TableArn': table_arn, 'S3Bucket': bucket, 'ClientToken': token}
        started = client.export_table_to_point_in_time(**request)['ExportDescription']
        export_arn = started['ExportArn']
        wait_for_export(client, export_arn)
        # The finished row must keep its TTL.
        ttl = cql.execute(SimpleStatement(
            "SELECT TTL(export_status), TTL(client_token) FROM system_distributed.alternator_export_to_s3_exports WHERE export_arn = %s",
            consistency_level=ConsistencyLevel.ONE), [export_arn]).one()
        assert 89 * 86400 < ttl[0] <= 90 * 86400 and 89 * 86400 < ttl[1] <= 90 * 86400, ttl
        cql.execute(SimpleStatement(
            "DELETE FROM system_distributed.alternator_export_to_s3_exports WHERE export_arn = %s",
            consistency_level=ConsistencyLevel.ONE), [export_arn])

        desc = client.export_table_to_point_in_time(**request)['ExportDescription']
        assert desc['ExportArn'] == export_arn
        assert desc['ExportStatus'] == 'IN_PROGRESS'
        assert desc['ClientToken'] == token
        # The times are those of the original export, not of this retry.
        assert desc['StartTime'] == started['StartTime']
        assert desc['ExportTime'] == started['ExportTime']

# ---------------------------------------------------------------------------
# DescribeExport tests
# ---------------------------------------------------------------------------

# Test that DescribeExport returns the full ExportDescription for a
# known export.
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


# An export id in the DynamoDB format: the StartTime in milliseconds since the epoch (14 digits),
# a dash, and 8 hex digits. `accepted_at` is the StartTime in seconds since the epoch.
def export_id_accepted_at(accepted_at):
    return f'{int(accepted_at * 1000):014d}-{uuid.uuid4().hex[:8]}'


# An export id that no export has.
def unknown_export_id():
    return export_id_accepted_at(time.time())


# Test that DescribeExport rejects with ValidationException an ARN which carries no export id: an
# absent or empty one, the ARN of a table, whether the table exists or not, and a table ARN which
# stops at `/export/`.
def test_describe_export_no_export_id(test_table_s):
    client = test_table_s.meta.client
    table_arn = get_table_arn(test_table_s)
    with pytest.raises(ClientError, match='ValidationException.*Export ?(ARN|Arn)'):
        client.describe_export()
    with pytest.raises(ClientError, match='ValidationException.*Export ?(ARN|Arn)'):
        client.describe_export(ExportArn='')
    with pytest.raises(ClientError, match='ValidationException.*Invalid Export ARN'):
        client.describe_export(ExportArn=table_arn.rsplit('/', 1)[0] + '/nonexistent_table_xyz')
    with pytest.raises(ClientError, match='ValidationException.*Invalid Export ARN'):
        client.describe_export(ExportArn=table_arn)
    with pytest.raises(ClientError, match='ValidationException.*Invalid Export ARN'):
        client.describe_export(ExportArn=table_arn + '/export/')


# Test that DescribeExport reports a well-formed ARN of an export that was never started as
# ExportNotFoundException, whether the table it names exists or not.
def test_describe_export_unknown(test_table_s):
    client = test_table_s.meta.client
    table_arn = get_table_arn(test_table_s)
    for export_arn in [f'{table_arn}/export/{unknown_export_id()}', f'{table_arn}_nonexistent/export/{unknown_export_id()}']:
        with pytest.raises(ClientError, match=f'ExportNotFoundException.*{re.escape(export_arn)}') as error:
            client.describe_export(ExportArn=export_arn)
        assert error.value.response['ResponseMetadata']['HTTPStatusCode'] == 400


# An ARN with the part at `index` - arn, partition, service, region, account, resource - replaced by
# `value`, or dropped if `value` is None.
def arn_with_part(arn, index, value):
    parts = arn.split(':', 5)
    parts[index:index + 1] = [] if value is None else [value]
    return ':'.join(parts)


# The ARN of an export of `table` that does not exist; `table` itself may exist. On Alternator, the
# ARN has the format of the export ARNs that ExportTableToPointInTime returns, not of the table ARN.
def unknown_export_arn(table):
    table_arn = get_table_arn(table)
    if table_arn.startswith('arn:scylla:'):
        keyspace = table_arn.split(':')[3]
        table_arn = f'arn:aws:dynamodb:us-east-1:000000000000:table/{keyspace}@{table.name}'
    return f'{table_arn}/export/{unknown_export_id()}'


# Test that DescribeExport reports as ValidationException an export ARN without the `arn:` prefix,
# one with too few parts, one with a different resource type, and one with an incorrect export id
# (the correct format is 14 digits, a dash and 8 hex digits).
def test_describe_export_incorrect_arns(test_table_s):
    client = test_table_s.meta.client
    export_arn = unknown_export_arn(test_table_s)
    with pytest.raises(ClientError, match='ValidationException.*Invalid Export ARN'):
        client.describe_export(ExportArn=arn_with_part(export_arn, 0, 'qwerty'))  # arn -> qwerty
    with pytest.raises(ClientError, match='ValidationException.*Invalid Export ARN'):
        client.describe_export(ExportArn=arn_with_part(export_arn, 4, None))  # account dropped
    with pytest.raises(ClientError, match='ValidationException.*Invalid Export ARN'):
        client.describe_export(ExportArn=export_arn.replace(':table/', ':qwerty/', 1))
    table_arn, export_id = export_arn.rsplit('/export/', 1)
    digits, hex_digits = export_id.split('-')
    for incorrect_id in ['abc',
                         f'{digits[:-1]}-{hex_digits}',  # 13 digits
                         f'{digits}0-{hex_digits}',      # 15 digits
                         f'{digits}-{hex_digits[:-1]}',  # 7 hex digits
                         f'{digits}-{hex_digits}0',      # 9 hex digits
                         f'{digits}_{hex_digits}',       # `_` instead of the dash
                         'a/b', '/', f'{export_id}/']:
        with pytest.raises(ClientError, match='ValidationException.*Export ?(ARN|Arn)'):
            client.describe_export(ExportArn=f'{table_arn}/export/{incorrect_id}')


# Test that DescribeExport reports as ValidationException an export ARN naming another partition,
# service or region, and as AccessDeniedException one naming another account.
@pytest.mark.xfail(reason='Alternator does not check the partition, service, region or account of an ARN')
def test_describe_export_incorrect_arn_parts(test_table_s):
    client = test_table_s.meta.client
    export_arn = unknown_export_arn(test_table_s)
    with pytest.raises(ClientError, match='ValidationException.*Export ARN'):
        client.describe_export(ExportArn=arn_with_part(export_arn, 1, 'qwerty'))  # partition -> qwerty
    with pytest.raises(ClientError, match='ValidationException.*Export ARN'):
        client.describe_export(ExportArn=arn_with_part(export_arn, 2, 'qwerty'))  # service -> qwerty
    with pytest.raises(ClientError, match='ValidationException.*Export ARN'):
        client.describe_export(ExportArn=arn_with_part(export_arn, 3, 'qwerty'))  # region -> qwerty
    with pytest.raises(ClientError, match='AccessDeniedException'):
        client.describe_export(ExportArn=arn_with_part(export_arn, 4, '123456789012'))  # account -> 123456789012


# Test that DescribeExport reports as ValidationException an export ARN that names a table which
# no table can be. A table name is not valid if it has fewer than 3 characters, more than 255
# characters, or a character other than a-z, A-Z, 0-9, `_`, `-` and `.`. The test also uses an ARN
# with `@` before the table name. For Alternator, the keyspace name in that ARN is empty. For
# DynamoDB, `@` is a character that a table name cannot have. A valid table name gives
# ExportNotFoundException. The test does not use the lengths from 193 to 255, because Alternator
# does not accept them as table names (issue #4480).
def test_describe_export_invalid_table_name(test_table_s):
    client = test_table_s.meta.client
    export_arn = unknown_export_arn(test_table_s)
    for name in ['ab', 'a' * 256, 'a b']:
        with pytest.raises(ClientError, match='ValidationException.*Invalid Export ARN'):
            client.describe_export(ExportArn=export_arn.replace(test_table_s.name, name))
    with pytest.raises(ClientError, match='ValidationException.*Invalid Export ARN'):
        client.describe_export(ExportArn=re.sub('table/[^/]*/', 'table/@abc/', export_arn))
    for name in ['abc', 'a' * 192]:
        with pytest.raises(ClientError, match='ExportNotFoundException'):
            client.describe_export(ExportArn=export_arn.replace(test_table_s.name, name))


# ---------------------------------------------------------------------------
# ListExports tests
# ---------------------------------------------------------------------------

# Test that ListExports returns an ExportSummaries list. In our case it must be empty
# as we use it on a freshly created table.
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


# Test that ListExports with the TableArn of a table that does not exist returns an empty body,
# without an ExportSummaries list. On Alternator, a table outside Alternator counts as one that does
# not exist.
def test_list_exports_nonexistent_table(dynamodb, test_table_s):
    client = dynamodb.meta.client
    response = client.list_exports(TableArn=get_table_arn(test_table_s) + '_nonexistent')
    assert 'ExportSummaries' not in response
    if not is_aws(dynamodb):
        assert 'ExportSummaries' not in client.list_exports(TableArn='arn:aws:dynamodb:us-east-1:000000000000:table/system@peers')


def iterate_over_exports(client, table_arn = None, max_results = None):
    kwargs = {}
    if table_arn is not None:
        kwargs['TableArn'] = table_arn
    if max_results is not None:
        kwargs['MaxResults'] = max_results

    while True:
        response = client.list_exports(**kwargs)
        results = response.get('ExportSummaries', ())
        if max_results is not None:
            assert len(results) <= max_results
        yield from results

        next_token = response.get('NextToken', None)
        if not next_token:
            break

        kwargs['NextToken'] = response['NextToken']


# Asserts that ListExports filtered by `table_arn` lists `export_arn` with `status`.
def assert_listed_with_status(client, table_arn, export_arn, status):
    for ex in iterate_over_exports(client, table_arn):
        if ex['ExportArn'] == export_arn:
            assert ex['ExportStatus'] == status
            return
    assert False, f"Export {export_arn} not found in list_exports"


# Test that ListExports reports ValidationException for: a MaxResults outside 1 to 25; a NextToken
# that is not an export ARN (a hex string, a table ARN, a table ARN that ends with `/export/`), or
# one of an export of another table than TableArn names; a TableArn that is not a table ARN (empty,
# too long, without the `arn:` prefix, with too few parts, with another resource type, or an export
# ARN).
def test_list_exports_invalid_parameter(test_table_s):
    client = test_table_s.meta.client
    table_arn = get_table_arn(test_table_s)
    export_arn = unknown_export_arn(test_table_s)
    exported_table_arn = export_arn.rsplit('/export/', 1)[0]
    with pytest.raises(ClientError, match='ValidationException.*[Mm]axResults'):
        client.list_exports(MaxResults=0)
    with pytest.raises(ClientError, match='ValidationException.*[Mm]axResults'):
        client.list_exports(MaxResults=26)
    with pytest.raises(ClientError, match='ValidationException.*[Nn]extToken') as error:
        client.list_exports(NextToken='0123456789abcdef' * 16)
    assert error.value.response['ResponseMetadata']['HTTPStatusCode'] == 400
    with pytest.raises(ClientError, match='ValidationException.*[Nn]extToken'):
        client.list_exports(NextToken=table_arn)
    with pytest.raises(ClientError, match='ValidationException.*[Nn]extToken'):
        client.list_exports(NextToken=table_arn + '/export/')
    with pytest.raises(ClientError, match='ValidationException.*[Nn]extToken'):
        client.list_exports(TableArn=exported_table_arn, NextToken=export_arn.replace(test_table_s.name, test_table_s.name + '_other'))
    with pytest.raises(ClientError, match='ValidationException.*[Tt]ableArn'):
        client.list_exports(TableArn='')
    with pytest.raises(ClientError, match='ValidationException.*[Tt]ableArn'):
        client.list_exports(TableArn='a' * 1025)
    with pytest.raises(ClientError, match='ValidationException.*[Tt]ableArn'):
        client.list_exports(TableArn=arn_with_part(exported_table_arn, 0, 'qwerty'))  # arn -> qwerty
    with pytest.raises(ClientError, match='ValidationException.*[Tt]ableArn'):
        client.list_exports(TableArn=arn_with_part(exported_table_arn, 4, None))  # account dropped
    with pytest.raises(ClientError, match='ValidationException.*[Tt]ableArn'):
        client.list_exports(TableArn=exported_table_arn.replace(':table/', ':qwerty/', 1))
    with pytest.raises(ClientError, match='ValidationException.*[Tt]ableArn'):
        client.list_exports(TableArn=export_arn)


# Test that ListExports reports SerializationException for a MaxResults that is not a number and for
# a NextToken that is not a string. botocore cannot send a TableArn that is not a string.
def test_list_exports_wrong_type(test_table_s):
    client = test_table_s.meta.client
    with pytest.raises(ClientError, match='SerializationException.*[Ii]nteger'):
        client.list_exports(MaxResults='5')
    with pytest.raises(ClientError, match='SerializationException.*[Ss]tring'):
        client.list_exports(NextToken=5)


# Test that ListExports truncates a fractional MaxResults and ignores a null parameter, as DynamoDB
# does: 25.9 is accepted as 25, 0.9 is rejected as 0.
def test_list_exports_fraction_and_null(test_table_s):
    client = test_table_s.meta.client
    client.list_exports(MaxResults=25.9)
    with pytest.raises(ClientError, match='ValidationException.*[Mm]axResults'):
        client.list_exports(MaxResults=0.9)
    client.list_exports(MaxResults=None, TableArn=None, NextToken=None)


# Test that ListExports reports as ValidationException a TableArn naming another partition, service
# or region, and as AccessDeniedException one naming another account.
@pytest.mark.xfail(reason='Alternator does not check the partition, service, region or account of an ARN')
def test_list_exports_incorrect_table_arn_parts(test_table_s):
    client = test_table_s.meta.client
    table_arn = unknown_export_arn(test_table_s).rsplit('/export/', 1)[0]
    with pytest.raises(ClientError, match='ValidationException.*Table ARN'):
        client.list_exports(TableArn=arn_with_part(table_arn, 1, 'qwerty'))  # partition -> qwerty
    with pytest.raises(ClientError, match='ValidationException.*Table ARN'):
        client.list_exports(TableArn=arn_with_part(table_arn, 2, 'qwerty'))  # service -> qwerty
    with pytest.raises(ClientError, match='ValidationException.*Table ARN'):
        client.list_exports(TableArn=arn_with_part(table_arn, 3, 'qwerty'))  # region -> qwerty
    with pytest.raises(ClientError, match='AccessDeniedException'):
        client.list_exports(TableArn=arn_with_part(table_arn, 4, '123456789012'))  # account -> 123456789012


# Test that ListExports reports as ValidationException a TableArn that names a table which no table
# can be. A table name is not valid if it has fewer than 3 characters, more than 255 characters, or
# a character other than a-z, A-Z, 0-9, `_`, `-` and `.`. The test also uses a TableArn with `@`
# before the table name. For Alternator, the keyspace name in that ARN is empty. For DynamoDB, `@`
# is a character that a table name cannot have. A valid name of a table that does not exist gives
# an empty body. The test does not use the lengths from 193 to 255, because Alternator does not
# accept them as table names (issue #4480).
def test_list_exports_invalid_table_name(test_table_s):
    client = test_table_s.meta.client
    table_arn = unknown_export_arn(test_table_s).rsplit('/export/', 1)[0]
    for name in ['ab', 'a' * 256, 'a b']:
        with pytest.raises(ClientError, match='ValidationException.*Invalid Table ARN'):
            client.list_exports(TableArn=table_arn.replace(test_table_s.name, name))
    with pytest.raises(ClientError, match='ValidationException.*Invalid Table ARN'):
        client.list_exports(TableArn=table_arn[:table_arn.index(':table/') + len(':table/')] + '@abc')
    for name in ['abc', 'a' * 192]:
        assert 'ExportSummaries' not in client.list_exports(TableArn=table_arn.replace(test_table_s.name, name))


# Test that after starting an export, ListExports includes it, both filtered by its table and
# not filtered at all.
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

        for filter_table_arn in [table_arn, None]:
            for e in iterate_over_exports(client, filter_table_arn):
                if e['ExportArn'] == export_arn:
                    break
            else:
                pytest.fail(f"Export {export_arn} not found in ListExports with TableArn={filter_table_arn}")


# Test that ListExports pagination via MaxResults and NextToken works.
def test_list_exports_pagination(dynamodb):
    # Skip if there is no S3 server: without an S3 endpoint, all exports are refused.
    make_s3_client(dynamodb)
    max_tables = 5
    exports_per_table = 2 if is_aws(dynamodb) else 5

    created_tables = []
    with ExitStack() as created_tables_context_manager:
        for _ in range(max_tables):
            created_tables.append(
                created_tables_context_manager.enter_context(new_test_table(dynamodb,
                    KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
                AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]))
            )

        export_arns = []
        # Start multiple exports to the same bucket with different prefixes
        # Note: we intentionally use the fake bucket, as it starts the export,
        # but fails it almost immediately, drastically reducing test runtime.
        # The exports will still be present in ListExports results (with status FAILED),
        # which is enough for testing pagination.
        for table in created_tables:
            client = enable_pitr(table)
            table_arn = get_table_arn(table)
            for _ in range(exports_per_table):
                resp = client.export_table_to_point_in_time(
                    TableArn=table_arn,
                    S3Bucket=unique_bucket_name(),
                    S3Prefix=f'prefix-{uuid.uuid4()}',
                    ClientToken=str(uuid.uuid4()),
                )
                export_arns.append(resp['ExportDescription']['ExportArn'])

        # List with max_results set to 4, so we get some pagination.
        all_arns_by_table = collections.defaultdict(list)
        all_arns = set()
        for summary in iterate_over_exports(client, max_results=4):
            assert 'ExportArn' in summary
            sm = summary["ExportArn"]
            all_arns.add(sm)

            # It seems ARNS are reversed (newest first), but only for the same table
            x = sm.find('/export/')
            assert x >= 0, sm
            arn_up_to_table = sm[:x]
            t = all_arns_by_table[arn_up_to_table]
            assert not t or sm < t[-1], (sm, t)
            t.append(sm)

        # we're not filtering, so we will get more stuff in the list, but at least all our exports should be there
        assert len(export_arns) <= len(all_arns)
        assert set(export_arns).issubset(set(all_arns))


# Puts two exports of `table` in the export history that ListExports reads, and returns their
# ARNs. DynamoDB gets real exports, into a bucket that does not exist, so they fail quickly but
# stay listed. Alternator gets their rows over CQL, so that they are in a known state.
@contextmanager
def two_listed_exports(request, table):
    client = table.meta.client
    table_arn = get_table_arn(table)
    if is_aws(client):
        yield [client.export_table_to_point_in_time(TableArn=table_arn, S3Bucket=unique_bucket_name(),
                   ClientToken=str(uuid.uuid4()))['ExportDescription']['ExportArn'] for _ in range(2)]
        return
    cql = request.getfixturevalue('cql')
    arns = [unstarted_export_arn(table_arn) for _ in range(2)]
    with ExitStack() as seeded:
        for export_arn in arns:
            seeded.enter_context(export_metadata(cql, export_arn, accepted_export_row(table_arn)))
        yield arns


# DynamoDB ties a NextToken to the TableArn filter of the listing that handed it out, and answers a
# request with another filter with ValidationException. Alternator serves such a request. The tests
# below record this difference and make sure that Alternator survives such requests: the xfail
# covers only a request that succeeds, so any other error, such as a crashed node, fails them.
next_token_tied_to_filter = pytest.mark.xfail(raises=pytest.fail.Exception,
    reason='Alternator does not tie NextToken to the TableArn filter of its listing')


# Test that a NextToken of a listing filtered by TableArn is rejected without the filter.
@next_token_tied_to_filter
def test_list_exports_filtered_next_token_without_filter(request, test_table_s_for_export_only):
    client = test_table_s_for_export_only.meta.client
    table_arn = get_table_arn(test_table_s_for_export_only)
    with two_listed_exports(request, test_table_s_for_export_only):
        next_token = client.list_exports(TableArn=table_arn, MaxResults=1)['NextToken']
        with pytest.raises(ClientError, match='ValidationException.*[Nn]ext[Tt]oken'):
            client.list_exports(MaxResults=1, NextToken=next_token)


# Test that a NextToken of an unfiltered listing is rejected with a TableArn filter, even one naming
# the table of the export the token ends on.
@next_token_tied_to_filter
def test_list_exports_unfiltered_next_token_with_filter(request, test_table_s_for_export_only):
    client = test_table_s_for_export_only.meta.client
    table_arn = get_table_arn(test_table_s_for_export_only)
    with two_listed_exports(request, test_table_s_for_export_only) as arns:
        # Page the unfiltered listing until a page ends on one of this table's exports.
        kwargs = {'MaxResults': 1}
        while True:
            response = client.list_exports(**kwargs)
            kwargs['NextToken'] = response['NextToken']
            if response['ExportSummaries'][0]['ExportArn'] in arns:
                break
        with pytest.raises(ClientError, match='ValidationException.*[Nn]ext[Tt]oken'):
            client.list_exports(TableArn=table_arn, MaxResults=1, NextToken=kwargs['NextToken'])


# Test that ExportSummary from ListExports contains the expected
# fields: ExportArn, ExportStatus, ExportType.
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
                assert 'ExportArn' in summary
                assert 'ExportStatus' in summary
                assert summary['ExportStatus'] == 'COMPLETED'
                assert 'ExportType' in summary
                assert summary['ExportType'] == 'FULL_EXPORT'
                break
        else:
            pytest.fail(f"Export {export_arn} not found in ListExports for table {table_arn}")


# Test that ExportTableToPointInTime accepts valid parameters.
# The bucket name below is not a bucket of the test. On AWS, it can belong to a different user, so
# this test is `scylla_only`. Scylla does not check the bucket when it accepts the request.
def test_export_table_basic(dynamodb, test_table_s_for_export_only, scylla_only):
    client = test_table_s_for_export_only.meta.client
    table_arn = get_table_arn(test_table_s_for_export_only)
    client_token = random_string(20)
    # Skip if there is no S3 server: without an S3 endpoint, the request is refused.
    make_s3_client(dynamodb)

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


# Test that ExportTableToPointInTime echoes ExportType only when the request carried one. The
# bucket does not exist, so on DynamoDB the export fails by itself without writing anything.
def test_export_table_export_type_echoed_only_when_requested(dynamodb, test_table_s_for_export_only):
    client = test_table_s_for_export_only.meta.client
    table_arn = get_table_arn(test_table_s_for_export_only)
    # Skip if there is no S3 server: without an S3 endpoint, the request is refused.
    make_s3_client(dynamodb)

    desc = client.export_table_to_point_in_time(TableArn=table_arn, S3Bucket=unique_bucket_name())['ExportDescription']
    assert 'ExportType' not in desc

    desc = client.export_table_to_point_in_time(TableArn=table_arn, S3Bucket=unique_bucket_name(),
                                                ExportType='FULL_EXPORT')['ExportDescription']
    assert desc['ExportType'] == 'FULL_EXPORT'


# Test that non-DYNAMODB_JSON format (ION) is rejected.
def test_export_table_unsupported_format_ion(test_table_s_for_export_only, scylla_only):
    client = test_table_s_for_export_only.meta.client
    table_arn = get_table_arn(test_table_s_for_export_only)

    with pytest.raises(ClientError, match='ValidationException.*[eE]xportFormat'):
        client.export_table_to_point_in_time(
            TableArn=table_arn,
            S3Bucket='my-bucket',
            ExportFormat='ION',
        )


# Test that call to UpdateContinuousBackups is rejected.
def test_update_continuous_backups_rejected(test_table_s_for_export_only, scylla_only):
    client = test_table_s_for_export_only.meta.client

    with pytest.raises(ClientError, match='UnknownOperationException.*UpdateContinuousBackups'):
        client.update_continuous_backups(
            TableName=test_table_s_for_export_only.name,
            PointInTimeRecoverySpecification={'PointInTimeRecoveryEnabled': True}
        )


# Test that incremental export is rejected.
def test_export_table_unsupported_incremental(test_table_s_for_export_only, scylla_only):
    client = test_table_s_for_export_only.meta.client
    table_arn = get_table_arn(test_table_s_for_export_only)

    with pytest.raises(ClientError, match='ValidationException.*[eE]xportType'):
        client.export_table_to_point_in_time(
            TableArn=table_arn,
            S3Bucket='my-bucket',
            ExportType='INCREMENTAL_EXPORT',
        )


# Test that IncrementalExportSpecification is rejected.
def test_export_table_unsupported_incremental_spec(test_table_s_for_export_only, scylla_only):
    client = test_table_s_for_export_only.meta.client
    table_arn = get_table_arn(test_table_s_for_export_only)

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
    table_arn = get_table_arn(test_table_s_for_export_only)

    with pytest.raises(ClientError, match=f'ValidationException.*{unsupported_parameter}'):
        client.export_table_to_point_in_time(
            TableArn=table_arn,
            S3Bucket='my-bucket',
            **{unsupported_parameter: value},
        )


# Test that ExportTime close to now is accepted, and reported rounded to the nearest millisecond, as
# DynamoDB does. boto3 sends an ExportTime in whole seconds, so the request is written out here. The
# bucket does not exist, so on DynamoDB the export fails by itself without writing anything.
def test_export_table_export_time_now(dynamodb, test_table_s_for_export_only):
    client = test_table_s_for_export_only.meta.client
    table_arn = get_table_arn(test_table_s_for_export_only)
    # Skip if there is no S3 server: without an S3 endpoint, the request is refused.
    make_s3_client(dynamodb)
    # DynamoDB exports only from its point-in-time recovery window, which can start after the
    # fixture sees recovery enabled. Alternator reports no such window.
    pitr = client.describe_continuous_backups(TableName=test_table_s_for_export_only.name)['ContinuousBackupsDescription']
    earliest = pitr['PointInTimeRecoveryDescription'].get('EarliestRestorableDateTime')
    second = max(int(time.time()) - 1, int(earliest.timestamp()) + 1 if earliest else 0)
    time.sleep(max(0, second + 1 - time.time()))
    payload = json.dumps({'TableArn': table_arn, 'S3Bucket': unique_bucket_name(), 'ExportTime': second + 0.1236,
                          'ClientToken': str(uuid.uuid4())})
    response = manual_request(dynamodb, 'ExportTableToPointInTime', payload)
    assert response['ExportDescription']['ExportTime'] == pytest.approx(second + 0.124, abs=1e-5)


# Test that ExportTime in the past (more than 5 minutes) is rejected.
def test_export_table_invalid_export_time_before_now(test_table_s_for_export_only, scylla_only):
    client = test_table_s_for_export_only.meta.client
    table_arn = get_table_arn(test_table_s_for_export_only)

    with pytest.raises(ClientError, match='InvalidExportTimeException.*Export ?Time'):
        client.export_table_to_point_in_time(
            TableArn=table_arn,
            S3Bucket='my-bucket',
            ExportTime=int(time.time()) - 60 * 5 - 1,
        )

# Test that ExportTime in the future (more than 5 minutes) is rejected.
# We add additional time (1 minute) to avoid a race (where enough time will pass before the request is processed).
def test_export_table_invalid_export_time_in_future(test_table_s_for_export_only, scylla_only):
    client = test_table_s_for_export_only.meta.client
    table_arn = get_table_arn(test_table_s_for_export_only)

    with pytest.raises(ClientError, match='InvalidExportTimeException.*Export ?Time'):
        client.export_table_to_point_in_time(
            TableArn=table_arn,
            S3Bucket='my-bucket',
            ExportTime=int(time.time()) + 60 * 5 + 60,
        )


# ---------------------------------------------------------------------------
# DescribeExport and ListExports tests driven from the export metadata table
# ---------------------------------------------------------------------------
# The tests below write rows to system_distributed.alternator_export_to_s3_exports over CQL, and
# check how DescribeExport and ListExports report them. A real export cannot make these rows on
# request: a status that DynamoDB does not have (FAILING), rows with known values in all columns,
# and corrupt rows. They run only on Scylla, because they use Alternator internal tables.

# An export ARN in the shape ExportTableToPointInTime hands out, for an export nobody started.
def unstarted_export_arn(table_arn):
    return f'{table_arn}/export/{unknown_export_id()}'


# The columns every export row carries from the moment the export is accepted, whatever state the
# export goes on to reach. The sub-second parts are halves and quarters of a second so that they
# survive the trip through a double exactly.
def accepted_export_row(table_arn, **extra_request_fields):
    return {
        'client_token': random_string(20),
        'request': json.dumps({'TableArn': table_arn, 'S3Bucket': 'my-bucket'} | extra_request_fields),
        'export_status': 'IN_PROGRESS',
        'table_id': uuid.uuid4(),
        'export_time': datetime.datetime(2026, 9, 20, 12, 0, 0, 500000, tzinfo=datetime.timezone.utc),  # 2026-09-20 12:00:00.500 UTC
        'accepted_at': datetime.datetime(2026, 9, 20, 12, 0, 1, 250000, tzinfo=datetime.timezone.utc),  # 2026-09-20 12:00:01.250 UTC
    }


# These tests run on a single node, so use CL=ONE
@contextmanager
def export_metadata(cql, export_arn, columns):
    names = ', '.join(['export_arn', *columns])
    placeholders = ', '.join(['%s'] * (1 + len(columns)))
    cql.execute(SimpleStatement(
        f"INSERT INTO system_distributed.alternator_export_to_s3_exports ({names}) VALUES ({placeholders})",
        consistency_level=ConsistencyLevel.ONE), [export_arn, *columns.values()])
    try:
        yield export_arn
    finally:
        cql.execute(SimpleStatement(
            "DELETE FROM system_distributed.alternator_export_to_s3_exports WHERE export_arn = %s",
            consistency_level=ConsistencyLevel.ONE), [export_arn])


# A private table for the ListExports tests below. Other tests start real exports of
# test_table_s_for_export_only, so ListExports of that table can show more rows.
@pytest.fixture
def table_for_listing(dynamodb):
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
        yield table


# When the finished exports below finished.
COMPLETED_AT = datetime.datetime(2026, 9, 20, 12, 5, 0, 750000, tzinfo=datetime.timezone.utc)  # 2026-09-20 12:05:00.750 UTC

# The fields of an ExportDescription which not every export has: the request fields a request may
# omit, and what a finished export produced.
OPTIONAL_EXPORT_DESCRIPTION_FIELDS = ['S3Prefix', 'ExportType', 'ExportManifest', 'ItemCount', 'BilledSizeBytes',
                                      'EndTime', 'FailureCode', 'FailureMessage']


# Test that DescribeExport shows an export correctly in each state that an export can be in:
# accepted and not finished, completed, and failed.
#
# Each parameter set is one export. The test writes the row of that export to the export metadata
# table. Then it calls DescribeExport and compares the ExportDescription with the expected values.
# The parameters are:
# - request_fields: the fields to add to the stored ExportTableToPointInTime request, for example
#   S3Prefix. DescribeExport copies these fields from the stored request.
# - columns: the columns to add to the row or to replace in it, for example export_status. These
#   columns hold the state of the export and the results of a finished export.
# - status: the ExportStatus that DescribeExport must show.
# - optional_fields: the optional fields that the ExportDescription must have, with their values.
#   The ExportDescription must not have other optional fields.
#
# For each export, the test also checks the fields that all ExportDescriptions have. DynamoDB has
# only three statuses: IN_PROGRESS, COMPLETED and FAILED. DescribeExport must show all other stored
# statuses, for example FAILING, as IN_PROGRESS.
@pytest.mark.parametrize('request_fields, columns, status, optional_fields', [
    pytest.param({}, {}, 'IN_PROGRESS', {}, id='in_progress'),
    pytest.param({}, {'export_status': 'FAILING'}, 'IN_PROGRESS', {}, id='failing'),
    pytest.param({'S3Prefix': 'exports/test', 'ExportType': 'FULL_EXPORT'}, {
        'export_status': 'COMPLETED',
        'export_manifest': 'AWSDynamoDB/01695353076000-06e2188f/manifest-files.json',
        'item_count': 17,
        'billed_size_bytes': 4096,
        'completed_at': COMPLETED_AT,
    }, 'COMPLETED', {
        'S3Prefix': 'exports/test',
        'ExportType': 'FULL_EXPORT',
        'ExportManifest': 'AWSDynamoDB/01695353076000-06e2188f/manifest-files.json',
        'ItemCount': 17,
        'BilledSizeBytes': 4096,
        'EndTime': COMPLETED_AT,
    }, id='completed'),
    pytest.param({}, {
        'export_status': 'FAILED',
        'failure_code': 'InternalServerError',
        'failure_message': 'the export could not be written',
        'completed_at': COMPLETED_AT,
    }, 'FAILED', {
        'FailureCode': 'InternalServerError',
        'FailureMessage': 'the export could not be written',
        'EndTime': COMPLETED_AT,
    }, id='failed'),
])
def test_describe_export_states(test_table_s_for_export_only, cql, request_fields, columns, status, optional_fields):
    client = test_table_s_for_export_only.meta.client
    table_arn = get_table_arn(test_table_s_for_export_only)
    export_arn = unstarted_export_arn(table_arn)
    row = accepted_export_row(table_arn, **request_fields) | columns
    with export_metadata(cql, export_arn, row):
        desc = client.describe_export(ExportArn=export_arn)['ExportDescription']
    assert desc['ExportArn'] == export_arn
    assert desc['ExportStatus'] == status
    assert desc['TableArn'] == table_arn
    assert desc['TableId'] == str(row['table_id'])
    assert desc['ClientToken'] == row['client_token']
    assert desc['S3Bucket'] == 'my-bucket'
    # No request names a format, so the default it was accepted with is reported.
    assert desc['ExportFormat'] == 'DYNAMODB_JSON'
    assert desc['ExportTime'] == row['export_time']
    assert desc['StartTime'] == row['accepted_at']
    assert {field: desc[field] for field in OPTIONAL_EXPORT_DESCRIPTION_FIELDS if field in desc} == optional_fields


# Test that DescribeExport answers a corrupt export row - one missing a column every export has, or
# one whose request cannot be read back - with InternalServerError. The node must stay up, and
# test.py runs it with --abort-on-internal-error, so an error reported with on_internal_error()
# would fail this test by taking the node down.
@pytest.mark.parametrize('corrupt', [
    pytest.param(lambda row: row.pop('request'), id='missing_column'),
    pytest.param(lambda row: row.update(request='not json'), id='request_not_json'),
    pytest.param(lambda row: row.update(request=json.dumps({'S3Bucket': 'my-bucket'})), id='request_without_table_arn'),
])
def test_describe_export_corrupt_row(test_table_s_for_export_only, cql, corrupt):
    client = test_table_s_for_export_only.meta.client
    table_arn = get_table_arn(test_table_s_for_export_only)
    export_arn = unstarted_export_arn(table_arn)
    row = accepted_export_row(table_arn)
    corrupt(row)
    with export_metadata(cql, export_arn, row):
        with pytest.raises(ClientError, match='InternalServerError'):
            client.describe_export(ExportArn=export_arn)


# Test that DescribeExport still describes an export after the table it was taken from has been
# deleted.
def test_describe_export_table_deleted(dynamodb, cql):
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
        client = table.meta.client
        table_arn = get_table_arn(table)
        export_arn = unstarted_export_arn(table_arn)
    row = accepted_export_row(table_arn)
    with export_metadata(cql, export_arn, row):
        desc = client.describe_export(ExportArn=export_arn)['ExportDescription']
    assert desc['ExportArn'] == export_arn
    assert desc['TableArn'] == table_arn
    assert desc['TableId'] == str(row['table_id'])


# Test that ListExports with a TableArn lists only the exports of that table, in ExportArn
# descending order. Each ExportSummary has the ExportArn, the ExportStatus (IN_PROGRESS for a stored
# status that DynamoDB does not have), and the ExportType of the request (FULL_EXPORT if the request
# has no ExportType). Without TableArn, ListExports also lists the export of the other table, and
# paging one export at a time keeps the ExportArn descending order across the two tables.
# The request with INCREMENTAL_EXPORT, which Alternator does not accept, shows that ListExports
# copies ExportType from the request instead of reporting the default.
def test_list_exports_summaries(table_for_listing, test_table_s, cql):
    client = table_for_listing.meta.client
    table_arn = get_table_arn(table_for_listing)
    other_table_arn = get_table_arn(test_table_s)
    # export ARN -> (the stored row, the fields of the expected ExportSummary)
    rows = {
        unstarted_export_arn(table_arn): (accepted_export_row(table_arn, ExportType='INCREMENTAL_EXPORT') | {'export_status': 'COMPLETED'},
                                                  {'ExportStatus': 'COMPLETED', 'ExportType': 'INCREMENTAL_EXPORT'}),
        unstarted_export_arn(table_arn): (accepted_export_row(table_arn) | {'export_status': 'FAILED'},
                                                  {'ExportStatus': 'FAILED', 'ExportType': 'FULL_EXPORT'}),
        unstarted_export_arn(table_arn): (accepted_export_row(table_arn),
                                                  {'ExportStatus': 'IN_PROGRESS', 'ExportType': 'FULL_EXPORT'}),
        unstarted_export_arn(table_arn): (accepted_export_row(table_arn) | {'export_status': 'NOT_STARTED'},
                                                  {'ExportStatus': 'IN_PROGRESS', 'ExportType': 'FULL_EXPORT'}),
    }
    other_arn = unstarted_export_arn(other_table_arn)
    # ensure all export_metadata - originated objects are deleted afterwards, with what their finally: provides
    with ExitStack() as seeded:
        for export_arn, (row, _) in rows.items():
            seeded.enter_context(export_metadata(cql, export_arn, row))
        seeded.enter_context(export_metadata(cql, other_arn, accepted_export_row(other_table_arn)))
        summaries = client.list_exports(TableArn=table_arn)['ExportSummaries']
        unfiltered = [summary['ExportArn'] for summary in iterate_over_exports(client, max_results=1)]
    # The filtered listing has exactly this table's exports, in ExportArn descending order, each with its expected fields.
    assert summaries == [{'ExportArn': export_arn} | rows[export_arn][1] for export_arn in sorted(rows, reverse=True)]
    # The unfiltered listing has all five exports, the other table's too, possibly among other exports.
    assert set(rows) | {other_arn} <= set(unfiltered)
    assert unfiltered == sorted(set(unfiltered), reverse=True)


# Test that ListExports pages through a table's exports MaxResults at a time, handing out a
# NextToken with every page but the last, and no NextToken when the exports fit the page exactly.
def test_list_exports_paging(table_for_listing, cql):
    client = table_for_listing.meta.client
    table_arn = get_table_arn(table_for_listing)
    arns = sorted((unstarted_export_arn(table_arn) for _ in range(5)), reverse=True)
    # ensure all export_metadata - originated objects are deleted afterwards, with what their finally: provides
    with ExitStack() as seeded:
        for export_arn in arns:
            seeded.enter_context(export_metadata(cql, export_arn, accepted_export_row(table_arn)))
        pages = []
        kwargs = {'TableArn': table_arn, 'MaxResults': 2}
        # Request pages until a response has no NextToken, but at most len(arns) pages: more pages
        # than exports means that the paging does not end.
        for _ in range(len(arns)):
            response = client.list_exports(**kwargs)
            pages.append([summary['ExportArn'] for summary in response['ExportSummaries']])
            if 'NextToken' not in response:
                break
            kwargs['NextToken'] = response['NextToken']
        else:  # len(arns) pages, and the last one still had a NextToken
            pytest.fail(f'ListExports keeps handing out a NextToken: {pages}')
        exact_fit = client.list_exports(TableArn=table_arn, MaxResults=len(arns))
    assert pages == [arns[0:2], arns[2:4], arns[4:]]
    assert [summary['ExportArn'] for summary in exact_fit['ExportSummaries']] == arns
    assert 'NextToken' not in exact_fit


# Test that ListExports filtered by a table which has been deleted reports no ExportSummaries at
# all, as DynamoDB does (test_export_start_and_delete_table), while the export the table left
# behind is still listed without the filter.
def test_list_exports_table_deleted(dynamodb, cql):
    # ensure all export_metadata - originated objects are deleted afterwards, with what their finally: provides
    with ExitStack() as seeded:
        with new_test_table(dynamodb,
                KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
                AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
            client = table.meta.client
            table_arn = get_table_arn(table)
            export_arn = unstarted_export_arn(table_arn)
            seeded.enter_context(export_metadata(cql, export_arn, accepted_export_row(table_arn)))
            # Sanity check: the export is listed while its table exists.
            assert [summary['ExportArn'] for summary in client.list_exports(TableArn=table_arn)['ExportSummaries']] == [export_arn]
        assert 'ExportSummaries' not in client.list_exports(TableArn=table_arn)
        assert export_arn in [summary['ExportArn'] for summary in iterate_over_exports(client)]


# Test that ListExports leaves out a corrupt export row - one missing a column every export has, or
# one whose ARN or request cannot be read back - and still lists the table's other exports.
@pytest.mark.parametrize('make_arn, columns', [
    pytest.param(lambda arn: arn, {'request': None}, id='missing_column'),
    pytest.param(lambda arn: arn, {'request': 'not json'}, id='request_not_json'),
    pytest.param(lambda arn: arn.rsplit('/', 1)[0] + '/', {}, id='arn_without_export_id'),
])
def test_list_exports_corrupt_row(table_for_listing, cql, make_arn, columns):
    client = table_for_listing.meta.client
    table_arn = get_table_arn(table_for_listing)
    export_arn = unstarted_export_arn(table_arn)
    corrupt_arn = make_arn(unstarted_export_arn(table_arn))
    with export_metadata(cql, export_arn, accepted_export_row(table_arn)), \
         export_metadata(cql, corrupt_arn, accepted_export_row(table_arn) | columns):
        summaries = client.list_exports(TableArn=table_arn)['ExportSummaries']
    assert [summary['ExportArn'] for summary in summaries] == [export_arn]


# ---------------------------------------------------------------------------
# Error injection tests
# ---------------------------------------------------------------------------

# Test that a failure in the background export fiber makes the export FAILED,
# that DescribeExport reports the failure, and that the export cleans up
# (the data already uploaded to S3 can stay).
def test_export_error_injection_fails_export(dynamodb, scylla_only, rest_api):
    injection = 'alternator_export_to_s3_in_item_handler'
    with new_test_table(dynamodb,
        KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
    ) as table:
        for i in range(10):
            table.put_item(Item={'p': f'item{i}', 'v': f'value{i}'})
        client = table.meta.client
        table_arn = get_table_arn(table)
        s3 = make_s3_client(dynamodb)

        with new_s3_bucket(s3) as bucket:
            with scylla_inject_error(rest_api, injection, one_shot=False):
                response = client.export_table_to_point_in_time(
                    TableArn=table_arn,
                    S3Bucket=bucket,
                )
                export_arn = response['ExportDescription']['ExportArn']
                desc = wait_for_export(client, export_arn)
            assert desc['ExportStatus'] == 'FAILED'
            assert desc['FailureCode'] == 'Exception'
            assert f'injected failure {injection}' in desc['FailureMessage']
            # A finished export has an end time.
            assert 'EndTime' in desc

            response = s3.list_objects_v2(Bucket=bucket)
            keys = [obj['Key'] for obj in response.get('Contents', [])]
            assert not [k for k in keys if 'manifest' in k], keys
            # A failed export must not leave a multipart upload (the listing above does not
            # show it, but it is stored and paid for).
            uploads = s3.list_multipart_uploads(Bucket=bucket).get('Uploads', [])
            assert not uploads, uploads

            assert_listed_with_status(client, table_arn, export_arn, 'FAILED')


# Test that a failure after the export row insert (before the fiber starts) fails the export. The
# request writes FAILED to the row, ExportTableToPointInTime returns FAILED, and DescribeExport
# and ListExports report the same ARN as FAILED.
def test_export_error_injection_after_export_row_insertion(dynamodb, scylla_only, rest_api, test_table_s):
    injection = 'alternator_export_to_s3_after_export_row_insertion'
    client = test_table_s.meta.client
    table_arn = get_table_arn(test_table_s)
    s3 = make_s3_client(dynamodb)

    with new_s3_bucket(s3) as bucket:
        with scylla_inject_error(rest_api, injection, one_shot=True):
            desc = client.export_table_to_point_in_time(
                TableArn=table_arn,
                S3Bucket=bucket,
            )['ExportDescription']
        # The export did not start, so the response is not IN_PROGRESS.
        assert desc['ExportStatus'] == 'FAILED'
        assert desc['FailureCode'] == 'Exception'
        assert f'injected failure {injection}' in desc['FailureMessage']
        # A finished export has an end time.
        assert 'EndTime' in desc

        export_arn = desc['ExportArn']
        assert client.describe_export(ExportArn=export_arn)['ExportDescription']['ExportStatus'] == 'FAILED'

        # Nothing was exported: only the fiber writes to the bucket, and it did not run.
        keys = [obj['Key'] for obj in s3.list_objects_v2(Bucket=bucket).get('Contents', [])]
        assert not keys, keys
        # The listing above does not show multipart uploads, so check them separately.
        uploads = s3.list_multipart_uploads(Bucket=bucket).get('Uploads', [])
        assert not uploads, uploads

        assert_listed_with_status(client, table_arn, export_arn, 'FAILED')


# Test that the export starts if the export row insert returns "not applied" but applied (this
# can occur with a contended CAS). The row is ours. If we do not start the export, the row stays
# IN_PROGRESS.
def test_export_error_injection_export_row_insert_not_applied(dynamodb, scylla_only, rest_api, test_table_s):
    client = test_table_s.meta.client
    table_arn = get_table_arn(test_table_s)
    s3 = make_s3_client(dynamodb)

    with new_s3_bucket(s3) as bucket:
        with scylla_inject_error(rest_api, 'alternator_export_to_s3_export_row_insert_not_applied', one_shot=True):
            desc = client.export_table_to_point_in_time(
                TableArn=table_arn,
                S3Bucket=bucket,
            )['ExportDescription']
        assert desc['ExportStatus'] == 'IN_PROGRESS'

        export_arn = desc['ExportArn']
        final = wait_for_export(client, export_arn)
        assert final['ExportStatus'] == 'COMPLETED'
        assert 'ExportManifest' in final

        assert_listed_with_status(client, table_arn, export_arn, 'COMPLETED')


# Test that a failure after the ClientToken row insert (before the export row
# insert) fails the export. The request writes the FAILED export row for the
# reserved ARN, so that DescribeExport knows the ARN, and retries do not get
# IN_PROGRESS.
def test_export_error_injection_after_client_token_insertion(dynamodb, cql, scylla_only, rest_api, test_table_s):
    client = test_table_s.meta.client
    table_arn = get_table_arn(test_table_s)
    s3 = make_s3_client(dynamodb)
    token = str(uuid.uuid4())

    with new_s3_bucket(s3) as bucket:
        with scylla_inject_error(rest_api, 'alternator_export_to_s3_after_client_token_insertion', one_shot=True):
            desc = client.export_table_to_point_in_time(
                TableArn=table_arn,
                S3Bucket=bucket,
                ClientToken=token,
            )['ExportDescription']
        assert desc['ExportStatus'] == 'FAILED'
        assert desc['ClientToken'] == token
        assert desc['FailureCode'] == 'Exception'
        assert 'injected failure alternator_export_to_s3_after_client_token_insertion' in desc['FailureMessage']
        assert 'EndTime' in desc

        # The failed export has the ARN of the ClientToken row. Each retry must report the same
        # failed export.
        row = cql.execute(SimpleStatement(
            "SELECT export_arn FROM system_distributed.alternator_export_to_s3_client_tokens WHERE client_token = %s",
            consistency_level=ConsistencyLevel.ONE), [token]).one()
        assert desc['ExportArn'] == row.export_arn
        assert client.describe_export(ExportArn=desc['ExportArn'])['ExportDescription']['ExportStatus'] == 'FAILED'
        again = client.export_table_to_point_in_time(
            TableArn=table_arn,
            S3Bucket=bucket,
            ClientToken=token,
        )['ExportDescription']
        assert again['ExportArn'] == desc['ExportArn']
        assert again['ExportStatus'] == 'FAILED'

        # The ClientToken is reserved, so different parameters cause a conflict.
        with pytest.raises(ClientError, match='ExportConflictException.*Duplicate request detected'):
            client.export_table_to_point_in_time(
                TableArn=table_arn,
                S3Bucket=bucket,
                S3Prefix='prefix',
                ClientToken=token,
            )


# Test that the export fails if the ClientToken row read fails (the read after the insert returns
# "not applied"). The row can have our ARN (a contended CAS can commit our value). Without our
# FAILED export row, retries get IN_PROGRESS until the token row expires.
def test_export_error_injection_client_token_read_failure(dynamodb, cql, scylla_only, rest_api, test_table_s):
    client = test_table_s.meta.client
    table_arn = get_table_arn(test_table_s)
    s3 = make_s3_client(dynamodb)
    token = str(uuid.uuid4())

    with new_s3_bucket(s3) as bucket:
        # The first injection makes the insert return "not applied" although it applied, so the
        # ClientToken row has our ARN. The second injection makes the read fail.
        with scylla_inject_error(rest_api, 'alternator_export_to_s3_client_token_insert_not_applied', one_shot=True), \
             scylla_inject_error(rest_api, 'alternator_export_to_s3_before_client_token_read', one_shot=True):
            desc = client.export_table_to_point_in_time(
                TableArn=table_arn,
                S3Bucket=bucket,
                ClientToken=token,
            )['ExportDescription']
        assert desc['ExportStatus'] == 'FAILED'
        assert desc['ClientToken'] == token
        assert desc['FailureCode'] == 'Exception'
        assert 'injected failure alternator_export_to_s3_before_client_token_read' in desc['FailureMessage']
        assert 'EndTime' in desc

        # The failed export has the ARN of the ClientToken row, so a retry reports it.
        row = cql.execute(SimpleStatement(
            "SELECT export_arn FROM system_distributed.alternator_export_to_s3_client_tokens WHERE client_token = %s",
            consistency_level=ConsistencyLevel.ONE), [token]).one()
        assert desc['ExportArn'] == row.export_arn
        assert client.describe_export(ExportArn=desc['ExportArn'])['ExportDescription']['ExportStatus'] == 'FAILED'
        again = client.export_table_to_point_in_time(
            TableArn=table_arn,
            S3Bucket=bucket,
            ClientToken=token,
        )['ExportDescription']
        assert again['ExportArn'] == desc['ExportArn']
        assert again['ExportStatus'] == 'FAILED'

        assert_listed_with_status(client, table_arn, desc['ExportArn'], 'FAILED')

        # Nothing was exported: only the fiber writes to the bucket, and it did not run.
        keys = [obj['Key'] for obj in s3.list_objects_v2(Bucket=bucket).get('Contents', [])]
        assert not keys, keys
        uploads = s3.list_multipart_uploads(Bucket=bucket).get('Uploads', [])
        assert not uploads, uploads


# ---------------------------------------------------------------------------
# Garbage collection tests
# ---------------------------------------------------------------------------

# Lets a test control the garbage collection of exports. In the context, the
# garbage collector does not run periodically. The yielded function starts one
# round and waits until it finishes.
@contextmanager
def export_garbage_collection(rest_api):
    wakeup = 'alternator_export_to_s3_gc_wakeup'
    finished = 'alternator_export_to_s3_gc_finished'
    def enter_count():
        response = requests.get(f'{rest_api}/v2/error_injection/injection/{finished}/enters', timeout=30)
        response.raise_for_status()
        return int(response.json())
    def run_garbage_collection(timeout=60):
        before = enter_count()
        requests.post(f'{rest_api}/v2/error_injection/injection/{wakeup}/message', timeout=30).raise_for_status()
        deadline = time.time() + timeout
        while time.time() < deadline:
            if enter_count() > before:
                return
            time.sleep(0.01)
        pytest.fail('garbage collection of exports did not finish in time')
    with scylla_inject_error(rest_api, finished), scylla_inject_error(rest_api, wakeup):
        yield run_garbage_collection

# Returns the node_id (see live_node_identifier) of the node behind `rest_api`:
# its host_id and gossip generation.
def node_id_of(rest_api):
    host_id = requests.get(f'{rest_api}/storage_service/hostid/local', timeout=30).json()
    generation = requests.get(f'{rest_api}/storage_service/generation_number', timeout=30).json()
    return f'{host_id}:{generation}'

# Returns a node_id (see live_node_identifier) of a dead node: the host_id of
# `node_id` with an old gossip generation.
def dead_node_id(node_id):
    host_id, _ = node_id.split(':')
    return f'{host_id}:1'

# Inserts an export row with the given status and node, yields its ARN, and
# removes the row on exit (the garbage collector does not fail rows of live nodes).
def in_progress_export(cql, table_arn, node_id, export_status='IN_PROGRESS'):
    export_arn = f'{table_arn}/export/{unknown_export_id()}'
    return export_metadata(cql, export_arn,
        accepted_export_row(table_arn) | {'node_id': node_id, 'export_status': export_status})

# Inserts a ClientToken row of the given node without an export row (as a node that died between
# the two inserts). Returns the reserved ARN.
def insert_client_token_reservation(cql, table_arn, node_id, export_id=None):
    export_arn = f'{table_arn}/export/{export_id or unknown_export_id()}'
    columns = accepted_export_row(table_arn)
    cql.execute(SimpleStatement(
        "INSERT INTO system_distributed.alternator_export_to_s3_client_tokens (client_token, export_arn, request, node_id) "
        "VALUES (%s, %s, %s, %s)",
        consistency_level=ConsistencyLevel.ONE), [columns['client_token'], export_arn, columns['request'], node_id])
    return export_arn

# Test that the garbage collector marks as FAILED an IN_PROGRESS export of a
# dead node (e.g. a restarted node). It must not change an IN_PROGRESS export of
# a live node or a finished export of a dead node.
def test_export_garbage_collection_fails_export_of_dead_node(cql, scylla_only, rest_api, test_table_s):
    client = test_table_s.meta.client
    table_arn = get_table_arn(test_table_s)
    live_node_id = node_id_of(rest_api)

    with export_garbage_collection(rest_api) as run_garbage_collection:
        with in_progress_export(cql, table_arn, live_node_id) as live_export_arn, \
             in_progress_export(cql, table_arn, dead_node_id(live_node_id)) as dead_export_arn, \
             in_progress_export(cql, table_arn, dead_node_id(live_node_id), export_status='COMPLETED') as finished_export_arn, \
             in_progress_export(cql, table_arn, 'not-a-node-id') as malformed_export_arn:
            run_garbage_collection()
            assert client.describe_export(ExportArn=live_export_arn)['ExportDescription']['ExportStatus'] == 'IN_PROGRESS'
            # A finished export of a dead node does not change.
            assert client.describe_export(ExportArn=finished_export_arn)['ExportDescription']['ExportStatus'] == 'COMPLETED'
            # A row with an incorrect node_id does not change, and the round continues.
            assert client.describe_export(ExportArn=malformed_export_arn)['ExportDescription']['ExportStatus'] == 'IN_PROGRESS'
            desc = client.describe_export(ExportArn=dead_export_arn)['ExportDescription']
            assert desc['ExportStatus'] == 'FAILED'
            assert desc['FailureCode'] == 'NodeFailure'
            assert desc['FailureMessage']
            assert desc['EndTime']

# Test that the garbage collector marks as FAILED an IN_PROGRESS export of a node
# which is not in the cluster any more (it was removed or replaced), and of which
# the gossiper has no state. Only a member which the gossiper does not know keeps
# its exports, because the gossip view of this node can still be incomplete.
def test_export_garbage_collection_fails_export_of_removed_node(cql, scylla_only, rest_api, test_table_s):
    client = test_table_s.meta.client
    table_arn = get_table_arn(test_table_s)
    removed_node_id = f'{uuid.uuid4()}:1'

    with export_garbage_collection(rest_api) as run_garbage_collection:
        with in_progress_export(cql, table_arn, removed_node_id) as export_arn:
            run_garbage_collection()
            desc = client.describe_export(ExportArn=export_arn)['ExportDescription']
            assert desc['ExportStatus'] == 'FAILED'
            assert desc['FailureCode'] == 'NodeFailure'

# Test that a later garbage collection round finishes a FAILING row of this node
# (the FAILING write applied, but the FAILED write did not).
def test_export_garbage_collection_finishes_own_failing_export(cql, scylla_only, rest_api, test_table_s):
    client = test_table_s.meta.client
    table_arn = get_table_arn(test_table_s)
    live_node_id = node_id_of(rest_api)

    with export_garbage_collection(rest_api) as run_garbage_collection:
        with in_progress_export(cql, table_arn, live_node_id, export_status='FAILING') as export_arn:
            run_garbage_collection()
            desc = client.describe_export(ExportArn=export_arn)['ExportDescription']
            assert desc['ExportStatus'] == 'FAILED'
            assert desc['FailureCode'] == 'NodeFailure'
            assert desc['FailureMessage']
            assert desc['EndTime']

# Test that the garbage collector writes a FAILED export row if a node died
# after the ClientToken row insert and before the export row insert.
def test_export_garbage_collection_creates_failed_export_for_client_token_of_dead_node(cql, scylla_only, rest_api, test_table_s):
    client = test_table_s.meta.client
    table_arn = get_table_arn(test_table_s)
    live_node_id = node_id_of(rest_api)

    with export_garbage_collection(rest_api) as run_garbage_collection:
        # A ClientToken row of a dead node without an export row, for an export accepted one
        # hour ago.
        accepted_at = time.time() - 3600
        export_arn = insert_client_token_reservation(cql, table_arn, dead_node_id(live_node_id),
                                                     export_id=export_id_accepted_at(accepted_at))

        run_garbage_collection()
        desc = client.describe_export(ExportArn=export_arn)['ExportDescription']
        assert desc['ExportStatus'] == 'FAILED'
        assert desc['FailureCode'] == 'NodeFailure'
        # StartTime and ExportTime are the accept time from the export id (the request has no
        # ExportTime). Only EndTime is the time of the round.
        assert desc['StartTime'].timestamp() == pytest.approx(int(accepted_at * 1000) / 1000, abs=1e-3)
        assert desc['ExportTime'] == desc['StartTime']
        assert desc['EndTime'].timestamp() > accepted_at + 1800

# Test that an unreadable ClientToken row of a dead node (the request is not JSON, and the ARN
# has no export id) does not stop the round: the export of that node still becomes FAILED.
def test_export_garbage_collection_unreadable_client_token_row(cql, scylla_only, rest_api, test_table_s):
    client = test_table_s.meta.client
    table_arn = get_table_arn(test_table_s)
    live_node_id = node_id_of(rest_api)

    with export_garbage_collection(rest_api) as run_garbage_collection:
        client_token = random_string(20)
        malformed_export_arn = f'{table_arn}-not-an-export-arn'
        cql.execute(SimpleStatement(
            "INSERT INTO system_distributed.alternator_export_to_s3_client_tokens (client_token, export_arn, request, node_id) "
            "VALUES (%s, %s, %s, %s)",
            consistency_level=ConsistencyLevel.ONE),
            [client_token, malformed_export_arn, 'not json', dead_node_id(live_node_id)])
        try:
            with in_progress_export(cql, table_arn, dead_node_id(live_node_id)) as dead_export_arn:
                run_garbage_collection()
                assert client.describe_export(ExportArn=dead_export_arn)['ExportDescription']['ExportStatus'] == 'FAILED'
        finally:
            # The round wrote an export row with the malformed ARN, which nothing else removes
            # until its TTL expires. Every later ListExports of this cluster would warn about
            # that ARN, so remove the row and the ClientToken row which caused it.
            cql.execute(SimpleStatement(
                "DELETE FROM system_distributed.alternator_export_to_s3_exports WHERE export_arn = %s",
                consistency_level=ConsistencyLevel.ONE), [malformed_export_arn])
            cql.execute(SimpleStatement(
                "DELETE FROM system_distributed.alternator_export_to_s3_client_tokens WHERE client_token = %s",
                consistency_level=ConsistencyLevel.ONE), [client_token])
