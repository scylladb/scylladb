# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# Tests for the DynamoDB bulk put items API: StartBulkPutItems,
# DescribeBulkOperation, ListBulkOperations and CancelBulkOperation. A bulk
# put items operation reads items from S3 and puts them into an existing
# table, so it is an import into an existing table. AWS's name is
# misleading: the feature is not a batch of PutItem requests, but an
# asynchronous import job. All tests pass on AWS. Where DynamoDB contradicts
# its documentation, the test's comment says so:
#   https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/BulkPutItems.HowItWorks.html
#   https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/BulkPutItems.Format.html
#   https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/BulkPutItems.Output.html
#   https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/BulkPutItems.Considerations.html
#   https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_StartBulkPutItems.html
#   https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BulkOperationDescription.html
#
# Import from S3 (test_import.py) loads the same kind of S3 data into a new
# table. This file reuses its S3 helpers and tests only what bulk put items
# adds: the target table exists, every record can carry a condition, and the
# results go to a report in S3. The bucket helpers are those of
# test_export.py: an operation writes into the bucket, so new_s3_bucket()
# must block writes before it empties the bucket.
#
# Scenarios. A scenario runs one operation on its own table. Its
# module-scoped fixture, next to the tests which check it, creates the table,
# uploads the input, runs the operation to its end and records what the
# operation did. pytest runs a fixture only for the tests which use it, so a
# test runs only the operations which it checks.
#
# Cost. On DynamoDB an operation takes about a minute, even for one item, and
# the operations run one after the other. So the whole file takes about
# half an hour on DynamoDB, and a test of one scenario 1 to 4 minutes. The
# inputs are tiny and the tables are PAY_PER_REQUEST. CreateTable creates the
# indexes and the stream, because an index added later takes many minutes.
# Some scenarios use the Accelerated tier, so the region must have it. A run
# costs about one cent: DynamoDB bills each written item, at least 1 KB. Most
# of the bill comes from the two scenarios which act while their operation
# writes.
#
# Left out, because they cannot apply to Alternator, are not deterministic or
# need a second AWS account: global tables and DAX (refused or bypassed by
# bulk put items), CloudTrail events, Kinesis data streams, CloudWatch
# metrics, AWS KMS keys, IAM permissions and the failure
# DynamoDBAccessDeniedException, buckets in another account, a transaction
# and a bulk write on one item, the 8-hour lifetime of a ClientToken, the
# write rate which MaxWriteRequestUnits sets, ObjectModifiedInS3DuringImport
# (it depends on when DynamoDB reads each object), the Timeout failure, a
# table which is being deleted or updated (Alternator does both at once), and
# request members of a wrong JSON type (botocore refuses most of them).
# Also left out are the quotas which need too much data, time or slots of the
# shared account: 5 operations per tier (LimitExceededException), 50,000
# input objects, 5 GB per object, 100 GB (Standard) or 1 TB (Accelerated) per
# operation, 200 MiB per report file, and the item collection limit of a
# table with an LSI (10 GB).

import copy
import gzip
import json
import re
import time
from contextlib import suppress
from decimal import Decimal
from functools import partial
from types import SimpleNamespace
from typing import NamedTuple

import botocore
import pytest
from boto3.dynamodb.types import Binary
from botocore.exceptions import ClientError
from packaging.version import Version

from test.alternator.test_export import (arn_with_part, make_s3_client, new_s3_bucket,
                                         unknown_export_id)
from test.alternator.test_gsi import assert_index_scan as assert_gsi_scan
from test.alternator.test_import import (GROUND_TRUTH_ITEMS, GROUND_TRUTH_SCHEMA,
                                         delete_table_afterwards,
                                         dynamodb_json_lines, unique_bucket_name,
                                         unique_prefix, upload_lines)
from test.alternator.test_lsi import assert_index_scan as assert_lsi_scan
from test.alternator.test_streams import fetch_more, list_shards, wait_for_active_stream
from test.alternator.util import (client_no_transform, create_test_table, full_scan,
                                  get_table_arn, is_aws, multiset, random_string,
                                  unique_table_name)
from test.pylib.skip_types import skip_env

NOT_IMPLEMENTED = 'SCYLLADB-6170'
NO_ACCOUNT_ID = 'Alternator has no AWS account id for S3BucketOwner'
# The reason which test_export.py gives for the same checks of an export ARN.
ARN_PARTS = 'Alternator does not check the partition, service, region or account of an ARN'

# Botocore added StartBulkPutItems, DescribeBulkOperation, ListBulkOperations
# and CancelBulkOperation in version 1.43.124.
@pytest.fixture(scope='module', autouse=True)
def need_bulk_put_items_in_botocore():
    if Version(botocore.__version__) < Version('1.43.124'):
        skip_env('Botocore version 1.43.124 or above required to run this test')

TERMINAL_STATUSES = {'COMPLETED', 'COMPLETED_WITH_ERRORS', 'CANCELLED', 'FAILED'}

# Members which every BulkOperationDescription has, from StartBulkPutItems's
# response on. MaxWriteRequestUnits is there only if the request had it.
START_MEMBERS = {'BulkOperationArn', 'BulkOperationStatus', 'ClientToken',
                 'FailedItemCount', 'InputS3Configuration', 'OutputS3Configuration',
                 'PerformanceTier', 'ProcessedItemCount', 'StartTime', 'TableDetails'}
# Members fixed when the operation starts. Status and counters change.
IMMUTABLE_MEMBERS = START_MEMBERS - {'BulkOperationStatus', 'FailedItemCount',
                                     'ProcessedItemCount'}
# Members which appear only when the operation ends.
END_MEMBERS = {'EndTime', 'BulkOperationUsage'}
# Members which appear only when DynamoDB has measured the input. An
# operation which fails before it reads the input never has them.
INPUT_SIZE_MEMBERS = {'ProcessedInputBytes', 'TotalInputBytes'}
# Members which only a FAILED or CANCELLED operation has.
FAILURE_MEMBERS = {'FailureCode', 'FailureMessage'}

IN = 'InputS3Configuration'
OUT = 'OutputS3Configuration'

# Every table has this key schema, except the one with a vector index.
KEY_SCHEMA = GROUND_TRUTH_SCHEMA

# Bulk put items meters each written item in 1 KB units, as PutItem does.
KB = 1024

# The item as the reports and DYNAMODB_JSON input show it: {'Item': {...}}.
def image(item):
    return json.loads(dynamodb_json_lines([item])[0])

# One DYNAMODB_JSON input record: the item, and optionally a condition with
# its substitution maps.
def record(item, condition=None, names=None, values=None):
    line = image(item)
    if condition is not None:
        line['ConditionExpression'] = condition
    if names is not None:
        line['ExpressionAttributeNames'] = names
    if values is not None:
        line['ExpressionAttributeValues'] = image(values)['Item']
    return json.dumps(line)

def key_of(item):
    return (item['p'], item['c'])

# Poll the operation `arn` until it ends, or until the timeout, and append
# each DescribeBulkOperation response to `samples`. An operation which does
# not end in time keeps a last sample which is not terminal, so it fails only
# the tests which check it. The inputs outside AWS are tiny, so there the
# timeout is 10 s. on_sample(description) is called for every sample, so a
# scenario can act while its operation runs.
def wait_for_operation(client, arn, samples, on_sample=lambda description: None):
    deadline = time.time() + (900 if is_aws(client) else 10)
    while True:
        description = client.describe_bulk_operation(BulkOperationArn=arn)['BulkOperationDescription']
        samples.append(description)
        on_sample(description)
        if description['BulkOperationStatus'] in TERMINAL_STATUSES or time.time() > deadline:
            return
        time.sleep(5 if is_aws(client) else 0.1)

# The ListBulkOperations summaries, newest first, up to the page which has
# the last of `arns`. The account has other operations, and their number
# only grows, so the helper does not read the whole listing.
def list_through(client, arns, **kwargs):
    summaries, missing = [], set(arns)
    for page in client.get_paginator('list_bulk_operations').paginate(**kwargs):
        summaries += page['BulkOperationsSummaryList']
        missing -= {s['BulkOperationArn'] for s in page['BulkOperationsSummaryList']}
        if not missing:
            break
    return summaries

# Key, size and ETag of every object under `prefix`.
def list_objects(s3, bucket, prefix=''):
    objects = {}
    for page in s3.get_paginator('list_objects_v2').paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            objects[obj['Key']] = SimpleNamespace(size=obj['Size'], etag=obj['ETag'].strip('"'))
    return objects

REPORT_FILE = re.compile(r'^-\d+/output/(errors|condition-check-failures)/output-\d+\.json\.gz$')

# The output of an operation: the manifest, the report records by type, and
# every output key. The output is under
#   <S3KeyPrefix>/AWSDynamoDB/BulkOperation/<account>:<operation id>
# The manifest is uncompressed JSON lines, the report files are gzip-compressed
# JSON lines in one directory per internal job (<operation id>-<n>).
def read_output(s3, bucket, prefix, arn):
    account = arn.split(':')[4]
    operation_id = arn.rpartition('/')[2]
    # One "/" follows the prefix, also when the prefix ends with one.
    root = (prefix.rstrip('/') + '/' if prefix else '') + f'AWSDynamoDB/BulkOperation/{account}:{operation_id}'
    output = SimpleNamespace(keys=[], manifest=None, reports={}, unexpected=[])
    for key in list_objects(s3, bucket, root):
        output.keys.append(key)
        body = s3.get_object(Bucket=bucket, Key=key)['Body'].read()
        rest = key[len(root):]
        if rest == '/manifest-files.json':
            output.manifest = [json.loads(line) for line in body.decode().splitlines() if line]
        elif match := REPORT_FILE.match(rest):
            records = [json.loads(line) for line in gzip.decompress(body).decode().splitlines() if line]
            output.reports.setdefault(match.group(1), []).extend(records)
        else:
            output.unexpected.append(key)
    return output

def s3_arn(bucket, key):
    return f'arn:aws:s3:::{bucket}/{key}'

# The manifest lines, by input key. Each line's etag must match the object.
def manifest_by_key(output, bucket, inputs):
    by_key = {}
    for line in output.manifest:
        assert set(line) == {'inputSourceArn', 'fileProcessingStatus', 'processedRanges', 'etag'}
        key = line['inputSourceArn'].removeprefix(s3_arn(bucket, ''))
        assert key not in by_key, f'{key} is in the manifest twice'
        assert line['etag'] == inputs[key].etag, key
        by_key[key] = (line['fileProcessingStatus'], line['processedRanges'])
    return by_key

# The report records of one type, by input key and itemIndex. A record
# without an itemIndex (its object stopped) is under index None.
def report_by_location(output, report_type, bucket):
    by_location = {}
    for row in output.reports.get(report_type, []):
        assert set(row) <= {'itemLocation', 'errorMessages', 'newImage', 'oldImage'}, row
        assert row['errorMessages'], row
        location = row['itemLocation']
        key = location['inputSourceArn'].removeprefix(s3_arn(bucket, ''))
        assert (key, location.get('itemIndex')) not in by_location, row
        by_location[(key, location.get('itemIndex'))] = row
    return by_location

def usage_of(description):
    [usage] = description['BulkOperationUsage']
    return usage

def table_bytes(description):
    return usage_of(description)['Table']['BulkPutBytes']


###############################################################################
# Running the operations. The fixture of a scenario creates its table and
# uploads its input, then run_operation() runs the operation to its end.

# The S3 client, and the bucket which holds the inputs and the outputs of the
# scenarios. Each scenario uses its own key prefixes in the bucket.
@pytest.fixture(scope='module')
def s3(dynamodb):
    return make_s3_client(dynamodb)

@pytest.fixture(scope='module')
def bucket(s3):
    with new_s3_bucket(s3) as name:
        yield name

# One tiny object, for the scenarios which need an input but do not check it.
@pytest.fixture(scope='module')
def small_input(s3, bucket):
    return upload_lines(s3, bucket, [record({'p': 'small', 'c': Decimal(1)})], context='small')

# Write `items` into `table`, before an operation starts on the table.
def seed(table, items):
    with table.batch_writer() as batch:
        for item in items:
            batch.put_item(item)

def string_attribute(name):
    return {'AttributeName': name, 'AttributeType': 'S'}

GSI = {'IndexName': 'gsi', 'KeySchema': [{'AttributeName': 'g', 'KeyType': 'HASH'}],
       'Projection': {'ProjectionType': 'ALL'}}

# Start an operation and return its description. DynamoDB allows 5 running
# operations per performance tier and account. While other users of the
# account hold all of them, start_in_quota() waits for a free one. Outside
# AWS, it does not wait.
def start_in_quota(client, request):
    deadline = time.time() + (600 if is_aws(client) else 0)
    while True:
        try:
            return client.start_bulk_put_items(**request)['BulkOperationDescription']
        except ClientError as e:
            if e.response['Error']['Code'] != 'LimitExceededException' or time.time() > deadline:
                raise
            time.sleep(5)

# Start an operation. Return its description, or the ClientError.
def try_start(client, request):
    try:
        return client.start_bulk_put_items(**request)['BulkOperationDescription']
    except ClientError as e:
        return e

def expect_error(response, code, message):
    assert isinstance(response, ClientError), f'expected {code}, got {response}'
    assert response.response['Error']['Code'] == code
    assert re.search(message, response.response['Error']['Message'])

# Run the operation of `request` on `table` until it ends, and return what it
# did: the request, the input objects, the start response, the ARN, the
# DescribeBulkOperation samples, the final description, and the output in
# `bucket`. on_sample(op, description) is called for every sample, so a
# scenario can act while its operation runs. An operation which does not end
# in time is cancelled, so that it stops writing before its table and bucket
# are deleted.
def run_operation(table, s3, bucket, request, on_sample=lambda op, description: None):
    client = table.meta.client
    config = request[IN]
    try:
        inputs = list_objects(s3, config['S3Bucket'], config.get('S3KeyPrefix', ''))
    except ClientError as e:
        if e.response['Error']['Code'] != 'NoSuchBucket':
            raise
        inputs = {}
    # The ARN and the ID of the table are read before the start, because a
    # scenario can delete the table.
    op = SimpleNamespace(request=request, inputs=inputs, table_name=table.name,
                         table_arn=table.table_arn, table_id=table.table_id, samples=[])
    op.start = start_in_quota(client, request)
    op.arn = op.start['BulkOperationArn']
    try:
        wait_for_operation(client, op.arn, op.samples, partial(on_sample, op))
    finally:
        if not op.samples or op.samples[-1]['BulkOperationStatus'] not in TERMINAL_STATUSES:
            with suppress(ClientError):
                client.cancel_bulk_operation(BulkOperationArn=op.arn)
    op.final = op.samples[-1]
    op.output = read_output(s3, bucket, request[OUT].get('S3KeyPrefix'), op.arn)
    return op

# The scenarios. The fixture of a scenario is its name with "_op" added. The
# tests which check several scenarios get the fixtures by these names.
SCENARIOS = ['missing_bucket', 'wrong_owner', 'empty_prefix', 'missing_output_bucket',
             'wrong_output_owner', 'cancel_early', 'writes', 'errors', 'ion', 'whole_bucket',
             'vector', 'unreadable', 'cancel_midway', 'deleted_table']


###############################################################################
# Synchronous validation. These tests start no operation.

# A table which does not exist, so that a request which DynamoDB wrongly
# accepts starts no operation.
def valid_request(table_name=None):
    bucket = unique_bucket_name()
    return {'TableName': table_name or unique_table_name(),
            'InputS3Configuration': {'S3Bucket': bucket, 'S3KeyPrefix': 'in/',
                                     'InputFormat': 'DYNAMODB_JSON',
                                     'InputCompressionType': 'NONE'},
            'OutputS3Configuration': {'S3Bucket': bucket, 'OutputType': ['ERROR']},
            'ClientToken': random_string(20)}

# Change `request` at `path`: a member name, or a configuration and a member.
# None deletes the member.
def changed(request, path, value):
    request = copy.deepcopy(request)
    target = request
    for step in path[:-1]:
        target = target[step]
    if value is None:
        target.pop(path[-1], None)
    else:
        target[path[-1]] = value
    return request

# Invalid members, which DynamoDB refuses before it looks for the table. The
# error names the member. The "dynamodb" fixture sets parameter_validation=False,
# so the requests reach the server.
@pytest.mark.xfail(reason=NOT_IMPLEMENTED)
@pytest.mark.parametrize('path,value,message', [
    pytest.param(('TableName',), None, "'TableName' is required", id='no_table_name'),
    pytest.param((IN,), None, "'inputS3Configuration'.*must not be null", id='no_input'),
    pytest.param((OUT,), None, "'outputS3Configuration'.*must not be null", id='no_output'),
    pytest.param((IN, 'S3Bucket'), None, "'inputS3Configuration.s3Bucket'.*must not be null",
                 id='no_input_bucket'),
    pytest.param((IN, 'InputFormat'), None, "'inputS3Configuration.inputFormat'.*must not be null",
                 id='no_input_format'),
    # CSV is a valid InputFormat of ImportTable, but not of StartBulkPutItems.
    pytest.param((IN, 'InputFormat'), 'CSV', "'inputS3Configuration.inputFormat'.*enum",
                 id='csv'),
    pytest.param((IN, 'InputFormat'), 'XML', "'inputS3Configuration.inputFormat'.*enum",
                 id='bad_format'),
    pytest.param((IN, 'InputCompressionType'), 'BZIP2',
                 "'inputS3Configuration.inputCompressionType'.*enum", id='bad_compression'),
    pytest.param((IN, 'S3Bucket'), 'x' * 256, "'inputS3Configuration.s3Bucket'",
                 id='input_bucket_too_long'),
    pytest.param((IN, 'S3Bucket'), '-nope!', "'inputS3Configuration.s3Bucket'.*pattern",
                 id='input_bucket_bad_chars'),
    pytest.param((IN, 'S3KeyPrefix'), 'p' * 1025, "'inputS3Configuration.s3KeyPrefix'",
                 id='input_prefix_too_long'),
    pytest.param((IN, 'S3BucketOwner'), '1' * 11, "'inputS3Configuration.s3BucketOwner'.*pattern",
                 id='input_owner_not_12_digits'),
    # The pattern must match the whole value.
    pytest.param((IN, 'S3BucketOwner'), '1' * 13, "'inputS3Configuration.s3BucketOwner'.*pattern",
                 id='input_owner_13_digits'),
    pytest.param((OUT, 'OutputType'), None, "'outputS3Configuration.outputType'.*must not be null",
                 id='no_output_type'),
    pytest.param((OUT, 'OutputType'), ['WARNING'], "'outputS3Configuration.outputType'.*enum",
                 id='bad_output_type'),
    pytest.param((OUT, 'S3Bucket'), '-nope!', "'outputS3Configuration.s3Bucket'.*pattern",
                 id='output_bucket_bad_chars'),
    pytest.param((OUT, 'S3Bucket'), 'x' * 256, "'outputS3Configuration.s3Bucket'.*length",
                 id='output_bucket_too_long'),
    pytest.param((OUT, 'S3KeyPrefix'), 'p' * 1025, "'outputS3Configuration.s3KeyPrefix'",
                 id='output_prefix_too_long'),
    pytest.param((OUT, 'S3BucketOwner'), 'abc', "'outputS3Configuration.s3BucketOwner'.*pattern",
                 id='output_owner_not_12_digits'),
    pytest.param((OUT, 'S3SseAlgorithm'), 'DES', "'outputS3Configuration.s3SseAlgorithm'.*enum",
                 id='bad_sse_algorithm'),
    pytest.param((OUT, 'S3SseKmsKeyId'), '', "'outputS3Configuration.s3SseKmsKeyId'.*length",
                 id='empty_kms_key'),
    pytest.param((OUT, 'S3SseKmsKeyId'), 'k' * 2049,
                 "'outputS3Configuration.s3SseKmsKeyId'.*2048", id='kms_key_too_long'),
    pytest.param(('MaxWriteRequestUnits',), 1023, "'maxWriteRequestUnits'.*1024",
                 id='max_write_request_units_1023'),
    pytest.param(('MaxWriteRequestUnits',), 0, "'maxWriteRequestUnits'.*1024",
                 id='max_write_request_units_0'),
    pytest.param(('PerformanceTier',), 'TURBO', "'performanceTier'.*enum", id='bad_tier'),
    # The enum values are case-sensitive.
    pytest.param(('PerformanceTier',), 'standard', "'performanceTier'.*enum", id='lower_case_tier'),
    pytest.param((IN, 'InputFormat'), 'ion', "'inputS3Configuration.inputFormat'.*enum",
                 id='lower_case_input_format'),
    pytest.param((IN, 'InputCompressionType'), 'gzip',
                 "'inputS3Configuration.inputCompressionType'.*enum", id='lower_case_compression'),
    pytest.param((OUT, 'OutputType'), ['error'], "'outputS3Configuration.outputType'.*enum",
                 id='lower_case_output_type'),
    pytest.param((OUT, 'S3SseAlgorithm'), 'aes256', "'outputS3Configuration.s3SseAlgorithm'.*enum",
                 id='lower_case_sse_algorithm'),
    pytest.param(('ClientToken',), '$', "'clientToken'.*pattern", id='token_dollar'),
    pytest.param(('ClientToken',), 'a$b', "'clientToken'.*pattern", id='token_with_dollar'),
    pytest.param(('ClientToken',), '', "'clientToken'.*pattern", id='empty_token'),
    # A table name has at most 255 characters, also inside an ARN. The
    # documented limit of 1,024 characters applies to the whole TableName.
    pytest.param(('TableName',), 't' * 256, "'tableName'.*255", id='table_name_too_long'),
    pytest.param(('TableName',), 't' * 1025, "'tableName'.*1024", id='table_name_above_1024'),
    pytest.param(('TableName',), 'not a table!', "'tableName'.*pattern", id='table_name_bad_chars'),
    # The documented minimum length is 1, but a table name has at least 3
    # characters.
    pytest.param(('TableName',), 'ab', "'tableName'.*greater than or equal to 3",
                 id='table_name_too_short'),
])
def test_start_validation(dynamodb, path, value, message):
    request = changed(valid_request(), path, value)
    with pytest.raises(ClientError, match=f'ValidationException.*{message}'):
        dynamodb.meta.client.start_bulk_put_items(**request)


# Invalid members which DynamoDB refuses only after it finds the table, so
# test_table_s is the target. The bucket does not exist, so a request which
# DynamoDB wrongly accepts fails within seconds.
@pytest.mark.xfail(reason=NOT_IMPLEMENTED)
@pytest.mark.parametrize('path,value,message', [
    pytest.param((OUT, 'OutputType'), [], 'OutputType cannot be empty', id='empty_output_type'),
    # The page of OutputS3Configuration says "required" in its introduction,
    # but "Required: No" at S3Bucket.
    pytest.param((OUT, 'S3Bucket'), None, 'OutputS3Configuration.S3Bucket is invalid',
                 id='no_output_bucket'),
    pytest.param((OUT, 'S3SseAlgorithm'), 'KMS', 'S3SseKmsKeyId cannot be empty',
                 id='kms_without_key'),
    pytest.param((OUT, 'S3SseKmsKeyId'), 'some-key', 'S3SseAlgorithm is required',
                 id='key_without_algorithm'),
    # The documentation gives S3KeyPrefix only a maximum of 1,024
    # characters. With a table, DynamoDB refuses fewer than 3 characters, and
    # more than 948 for the input or 824 for the output.
    pytest.param((IN, 'S3KeyPrefix'), '', "'inputS3Configuration.s3KeyPrefix'.*equal to 3",
                 id='empty_input_prefix'),
    pytest.param((IN, 'S3KeyPrefix'), 'ab', "'inputS3Configuration.s3KeyPrefix'.*equal to 3",
                 id='short_input_prefix'),
    pytest.param((OUT, 'S3KeyPrefix'), '', "'outputS3Configuration.s3KeyPrefix'.*equal to 3",
                 id='empty_output_prefix'),
    pytest.param((OUT, 'S3KeyPrefix'), 'ab', "'outputS3Configuration.s3KeyPrefix'.*equal to 3",
                 id='short_output_prefix'),
    pytest.param((IN, 'S3KeyPrefix'), 'p' * 949, 'InputS3Configuration.S3KeyPrefix is invalid',
                 id='input_prefix_949'),
    pytest.param((OUT, 'S3KeyPrefix'), 'p' * 825, 'OutputS3Configuration.S3KeyPrefix is invalid',
                 id='output_prefix_825'),
])
def test_start_validation_on_table(dynamodb, test_table_s, path, value, message):
    request = changed(valid_request(test_table_s.name), path, value)
    with pytest.raises(ClientError, match=f'ValidationException.*{message}'):
        dynamodb.meta.client.start_bulk_put_items(**request)

# Values at the limits which DynamoDB accepts. It checks some limits only
# when the table exists (see test_start_validation_on_table), so the target
# is test_table_s. The input bucket does not exist, so the operation fails
# within seconds. The documentation gives no maximum of MaxWriteRequestUnits,
# and a ClientToken may hold spaces.
@pytest.mark.xfail(reason=NOT_IMPLEMENTED)
@pytest.mark.parametrize('changes', [
    pytest.param({(IN, 'S3KeyPrefix'): 'abc', (OUT, 'S3KeyPrefix'): 'abc'}, id='minimum'),
    pytest.param({(IN, 'S3Bucket'): 'x' * 255, (IN, 'S3KeyPrefix'): 'p' * 948,
                  (OUT, 'S3KeyPrefix'): 'p' * 824, ('MaxWriteRequestUnits',): 2**63 - 1,
                  ('ClientToken',): f' {random_string(20)} '}, id='maximum'),
])
def test_start_at_limits(dynamodb, test_table_s, changes):
    client = dynamodb.meta.client
    request = valid_request(test_table_s.name)
    for path, value in changes.items():
        request = changed(request, path, value)
    samples = []
    wait_for_operation(client, start_in_quota(client, request)['BulkOperationArn'], samples)
    assert samples[-1]['FailureCode'] == 'S3NoSuchBucket'

# A KMS key ID is refused with the AES256 algorithm.
@pytest.mark.xfail(reason=NOT_IMPLEMENTED)
def test_start_kms_key_with_aes256(dynamodb, test_table_s):
    request = valid_request(test_table_s.name)
    request[OUT].update(S3SseAlgorithm='AES256', S3SseKmsKeyId='some-key')
    with pytest.raises(ClientError, match="ValidationException.*Cannot provide "
                                          ".*S3SseKmsKeyId when .*S3SseAlgorithm is 'AES256'"):
        dynamodb.meta.client.start_bulk_put_items(**request)

# The documentation says that ClientToken is optional and that an SDK
# generates one. DynamoDB requires it. botocore generates it, so the request
# is sent without botocore's handlers.
@pytest.mark.xfail(reason=NOT_IMPLEMENTED)
def test_start_client_token_required(dynamodb, test_table_s):
    request = valid_request(test_table_s.name)
    del request['ClientToken']
    with client_no_transform(dynamodb.meta.client) as client:
        with pytest.raises(ClientError, match="ValidationException.*'clientToken'.*must not be null"):
            client.start_bulk_put_items(**request)

# A table which does not exist, named by its name or by its ARN.
@pytest.mark.xfail(reason=NOT_IMPLEMENTED)
@pytest.mark.parametrize('by', ['name', 'arn'])
def test_start_missing_table(dynamodb, test_table_s, by):
    name = unique_table_name()
    if by == 'arn':
        name = get_table_arn(test_table_s).rpartition('/')[0] + '/' + name
    with pytest.raises(ClientError, match='ResourceNotFoundException.*not found'):
        dynamodb.meta.client.start_bulk_put_items(**valid_request(name))

# A TableName ARN must name a table of the caller's partition, service,
# region and account, so that a request cannot write into a table of the same
# name elsewhere. The limit of a table name also holds inside an ARN. In the
# ARN of a missing table, `value` changes the part at index `part` (see
# arn_with_part()).
@pytest.mark.xfail(reason=NOT_IMPLEMENTED)
@pytest.mark.parametrize('part,value,error', [
    pytest.param(1, lambda old: 'aws-cn', 'ValidationException.*Invalid AWS partition',
                 id='other_partition', marks=pytest.mark.xfail(reason=ARN_PARTS)),
    pytest.param(2, lambda old: 's3', 'ValidationException.*Invalid vendor code',
                 id='other_service', marks=pytest.mark.xfail(reason=ARN_PARTS)),
    pytest.param(3, lambda old: 'us-west-2' if old != 'us-west-2' else 'us-east-2',
                 'ValidationException.*Invalid AWS region', id='other_region',
                 marks=pytest.mark.xfail(reason=ARN_PARTS)),
    pytest.param(4, lambda old: '123456789012', 'AccessDeniedException', id='other_account',
                 marks=pytest.mark.xfail(reason=NO_ACCOUNT_ID)),
    pytest.param(5, lambda old: 'tablex/' + old.partition('/')[2],
                 'ValidationException.*Invalid resource type', id='not_a_table'),
    pytest.param(5, lambda old: old + '/index/gsi',
                 "ValidationException.*'tableName'.*Valid ARN format", id='index_arn'),
    pytest.param(5, lambda old: 'table/' + 't' * 256, 'ValidationException.*ARN.*255',
                 id='name_too_long'),
])
def test_start_bad_table_arn(dynamodb, test_table_s, part, value, error):
    arn = get_table_arn(test_table_s).rpartition('/')[0] + '/' + unique_table_name()
    old = arn.split(':', 5)[part]
    request = valid_request(arn_with_part(arn, part, value(old)))
    with pytest.raises(ClientError, match=error):
        dynamodb.meta.client.start_bulk_put_items(**request)

# A well-formed ARN of an operation which does not exist.
def unknown_operation_arn(table):
    return get_table_arn(table).partition(':table/')[0] + f':bulk-operation/{unknown_export_id()}'

# DescribeBulkOperation and CancelBulkOperation check the ARN in the same
# way: a malformed ARN is a ValidationException, an unknown one is not found.
@pytest.mark.xfail(reason=NOT_IMPLEMENTED)
@pytest.mark.parametrize('operation', ['describe_bulk_operation', 'cancel_bulk_operation'])
@pytest.mark.parametrize('make_arn,error', [
    pytest.param(lambda table: None, 'ValidationException.*Invalid Bulk operation ARN',
                 id='missing'),
    pytest.param(lambda table: 'x', 'ValidationException.*Invalid Bulk operation ARN',
                 id='not_an_arn'),
    # Not an ARN, though as long as the documented minimum of 37 characters.
    pytest.param(lambda table: 'a' * 37, 'ValidationException.*Invalid Bulk operation ARN',
                 id='minimum_length'),
    pytest.param(lambda table: get_table_arn(table) + '/bulk-operation/01759414677739-041d6d92',
                 'ValidationException.*Invalid Bulk operation ARN', id='under_a_table'),
    pytest.param(lambda table: unknown_operation_arn(table).rpartition('/')[0] + '/nonsense',
                 'ValidationException.*Bulk operation ARN is invalid', id='bad_operation_id'),
    pytest.param(unknown_operation_arn, 'ResourceNotFoundException.*does not exist',
                 id='unknown'),
    # The caller must be in the account of the operation.
    pytest.param(lambda table: arn_with_part(unknown_operation_arn(table), 4, '123456789012'),
                 'AccessDeniedException', id='other_account',
                 marks=pytest.mark.xfail(reason=NO_ACCOUNT_ID)),
    pytest.param(lambda table: arn_with_part(unknown_operation_arn(table), 1, 'aws-cn'),
                 'ValidationException.*Bulk operation ARN is invalid', id='other_partition',
                 marks=pytest.mark.xfail(reason=ARN_PARTS)),
    pytest.param(lambda table: arn_with_part(unknown_operation_arn(table), 2, 's3'),
                 'ValidationException.*Bulk operation ARN is invalid', id='other_service',
                 marks=pytest.mark.xfail(reason=ARN_PARTS)),
])
def test_bad_operation_arn(dynamodb, test_table_s, operation, make_arn, error):
    arn = make_arn(test_table_s)
    kwargs = {} if arn is None else {'BulkOperationArn': arn}
    with pytest.raises(ClientError, match=error):
        getattr(dynamodb.meta.client, operation)(**kwargs)

# MaxResults must be 1 to 25. NextToken must match ([0-9a-f]{16})+ and have
# at least 112 characters: the documented minimum length of 1 is wrong. A
# well-formed token which ListBulkOperations did not return is refused too.
@pytest.mark.xfail(reason=NOT_IMPLEMENTED)
@pytest.mark.parametrize('kwargs,message', [
    pytest.param({'MaxResults': 0}, "'maxResults'", id='max_results_0'),
    pytest.param({'MaxResults': -1}, "'maxResults'", id='max_results_negative'),
    pytest.param({'MaxResults': 26}, "'maxResults'", id='max_results_26'),
    pytest.param({'NextToken': ''}, "'nextToken'", id='empty_token'),
    pytest.param({'NextToken': '0' * 96}, "'nextToken'.*112", id='token_too_short'),
    pytest.param({'NextToken': 'xyz'}, "'nextToken'", id='token_not_hex'),
    pytest.param({'NextToken': '0' * 1040}, "'nextToken'", id='token_too_long'),
    pytest.param({'NextToken': '0' * 112}, 'nextToken is invalid', id='token_unknown'),
])
def test_list_validation(dynamodb, kwargs, message):
    with pytest.raises(ClientError, match=f'ValidationException.*{message}'):
        dynamodb.meta.client.list_bulk_operations(**kwargs)


###############################################################################
# Descriptions, from the start response to the end.

# StartBulkPutItems's response: the operation is IN_PROGRESS and echoes its
# request, with the defaults filled in.
@pytest.mark.xfail(reason=NOT_IMPLEMENTED)
@pytest.mark.parametrize('name', SCENARIOS)
def test_start_response(request, name):
    op = request.getfixturevalue(f'{name}_op')
    start = op.start
    assert set(start) == START_MEMBERS | ({'MaxWriteRequestUnits'} & op.request.keys())
    assert start['BulkOperationStatus'] == 'IN_PROGRESS'
    assert start['ProcessedItemCount'] == start['FailedItemCount'] == 0
    # The ARN is the account's, not the table's.
    arn_prefix, _, operation_id = start['BulkOperationArn'].partition(':bulk-operation/')
    assert arn_prefix == op.table_arn.partition(':table/')[0]
    assert re.fullmatch(r'\d{14}-[0-9a-f]{8}', operation_id)
    # botocore generates a ClientToken when the request has none.
    assert start['ClientToken'] == op.request.get('ClientToken', start['ClientToken'])
    assert start['ClientToken']
    # S3BucketOwner defaults to the caller's account, and
    # InputCompressionType to NONE.
    # FIXME: Alternator has no AWS account ID to report as S3BucketOwner.
    # Decide what it reports before this test loses its xfail mark.
    account = op.table_arn.split(':')[4]
    assert start[IN] == {'S3BucketOwner': account, 'InputCompressionType': 'NONE', **op.request[IN]}
    assert start[OUT] == {'S3BucketOwner': account, **op.request[OUT]}
    assert start['PerformanceTier'] == op.request.get('PerformanceTier', 'STANDARD')
    assert start.get('MaxWriteRequestUnits') == op.request.get('MaxWriteRequestUnits')
    # A request may name the table by its ARN; TableDetails has the name.
    assert start['TableDetails'] == [{'TableName': op.table_name, 'TableId': op.table_id}]

# DescribeBulkOperation while an operation runs: no member of the end yet,
# counters which never decrease, and a TotalInputBytes which never changes.
@pytest.mark.xfail(reason=NOT_IMPLEMENTED)
@pytest.mark.parametrize('name', SCENARIOS)
def test_in_flight_descriptions(request, name):
    samples = request.getfixturevalue(f'{name}_op').samples
    running = [d for d in samples if d['BulkOperationStatus'] not in TERMINAL_STATUSES]
    for description in running:
        assert description['BulkOperationStatus'] in ('IN_PROGRESS', 'CANCELLING')
        assert not set(description) & (END_MEMBERS | FAILURE_MEMBERS)
    for counter in ('ProcessedItemCount', 'FailedItemCount', 'ProcessedInputBytes'):
        seen = [d.get(counter, 0) for d in samples]
        assert seen == sorted(seen), f'{counter} went backwards: {seen}'
    totals = {d['TotalInputBytes'] for d in samples if 'TotalInputBytes' in d}
    assert len(totals) <= 1, f'TotalInputBytes changed: {totals}'

# The final description: the immutable members are unchanged, and the members
# of the end are there.
@pytest.mark.xfail(reason=NOT_IMPLEMENTED)
@pytest.mark.parametrize('name', SCENARIOS)
def test_final_description(request, name):
    op = request.getfixturevalue(f'{name}_op')
    final = op.final
    assert final['BulkOperationStatus'] in TERMINAL_STATUSES
    for member in IMMUTABLE_MEMBERS:
        assert final[member] == op.start[member], member
    assert final.get('MaxWriteRequestUnits') == op.start.get('MaxWriteRequestUnits')
    assert final['EndTime'] >= final['StartTime']
    usage = usage_of(final)
    assert usage['TableName'] == op.table_name
    # BulkPutBytes covers the table and its secondary indexes, not the
    # vector indexes.
    assert usage['BulkPutBytes'] == usage['Table']['BulkPutBytes'] + sum(
        index['BulkPutBytes'] for kind in ('GlobalSecondaryIndexes', 'LocalSecondaryIndexes')
        for index in usage.get(kind, {}).values())
    failed = final['BulkOperationStatus'] in ('FAILED', 'CANCELLED')
    assert set(final) & FAILURE_MEMBERS == (FAILURE_MEMBERS if failed else set())
    # Each output key is the manifest or a report file.
    assert op.output.unexpected == []
    if final['BulkOperationStatus'].startswith('COMPLETED'):
        # ProcessedInputBytes and TotalInputBytes count the bytes stored in
        # S3, so compressed bytes for a compressed input. ImportTable's
        # ProcessedSizeBytes counts uncompressed bytes instead.
        stored = sum(obj.size for obj in op.inputs.values())
        assert final['ProcessedInputBytes'] == final['TotalInputBytes'] == stored
        assert (final['FailedItemCount'] > 0) == (final['BulkOperationStatus'] == 'COMPLETED_WITH_ERRORS')


###############################################################################
# The outcome of each scenario.

# Records which exercise PutItem's semantics and the per-record conditions.
# The table has a GSI on "g", an LSI on "l" and a stream.
WRITES_SEED = [
    {'p': 'replaced', 'c': Decimal(1), 'old': 'gone', 'g': 'g-old'},
    {'p': 'existing', 'c': Decimal(1), 'v': 'original'},
    {'p': 'versioned', 'c': Decimal(1), 'ver': Decimal(5)},
    {'p': 'newer', 'c': Decimal(1), 'ver': Decimal(9)},
    {'p': 'status', 'c': Decimal(1), 'status': 'ACTIVE'},
]
SEED_BY_KEY = {key_of(item): item for item in WRITES_SEED}

# The records in file order. The itemIndex of a record is its position here:
# DynamoDB does not count the blank line which write_records() puts after
# WRITES_BLANK_AFTER. `written` says whether the condition holds.
class Write(NamedTuple):
    item: dict
    condition: str | None = None
    names: dict | None = None
    values: dict | None = None
    written: bool = True

WRITES = [
    # No condition: the whole item is replaced, as with PutItem, and it moves
    # in both indexes.
    Write({'p': 'replaced', 'c': Decimal(1), 'new': 'here', 'g': 'g-new', 'l': 'l-new'}),
    # Insert-only, on an existing item and on a new one.
    Write({'p': 'existing', 'c': Decimal(1), 'v': 'clobbered'}, 'attribute_not_exists(p)', written=False),
    Write({'p': 'fresh', 'c': Decimal(1)}, 'attribute_not_exists(p)'),
    # A version guard, with a newer version and with a stale one.
    Write({'p': 'versioned', 'c': Decimal(1), 'ver': Decimal(6)},
          '#v < :new', {'#v': 'ver'}, {':new': Decimal(6)}),
    Write({'p': 'newer', 'c': Decimal(1), 'ver': Decimal(3)},
          '#v < :new', {'#v': 'ver'}, {':new': Decimal(3)}, written=False),
    # A condition on a missing item. Its report record has no oldImage.
    Write({'p': 'absent', 'c': Decimal(1)}, 'attribute_exists(p)', written=False),
    # The documented guard for new items and newer versions together.
    Write({'p': 'brand-new', 'c': Decimal(1), 'ver': Decimal(1)},
          'attribute_not_exists(#v) OR #v < :new', {'#v': 'ver'}, {':new': Decimal(1)}),
    # A reserved word, through ExpressionAttributeNames.
    Write({'p': 'status', 'c': Decimal(1), 'status': 'DONE'},
          '#s = :v', {'#s': 'status'}, {':v': 'ACTIVE'}),
    Write({'p': 'indexed', 'c': Decimal(1), 'g': 'g-indexed', 'l': 'l-indexed'}),
    # The same key twice in one object. One of the two remains.
    Write({'p': 'twice', 'c': Decimal(1), 'which': 'first'}),
    Write({'p': 'twice', 'c': Decimal(1), 'which': 'second'}),
]
WRITES_BLANK_AFTER = 7
TWICE = ('twice', Decimal(1))

def write_records():
    lines = [record(w.item, w.condition, w.names, w.values) for w in WRITES]
    lines.insert(WRITES_BLANK_AFTER + 1, '')
    return lines

WRITES_TABLE_SCHEMA = {
    **KEY_SCHEMA,
    'AttributeDefinitions': KEY_SCHEMA['AttributeDefinitions'] + [string_attribute('g'),
                                                                  string_attribute('l')],
    'GlobalSecondaryIndexes': [GSI],
    'LocalSecondaryIndexes': [{'IndexName': 'lsi',
                               'KeySchema': [{'AttributeName': 'p', 'KeyType': 'HASH'},
                                             {'AttributeName': 'l', 'KeyType': 'RANGE'}],
                               'Projection': {'ProjectionType': 'ALL'}}],
    'StreamSpecification': {'StreamEnabled': True, 'StreamViewType': 'NEW_AND_OLD_IMAGES'},
}

@pytest.fixture(scope='module')
def writes_op(dynamodb, dynamodbstreams, s3, bucket):
    prefix = upload_lines(s3, bucket, write_records(), context='writes', name='00-conditions')
    upload_lines(s3, bucket, dynamodb_json_lines(GROUND_TRUTH_ITEMS), prefix=prefix,
                 name='01-ground-truth')
    s3.put_object(Bucket=bucket, Key=f'{prefix}02-empty.json', Body=b'')
    # An object outside the input. The request selects the keys which begin
    # with "<prefix>0", so the operation reads the three objects above, and
    # not "<prefix>10-other".
    upload_lines(s3, bucket, [record({'p': 'not-read', 'c': Decimal(1)})],
                 prefix=prefix, name='10-other')
    table = create_test_table(dynamodb, **WRITES_TABLE_SCHEMA)
    with delete_table_afterwards(dynamodb.meta.client, table.name):
        # A new stream can be ENABLING after its table is ACTIVE. Until it is
        # ENABLED, it can miss writes.
        stream_arn, _ = wait_for_active_stream(dynamodbstreams, table)
        seed(table, WRITES_SEED)
        op = run_operation(table, s3, bucket, {
            'TableName': table.name, 'ClientToken': random_string(20),
            IN: {'S3Bucket': bucket, 'S3KeyPrefix': prefix + '0', 'InputFormat': 'DYNAMODB_JSON'},
            OUT: {'S3Bucket': bucket, 'S3KeyPrefix': 'bulk-output/writes',
                  'OutputType': ['ERROR', 'CONDITION_CHECK_FAILURE']}})
        op.stream_arn = stream_arn
        op.items = full_scan(table)
        yield op

# Conditions, PutItem semantics, and both reports of a COMPLETED operation:
# condition failures do not make it COMPLETED_WITH_ERRORS. S3KeyPrefix is a
# key prefix, not a directory: the object "10-other" is not read.
@pytest.mark.xfail(reason=NOT_IMPLEMENTED)
def test_writes(writes_op, bucket):
    final = writes_op.final
    assert final['BulkOperationStatus'] == 'COMPLETED'
    # ProcessedItemCount counts records, including the condition failures.
    # FailedItemCount does not count condition failures.
    assert final['ProcessedItemCount'] == len(WRITES) + len(GROUND_TRUTH_ITEMS)
    assert final['FailedItemCount'] == 0
    # An unconditional record replaces the whole item.
    items = [item for item in writes_op.items if key_of(item) != TWICE]
    table = {**SEED_BY_KEY,
             **{key_of(w.item): w.item for w in WRITES if w.written and key_of(w.item) != TWICE},
             **{key_of(item): item for item in GROUND_TRUTH_ITEMS}}
    assert multiset(items) == multiset(table.values())
    [twice] = [item for item in writes_op.items if key_of(item) == TWICE]
    assert twice in [w.item for w in WRITES if key_of(w.item) == TWICE]
    # Every condition failure is reported, with the item of the record and,
    # if the item exists, the item in the table.
    assert set(writes_op.output.reports) == {'condition-check-failures'}
    conditions_key = [key for key in writes_op.inputs if key.endswith('00-conditions.json')][0]
    expected = {}
    for index, write in enumerate(WRITES):
        if not write.written:
            row = {'itemLocation': {'inputSourceArn': s3_arn(bucket, conditions_key),
                                    'itemIndex': index},
                   'errorMessages': ['The conditional request failed'],
                   'newImage': image(write.item)}
            if key_of(write.item) in SEED_BY_KEY:
                row['oldImage'] = image(SEED_BY_KEY[key_of(write.item)])
            expected[(conditions_key, index)] = row
    assert report_by_location(writes_op.output, 'condition-check-failures', bucket) == expected
    # The manifest has every object. An empty object has no range.
    manifest = manifest_by_key(writes_op.output, bucket, writes_op.inputs)
    assert manifest == {
        conditions_key: ('FULLY_PROCESSED', [[0, len(WRITES) - 1]]),
        conditions_key.replace('00-conditions', '01-ground-truth'):
            ('FULLY_PROCESSED', [[0, len(GROUND_TRUTH_ITEMS) - 1]]),
        conditions_key.replace('00-conditions', '02-empty'): ('FULLY_PROCESSED', []),
    }

# The GSI and the LSI are updated. Each index write is metered on its own:
# moving an item in the GSI is two writes, as with PutItem.
@pytest.mark.xfail(reason=NOT_IMPLEMENTED)
def test_writes_indexes(writes_op, dynamodb):
    table = dynamodb.Table(writes_op.table_name)
    with_g = [item for item in writes_op.items if 'g' in item]
    with_l = [item for item in writes_op.items if 'l' in item]
    assert {item['p'] for item in with_g} == {item['p'] for item in with_l} == {'replaced', 'indexed'}
    assert_gsi_scan(table, 'gsi', with_g)
    assert_lsi_scan(table, 'lsi', with_l)
    usage = usage_of(writes_op.final)
    assert usage['GlobalSecondaryIndexes'] == {'gsi': {'BulkPutBytes': 3 * KB}}
    assert usage['LocalSecondaryIndexes'] == {'lsi': {'BulkPutBytes': 2 * KB}}
    # Every record which reaches the table is metered, also a condition
    # failure: at least 1 KB each. An item above 1 KB is metered at its size,
    # not rounded up to whole KB. The DynamoDB JSON of a record is longer
    # than its item, so the line lengths give an upper limit.
    assert table_bytes(writes_op.final) >= (len(WRITES) + len(GROUND_TRUTH_ITEMS)) * KB
    lines = [line for line in write_records() + dynamodb_json_lines(GROUND_TRUTH_ITEMS) if line]
    assert table_bytes(writes_op.final) <= sum(max(KB, len(line.encode())) for line in lines)

# Each written item makes a stream record, as an application write does. A
# condition failure makes none. The table is new, so its stream from
# TRIM_HORIZON has the seed writes first, then the operation's writes.
@pytest.mark.xfail(reason=NOT_IMPLEMENTED)
def test_writes_stream(writes_op, dynamodbstreams):
    # The events of each key, in order. On DynamoDB the records of one key
    # are in order.
    expected = {key_of(item): ['INSERT'] for item in WRITES_SEED}
    seen = set(expected)
    for write in WRITES:
        if write.written:
            key = key_of(write.item)
            expected.setdefault(key, []).append('MODIFY' if key in seen else 'INSERT')
            seen.add(key)
    for item in GROUND_TRUTH_ITEMS:
        expected[key_of(item)] = ['INSERT']
    arn = writes_op.stream_arn
    iterators = [dynamodbstreams.get_shard_iterator(
                     StreamArn=arn, ShardId=shard, ShardIteratorType='TRIM_HORIZON')['ShardIterator']
                 for shard in list_shards(dynamodbstreams, arn)]
    events = []
    deadline = time.time() + 120
    while sum(map(len, expected.values())) > len(events) and time.time() < deadline:
        iterators = fetch_more(dynamodbstreams, iterators, events)
        time.sleep(1)
    got = {}
    for event in events:
        keys = event['dynamodb']['Keys']
        got.setdefault((keys['p']['S'], Decimal(keys['c']['N'])), []).append(event)
    assert {key: [e['eventName'] for e in key_events] for key, key_events in got.items()} == expected
    # The images, for the small items. GROUND_TRUTH_ITEMS have binary values,
    # which the stream returns as bytes, not base64.
    for write in WRITES:
        if write.written and key_of(write.item) != TWICE:
            event = got[key_of(write.item)][-1]
            assert event['dynamodb']['NewImage'] == image(write.item)['Item']
            if key_of(write.item) in SEED_BY_KEY:
                assert event['dynamodb']['OldImage'] == image(SEED_BY_KEY[key_of(write.item)])['Item']

# A value `levels` deep: maps, with a string at the end.
def nested(levels):
    return 'leaf' if levels == 1 else {'m': nested(levels - 1)}

# Records which fail validation one by one. Each is reported with its
# itemIndex, and DynamoDB reads the rest of the object. The report row of
# some of them also has the record's item as newImage.
# (line, message, has newImage)
RECORD_ERRORS = [
    (record({'p': 'r'}), 'Missing the key c', True),
    (record({'p': 'r', 'c': '2'}), 'Type mismatch for key c', False),
    (record({'p': 'r', 'c': Decimal(3), 'big': 'y' * 410000}),
     'Item size has exceeded the maximum allowed size', False),
    (record({'p': 'r', 'c': Decimal(4)}, 'attribute_not_exists('), 'Syntax error', False),
    (record({'p': 'r', 'c': Decimal(5)}, 'v = :undefined'),
     'An expression attribute value used in expression is not defined', False),
    (record({'p': 'r', 'c': Decimal(6)}, 'attribute_not_exists(p)', {'#unused': 'x'}),
     'Value provided in ExpressionAttributeNames unused in expressions', False),
    (record({'p': 'r', 'c': Decimal(10)}, 'attribute_not_exists(p)', values={':unused': 'x'}),
     'Value provided in ExpressionAttributeValues unused in expressions', False),
    (record({'p': 'r', 'c': Decimal(13)}, ''), 'The expression can not be empty', False),
    (record({'p': 'r', 'c': Decimal(14), 'm': nested(33)}),
     'Nesting Levels have exceeded supported limits', True),
    (record({'p': 'r', 'c': Decimal(7), 'g': Decimal(7)}), 'Type mismatch for Index Key g', True),
    (record({'p': 'r', 'c': Decimal(8), 'g': ''}), 'empty string value', True),
    # boto3 cannot serialize an empty set.
    ('{"Item": {"p": {"S": "r"}, "c": {"N": "9"}, "s": {"SS": []}}}', 'may not be empty', False),
]
ERRORS_SEED = [{'p': 'seeded', 'c': Decimal(0)}]
# Records which DynamoDB writes, although they look wrong: it ignores
# ExpressionAttributeNames and ExpressionAttributeValues without a
# ConditionExpression, which PutItem refuses. An item may nest 32 levels
# deep. (item, names, values)
ODD_WRITES = [
    ({'p': 'r', 'c': Decimal(15)}, {'#a': 'p'}, None),
    ({'p': 'r', 'c': Decimal(16)}, None, {':v': 'x'}),
    ({'p': 'r', 'c': Decimal(17), 'm': nested(32)}, None, None),
]

def record_error_lines():
    return ([record({'p': 'r', 'c': Decimal(0)})]
            + [line for line, _, _ in RECORD_ERRORS]
            # Condition failures on an existing and on a missing item, which
            # are not reported: the scenario requests only the ERROR report.
            + [record({'p': 'seeded', 'c': Decimal(0)}, 'attribute_not_exists(p)'),
               record({'p': 'r', 'c': Decimal(12)}, 'attribute_exists(p)'),
               record({'p': 'r', 'c': Decimal(11)})]
            + [record(item, None, names, values) for item, names, values in ODD_WRITES])

# Records after which DynamoDB cannot find the next record boundary. It
# reports them without an itemIndex and stops reading the object. Each one
# gets its own object, after a valid record and, unless the object ends with
# it, before another one. (line, message)
OBJECT_ERRORS = {
    'malformed-json': ('{"Item": {"p": {"S": "x"}, ', 'Unexpected token'),
    'unknown-field': (json.dumps({**image({'p': 'x', 'c': Decimal(1)}), 'Extra': 1}),
                      'Unexpected top-level field'),
    'missing-item': (json.dumps({'ConditionExpression': 'attribute_not_exists(p)'}),
                     "Expected 'Item' top level container"),
    'duplicate-item': ('{"Item": {"p": {"S": "x"}, "c": {"N": "1"}}, '
                       '"Item": {"p": {"S": "x"}, "c": {"N": "2"}}}', 'Expected character'),
    'invalid-number': ('{"Item": {"p": {"S": "x"}, "c": {"N": "abc"}}}',
                       'Expected a valid numeric'),
    'unknown-type': ('{"Item": {"p": {"S": "x"}, "c": {"N": "1"}, "v": {"X": "1"}}}',
                     'Expected DynamoDB type to be one of'),
    'invalid-base64': ('{"Item": {"p": {"S": "x"}, "c": {"N": "1"}, "b": {"B": "!!!"}}}',
                       'Expected a binary value in base64'),
    'number-condition': ('{"Item": {"p": {"S": "x"}, "c": {"N": "1"}}, "ConditionExpression": 5}',
                         'Expected a quoted char sequence'),
    'item-not-a-map': ('{"Item": "x"}', 'Expected character'),
    'null-item': ('{"Item": null}', 'Expected character'),
    'not-an-object': ('[1, 2]', 'Unexpected token'),
    # A record cut at the end of its object, as an interrupted upload leaves it.
    'truncated': (record({'p': 'x', 'c': Decimal(1)})[:-9], 'Unexpected end of item'),
}
STOPPED = 'Remainder of the file will not be processed'

ERRORS_TABLE_SCHEMA = {
    **KEY_SCHEMA,
    'AttributeDefinitions': KEY_SCHEMA['AttributeDefinitions'] + [string_attribute('g')],
    'GlobalSecondaryIndexes': [GSI],
}

@pytest.fixture(scope='module')
def errors_op(dynamodb, s3, bucket):
    prefix = upload_lines(s3, bucket, record_error_lines(), context='errors',
                          name='00-records', compression='GZIP')
    for name, (line, _) in OBJECT_ERRORS.items():
        if name == 'truncated':
            s3.put_object(Bucket=bucket, Key=f'{prefix}{name}.json.gz', Body=gzip.compress(
                (record({'p': name, 'c': Decimal(0)}) + '\n' + line).encode()))
            continue
        upload_lines(s3, bucket, [record({'p': name, 'c': Decimal(0)}), line,
                                  record({'p': name, 'c': Decimal(2)})],
                     prefix=prefix, name=name, compression='GZIP')
    # DynamoDB skips lines which hold only whitespace.
    upload_lines(s3, bucket, [record({'p': 'w', 'c': Decimal(0)}), '   ', '\t',
                              record({'p': 'w', 'c': Decimal(1)})],
                 prefix=prefix, name='whitespace', compression='GZIP')
    # Forms which a gzip reader can refuse: a zero-byte object, which is
    # not a gzip stream, and a stream of two members. And CRLF line ends
    # with no line end after the last record.
    s3.put_object(Bucket=bucket, Key=f'{prefix}empty.json.gz', Body=b'')
    s3.put_object(Bucket=bucket, Key=f'{prefix}members.json.gz', Body=b''.join(
        gzip.compress((record({'p': 'm', 'c': Decimal(c)}) + '\n').encode()) for c in (0, 1)))
    s3.put_object(Bucket=bucket, Key=f'{prefix}crlf.json.gz', Body=gzip.compress('\r\n'.join(
        record({'p': 'crlf', 'c': Decimal(c)}) for c in (0, 1)).encode()))
    # The documentation asks for one record per line, but DynamoDB reads
    # a stream of JSON values: a record may go over two lines, and two
    # records may share one line.
    s3.put_object(Bucket=bucket, Key=f'{prefix}stream.json.gz', Body=gzip.compress((
        record({'p': 'stream', 'c': Decimal(0)}).replace(', "c"', ',\n"c"') + ' '
        + record({'p': 'stream', 'c': Decimal(1)})).encode()))
    table = create_test_table(dynamodb, **ERRORS_TABLE_SCHEMA)
    with delete_table_afterwards(dynamodb.meta.client, table.name):
        seed(table, ERRORS_SEED)
        # A 1,000-character ClientToken is valid: the documentation gives no
        # maximum length.
        op = run_operation(table, s3, bucket, {
            'TableName': table.name, 'PerformanceTier': 'ACCELERATED', 'ClientToken': random_string(1000),
            IN: {'S3Bucket': bucket, 'S3KeyPrefix': prefix, 'InputFormat': 'DYNAMODB_JSON',
                 'InputCompressionType': 'GZIP'},
            OUT: {'S3Bucket': bucket, 'S3KeyPrefix': 'bulk-output/errors/', 'OutputType': ['ERROR']}})
        op.items = full_scan(table)
        yield op

# Validation errors: a record which fails alone is reported with its
# itemIndex, a record which stops its object is reported without one. Only
# the requested ERROR report is written, although a condition fails too.
@pytest.mark.xfail(reason=NOT_IMPLEMENTED)
def test_errors(errors_op, bucket):
    final = errors_op.final
    assert final['BulkOperationStatus'] == 'COMPLETED_WITH_ERRORS'
    records = record_error_lines()
    # Records after an object-stopping record are not counted. The
    # stopping record is.
    assert final['ProcessedItemCount'] == len(records) + 2 * len(OBJECT_ERRORS) + 8
    assert final['FailedItemCount'] == len(RECORD_ERRORS) + len(OBJECT_ERRORS)
    assert set(errors_op.output.reports) == {'errors'}
    report = report_by_location(errors_op.output, 'errors', bucket)
    key = lambda name: [k for k in errors_op.inputs if k.endswith(f'/{name}.json.gz')][0]
    expected_locations = ({(key('00-records'), index) for index in range(1, len(RECORD_ERRORS) + 1)}
                          | {(key(name), None) for name in OBJECT_ERRORS})
    assert set(report) == expected_locations
    for index, (line, message, has_image) in enumerate(RECORD_ERRORS, start=1):
        row = report[(key('00-records'), index)]
        assert re.search(message, ' '.join(row['errorMessages']))
        assert row.get('newImage') == (json.loads(line) if has_image else None), index
    for name, (_, message) in OBJECT_ERRORS.items():
        [text] = report[(key(name), None)]['errorMessages']
        assert re.search(message, text) and STOPPED in text, name
    # A stopped object is PARTIALLY_PROCESSED: only its first record is in
    # a range. The operation still read every byte.
    assert manifest_by_key(errors_op.output, bucket, errors_op.inputs) == {
        key('00-records'): ('FULLY_PROCESSED', [[0, len(records) - 1]]),
        key('whitespace'): ('FULLY_PROCESSED', [[0, 1]]),
        key('empty'): ('FULLY_PROCESSED', []),
        key('members'): ('FULLY_PROCESSED', [[0, 1]]),
        key('crlf'): ('FULLY_PROCESSED', [[0, 1]]),
        key('stream'): ('FULLY_PROCESSED', [[0, 1]]),
        **{key(name): ('PARTIALLY_PROCESSED', [[0, 0]]) for name in OBJECT_ERRORS},
    }
    written = ([{'p': 'r', 'c': Decimal(0)}, {'p': 'r', 'c': Decimal(11)}] + ERRORS_SEED
               + [item for item, _, _ in ODD_WRITES]
               + [{'p': p, 'c': Decimal(c)} for p in ('w', 'm', 'crlf', 'stream') for c in (0, 1)]
               + [{'p': name, 'c': Decimal(0)} for name in OBJECT_ERRORS])
    assert multiset(errors_op.items) == multiset(written)
    # A record which fails validation is not metered, also not on the GSI.
    # The two condition failures are metered: they reached the table, and a
    # missing item counts as 1 KB. The seed itself is not written by the
    # operation.
    assert table_bytes(final) == (len(written) - len(ERRORS_SEED) + 2) * KB
    assert usage_of(final)['BulkPutBytes'] == table_bytes(final)

# Ion input. The supported types are string, decimal, bool, blob, null, list
# and struct, plus a list with a $dynamodb_SS, $dynamodb_NS or $dynamodb_BS
# annotation for a set.
ION_TYPES_LINE = ('$ion_1_0 {Item:{p:"types",c:1.,s:"text",n:-12.50,e:1d2,'
                  'b:{{aGVsbG8=}},t:true,f:false,z:null,l:["x",1.,false,null],'
                  'm:{k:"v",n:{d:2.}},el:[],em:{},ss:$dynamodb_SS::["a","b"],'
                  'ns:$dynamodb_NS::[1.,2.5],bs:$dynamodb_BS::[{{AA==}},{{AQ==}}]}}')
ION_TYPES_ITEM = {'p': 'types', 'c': Decimal(1), 's': 'text', 'n': Decimal('-12.5'),
                  'e': Decimal(100), 'b': Binary(b'hello'), 't': True, 'f': False,
                  'z': None, 'l': ['x', Decimal(1), False, None],
                  'm': {'k': 'v', 'n': {'d': Decimal(2)}}, 'el': [], 'em': {},
                  'ss': {'a', 'b'}, 'ns': {Decimal(1), Decimal('2.5')},
                  'bs': {Binary(b'\x00'), Binary(b'\x01')}}
ION_SEED = [{'p': 'seeded', 'c': Decimal(0), 'v': 'kept'},
            {'p': 'guarded', 'c': Decimal(1), 'status': 'ACTIVE'}]
ION_LINES = [
    ION_TYPES_LINE,
    # The documentation says that each line begins with an Ion version
    # marker, but DynamoDB accepts a record without one.
    '{Item:{p:"no-marker",c:1.}}',
    '$ion_1_0 {Item:{p:"seeded",c:0.},ConditionExpression:"attribute_not_exists(p)"}',
    '$ion_1_0 {Item:{p:"guarded",c:1.,status:"DONE"},ConditionExpression:"#s = :v",'
    'ExpressionAttributeNames:{"#s":"status"},ExpressionAttributeValues:{":v":"ACTIVE"}}',
    # Not reported: the scenario requests only the CONDITION_CHECK_FAILURE
    # report.
    '$ion_1_0 {Item:{p:"missing-sort-key"}}',
    # A record may go over two lines.
    '$ion_1_0 {Item:{p:"two-lines",\nc:1.}}',
    # A DYNAMODB_JSON record is also valid Ion, so DynamoDB reads it as an Ion
    # struct and refuses it: its key p is a struct, not a string. Not
    # reported either.
    record({'p': 'json', 'c': Decimal(1)}),
]
ION_WRITTEN = [ION_TYPES_ITEM, {'p': 'no-marker', 'c': Decimal(1)},
               {'p': 'guarded', 'c': Decimal(1), 'status': 'DONE'},
               {'p': 'two-lines', 'c': Decimal(1)}]

def ion_record(p, c, *extra):
    return '$ion_1_0 {Item:{' + ','.join([f'p:"{p}"', f'c:{c}.', *extra]) + '}}'

# Ion records which stop their object: a value of an unsupported type (even
# an int, because an Ion number must be a decimal), and the structure errors
# which also stop a DYNAMODB_JSON object. Each one gets its own object,
# between two valid records.
ION_OBJECT_ERRORS = {
    **{name: ion_record(name, 1, value) for name, value in {
        'int': 'i:7',
        'float': 'f:1.5e0',
        'timestamp': 't:2020-01-01T00:00Z',
        'symbol': 's:abc',
        'clob': 'x:{{"text"}}',
        'sexp': 'x:(a b)',
        'typed-null': 'x:null.string',
        'other-annotation': 'x:foo::"x"',
    }.items()},
    'unknown-field': '$ion_1_0 {Item:{p:"unknown-field",c:1.},Extra:1.}',
    'missing-item': '$ion_1_0 {ConditionExpression:"attribute_not_exists(p)"}',
    'duplicate-item': '$ion_1_0 {Item:{p:"duplicate-item",c:1.},Item:{p:"duplicate-item",c:3.}}',
}

@pytest.fixture(scope='module')
def ion_op(dynamodb, s3, bucket):
    prefix = upload_lines(s3, bucket, ION_LINES, context='ion', name='00-types',
                          extension='.ion', compression='ZSTD')
    for name, line in ION_OBJECT_ERRORS.items():
        upload_lines(s3, bucket, [ion_record(name, 0), line, ion_record(name, 2)],
                     prefix=prefix, name=name, extension='.ion', compression='ZSTD')
    table = create_test_table(dynamodb, **KEY_SCHEMA)
    with delete_table_afterwards(dynamodb.meta.client, table.name):
        seed(table, ION_SEED)
        # TableName may be the table's ARN.
        op = run_operation(table, s3, bucket, {
            'TableName': table.table_arn, 'PerformanceTier': 'ACCELERATED',
            IN: {'S3Bucket': bucket, 'S3KeyPrefix': prefix, 'InputFormat': 'ION',
                 'InputCompressionType': 'ZSTD'},
            OUT: {'S3Bucket': bucket, 'S3KeyPrefix': 'bulk-output/ion',
                  'OutputType': ['CONDITION_CHECK_FAILURE'], 'S3SseAlgorithm': 'AES256'}})
        op.items = full_scan(table)
        yield op

# Ion input: the supported types and conditions, and the unsupported types
# and structure errors, which stop their object. Only the requested
# CONDITION_CHECK_FAILURE report is written, although records fail
# validation too, so the messages of the Ion errors are not checked.
@pytest.mark.xfail(reason=NOT_IMPLEMENTED)
def test_ion(ion_op, bucket):
    final = ion_op.final
    assert final['BulkOperationStatus'] == 'COMPLETED_WITH_ERRORS'
    assert final['ProcessedItemCount'] == len(ION_LINES) + 2 * len(ION_OBJECT_ERRORS)
    # The record without its sort key and the DYNAMODB_JSON record fail alone.
    assert final['FailedItemCount'] == 2 + len(ION_OBJECT_ERRORS)
    goods = [{'p': name, 'c': Decimal(0)} for name in ION_OBJECT_ERRORS]
    assert multiset(ion_op.items) == multiset(ION_WRITTEN + ION_SEED[:1] + goods)
    assert set(ion_op.output.reports) == {'condition-check-failures'}
    key = lambda name: [k for k in ion_op.inputs if k.endswith(f'/{name}.ion.zst')][0]
    # Report images are DynamoDB JSON, whatever the input format.
    assert report_by_location(ion_op.output, 'condition-check-failures', bucket) == {
        (key('00-types'), 2): {
            'itemLocation': {'inputSourceArn': s3_arn(bucket, key('00-types')), 'itemIndex': 2},
            'errorMessages': ['The conditional request failed'],
            'newImage': image({'p': 'seeded', 'c': Decimal(0)}),
            'oldImage': image(ION_SEED[0])}}
    assert manifest_by_key(ion_op.output, bucket, ion_op.inputs) == {
        key('00-types'): ('FULLY_PROCESSED', [[0, len(ION_LINES) - 1]]),
        **{key(name): ('PARTIALLY_PROCESSED', [[0, 0]]) for name in ION_OBJECT_ERRORS},
    }
    # The written items and the one condition failure, 1 KB each.
    assert table_bytes(final) == (len(ION_WRITTEN) + len(goods) + 1) * KB

# small_input puts an object into the bucket. So reading the whole bucket
# instead of the prefix would be caught.
@pytest.fixture(scope='module')
def empty_prefix_op(dynamodb, s3, bucket, small_input):
    table = create_test_table(dynamodb, **KEY_SCHEMA)
    with delete_table_afterwards(dynamodb.meta.client, table.name):
        op = run_operation(table, s3, bucket, {
            'TableName': table.name, 'PerformanceTier': 'ACCELERATED',
            IN: {'S3Bucket': bucket, 'S3KeyPrefix': unique_prefix('empty'), 'InputFormat': 'DYNAMODB_JSON'},
            OUT: {'S3Bucket': bucket, 'S3KeyPrefix': 'bulk-output/empty_prefix',
                  'OutputType': ['ERROR', 'CONDITION_CHECK_FAILURE']}})
        op.items = full_scan(table)
        yield op

# A prefix which matches no object is an empty success, with an empty
# manifest.
@pytest.mark.xfail(reason=NOT_IMPLEMENTED)
def test_empty_prefix(empty_prefix_op):
    final = empty_prefix_op.final
    assert final['BulkOperationStatus'] == 'COMPLETED'
    assert final['ProcessedItemCount'] == final['FailedItemCount'] == 0
    assert final['ProcessedInputBytes'] == final['TotalInputBytes'] == 0
    assert usage_of(final) == {'TableName': empty_prefix_op.table_name, 'BulkPutBytes': 0,
                               'Table': {'BulkPutBytes': 0}}
    assert empty_prefix_op.output.manifest == []
    assert empty_prefix_op.output.reports == {}
    assert empty_prefix_op.items == []

# The input is a bucket of its own: one object in a "directory" and one at
# the top level.
@pytest.fixture(scope='module')
def whole_bucket_op(dynamodb, s3, bucket):
    with new_s3_bucket(s3) as whole_bucket:
        upload_lines(s3, whole_bucket, [record({'p': 'whole', 'c': Decimal(1)})],
                     context='top')
        s3.put_object(Bucket=whole_bucket, Key='at-the-top.json',
                      Body=(record({'p': 'whole', 'c': Decimal(2)}) + '\n').encode())
        table = create_test_table(dynamodb, **KEY_SCHEMA)
        with delete_table_afterwards(dynamodb.meta.client, table.name):
            # No S3KeyPrefix in the input or in the output.
            op = run_operation(table, s3, bucket, {
                'TableName': table.name, 'PerformanceTier': 'ACCELERATED',
                IN: {'S3Bucket': whole_bucket, 'InputFormat': 'DYNAMODB_JSON'},
                OUT: {'S3Bucket': bucket, 'OutputType': ['ERROR', 'CONDITION_CHECK_FAILURE']}})
            op.items = full_scan(table)
            yield op

# Without S3KeyPrefix the input is the whole bucket, also the objects in
# "directories", and the output is at the bucket's top level.
@pytest.mark.xfail(reason=NOT_IMPLEMENTED)
def test_whole_bucket(whole_bucket_op):
    final = whole_bucket_op.final
    assert final['BulkOperationStatus'] == 'COMPLETED'
    assert 'S3KeyPrefix' not in final[IN] and 'S3KeyPrefix' not in final[OUT]
    # read_output() looks at the bucket's top level, as no prefix was given.
    assert whole_bucket_op.output.manifest is not None
    assert multiset(whole_bucket_op.items) == multiset([{'p': 'whole', 'c': Decimal(1)},
                                                        {'p': 'whole', 'c': Decimal(2)}])
    manifest = manifest_by_key(whole_bucket_op.output, final[IN]['S3Bucket'], whole_bucket_op.inputs)
    assert manifest == {key: ('FULLY_PROCESSED', [[0, 0]]) for key in whole_bucket_op.inputs}

# The documentation gives no maximum of MaxWriteRequestUnits.
@pytest.fixture(scope='module')
def missing_bucket_op(dynamodb, s3, bucket, small_input):
    table = create_test_table(dynamodb, **KEY_SCHEMA)
    with delete_table_afterwards(dynamodb.meta.client, table.name):
        op = run_operation(table, s3, bucket, {
            'TableName': table.name, 'PerformanceTier': 'ACCELERATED', 'ClientToken': random_string(20),
            'MaxWriteRequestUnits': 2**62,
            IN: {'S3Bucket': unique_bucket_name(), 'S3KeyPrefix': small_input, 'InputFormat': 'DYNAMODB_JSON'},
            OUT: {'S3Bucket': bucket, 'S3KeyPrefix': 'bulk-output/missing_bucket',
                  'OutputType': ['ERROR', 'CONDITION_CHECK_FAILURE']}})
        op.items = full_scan(table)
        yield op

# OutputType may repeat a value.
@pytest.fixture(scope='module')
def wrong_owner_op(dynamodb, s3, bucket, small_input):
    table = create_test_table(dynamodb, **KEY_SCHEMA)
    with delete_table_afterwards(dynamodb.meta.client, table.name):
        op = run_operation(table, s3, bucket, {
            'TableName': table.name, 'PerformanceTier': 'ACCELERATED',
            IN: {'S3Bucket': bucket, 'S3KeyPrefix': small_input, 'InputFormat': 'DYNAMODB_JSON',
                 'S3BucketOwner': '0' * 12},
            OUT: {'S3Bucket': bucket, 'S3KeyPrefix': 'bulk-output/wrong_owner', 'OutputType': ['ERROR', 'ERROR']}})
        op.items = full_scan(table)
        yield op

# Operations which fail before they read the input: no item, no manifest,
# no report. The members of the input size are absent.
@pytest.mark.xfail(reason=NOT_IMPLEMENTED)
@pytest.mark.parametrize('name,code,message', [
    # The documented table of failure codes does not have S3NoSuchBucket.
    ('missing_bucket', 'S3NoSuchBucket', 'The specified bucket does not exist'),
    # S3BucketOwner guards against a bucket which changed hands. Alternator
    # has no AWS account ids, so this needs a design decision there; see
    # test_import_with_wrong_s3_bucket_owner in test_import.py.
    pytest.param('wrong_owner', 'S3AccessDenied', 'Access Denied',
                 marks=pytest.mark.xfail(reason=NO_ACCOUNT_ID)),
])
def test_early_failure(request, name, code, message):
    op = request.getfixturevalue(f'{name}_op')
    final = op.final
    assert final['BulkOperationStatus'] == 'FAILED'
    assert final['FailureCode'] == code
    assert message in final['FailureMessage']
    assert not set(final) & INPUT_SIZE_MEMBERS
    assert final['ProcessedItemCount'] == final['FailedItemCount'] == 0
    assert table_bytes(final) == 0
    assert op.output.keys == []
    assert op.items == []

@pytest.fixture(scope='module')
def missing_output_bucket_op(dynamodb, s3, bucket, small_input):
    table = create_test_table(dynamodb, **KEY_SCHEMA)
    with delete_table_afterwards(dynamodb.meta.client, table.name):
        op = run_operation(table, s3, bucket, {
            'TableName': table.name,
            IN: {'S3Bucket': bucket, 'S3KeyPrefix': small_input, 'InputFormat': 'DYNAMODB_JSON'},
            OUT: {'S3Bucket': unique_bucket_name(), 'S3KeyPrefix': 'bulk-output/missing_output_bucket',
                  'OutputType': ['ERROR', 'CONDITION_CHECK_FAILURE']}})
        op.items = full_scan(table)
        yield op

@pytest.fixture(scope='module')
def wrong_output_owner_op(dynamodb, s3, bucket, small_input):
    table = create_test_table(dynamodb, **KEY_SCHEMA)
    with delete_table_afterwards(dynamodb.meta.client, table.name):
        op = run_operation(table, s3, bucket, {
            'TableName': table.name, 'PerformanceTier': 'ACCELERATED',
            IN: {'S3Bucket': bucket, 'S3KeyPrefix': small_input, 'InputFormat': 'DYNAMODB_JSON'},
            OUT: {'S3Bucket': bucket, 'S3KeyPrefix': 'bulk-output/wrong_output_owner',
                  'OutputType': ['ERROR', 'CONDITION_CHECK_FAILURE'], 'S3BucketOwner': '0' * 12}})
        op.items = full_scan(table)
        yield op

# Operations whose output DynamoDB cannot write fail only at their end:
# DynamoDB writes the items, cannot write the manifest, and writes no output.
@pytest.mark.xfail(reason=NOT_IMPLEMENTED)
@pytest.mark.parametrize('name,code', [
    ('missing_output_bucket', 'S3NoSuchBucket'),
    # See the wrong_owner case of test_early_failure.
    pytest.param('wrong_output_owner', 'S3AccessDenied',
                 marks=pytest.mark.xfail(reason=NO_ACCOUNT_ID)),
])
def test_late_failure(request, name, code):
    op = request.getfixturevalue(f'{name}_op')
    final = op.final
    assert final['BulkOperationStatus'] == 'FAILED'
    assert final['FailureCode'] == code
    assert final['FailureMessage'].startswith('Manifest is not available.')
    assert final['ProcessedItemCount'] == 1 and final['FailedItemCount'] == 0
    assert table_bytes(final) == KB
    assert op.output.keys == []
    assert op.items == [{'p': 'small', 'c': Decimal(1)}]

@pytest.fixture(scope='module')
def unreadable_op(dynamodb, s3, bucket):
    # One valid object, one which is not GZIP although the request says
    # so, and one with invalid UTF-8 in its second record.
    prefix = upload_lines(s3, bucket, [record({'p': 'g', 'c': Decimal(1)})],
                          context='unreadable', name='0-good', compression='GZIP')
    s3.put_object(Bucket=bucket, Key=f'{prefix}1-plain.json.gz',
                  Body=(record({'p': 'plain', 'c': Decimal(1)}) + '\n').encode())
    # upload_lines() takes text, which cannot hold invalid UTF-8.
    s3.put_object(Bucket=bucket, Key=f'{prefix}2-invalid-utf8.json.gz', Body=gzip.compress(
        (record({'p': 'u', 'c': Decimal(0)}) + '\n').encode()
        + b'{"Item": {"p": {"S": "u\xff\xfe"}, "c": {"N": "1"}}}\n'
        + (record({'p': 'u', 'c': Decimal(2)}) + '\n').encode()))
    table = create_test_table(dynamodb, **KEY_SCHEMA)
    with delete_table_afterwards(dynamodb.meta.client, table.name):
        op = run_operation(table, s3, bucket, {
            'TableName': table.name, 'PerformanceTier': 'ACCELERATED',
            IN: {'S3Bucket': bucket, 'S3KeyPrefix': prefix, 'InputFormat': 'DYNAMODB_JSON',
                 'InputCompressionType': 'GZIP'},
            OUT: {'S3Bucket': bucket, 'S3KeyPrefix': 'bulk-output/unreadable', 'OutputType': ['ERROR']}})
        op.items = full_scan(table)
        yield op

# An object which DynamoDB cannot decompress leaves stored bytes unread, so
# the operation fails with ValidationException, though it wrote the valid
# records. Invalid UTF-8 stops its object, as the errors of test_errors do.
@pytest.mark.xfail(reason=NOT_IMPLEMENTED)
def test_unreadable_objects(unreadable_op, bucket):
    final = unreadable_op.final
    assert final['BulkOperationStatus'] == 'FAILED'
    assert final['FailureCode'] == 'ValidationException'
    assert 'failed validation checks' in final['FailureMessage']
    assert final['ProcessedInputBytes'] < final['TotalInputBytes']
    assert final['ProcessedItemCount'] == 4 and final['FailedItemCount'] == 2
    assert table_bytes(final) == 2 * KB
    assert multiset(unreadable_op.items) == multiset([{'p': 'g', 'c': Decimal(1)},
                                                      {'p': 'u', 'c': Decimal(0)}])
    key = lambda name: [k for k in unreadable_op.inputs if k.endswith(f'/{name}.json.gz')][0]
    report = report_by_location(unreadable_op.output, 'errors', bucket)
    assert set(report) == {(key('1-plain'), None), (key('2-invalid-utf8'), None)}
    assert report[(key('1-plain'), None)]['errorMessages'] == ['The S3 object could not be decompressed']
    [text] = report[(key('2-invalid-utf8'), None)]['errorMessages']
    assert 'UTF-8' in text and STOPPED in text
    assert manifest_by_key(unreadable_op.output, bucket, unreadable_op.inputs) == {
        key('0-good'): ('FULLY_PROCESSED', [[0, 0]]),
        key('1-plain'): ('PARTIALLY_PROCESSED', []),
        key('2-invalid-utf8'): ('PARTIALLY_PROCESSED', [[0, 0]])}

# The input of deleted_table and cancel_midway, which act while their
# operation writes: CANCEL_OBJECTS GZIP objects of CANCEL_ITEMS tiny items
# each. MaxWriteRequestUnits holds the operation to at most 1,024 items per
# second, so it still writes when its scenario acts.
# FIXME: at that rate the 200,000 items take about 195 s, but
# wait_for_operation() allows only 10 s outside AWS. When Alternator obeys
# MaxWriteRequestUnits, change the local timeout or the input size.
CANCEL_OBJECTS, CANCEL_ITEMS = 4, 50000

def cancel_key(obj, index):
    return {'p': f'k{obj}-{index:06}', 'c': Decimal(1)}

@pytest.fixture(scope='module')
def long_input(s3, bucket):
    prefix = unique_prefix('cancel')
    for obj in range(CANCEL_OBJECTS):
        upload_lines(s3, bucket, dynamodb_json_lines(cancel_key(obj, i) for i in range(CANCEL_ITEMS)),
                     prefix=prefix, name=f'{obj}', compression='GZIP')
    return prefix

# The table is deleted when the progress of the operation first shows: then
# the operation writes. It writes thousands of items before the delete takes
# effect, so it uses the Standard tier, which costs half as much per item.
@pytest.fixture(scope='module')
def deleted_table_op(dynamodb, s3, bucket, long_input):
    client = dynamodb.meta.client
    actions = {}
    def delete_while_writing(op, description):
        if (actions or description['BulkOperationStatus'] != 'IN_PROGRESS'
                or not description['ProcessedItemCount']):
            return
        actions['progress'] = description
        # DynamoDB allows DeleteTable while an operation writes to the table.
        actions['delete'] = client.delete_table(TableName=op.table_name)
    table = create_test_table(dynamodb, **KEY_SCHEMA)
    with delete_table_afterwards(client, table.name):
        op = run_operation(table, s3, bucket, {
            'TableName': table.name, 'PerformanceTier': 'STANDARD', 'MaxWriteRequestUnits': 1024,
            IN: {'S3Bucket': bucket, 'S3KeyPrefix': long_input, 'InputFormat': 'DYNAMODB_JSON',
                 'InputCompressionType': 'GZIP'},
            OUT: {'S3Bucket': bucket, 'S3KeyPrefix': 'bulk-output/deleted_table',
                  'OutputType': ['ERROR', 'CONDITION_CHECK_FAILURE']}},
            on_sample=delete_while_writing)
        op.actions = actions
        yield op

# An operation whose table is deleted while it writes fails. The manifest
# lists every object as not fully processed: NOT_AVAILABLE, with no range,
# means that DynamoDB cannot say which records it processed.
@pytest.mark.xfail(reason=NOT_IMPLEMENTED)
def test_deleted_table(deleted_table_op, bucket):
    final, actions = deleted_table_op.final, deleted_table_op.actions
    assert actions, 'the operation did not show progress while it ran'
    assert actions['delete']['TableDescription']['TableStatus'] == 'DELETING'
    assert final['BulkOperationStatus'] == 'FAILED'
    assert final['FailureCode'] == 'DynamoDBResourceNotFoundException'
    assert 'not found' in final['FailureMessage']
    assert final['ProcessedItemCount'] > 0 and table_bytes(final) > 0
    manifest = manifest_by_key(deleted_table_op.output, bucket, deleted_table_op.inputs)
    assert set(manifest) == set(deleted_table_op.inputs)
    for status, ranges in manifest.values():
        assert status in ('PARTIALLY_PROCESSED', 'NOT_PROCESSED', 'NOT_AVAILABLE')
        if status != 'PARTIALLY_PROCESSED':
            assert ranges == []

# The operation is cancelled at its first sample, which follows the start at
# once.
@pytest.fixture(scope='module')
def cancel_early_op(dynamodb, s3, bucket, small_input):
    client = dynamodb.meta.client
    def cancel_at_once(op, description):
        if len(op.samples) == 1:
            op.cancel = client.cancel_bulk_operation(BulkOperationArn=op.arn)
    table = create_test_table(dynamodb, **KEY_SCHEMA)
    with delete_table_afterwards(client, table.name):
        op = run_operation(table, s3, bucket, {
            'TableName': table.name, 'ClientToken': random_string(20),
            IN: {'S3Bucket': bucket, 'S3KeyPrefix': small_input, 'InputFormat': 'DYNAMODB_JSON'},
            OUT: {'S3Bucket': bucket, 'S3KeyPrefix': 'bulk-output/cancel_early',
                  'OutputType': ['ERROR', 'CONDITION_CHECK_FAILURE']}},
            on_sample=cancel_at_once)
        op.items = full_scan(table)
        yield op

# An operation cancelled at once ends CANCELLED within seconds. The
# documentation says that the manifest records what a cancelled operation
# processed, but there is no manifest.
@pytest.mark.xfail(reason=NOT_IMPLEMENTED)
def test_cancel_early(cancel_early_op):
    cancel = {k: v for k, v in cancel_early_op.cancel.items() if k != 'ResponseMetadata'}
    assert cancel == {'BulkOperationArn': cancel_early_op.arn, 'BulkOperationStatus': 'CANCELLING'}
    final = cancel_early_op.final
    assert final['BulkOperationStatus'] == 'CANCELLED'
    assert final['FailureCode'] == 'RequestCancelled'
    assert final['FailureMessage'] == 'The request was cancelled'
    assert cancel_early_op.output.keys == []
    assert cancel_early_op.items == []

# The operation is cancelled when its progress first shows: then it writes.
# A cancel soon after the first progress leaves most items unwritten. Before
# the cancel, the scenario lists the operations and tries a second start on
# the table.
@pytest.fixture(scope='module')
def cancel_midway_op(dynamodb, s3, bucket, long_input):
    client = dynamodb.meta.client
    actions = {}
    def cancel_while_writing(op, description):
        if (actions or description['BulkOperationStatus'] != 'IN_PROGRESS'
                or not description['ProcessedItemCount']):
            return
        actions['progress'] = description
        actions['listed'] = list_through(client, [op.arn], MaxResults=25)
        actions['second_start'] = try_start(client, {**op.request, 'ClientToken': random_string(20)})
        for action in ('cancel', 'cancel_again'):
            actions[action] = client.cancel_bulk_operation(BulkOperationArn=op.arn)
    table = create_test_table(dynamodb, **KEY_SCHEMA)
    with delete_table_afterwards(client, table.name):
        op = run_operation(table, s3, bucket, {
            'TableName': table.name, 'MaxWriteRequestUnits': 1024,
            IN: {'S3Bucket': bucket, 'S3KeyPrefix': long_input, 'InputFormat': 'DYNAMODB_JSON',
                 'InputCompressionType': 'GZIP'},
            OUT: {'S3Bucket': bucket, 'S3KeyPrefix': 'bulk-output/cancel_midway',
                  'OutputType': ['ERROR', 'CONDITION_CHECK_FAILURE']}},
            on_sample=cancel_while_writing)
        op.actions = actions
        second = actions.get('second_start')
        if second is not None and not isinstance(second, ClientError):
            wait_for_operation(client, second['BulkOperationArn'], [])
        op.items = full_scan(table)
        yield op

# An operation cancelled while it writes keeps the items it wrote. The
# manifest and the counters say which ones.
@pytest.mark.xfail(reason=NOT_IMPLEMENTED)
def test_cancel_midway(cancel_midway_op, bucket):
    final, actions = cancel_midway_op.final, cancel_midway_op.actions
    assert actions, 'the operation did not show progress while it ran'
    # A start on a table with a running operation fails at once. Two starts
    # at the same time can both get an ARN; see test_concurrent_starts.
    expect_error(actions['second_start'], 'ResourceInUseException',
                 'Table has ongoing bulk operations')
    for action in ('cancel', 'cancel_again'):
        assert actions[action]['BulkOperationStatus'] == 'CANCELLING'
    # The status goes from IN_PROGRESS through CANCELLING to CANCELLED. The
    # samples are 5 s apart, so they can miss CANCELLING.
    statuses = [d['BulkOperationStatus'] for d in cancel_midway_op.samples]
    order = ['IN_PROGRESS', 'CANCELLING', 'CANCELLED']
    assert statuses[-1] == 'CANCELLED'
    assert statuses == sorted(statuses, key=order.index)
    assert final['FailureCode'] == 'RequestCancelled'
    assert final['ProcessedInputBytes'] <= final['TotalInputBytes']
    # Every written item is within a processed range of its object, and every
    # processed record is written.
    manifest = manifest_by_key(cancel_midway_op.output, bucket, cancel_midway_op.inputs)
    assert len(manifest) == CANCEL_OBJECTS
    assert 'FULLY_PROCESSED' not in {status for status, _ in manifest.values()}
    expected = []
    for key, (_, ranges) in manifest.items():
        obj = int(key.rpartition('/')[2].split('.')[0])
        for first, last in ranges:
            expected += [cancel_key(obj, index) for index in range(first, last + 1)]
    assert 0 < len(expected) < CANCEL_OBJECTS * CANCEL_ITEMS
    assert multiset(cancel_midway_op.items) == multiset(expected)
    assert final['ProcessedItemCount'] == len(expected)
    assert table_bytes(final) == len(expected) * KB

# Cancelling an operation which has ended is refused, whatever its terminal
# status, also when the operation ended because it was cancelled.
@pytest.mark.xfail(reason=NOT_IMPLEMENTED)
@pytest.mark.parametrize('name', ['writes', 'errors', 'missing_bucket', 'cancel_early'])
def test_cancel_finished(request, dynamodb, name):
    op = request.getfixturevalue(f'{name}_op')
    status = op.final['BulkOperationStatus']
    with pytest.raises(ClientError, match=f'ValidationException.*already in terminal state: {status}'):
        dynamodb.meta.client.cancel_bulk_operation(BulkOperationArn=op.arn)

# Two operations started together on one table, each with its own output
# prefix, and what they did.
@pytest.fixture(scope='module')
def concurrent_starts(dynamodb, s3, bucket, small_input):
    client = dynamodb.meta.client
    table = create_test_table(dynamodb, **KEY_SCHEMA)
    with delete_table_afterwards(client, table.name):
        ops, start_errors = [], []
        for i in range(2):
            request = {'TableName': table.name,
                       IN: {'S3Bucket': bucket, 'S3KeyPrefix': small_input, 'InputFormat': 'DYNAMODB_JSON'},
                       OUT: {'S3Bucket': bucket, 'S3KeyPrefix': f'bulk-output/concurrent-{i}',
                             'OutputType': ['ERROR', 'CONDITION_CHECK_FAILURE']}}
            response = try_start(client, request)
            if isinstance(response, ClientError):
                start_errors.append(response)
            else:
                ops.append(SimpleNamespace(request=request, arn=response['BulkOperationArn'], samples=[]))
        for op in ops:
            wait_for_operation(client, op.arn, op.samples)
            op.output = read_output(s3, bucket, op.request[OUT]['S3KeyPrefix'], op.arn)
        yield SimpleNamespace(ops=ops, start_errors=start_errors, items=full_scan(table))

# Two operations started together on one table: one runs, and the other is
# refused with ResourceInUseException. The documentation says that each start
# "can" return an ARN: then the refused operation fails at once and writes no
# output. Otherwise its start fails.
@pytest.mark.xfail(reason=NOT_IMPLEMENTED)
def test_concurrent_starts(concurrent_starts):
    for error in concurrent_starts.start_errors:
        expect_error(error, 'ResourceInUseException', 'ongoing bulk operations')
    by_status = {}
    for op in concurrent_starts.ops:
        by_status.setdefault(op.samples[-1]['BulkOperationStatus'], []).append(op)
    [completed] = by_status.pop('COMPLETED')
    failed = by_status.pop('FAILED', [])
    assert by_status == {} and len(failed) + len(concurrent_starts.start_errors) == 1
    for op in failed:
        assert op.samples[-1]['FailureCode'] == 'ResourceInUseException'
        assert 'another bulk operation is in progress' in op.samples[-1]['FailureMessage']
        assert op.output.keys == []
    assert completed.output.manifest
    assert concurrent_starts.items == [{'p': 'small', 'c': Decimal(1)}]

# Once an operation ended, its table accepts a new one. The new operation
# names a missing bucket, so it fails within seconds for that reason, not
# with ResourceInUseException. Its ClientToken differs from the old one only
# in case: a token is case-sensitive, so it is a new token.
@pytest.mark.xfail(reason=NOT_IMPLEMENTED)
@pytest.mark.parametrize('name', ['empty_prefix', 'missing_bucket', 'cancel_early'])
def test_table_free_after_end(request, dynamodb, name):
    client = dynamodb.meta.client
    op = request.getfixturevalue(f'{name}_op')
    assert op.final['BulkOperationStatus'] in TERMINAL_STATUSES
    new_request = changed(op.request, ('ClientToken',), op.start['ClientToken'].swapcase())
    new_request[IN]['S3Bucket'] = unique_bucket_name()
    arn = start_in_quota(client, new_request)['BulkOperationArn']
    assert arn != op.arn
    samples = []
    wait_for_operation(client, arn, samples)
    assert samples[-1]['FailureCode'] == 'S3NoSuchBucket'

VECTOR_TABLE_SCHEMA = {
    'KeySchema': [{'AttributeName': 'p', 'KeyType': 'HASH'}],
    'AttributeDefinitions': [{'AttributeName': 'p', 'AttributeType': 'S'}],
    'VectorIndexes': [{'IndexName': 'vind', 'VectorAttribute': {'AttributeName': 'v'},
                       'Dimensions': 3, 'DistanceFunction': 'COSINE',
                       'Projection': {'ProjectionType': 'KEYS_ONLY'}}],
}
VECTOR_ITEM = {'p': 'vector', 'v': [Decimal(1), Decimal(0), Decimal(0)]}

@pytest.fixture(scope='module')
def vector_op(dynamodb, s3, bucket):
    prefix = upload_lines(s3, bucket, dynamodb_json_lines([VECTOR_ITEM]), context='vector')
    table = create_test_table(dynamodb, **VECTOR_TABLE_SCHEMA)
    with delete_table_afterwards(dynamodb.meta.client, table.name):
        op = run_operation(table, s3, bucket, {
            'TableName': table.name,
            IN: {'S3Bucket': bucket, 'S3KeyPrefix': prefix, 'InputFormat': 'DYNAMODB_JSON'},
            OUT: {'S3Bucket': bucket, 'S3KeyPrefix': 'bulk-output/vector',
                  'OutputType': ['ERROR', 'CONDITION_CHECK_FAILURE']}})
        op.items = full_scan(table)
        yield op

# A table with a vector index: the Accelerated tier is refused, the Standard
# tier works, and the vector index bytes are metered apart.
@pytest.mark.xfail(reason=NOT_IMPLEMENTED)
def test_vector_index(vector_op, dynamodb):
    with pytest.raises(ClientError, match='ValidationException.*PerformanceTier is not supported '
                                          'for tables with vector indexes'):
        dynamodb.meta.client.start_bulk_put_items(**{**vector_op.request, 'PerformanceTier': 'ACCELERATED'})
    assert vector_op.final['BulkOperationStatus'] == 'COMPLETED'
    assert multiset(vector_op.items) == multiset([VECTOR_ITEM])
    assert usage_of(vector_op.final) == {
        'TableName': vector_op.table_name, 'BulkPutBytes': KB, 'Table': {'BulkPutBytes': KB},
        'VectorIndexes': {'vind': {'BulkPutVectorBytes': KB}}}

# A table which is being created is not found: ResourceNotFoundException
# also means a table which is not ACTIVE.
@pytest.mark.xfail(reason=NOT_IMPLEMENTED)
def test_table_state_creating(dynamodb):
    client = dynamodb.meta.client
    name = unique_table_name()
    client.create_table(TableName=name, BillingMode='PAY_PER_REQUEST', **KEY_SCHEMA)
    with delete_table_afterwards(client, name):
        with pytest.raises(ClientError, match='ResourceNotFoundException.*not found'):
            client.start_bulk_put_items(**valid_request(name))

LATE_INDEX = {'IndexName': 'late', 'KeySchema': [{'AttributeName': 'x', 'KeyType': 'HASH'}],
              'Projection': {'ProjectionType': 'KEYS_ONLY'}}

# Delete the index LATE_INDEX while it is built. DynamoDB can delete an
# index during its backfill, but refuses before the backfill starts. Waiting
# for the backfill to end would take many minutes.
def delete_late_index(client, table):
    deadline = time.time() + (600 if is_aws(client) else 60)
    while True:
        try:
            return client.update_table(TableName=table, GlobalSecondaryIndexUpdates=[
                {'Delete': {'IndexName': LATE_INDEX['IndexName']}}])
        except ClientError as e:
            if e.response['Error']['Code'] != 'ResourceInUseException' or time.time() > deadline:
                raise
            time.sleep(5 if is_aws(client) else 0.1)

# A table which builds an index is busy.
@pytest.mark.xfail(reason=NOT_IMPLEMENTED)
def test_table_state_index_building(dynamodb):
    client = dynamodb.meta.client
    table = create_test_table(dynamodb, **KEY_SCHEMA)
    with delete_table_afterwards(client, table.name):
        client.update_table(TableName=table.name,
                            AttributeDefinitions=KEY_SCHEMA['AttributeDefinitions'] + [string_attribute('x')],
                            GlobalSecondaryIndexUpdates=[{'Create': LATE_INDEX}])
        try:
            with pytest.raises(ClientError, match='ResourceInUseException.*ongoing index operations'):
                client.start_bulk_put_items(**valid_request(table.name))
        finally:
            delete_late_index(client, table.name)


###############################################################################
# ClientToken.

# A request repeated with the same ClientToken returns the same operation,
# also when it has ended - even when it failed.
@pytest.mark.xfail(reason=NOT_IMPLEMENTED)
@pytest.mark.parametrize('name', ['writes', 'missing_bucket', 'cancel_early'])
def test_client_token_replay(request, dynamodb, name):
    op = request.getfixturevalue(f'{name}_op')
    replayed = dynamodb.meta.client.start_bulk_put_items(**op.request)['BulkOperationDescription']
    assert replayed['BulkOperationArn'] == op.arn
    assert replayed['BulkOperationStatus'] == op.final['BulkOperationStatus']
    for member in IMMUTABLE_MEMBERS:
        assert replayed[member] == op.start[member], member
    # The token check compares the requests with their defaults filled in.
    if name == 'writes':
        explicit = changed(changed(op.request, ('PerformanceTier',), 'STANDARD'),
                           (IN, 'InputCompressionType'), 'NONE')
        replayed = dynamodb.meta.client.start_bulk_put_items(**explicit)['BulkOperationDescription']
        assert replayed['BulkOperationArn'] == op.arn

# The same ClientToken with a changed parameter is refused.
@pytest.mark.xfail(reason=NOT_IMPLEMENTED)
@pytest.mark.parametrize('path,value', [
    pytest.param(('PerformanceTier',), 'ACCELERATED', id='tier'),
    pytest.param((IN, 'S3KeyPrefix'), 'other/', id='input_prefix'),
    pytest.param((OUT, 'OutputType'), ['ERROR'], id='output_type'),
    pytest.param(('MaxWriteRequestUnits',), 2048, id='max_write_request_units'),
    pytest.param(('TableName',), 'table', id='table'),
])
def test_client_token_mismatch(writes_op, empty_prefix_op, dynamodb, path, value):
    if path == ('TableName',):
        value = empty_prefix_op.table_name
    request = changed(writes_op.request, path, value)
    with pytest.raises(ClientError, match='IdempotentParameterMismatchException'):
        dynamodb.meta.client.start_bulk_put_items(**request)


###############################################################################
# ListBulkOperations.

SUMMARY_MEMBERS = {'BulkOperationArn', 'BulkOperationStatus', 'StartTime', 'EndTime'}

# The operation of every scenario, by scenario name. The list tests need all
# of them.
@pytest.fixture(scope='module')
def all_ops(request):
    return {name: request.getfixturevalue(f'{name}_op') for name in SCENARIOS}

# The summaries match the descriptions. A running operation is listed,
# without EndTime: cancel_midway lists the operations while it writes.
@pytest.mark.xfail(reason=NOT_IMPLEMENTED)
def test_list_contents(all_ops, dynamodb):
    listed = list_through(dynamodb.meta.client, [op.arn for op in all_ops.values()], MaxResults=25)
    by_arn = {s['BulkOperationArn']: s for s in listed}
    for op in all_ops.values():
        summary = by_arn[op.arn]
        assert set(summary) == SUMMARY_MEMBERS
        assert summary == {member: op.final[member] for member in SUMMARY_MEMBERS}
    midway = all_ops['cancel_midway']
    [running] = [s for s in midway.actions['listed'] if s['BulkOperationArn'] == midway.arn]
    assert running['BulkOperationStatus'] == 'IN_PROGRESS'
    assert set(running) == SUMMARY_MEMBERS - {'EndTime'}

# The newest operations come first. The account's other operations may
# appear among ours.
@pytest.mark.xfail(reason=NOT_IMPLEMENTED)
def test_list_order(all_ops, dynamodb):
    arns = {op.arn for op in all_ops.values()}
    listed = list_through(dynamodb.meta.client, arns, MaxResults=25)
    start_times = [s['StartTime'] for s in listed]
    assert start_times == sorted(start_times, reverse=True)
    assert arns <= {s['BulkOperationArn'] for s in listed}

# Paging returns every operation once. Without MaxResults, a page has 10
# summaries: the module started more than 10 operations.
@pytest.mark.xfail(reason=NOT_IMPLEMENTED)
def test_list_paging(all_ops, dynamodb):
    client = dynamodb.meta.client
    arns = {op.arn for op in all_ops.values()}
    assert len(arns) > 10
    response = client.list_bulk_operations()
    assert len(response['BulkOperationsSummaryList']) == 10
    assert 'NextToken' in response
    response = client.list_bulk_operations(MaxResults=1)
    assert len(response['BulkOperationsSummaryList']) == 1
    assert re.fullmatch('([0-9a-f]{16})+', response['NextToken'])
    paged = [s['BulkOperationArn'] for s in list_through(client, arns, MaxResults=4)]
    assert len(paged) == len(set(paged)), 'an operation was returned on two pages'
    assert arns <= set(paged)
    # A NextToken means that more operations follow, so no page after the
    # first one is empty. This reads the whole listing of the account, with
    # the default page size, so that it also has several pages on Alternator.
    pages = list(client.get_paginator('list_bulk_operations').paginate())
    assert all(page['BulkOperationsSummaryList'] for page in pages[1:])
