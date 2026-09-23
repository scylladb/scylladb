# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# Tests for the DynamoDB import from S3 API: ImportTable, DescribeImport and
# ListImports. They all pass on AWS. Where DynamoDB contradicts its
# documentation, the test's comment says so:
#   https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_ImportTable.html
#   https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DescribeImport.html
#   https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_ListImports.html

import base64
import gzip
import json
import time
import uuid
from compression import zstd
from contextlib import contextmanager
from decimal import Decimal

import boto3
import pytest
from boto3.dynamodb.types import Binary, TypeSerializer
from botocore.exceptions import ClientError

from test.alternator.util import (ManualRequestError, client_no_transform,
                                  full_scan, get_table_arn, is_aws,
                                  manual_request, multiset, random_string,
                                  unique_table_name)


COMPRESSORS = {
    'GZIP': (gzip.compress, '.gz'),
    'ZSTD': (zstd.compress, '.zst'),
}

GROUND_TRUTH_SCHEMA = {
    'KeySchema': [{'AttributeName': 'p', 'KeyType': 'HASH'},
                  {'AttributeName': 'c', 'KeyType': 'RANGE'}],
    'AttributeDefinitions': [{'AttributeName': 'p', 'AttributeType': 'S'},
                             {'AttributeName': 'c', 'AttributeType': 'N'}],
}


# Bucket names allow neither uppercase letters nor underscores, so
# unique_table_name() cannot be used.
def unique_bucket_name():
    return f"alternator-import-test-{uuid.uuid4().hex[:12]}"

# An S3 client in the "dynamodb" fixture's region - DynamoDB refuses buckets
# in other regions. Alternator has no S3 support yet.
def make_s3_client(dynamodb):
    if is_aws(dynamodb):
        return boto3.client('s3', region_name=dynamodb.meta.client.meta.region_name)
    raise NotImplementedError(
        'Alternator cannot read or write S3 yet, so there is no S3 endpoint '
        'to point a client at')

# Create an S3 bucket and delete it, with its contents, on exit. An import
# only reads the bucket, so nothing writes to it behind the cleanup.
@contextmanager
def new_s3_bucket(s3_client):
    bucket_name = unique_bucket_name()
    region = s3_client.meta.region_name
    kwargs = {'Bucket': bucket_name}
    # us-east-1 refuses a LocationConstraint; every other region needs one.
    if region and region != 'us-east-1':
        kwargs['CreateBucketConfiguration'] = {'LocationConstraint': region}
    s3_client.create_bucket(**kwargs)
    try:
        yield bucket_name
    finally:
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket_name):
            if 'Contents' in page:
                s3_client.delete_objects(
                    Bucket=bucket_name,
                    Delete={'Objects': [{'Key': obj['Key']} for obj in page['Contents']]})
        s3_client.delete_bucket(Bucket=bucket_name)

# A fresh S3 key prefix. `context` says what the objects under it are for.
def unique_prefix(context):
    return f'alternator-import-test/{context}-{random_string(16).lower()}/'

# Upload `lines`, newline-terminated, as one S3 object; return its prefix.
# The key is prefix + name + extension, plus '.gz'/'.zst' if `compression` is
# set. An import reads every object under a prefix. Pass `context` to start a
# new prefix (object '00000' unless `name` is given), or an existing `prefix`
# and a new `name` to add an object. Objects are never deleted one by one:
# each test's new_s3_bucket() deletes the bucket with its contents.
def upload_lines(s3, bucket, lines, *, context=None, compression=None,
                 prefix=None, name=None, extension='.json'):
    assert (context is None) != (prefix is None), \
        'pass context= to start a new import source, or prefix= to add to one'
    if prefix is None:
        prefix = unique_prefix(context)
        name = name or '00000'
    else:
        assert name, 'adding an object to an existing prefix needs its own name'
    body = ('\n'.join(lines) + '\n').encode()
    suffix = ''
    if compression is not None:
        compressor, suffix = COMPRESSORS[compression]
        body = compressor(body)
    s3.put_object(Bucket=bucket, Key=f'{prefix}{name}{extension}{suffix}', Body=body)
    return prefix

GROUND_TRUTH_ITEMS = [
    # The minimal item: nothing but the key.
    {'p': 'minimal', 'c': Decimal(1)},
    # Strings needing JSON escaping, and the empty one.
    {'p': 'strings', 'c': Decimal(2), 'ascii': 'hello', 'unicode': 'zażółć 日本語 🦑',
     'escapes': 'quote" backslash\\ newline\n tab\t', 'empty': '', 'long': 'x' * 1000},
    # 38 digits, but not the documented E+125 maximum: DynamoDB returns that
    # expanded to 129 digits, which boto3 cannot deserialize.
    {'p': 'numbers', 'c': Decimal(3), 'zero': Decimal(0), 'neg': Decimal('-17.5'),
     'digits38': Decimal('1' * 38), 'big': Decimal('9.99999E+37'),
     'small': Decimal('1E-130'), 'frac': Decimal('0.000001')},
    # Binary: empty, and bytes that are not UTF-8.
    {'p': 'binary', 'c': Decimal(4), 'bytes': Binary(b'\x00\x01\xfe\xff'),
     'empty': Binary(b''), 'text': Binary(b'not really binary')},
    # The scalar types that are not S/N/B.
    {'p': 'scalars', 'c': Decimal(5), 'yes': True, 'no': False, 'nothing': None},
    # The three set types.
    {'p': 'sets', 'c': Decimal(6), 'ss': {'a', 'b', 'c'},
     'ns': {Decimal(1), Decimal(-2), Decimal('3.5')},
     'bs': {Binary(b'\x00'), Binary(b'\x01')}},
    # Documents: mixed list and map, both also empty.
    {'p': 'documents', 'c': Decimal(7),
     'list': ['s', Decimal(1), True, None, Binary(b'b'), [], {}],
     'map': {'a': Decimal(1), 'b': {'c': 'd'}}, 'empty_list': [], 'empty_map': {}},
    # Deep nesting.
    {'p': 'nested', 'c': Decimal(8), 'deep': {"0": {"1": {"2": "leaf"}}}},
    # Awkward attribute names: a reserved word, a dotted one, one differing from
    # the key only in case, and a long one.
    {'p': 'names', 'c': Decimal(9), 'Size': 'reserved word', 'a.b': 'dotted',
     'P': 'not the key', 'n' * 255: 'long name'},
    # Two items in one partition.
    {'p': 'shared', 'c': Decimal(10), 'which': 'first'},
    {'p': 'shared', 'c': Decimal(11), 'which': 'second'},
    # Close to the 400 KB item limit.
    {'p': 'big', 'c': Decimal(12), 'blob': 'y' * 380000},
]

DUPLICATE_ITEMS = [
    {'p': 'dup', 'c': Decimal(1), 'which': 'first'},
    {'p': 'dup', 'c': Decimal(2), 'which': 'first'},
    {'p': 'unique', 'c': Decimal(3), 'which': 'only'},
    {'p': 'dup', 'c': Decimal(1), 'which': 'second'},
    {'p': 'dup', 'c': Decimal(2), 'which': 'second'},
]

# The keys left once duplicates overwrite each other.
DISTINCT_KEYS = {(item['p'], item['c']) for item in DUPLICATE_ITEMS}

# Two items which must survive an import that also contains invalid lines.
VALID_ITEMS = [{'p': 'good', 'c': Decimal(1), 'v': 'kept'},
               {'p': 'good', 'c': Decimal(2), 'v': 'kept too'}]

INVALID_LINES = {
    'no Item member': '{"NotAnItem": {"p": {"S": "x"}, "c": {"N": "1"}}}',
    'missing partition key': '{"Item": {"c": {"N": "1"}}}',
    'missing sort key': '{"Item": {"p": {"S": "x"}}}',
    'sort key of the wrong type': '{"Item": {"p": {"S": "x"}, "c": {"S": "1"}}}',
    'not JSON at all': '{"Item": ',
    'empty attribute name': '{"Item": {"p": {"S": "x"}, "c": {"N": "1"}, "": {"S": "v"}}}',
}

# DynamoDB skips blank lines, counting them neither as items nor as errors.
IGNORED_LINES = ['', '   ']

# TypeSerializer returns binary as bytes; DYNAMODB_JSON wants base64 strings.
def _encode_value(av):
    [(tag, value)] = av.items()
    if tag == 'B':
        return {'B': base64.b64encode(bytes(value)).decode()}
    if tag == 'BS':
        return {'BS': [base64.b64encode(bytes(b)).decode() for b in value]}
    if tag == 'L':
        return {'L': [_encode_value(v) for v in value]}
    if tag == 'M':
        return {'M': {k: _encode_value(v) for k, v in value.items()}}
    return av

# The items as DYNAMODB_JSON lines.
def dynamodb_json_lines(items) -> list[str]:
    serializer = TypeSerializer()
    return [json.dumps({'Item': {name: _encode_value(serializer.serialize(value))
                                 for name, value in item.items()}})
            for item in items]

# Uncompressed size in bytes of the object upload_lines() writes for
# GROUND_TRUTH_ITEMS.
GROUND_TRUTH_BYTES = sum(len(line.encode()) + 1
                         for line in dynamodb_json_lines(GROUND_TRUTH_ITEMS))

# Build an ImportTable request. An override replaces its member, except that a
# dict is merged field by field. None deletes the member or field.
def import_kwargs(bucket, prefix, table_name=None, **overrides):
    kwargs: dict = {
        'S3BucketSource': {'S3Bucket': bucket, 'S3KeyPrefix': prefix},
        'InputFormat': 'DYNAMODB_JSON',
        'InputCompressionType': 'NONE',
        'TableCreationParameters': {'TableName': table_name or unique_table_name(),
                                    'BillingMode': 'PAY_PER_REQUEST', **GROUND_TRUTH_SCHEMA},
    }
    for member, override in overrides.items():
        if override is None:
            kwargs.pop(member, None)
        elif isinstance(override, dict) and isinstance(kwargs.get(member), dict):
            merged = dict(kwargs[member])
            for field, value in override.items():
                if value is None:
                    merged.pop(field, None)
                else:
                    merged[field] = value
            kwargs[member] = merged
        else:
            kwargs[member] = override
    return kwargs

# A copy of `kwargs` with `member` changed. A dict value is merged in.
def copy_with_member_override(kwargs, member, value):
    changed = dict(kwargs)
    changed[member] = {**kwargs[member], **value} if isinstance(value, dict) else value
    return changed

# Seconds between DescribeImport polls.
def _poll_interval(client):
    return 5 if is_aws(client) else 0.1

TERMINAL_IMPORT_STATUSES = ['COMPLETED', 'FAILED', 'CANCELLED']

# The counters DescribeImport reports. They never decrease.
COUNTERS = ['ProcessedItemCount', 'ImportedItemCount', 'ErrorCount',
            'ProcessedSizeBytes']

# Yield DescribeImport descriptions until a terminal one, or raise TimeoutError.
def poll_import(client, import_arn, timeout=None):
    if timeout is None:
        timeout = 1800 if is_aws(client) else 60
    interval = _poll_interval(client)
    deadline = time.time() + timeout
    while True:
        description = client.describe_import(ImportArn=import_arn)['ImportTableDescription']
        yield description
        if description['ImportStatus'] in TERMINAL_IMPORT_STATUSES:
            return
        if time.time() >= deadline:
            raise TimeoutError(
                f'Import {import_arn} did not finish within {timeout}s; '
                f"last status {description['ImportStatus']}")
        time.sleep(interval)

# Return the terminal DescribeImport description.
def wait_for_import(client, import_arn, timeout=None):
    for description in poll_import(client, import_arn, timeout):
        pass
    return description

# Delete the table an import created, once the import has ended. A missing
# table is fine: a failed import may not create one.
@contextmanager
def delete_table_afterwards(client, table_name):
    try:
        yield
    finally:
        deadline = time.time() + (300 if is_aws(client) else 60)
        while True:
            try:
                client.delete_table(TableName=table_name)
                break
            except ClientError as e:
                code = e.response['Error']['Code']
                if code == 'ResourceNotFoundException':
                    break
                # DynamoDB refuses to delete a table while its import runs.
                if code == 'ResourceInUseException' and time.time() < deadline:
                    time.sleep(_poll_interval(client))
                    continue
                pytest.fail(f'Leaked table {table_name}: {e}')

# All ListImports summaries, following NextToken.
def list_all_imports(client, **kwargs):
    summaries, token = [], None
    while True:
        response = client.list_imports(**kwargs, **({'NextToken': token} if token else {}))
        summaries += response['ImportSummaryList']
        token = response.get('NextToken')
        if not token:
            return summaries

# Members ImportTable's response always has. CloudWatchLogGroupArn is left
# out: Alternator has no CloudWatch; see test_cloudwatch_log_group_arn.
INITIAL_MEMBERS = ['ImportArn', 'ImportStatus', 'TableArn', 'TableId', 'ClientToken',
                   'S3BucketSource', 'InputFormat', 'InputCompressionType',
                   'TableCreationParameters', 'StartTime', 'ProcessedItemCount',
                   'ImportedItemCount', 'ErrorCount']

# Members fixed when the import is accepted. Status and counters change.
IMMUTABLE_MEMBERS = [member for member in INITIAL_MEMBERS
                     if member != 'ImportStatus' and member not in COUNTERS]

# Members absent from ImportTable's response. The first two appear once the
# import ends; they do not start at 0 like the item counters. The other two
# appear only if the import fails.
ABSENT_UNTIL_THE_END = ['EndTime', 'ProcessedSizeBytes']
ABSENT_UNLESS_FAILED = ['FailureCode', 'FailureMessage']
ABSENT_INITIAL_MEMBERS = ABSENT_UNTIL_THE_END + ABSENT_UNLESS_FAILED

# One import end to end: ImportTable's response, the final DescribeImport
# description, and the created table. TableId is allocated up front.
@pytest.mark.xfail(reason="SCYLLADB-1369")
def test_import_completes(dynamodb):
    client = dynamodb.meta.client
    s3 = make_s3_client(dynamodb)
    with new_s3_bucket(s3) as bucket:
        prefix = upload_lines(s3, bucket, dynamodb_json_lines(GROUND_TRUTH_ITEMS),
                              context='ground-truth')
        # An empty object is accepted and changes no counter, not even
        # ProcessedSizeBytes, which must equal GROUND_TRUTH_BYTES exactly.
        s3.put_object(Bucket=bucket, Key=f'{prefix}empty.json', Body=b'')
        table_name = unique_table_name()
        kwargs = import_kwargs(bucket, prefix, table_name)
        # Sanity check: the ClientToken below is the one botocore generates.
        assert 'ClientToken' not in kwargs
        initial = client.import_table(**kwargs)['ImportTableDescription']
        with delete_table_afterwards(client, table_name):
            # ImportTable's response
            for member in INITIAL_MEMBERS:
                assert member in initial
            for member in ABSENT_INITIAL_MEMBERS:
                assert member not in initial
            assert initial['ImportStatus'] == 'IN_PROGRESS'
            assert all(initial[counter] == 0 for counter in
                       ['ProcessedItemCount', 'ImportedItemCount', 'ErrorCount'])
            # An import ARN is its table's ARN with an import id appended.
            table_arn, separator, import_id = initial['ImportArn'].rpartition('/import/')
            assert separator and import_id
            assert table_arn == initial['TableArn']
            assert initial['ClientToken']

            # The final description
            final = wait_for_import(client, initial['ImportArn'])
            assert final['ImportStatus'] == 'COMPLETED'
            for member in ABSENT_UNTIL_THE_END:
                assert member in final
            for member in ABSENT_UNLESS_FAILED:
                assert member not in final
            assert final['EndTime'] >= final['StartTime']
            assert final['ProcessedSizeBytes'] == GROUND_TRUTH_BYTES
            assert final['ErrorCount'] == 0
            assert final['ProcessedItemCount'] == final['ImportedItemCount'] == len(GROUND_TRUTH_ITEMS)
            for member in IMMUTABLE_MEMBERS:
                assert final[member] == initial[member], member

            # The created table
            assert multiset(GROUND_TRUTH_ITEMS) == multiset(full_scan(dynamodb.Table(table_name)))
            described = client.describe_table(TableName=table_name)['Table']
            assert described['TableStatus'] == 'ACTIVE'
            assert described['TableId'] == initial['TableId']

# What is only visible while an import runs: DescribeTable, ListImports and
# the counters.
@pytest.mark.xfail(reason="SCYLLADB-1369")
def test_import_in_flight(dynamodb):
    client = dynamodb.meta.client
    s3 = make_s3_client(dynamodb)
    with new_s3_bucket(s3) as bucket:
        prefix = upload_lines(s3, bucket, dynamodb_json_lines(GROUND_TRUTH_ITEMS),
                              context='ground-truth')
        table_name = unique_table_name()
        initial = client.import_table(
            **import_kwargs(bucket, prefix, table_name))['ImportTableDescription']
        arn = initial['ImportArn']
        with delete_table_afterwards(client, table_name):
            first = client.describe_import(ImportArn=arn)['ImportTableDescription']
            samples = [first]
            # A small import may end before the first poll. Then there is
            # nothing in flight to check, only the final description.
            if first['ImportStatus'] not in TERMINAL_IMPORT_STATUSES:
                assert 'EndTime' not in first
                # ImportTable returns before the table exists, so DescribeTable
                # may land before creation (ResourceNotFoundException) or after
                # (CREATING). Neither may be ACTIVE while the import runs.
                try:
                    described = client.describe_table(TableName=table_name)['Table']
                except ClientError as e:
                    assert e.response['Error']['Code'] == 'ResourceNotFoundException'
                else:
                    assert described['TableStatus'] == 'CREATING'
                # Running imports are listed, although the documentation says
                # ListImports lists "completed imports within the past 90 days".
                listed = [s for s in list_all_imports(client, TableArn=initial['TableArn'])
                          if s['ImportArn'] == arn]
                assert listed, 'a running import was not listed'
                samples += poll_import(client, arn)
            last_sample = samples[-1]
            in_progress = samples[:-1]
            assert last_sample['ImportStatus'] == 'COMPLETED'
            assert last_sample['ProcessedItemCount'] == len(GROUND_TRUTH_ITEMS)
            assert last_sample['ImportedItemCount'] == len(GROUND_TRUTH_ITEMS)
            assert last_sample['ErrorCount'] == 0
            assert last_sample['ProcessedSizeBytes'] == GROUND_TRUTH_BYTES
            if is_aws(dynamodb):
                # DynamoDB reports 0 while the import runs (even for ten
                # minutes) and sets the totals only when it ends. The
                # documentation does not say so.
                for sample in in_progress:
                    assert sample['ProcessedItemCount'] == 0
                    assert sample['ImportedItemCount'] == 0
                    assert sample['ErrorCount'] == 0
            else:
                # Alternator deliberately reports live counters.
                for member in COUNTERS:
                    seen = [sample.get(member, 0) for sample in samples]
                    assert seen == sorted(seen), f'{member} went backwards: {seen}'


# Table shapes for the replays below. ImportTable does not support LSIs.
OTHER_SCHEMA = {
    'AttributeDefinitions': [{'AttributeName': 'other', 'AttributeType': 'S'},
                             {'AttributeName': 'c', 'AttributeType': 'N'}],
    'KeySchema': [{'AttributeName': 'other', 'KeyType': 'HASH'},
                  {'AttributeName': 'c', 'KeyType': 'RANGE'}],
}
A_GSI = [{'IndexName': 'replayed-gsi',
          'KeySchema': [{'AttributeName': 'c', 'KeyType': 'HASH'}],
          'Projection': {'ProjectionType': 'ALL'}}]
A_VECTOR_INDEX = [{'IndexName': 'replayed-vector',
                   'VectorAttribute': {'AttributeName': 'v'},
                   'Projection': {'ProjectionType': 'ALL'},
                   'Dimensions': 4,
                   'DistanceFunction': 'COSINE'}]

# Members a request replayed with the same ClientToken may change. DynamoDB
# returns the original import and silently ignores the change, even to the key
# schema or indexes. Only a changed table name conflicts.
TOKEN_REPLAY_IGNORED_MEMBERS = [
    ('compression', 'InputCompressionType', 'GZIP'),
    ('format', 'InputFormat', 'CSV'),
    ('prefix', 'S3BucketSource', {'S3KeyPrefix': 'some/other/prefix/'}),
    ('bucket', 'S3BucketSource', {'S3Bucket': 'some-other-bucket-name'}),
    # AttributeDefinitions must cover KeySchema, so both change together;
    # changing one alone fails validation before the token is checked.
    ('key schema and attribute definitions', 'TableCreationParameters',
     OTHER_SCHEMA),
    ('billing mode', 'TableCreationParameters', {'BillingMode': 'PROVISIONED',
     'ProvisionedThroughput': {'ReadCapacityUnits': 1, 'WriteCapacityUnits': 1}}),
    ('global secondary index', 'TableCreationParameters',
     {'GlobalSecondaryIndexes': A_GSI}),
    ('vector index', 'TableCreationParameters',
     {'VectorIndexes': A_VECTOR_INDEX}),
]

# The ClientToken duplicate check. On DynamoDB it is only observable while
# the import runs; see replay() below.
@pytest.mark.xfail(reason="SCYLLADB-1369")
def test_client_token_reuse(dynamodb):
    client = dynamodb.meta.client
    token = random_string(20)
    s3 = make_s3_client(dynamodb)
    with new_s3_bucket(s3) as bucket:
        prefix = upload_lines(s3, bucket, dynamodb_json_lines(VALID_ITEMS[:1]), context='tiny')
        table_name = unique_table_name()
        kwargs = import_kwargs(bucket, prefix, table_name, ClientToken=token)
        initial = client.import_table(**kwargs)['ImportTableDescription']
        # Replay `request`. A ClientToken is documented to stay valid for
        # 8 hours, so a replay is the same import even after it ends.
        # DynamoDB checks for an existing table first, so once the import
        # ends it raises ResourceInUseException and the replays cannot be
        # checked: return None then. Alternator imports into existing
        # tables, so it never raises this and checks every replay.
        def replay(request):
            try:
                return client.import_table(**request)['ImportTableDescription']
            except ClientError as e:
                if e.response['Error']['Code'] == 'ResourceInUseException':
                    return None
                raise
        with delete_table_afterwards(client, table_name):
            assert initial['ClientToken'] == token
            # An identical request returns the same import. Its status and
            # counters may have changed in between.
            repeated = replay(kwargs)
            if repeated is None:
                return
            for member in IMMUTABLE_MEMBERS:
                assert repeated[member] == initial[member], member
            for label, member, value in TOKEN_REPLAY_IGNORED_MEMBERS:
                replayed = replay(copy_with_member_override(kwargs, member, value))
                if replayed is None:
                    return
                assert replayed['ImportArn'] == initial['ImportArn'], label
                assert replayed[member] == initial[member], label
            # A different table name conflicts. The error is
            # ImportConflictException, not the documented
            # IdempotentParameterMismatch.
            other_name = unique_table_name()
            other = copy_with_member_override(
                kwargs, 'TableCreationParameters', {'TableName': other_name})
            with pytest.raises(ClientError,
                               match='ImportConflictException.*Duplicate request'):
                # If the request is wrongly accepted, clean up the import
                # it started; pytest.raises then fails the test.
                accepted = client.import_table(**other)['ImportTableDescription']
                wait_for_import(client, accepted['ImportArn'])
                client.delete_table(TableName=other_name)
            wait_for_import(client, initial['ImportArn'])

# DynamoDB refuses to import into an existing table, whatever the token:
# botocore generates a different one per call. Alternator deliberately allows
# the import; see test_import_into_existing_table.
@pytest.mark.xfail(reason="Alternator will keep allowing this")
def test_import_into_existing_table_rejected(dynamodb):
    client = dynamodb.meta.client
    s3 = make_s3_client(dynamodb)
    with new_s3_bucket(s3) as bucket:
        prefix = upload_lines(s3, bucket, dynamodb_json_lines(VALID_ITEMS[:1]),
                              context='tiny')
        table_name = unique_table_name()
        kwargs = import_kwargs(bucket, prefix, table_name)
        initial = client.import_table(**kwargs)['ImportTableDescription']
        with delete_table_afterwards(client, table_name):
            wait_for_import(client, initial['ImportArn'])
            with pytest.raises(ClientError,
                               match='ResourceInUseException.*Table already exists'):
                client.import_table(**kwargs)

# Two imports into one table. The table must hold the items of both.
@pytest.mark.xfail(reason="SCYLLADB-1369")
def test_import_into_existing_table(dynamodb, scylla_only):
    client = dynamodb.meta.client
    s3 = make_s3_client(dynamodb)
    first_half, second_half = GROUND_TRUTH_ITEMS[:6], GROUND_TRUTH_ITEMS[6:]
    with new_s3_bucket(s3) as bucket:
        first_prefix = upload_lines(s3, bucket, dynamodb_json_lines(first_half),
                                    context='first-half')
        second_prefix = upload_lines(s3, bucket, dynamodb_json_lines(second_half),
                                     context='second-half')
        table_name = unique_table_name()
        with delete_table_afterwards(client, table_name):
            first = client.import_table(
                **import_kwargs(bucket, first_prefix, table_name))['ImportTableDescription']
            assert wait_for_import(client, first['ImportArn'])['ImportStatus'] == 'COMPLETED'
            assert multiset(first_half) == multiset(full_scan(dynamodb.Table(table_name)))
            second = client.import_table(
                **import_kwargs(bucket, second_prefix, table_name))['ImportTableDescription']
            final = wait_for_import(client, second['ImportArn'])
            assert final['ImportStatus'] == 'COMPLETED'
            assert final['ErrorCount'] == 0
            assert final['ImportedItemCount'] == len(second_half)
            assert multiset(GROUND_TRUTH_ITEMS) == multiset(full_scan(dynamodb.Table(table_name)))

# ClientToken is documented as optional but is required. botocore generates
# one when it is missing, so the request is sent without botocore's handlers.
@pytest.mark.xfail(reason="SCYLLADB-1369")
def test_client_token_required(dynamodb):
    kwargs = import_kwargs(unique_bucket_name(), unique_prefix('unused'))
    assert 'ClientToken' not in kwargs
    with client_no_transform(dynamodb.meta.client) as client:
        with pytest.raises(ClientError, match='ValidationException.*[Cc]lientToken'):
            client.import_table(**kwargs)

# ClientToken must match ^[^\$]+$. botocore checks neither the pattern nor a
# minimum length, so these reach the server.
@pytest.mark.xfail(reason="SCYLLADB-1369")
def test_client_token_invalid(dynamodb):
    for token in ['$', 'has$dollar', '$leading', 'trailing$', '']:
        kwargs = import_kwargs(unique_bucket_name(), unique_prefix('unused'),
                            ClientToken=token)
        with pytest.raises(ClientError, match='ValidationException.*[Cc]lientToken'):
            dynamodb.meta.client.import_table(**kwargs)

# A well-formed ARN of an import which does not exist.
def unknown_import_arn(table):
    return f'{get_table_arn(table)}/import/{int(time.time() * 1000):014}-0badf00d'

# An unknown import is ImportNotFoundException, not ValidationException.
@pytest.mark.xfail(reason="SCYLLADB-1369")
def test_describe_import_not_found(dynamodb, test_table_s):
    arn = unknown_import_arn(test_table_s)
    with pytest.raises(ClientError, match='ImportNotFoundException'):
        dynamodb.meta.client.describe_import(ImportArn=arn)

# An import ARN whose table name is below the 3-character minimum.
@pytest.mark.xfail(reason="SCYLLADB-1369")
def test_describe_import_malformed_arn(dynamodb, test_table_s):
    arn_up_to_table, _, _ = get_table_arn(test_table_s).rpartition('/')
    arn = f'{arn_up_to_table}/x/import/01658528578619-c4d4e311'
    with pytest.raises(ClientError, match='ValidationException.*Invalid Import ARN'):
        dynamodb.meta.client.describe_import(ImportArn=arn)

# ImportSummary members, except CloudWatchLogGroupArn.
IMPORT_SUMMARY_MEMBERS = {'ImportArn', 'ImportStatus', 'TableArn', 'S3BucketSource',
                          'InputFormat', 'StartTime', 'EndTime'}

# One finished import. Yields its final DescribeImport description.
@contextmanager
def single_finished_import(dynamodb):
    client = dynamodb.meta.client
    s3 = make_s3_client(dynamodb)
    with new_s3_bucket(s3) as bucket:
        prefix = upload_lines(s3, bucket, dynamodb_json_lines(VALID_ITEMS[:1]),
                              context='tiny')
        table_name = unique_table_name()
        with delete_table_afterwards(client, table_name):
            initial = client.import_table(
                **import_kwargs(bucket, prefix, table_name))['ImportTableDescription']
            yield wait_for_import(client, initial['ImportArn'])

# Two finished imports into two tables.
@contextmanager
def two_finished_imports(dynamodb):
    client = dynamodb.meta.client
    s3 = make_s3_client(dynamodb)
    with new_s3_bucket(s3) as bucket:
        prefix = upload_lines(s3, bucket, dynamodb_json_lines(VALID_ITEMS[:1]),
                              context='tiny')
        first_table, second_table = unique_table_name(), unique_table_name()
        with delete_table_afterwards(client, first_table), \
             delete_table_afterwards(client, second_table):
            first = client.import_table(
                **import_kwargs(bucket, prefix, first_table))['ImportTableDescription']
            second = client.import_table(
                **import_kwargs(bucket, prefix, second_table))['ImportTableDescription']
            wait_for_import(client, first['ImportArn'])
            wait_for_import(client, second['ImportArn'])
            yield first, second

# The ListImports summary of a finished import has every member.
@pytest.mark.xfail(reason="SCYLLADB-1369")
def test_list_imports_contents(dynamodb):
    client = dynamodb.meta.client
    with single_finished_import(dynamodb) as imported:
        summaries = list_all_imports(client)
        arn = imported['ImportArn']
        assert arn in {s['ImportArn'] for s in summaries}
        summary, = [s for s in summaries if s['ImportArn'] == arn]
        summary_fields = set(summary) - {'CloudWatchLogGroupArn'}
        assert summary_fields <= IMPORT_SUMMARY_MEMBERS, \
            f"Unexpected fields in summary {summary_fields - IMPORT_SUMMARY_MEMBERS}"
        assert summary_fields >= IMPORT_SUMMARY_MEMBERS, \
            f"Not enough fields in summary {IMPORT_SUMMARY_MEMBERS - summary_fields}"

# DynamoDB returns CloudWatchLogGroupArn in DescribeImport and ListImports.
# Alternator has no CloudWatch, so other tests leave the member out.
@pytest.mark.xfail(reason="Alternator has no CloudWatch")
def test_cloudwatch_log_group_arn(dynamodb):
    client = dynamodb.meta.client
    with single_finished_import(dynamodb) as imported:
        assert 'CloudWatchLogGroupArn' in imported
        summary, = [s for s in list_all_imports(client)
                    if s['ImportArn'] == imported['ImportArn']]
        assert 'CloudWatchLogGroupArn' in summary

# A table whose only key is a string partition key.
HASH_ONLY_SCHEMA = {
    'KeySchema': [{'AttributeName': 'p', 'KeyType': 'HASH'}],
    'AttributeDefinitions': [{'AttributeName': 'p', 'AttributeType': 'S'}],
}

CSV_COLUMNS = ['p', 'name', 'note']
CSV_ROWS = ['row1,alice,first', 'row2,bob,second']
# CSV values are imported as strings.
CSV_ITEMS = [{'p': 'row1', 'name': 'alice', 'note': 'first'},
             {'p': 'row2', 'name': 'bob', 'note': 'second'}]

# The header comes from the object's first line or from HeaderList.
@pytest.mark.xfail(reason="SCYLLADB-1369")
@pytest.mark.parametrize('header', ['in_the_object', 'in_the_request'])
def test_import_csv(dynamodb, header):
    client = dynamodb.meta.client
    s3 = make_s3_client(dynamodb)
    options: dict = {'Csv': {'Delimiter': ','}}
    lines = list(CSV_ROWS)
    if header == 'in_the_object':
        lines.insert(0, ','.join(CSV_COLUMNS))
    else:
        options['Csv']['HeaderList'] = CSV_COLUMNS
    with new_s3_bucket(s3) as bucket:
        prefix = upload_lines(s3, bucket, lines, context='csv', extension='.csv')
        table_name = unique_table_name()
        kwargs = import_kwargs(bucket, prefix, table_name, InputFormat='CSV',
                               InputFormatOptions=options,
                               TableCreationParameters=HASH_ONLY_SCHEMA)
        initial = client.import_table(**kwargs)['ImportTableDescription']
        with delete_table_afterwards(client, table_name):
            final = wait_for_import(client, initial['ImportArn'])
            assert final['ImportStatus'] == 'COMPLETED'
            assert final['ErrorCount'] == 0
            assert final['ImportedItemCount'] == len(CSV_ITEMS)
            assert multiset(CSV_ITEMS) == multiset(full_scan(dynamodb.Table(table_name)))

# Ion is a superset of JSON. ION items have the "Item" wrapper of DYNAMODB_JSON
# but no type descriptors.
ION_ITEMS = [{'p': 'ion1', 'c': Decimal(1), 'text': 'hello'},
             {'p': 'ion2', 'c': Decimal(2), 'text': 'world'}]
ION_LINES = ['{"Item": {"p": "ion1", "c": 1.0, "text": "hello"}}',
             '{"Item": {"p": "ion2", "c": 2.0, "text": "world"}}']

# Import of ION_LINES with InputFormat ION.
@pytest.mark.xfail(reason="SCYLLADB-1369")
def test_import_ion(dynamodb):
    client = dynamodb.meta.client
    s3 = make_s3_client(dynamodb)
    with new_s3_bucket(s3) as bucket:
        prefix = upload_lines(s3, bucket, ION_LINES, context='ion', extension='.ion')
        table_name = unique_table_name()
        initial = client.import_table(
            **import_kwargs(bucket, prefix, table_name,
                            InputFormat='ION'))['ImportTableDescription']
        with delete_table_afterwards(client, table_name):
            final = wait_for_import(client, initial['ImportArn'])
            assert final['ImportStatus'] == 'COMPLETED'
            assert final['ErrorCount'] == 0
            assert final['ImportedItemCount'] == len(ION_ITEMS)
            assert multiset(ION_ITEMS) == multiset(full_scan(dynamodb.Table(table_name)))

# The account id from arn:aws:dynamodb:<region>:<account>:table/<name>.
def aws_account_id(table):
    return get_table_arn(table).split(':')[4]

# S3BucketOwner guards against the bucket changing hands. Naming the real
# owner changes nothing.
@pytest.mark.xfail(reason="SCYLLADB-1369")
def test_import_with_s3_bucket_owner(dynamodb, test_table_s):
    client = dynamodb.meta.client
    s3 = make_s3_client(dynamodb)
    with new_s3_bucket(s3) as bucket:
        prefix = upload_lines(s3, bucket, dynamodb_json_lines(VALID_ITEMS),
                              context='owner')
        table_name = unique_table_name()
        initial = client.import_table(**import_kwargs(
            bucket, prefix, table_name,
            S3BucketSource={'S3BucketOwner': aws_account_id(test_table_s)},
        ))['ImportTableDescription']
        with delete_table_afterwards(client, table_name):
            final = wait_for_import(client, initial['ImportArn'])
            assert final['ImportStatus'] == 'COMPLETED'
            assert multiset(VALID_ITEMS) == multiset(full_scan(dynamodb.Table(table_name)))

# S3BucketOwner naming another account stops the import.
@pytest.mark.xfail(reason="SCYLLADB-1369")
def test_import_with_wrong_s3_bucket_owner(dynamodb):
    client = dynamodb.meta.client
    s3 = make_s3_client(dynamodb)
    with new_s3_bucket(s3) as bucket:
        prefix = upload_lines(s3, bucket, dynamodb_json_lines(VALID_ITEMS),
                              context='wrong-owner')
        table_name = unique_table_name()
        kwargs = import_kwargs(bucket, prefix, table_name,
                               S3BucketSource={'S3BucketOwner': '0' * 12})
        # DynamoDB accepts the request and only fails when it reads the
        # bucket, without creating the table.
        initial = client.import_table(**kwargs)['ImportTableDescription']
        with delete_table_afterwards(client, table_name):
            final = wait_for_import(client, initial['ImportArn'])
            assert final['ImportStatus'] == 'FAILED'
            assert final['FailureCode'] == 'S3AccessDenied'
            assert 'Access Denied' in final['FailureMessage']
            with pytest.raises(ClientError, match='ResourceNotFoundException'):
                client.describe_table(TableName=table_name)

# Without S3KeyPrefix the whole bucket is imported. The bucket holds only
# GROUND_TRUTH_ITEMS.
@pytest.mark.xfail(reason="SCYLLADB-1369")
def test_import_without_s3_key_prefix(dynamodb):
    client = dynamodb.meta.client
    s3 = make_s3_client(dynamodb)
    with new_s3_bucket(s3) as bucket:
        upload_lines(s3, bucket, dynamodb_json_lines(GROUND_TRUTH_ITEMS),
                     context='whole-bucket')
        table_name = unique_table_name()
        kwargs = import_kwargs(bucket, '', table_name,
                               S3BucketSource={'S3KeyPrefix': None})
        assert 'S3KeyPrefix' not in kwargs['S3BucketSource']
        initial = client.import_table(**kwargs)['ImportTableDescription']
        with delete_table_afterwards(client, table_name):
            final = wait_for_import(client, initial['ImportArn'])
            assert final['ImportStatus'] == 'COMPLETED'
            assert final['ErrorCount'] == 0
            assert final['ProcessedItemCount'] == len(GROUND_TRUTH_ITEMS)
            assert multiset(GROUND_TRUTH_ITEMS) == multiset(full_scan(dynamodb.Table(table_name)))

# BillingMode defaults to PROVISIONED, as in CreateTable, so a request without
# ProvisionedThroughput is refused.
@pytest.mark.xfail(reason="SCYLLADB-1369")
def test_import_default_billing_mode_is_provisioned(dynamodb):
    kwargs = import_kwargs(unique_bucket_name(), unique_prefix('unused'),
                           TableCreationParameters={'BillingMode': None})
    assert 'BillingMode' not in kwargs['TableCreationParameters']
    with pytest.raises(ClientError,
                       match='ValidationException.*ReadCapacityUnits and '
                             'WriteCapacityUnits must both be specified'):
        dynamodb.meta.client.import_table(**kwargs)

# The default BillingMode with ProvisionedThroughput creates a PROVISIONED
# table.
@pytest.mark.xfail(reason="SCYLLADB-1369")
def test_import_default_billing_mode_with_throughput(dynamodb):
    client = dynamodb.meta.client
    s3 = make_s3_client(dynamodb)
    with new_s3_bucket(s3) as bucket:
        prefix = upload_lines(s3, bucket, dynamodb_json_lines(VALID_ITEMS),
                              context='default-billing')
        table_name = unique_table_name()
        kwargs = import_kwargs(bucket, prefix, table_name,
                               TableCreationParameters={
                                   'BillingMode': None,
                                   'ProvisionedThroughput': {
                                       'ReadCapacityUnits': 1,
                                       'WriteCapacityUnits': 1}})
        assert 'BillingMode' not in kwargs['TableCreationParameters']
        initial = client.import_table(**kwargs)['ImportTableDescription']
        with delete_table_afterwards(client, table_name):
            final = wait_for_import(client, initial['ImportArn'])
            assert final['ImportStatus'] == 'COMPLETED'
            described = client.describe_table(TableName=table_name)['Table']
            assert described['ProvisionedThroughput']['ReadCapacityUnits'] == 1
            assert described['ProvisionedThroughput']['WriteCapacityUnits'] == 1
            assert multiset(VALID_ITEMS) == multiset(full_scan(dynamodb.Table(table_name)))

# A table with no sort key at all.
HASH_ONLY_ITEMS = [{'p': 'only-a-partition-key', 'v': 'kept'},
                   {'p': 'another', 'v': 'kept too'}]

# Import into a table with HASH_ONLY_SCHEMA.
@pytest.mark.xfail(reason="SCYLLADB-1369")
def test_import_into_hash_only_table(dynamodb):
    client = dynamodb.meta.client
    s3 = make_s3_client(dynamodb)
    with new_s3_bucket(s3) as bucket:
        prefix = upload_lines(s3, bucket, dynamodb_json_lines(HASH_ONLY_ITEMS),
                              context='hash-only')
        table_name = unique_table_name()
        initial = client.import_table(**import_kwargs(
            bucket, prefix, table_name,
            TableCreationParameters=HASH_ONLY_SCHEMA))['ImportTableDescription']
        with delete_table_afterwards(client, table_name):
            final = wait_for_import(client, initial['ImportArn'])
            assert final['ImportStatus'] == 'COMPLETED'
            assert final['ErrorCount'] == 0
            assert final['ImportedItemCount'] == len(HASH_ONLY_ITEMS)
            described = client.describe_table(TableName=table_name)['Table']
            assert described['KeySchema'] == HASH_ONLY_SCHEMA['KeySchema']
            assert multiset(HASH_ONLY_ITEMS) == multiset(full_scan(dynamodb.Table(table_name)))

# TableArn selects the imports into that table.
@pytest.mark.xfail(reason="SCYLLADB-1369")
def test_list_imports_table_arn_filter(dynamodb):
    client = dynamodb.meta.client
    with two_finished_imports(dynamodb) as (first, second):
        table_arn = first['TableArn']
        listed = {s['ImportArn'] for s in list_all_imports(client, TableArn=table_arn)}
        assert first['ImportArn'] in listed
        assert second['ImportArn'] not in listed
        assert all(arn.startswith(table_arn + '/import/') for arn in listed)

# ListImports orders by StartTime, newest first. The documentation promises no
# order. The older import has the higher ARN, which rules out ImportArn order.
@pytest.mark.xfail(reason="SCYLLADB-1369")
def test_list_imports_ordering(dynamodb):
    client = dynamodb.meta.client
    s3 = make_s3_client(dynamodb)
    base = unique_table_name()
    older_but_higher_arn, newer_but_lower_arn = f'{base}_zzz', f'{base}_aaa'
    with new_s3_bucket(s3) as bucket:
        prefix = upload_lines(s3, bucket, dynamodb_json_lines(VALID_ITEMS[:1]),
                              context='ordering')
        with delete_table_afterwards(client, older_but_higher_arn), \
             delete_table_afterwards(client, newer_but_lower_arn):
            older = client.import_table(**import_kwargs(
                bucket, prefix, older_but_higher_arn))['ImportTableDescription']
            newer = client.import_table(**import_kwargs(
                bucket, prefix, newer_but_lower_arn))['ImportTableDescription']
            assert older['ImportArn'] > newer['ImportArn'], 'the setup is wrong'
            wait_for_import(client, older['ImportArn'])
            wait_for_import(client, newer['ImportArn'])
            ours = {older['ImportArn'], newer['ImportArn']}
            listed = [s for s in list_all_imports(client) if s['ImportArn'] in ours]
            assert len(listed) == len(ours)
            assert [s['ImportArn'] for s in listed] == \
                [newer['ImportArn'], older['ImportArn']], \
                'not newest first - this is ImportArn order instead'
            start_times = [s['StartTime'] for s in listed]
            assert start_times == sorted(start_times, reverse=True)

# Paging returns every import exactly once.
@pytest.mark.xfail(reason="SCYLLADB-1369")
def test_list_imports_paging(dynamodb):
    client = dynamodb.meta.client
    with two_finished_imports(dynamodb) as (first, second):
        ours = {first['ImportArn'], second['ImportArn']}
        response = client.list_imports(PageSize=1)
        assert len(response['ImportSummaryList']) == 1
        assert 'NextToken' in response
        paged = [s['ImportArn'] for s in list_all_imports(client, PageSize=1)]
        assert len(paged) == len(set(paged)), 'an import was returned on two pages'
        # ListImports lists the whole account's imports, so others may appear.
        assert ours <= set(paged)

# PageSize must be 1 to 25. botocore checks only the minimum, so -1 and 0
# reach the server only because the "dynamodb" fixture sets
# parameter_validation=False; 26 and 1000 reach it regardless.
@pytest.mark.xfail(reason="SCYLLADB-1369")
@pytest.mark.parametrize('page_size', [-1, 0, 26, 1000])
def test_list_imports_page_size_out_of_range(dynamodb, page_size):
    with pytest.raises(ClientError, match='ValidationException.*[Pp]age[Ss]ize'):
        dynamodb.meta.client.list_imports(PageSize=page_size)

# NextToken must be 112 to 1024 characters matching ([0-9a-f]{16})+. The
# 111-character token reaches the server only because the "dynamodb" fixture
# sets parameter_validation=False: botocore checks the minimum length.
@pytest.mark.xfail(reason="SCYLLADB-1369")
@pytest.mark.parametrize('token', [
    pytest.param('0' * 111, id='one_short_of_the_minimum'),
    pytest.param('0' * 112, id='minimum_length_but_not_a_token'),
    pytest.param('0' * 1025, id='one_past_the_maximum'),
])
def test_list_imports_bad_next_token(dynamodb, token):
    with pytest.raises(ClientError, match='ValidationException.*[Nn]ext[Tt]oken'):
        dynamodb.meta.client.list_imports(NextToken=token)

# Duplicate keys are not an error. The documentation says they "overwrite
# each other in random order until one remains".
@pytest.mark.xfail(reason="SCYLLADB-1369")
def test_duplicate_items(dynamodb):
    client = dynamodb.meta.client
    s3 = make_s3_client(dynamodb)
    with new_s3_bucket(s3) as bucket:
        prefix = upload_lines(s3, bucket, dynamodb_json_lines(DUPLICATE_ITEMS),
                              context='dupes')
        table_name = unique_table_name()
        initial = client.import_table(
            **import_kwargs(bucket, prefix, table_name))['ImportTableDescription']
        with delete_table_afterwards(client, table_name):
            final = wait_for_import(client, initial['ImportArn'])
            assert final['ImportStatus'] == 'COMPLETED'
            assert final['ErrorCount'] == 0
            # Both counters count writes, not surviving rows.
            assert final['ProcessedItemCount'] == len(DUPLICATE_ITEMS)
            assert final['ImportedItemCount'] == len(DUPLICATE_ITEMS)
            items = full_scan(dynamodb.Table(table_name))
            assert {(item['p'], item['c']) for item in items} == DISTINCT_KEYS
            assert len(items) == len(DISTINCT_KEYS)
            assert all(item in DUPLICATE_ITEMS for item in items)

EXPECTED_BAD_COUNTERS = {
    'ProcessedItemCount': len(VALID_ITEMS) + len(INVALID_LINES),
    'ImportedItemCount': len(VALID_ITEMS),
    'ErrorCount': len(INVALID_LINES),
}

# An import with an invalid item fails, but keeps the table with the valid
# items.
@pytest.mark.xfail(reason="SCYLLADB-1369")
def test_invalid_items(dynamodb):
    client = dynamodb.meta.client
    s3 = make_s3_client(dynamodb)
    with new_s3_bucket(s3) as bucket:
        prefix = upload_lines(s3, bucket,
                              dynamodb_json_lines(VALID_ITEMS) + IGNORED_LINES,
                              context='bad', name='00000-good')
        # DynamoDB skips the rest of an object after its first invalid line,
        # both invalid JSON and valid JSON without the top-level "Item"
        # member. So each invalid line gets its own object.
        for i, line in enumerate(INVALID_LINES.values(), start=1):
            upload_lines(s3, bucket, [line], prefix=prefix,
                         name=f'{i:05}-bad')
        table_name = unique_table_name()
        initial = client.import_table(
            **import_kwargs(bucket, prefix, table_name))['ImportTableDescription']
        with delete_table_afterwards(client, table_name):
            final = wait_for_import(client, initial['ImportArn'])
            assert final['ImportStatus'] == 'FAILED'
            assert final['FailureCode'] == 'ItemValidationError'
            assert final['FailureMessage']
            assert {c: final.get(c) for c in EXPECTED_BAD_COUNTERS} == \
                EXPECTED_BAD_COUNTERS
            assert final['ProcessedItemCount'] == final['ImportedItemCount'] + final['ErrorCount']
            assert multiset(VALID_ITEMS) == multiset(full_scan(dynamodb.Table(table_name)))

# ProcessedSizeBytes counts uncompressed bytes, for every codec.
@pytest.mark.xfail(reason="SCYLLADB-1369")
@pytest.mark.parametrize('compression', [
    'GZIP',
    pytest.param('ZSTD', marks=pytest.mark.xfail(
        reason="ZSTD is not in MVP")),
])
def test_processed_size_bytes_is_uncompressed(dynamodb, compression):
    client = dynamodb.meta.client
    lines = dynamodb_json_lines(GROUND_TRUTH_ITEMS)
    s3 = make_s3_client(dynamodb)
    with new_s3_bucket(s3) as bucket:
        plain_prefix = upload_lines(s3, bucket, lines, context='plain')
        packed_prefix = upload_lines(s3, bucket, lines, context=compression.lower(),
                                     compression=compression)
        plain_table, packed_table = unique_table_name(), unique_table_name()
        # An absent InputCompressionType means NONE.
        plain_kwargs = import_kwargs(bucket, plain_prefix, plain_table,
                                     InputCompressionType=None)
        assert 'InputCompressionType' not in plain_kwargs
        packed_kwargs = import_kwargs(bucket, packed_prefix, packed_table,
                                      InputCompressionType=compression)
        with delete_table_afterwards(client, plain_table), \
             delete_table_afterwards(client, packed_table):
            plain = client.import_table(**plain_kwargs)['ImportTableDescription']
            packed = client.import_table(**packed_kwargs)['ImportTableDescription']
            plain_final = wait_for_import(client, plain['ImportArn'])
            packed_final = wait_for_import(client, packed['ImportArn'])
            assert plain_final['ImportStatus'] == 'COMPLETED'
            assert plain_final['ErrorCount'] == 0
            assert plain_final['ProcessedItemCount'] == plain_final['ImportedItemCount'] == len(GROUND_TRUTH_ITEMS)
            assert plain_final['ProcessedSizeBytes'] == GROUND_TRUTH_BYTES
            assert packed_final['ImportStatus'] == 'COMPLETED'
            assert packed_final['ProcessedItemCount'] == packed_final['ImportedItemCount'] == len(GROUND_TRUTH_ITEMS)
            assert packed_final['ProcessedSizeBytes'] == GROUND_TRUTH_BYTES
            assert packed_final['ProcessedSizeBytes'] == plain_final['ProcessedSizeBytes']
            assert multiset(GROUND_TRUTH_ITEMS) == multiset(full_scan(dynamodb.Table(plain_table)))
            assert multiset(GROUND_TRUTH_ITEMS) == multiset(full_scan(dynamodb.Table(packed_table)))

# DynamoDB refuses CsvOptions with DYNAMODB_JSON. The documentation does not
# say so.
@pytest.mark.xfail(reason="SCYLLADB-1369")
def test_csv_options_with_dynamodb_json_rejected(dynamodb):
    kwargs = import_kwargs(unique_bucket_name(), unique_prefix('unused'),
                           InputFormatOptions={'Csv': {'Delimiter': ','}})
    with pytest.raises(ClientError, match='ValidationException.*[Ii]nputFormatOptions'):
        dynamodb.meta.client.import_table(**kwargs)

# Invalid parameters. The error must name the parameter.
@pytest.mark.xfail(reason="SCYLLADB-1369")
@pytest.mark.parametrize('overrides,parameter', [
    pytest.param({'InputFormat': 'NOT_A_FORMAT'}, '[Ii]nputFormat', id='bad_format'),
    pytest.param({'InputCompressionType': 'BZIP2'}, '[Ii]nputCompressionType', id='bad_compression'),
    pytest.param({'S3BucketSource': {'S3Bucket': 'x' * 256}}, '[Ss]3Bucket', id='bucket_too_long'),
    pytest.param({'S3BucketSource': {'S3Bucket': '-nope!'}}, '[Ss]3Bucket', id='bucket_bad_chars'),
    pytest.param({'S3BucketSource': {'S3KeyPrefix': 'p' * 1025}}, '[Ss]3KeyPrefix', id='prefix_too_long'),
    pytest.param({'TableCreationParameters': {'TableName': 'n' * 256}}, '[Tt]ableName', id='name_too_long'),
    pytest.param({'TableCreationParameters': {'TableName': 'not a table name!'}},
                 '[Tt]ableName', id='name_bad_chars'),
    pytest.param({'TableCreationParameters': {'AttributeDefinitions': [
        {'AttributeName': 'unrelated', 'AttributeType': 'S'}]}},
        '[Aa]ttributeDefinitions|[Kk]eySchema', id='attrs_do_not_cover_keys'),
])
def test_import_table_validation(dynamodb, overrides, parameter):
    kwargs = import_kwargs(unique_bucket_name(), unique_prefix('unused'), **overrides)
    with pytest.raises(ClientError, match=f'ValidationException.*({parameter})'):
        dynamodb.meta.client.import_table(**kwargs)

# DynamoDB refuses WarmThroughput on a GSI, though ImportTable's documented
# request syntax lists it.
@pytest.mark.xfail(reason="SCYLLADB-1369")
def test_gsi_warm_throughput_rejected(dynamodb):
    gsi = [{**A_GSI[0], 'WarmThroughput': {'ReadUnitsPerSecond': 12000,
                                           'WriteUnitsPerSecond': 4000}}]
    kwargs = import_kwargs(unique_bucket_name(), unique_prefix('unused'),
                           TableCreationParameters={'GlobalSecondaryIndexes': gsi})
    with pytest.raises(ClientError, match='ValidationException.*WarmThroughput is not '
                                          'supported on Global Secondary Indexes via ImportTable'):
        dynamodb.meta.client.import_table(**kwargs)

# CreateTable parameters missing from TableCreationParameters. DynamoDB drops
# them unread instead of refusing them: even an LSI CreateTable would refuse,
# or one of the wrong type, passes. The import completes and the table does
# not have them. botocore cannot serialize these members, so the request is
# sent as raw JSON.
LSI_ON_LSI_KEY = [{'IndexName': 'lsi',
                   'KeySchema': [{'AttributeName': 'p', 'KeyType': 'HASH'},
                                 {'AttributeName': 'lsi_key', 'KeyType': 'RANGE'}],
                   'Projection': {'ProjectionType': 'ALL'}}]

@pytest.mark.xfail(reason="SCYLLADB-1369")
@pytest.mark.parametrize('extra,absent', [
    # lsi_key is not in AttributeDefinitions.
    pytest.param({'LocalSecondaryIndexes': LSI_ON_LSI_KEY},
                 lambda client, d: 'LocalSecondaryIndexes' not in d, id='lsi_undefined_key'),
    pytest.param({'LocalSecondaryIndexes': 'not-a-list'},
                 lambda client, d: 'LocalSecondaryIndexes' not in d, id='lsi_wrong_type'),
    pytest.param({'StreamSpecification': {'StreamEnabled': True,
                                          'StreamViewType': 'NEW_AND_OLD_IMAGES'}},
                 lambda client, d: 'StreamSpecification' not in d, id='stream'),
    pytest.param({'Tags': [{'Key': 'k', 'Value': 'v'}]},
                 lambda client, d: client.list_tags_of_resource(
                     ResourceArn=d['TableArn']).get('Tags', []) == [], id='tags'),
    # Above the on-demand defaults of 12000/4000, which the table keeps.
    pytest.param({'WarmThroughput': {'ReadUnitsPerSecond': 13000,
                                     'WriteUnitsPerSecond': 5000}},
                 lambda client, d: d.get('WarmThroughput', {}).get('ReadUnitsPerSecond') == 12000,
                 id='warm_throughput'),
    # Not a CreateTable parameter either: unknown members are dropped too.
    pytest.param({'NotACreateTableField': 'x'},
                 lambda client, d: True, id='unknown'),
])
def test_unsupported_creation_parameters_dropped(dynamodb, extra, absent):
    client = dynamodb.meta.client
    s3 = make_s3_client(dynamodb)
    with new_s3_bucket(s3) as bucket:
        prefix = upload_lines(s3, bucket, dynamodb_json_lines(VALID_ITEMS),
                              context='dropped-parameters')
        table_name = unique_table_name()
        request = import_kwargs(bucket, prefix, table_name,
                                ClientToken=random_string(20),
                                TableCreationParameters=extra)
        initial = manual_request(dynamodb, 'ImportTable',
                                 json.dumps(request))['ImportTableDescription']
        with delete_table_afterwards(client, table_name):
            for field in extra:
                assert field not in initial['TableCreationParameters'], field
            final = wait_for_import(client, initial['ImportArn'])
            assert final['ImportStatus'] == 'COMPLETED'
            described = client.describe_table(TableName=table_name)['Table']
            assert absent(client, described)
            assert multiset(VALID_ITEMS) == multiset(full_scan(dynamodb.Table(table_name)))

# Because the LSI is dropped before validation, a valid LSI makes the request
# invalid: its key attribute is left defined but unused by KeySchema.
@pytest.mark.xfail(reason="SCYLLADB-1369")
def test_valid_lsi_leaves_unused_attribute(dynamodb):
    request = import_kwargs(unique_bucket_name(), unique_prefix('unused'),
                            TableCreationParameters={
                                'AttributeDefinitions': GROUND_TRUTH_SCHEMA['AttributeDefinitions'] + [
                                    {'AttributeName': 'lsi_key', 'AttributeType': 'S'}],
                                'LocalSecondaryIndexes': LSI_ON_LSI_KEY})
    with pytest.raises(ManualRequestError, match='ValidationException.*Number of attributes '
                       'in KeySchema does not exactly match number of attributes defined '
                       'in AttributeDefinitions'):
        manual_request(dynamodb, 'ImportTable', json.dumps(request))

# A missing bucket fails the import with S3NoSuchBucket, and no table is left.
@pytest.mark.xfail(reason="SCYLLADB-1369")
def test_missing_bucket_fails_the_import(dynamodb):
    client = dynamodb.meta.client
    table_name = unique_table_name()
    initial = client.import_table(**import_kwargs(
        unique_bucket_name(), unique_prefix('nowhere'), table_name))['ImportTableDescription']
    with delete_table_afterwards(client, table_name):
        final = wait_for_import(client, initial['ImportArn'])
        assert final['ImportStatus'] == 'FAILED'
        assert final['FailureCode'] == 'S3NoSuchBucket'
        assert final['FailureMessage']
        assert final['ProcessedItemCount'] == final['ImportedItemCount'] == 0
        assert final['ErrorCount'] == 0
        assert final.get('ProcessedSizeBytes', 0) == 0
        with pytest.raises(ClientError, match='ResourceNotFoundException'):
            dynamodb.meta.client.describe_table(TableName=table_name)

# A prefix matching no object in the bucket is not a failure, unlike a missing
# bucket. The import completes and leaves an empty table. The documented
# FailureCodes suggest otherwise.
@pytest.mark.xfail(reason="SCYLLADB-1369")
def test_empty_prefix_is_an_empty_success(dynamodb):
    client = dynamodb.meta.client
    s3 = make_s3_client(dynamodb)
    with new_s3_bucket(s3) as bucket:
        # The bucket is not empty, so importing the whole bucket instead of
        # the prefix would be caught.
        upload_lines(s3, bucket, dynamodb_json_lines(VALID_ITEMS),
                     context='elsewhere')
        table_name = unique_table_name()
        initial = client.import_table(**import_kwargs(
            bucket, unique_prefix('empty'), table_name))['ImportTableDescription']
        with delete_table_afterwards(client, table_name):
            final = wait_for_import(client, initial['ImportArn'])
            assert final['ImportStatus'] == 'COMPLETED'
            assert 'FailureCode' not in final
            assert final['ProcessedItemCount'] == final['ImportedItemCount'] == 0
            assert final['ErrorCount'] == 0
            assert final.get('ProcessedSizeBytes', 0) == 0
            table = client.describe_table(TableName=table_name)['Table']
            assert table['TableStatus'] == 'ACTIVE'
            assert table['TableId'] == final['TableId']
            assert full_scan(dynamodb.Table(table_name)) == []
