# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# Conformance tests for the DynamoDB "import from S3" API family - ImportTable,
# DescribeImport and ListImports.
#
# What real DynamoDB does, measured against Amazon. (!) marks the ones that
# contradict the API reference.
#  1 (!) ClientToken: only a different TableName conflicts ("Duplicate request
#       detected with conflicting parameters"); a different bucket, prefix,
#       format or compression silently returns the original import. And once
#       the import finishes, its table exists, so a replay hits
#       ResourceInUseException before the token is consulted at all.
#  2     ListImports returns IN_PROGRESS imports, not only finished ones
#  3 (!) a prefix matching no object COMPLETES - not FAILED - and the empty table
#       it asked for is created and kept
#  4     absent InputCompressionType means NONE
#  5     ProcessedSizeBytes counts uncompressed bytes - a gzipped and a plain
#       import of the same lines report the same number, the uncompressed one
#       (the object data has changed since, so the figure itself is not recorded
#       here; GROUND_TRUTH_BYTES computes it from the object data)
#  6     empty and whitespace-only lines are ignored, neither items nor errors
#  7     a zero-length object is accepted as zero items
#  8     InputFormatOptions.Csv with DYNAMODB_JSON is rejected
#  9 (!) the counters do NOT advance while the import is IN_PROGRESS: every poll
#       of a ten-minute import reported zero, and the totals appeared only once
#       it ended - so they are useless as progress
# 10 (!) EndTime is absent until the import ends, and ProcessedSizeBytes is
#       absent until it has read something - neither is reported as a zero
# 11     ProcessedItemCount == ImportedItemCount + ErrorCount
# 12 (!) the first line DynamoDB cannot turn into an item abandons the rest of
#       its object, and that includes a line which is valid JSON but has no
#       Item member - not only malformed JSON. All six defects below in one
#       object measured as a single error; each therefore gets an object of
#       its own, so that each is actually reached
# 13     importing into an existing table raises ResourceInUseException
# 14     CloudWatchLogGroupArn is always returned
# 15     TableId is allocated up front and never changes
# 16     ImportedItemCount counts writes, not surviving rows (5 for 3 keys)
# 17     which of two duplicate items wins is not pinned down here
# 18 (!) ClientToken is mandatory, though the reference marks it optional: a
#        request that really carries none is a ValidationException. boto3 hides
#        this - ClientToken is an idempotencyToken, so botocore invents one for
#        every request that omits it, and no ordinary caller ever sees it.

import base64
import gzip
import json
import logging
import time
from contextlib import contextmanager
from decimal import Decimal

import pytest
from boto3.dynamodb.types import Binary, TypeSerializer
from botocore.exceptions import ClientError

from test.alternator.util import (client_no_transform, full_scan, is_aws,
                                  make_s3_client, multiset, new_s3_bucket,
                                  random_string, unique_bucket_name,
                                  unique_table_name)

GROUND_TRUTH_SCHEMA = {
    'KeySchema': [{'AttributeName': 'p', 'KeyType': 'HASH'},
                  {'AttributeName': 'c', 'KeyType': 'RANGE'}],
    'AttributeDefinitions': [{'AttributeName': 'p', 'AttributeType': 'S'},
                             {'AttributeName': 'c', 'AttributeType': 'N'}],
}


# The import reads the source objects itself, so there is no way to fake them:
# they have to really be in S3. Against AWS that means a real bucket, which the
# tests create and delete themselves; against Scylla it will mean MinIo, which
# is not wired up yet - make_s3_client() asserts until it is. That plumbing -
# make_s3_client(), new_s3_bucket() and unique_bucket_name() - is shared with
# test_export.py and lives in util.py; only the object data built below is
# specific to imports.

def unique_prefix(what):
    return f'alternator-import-test/{what}-{random_string(16).lower()}/'

def upload_lines(s3, bucket, lines, *, what='object_data', compress=False,
                 prefix=None, name='00000'):
    """Put `lines` into a single S3 object and return the prefix it lives under.
    The body is the lines joined by newlines with a trailing one, which is what
    GROUND_TRUTH_BYTES counts. Pass a `prefix` an earlier call returned
    and a different `name`, to add a second object to the same object data."""
    if prefix is None:
        prefix = unique_prefix(what)
    body = ('\n'.join(lines) + '\n').encode()
    s3.put_object(Bucket=bucket, Key=f'{prefix}{name}.json' + ('.gz' if compress else ''),
                  Body=gzip.compress(body) if compress else body)
    return prefix

# Nothing here deletes the source objects one prefix at a time: the source
# objects of an import are user-owned - neither DynamoDB nor Alternator ever
# touches them - and util.new_s3_bucket() purges the whole bucket at the end.


# ---------------------------------------------------------- the object data

GROUND_TRUTH_ITEMS = [
    # The minimal item: nothing but the key.
    {'p': 'minimal', 'c': Decimal(1)},
    # Strings needing JSON escaping, and the empty one.
    {'p': 'strings', 'c': Decimal(2), 'ascii': 'hello', 'unicode': 'zażółć 日本語 🦑',
     'escapes': 'quote" backslash\\ newline\n tab\t', 'empty': '', 'long': 'x' * 1000},
    # 38 significant digits and a small exponent - but NOT the documented +125
    # maximum: DynamoDB returns such a number expanded (129 digits), which
    # boto3's 38-digit DYNAMODB_CONTEXT then refuses to deserialize at all.
    {'p': 'numbers', 'c': Decimal(3), 'zero': Decimal(0), 'neg': Decimal('-17.5'),
     'digits38': Decimal('1' * 38), 'big': Decimal('9.99999E+37'),
     'small': Decimal('1E-130'), 'frac': Decimal('0.000001')},
    # Binary: empty, and bytes that are not UTF-8 - an export base64-encodes
    # them, so this catches an encoding shortcut.
    {'p': 'binary', 'c': Decimal(4), 'bytes': Binary(b'\x00\x01\xfe\xff'),
     'empty': Binary(b''), 'text': Binary(b'not really binary')},
    # The scalar types that are not S/N/B.
    {'p': 'scalars', 'c': Decimal(5), 'yes': True, 'no': False, 'nothing': None},
    # The three set types. Sets are unordered, so an export may reorder them.
    {'p': 'sets', 'c': Decimal(6), 'ss': {'a', 'b', 'c'},
     'ns': {Decimal(1), Decimal(-2), Decimal('3.5')},
     'bs': {Binary(b'\x00'), Binary(b'\x01')}},
    # Documents: mixed list and map, both also empty.
    {'p': 'documents', 'c': Decimal(7),
     'list': ['s', Decimal(1), True, None, Binary(b'b'), [], {}],
     'map': {'a': Decimal(1), 'b': {'c': 'd'}}, 'empty_list': [], 'empty_map': {}},
    # Deep nesting, which the JSON-lines parser has to recurse through.
    {'p': 'nested', 'c': Decimal(8), 'deep': {"0": {"1": {"2": "leaf"}}}},
    # Awkward attribute names: a reserved word, a dotted one, one differing from
    # the key only in case, and a long one.
    {'p': 'names', 'c': Decimal(9), 'Size': 'reserved word', 'a.b': 'dotted',
     'P': 'not the key', 'n' * 255: 'long name'},
    # Two items in one partition - an export writes a partition's items together.
    {'p': 'shared', 'c': Decimal(10), 'which': 'first'},
    {'p': 'shared', 'c': Decimal(11), 'which': 'second'},
    # Close to the 400 KB item limit, so one item will not fit a small buffer.
    {'p': 'big', 'c': Decimal(12), 'blob': 'y' * 380000},
]

DUPLICATE_ITEMS = [
    {'p': 'dup', 'c': Decimal(1), 'which': 'first'},
    {'p': 'dup', 'c': Decimal(2), 'which': 'first'},
    {'p': 'unique', 'c': Decimal(3), 'which': 'only'},
    {'p': 'dup', 'c': Decimal(1), 'which': 'second'},
    {'p': 'dup', 'c': Decimal(2), 'which': 'second'},
]
DUPLICATE_KEYS = 3

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

# These are ignored outright rather than counted as items or as errors.
IGNORED_LINES = ['', '   ']

def _encode_value(av):
    """TypeSerializer.serialize() hands back binary as `bytes`, but the AWS API
    reference specifies a binary AttributeValue as a base64-encoded string - so
    B and BS are re-encoded, and L and M are walked in case they hold one."""
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

def dynamodb_json_lines(items) -> list[str]:
    """The items as DYNAMODB_JSON lines - one `{"Item": {...}}` string per item,
    which is the format ImportTable reads and ExportTableToPointInTime writes."""
    serializer = TypeSerializer()
    return [json.dumps({'Item': {name: _encode_value(serializer.serialize(value))
                                 for name, value in item.items()}})
            for item in items]

# What the ground-truth object data weighs before compression. upload_lines()
# joins the lines with newlines, ends the object with one, and uploads that
# UTF-8 encoded.
GROUND_TRUTH_BYTES = sum(len(line.encode()) + 1
                         for line in dynamodb_json_lines(GROUND_TRUTH_ITEMS))


# ------------------------------------------------------------ import plumbing

def import_kwargs(bucket, prefix, table_name=None, **overrides):
    """Build an ImportTable request. Every override replaces the member it
    names, except that a dict is merged into the existing member one field at a
    time, and a None deletes the member (or the field) outright."""
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

def with_member(kwargs, member, value):
    """A copy of an already-built request with one member changed; a dict value
    is merged into the existing one, so a single nested field can be altered."""
    changed = dict(kwargs)
    changed[member] = {**kwargs[member], **value} if isinstance(value, dict) else value
    return changed

def _poll_interval(client):
    return 5 if is_aws(client) else 0.1

TERMINAL_IMPORT_STATUSES = ['COMPLETED', 'FAILED', 'CANCELLED']

def poll_import(client, import_arn, timeout=None):
    """Yield DescribeImport descriptions until the import reaches a terminal
    status - the last one yielded - or raise TimeoutError if it never does."""
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

def wait_for_import(client, import_arn, timeout=None):
    """Poll DescribeImport until the import reaches a terminal status, and return
    that last description."""
    # poll_import() always yields at least once, and its last is the terminal one.
    for description in poll_import(client, import_arn, timeout):
        pass
    return description

@contextmanager
def delete_table_afterwards(client, table_name):
    """Delete the table an import created, whatever the import made of it. The
    body must have waited for the import to reach a terminal status first -
    DynamoDB will not delete a table an import is still writing into - and an
    import that failed before creating anything leaves no table at all, which
    is not an error here."""
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
                if code == 'ResourceInUseException' and time.time() < deadline:
                    time.sleep(_poll_interval(client))
                    continue
                if code != 'ResourceNotFoundException':
                    logging.error('Failed to delete table %s: %s', table_name, e)
                break

def list_all_imports(client, **kwargs):
    summaries, token = [], None
    while True:
        response = client.list_imports(**kwargs, **({'NextToken': token} if token else {}))
        summaries += response['ImportSummaryList']
        token = response.get('NextToken')
        if not token:
            return summaries

INITIAL_MEMBERS = ['ImportArn', 'ImportStatus', 'TableArn', 'TableId', 'ClientToken',
                   'S3BucketSource', 'InputFormat', 'InputCompressionType',
                   'TableCreationParameters', 'StartTime', 'ProcessedItemCount',
                   'ImportedItemCount', 'ErrorCount']

ABSENT_INITIAL_MEMBERS = ['EndTime', 'FailureCode', 'FailureMessage', 'ProcessedSizeBytes']

# Everything ImportTable must report the moment it accepts a request.
@pytest.mark.xfail(reason="SCYLLADB-1369")
def test_import_table_description(dynamodb):
    client = dynamodb.meta.client
    s3 = make_s3_client(dynamodb)
    with new_s3_bucket(s3, kind='import') as bucket:
        prefix = upload_lines(s3, bucket, dynamodb_json_lines(GROUND_TRUTH_ITEMS))
        table_name = unique_table_name()
        kwargs = import_kwargs(bucket, prefix, table_name)
        assert 'ClientToken' not in kwargs
        initial = client.import_table(**kwargs)['ImportTableDescription']
        with delete_table_afterwards(client, table_name):
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
            # botocore fills in ClientToken
            assert initial['ClientToken']
            final = wait_for_import(client, initial['ImportArn'])
            assert final['ImportStatus'] == 'COMPLETED'
            if is_aws(dynamodb):
                assert 'CloudWatchLogGroupArn' in final
            else:
                assert 'CloudWatchLogGroupArn' not in final

# What DescribeImport must say once the import ended, and what the table it
# created must look like. The members fixed at request time have to still match
# what ImportTable reported, TableId included: it is allocated up front and
# never changes.
@pytest.mark.xfail(reason="SCYLLADB-1369")
def test_import_completes(dynamodb):
    client = dynamodb.meta.client
    s3 = make_s3_client(dynamodb)
    with new_s3_bucket(s3, kind='import') as bucket:
        prefix = upload_lines(s3, bucket, dynamodb_json_lines(GROUND_TRUTH_ITEMS))
        # A zero-length object in the same object data is accepted and contributes
        # nothing - so none of the counters below move because of it.
        s3.put_object(Bucket=bucket, Key=f'{prefix}empty.json', Body=b'')
        table_name = unique_table_name()
        initial = client.import_table(
            **import_kwargs(bucket, prefix, table_name))['ImportTableDescription']
        with delete_table_afterwards(client, table_name):
            final = wait_for_import(client, initial['ImportArn'])
            assert final['ImportStatus'] == 'COMPLETED'
            assert 'EndTime' in final
            assert final['EndTime'] >= final['StartTime']
            assert final['ProcessedSizeBytes'] > 0
            assert final['ErrorCount'] == 0
            assert final['ProcessedItemCount'] == final['ImportedItemCount'] == len(GROUND_TRUTH_ITEMS)
            # DescribeImport must describe the same import ImportTable reported
            for member in ['ImportArn', 'TableArn', 'TableId', 'ClientToken',
                           'S3BucketSource', 'InputFormat', 'InputCompressionType',
                           'TableCreationParameters', 'StartTime']:
                assert final[member] == initial[member]
            assert multiset(GROUND_TRUTH_ITEMS) == multiset(full_scan(dynamodb.Table(table_name)))
            described = client.describe_table(TableName=table_name)['Table']
            assert described['TableStatus'] == 'ACTIVE'
            assert described['TableId'] == initial['TableId']

# What can only be seen while the import is still running. Three questions share
# the one import because there is no second chance at any of them: whether the
# counters advance live or are only written at the end, what
# DescribeTable says about a table being imported into, and whether ListImports
# admits to a running import at all.
@pytest.mark.xfail(reason="SCYLLADB-1369")
def test_import_in_flight(dynamodb):
    client = dynamodb.meta.client
    s3 = make_s3_client(dynamodb)
    with new_s3_bucket(s3, kind='import') as bucket:
        prefix = upload_lines(s3, bucket, dynamodb_json_lines(GROUND_TRUTH_ITEMS))
        table_name = unique_table_name()
        initial = client.import_table(
            **import_kwargs(bucket, prefix, table_name))['ImportTableDescription']
        arn = initial['ImportArn']
        with delete_table_afterwards(client, table_name):
            first = client.describe_import(ImportArn=arn)['ImportTableDescription']
            assert first['ImportStatus'] == 'IN_PROGRESS', \
                'the import finished before a single poll saw it running'
            assert 'EndTime' not in first
            try:
                described = client.describe_table(TableName=table_name)['Table']
            except ClientError as e:
                assert e.response['Error']['Code'] == 'ResourceNotFoundException'
            else:
                assert described['TableStatus'] == 'CREATING'
            # The API reference says ListImports lists "completed imports within
            # the past 90 days"; the same claim about ListExports was wrong.
            listed = [s for s in list_all_imports(client, TableArn=initial['TableArn'])
                      if s['ImportArn'] == arn]
            assert listed, 'a running import was not listed'
            # Preserve last in progress sample - the counters are not updated
            # until the import is finished.
            in_progress_sample = first
            for last_sample in poll_import(client, arn):
                if last_sample['ImportStatus'] in TERMINAL_IMPORT_STATUSES:
                    break
                in_progress_sample = last_sample
            assert last_sample['ImportStatus'] == 'COMPLETED'
            assert in_progress_sample['ProcessedItemCount'] == 0
            assert in_progress_sample['ImportedItemCount'] == 0
            assert in_progress_sample['ErrorCount'] == 0


# The request replayed with the same ClientToken while the import is running.
# "identical" must give the original import back; the rest change one member the
# duplicate check turns out to ignore.
TOKEN_REPLAYS = [
    ('compression', 'InputCompressionType', 'GZIP'),
    ('format', 'InputFormat', 'CSV'),
    ('prefix', 'S3BucketSource', {'S3KeyPrefix': 'some/other/prefix/'}),
    ('bucket', 'S3BucketSource', {'S3Bucket': 'some-other-bucket-name'}),
]

# Everything the ClientToken duplicate check does, from one import. The replays
# happen while that import is still running because that is the only window in
# which the token is consulted at all - once the import finishes its table
# exists, and ResourceInUseException is raised first - so they cannot be split
# into a test each without an import each.
@pytest.mark.xfail(reason="SCYLLADB-1369")
def test_client_token_reuse(dynamodb):
    client = dynamodb.meta.client
    token = random_string(20)
    s3 = make_s3_client(dynamodb)
    with new_s3_bucket(s3, kind='import') as bucket:
        prefix = upload_lines(s3, bucket, dynamodb_json_lines(VALID_ITEMS[:1]), what='tiny')
        table_name = unique_table_name()
        kwargs = import_kwargs(bucket, prefix, table_name, ClientToken=token)
        initial = client.import_table(**kwargs)['ImportTableDescription']
        with delete_table_afterwards(client, table_name):
            assert initial['ClientToken'] == token
            # An identical request returns the import that already exists.
            repeated = client.import_table(**kwargs)['ImportTableDescription']
            assert repeated['ImportArn'] == initial['ImportArn']
            assert repeated['TableId'] == initial['TableId']
            # Everything except the target table is ignored by that check: the
            # same token with a different bucket, prefix, format or compression
            # silently returns the *original* import, source and all - a
            # footgun, since a client that edits its request and retries gets no
            # hint that its change was dropped.
            for label, member, value in TOKEN_REPLAYS:
                replay = client.import_table(
                    **with_member(kwargs, member, value))['ImportTableDescription']
                assert replay['ImportArn'] == initial['ImportArn'], label
                assert replay[member] == initial[member], label
            # A *different* table is the one conflict - "Duplicate request
            # detected with conflicting parameters" - and ImportConflictException
            # is the name it uses, not the IdempotentParameterMismatch the
            # ClientToken documentation mentions.
            other = with_member(kwargs, 'TableCreationParameters',
                                {'TableName': unique_table_name()})
            with pytest.raises(ClientError,
                               match='ImportConflictException.*Duplicate request'):
                client.import_table(**other)
            # Once the import has ended its table exists, and a replay is refused
            # on that ground before the token is consulted at all.
            wait_for_import(client, initial['ImportArn'])
            with pytest.raises(ClientError,
                               match='ResourceInUseException.*Table already exists'):
                client.import_table(**kwargs)

# ClientToken is documented as optional, and is not: ImportTable refuses a
# request without one. Every other test here gets a token without asking,
# because botocore invents one for an idempotencyToken member.
@pytest.mark.xfail(reason="SCYLLADB-1369")
def test_client_token_required(dynamodb):
    kwargs = import_kwargs(unique_bucket_name('import'), unique_prefix('unused'))
    assert 'ClientToken' not in kwargs
    with client_no_transform(dynamodb.meta.client) as client:
        with pytest.raises(ClientError, match='ValidationException.*[Cc]lientToken'):
            client.import_table(**kwargs)

# The API reference constrains ClientToken to ^[^\$]+$ - a dollar sign anywhere
# in it, or an empty token, is a validation error. All five of these reach the
# service: botocore does not enforce the pattern or the minimum length.
@pytest.mark.xfail(reason="SCYLLADB-1369")
@pytest.mark.parametrize('token', ['$', 'has$dollar', '$leading', 'trailing$', ''])
def test_client_token_invalid(dynamodb, token):
    kwargs = import_kwargs(unique_bucket_name('import'), unique_prefix('unused'),
                           ClientToken=token)
    with pytest.raises(ClientError, match='ValidationException'):
        dynamodb.meta.client.import_table(**kwargs)

# -------------------------------------------------------------- DescribeImport

def unknown_import_arn(dynamodb, table):
    table_arn = dynamodb.meta.client.describe_table(
        TableName=table.name)['Table']['TableArn']
    return f'{table_arn}/import/{int(time.time() * 1000):014}-0badf00d'

# An import which does not exist is not a validation problem - the ARN was fine.
@pytest.mark.xfail(reason="SCYLLADB-1369")
def test_describe_import_not_found(dynamodb, test_table_s):
    arn = unknown_import_arn(dynamodb, test_table_s)
    with pytest.raises(ClientError, match='ImportNotFoundException'):
        dynamodb.meta.client.describe_import(ImportArn=arn)

# An ARN that looks right but names a 1-character table: "Invalid Import ARN".
@pytest.mark.xfail(reason="SCYLLADB-1369")
def test_describe_import_malformed_arn(dynamodb, test_table_s):
    # Built from a real table's ARN.
    table_arn = dynamodb.meta.client.describe_table(
        TableName=test_table_s.name)['Table']['TableArn']
    arn_up_to_table, _, _ = table_arn.rpartition('/')
    # below TableName's 3-character minimum.
    arn = f'{arn_up_to_table}/x/import/01658528578619-c4d4e311'
    with pytest.raises(ClientError, match='ValidationException.*[Ii]mport.*ARN'):
        dynamodb.meta.client.describe_import(ImportArn=arn)

# ---------------------------------------------------------------- ListImports

# Everything ImportSummary may contain. The upper bound is the point: an
# implementation returning the full ImportTableDescription - counters,
# TableCreationParameters and all - passes any test that only checks presence.
IMPORT_SUMMARY_MEMBERS = {'ImportArn', 'ImportStatus', 'TableArn', 'S3BucketSource',
                          'InputFormat', 'StartTime', 'EndTime', 'CloudWatchLogGroupArn'}

@contextmanager
def two_finished_imports(dynamodb):
    client = dynamodb.meta.client
    s3 = make_s3_client(dynamodb)
    with new_s3_bucket(s3, kind='import') as bucket:
        prefix = upload_lines(s3, bucket, dynamodb_json_lines(VALID_ITEMS[:1]),
                              what='tiny')
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

# Check members of list_imports() summary
@pytest.mark.xfail(reason="SCYLLADB-1369")
def test_list_imports_contents(dynamodb):
    client = dynamodb.meta.client
    with two_finished_imports(dynamodb) as (first, second):
        summaries = list_all_imports(client)
        ours = {first['ImportArn'], second['ImportArn']}
        # There should be at least our imports in summaries
        assert ours <= {s['ImportArn'] for s in summaries}
        summary, = [s for s in summaries if s['ImportArn'] == first['ImportArn']]
        summary_fields = set(summary)
        # CloudWatchLogGroupArn is AWS-only: Alternator has no CloudWatch, and
        # test_import_table_description insists DescribeImport omits it there.
        expected = IMPORT_SUMMARY_MEMBERS if is_aws(dynamodb) \
            else IMPORT_SUMMARY_MEMBERS - {'CloudWatchLogGroupArn'}
        assert summary_fields <= expected, \
            f"Unexpected fields in summary {summary_fields - expected}"
        # Both imports have finished, so every member is due.
        assert summary_fields >= expected, \
            f"Not enough fields in summary {expected - summary_fields}"

# The filter selects by the table an import targeted - check the two agree.
@pytest.mark.xfail(reason="SCYLLADB-1369")
def test_list_imports_table_arn_filter(dynamodb):
    client = dynamodb.meta.client
    with two_finished_imports(dynamodb) as (first, second):
        table_arn = first['TableArn']
        listed = {s['ImportArn'] for s in list_all_imports(client, TableArn=table_arn)}
        assert first['ImportArn'] in listed
        assert second['ImportArn'] not in listed
        assert all(arn.startswith(table_arn + '/import/') for arn in listed)

# Paging a page at a time must reach every import, and must not hand the same
# one out on two pages.
@pytest.mark.xfail(reason="SCYLLADB-1369")
def test_list_imports_paging(dynamodb):
    client = dynamodb.meta.client
    with two_finished_imports(dynamodb) as (first, second):
        ours = {first['ImportArn'], second['ImportArn']}
        response = client.list_imports(PageSize=1)
        assert len(response['ImportSummaryList']) == 1
        # Two imports of ours exist, so there is certainly more to come.
        assert 'NextToken' in response
        paged = [s['ImportArn'] for s in list_all_imports(client, PageSize=1)]
        assert len(paged) == len(set(paged)), 'an import was returned on two pages'
        # There can be additional imports
        assert ours <= set(paged)

# PageSize is documented as 1 to 25. 0 and below are refused by botocore.
@pytest.mark.xfail(reason="SCYLLADB-1369")
@pytest.mark.parametrize('page_size', [26, 1000])
def test_list_imports_page_size_too_large(dynamodb, page_size):
    with pytest.raises(ClientError, match='ValidationException'):
        dynamodb.meta.client.list_imports(PageSize=page_size)

# Only a token of the right length can be tested here,
# botocore refuses a shorter one locally.
@pytest.mark.xfail(reason="SCYLLADB-1369")
def test_list_imports_bad_next_token(dynamodb):
    with pytest.raises(ClientError, match='ValidationException'):
        dynamodb.meta.client.list_imports(NextToken='0' * 112)

# ------------------------------------------------------------------- counters

# Duplicate keys are normal input, not an error - DynamoDB says they "overwrite
# each other in random order until one remains".
@pytest.mark.xfail(reason="SCYLLADB-1369")
def test_duplicate_items(dynamodb):
    client = dynamodb.meta.client
    s3 = make_s3_client(dynamodb)
    with new_s3_bucket(s3, kind='import') as bucket:
        prefix = upload_lines(s3, bucket, dynamodb_json_lines(DUPLICATE_ITEMS),
                              what='dupes')
        table_name = unique_table_name()
        initial = client.import_table(
            **import_kwargs(bucket, prefix, table_name))['ImportTableDescription']
        with delete_table_afterwards(client, table_name):
            final = wait_for_import(client, initial['ImportArn'])
            assert final['ImportStatus'] == 'COMPLETED'
            assert final['ErrorCount'] == 0
            # Both counters count writes, not surviving rows - otherwise neither
            # could ever exceed the table's item count and the two would be the
            # same number.
            assert final['ProcessedItemCount'] == len(DUPLICATE_ITEMS)
            assert final['ImportedItemCount'] == len(DUPLICATE_ITEMS)
            items = full_scan(dynamodb.Table(table_name))
            assert len(items) == DUPLICATE_KEYS
            # AWS calls the order random, assert either first or second of our items.
            assert {item['which'] for item in items} <= {'first', 'second', 'only'}

EXPECTED_BAD_COUNTERS = {
    'ProcessedItemCount': len(VALID_ITEMS) + len(INVALID_LINES),
    'ImportedItemCount': len(VALID_ITEMS),
    'ErrorCount': len(INVALID_LINES),
}

# An import which skipped even one item ends as FAILED - but its table is kept,
# with everything that did pass validation in it. That combination is the whole
# of DynamoDB's ItemValidationError semantics.
@pytest.mark.xfail(reason="SCYLLADB-1369")
def test_invalid_items(dynamodb):
    client = dynamodb.meta.client
    s3 = make_s3_client(dynamodb)
    with new_s3_bucket(s3, kind='import') as bucket:
        prefix = upload_lines(s3, bucket,
                              dynamodb_json_lines(VALID_ITEMS) + IGNORED_LINES,
                              what='bad', name='00000-good')
        # One object per defect. All six in a single object measured as *one*
        # error, because the first line DynamoDB cannot turn into an item
        # abandons the rest of that object - so the other five were never
        # read and nothing was learnt about them. An object each is the only way
        # to see what every one of them does.
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

# Both compressed and uncompressed objects are counted as uncompressed.
# Assert that the ProcessedSizeBytes is the same.
@pytest.mark.xfail(reason="SCYLLADB-1369")
def test_processed_size_bytes_is_uncompressed(dynamodb):
    client = dynamodb.meta.client
    lines = dynamodb_json_lines(GROUND_TRUTH_ITEMS)
    s3 = make_s3_client(dynamodb)
    with new_s3_bucket(s3, kind='import') as bucket:
        plain_prefix = upload_lines(s3, bucket, lines, what='plain')
        gzip_prefix = upload_lines(s3, bucket, lines, what='gzip', compress=True)
        plain_table, gzip_table = unique_table_name(), unique_table_name()
        plain_kwargs = import_kwargs(bucket, plain_prefix, plain_table,
                                     InputCompressionType=None)
        assert 'InputCompressionType' not in plain_kwargs
        gzip_kwargs = import_kwargs(bucket, gzip_prefix, gzip_table,
                                    InputCompressionType='GZIP')
        with delete_table_afterwards(client, plain_table), \
             delete_table_afterwards(client, gzip_table):
            plain = client.import_table(**plain_kwargs)['ImportTableDescription']
            gzipped = client.import_table(**gzip_kwargs)['ImportTableDescription']
            plain_final = wait_for_import(client, plain['ImportArn'])
            gzip_final = wait_for_import(client, gzipped['ImportArn'])
            assert plain_final['ImportStatus'] == 'COMPLETED'
            assert plain_final['ErrorCount'] == 0
            assert plain_final['ProcessedItemCount'] == plain_final['ImportedItemCount'] == len(GROUND_TRUTH_ITEMS)
            assert plain_final['ProcessedSizeBytes'] == GROUND_TRUTH_BYTES
            # The gzipped half is the same content through the decompressor: same
            # byte count, and the same items out the other end.
            assert gzip_final['ImportStatus'] == 'COMPLETED'
            assert gzip_final['ProcessedItemCount'] == gzip_final['ImportedItemCount'] == len(GROUND_TRUTH_ITEMS)
            assert gzip_final['ProcessedSizeBytes'] == GROUND_TRUTH_BYTES
            assert gzip_final['ProcessedSizeBytes'] == plain_final['ProcessedSizeBytes']
            assert multiset(GROUND_TRUTH_ITEMS) == multiset(full_scan(dynamodb.Table(plain_table)))
            assert multiset(GROUND_TRUTH_ITEMS) == multiset(full_scan(dynamodb.Table(gzip_table)))

# CsvOptions alongside DYNAMODB_JSON is undocumented; DynamoDB refuses it.
@pytest.mark.xfail(reason="SCYLLADB-1369")
def test_csv_options_with_dynamodb_json_rejected(dynamodb):
    kwargs = import_kwargs(unique_bucket_name('import'), unique_prefix('unused'),
                           InputFormatOptions={'Csv': {'Delimiter': ','}})
    with pytest.raises(ClientError, match='ValidationException.*[Ii]nputFormatOptions'):
        dynamodb.meta.client.import_table(**kwargs)

# Requests ImportTable must refuse before it accepts anything. Every row names
# the member it is about, and the match insists the error mentions it.
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
    kwargs = import_kwargs(unique_bucket_name('import'), unique_prefix('unused'), **overrides)
    with pytest.raises(ClientError, match=f'ValidationException.*({parameter})'):
        dynamodb.meta.client.import_table(**kwargs)

# A bucket which does not exist is the one failure DynamoDB documents a code for.
@pytest.mark.xfail(reason="SCYLLADB-1369")
def test_missing_bucket_fails_the_import(dynamodb):
    client = dynamodb.meta.client
    table_name = unique_table_name()
    initial = client.import_table(**import_kwargs(
        unique_bucket_name('import'), unique_prefix('nowhere'), table_name))['ImportTableDescription']
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

# A prefix matching no object is *not* a failure: the import COMPLETES having
# done nothing, and the empty table it was asked for is created and kept.
@pytest.mark.xfail(reason="SCYLLADB-1369")
def test_empty_prefix_is_an_empty_success(dynamodb):
    client = dynamodb.meta.client
    s3 = make_s3_client(dynamodb)
    with new_s3_bucket(s3, kind='import') as bucket:
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
