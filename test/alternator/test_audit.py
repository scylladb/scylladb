# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# Tests for Alternator integration with Scylla's audit logging.
# Audit is a Scylla-only feature, so every test in this file is
# Scylla-only (will be skipped when running against AWS DynamoDB).

import time

from botocore.exceptions import ClientError
import pytest
from cassandra import ConsistencyLevel, InvalidRequest
from cassandra.query import SimpleStatement

from test.alternator.util import new_test_table, unique_table_name


# Skip the entire module when running against AWS DynamoDB.
@pytest.fixture(autouse=True)
def _scylla_only(scylla_only):
    pass


# Shared table schemas reused across audit tests.
HASH_AND_RANGE_SCHEMA = {
    "KeySchema": [
        {"AttributeName": "p", "KeyType": "HASH"},
        {"AttributeName": "c", "KeyType": "RANGE"},
    ],
    "AttributeDefinitions": [
        {"AttributeName": "p", "AttributeType": "S"},
        {"AttributeName": "c", "AttributeType": "S"},
    ],
}

HASH_ONLY_SCHEMA = {
    "KeySchema": [{"AttributeName": "p", "KeyType": "HASH"}],
    "AttributeDefinitions": [{"AttributeName": "p", "AttributeType": "S"}],
}


# Returns the number of entries in the audit log table.
def _get_audit_log_count(cql):
    try:
        row = cql.execute(SimpleStatement("SELECT count(*) FROM audit.audit_log",
                                         consistency_level=ConsistencyLevel.ONE)).one()
    except InvalidRequest:
        return 0
    return row[0]


def _get_audit_log_rows(cql):
    try:
        return list(cql.execute(SimpleStatement("SELECT * FROM audit.audit_log",
                                               consistency_level=ConsistencyLevel.ONE)))
    except InvalidRequest:
        # Auditing table may not exist yet
        return []


# Waits until the audit log has grown by at least `min_delta` entries, or fails after `timeout` seconds.
# Although audit is currently synchronous (the HTTP response is sent only after audit::inspect()
# completes), we use polling rather than a single read to avoid coupling the test to that
# implementation detail — if audit ever becomes asynchronous, these tests should still pass.
def _wait_for_audit_log_growth(cql, initial_count, min_delta=1, timeout=10):
    deadline = time.time() + timeout
    last = initial_count
    while time.time() < deadline:
        current = _get_audit_log_count(cql)
        if current - initial_count >= min_delta:
            return
        last = current
        time.sleep(0.1)
    pytest.fail(f"Audit log did not grow by at least {min_delta} entries (before={initial_count}, after={last})")


def _get_new_audit_log_rows(cql, rows_before, expected_new_row_count, timeout=10):
    before_count = len(rows_before)
    before_set = set(rows_before)
    _wait_for_audit_log_growth(cql, before_count, min_delta=expected_new_row_count, timeout=timeout)
    rows_after = _get_audit_log_rows(cql)
    new_rows = [row for row in rows_after if row not in before_set]
    return new_rows


def _simplify_rows(rows):
    # Map raw audit rows to a subset of fields we care about.
    simplified = []
    for row in rows:
        simplified.append(
            (
                row.category,
                row.consistency,
                bool(row.error),
                row.keyspace_name,
                row.table_name,
                row.operation,
            )
        )
    return simplified


# Verify audit entries against expected values:
# 1) Optionally filter rows by ks_name/table_name when provided (not None).
# 2) Check that the count of relevant entries matches the expected count.
# 3) Compare category, consistency, error (bool), keyspace_name and table_name field-by-field.
# 4) When table_name is provided and non-empty, assert it appears in the operation text.
# 5) Assert that all fragment strings from the expected tuple appear in the operation text.
def _assert_audit_entries(rows, expected, ks_name=None, table_name=None):
    # When ks_name or table_name is provided, filter to matching entries only.
    if ks_name is not None or table_name is not None:
        def is_relevant(r):
            if ks_name is not None and r.keyspace_name != ks_name:
                return False
            if table_name is not None and r.table_name != table_name:
                return False
            return True
        irrelevant = [(idx, row) for idx, row in enumerate(rows) if not is_relevant(row)]
        if len(irrelevant) > 0:
            print(f"Found {len(irrelevant)} irrelevant audit entries at indices {[idx for idx, _ in irrelevant]}: {irrelevant}")
        relevant = [row for row in rows if is_relevant(row)]
    else:
        relevant = list(rows)
    assert len(relevant) == len(expected), f"Expected {len(expected)} audit entries, got {len(relevant)}: {relevant}"

    # Include the operation text in the simplified actual rows, and keep the
    # expected structure as (category, consistency, error, keyspace_name,
    # table_name, [fragments...]). Sort both lists by the first five fields
    # and then compare element-by-element, treating the last field specially.
    actual_simple = sorted(_simplify_rows(relevant), key=lambda r: r[:5])
    expected_simple = sorted(expected, key=lambda e: e[:5])

    assert len(actual_simple) == len(expected_simple), f"Unexpected audit entries: expected={expected_simple}, actual={actual_simple}"

    for actual_entry, expected_entry in zip(actual_simple, expected_simple):
        # Compare the basic audit fields one-to-one.
        assert actual_entry[:5] == expected_entry[:5], f"Unexpected audit entry fields: expected={expected_entry[:5]}, actual={actual_entry[:5]}"
        actual_operation = actual_entry[5]
        expected_fragments = expected_entry[5]
        # Basic sanity for the recorded operation text.
        assert actual_operation, "Audit entry has empty operation string"
        if table_name:
            assert table_name in actual_operation, f"Table name {table_name} not found in operation {actual_operation}"
        # The last element of the expected tuple is a list of fragments
        # that should all appear in the operation text.
        for fragment in expected_fragments:
            assert fragment in actual_operation, f"Expected substring '{fragment}' not found in operation {actual_operation}"


# Assert that no entries in `rows` match the given filters.
# Used by negative (unhappy-path) tests to verify that operations which should
# NOT be audited did not produce any audit entries.
def _assert_no_audit_entries_for(rows, ks_name=None, table_name=None, category=None):
    matching = [r for r in rows if
        (ks_name is None or r.keyspace_name == ks_name) and
        (table_name is None or r.table_name == table_name) and
        (category is None or r.category == category)]
    assert len(matching) == 0, (
        f"Expected no audit entries matching ks={ks_name}, table={table_name}, "
        f"category={category}, but found {len(matching)}: {_simplify_rows(matching)}")


# system.config stores values as JSON-encoded strings with surrounding quotes.
# Strip them so that writing back via a parameterized UPDATE doesn't double-quote.
def _strip_config_quotes(val):
    if val and val.startswith('"') and val.endswith('"'):
        return val[1:-1]
    return val


# A fixture to enable auditing for all audit categories for the duration of the test.
# The main config flag "audit" is not live updatable, so it is required to be already enabled.
# After the test, the previous audit settings are restored.
@pytest.fixture(scope="function")
def alternator_audit_enabled(cql):
    # Store current values of audit config keys in the system.config table
    names = ("audit_categories", "audit_keyspaces", "audit_tables")
    names_in_clause = ", ".join(f"'{n}'" for n in names)
    rows = cql.execute(f"SELECT name, value FROM system.config WHERE name IN ({names_in_clause})")
    original_config_vals = {row.name: row.value for row in rows}

    def get_original_config_vals(name, default):
        val = original_config_vals[name] if name in original_config_vals and original_config_vals[name] is not None else default
        return _strip_config_quotes(val)

    # Enable auditing for all categories of operations
    # Note: "audit" itself is not changed here, assuming that auditing is already enabled
    cql.execute(
        "UPDATE system.config SET value=%s WHERE name='audit_categories'",
        ("ADMIN,AUTH,QUERY,DML,DDL,DCL",),
    )
    yield
    # Restore previous values of "audit_categories", "audit_keyspaces" in the system.config table and verify the restoration
    for name in names:
        if name in original_config_vals:
            original_val = get_original_config_vals(name, "")
            cql.execute("UPDATE system.config SET value=%s WHERE name=%s", (original_val, name))
            restored = cql.execute("SELECT value FROM system.config WHERE name=%s", (name,)).one()
            restored_value = _strip_config_quotes(restored.value) if restored else None
            assert restored_value == original_val, (
                f"Config '{name}' not properly restored: expected '{original_val}', got '{restored_value}'"
            )
        else:
            # If the key wasn't present before the test, remove it so we don't leave test artifacts behind
            cql.execute("DELETE FROM system.config WHERE name=%s", (name,))

def _wait_for_active_stream(client, table_name, timeout=10):
    # Wait until the table has an active stream and return the stream ARN.
    deadline = time.time() + timeout
    while time.time() < deadline:
        desc = client.describe_table(TableName=table_name)
        stream_spec = desc['Table'].get('StreamSpecification', {})
        if stream_spec.get('StreamEnabled'):
            latest_arn = desc['Table'].get('LatestStreamArn')
            if latest_arn:
                return latest_arn
        time.sleep(0.1)
    pytest.fail(f"Stream did not become active for table {table_name} within {timeout}s")


# Test auditing of DML item operations: PutItem, UpdateItem, DeleteItem.
# One call per operation type, producing 3 audit entries total.
def test_audit_dml_operations(dynamodb, cql, alternator_audit_enabled):
    # Use a schema with both hash and range keys, to allow more varied Query-s.
    with new_test_table(dynamodb, **HASH_AND_RANGE_SCHEMA) as table:
        ks_name = f"alternator_{table.name}"
        # Enable audit for the current table's keyspace. The `alternator_audit_enabled` fixture
        # ensures that `audit_keyspaces` in system.config has been already stored too and will be
        # restored after the test.
        cql.execute("UPDATE system.config SET value=%s WHERE name='audit_keyspaces'", (ks_name,))
        before_rows = _get_audit_log_rows(cql)
        # The format inside expected is: (category, consistency, error(bool), keyspace_name, table_name, [fragments that should appear in the operation text])
        expected = []
        # PutItem
        table.put_item(Item={"p": "pk_0", "c": "ck_0", "v": "val_0"})
        expected.append(("DML", "LOCAL_QUORUM", False, ks_name, table.name, ["PutItem", "pk_0", "ck_0", "val_0"]))
        # UpdateItem
        table.update_item(Key={"p": "pk_0", "c": "ck_0"}, AttributeUpdates={"v": {"Value": "updated_0", "Action": "PUT"}})
        expected.append(("DML", "LOCAL_QUORUM", False, ks_name, table.name, ["UpdateItem", "pk_0", "ck_0", "updated_0"]))
        # DeleteItem
        table.delete_item(Key={"p": "pk_0", "c": "ck_0"})
        expected.append(("DML", "LOCAL_QUORUM", False, ks_name, table.name, ["DeleteItem", "pk_0", "ck_0"]))
        # Each individual Alternator call above must be audited.
        new_rows = _get_new_audit_log_rows(cql, before_rows, expected_new_row_count=len(expected))
        _assert_audit_entries(new_rows, expected, ks_name, table.name)


# Test auditing of the DML batch operation: BatchWriteItem.
# A single BatchWriteItem call produces one audit entry regardless of the number of items in the batch.
# Batch operations leave keyspace_name empty because they can span multiple tables.
def test_audit_dml_batch_operations(dynamodb, cql, alternator_audit_enabled):
    with new_test_table(dynamodb, **HASH_ONLY_SCHEMA) as table:
        ks_name = f"alternator_{table.name}"
        # Enable audit for the current table's keyspace. The `alternator_audit_enabled` fixture
        # ensures that `audit_keyspaces` in system.config has been already stored too and will be
        # restored after the test.
        cql.execute("UPDATE system.config SET value=%s WHERE name='audit_keyspaces'", (ks_name,))
        before_rows = _get_audit_log_rows(cql)
        client = table.meta.client
        # BatchWriteItem with PutRequest items targeting a single table.
        client.batch_write_item(RequestItems={
            table.name: [{"PutRequest": {"Item": {"p": f"pk_{i}", "v": f"val_{i}"}}} for i in range(4)]
        })
        # The format inside expected is: (category, consistency, error(bool), keyspace_name, table_name, [fragments that should appear in the operation text])
        expected = [
            ("DML", "LOCAL_QUORUM", False, "", table.name, ["BatchWriteItem", "pk_0", "val_0", "pk_1", "val_1", "pk_2", "val_2", "pk_3", "val_3"]),
        ]
        # Each individual Alternator call above must be audited.
        new_rows = _get_new_audit_log_rows(cql, before_rows, expected_new_row_count=len(expected))
        _assert_audit_entries(new_rows, expected, table_name=table.name)


# Test auditing of QUERY item operations: GetItem, Query, Scan.
# Exercises both ConsistentRead=True (LOCAL_QUORUM) and False (LOCAL_ONE),
# as well as range vs. exact Query predicates and Scan with/without FilterExpression.
def test_audit_query_item_operations(dynamodb, cql, alternator_audit_enabled):
    # Use a schema with both hash and range keys, to allow more varied Query-s.
    with new_test_table(dynamodb, **HASH_AND_RANGE_SCHEMA) as table:
        ks_name = f"alternator_{table.name}"
        # Enable audit for the current table's keyspace. The `alternator_audit_enabled` fixture
        # ensures that `audit_keyspaces` in system.config has been already stored too and will be
        # restored after the test.
        cql.execute("UPDATE system.config SET value=%s WHERE name='audit_keyspaces'", (ks_name,))
        before_rows = _get_audit_log_rows(cql)
        # The format inside expected is: (category, consistency, error(bool), keyspace_name, table_name, [fragments that should appear in the operation text])
        expected = []
        # GetItem: one strongly consistent, one eventually consistent.
        table.get_item(Key={"p": "pk_0", "c": "ck_0"}, ConsistentRead=True)
        expected.append(("QUERY", "LOCAL_QUORUM", False, ks_name, table.name, ["GetItem", "pk_0", "ck_0"]))
        table.get_item(Key={"p": "pk_0", "c": "ck_1"})
        expected.append(("QUERY", "LOCAL_ONE", False, ks_name, table.name, ["GetItem", "pk_0", "ck_1"]))
        # Query: one range predicate, one exact predicate.
        table.query(
            KeyConditionExpression="#p = :pval AND #c BETWEEN :cmin AND :cmax",
            ExpressionAttributeNames={"#p": "p", "#c": "c"},
            ExpressionAttributeValues={
                ":pval": "pk_0",
                ":cmin": "ck_0",
                ":cmax": "ck_2",
            },
            ConsistentRead=True,
            Limit=2,
        )
        expected.append(("QUERY", "LOCAL_QUORUM", False, ks_name, table.name, ["Query", "BETWEEN", "pk_0", "ck_0", "ck_2"]))
        table.query(
            KeyConditionExpression="#p = :pval AND #c = :cval",
            ExpressionAttributeNames={"#p": "p", "#c": "c"},
            ExpressionAttributeValues={
                ":pval": "pk_0",
                ":cval": "ck_1",
            },
            Limit=1,
        )
        expected.append(("QUERY", "LOCAL_ONE", False, ks_name, table.name, ["Query", "pk_0", "ck_1"]))
        # Scan: one with FilterExpression, one without.
        table.scan(
            FilterExpression="#v = :vval",
            ExpressionAttributeNames={"#v": "v"},
            ExpressionAttributeValues={":vval": "item_0_0"},
            ConsistentRead=True,
        )
        expected.append(("QUERY", "LOCAL_QUORUM", False, ks_name, table.name, ["Scan", "item_0_0"]))
        table.scan()
        expected.append(("QUERY", "LOCAL_ONE", False, ks_name, table.name, ["Scan"]))
        # Each individual Alternator call above must be audited.
        new_rows = _get_new_audit_log_rows(cql, before_rows, expected_new_row_count=len(expected))
        _assert_audit_entries(new_rows, expected, ks_name, table.name)


# Test auditing of the QUERY batch operation: BatchGetItem.
# A single BatchGetItem call produces one audit entry.
# The audit entry records CL=ANY as a placeholder; per-item consistency is set individually.
# Batch operations leave keyspace_name empty because they can span multiple tables.
def test_audit_query_batch_operations(dynamodb, cql, alternator_audit_enabled):
    with new_test_table(dynamodb, **HASH_ONLY_SCHEMA) as table:
        ks_name = f"alternator_{table.name}"
        # Pre-populate the table.
        for i in range(4):
            table.put_item(Item={"p": f"pk_{i}"})
        # Enable audit for the current table's keyspace. The `alternator_audit_enabled` fixture
        # ensures that `audit_keyspaces` in system.config has been already stored too and will be
        # restored after the test.
        cql.execute("UPDATE system.config SET value=%s WHERE name='audit_keyspaces'", (ks_name,))
        before_rows = _get_audit_log_rows(cql)
        client = table.meta.client
        client.batch_get_item(RequestItems={table.name: {"Keys": [{"p": f"pk_{i}"} for i in range(4)]}})
        expected = [("QUERY", "ANY", False, "", table.name, ["BatchGetItem", "pk_0", "pk_1", "pk_2", "pk_3"]),]
        # Each individual Alternator call above must be audited.
        new_rows = _get_new_audit_log_rows(cql, before_rows, expected_new_row_count=len(expected))
        _assert_audit_entries(new_rows, expected, table_name=table.name)


# Test auditing of DDL operations: CreateTable, UpdateTable (with GSI),
# TagResource, UntagResource, UpdateTimeToLive, DeleteTable.
# DDL and metadata-query operations have no meaningful CL (stored as "").
# The DescribeTable call (used to fetch the TableArn) also produces a QUERY entry.
# Produces 7 audit entries.
def test_audit_ddl_operations(dynamodb, cql, alternator_audit_enabled):
    client = dynamodb.meta.client
    table_name = unique_table_name()
    ks_name = f"alternator_{table_name}"
    # Enable audit for the current table's keyspace. The `alternator_audit_enabled` fixture
    # ensures that `audit_keyspaces` in system.config has been already stored too and will be
    # restored after the test.
    cql.execute("UPDATE system.config SET value=%s WHERE name='audit_keyspaces'", (ks_name,))
    before_rows = _get_audit_log_rows(cql)
    # The format inside expected is: (category, consistency, error(bool), keyspace_name, table_name, [fragments that should appear in the operation text])
    expected = []
    try:
        # CreateTable
        client.create_table(
            TableName=table_name,
            KeySchema=HASH_ONLY_SCHEMA["KeySchema"],
            AttributeDefinitions=HASH_ONLY_SCHEMA["AttributeDefinitions"],
            BillingMode='PAY_PER_REQUEST',
        )
        expected.append(("DDL", "", False, ks_name, table_name, ["CreateTable", table_name]))
        # Get TableArn via describe_table (CreateTable response may omit it in Alternator).
        desc = client.describe_table(TableName=table_name)
        table_arn = desc['Table']['TableArn']
        expected.append(("QUERY", "", False, ks_name, table_name, ["DescribeTable", table_name]))
        # UpdateTable - add a GSI which requires a new attribute definition.
        # AttributeDefinitions declares only the new GSI key attribute ("x").
        # Re-declaring existing table key attributes (e.g. "p") in
        # AttributeDefinitions is rejected by Scylla as spurious.
        client.update_table(
            TableName=table_name,
            AttributeDefinitions=[
                {"AttributeName": "x", "AttributeType": "S"},
            ],
            GlobalSecondaryIndexUpdates=[{
                "Create": {
                    "IndexName": "x_index",
                    "KeySchema": [{"AttributeName": "x", "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "ALL"},
                }
            }],
        )
        expected.append(("DDL", "", False, ks_name, table_name, ["UpdateTable", table_name, "x_index"]))
        # TagResource
        client.tag_resource(ResourceArn=table_arn, Tags=[{"Key": "env", "Value": "test"}])
        expected.append(("DDL", "", False, ks_name, table_name, ["TagResource", "env", "test"]))
        # UntagResource
        client.untag_resource(ResourceArn=table_arn, TagKeys=["env"])
        expected.append(("DDL", "", False, ks_name, table_name, ["UntagResource", "env"]))
        # UpdateTimeToLive
        client.update_time_to_live(
            TableName=table_name,
            TimeToLiveSpecification={"Enabled": True, "AttributeName": "ttl"},
        )
        expected.append(("DDL", "", False, ks_name, table_name, ["UpdateTimeToLive", table_name, "ttl"]))
        # DeleteTable
        client.delete_table(TableName=table_name)
        expected.append(("DDL", "", False, ks_name, table_name, ["DeleteTable", table_name]))
        # Each individual Alternator call above must be audited.
        new_rows = _get_new_audit_log_rows(cql, before_rows, expected_new_row_count=len(expected))
        _assert_audit_entries(new_rows, expected, ks_name, table_name)
    finally:
        try:
            client.delete_table(TableName=table_name)
        except ClientError:
            pass  # Table was already deleted by the test


# Test auditing of QUERY table-level operations: DescribeTable, ListTagsOfResource,
# DescribeTimeToLive, DescribeContinuousBackups, ListTables, DescribeEndpoints.
# ListTables and DescribeEndpoints have empty keyspace/table.
# Produces 6 audit entries.
def test_audit_query_table_operations(dynamodb, cql, alternator_audit_enabled):
    with new_test_table(dynamodb, **HASH_ONLY_SCHEMA) as table:
        ks_name = f"alternator_{table.name}"
        # Enable audit for the current table's keyspace. The `alternator_audit_enabled` fixture
        # ensures that `audit_keyspaces` in system.config has been already stored too and will be
        # restored after the test.
        cql.execute("UPDATE system.config SET value=%s WHERE name='audit_keyspaces'", (ks_name,))
        before_rows = _get_audit_log_rows(cql)
        expected = []
        client = table.meta.client
        # DescribeTable
        desc = client.describe_table(TableName=table.name)
        table_arn = desc['Table']['TableArn']
        expected.append(("QUERY", "", False, ks_name, table.name, ["DescribeTable", table.name]))
        # ListTagsOfResource
        client.list_tags_of_resource(ResourceArn=table_arn)
        expected.append(("QUERY", "", False, ks_name, table.name, ["ListTagsOfResource", table_arn]))
        # DescribeTimeToLive
        client.describe_time_to_live(TableName=table.name)
        expected.append(("QUERY", "", False, ks_name, table.name, ["DescribeTimeToLive", table.name]))
        # DescribeContinuousBackups
        client.describe_continuous_backups(TableName=table.name)
        expected.append(("QUERY", "", False, ks_name, table.name, ["DescribeContinuousBackups", table.name]))
        # ListTables (empty keyspace)
        client.list_tables()
        expected.append(("QUERY", "", False, "", "", ["ListTables"]))
        # DescribeEndpoints (empty keyspace)
        client.describe_endpoints()
        expected.append(("QUERY", "", False, "", "", ["DescribeEndpoints"]))
        # Each individual Alternator call above must be audited.
        new_rows = _get_new_audit_log_rows(cql, before_rows, expected_new_row_count=len(expected))
        _assert_audit_entries(new_rows, expected)


# Test auditing of DynamoDB Streams operations: ListStreams, DescribeStream, GetShardIterator, GetRecords.
# Each operation's audit entry uses different keyspace/table naming conventions:
#   - ListStreams: audits the input table name (if specified), or empty
#     keyspace/table when no TableName is given; CL is empty
#     (metadata-only operation).
#   - DescribeStream: keyspace is the CDC log table's keyspace,
#     table is pipe-separated "base_table|cdc_table".
#     CL is QUORUM for multi-node clusters, ONE for single-node (tests run single-node).
#   - GetShardIterator: keyspace is the CDC log table's keyspace,
#     table is pipe-separated "base_table|cdc_table". CL is empty
#     (uses only node-local metadata).
#   - GetRecords: keyspace is the CDC log table's keyspace,
#     table is pipe-separated "base_table|cdc_table". CL=LOCAL_QUORUM.
# Produces 5 audit entries.
def test_audit_streams_operations(dynamodb, dynamodbstreams, cql, alternator_audit_enabled):
    with new_test_table(dynamodb, StreamSpecification={"StreamEnabled": True, "StreamViewType": "NEW_AND_OLD_IMAGES"}, **HASH_ONLY_SCHEMA) as table:
        ks_name = f"alternator_{table.name}"
        client = table.meta.client
        # Write data so that stream records exist.
        table.put_item(Item={"p": "pk_0"})
        stream_arn = _wait_for_active_stream(client, table.name)
        # Naming for audit entries: CDC log table names and pipe-separated base|cdc names.
        # In Alternator the base and CDC tables share the same keyspace.
        cdc_table = f"{table.name}_scylla_cdc_log"
        piped_table = f"{table.name}|{cdc_table}"
        # Enable audit for the current table's keyspace.
        # The `alternator_audit_enabled` fixture ensures that `audit_keyspaces` in system.config
        # has been already stored too and will be restored after the test.
        cql.execute("UPDATE system.config SET value=%s WHERE name='audit_keyspaces'", (ks_name,))
        before_rows = _get_audit_log_rows(cql)
        expected = []
        # ListStreams - audits the input table name when TableName is given.
        dynamodbstreams.list_streams(TableName=table.name)
        expected.append(("QUERY", "", False, ks_name, table.name, ["ListStreams", table.name]))
        # ListStreams without TableName - audits with empty keyspace/table.
        dynamodbstreams.list_streams()
        expected.append(("QUERY", "", False, "", "", ["ListStreams"]))
        # DescribeStream - keyspace is the CDC log table's keyspace, table is pipe-separated base|cdc.
        # CL is QUORUM for multi-node clusters, ONE for single-node (our test environment).
        desc_resp = dynamodbstreams.describe_stream(StreamArn=stream_arn)
        shards = desc_resp['StreamDescription']['Shards']
        expected.append(("QUERY", "ONE", False, ks_name, piped_table, ["DescribeStream", stream_arn]))
        # GetShardIterator - keyspace is the CDC log table's keyspace, table is pipe-separated base|cdc.
        iter_resp = dynamodbstreams.get_shard_iterator(
            StreamArn=stream_arn, ShardId=shards[0]['ShardId'], ShardIteratorType='LATEST')
        expected.append(("QUERY", "", False, ks_name, piped_table, ["GetShardIterator", stream_arn]))
        # GetRecords - keyspace is the CDC log table's keyspace, table is pipe-separated base|cdc. CL=LOCAL_QUORUM.
        dynamodbstreams.get_records(ShardIterator=iter_resp['ShardIterator'])
        expected.append(("QUERY", "LOCAL_QUORUM", False, ks_name, piped_table, ["GetRecords"]))
        # Each individual Alternator call above must be audited.
        new_rows = _get_new_audit_log_rows(cql, before_rows, expected_new_row_count=len(expected))
        _assert_audit_entries(new_rows, expected)


# --- Unhappy-path / negative tests ---
# The tests below verify that audit entries are NOT generated when the audit
# configuration should filter them out, and that error entries are recorded
# correctly.


# Test that operations whose category is excluded from audit_categories are NOT logged.
# Each phase enables only one category and performs both a positive (should-be-logged)
# and a negative (should-NOT-be-logged) operation. The negative event is performed first;
# once the positive event's audit entry arrives, the absence of the negative entry is
# conclusive — they share the same audit pipeline.
def test_audit_category_filtering(dynamodb, cql, alternator_audit_enabled):
    with new_test_table(dynamodb, **HASH_AND_RANGE_SCHEMA) as table:
        ks_name = f"alternator_{table.name}"
        client = table.meta.client
        # Pre-populate so reads return data.
        table.put_item(Item={"p": "pk_0", "c": "ck_0", "v": "val"})
        cql.execute("UPDATE system.config SET value=%s WHERE name='audit_keyspaces'", (ks_name,))

        # Phase A: DML excluded (only QUERY enabled).
        cql.execute("UPDATE system.config SET value=%s WHERE name='audit_categories'", ("QUERY",))
        before_rows = _get_audit_log_rows(cql)
        # Negative: PutItem is DML — should NOT be logged.
        table.put_item(Item={"p": "pk_neg", "c": "ck_neg", "v": "neg"})
        # Positive: GetItem is QUERY — should be logged.
        table.get_item(Key={"p": "pk_0", "c": "ck_0"})
        expected_a = [("QUERY", "LOCAL_ONE", False, ks_name, table.name, ["GetItem", "pk_0", "ck_0"])]
        new_rows = _get_new_audit_log_rows(cql, before_rows, expected_new_row_count=1)
        _assert_audit_entries(new_rows, expected_a, ks_name, table.name)
        _assert_no_audit_entries_for(new_rows, category="DML")
        with pytest.raises(AssertionError):
            _assert_no_audit_entries_for(new_rows, category="QUERY")  # sanity check

        # Phase B: QUERY excluded (only DML enabled).
        cql.execute("UPDATE system.config SET value=%s WHERE name='audit_categories'", ("DML",))
        before_rows = _get_audit_log_rows(cql)
        # Negative: GetItem is QUERY — should NOT be logged.
        table.get_item(Key={"p": "pk_0", "c": "ck_0"})
        # Positive: PutItem is DML — should be logged.
        table.put_item(Item={"p": "pk_pos_b", "c": "ck_pos_b", "v": "pos_b"})
        expected_b = [("DML", "LOCAL_QUORUM", False, ks_name, table.name, ["PutItem", "pk_pos_b"])]
        new_rows = _get_new_audit_log_rows(cql, before_rows, expected_new_row_count=1)
        _assert_audit_entries(new_rows, expected_b, ks_name, table.name)
        _assert_no_audit_entries_for(new_rows, category="QUERY")
        with pytest.raises(AssertionError):
            _assert_no_audit_entries_for(new_rows, category="DML")  # sanity check

        # Phase C: DDL excluded (only DML and QUERY enabled).
        cql.execute("UPDATE system.config SET value=%s WHERE name='audit_categories'", ("DML,QUERY",))
        before_rows = _get_audit_log_rows(cql)
        # Get table ARN for TagResource (a DDL operation).
        desc = client.describe_table(TableName=table.name)
        table_arn = desc['Table']['TableArn']
        # Negative: TagResource is DDL — should NOT be logged.
        client.tag_resource(ResourceArn=table_arn, Tags=[{"Key": "env", "Value": "test"}])
        # Positive: PutItem is DML — should be logged.
        # Note: DescribeTable above is QUERY and will also be logged.
        table.put_item(Item={"p": "pk_pos_c", "c": "ck_pos_c", "v": "pos_c"})
        expected_c = [
            ("QUERY", "", False, ks_name, table.name, ["DescribeTable", table.name]),
            ("DML", "LOCAL_QUORUM", False, ks_name, table.name, ["PutItem", "pk_pos_c"]),
        ]
        new_rows = _get_new_audit_log_rows(cql, before_rows, expected_new_row_count=2)
        _assert_audit_entries(new_rows, expected_c, ks_name, table.name)
        _assert_no_audit_entries_for(new_rows, category="DDL")
        with pytest.raises(AssertionError):
            _assert_no_audit_entries_for(new_rows, category="DML")  # sanity check
        with pytest.raises(AssertionError):
            _assert_no_audit_entries_for(new_rows, category="QUERY")  # sanity check


# Test that operations on a keyspace NOT listed in audit_keyspaces are NOT logged.
# Two tables are created; audit_keyspaces is set to only one table's keyspace.
# Operations on the non-audited table should produce no entries, while operations
# on the audited table (positive canary) confirm the audit pipeline is working.
def test_audit_keyspace_filtering(dynamodb, cql, alternator_audit_enabled):
    with new_test_table(dynamodb, **HASH_ONLY_SCHEMA) as table_a:
        with new_test_table(dynamodb, **HASH_ONLY_SCHEMA) as table_b:
            ks_a = f"alternator_{table_a.name}"
            ks_b = f"alternator_{table_b.name}"
            # Audit only table_a's keyspace.
            cql.execute("UPDATE system.config SET value=%s WHERE name='audit_keyspaces'", (ks_a,))
            before_rows = _get_audit_log_rows(cql)
            # Negative: operations on table_b (wrong keyspace) — should NOT be logged.
            table_b.put_item(Item={"p": "pk_b"})
            table_b.get_item(Key={"p": "pk_b"})
            # Positive: PutItem on table_a (correct keyspace) — should be logged.
            table_a.put_item(Item={"p": "pk_a"})
            expected = [("DML", "LOCAL_QUORUM", False, ks_a, table_a.name, ["PutItem", "pk_a"])]
            new_rows = _get_new_audit_log_rows(cql, before_rows, expected_new_row_count=1)
            _assert_audit_entries(new_rows, expected, ks_a, table_a.name)
            _assert_no_audit_entries_for(new_rows, ks_name=ks_b)
            with pytest.raises(AssertionError):
                _assert_no_audit_entries_for(new_rows, ks_name=ks_a)  # sanity check


# Test that a failed operation (one that throws after audit_info is set) generates
# an audit entry with error=True.
# GetItem with an extra bogus key attribute passes table lookup (audit_info is set)
# but then check_key() throws ValidationException. A normal GetItem follows as the
# positive canary (error=False). Both entries should be present.
def test_audit_error_entry(dynamodb, cql, alternator_audit_enabled):
    with new_test_table(dynamodb, **HASH_ONLY_SCHEMA) as table:
        ks_name = f"alternator_{table.name}"
        # Insert data so the successful GetItem has something to return.
        table.put_item(Item={"p": "pk_0"})
        cql.execute("UPDATE system.config SET value=%s WHERE name='audit_keyspaces'", (ks_name,))
        before_rows = _get_audit_log_rows(cql)
        # Negative operation: GetItem with an extra key attribute beyond the schema.
        # The table has only "p" as the hash key, so passing "bogus" triggers check_key()
        # which throws api_error::validation after audit_info is already set.
        with pytest.raises(ClientError, match='ValidationException'):
            table.get_item(Key={"p": "pk_0", "bogus": "junk"})
        # Positive operation: normal GetItem — should succeed and produce error=False entry.
        table.get_item(Key={"p": "pk_0"})
        expected = [
            ("QUERY", "LOCAL_ONE", True, ks_name, table.name, ["GetItem", "pk_0", "bogus"]),
            ("QUERY", "LOCAL_ONE", False, ks_name, table.name, ["GetItem", "pk_0"]),
        ]
        new_rows = _get_new_audit_log_rows(cql, before_rows, expected_new_row_count=2)
        _assert_audit_entries(new_rows, expected, ks_name, table.name)


# Test that operations with empty keyspace (ListTables, DescribeEndpoints) are
# logged regardless of what audit_keyspaces is configured to, because the
# should_log() function short-circuits on keyspace().empty().
# Meanwhile, operations with a non-empty keyspace that is NOT in audit_keyspaces
# should NOT be logged.
def test_audit_empty_keyspace_bypass(dynamodb, cql, alternator_audit_enabled):
    with new_test_table(dynamodb, **HASH_ONLY_SCHEMA) as table:
        ks_name = f"alternator_{table.name}"
        client = table.meta.client
        # Set audit_keyspaces to an unrelated keyspace — NOT the table's keyspace
        # and NOT an empty string. This means table-scoped operations on our table
        # should be filtered out, but empty-keyspace operations should still pass.
        cql.execute("UPDATE system.config SET value=%s WHERE name='audit_keyspaces'", ("nonexistent_ks",))
        before_rows = _get_audit_log_rows(cql)
        # Negative: PutItem on the table (non-empty keyspace, not in audit_keyspaces) — should NOT be logged.
        table.put_item(Item={"p": "pk_0"})
        # Positive: ListTables and DescribeEndpoints (empty keyspace) — should be logged.
        client.list_tables()
        client.describe_endpoints()
        expected = [
            ("QUERY", "", False, "", "", ["ListTables"]),
            ("QUERY", "", False, "", "", ["DescribeEndpoints"]),
        ]
        new_rows = _get_new_audit_log_rows(cql, before_rows, expected_new_row_count=2)
        _assert_audit_entries(new_rows, expected)
        _assert_no_audit_entries_for(new_rows, ks_name=ks_name)
        with pytest.raises(AssertionError):
            _assert_no_audit_entries_for(new_rows, ks_name="")  # sanity check


# Test the audit_tables=alternator.<table> shorthand. When the user configures
# audit_tables=alternator.<table_a>, the parser expands this to the internal
# keyspace name alternator_<table_a> with table <table_a>. Only operations on
# table_a should be audited; operations on table_b should NOT appear.
def test_audit_tables_filtering(dynamodb, cql, alternator_audit_enabled):
    with new_test_table(dynamodb, **HASH_ONLY_SCHEMA) as table_a:
        with new_test_table(dynamodb, **HASH_ONLY_SCHEMA) as table_b:
            ks_a = f"alternator_{table_a.name}"
            ks_b = f"alternator_{table_b.name}"
            # Use the alternator.<table> shorthand in audit_tables.
            cql.execute("UPDATE system.config SET value=%s WHERE name='audit_tables'",
                        (f"alternator.{table_a.name}",))
            # Clear audit_keyspaces so it doesn't interfere with the test.
            cql.execute("UPDATE system.config SET value=%s WHERE name='audit_keyspaces'", ("",))
            before_rows = _get_audit_log_rows(cql)
            # Negative: PutItem on table_b — should NOT be logged.
            table_b.put_item(Item={"p": "pk_b"})
            # Positive canary: PutItem on table_a — should be logged.
            table_a.put_item(Item={"p": "pk_a"})
            expected = [("DML", "LOCAL_QUORUM", False, ks_a, table_a.name, ["PutItem", "pk_a"])]
            new_rows = _get_new_audit_log_rows(cql, before_rows, expected_new_row_count=1)
            _assert_audit_entries(new_rows, expected, ks_a, table_a.name)
            _assert_no_audit_entries_for(new_rows, ks_name=ks_b)
            with pytest.raises(AssertionError):
                _assert_no_audit_entries_for(new_rows, ks_name=ks_a)  # sanity check
