# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

# Tests for Alternator integration with Scylla's audit logging.

import time
import random

import pytest
from cassandra import InvalidRequest

from test.alternator.util import new_test_table
from test.cqlpy.cassandra_tests.porting import row


# Returns the number of entries in the audit log table.
def _get_audit_log_count(cql):
    try:
        row = cql.execute("SELECT count(*) FROM audit.audit_log").one()
    except InvalidRequest:
        return 0
    return row[0]


def _get_audit_log_rows(cql):
    try:
        return list(cql.execute("SELECT * FROM audit.audit_log"))
    except InvalidRequest:
        # Auditing table may not exist yet
        return []


# Waits until the audit log has grown by at least `min_delta` entries, or fails after `timeout` seconds.
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


# Verify two things:
# 1) That the expected audit entries are present, by testing category, consistency, error (bool), keyspace_name and table_name
# 2) That the operation text is non-empty and contains at least the correct table name and the fragments of the request provided as a list.
def _assert_audit_entries(rows, expected, ks_name, table_name):
    # Only consider entries for the keyspace/table under test.
    irrelevant = [(idx, row) for idx, row in enumerate(rows) if row.keyspace_name != ks_name or row.table_name != table_name]
    if len(irrelevant) > 0:
        print(f"Found {len(irrelevant)} irrelevant audit entries at indices {[idx for idx, _ in irrelevant]}: {irrelevant}")
    relevant = [row for row in rows if row.keyspace_name == ks_name and row.table_name == table_name]
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
        assert table_name in actual_operation, f"Table name {table_name} not found in operation {actual_operation}"
        # The last element of the expected tuple is a list of fragments
        # that should all appear in the operation text.
        for fragment in expected_fragments:
            assert fragment in actual_operation, f"Expected substring '{fragment}' not found in operation {actual_operation}"


# A fixture to enable auditing for all audit categories for the duration of the test.
# The main config flag "audit" is not live updatable, so it is required to be already enabled.
# After the test, the previous audit settings are restored.
@pytest.fixture(scope="function")
def alternator_audit_enabled(cql):
    # Store current values of "audit_categories", "audit_keyspaces" in the system.config table
    names = ("audit_categories", "audit_keyspaces")
    names_serialized = ", ".join(f"'{n}'" for n in names)
    rows = cql.execute(f"SELECT name, value FROM system.config WHERE name IN ({names_serialized})")
    original_config_vals = {row.name: row.value for row in rows}

    def get_original_config_vals(name, default):
        return original_config_vals[name] if name in original_config_vals and original_config_vals[name] is not None else default

    try:
        # Enable auditing for all categories of operations
        # Note: "audit" itself is not changed here, assuming that auditing is already enabled
        cql.execute(
            "UPDATE system.config SET value=%s WHERE name='audit_categories'",
            ("ADMIN,AUTH,QUERY,DML,DDL,DCL",),
        )
        yield
    finally:
        # Restore previous values of "audit_categories", "audit_keyspaces" in the system.config table
        for name in names:
            if name in original_config_vals:
                cql.execute("UPDATE system.config SET value=%s WHERE name=%s", (get_original_config_vals(name, ""), name))


def test_auditing_of_operations(scylla_only, dynamodb, cql, alternator_audit_enabled):
    # Use a schema with both hash and range keys, to allow more varied Query-s.
    schema = {
        "KeySchema": [
            {"AttributeName": "p", "KeyType": "HASH"},
            {"AttributeName": "c", "KeyType": "RANGE"},
        ],
        "AttributeDefinitions": [
            {"AttributeName": "p", "AttributeType": "S"},
            {"AttributeName": "c", "AttributeType": "S"},
        ],
    }

    with new_test_table(dynamodb, **schema) as table:
        ks_name = f"alternator_{table.name}"
        # Enable audit for the current table's keyspace. The `alternator_audit_enabled` fixture
        # ensures that `audit_keyspaces` in system.config has been already stored too and will be
        # restored after the test.
        cql.execute("UPDATE system.config SET value=%s WHERE name='audit_keyspaces'", (ks_name,))
        before_rows = _get_audit_log_rows(cql)
        tab = dynamodb.Table(table.name)
        expected = []
        # PutItem
        for hash_i in range(4):
            for range_i in range(4):
                tab.put_item(Item={"p": f"pk_{hash_i}", "c": f"ck_{range_i}", "v": f"item_{hash_i}_{range_i}"})
                expected.append(("DML", "LOCAL_QUORUM", False, ks_name, table.name, [f"pk_{hash_i}", f"ck_{range_i}", f"item_{hash_i}_{range_i}"]))
        # GetItem, with deterministically chosen consistency.
        for hash_i in range(4):
            for range_i in range(4):
                strongly_consistent = bool((hash_i + range_i) % 2)
                tab.get_item(Key={"p": f"pk_{hash_i}", "c": f"ck_{range_i}"}, ConsistentRead=True if strongly_consistent else None)
                expected.append(("QUERY", "LOCAL_QUORUM" if strongly_consistent else "LOCAL_ONE", False, ks_name, table.name, [f"pk_{hash_i}", f"ck_{range_i}"]))
        # UpdateItem
        for hash_i in range(4):
            for range_i in range(4):
                tab.update_item(
                    Key={"p": f"pk_{hash_i}", "c": f"ck_{range_i}"},
                    AttributeUpdates={"v": {"Value": f"updated_item_{hash_i}_{range_i}", "Action": "PUT"}},
                )
                expected.append(("DML", "LOCAL_QUORUM", False, ks_name, table.name, [f"pk_{hash_i}", f"ck_{range_i}", f"updated_item_{hash_i}_{range_i}"]))
        # Query, with both exact and range predicates.
        for hash_i in range(2):
            strongly_consistent = bool(hash_i % 2)
            tab.query(
                KeyConditionExpression="#p = :pval AND #c BETWEEN :cmin AND :cmax",
                ExpressionAttributeNames={"#p": "p", "#c": "c"},
                ExpressionAttributeValues={
                    ":pval": f"pk_{hash_i}",
                    ":cmin": f"ck_{hash_i}",
                    ":cmax": f"ck_{hash_i+2}",
                },
                ConsistentRead=True if strongly_consistent else None,
                Limit=2,
            )
            expected.append(("QUERY", "LOCAL_QUORUM" if strongly_consistent else "LOCAL_ONE", False, ks_name, table.name, ["BETWEEN", f"pk_{hash_i}", f"ck_{hash_i}", f"ck_{hash_i+2}"]))
            tab.query(
                KeyConditionExpression="#p = :pval AND #c = :cval",
                ExpressionAttributeNames={"#p": "p", "#c": "c"},
                ExpressionAttributeValues={
                    ":pval": f"pk_{hash_i}",
                    ":cval": f"ck_{hash_i+1}",
                },
                ConsistentRead=True if strongly_consistent else None,
                Limit=1,
            )
            expected.append(("QUERY", "LOCAL_QUORUM" if strongly_consistent else "LOCAL_ONE", False, ks_name, table.name, [f"pk_{hash_i}", f"ck_{hash_i+1}"]))
        # Scan
        for i in range(4):
            strongly_consistent = bool(i % 2)
            use_filter = (i // 2) % 2 # use FilterExpression for the latter two loop runs
            if use_filter:
                tab.scan(
                    FilterExpression="#v = :vval",
                    ExpressionAttributeNames={"#v": "v"},
                    ExpressionAttributeValues={":vval": f"updated_item_{i}_{i}"},
                    ConsistentRead=True if strongly_consistent else None,
                )
                expected.append(("QUERY", "LOCAL_QUORUM" if strongly_consistent else "LOCAL_ONE", False, ks_name, table.name, [f"updated_item_{i}_{i}"]))
            else:
                tab.scan(ConsistentRead=True if strongly_consistent else None)
                expected.append(("QUERY", "LOCAL_QUORUM" if strongly_consistent else "LOCAL_ONE", False, ks_name, table.name, []))
        # DeleteItem
        for hash_i in range(4):
            for range_i in range(4):
                tab.delete_item(Key={"p": f"pk_{hash_i}", "c": f"ck_{range_i}"})
                expected.append(("DML", "LOCAL_QUORUM", False, ks_name, table.name, [f"pk_{hash_i}", f"ck_{range_i}"]))
        # Each individual Alternator call above must be audited.
        new_rows = _get_new_audit_log_rows(cql, before_rows, expected_new_row_count=len(expected))  # pyright: ignore[reportCallIssue]
        _assert_audit_entries(new_rows, expected, ks_name, table.name)
