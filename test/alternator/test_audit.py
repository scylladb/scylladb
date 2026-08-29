# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# Tests for Alternator integration with Scylla's audit logging.
# Audit is a Scylla-only feature, so every test in this file is
# Scylla-only (will be skipped when running against AWS DynamoDB).

from concurrent.futures import ThreadPoolExecutor
import gzip
import json
import struct
import time

from botocore.exceptions import ClientError
import pytest
import requests
from cassandra import ConsistencyLevel, InvalidRequest
from cassandra.query import SimpleStatement

from test.alternator.util import get_signed_request, new_test_table, scylla_inject_error, unique_table_name
from test.alternator.test_vector import need_vector_search_in_botocore


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


def _assert_audit_operation_excludes(row, excluded_fragments):
    for fragment in excluded_fragments:
        assert fragment not in row.operation, f"Unexpected substring '{fragment}' found in operation {row.operation}"


# system.config stores values as JSON-encoded strings with surrounding quotes.
# Strip them so that writing back via a parameterized UPDATE doesn't double-quote.
def _strip_config_quotes(val):
    if val and val.startswith('"') and val.endswith('"'):
        return val[1:-1]
    return val


def _set_audit_rules(cql, rules):
    cql.execute("UPDATE system.config SET value=%s WHERE name='audit_rules'", (json.dumps(rules),))


def _batch_audit_metadata_memory(request):
    table_names = list(request["RequestItems"])
    table_name_bytes = sum(len(name.encode()) for name in table_names)
    table_count = len(table_names)
    joined_table_names = table_name_bytes + max(table_count - 1, 0)
    retained_table_set = 2 * table_name_bytes + table_count * (len("alternator_") + 2 + 128)
    sink_table_refs = 2 * table_count * struct.calcsize("P")
    return retained_table_set, retained_table_set + sink_table_refs + 6 * joined_table_names


# A fixture to enable auditing for all audit categories for the duration of the test.
# The main config flag "audit" is not live updatable, so it is required to be already enabled.
# After the test, the previous audit settings are restored.
@pytest.fixture(scope="function")
def alternator_audit_enabled(cql):
    # Store current values of audit config keys in the system.config table
    names = ("audit_categories", "audit_keyspaces", "audit_tables", "audit_rules")
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
    # Restore previous values of audit config keys in the system.config table and verify the restoration
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


def _request_permit_before_audit(
        dynamodb, rest_api, operation, payload, transport,
        expected_status=200, expect_audit_serialization=True, expected_audit_decision_units=None,
        return_measurements=False):
    injection = "alternator_request_before_audit"
    encoded_payload = gzip.compress(payload) if transport == "gzip" else payload
    extra_headers = {"Content-Encoding": "gzip"} if transport == "gzip" else None
    signed_request = get_signed_request(dynamodb, operation, encoded_payload, extra_headers)
    headers = {key: value for key, value in signed_request.headers.items() if key.lower() != "content-length"}
    if transport == "chunked":
        request_body = iter((signed_request.body,))
    else:
        headers["Content-Length"] = str(len(signed_request.body))
        request_body = signed_request.body

    with ThreadPoolExecutor(max_workers=1) as executor, scylla_inject_error(rest_api, injection):
        request = executor.submit(
            requests.post,
            signed_request.url,
            headers=headers,
            data=request_body,
            verify=False,
            cert=signed_request.cert,
            timeout=60,
        )
        try:
            deadline = time.time() + 30
            while time.time() < deadline:
                if request.done():
                    response = request.result()
                    pytest.fail(f"Request completed before reaching permit injection: {response.status_code} {response.text}")
                response = requests.get(f"{rest_api}/v2/error_injection/injection/{injection}/enters", timeout=5)
                response.raise_for_status()
                if response.json() > 0:
                    break
                time.sleep(0.1)
            else:
                pytest.fail("Request did not reach the pre-audit injection")

            response = requests.get(f"{rest_api}/v2/error_injection/injection/{injection}", timeout=5)
            response.raise_for_status()
            injection_info = response.json()
            def parameters_named(name):
                return [
                    (shard_id, int(parameter["value"]))
                    for shard_id, shard in enumerate(injection_info)
                    for parameter in shard.get("parameters", [])
                    if parameter["key"] == name
                ]

            permit_units = parameters_named("request_permit_units")
            assert len(permit_units) == 1, f"Expected one request permit measurement, got {permit_units}"
            audit_serialization_shards = [
                shard_id
                for shard_id, shard in enumerate(injection_info)
                for parameter in shard.get("parameters", [])
                if parameter["key"] == "audit_serialization_shard"
            ]
            if expect_audit_serialization:
                assert audit_serialization_shards == [permit_units[0][0]]
            else:
                assert not audit_serialization_shards
            dom_memory = parameters_named("request_dom_memory_bytes")
            assert len(dom_memory) == 1, f"Expected one DOM memory measurement, got {dom_memory}"
            before_parse_units = parameters_named("request_permit_units_before_parse")
            assert len(before_parse_units) == 1, f"Expected one pre-parse permit measurement, got {before_parse_units}"
            batch_reparse_memory = parameters_named("batch_reparse_memory_bytes")
            assert len(batch_reparse_memory) == 1, f"Expected one batch reparse memory measurement, got {batch_reparse_memory}"
            audit_decision_units = parameters_named("request_permit_units_after_audit_decision")
            if expected_audit_decision_units is not None:
                assert audit_decision_units == [(permit_units[0][0], expected_audit_decision_units)]
        finally:
            response = requests.post(f"{rest_api}/v2/error_injection/injection/{injection}/message", timeout=5)
            response.raise_for_status()
        response = request.result(timeout=30)
        assert response.status_code == expected_status, response.text
    if return_measurements:
        return {
            "permit_units": permit_units[0][1],
            "before_parse_units": before_parse_units[0][1],
            "dom_memory": dom_memory[0][1],
            "batch_reparse_memory": batch_reparse_memory[0][1],
            "audit_decision_units": audit_decision_units[0][1] if audit_decision_units else None,
        }
    return permit_units[0][1]


# A large request must remain covered by its memory permit until the audit
# write completes. This reproduces issue #31297 for Content-Length, chunked,
# and compressed requests.
@pytest.mark.parametrize("transport", ["content-length", "chunked", "gzip"])
def test_audit_large_request(dynamodb, cql, rest_api, alternator_audit_enabled, transport):
    with new_test_table(dynamodb, **HASH_ONLY_SCHEMA) as table:
        ks_name = f"alternator_{table.name}"
        cql.execute("UPDATE system.config SET value=%s WHERE name='audit_keyspaces'", (ks_name,))
        before_rows = _get_audit_log_rows(cql)
        marker = "audit-large-request-end"
        payload = json.dumps({
            "TableName": table.name,
            "Item": {"p": {"S": "pk"}, "v": {"S": "x" * (256 * 1024) + marker}},
        }, separators=(",", ":")).encode()
        permit_units = _request_permit_before_audit(dynamodb, rest_api, "PutItem", payload, transport)
        assert permit_units == len(payload) * 6 + 8000
        new_rows = _get_new_audit_log_rows(cql, before_rows, expected_new_row_count=1)
        expected = [("DML", "LOCAL_QUORUM", False, ks_name, table.name, ["PutItem", marker])]
        _assert_audit_entries(new_rows, expected, ks_name, table.name)


# Batch audit filtering reparses the retained request and serializes a filtered
# copy. Verify that this path retains its larger reservation through auditing.
def test_audit_large_batch_request(dynamodb, cql, rest_api, alternator_audit_enabled):
    with new_test_table(dynamodb, **HASH_ONLY_SCHEMA) as table:
        ks_name = f"alternator_{table.name}"
        cql.execute("UPDATE system.config SET value=%s WHERE name='audit_keyspaces'", (ks_name,))
        before_rows = _get_audit_log_rows(cql)
        marker = "audit-large-batch-request-end"
        request = {
            "RequestItems": {
                table.name: [{
                    "PutRequest": {
                        "Item": {"p": {"S": "pk"}, "v": {"S": "x" * (256 * 1024) + marker}},
                    },
                }],
            },
        }
        payload = json.dumps(request, separators=(",", ":")).encode()
        measurements = _request_permit_before_audit(
            dynamodb, rest_api, "BatchWriteItem", payload, "content-length", return_measurements=True)
        request_metadata, audit_metadata = _batch_audit_metadata_memory(request)
        request_memory = max(len(payload) * 2, measurements["dom_memory"])
        assert measurements["permit_units"] == max(
            request_memory + len(payload) * 6 + request_metadata,
            measurements["batch_reparse_memory"] + len(payload) + audit_metadata,
            len(payload) * 7 + audit_metadata,
        ) + 8000
        new_rows = _get_new_audit_log_rows(cql, before_rows, expected_new_row_count=1)
        expected = [("DML", "LOCAL_QUORUM", False, "", table.name, ["BatchWriteItem", marker])]
        _assert_audit_entries(new_rows, expected, table_name=table.name)


# Canonical JSON can be larger than the request encoding. Issue #31297 requires
# the permit to cover both request processing and the later table-audit write.
def test_audit_expanding_json_request(dynamodb, cql, rest_api, alternator_audit_enabled):
    before_rows = _get_audit_log_rows(cql)
    value_count = 1024
    payload = b'{"unused":[' + b",".join([b"1e20"] * value_count) + b"]}"
    serialized_number = b"100000000000000000000.0"
    serialized_size = len(b'{"unused":[') + value_count * len(serialized_number) + value_count - 1 + len(b"]}")
    assert serialized_size > len(payload)

    permit_units = _request_permit_before_audit(dynamodb, rest_api, "ListTables", payload, "content-length")
    # Request parsing/retention needs 2L + 2S. The table audit write can peak
    # at six canonical-size copies while its mutation is being frozen.
    assert permit_units == max(len(payload) * 2 + serialized_size * 2, serialized_size * 6) + 8000

    new_rows = _get_new_audit_log_rows(cql, before_rows, expected_new_row_count=1)
    expected = [("QUERY", "", False, "", "", ["ListTables", serialized_number.decode()])]
    _assert_audit_entries(new_rows, expected, ks_name="", table_name="")


# Dense scalar arrays retain much more RapidJSON DOM memory than their wire
# size. Keep that measured DOM charge while serializing and writing the audit.
def test_audit_dense_json_request_memory(dynamodb, rest_api, alternator_audit_enabled):
    value_count = 32 * 1024
    payload = b'{"unused":[' + b",".join([b"0"] * value_count) + b"]}"
    measurements = _request_permit_before_audit(
        dynamodb, rest_api, "ListTables", payload, "content-length", return_measurements=True)

    request_memory = max(len(payload) * 2, measurements["dom_memory"])
    assert measurements["dom_memory"] > len(payload) * 2
    assert measurements["permit_units"] == max(
        request_memory + len(payload) * 2,
        len(payload) * 6,
    ) + 8000


# Batch table auditing keeps the original request while writing its filtered
# request. Use expanding numbers so this seven-copy audit-write peak exceeds
# the intentionally conservative 2L + 6S batch request reservation.
def test_audit_expanding_json_batch_request(dynamodb, cql, rest_api, alternator_audit_enabled):
    with new_test_table(dynamodb, **HASH_ONLY_SCHEMA) as table:
        ks_name = f"alternator_{table.name}"
        cql.execute("UPDATE system.config SET value=%s WHERE name='audit_keyspaces'", (ks_name,))
        before_rows = _get_audit_log_rows(cql)
        value_count = 1024
        payload_prefix = (
            b'{"RequestItems":{"' + table.name.encode() +
            b'":[{"PutRequest":{"Item":{"p":{"S":"pk"}}}}]},"unused":['
        )
        payload = payload_prefix + b",".join([b"1e20"] * value_count) + b"]}"
        serialized_number = b"100000000000000000000.0"
        serialized_size = len(payload) + value_count * (len(serialized_number) - len(b"1e20"))
        assert serialized_size > len(payload) * 2

        measurements = _request_permit_before_audit(
            dynamodb, rest_api, "BatchWriteItem", payload, "content-length", return_measurements=True)
        request_metadata, audit_metadata = _batch_audit_metadata_memory({"RequestItems": {table.name: None}})
        request_memory = max(len(payload) * 2, measurements["dom_memory"])
        assert measurements["permit_units"] == max(
            request_memory + serialized_size * 6 + request_metadata,
            measurements["batch_reparse_memory"] + serialized_size + audit_metadata,
            serialized_size * 7 + audit_metadata,
        ) + 8000

        new_rows = _get_new_audit_log_rows(cql, before_rows, expected_new_row_count=1)
        expected = [("DML", "LOCAL_QUORUM", False, "", table.name, ["BatchWriteItem", serialized_number.decode()])]
        _assert_audit_entries(new_rows, expected, table_name=table.name)


# Batch filtering reparses the retained query. Its reservation must cover the
# rebuilt DOM and RapidJSON construction stack while the canonical query is
# still retained.
def test_audit_dense_json_batch_request_memory(dynamodb, cql, rest_api, alternator_audit_enabled):
    with new_test_table(dynamodb, **HASH_ONLY_SCHEMA) as table:
        ks_name = f"alternator_{table.name}"
        cql.execute("UPDATE system.config SET value=%s WHERE name='audit_keyspaces'", (ks_name,))
        value_count = 32 * 1024
        payload_prefix = (
            b'{"RequestItems":{"' + table.name.encode() +
            b'":[{"PutRequest":{"Item":{"p":{"S":"pk"}}}}]},"unused":['
        )
        payload = payload_prefix + b",".join([b"0"] * value_count) + b"]}"
        measurements = _request_permit_before_audit(
            dynamodb, rest_api, "BatchWriteItem", payload, "content-length", return_measurements=True)

        request_metadata, audit_metadata = _batch_audit_metadata_memory({"RequestItems": {table.name: None}})
        request_memory = max(len(payload) * 2, measurements["dom_memory"])
        assert measurements["dom_memory"] > len(payload) * 2
        assert measurements["batch_reparse_memory"] > measurements["dom_memory"] * 2
        assert measurements["before_parse_units"] >= measurements["permit_units"]
        assert measurements["permit_units"] == max(
            request_memory + len(payload) * 6 + request_metadata,
            measurements["batch_reparse_memory"] + len(payload) + audit_metadata,
            len(payload) * 7 + audit_metadata,
        ) + 8000


# Insignificant whitespace is discarded by canonical serialization. Keep the
# request-processing reservation when it is larger than the later audit-write
# peak; the two phase estimates must not be added together.
def test_audit_shrinking_json_request(dynamodb, cql, rest_api, alternator_audit_enabled):
    before_rows = _get_audit_log_rows(cql)
    canonical_payload = b'{"unused":[]}'
    payload = canonical_payload + b" " * (256 * 1024)
    assert len(canonical_payload) < len(payload)

    permit_units = _request_permit_before_audit(dynamodb, rest_api, "ListTables", payload, "content-length")
    assert permit_units == len(payload) * 2 + len(canonical_payload) * 2 + 8000

    new_rows = _get_new_audit_log_rows(cql, before_rows, expected_new_row_count=1)
    expected = [("QUERY", "", False, "", "", ["ListTables", canonical_payload.decode()])]
    _assert_audit_entries(new_rows, expected, ks_name="", table_name="")


def test_audit_ddl_serialization_matches_request_shard(dynamodb, cql, rest_api, alternator_audit_enabled):
    # CreateTable audits after validating TableName but before requiring the
    # remaining schema, so this request does not create a table.
    table_name = unique_table_name()
    cql.execute("UPDATE system.config SET value=%s WHERE name='audit_keyspaces'", (f"alternator_{table_name}",))
    payload = json.dumps({"TableName": table_name}, separators=(",", ":")).encode()
    _request_permit_before_audit(dynamodb, rest_api, "CreateTable", payload, "content-length", expected_status=400)

    # An otherwise empty UpdateTable request is audited before its validation
    # error and likewise avoids making a schema change.
    with new_test_table(dynamodb, **HASH_ONLY_SCHEMA) as table:
        cql.execute("UPDATE system.config SET value=%s WHERE name='audit_keyspaces'", (f"alternator_{table.name}",))
        payload = json.dumps({"TableName": table.name}, separators=(",", ":")).encode()
        _request_permit_before_audit(dynamodb, rest_api, "UpdateTable", payload, "content-length", expected_status=400)

    # Reject invalid table names before they can populate audit metadata. In
    # particular, syslog does not escape its keyspace and table fields.
    cql.execute("UPDATE system.config SET value=%s WHERE name='audit_categories'", ("",))
    cql.execute("UPDATE system.config SET value=%s WHERE name='audit_keyspaces'", ("",))
    cql.execute("UPDATE system.config SET value=%s WHERE name='audit_tables'", ("",))
    _set_audit_rules(cql, [{
        "sinks": ["table"],
        "categories": ["DDL", "QUERY"],
        "qualified_table_names": ["*"],
        "roles": ["*"],
    }])
    invalid_table_name = 'invalid\n"table'
    payload = json.dumps({"TableName": invalid_table_name}, separators=(",", ":")).encode()
    for operation in ["UpdateTable", "DeleteTable", "DescribeContinuousBackups"]:
        _request_permit_before_audit(
            dynamodb, rest_api, operation, payload, "content-length",
            expected_status=400, expect_audit_serialization=False)


def test_audit_update_existing_legacy_table_name(dynamodb, cql, rest_api, alternator_audit_enabled):
    # CQL can create a table whose two-byte name predates/does not satisfy the
    # current Alternator minimum, without exceeding today's keyspace limit.
    table_name = "zz"
    ks_name = f"alternator_{table_name}"
    this_dc = cql.execute("SELECT data_center FROM system.local").one().data_center
    cql.execute(
        f'CREATE KEYSPACE "{ks_name}" WITH REPLICATION = '
        f"{{'class': 'NetworkTopologyStrategy', '{this_dc}': 1}}")
    try:
        cql.execute(f'CREATE TABLE "{ks_name}"."{table_name}" (p text PRIMARY KEY)')
        cql.execute("UPDATE system.config SET value=%s WHERE name='audit_keyspaces'", (ks_name,))
        payload = json.dumps({"TableName": table_name}, separators=(",", ":")).encode()
        # Current validation rejects creating a two-byte name, but operations
        # on an existing table must continue to work and audit.
        _request_permit_before_audit(
            dynamodb, rest_api, "UpdateTable", payload, "content-length", expected_status=400)
    finally:
        cql.execute(f'DROP KEYSPACE "{ks_name}"')


# An initialized audit service should not impose audit-copy reservations on an
# operation whose category cannot be logged.
def test_unaudited_request_memory(dynamodb, cql, rest_api, alternator_audit_enabled):
    with new_test_table(dynamodb, **HASH_ONLY_SCHEMA) as table:
        cql.execute("UPDATE system.config SET value=%s WHERE name='audit_categories'", ("DDL",))
        _set_audit_rules(cql, [])
        payload = json.dumps({
            "TableName": table.name,
            "Item": {"p": {"S": "pk"}, "v": {"S": "x" * (256 * 1024)}},
        }, separators=(",", ":")).encode()
        measurements = _request_permit_before_audit(
            dynamodb, rest_api, "PutItem", payload, "content-length",
            expect_audit_serialization=False, return_measurements=True)
        request_memory = max(len(payload) * 2, measurements["dom_memory"])
        assert measurements["permit_units"] == request_memory + 8000

    value_count = 32 * 1024
    payload = b'{"unused":[' + b",".join([b"0"] * value_count) + b"]}"
    measurements = _request_permit_before_audit(
        dynamodb, rest_api, "ListTables", payload, "content-length",
        expect_audit_serialization=False, return_measurements=True)
    request_memory = max(len(payload) * 2, measurements["dom_memory"])
    assert measurements["before_parse_units"] > measurements["dom_memory"]
    assert measurements["permit_units"] == request_memory + 8000


# A category-level admission decision is necessarily conservative because the
# table is known only after parsing. Once table or role filtering excludes the
# request, return the audit-only part of the permit before doing database work.
def test_filtered_request_releases_audit_memory(dynamodb, cql, rest_api, alternator_audit_enabled):
    with new_test_table(dynamodb, **HASH_ONLY_SCHEMA) as audited_table:
        with new_test_table(dynamodb, **HASH_ONLY_SCHEMA) as filtered_table:
            audited_ks = f"alternator_{audited_table.name}"
            cql.execute("UPDATE system.config SET value=%s WHERE name='audit_keyspaces'", (audited_ks,))
            cql.execute("UPDATE system.config SET value=%s WHERE name='audit_tables'", ("",))
            _set_audit_rules(cql, [])

            requests_to_check = [
                ("PutItem", {
                    "TableName": filtered_table.name,
                    "Item": {"p": {"S": "pk"}, "padding": {"S": "x" * (256 * 1024)}},
                }, 200),
                ("BatchWriteItem", {
                    "RequestItems": {
                        filtered_table.name: [{"PutRequest": {"Item": {"p": {"S": "batch-pk"}}}}],
                    },
                    "padding": "x" * (256 * 1024),
                }, 200),
                ("BatchGetItem", {
                    "RequestItems": {
                        filtered_table.name: {"Keys": [{"p": {"S": "missing-pk"}}]},
                    },
                    "padding": "x" * (256 * 1024),
                }, 200),
                # This fails inside vector-query validation.
                # VectorSearch auditing predates this PR and is not implemented;
                # it must still resolve the pending audit reservation first.
                ("Query", {
                    "TableName": audited_table.name,
                    "VectorSearch": {},
                    "padding": "x" * (256 * 1024),
                }, 400),
            ]

            for operation, request, expected_status in requests_to_check:
                payload = json.dumps(request, separators=(",", ":")).encode()
                measurements = _request_permit_before_audit(
                    dynamodb, rest_api, operation, payload, "content-length",
                    expected_status=expected_status, expect_audit_serialization=False, return_measurements=True)
                request_memory = max(len(payload) * 2, measurements["dom_memory"])
                assert measurements["audit_decision_units"] == request_memory + 8000
                assert measurements["permit_units"] == request_memory + 8000


def test_filtered_dense_request_retains_dom_memory(dynamodb, cql, rest_api, alternator_audit_enabled):
    with new_test_table(dynamodb, **HASH_ONLY_SCHEMA) as audited_table:
        with new_test_table(dynamodb, **HASH_ONLY_SCHEMA) as filtered_table:
            cql.execute("UPDATE system.config SET value=%s WHERE name='audit_keyspaces'", (f"alternator_{audited_table.name}",))
            value_count = 32 * 1024
            payload_prefix = json.dumps({
                "TableName": filtered_table.name,
                "Item": {"p": {"S": "pk"}},
            }, separators=(",", ":"))[:-1].encode()
            payload = payload_prefix + b',"unused":[' + b",".join([b"0"] * value_count) + b"]}"

            measurements = _request_permit_before_audit(
                dynamodb, rest_api, "PutItem", payload, "content-length",
                expect_audit_serialization=False, return_measurements=True)
            request_memory = max(len(payload) * 2, measurements["dom_memory"])
            assert measurements["dom_memory"] > len(payload) * 2
            assert measurements["audit_decision_units"] == request_memory + 8000
            assert measurements["permit_units"] == request_memory + 8000


def test_role_filtered_request_releases_audit_memory(dynamodb, cql, rest_api, alternator_audit_enabled):
    with new_test_table(dynamodb, **HASH_ONLY_SCHEMA) as table:
        ks_name = f"alternator_{table.name}"
        cql.execute("UPDATE system.config SET value=%s WHERE name='audit_categories'", ("",))
        cql.execute("UPDATE system.config SET value=%s WHERE name='audit_keyspaces'", ("",))
        cql.execute("UPDATE system.config SET value=%s WHERE name='audit_tables'", ("",))
        _set_audit_rules(cql, [{
            "sinks": ["table"],
            "categories": ["DML"],
            "qualified_table_names": [f"{ks_name}.*"],
            "roles": ["role_that_does_not_match_the_test_client"],
        }])
        payload = json.dumps({
            "TableName": table.name,
            "Item": {"p": {"S": "pk"}, "padding": {"S": "x" * (256 * 1024)}},
        }, separators=(",", ":")).encode()

        measurements = _request_permit_before_audit(
            dynamodb, rest_api, "PutItem", payload, "content-length", expect_audit_serialization=False,
            return_measurements=True)
        request_memory = max(len(payload) * 2, measurements["dom_memory"])
        assert measurements["audit_decision_units"] == request_memory + 8000
        assert measurements["permit_units"] == request_memory + 8000


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


# Test auditing of the QUERY vector-search operation: SearchVectors.
# Unlike GetItem/Query/Scan, SearchVectors has no ConsistentRead parameter -
# it always goes through the (eventually-consistent) vector store, so it is
# always audited with consistency LOCAL_ONE.
# This test doesn't need a working vector store: maybe_audit() runs before
# SearchVectors reaches the vector store, so the audit entry is produced
# regardless of whether the search itself succeeds or fails (e.g., with
# "Vector Store is disabled" if none is configured) - hence we don't check
# the error(bool) field here, unlike the other audit tests above.
def test_audit_search_vectors(dynamodb, cql, alternator_audit_enabled, need_vector_search_in_botocore):
    with new_test_table(dynamodb,
            KeySchema=[{"AttributeName": "p", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "p", "AttributeType": "S"}],
            VectorIndexes=[{
                "IndexName": "vind",
                "VectorAttribute": {"AttributeName": "v"},
                "Dimensions": 3,
                "DistanceFunction": "COSINE",
                "Projection": {"ProjectionType": "KEYS_ONLY"},
            }]) as table:
        ks_name = f"alternator_{table.name}"
        # Enable audit for the current table's keyspace. The `alternator_audit_enabled` fixture
        # ensures that `audit_keyspaces` in system.config has been already stored too and will be
        # restored after the test.
        cql.execute("UPDATE system.config SET value=%s WHERE name='audit_keyspaces'", (ks_name,))
        before_rows = _get_audit_log_rows(cql)
        try:
            table.meta.client.search_vectors(
                TableName=table.name, IndexName="vind", SearchVector=[1, 0, 0], TopK=1)
        except ClientError:
            pass
        new_rows = _get_new_audit_log_rows(cql, before_rows, expected_new_row_count=1)
        assert len(new_rows) == 1
        row = new_rows[0]
        assert row.category == "QUERY"
        assert row.consistency == "LOCAL_ONE"
        assert row.keyspace_name == ks_name
        assert row.table_name == table.name
        assert "SearchVectors" in row.operation
        assert "vind" in row.operation


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


# Test that BatchWriteItem respects audit_tables filtering.
# When audit_tables is set to a specific table, batch operations should only
# log entries for that table. A batch touching only non-audited tables should
# produce no audit entry at all. A batch touching both audited and non-audited
# tables should only include the audited table in the log entry.
def test_audit_batch_write_item_respects_table_filter(dynamodb, cql, alternator_audit_enabled):
    with new_test_table(dynamodb, **HASH_ONLY_SCHEMA) as table_a:
        with new_test_table(dynamodb, **HASH_ONLY_SCHEMA) as table_b:
            ks_a = f"alternator_{table_a.name}"
            # Only audit table_a via audit_tables.
            cql.execute("UPDATE system.config SET value=%s WHERE name='audit_tables'",
                        (f"alternator.{table_a.name}",))
            cql.execute("UPDATE system.config SET value=%s WHERE name='audit_keyspaces'", ("",))
            client = table_a.meta.client

            # --- Batch 1: only table_a (audited) ---
            before_rows = _get_audit_log_rows(cql)
            client.batch_write_item(RequestItems={
                table_a.name: [{"PutRequest": {"Item": {"p": "pk_a"}}}]
            })
            new_rows = _get_new_audit_log_rows(cql, before_rows, expected_new_row_count=1)
            _assert_audit_entries(new_rows, [
                ("DML", "LOCAL_QUORUM", False, "", table_a.name,
                 ["BatchWriteItem", "pk_a"]),
            ], table_name=table_a.name)

            # --- Batch 2: only table_b (not audited) ---
            # Send a fence PutItem to table_a right after so we can wait for
            # a known entry and then safely assume table_b produced nothing.
            before_rows = _get_audit_log_rows(cql)
            client.batch_write_item(RequestItems={
                table_b.name: [{"PutRequest": {"Item": {"p": "pk_b"}}}]
            })
            table_a.put_item(Item={"p": "pk_fence"})
            new_rows = _get_new_audit_log_rows(cql, before_rows, expected_new_row_count=1)
            _assert_audit_entries(new_rows, [
                ("DML", "LOCAL_QUORUM", False, ks_a, table_a.name,
                 ["PutItem", "pk_fence"]),
            ], ks_name=ks_a, table_name=table_a.name)
            _assert_no_audit_entries_for(new_rows, table_name=table_b.name, category="DML")

            # --- Batch 3: both tables (only table_a should appear) ---
            before_rows = _get_audit_log_rows(cql)
            client.batch_write_item(RequestItems={
                table_a.name: [{"PutRequest": {"Item": {"p": "pk_a"}}}],
                table_b.name: [{"PutRequest": {"Item": {"p": "pk_b"}}}],
            })
            new_rows = _get_new_audit_log_rows(cql, before_rows, expected_new_row_count=1)
            _assert_audit_entries(new_rows, [
                ("DML", "LOCAL_QUORUM", False, "", table_a.name,
                 ["BatchWriteItem", "pk_a"]),
            ], table_name=table_a.name)
            _assert_no_audit_entries_for(new_rows, table_name=table_b.name, category="DML")
            _assert_audit_operation_excludes(new_rows[0], [table_b.name, "pk_b"])


# Test that BatchGetItem respects audit_tables filtering.
# This mirrors test_audit_batch_write_item_respects_table_filter for QUERY
# batches: only audited tables should appear in the audit row, and the JSON
# request stored in the operation field should not contain non-audited tables.
def test_audit_batch_get_item_respects_table_filter(dynamodb, cql, alternator_audit_enabled):
    with new_test_table(dynamodb, **HASH_ONLY_SCHEMA) as table_a:
        with new_test_table(dynamodb, **HASH_ONLY_SCHEMA) as table_b:
            table_a.put_item(Item={"p": "pk_a"})
            table_b.put_item(Item={"p": "pk_b"})

            ks_a = f"alternator_{table_a.name}"
            # Only audit table_a via audit_tables.
            cql.execute("UPDATE system.config SET value=%s WHERE name='audit_tables'",
                        (f"alternator.{table_a.name}",))
            cql.execute("UPDATE system.config SET value=%s WHERE name='audit_keyspaces'", ("",))
            client = table_a.meta.client

            # --- Batch 1: only table_a (audited) ---
            before_rows = _get_audit_log_rows(cql)
            client.batch_get_item(RequestItems={
                table_a.name: {"Keys": [{"p": "pk_a"}]}
            })
            new_rows = _get_new_audit_log_rows(cql, before_rows, expected_new_row_count=1)
            _assert_audit_entries(new_rows, [
                ("QUERY", "ANY", False, "", table_a.name,
                 ["BatchGetItem", "pk_a"]),
            ], table_name=table_a.name)

            # --- Batch 2: only table_b (not audited) ---
            # Send a fence GetItem to table_a right after so we can wait for
            # a known entry and then safely assume table_b produced nothing.
            before_rows = _get_audit_log_rows(cql)
            client.batch_get_item(RequestItems={
                table_b.name: {"Keys": [{"p": "pk_b"}]}
            })
            table_a.get_item(Key={"p": "pk_a"})
            new_rows = _get_new_audit_log_rows(cql, before_rows, expected_new_row_count=1)
            _assert_audit_entries(new_rows, [
                ("QUERY", "LOCAL_ONE", False, ks_a, table_a.name,
                 ["GetItem", "pk_a"]),
            ], ks_name=ks_a, table_name=table_a.name)
            _assert_no_audit_entries_for(new_rows, table_name=table_b.name, category="QUERY")

            # --- Batch 3: both tables (only table_a should appear) ---
            before_rows = _get_audit_log_rows(cql)
            client.batch_get_item(RequestItems={
                table_a.name: {"Keys": [{"p": "pk_a"}]},
                table_b.name: {"Keys": [{"p": "pk_b"}]},
            })
            new_rows = _get_new_audit_log_rows(cql, before_rows, expected_new_row_count=1)
            _assert_audit_entries(new_rows, [
                ("QUERY", "ANY", False, "", table_a.name,
                 ["BatchGetItem", "pk_a"]),
            ], table_name=table_a.name)
            _assert_no_audit_entries_for(new_rows, table_name=table_b.name, category="QUERY")
            _assert_audit_operation_excludes(new_rows[0], [table_b.name, "pk_b"])


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
# DescribeTimeToLive, DescribeContinuousBackups, ExportTableToPointInTime,
# ListTables, DescribeEndpoints.
# ListTables and DescribeEndpoints have empty keyspace/table.
# Produces 7 audit entries.
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
        # ExportTableToPointInTime
        client.export_table_to_point_in_time(TableArn=table_arn, S3Bucket="my-bucket")
        expected.append(("QUERY", "", False, ks_name, table.name, ["ExportTableToPointInTime", table_arn, "my-bucket"]))
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
            table_a.put_item(Item={"p": "canary_a"})
            expected = [("DML", "LOCAL_QUORUM", False, ks_a, table_a.name, ["PutItem", "canary_a"])]
            new_rows = _get_new_audit_log_rows(cql, before_rows, expected_new_row_count=1)
            _assert_audit_entries(new_rows, expected, ks_a, table_a.name)
            _assert_no_audit_entries_for(new_rows, ks_name=ks_b)
            with pytest.raises(AssertionError):
                _assert_no_audit_entries_for(new_rows, ks_name=ks_a)  # sanity check


# Test that failed operations generate audit entries with error=True.
# Alternator reports errors in two ways: by throwing an exception, and by
# returning api_error in request_return_type. GetItem with an extra bogus key
# attribute covers the throwing path after audit_info is set. PutItem with an
# unmet condition covers the returned api_error path. A normal GetItem follows
# as the positive canary (error=False). All entries should be present.
def test_audit_error_entry(dynamodb, cql, alternator_audit_enabled):
    with new_test_table(dynamodb, **HASH_ONLY_SCHEMA) as table:
        ks_name = f"alternator_{table.name}"
        # Insert data so the GetItems have something to return.
        table.put_item(Item={"p": "pk_0"})
        table.put_item(Item={"p": "canary_0"})
        cql.execute("UPDATE system.config SET value=%s WHERE name='audit_keyspaces'", (ks_name,))
        before_rows = _get_audit_log_rows(cql)
        # Negative operation: GetItem with an extra key attribute beyond the schema.
        # The table has only "p" as the hash key, so passing "bogus" triggers check_key()
        # which throws api_error::validation after audit_info is already set.
        with pytest.raises(ClientError, match='ValidationException'):
            table.get_item(Key={"p": "pk_0", "bogus": "junk"})
        # Negative operation: PutItem with an unmet condition. Conditional check
        # failures are returned as api_error in the normal request_return_type path.
        with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
            table.put_item(Item={"p": "pk_0", "v": "new"}, ConditionExpression="attribute_not_exists(p)")
        # Positive operation: normal GetItem — should succeed and produce error=False entry.
        table.get_item(Key={"p": "canary_0"})
        expected = [
            ("QUERY", "LOCAL_ONE", True, ks_name, table.name, ["GetItem", "pk_0", "bogus"]),
            ("DML", "LOCAL_QUORUM", True, ks_name, table.name, ["PutItem", "pk_0", "attribute_not_exists"]),
            ("QUERY", "LOCAL_ONE", False, ks_name, table.name, ["GetItem", "canary_0"]),
        ]
        new_rows = _get_new_audit_log_rows(cql, before_rows, expected_new_row_count=3)
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
            table_a.put_item(Item={"p": "canary_a"})
            expected = [("DML", "LOCAL_QUORUM", False, ks_a, table_a.name, ["PutItem", "canary_a"])]
            new_rows = _get_new_audit_log_rows(cql, before_rows, expected_new_row_count=1)
            _assert_audit_entries(new_rows, expected, ks_a, table_a.name)
            _assert_no_audit_entries_for(new_rows, ks_name=ks_b)
            with pytest.raises(AssertionError):
                _assert_no_audit_entries_for(new_rows, ks_name=ks_a)  # sanity check


# Verify that single-table operations respect audit_rules filtering:
# only operations on a table matched by a rule are logged.
def test_audit_rules_basic_filtering(dynamodb, cql, alternator_audit_enabled):
    with new_test_table(dynamodb, **HASH_ONLY_SCHEMA) as table_a:
        with new_test_table(dynamodb, **HASH_ONLY_SCHEMA) as table_b:
            ks_a = f"alternator_{table_a.name}"
            ks_b = f"alternator_{table_b.name}"
            # Clear legacy config so only audit_rules controls auditing.
            cql.execute("UPDATE system.config SET value=%s WHERE name='audit_categories'", ("",))
            cql.execute("UPDATE system.config SET value=%s WHERE name='audit_keyspaces'", ("",))
            _set_audit_rules(cql, [{"sinks": ["table"], "categories": ["DML", "QUERY"],
                                    "qualified_table_names": [f"{ks_a}.*"], "roles": ["*"]}])
            before_rows = _get_audit_log_rows(cql)

            # Matched by the rule — should be logged.
            table_a.put_item(Item={"p": "pk_a"})
            table_a.get_item(Key={"p": "pk_a"})
            # Not matched by the rule — should NOT be logged.
            table_b.put_item(Item={"p": "pk_b"})
            table_b.get_item(Key={"p": "pk_b"})

            expected = [
                ("DML", "LOCAL_QUORUM", False, ks_a, table_a.name, ["PutItem", "pk_a"]),
                ("QUERY", "LOCAL_ONE", False, ks_a, table_a.name, ["GetItem", "pk_a"]),
            ]
            new_rows = _get_new_audit_log_rows(cql, before_rows, expected_new_row_count=len(expected))
            _assert_audit_entries(new_rows, expected, ks_a, table_a.name)
            _assert_no_audit_entries_for(new_rows, ks_name=ks_b)


# Verify that Alternator's cross-table batch operations respect audit_rules filtering:
# only matching tables are recorded, and non-matching table data is not
# leaked into the audit operation text.
def test_audit_rules_batch_filtering(dynamodb, cql, alternator_audit_enabled):
    with new_test_table(dynamodb, **HASH_ONLY_SCHEMA) as table_a:
        with new_test_table(dynamodb, **HASH_ONLY_SCHEMA) as table_b:
            ks_a = f"alternator_{table_a.name}"
            client = table_a.meta.client
            # Clear legacy config so only audit_rules controls auditing.
            cql.execute("UPDATE system.config SET value=%s WHERE name='audit_categories'", ("",))
            cql.execute("UPDATE system.config SET value=%s WHERE name='audit_keyspaces'", ("",))
            _set_audit_rules(cql, [{"sinks": ["table"], "categories": ["DML", "QUERY"],
                                    "qualified_table_names": [f"{ks_a}.*"], "roles": ["*"]}])
            before_rows = _get_audit_log_rows(cql)

            # Batch spanning two tables. The rule matches only table_a, so the
            # audit row should include only table_a and its request body.
            client.batch_write_item(RequestItems={
                table_a.name: [{"PutRequest": {"Item": {"p": "batch_a"}}}],
                table_b.name: [{"PutRequest": {"Item": {"p": "batch_b"}}}],
            })

            expected = [
                ("DML", "LOCAL_QUORUM", False, "", table_a.name, ["BatchWriteItem", "batch_a"]),
            ]
            new_rows = _get_new_audit_log_rows(cql, before_rows, expected_new_row_count=len(expected))
            _assert_audit_entries(new_rows, expected)
            _assert_no_audit_entries_for(new_rows, table_name=table_b.name, category="DML")
            _assert_audit_operation_excludes(new_rows[0], [table_b.name, "batch_b"])

            # Batch touching only non-matching tables should not produce a batch
            # audit entry. Use a matching PutItem canary so the test can wait for
            # a known audit row before checking the negative case.
            before_rows = _get_audit_log_rows(cql)
            client.batch_write_item(RequestItems={
                table_b.name: [{"PutRequest": {"Item": {"p": "batch_b_only"}}}],
            })
            table_a.put_item(Item={"p": "canary_a"})

            expected = [
                ("DML", "LOCAL_QUORUM", False, ks_a, table_a.name, ["PutItem", "canary_a"]),
            ]
            new_rows = _get_new_audit_log_rows(cql, before_rows, expected_new_row_count=len(expected))
            _assert_audit_entries(new_rows, expected, ks_name=ks_a, table_name=table_a.name)
            _assert_no_audit_entries_for(new_rows, table_name=table_b.name, category="DML")
            for row in new_rows:
                _assert_audit_operation_excludes(row, [table_b.name, "batch_b_only"])
