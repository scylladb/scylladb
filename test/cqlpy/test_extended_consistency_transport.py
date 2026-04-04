# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

#############################################################################
# Tests for Phase 1 of the SCYLLA_EXTENDED_CONSISTENCY protocol extension:
# - Custom payload frame parsing in the transport layer
# - SCYLLA_EXTENDED_CONSISTENCY capability advertisement and negotiation
#
# These tests verify transport-level behavior only. No consistency semantics
# are tested here — payloads are parsed and ignored by the server in Phase 1.
#############################################################################

import pytest
from cassandra.query import SimpleStatement, BatchStatement, BatchType
from cassandra.protocol import ProtocolException
from .util import new_test_table


# ---------------------------------------------------------------------------
# Capability negotiation: SUPPORTED advertisement
# ---------------------------------------------------------------------------

def test_extended_consistency_advertised_in_supported(cql):
    """
    Server must advertise SCYLLA_EXTENDED_CONSISTENCY in its SUPPORTED response.
    The Python driver stores options received in SUPPORTED on the cluster object.
    """
    # The scylla python driver exposes server options as a dict on the cluster.
    # Try the known attribute locations in order of likelihood.
    supported = (
        getattr(cql.cluster, '_supported_options', None)
        or getattr(cql.cluster, 'server_supported_options', None)
        or {}
    )
    assert 'SCYLLA_EXTENDED_CONSISTENCY' in supported, (
        f"SCYLLA_EXTENDED_CONSISTENCY not found in SUPPORTED response. "
        f"Keys present: {sorted(supported.keys())}"
    )


# ---------------------------------------------------------------------------
# Custom payload on QUERY frames
# ---------------------------------------------------------------------------

def test_custom_payload_on_query_ignored_without_negotiation(cql):
    """
    A standard driver that never sent SCYLLA_EXTENDED_CONSISTENCY in STARTUP
    can still set the custom_payload frame flag. The server must parse the
    payload bytes without crashing and return a normal result.
    """
    stmt = SimpleStatement("SELECT key FROM system.local")
    stmt.custom_payload = {
        "scylla-extended-consistency": b'{"mode":"n_of_m_dcs","required_dcs":1}'
    }
    rows = list(cql.execute(stmt))
    assert len(rows) >= 1


def test_custom_payload_unknown_keys_ignored(cql):
    """
    Payload keys that Scylla does not recognise must be silently ignored.
    The query must succeed normally.
    """
    stmt = SimpleStatement("SELECT key FROM system.local")
    stmt.custom_payload = {
        "completely-unknown-key-xyz": b'\x00\x01\x02\x03',
        "another-unknown-key":        b'hello world',
    }
    rows = list(cql.execute(stmt))
    assert len(rows) >= 1


def test_custom_payload_multiple_entries(cql):
    """
    A [bytes map] with multiple entries (n > 1) must be parsed correctly.
    All entries must be consumed so the reader cursor lands at the query body.
    """
    stmt = SimpleStatement("SELECT key FROM system.local")
    stmt.custom_payload = {
        "scylla-extended-consistency": b'{"mode":"n_of_m_dcs","required_dcs":1}',
        "other-key-a":                 b'\xde\xad',
        "other-key-b":                 b'\xbe\xef',
    }
    rows = list(cql.execute(stmt))
    assert len(rows) >= 1


def test_custom_payload_empty_map(cql):
    """
    An empty [bytes map] (n=0) is valid per the native protocol spec.
    The server must handle it without error.
    """
    stmt = SimpleStatement("SELECT key FROM system.local")
    stmt.custom_payload = {}
    rows = list(cql.execute(stmt))
    assert len(rows) >= 1


def test_custom_payload_large_value(cql):
    """
    A payload value that is several kilobytes long must be read and
    discarded without error. This exercises the cross-fragment path
    in read_bytes_view when the value spans buffer boundaries.
    """
    stmt = SimpleStatement("SELECT key FROM system.local")
    stmt.custom_payload = {
        "scylla-extended-consistency": b'x' * 8192,
    }
    rows = list(cql.execute(stmt))
    assert len(rows) >= 1


def test_custom_payload_binary_value(cql):
    """
    Payload values are raw bytes and may contain arbitrary binary data
    including null bytes. The server must not treat them as strings.
    """
    stmt = SimpleStatement("SELECT key FROM system.local")
    stmt.custom_payload = {
        "scylla-extended-consistency": bytes(range(256)),
    }
    rows = list(cql.execute(stmt))
    assert len(rows) >= 1


def test_query_result_correct_with_payload(cql, test_keyspace):
    """
    The presence of a custom payload must not affect query results.
    The server must return the same rows regardless of whether a
    payload is attached.
    """
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, v int") as table:
        cql.execute(f"INSERT INTO {table} (p, v) VALUES (1, 100)")
        cql.execute(f"INSERT INTO {table} (p, v) VALUES (2, 200)")

        without_payload = list(cql.execute(f"SELECT p, v FROM {table}"))

        stmt = SimpleStatement(f"SELECT p, v FROM {table}")
        stmt.custom_payload = {
            "scylla-extended-consistency": b'{"mode":"n_of_m_dcs","required_dcs":1}'
        }
        with_payload = list(cql.execute(stmt))

        assert sorted(without_payload) == sorted(with_payload)


# ---------------------------------------------------------------------------
# Custom payload on EXECUTE frames (prepared statements)
# ---------------------------------------------------------------------------

def test_custom_payload_on_execute(cql):
    """
    Custom payload must be accepted on EXECUTE (prepared statement) frames.
    The payload precedes the bound statement body in the frame.
    """
    prepared = cql.prepare("SELECT key FROM system.local")
    bound = prepared.bind([])
    bound.custom_payload = {
        "scylla-extended-consistency": b'{"mode":"n_of_m_dcs","required_dcs":1}'
    }
    rows = list(cql.execute(bound))
    assert len(rows) >= 1


def test_custom_payload_on_execute_multiple_entries(cql):
    """
    EXECUTE frame with multiple payload entries must parse correctly
    and produce the right query result.
    """
    prepared = cql.prepare("SELECT key FROM system.local")
    bound = prepared.bind([])
    bound.custom_payload = {
        "scylla-extended-consistency": b'{"mode":"n_of_m_dcs","required_dcs":2}',
        "extra-key":                   b'\xff\xfe\xfd',
    }
    rows = list(cql.execute(bound))
    assert len(rows) >= 1


def test_execute_result_correct_with_payload(cql, test_keyspace):
    """
    Results from a prepared+bound statement must be identical whether or
    not a custom payload is attached.
    """
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, v int") as table:
        for i in range(5):
            cql.execute(f"INSERT INTO {table} (p, v) VALUES ({i}, {i * 10})")

        prepared = cql.prepare(f"SELECT p, v FROM {table} WHERE p = ?")

        for i in range(5):
            without_payload = list(cql.execute(prepared, [i]))

            bound = prepared.bind([i])
            bound.custom_payload = {
                "scylla-extended-consistency": b'{"mode":"n_of_m_dcs","required_dcs":1}'
            }
            with_payload = list(cql.execute(bound))

            assert without_payload == with_payload


# ---------------------------------------------------------------------------
# Custom payload on BATCH frames
# ---------------------------------------------------------------------------

def test_custom_payload_on_batch(cql, test_keyspace):
    """
    Custom payload must be accepted on BATCH frames. The payload precedes
    the batch body in the frame. The batch must execute normally.
    """
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, v int") as table:
        batch = BatchStatement(batch_type=BatchType.UNLOGGED)
        batch.add(SimpleStatement(f"INSERT INTO {table} (p, v) VALUES (1, 10)"))
        batch.add(SimpleStatement(f"INSERT INTO {table} (p, v) VALUES (2, 20)"))
        batch.custom_payload = {
            "scylla-extended-consistency": b'{"mode":"n_of_m_dcs","required_dcs":1}'
        }
        cql.execute(batch)

        rows = {(r.p, r.v) for r in cql.execute(f"SELECT p, v FROM {table}")}
        assert rows == {(1, 10), (2, 20)}


def test_custom_payload_on_batch_multiple_entries(cql, test_keyspace):
    """
    BATCH frame with multiple payload entries must parse and execute correctly.
    """
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, v int") as table:
        batch = BatchStatement(batch_type=BatchType.LOGGED)
        batch.add(SimpleStatement(f"INSERT INTO {table} (p, v) VALUES (42, 99)"))
        batch.custom_payload = {
            "scylla-extended-consistency": b'{"mode":"n_of_m_dcs","required_dcs":1}',
            "irrelevant-key":              b'\x00\x00',
        }
        cql.execute(batch)

        rows = list(cql.execute(f"SELECT p, v FROM {table} WHERE p = 42"))
        assert len(rows) == 1
        assert rows[0].v == 99


# ---------------------------------------------------------------------------
# Cursor integrity: payload bytes must not bleed into query body
# ---------------------------------------------------------------------------

def test_payload_cursor_does_not_corrupt_query_body(cql, test_keyspace):
    """
    After parsing the payload [bytes map], the frame cursor must sit exactly
    at the start of the query body. If even one byte bleeds over, the query
    string or its parameters will be misread, causing a parse error or wrong
    results. This test uses a non-trivial payload to make misalignment likely
    to corrupt the query string.
    """
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, v text") as table:
        cql.execute(f"INSERT INTO {table} (p, v) VALUES (7, 'hello')")

        stmt = SimpleStatement(f"SELECT p, v FROM {table} WHERE p = 7")
        stmt.custom_payload = {
            "key-one":   b'A' * 100,
            "key-two":   b'B' * 200,
            "key-three": b'C' * 50,
        }
        rows = list(cql.execute(stmt))
        assert len(rows) == 1
        assert rows[0].p == 7
        assert rows[0].v == 'hello'


def test_payload_cursor_integrity_prepared(cql, test_keyspace):
    """
    Same cursor integrity check for prepared (EXECUTE) frames.
    A payload with several entries must not corrupt the bound values.
    """
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, v int") as table:
        cql.execute(f"INSERT INTO {table} (p, v) VALUES (99, 12345)")

        prepared = cql.prepare(f"SELECT v FROM {table} WHERE p = ?")
        bound = prepared.bind([99])
        bound.custom_payload = {
            "key-a": b'\xab' * 300,
            "key-b": b'\xcd' * 150,
        }
        rows = list(cql.execute(bound))
        assert len(rows) == 1
        assert rows[0].v == 12345


# ---------------------------------------------------------------------------
# Protocol version guard: payload only valid on v4+
# ---------------------------------------------------------------------------

def test_custom_payload_requires_protocol_v4(cql):
    """
    Custom payloads are a protocol v4 feature. Confirm the connected session
    is using at least v4. If protocol v3 connections are ever needed in tests,
    they should not send custom payloads.
    """
    # The Python driver negotiates the highest mutually supported version.
    # Scylla supports v4, so this should always be >= 4 in CI.
    version = cql.cluster.protocol_version
    assert version >= 4, (
        f"Tests require protocol v4+, but connected with v{version}. "
        "Custom payload parsing only applies to v4+."
    )