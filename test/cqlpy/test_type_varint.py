# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Tests involving the "varint" column type, focusing on non-canonical
# byte representations.
#
# The CQL varint type uses signed big-endian two's complement encoding.
# The canonical form is minimal: no redundant leading 0x00 (for non-negative)
# or 0xFF (for negative) bytes.  However, non-canonical representations with
# extra leading sign-extension bytes (e.g., 0x00 0x01 instead of 0x01 for
# the value 1) are accepted by both Scylla and Cassandra and can be produced
# by drivers via prepared-statement bind variables.
#
# The key issue is the same as for decimal (see test_type_decimal.py):
# partition keys are identified by raw byte representation (fed into the
# partitioner hash), while clustering keys are compared by semantic value.
# Two non-canonical representations of the same varint value will hash to
# different partitions but compare as equal in clustering order.
#############################################################################

import cassandra.cqltypes
import pytest

from .util import new_test_table, unique_key_int


class NonCanonicalVarint:
    """Wrapper around an int that causes the patched driver serializer to
    produce a non-canonical (padded) varint encoding.

    For non-negative values, one extra leading 0x00 byte is prepended.
    For negative values, one extra leading 0xFF byte is prepended.
    """
    def __init__(self, value):
        self.value = value


# Original serializer, saved once at import time.
_orig_serialize = cassandra.cqltypes.IntegerType.serialize


def _patched_serialize(val, protocol_version):
    if isinstance(val, NonCanonicalVarint):
        canonical = _orig_serialize(val.value, protocol_version)
        pad = b'\xff' if val.value < 0 else b'\x00'
        return pad + canonical
    return _orig_serialize(val, protocol_version)


@pytest.fixture(autouse=True)
def _patch_varint_serializer():
    cassandra.cqltypes.IntegerType.serialize = staticmethod(_patched_serialize)
    yield
    cassandra.cqltypes.IntegerType.serialize = _orig_serialize


# Verify that the same varint value with canonical vs non-canonical byte
# encoding behaves differently as a partition key (byte-based identity)
# but identically as a clustering key (value-based comparison).
# This mirrors test_decimal_key_representation in test_type_decimal.py.
# We test both positive (sign-extension 0x00) and negative (0xFF) values.
def test_varint_key_representation(cql, test_keyspace):
    with new_test_table(cql, test_keyspace,
            "p varint, c varint, v text, PRIMARY KEY (p, c)") as table:
        stmt = cql.prepare(
            f"INSERT INTO {table} (p, c, v) VALUES (?, ?, ?)")

        for val, label in [(1, 'pos'), (-1, 'neg')]:
            canonical = val
            non_canonical = NonCanonicalVarint(val)

            # Insert with canonical PK and CK.
            cql.execute(stmt, [canonical, canonical, f'{label}_a'])
            # Same canonical PK, non-canonical CK — same CK *value*, so
            # this overwrites the previous row (CK comparison is value-based).
            cql.execute(stmt, [canonical, non_canonical, f'{label}_b'])
            # Non-canonical PK — different partition entirely, despite
            # the same numeric PK value (PK identity is byte-based).
            cql.execute(stmt, [non_canonical, canonical, f'{label}_c'])

            # Canonical partition: 1 row (overwritten to _b).
            rows = list(cql.execute(
                cql.prepare(f"SELECT v FROM {table} WHERE p = ?"),
                [canonical]))
            assert len(rows) == 1
            assert rows[0].v == f'{label}_b'

            # Non-canonical partition: 1 row.
            rows_nc = list(cql.execute(
                cql.prepare(f"SELECT v FROM {table} WHERE p = ?"),
                [non_canonical]))
            assert len(rows_nc) == 1
            assert rows_nc[0].v == f'{label}_c'

            # Cross-representation CK lookup: non-canonical CK in
            # canonical partition should find the row.
            rows_cross = list(cql.execute(
                cql.prepare(
                    f"SELECT v FROM {table} WHERE p = ? AND c = ?"),
                [canonical, non_canonical]))
            assert len(rows_cross) == 1
            assert rows_cross[0].v == f'{label}_b'
