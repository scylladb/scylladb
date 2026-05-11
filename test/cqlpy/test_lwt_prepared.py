# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Tests for LWT (Light-Weight Transaction) prepared statements with
# parameter markers. These tests verify that IF conditions work correctly
# with bound parameters across all CQL types, collections, and nested
# collections.
#
# Ported from scylla-dtest/cql_prepared_test.py (SCYLLADB-1923).
#############################################################################

import uuid
from datetime import datetime
from decimal import Decimal

import pytest
from cassandra.util import Date, SortedSet, Time, uuid_from_time

from .util import new_test_table, is_scylla as _is_scylla


@pytest.fixture(scope="module")
def is_scylla(cql):
    yield _is_scylla(cql)


def _assert_lwt_applied(cql, stmt, is_scylla, expected_applied, expected_prev, **kwargs):
    """Execute a prepared LWT statement and assert the [applied] flag and
    previous-value column match expectations.

    ``expected_prev`` is the expected value of the *first non-[applied]* column
    in the result row (typically the previous value of the column under test).

    When ``expected_applied`` is True, Cassandra returns only ``[applied]``
    while Scylla also returns the previous value of the column.  The check
    on ``expected_prev`` is therefore skipped on Cassandra for successful CAS
    (see docs/kb/lwt-differences.rst).
    """
    rs = list(cql.execute(stmt, kwargs))
    assert len(rs) == 1
    assert rs[0][0] == expected_applied
    if expected_applied and not is_scylla:
        # Cassandra only returns [applied] when the CAS succeeds
        return
    assert rs[0][1] == expected_prev


# ---------------------------------------------------------------------------
# Primitive-type LWT prepared updates
# ---------------------------------------------------------------------------

def test_lwt_update_prepared(cql, test_keyspace, is_scylla):
    """Test prepared LWT UPDATE ... IF value=:v / IF value IN (:v) / IF value IN :v
    across all primitive CQL types with null/empty/zero/min/max boundary values.

    Ported from scylla-dtest cql_prepared_test.py::test_lwt_update_prepared.
    """
    # Test data for each primitive CQL type: a list of (init_value, update_value)
    # tuples plus IF condition patterns.
    primitive_types = {
        "boolean": {
            "cases": [(None, True), (False, True)],
        },
        "blob": {
            "cases": [(None, b"\x00\x00\x00\x01"), (b"", b"\x00\x00\x00\x01"),
                      (b"\x00\x00\x00\x00", b"\x00\x00\x00\x01")],
        },
        "ascii": {
            "cases": [(None, "def"), ("", "def"), ("abc", "def")],
        },
        "decimal": {
            "cases": [(None, Decimal("2.35")),
                      (Decimal("1.23"), Decimal("2.35"))],
        },
        "double": {
            "cases": [(None, 1.0), (0.0, 1.0),
                      (2.2250738585072014e-308, 1.0),
                      (1.7976931348623157e308, 1.0),
                      (-1.7976931348623157e308, 1.0)],
        },
        "float": {
            "cases": [(None, 1.0), (0.0, 1.0),
                      (1.1754943508222875e-38, 1.0),
                      (3.4028234663852886e38, 1.0),
                      (-3.4028234663852886e38, 1.0)],
        },
        "text": {
            "cases": [(None, "def"), ("", "def"), ("abc", "def")],
        },
        "varchar": {
            "cases": [(None, "def"), ("", "def"), ("abc", "def")],
        },
        "bigint": {
            "cases": [(None, 1), (0, 1), (2**63 - 1, 1), (-(2**63), 1)],
        },
        "int": {
            "cases": [(None, 1), (0, 1), (2**31 - 1, 1), (-(2**31), 1)],
        },
        "smallint": {
            "cases": [(None, 1), (0, 1), (2**15 - 1, 1), (-(2**15), 1)],
        },
        "tinyint": {
            "cases": [(None, 1), (0, 1), (2**7 - 1, 1), (-(2**7), 1)],
        },
        "varint": {
            "cases": [(None, 1), (0, 1), (2**128, 1), (-(2**128), 1)],
        },
        "timestamp": {
            "cases": [
                (None, datetime(2021, 2, 3, 15, 14, 13, 2000)),
                (datetime(1970, 1, 1, 0, 0), datetime(2021, 2, 3, 15, 14, 13, 2000)),
                (datetime(2020, 1, 2, 14, 13, 12, 1000), datetime(2021, 2, 3, 15, 14, 13, 2000)),
            ],
        },
        "date": {
            "cases": [(None, Date("2021-2-3")), (Date(0), Date("2021-2-3")),
                      (Date("2020-1-2"), Date("2021-2-3"))],
        },
        "time": {
            "cases": [(None, Time("13:14:15.002")), (Time(0), Time("13:14:15.002")),
                      (Time("12:13:14.001"), Time("13:14:15.002"))],
        },
        "timeuuid": {
            "cases": [
                (None, uuid_from_time(datetime(2021, 2, 3, 4, 5, 6, 1))),
                (uuid_from_time(datetime(2020, 1, 2, 3, 4, 5, 0)),
                 uuid_from_time(datetime(2021, 2, 3, 4, 5, 6, 1))),
            ],
        },
        "uuid": {
            "cases": [
                (None, uuid.UUID("12345678-1234-5678-1234-567812345678")),
                (uuid.UUID(bytes=b"\x00" * 16),
                 uuid.UUID("12345678-1234-5678-1234-567812345678")),
            ],
        },
    }
    # Build a single table with one column per type to avoid creating 18
    # separate tables (much faster).
    col_defs = ", ".join(f"val_{t} {t}" for t in primitive_types)
    with new_test_table(cql, test_keyspace,
                        f"k int PRIMARY KEY, {col_defs}") as table:
        row_key = 0
        for cql_type, type_info in primitive_types.items():
            col = f"val_{cql_type}"
            for init_val, upd_val in type_info["cases"]:
                # Insert initial row
                insert = cql.prepare(f"INSERT INTO {table} (k, {col}) VALUES (?, ?)")
                cql.execute(insert, [row_key, init_val])

                # Pattern 1: IF col=:v
                stmt = cql.prepare(f"UPDATE {table} SET {col}=:upd_v WHERE k=:k IF {col}=:v")
                _assert_lwt_applied(cql, stmt, is_scylla, True, init_val,
                                    upd_v=upd_val, k=row_key, v=init_val)
                _assert_lwt_applied(cql, stmt, is_scylla, False, upd_val,
                                    upd_v=upd_val, k=row_key, v=init_val)

                # Reset for next pattern
                cql.execute(insert, [row_key, init_val])

                # Pattern 2: IF col IN (:v)
                stmt2 = cql.prepare(f"UPDATE {table} SET {col}=:upd_v WHERE k=:k IF {col} IN (:v)")
                _assert_lwt_applied(cql, stmt2, is_scylla, True, init_val,
                                    upd_v=upd_val, k=row_key, v=init_val)
                _assert_lwt_applied(cql, stmt2, is_scylla, False, upd_val,
                                    upd_v=upd_val, k=row_key, v=init_val)

                # Reset
                cql.execute(insert, [row_key, init_val])

                # Pattern 3: IF col IN :v (with tuple parameter)
                stmt3 = cql.prepare(f"UPDATE {table} SET {col}=:upd_v WHERE k=:k IF {col} IN :v")
                if init_val is not None:
                    _assert_lwt_applied(cql, stmt3, is_scylla, True, init_val,
                                        upd_v=upd_val, k=row_key, v=(init_val,))
                    _assert_lwt_applied(cql, stmt3, is_scylla, False, upd_val,
                                        upd_v=upd_val, k=row_key, v=(init_val,))
                else:
                    # When init_val is None, pass a tuple containing None
                    _assert_lwt_applied(cql, stmt3, is_scylla, True, init_val,
                                        upd_v=upd_val, k=row_key, v=(None,))
                    _assert_lwt_applied(cql, stmt3, is_scylla, False, upd_val,
                                        upd_v=upd_val, k=row_key, v=(None,))

                row_key += 1


def _build_collection_typename(element_type, frozen, collection_type):
    name = f"{collection_type}<{element_type}>"
    if frozen:
        name = f"frozen<{name}>"
    return name


def test_lwt_update_prepared_collections(cql, test_keyspace, is_scylla):
    """Test prepared LWT UPDATE with IF conditions on list/set/tuple columns
    (frozen and non-frozen) with parameter markers.

    Ported from scylla-dtest cql_prepared_test.py::test_lwt_update_prepared_listlike_and_tuples.
    """
    # Subset of types to keep the collection test tractable while still covering
    # the key categories (numeric, string, blob, temporal, uuid).
    collection_element_types = {
        "int": {"cases": [(0, 1), (2**31 - 1, 1), (-(2**31), 1)]},
        "text": {"cases": [("", "def"), ("abc", "def")]},
        "boolean": {"cases": [(False, True)]},
        "blob": {"cases": [(b"", b"\x01"), (b"\x00\x00", b"\x01")]},
        "bigint": {"cases": [(0, 1), (2**63 - 1, 1)]},
        "double": {"cases": [(0.0, 1.0)]},
        "timestamp": {
            "cases": [(datetime(1970, 1, 1), datetime(2021, 2, 3, 15, 14, 13, 2000))],
        },
        "uuid": {
            "cases": [(uuid.UUID(bytes=b"\x00" * 16),
                       uuid.UUID("12345678-1234-5678-1234-567812345678"))],
        },
    }
    # Build one table per (collection_type, frozen) combination, with one
    # column per element type. This avoids creating a separate table for each
    # (collection_type, frozen, element_type) triple.
    for collection_type in ("list", "set", "tuple"):
        for is_frozen in (False, True):
            col_defs = ", ".join(
                f"val_{et} {_build_collection_typename(et, is_frozen, collection_type)}"
                for et in collection_element_types)
            with new_test_table(cql, test_keyspace,
                                f"k int PRIMARY KEY, {col_defs}") as table:
                row_key = 0
                for elem_type, type_info in collection_element_types.items():
                    col = f"val_{elem_type}"
                    for elem_init, elem_upd in type_info["cases"]:
                        init_val = [elem_init]
                        upd_val = [elem_upd]
                        if collection_type == "tuple":
                            init_val = tuple(init_val)
                            upd_val = tuple(upd_val)

                        insert = cql.prepare(f"INSERT INTO {table} (k, {col}) VALUES (?, ?)")
                        cql.execute(insert, [row_key, init_val])

                        # IF col=:v
                        stmt = cql.prepare(f"UPDATE {table} SET {col}=:upd_v WHERE k=:k IF {col}=:v")
                        _assert_lwt_applied(cql, stmt, is_scylla, True, init_val,
                                            upd_v=upd_val, k=row_key, v=init_val)
                        _assert_lwt_applied(cql, stmt, is_scylla, False, upd_val,
                                            upd_v=upd_val, k=row_key, v=init_val)

                        # Reset
                        cql.execute(insert, [row_key, init_val])

                        # IF col IN (:v)
                        stmt2 = cql.prepare(f"UPDATE {table} SET {col}=:upd_v WHERE k=:k IF {col} IN (:v)")
                        _assert_lwt_applied(cql, stmt2, is_scylla, True, init_val,
                                            upd_v=upd_val, k=row_key, v=init_val)
                        _assert_lwt_applied(cql, stmt2, is_scylla, False, upd_val,
                                            upd_v=upd_val, k=row_key, v=init_val)

                        # Reset
                        cql.execute(insert, [row_key, init_val])

                        # IF col IN :v (tuple of values)
                        stmt3 = cql.prepare(f"UPDATE {table} SET {col}=:upd_v WHERE k=:k IF {col} IN :v")
                        _assert_lwt_applied(cql, stmt3, is_scylla, True, init_val,
                                            upd_v=upd_val, k=row_key, v=[init_val])
                        _assert_lwt_applied(cql, stmt3, is_scylla, False, upd_val,
                                            upd_v=upd_val, k=row_key, v=[init_val])

                        # Reset
                        cql.execute(insert, [row_key, init_val])

                        # List-only: IF col[:i]=:v, IF col[:i] IN (:v), IF col[:i] IN :v
                        if collection_type == "list":
                            stmt4 = cql.prepare(f"UPDATE {table} SET {col}=:upd_v WHERE k=:k IF {col}[:i]=:v")
                            _assert_lwt_applied(cql, stmt4, is_scylla, True, init_val,
                                                upd_v=upd_val, k=row_key, i=0, v=elem_init)
                            _assert_lwt_applied(cql, stmt4, is_scylla, False, upd_val,
                                                upd_v=upd_val, k=row_key, i=0, v=elem_init)

                            cql.execute(insert, [row_key, init_val])

                            stmt5 = cql.prepare(f"UPDATE {table} SET {col}=:upd_v WHERE k=:k IF {col}[:i] IN (:v)")
                            _assert_lwt_applied(cql, stmt5, is_scylla, True, init_val,
                                                upd_v=upd_val, k=row_key, i=0, v=elem_init)
                            _assert_lwt_applied(cql, stmt5, is_scylla, False, upd_val,
                                                upd_v=upd_val, k=row_key, i=0, v=elem_init)

                            cql.execute(insert, [row_key, init_val])

                            stmt6 = cql.prepare(f"UPDATE {table} SET {col}=:upd_v WHERE k=:k IF {col}[:i] IN :v")
                            _assert_lwt_applied(cql, stmt6, is_scylla, True, init_val,
                                                upd_v=upd_val, k=row_key, i=0, v=[elem_init])
                            _assert_lwt_applied(cql, stmt6, is_scylla, False, upd_val,
                                                upd_v=upd_val, k=row_key, i=0, v=[elem_init])

                        row_key += 1


def test_lwt_compare_collection_with_null(cql, test_keyspace, is_scylla):
    """Test that comparing empty collection to null yields correct results
    in prepared LWT statements.

    Non-frozen empty collection should equal null; frozen empty collection
    should NOT equal null.

    Ported from scylla-dtest cql_prepared_test.py::test_lwt_compare_collection_with_null.
    Reproduces https://github.com/scylladb/scylladb/issues/5855.
    """
    with new_test_table(cql, test_keyspace,
                        "k int PRIMARY KEY, "
                        "lvalue list<boolean>, flvalue frozen<list<boolean>>, "
                        "svalue set<boolean>, fsvalue frozen<set<boolean>>, "
                        "mvalue map<boolean, boolean>, fmvalue frozen<map<boolean, boolean>>") as table:
        cql.execute(f"INSERT INTO {table} (k) VALUES (0)")

        # (column, frozen_column, empty_value, non_empty_value)
        test_cases = [
            ("lvalue", "flvalue", [], [False]),
            ("svalue", "fsvalue", [], [False]),
            ("mvalue", "fmvalue", {}, {False: True}),
        ]

        for col, fcol, empty, non_empty in test_cases:
            # Non-frozen: empty collection == null
            stmt = cql.prepare(
                f"UPDATE {table} SET {col}=:update_val WHERE k=0 IF {col}=:v")
            _assert_lwt_applied(cql, stmt, is_scylla, True, None,
                                update_val=non_empty, v=None)
            _assert_lwt_applied(cql, stmt, is_scylla, False, non_empty,
                                update_val=empty, v=empty)

            # Reset
            cql.execute(f"DELETE FROM {table} WHERE k=0")
            cql.execute(f"INSERT INTO {table} (k) VALUES (0)")

            # Frozen: empty collection != null
            fstmt = cql.prepare(
                f"UPDATE {table} SET {fcol}=:update_val WHERE k=0 IF {fcol}=:v")
            _assert_lwt_applied(cql, fstmt, is_scylla, False, None,
                                update_val=empty, v=empty)
            _assert_lwt_applied(cql, fstmt, is_scylla, True, None,
                                update_val=non_empty, v=None)
            _assert_lwt_applied(cql, fstmt, is_scylla, False, non_empty,
                                update_val=empty, v=empty)


def test_lwt_nested_collections_list_set(cql, test_keyspace, is_scylla):
    """Test prepared LWT with nested collection list<frozen<set<int>>>
    using =, >, >= comparisons with parameter markers.

    Ported from scylla-dtest cql_prepared_test.py::test_lwt_nested_collections_list_set.
    """
    with new_test_table(cql, test_keyspace,
                        "k int PRIMARY KEY, list_set list<frozen<set<int>>>") as table:
        insert = cql.prepare(f"INSERT INTO {table} (k, list_set) VALUES (1, ?)")
        cql.execute(insert, [[SortedSet([1, 2]), SortedSet([1, 2])]])

        # = comparison
        stmt = cql.prepare(f"UPDATE {table} SET list_set=:update_val WHERE k=1 IF list_set=:v")
        _assert_lwt_applied(cql, stmt, is_scylla, True,
                            [SortedSet([1, 2]), SortedSet([1, 2])],
                            update_val=[SortedSet([3, 4]), SortedSet([4, 5])],
                            v=[SortedSet([1, 2]), SortedSet([1, 2])])

        # > comparison
        stmt = cql.prepare(f"UPDATE {table} SET list_set=:update_val WHERE k=1 IF list_set > :v")
        _assert_lwt_applied(cql, stmt, is_scylla, True,
                            [SortedSet([3, 4]), SortedSet([4, 5])],
                            update_val=[SortedSet([3, 4, 5]), SortedSet([4, 5, 6])],
                            v=[SortedSet([3, 3]), SortedSet([4, 4])])

        # >= comparison (strictly greater)
        stmt = cql.prepare(f"UPDATE {table} SET list_set=:update_val WHERE k=1 IF list_set >= :v")
        _assert_lwt_applied(cql, stmt, is_scylla, True,
                            [SortedSet([3, 4, 5]), SortedSet([4, 5, 6])],
                            update_val=[SortedSet([5, 6, 7]), SortedSet([7, 8, 9])],
                            v=[SortedSet([3, 3]), SortedSet([5, 4])])

        # >= comparison (equal)
        stmt = cql.prepare(f"UPDATE {table} SET list_set=:update_val WHERE k=1 IF list_set >= :v")
        _assert_lwt_applied(cql, stmt, is_scylla, True,
                            [SortedSet([5, 6, 7]), SortedSet([7, 8, 9])],
                            update_val=[SortedSet([5, 6, 7]), SortedSet([7, 8, 9])],
                            v=[SortedSet([3, 4]), SortedSet([4, 5])])


def test_lwt_nested_collections_set_list(cql, test_keyspace, is_scylla):
    """Test prepared LWT with nested collection set<frozen<list<int>>>
    using =, >, >= comparisons with parameter markers.

    Ported from scylla-dtest cql_prepared_test.py::test_lwt_nested_collections_set_list.
    """
    with new_test_table(cql, test_keyspace,
                        "k int PRIMARY KEY, set_list set<frozen<list<int>>>") as table:
        insert = cql.prepare(f"INSERT INTO {table} (k, set_list) VALUES (1, ?)")
        # {[1,2], [1,2]} => set deduplicates to {[1,2]}
        cql.execute(insert, [SortedSet([[1, 2], [1, 2]])])

        # = comparison
        stmt = cql.prepare(f"UPDATE {table} SET set_list=:update_val WHERE k=1 IF set_list=:v")
        _assert_lwt_applied(cql, stmt, is_scylla, True,
                            SortedSet([[1, 2]]),
                            update_val=SortedSet([[3, 3], [4, 4]]),
                            v=SortedSet([[1, 2], [1, 2]]))

        # > comparison (should fail: {[3,3],[4,4]} is NOT > {[3,3],[4,4]})
        stmt = cql.prepare(f"UPDATE {table} SET set_list=:update_val WHERE k=1 IF set_list > :v")
        _assert_lwt_applied(cql, stmt, is_scylla, False,
                            SortedSet([[3, 3], [4, 4]]),
                            update_val=SortedSet([[5, 5, 5], [4, 4, 4]]),
                            v=SortedSet([[3, 3], [4, 4]]))

        # >= comparison (should succeed: {[3,3],[4,4]} >= {[3,3],[4,4]})
        stmt = cql.prepare(f"UPDATE {table} SET set_list=:update_val WHERE k=1 IF set_list >= :v")
        _assert_lwt_applied(cql, stmt, is_scylla, True,
                            SortedSet([[3, 3], [4, 4]]),
                            update_val=SortedSet([[5, 5, 5], [4, 4, 4]]),
                            v=SortedSet([[3, 3], [4, 4]]))
