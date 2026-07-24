# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Tests for INSERT INTO <target> [(cols...)] SELECT ... FROM <source>
#
# INSERT ... SELECT copies the result set of an inner SELECT into a target
# table. It is a distributed, paged copy: it is NOT atomic and NOT idempotent
# across coordinator failure. These tests cover the user-visible semantics and
# the validation rules enforced at prepare time.
#############################################################################

import pytest
from cassandra.protocol import InvalidRequest
from .util import unique_name, unique_key_int, new_test_table, new_cql


@pytest.fixture(scope="module")
def src_table(cql, test_keyspace):
    schema = 'p int, c int, v text, w int, PRIMARY KEY (p, c)'
    with new_test_table(cql, test_keyspace, schema) as table:
        yield table


def _rows(cql, table):
    return sorted(list(cql.execute(f"SELECT p, c, v, w FROM {table}")))


# --- Basic copy: matching schema, no explicit column list ------------------

def test_copy_all_columns_by_name(cql, test_keyspace, src_table):
    cql.execute(f"INSERT INTO {src_table}(p,c,v,w) VALUES (1, 10, 'a', 100)")
    cql.execute(f"INSERT INTO {src_table}(p,c,v,w) VALUES (1, 20, 'b', 200)")
    cql.execute(f"INSERT INTO {src_table}(p,c,v,w) VALUES (2, 30, 'c', 300)")
    schema = 'p int, c int, v text, w int, PRIMARY KEY (p, c)'
    with new_test_table(cql, test_keyspace, schema) as dst:
        cql.execute(f"INSERT INTO {dst} SELECT * FROM {src_table}")
        assert _rows(cql, dst) == _rows(cql, src_table)


# --- Range-parallel full-table copy ----------------------------------------

def test_full_table_copy_is_complete_and_deduplicated(cql, test_keyspace):
    # A whole-table INSERT...SELECT (no WHERE) takes the range-parallel path:
    # the token ring is split into several disjoint sub-ranges scanned
    # concurrently. This verifies that the union is an exact copy -- every row
    # present once, none missing and none duplicated -- across enough distinct
    # partitions to span multiple sub-ranges of the ring. Uses dedicated source
    # and destination tables so the row set is known exactly (the shared
    # src_table fixture is mutated by other tests).
    n = 500
    schema = 'p int, c int, v text, w int, PRIMARY KEY (p, c)'
    with new_test_table(cql, test_keyspace, schema) as src, \
         new_test_table(cql, test_keyspace, schema) as dst:
        for p in range(n):
            cql.execute(f"INSERT INTO {src}(p,c,v,w) VALUES ({p}, {p % 7}, 'x{p}', {p * 2})")
        cql.execute(f"INSERT INTO {dst} SELECT * FROM {src}")
        dstrows = _rows(cql, dst)
        assert len(dstrows) == n
        assert dstrows == _rows(cql, src)


# --- Unqualified table names under a default (USE) keyspace ----------------

def test_unqualified_names_with_use_keyspace(cql, test_keyspace, src_table):
    # With "USE <ks>", both the target and the *inner* SELECT's source are named
    # without a keyspace; their keyspace must be resolved from the connection.
    # Regression: the source table's keyspace was previously left unresolved,
    # tripping a has_keyspace() assertion at prepare time (node abort).
    cql.execute(f"INSERT INTO {src_table}(p,c,v,w) VALUES (90, 900, 'u', 9000)")
    src_unqualified = src_table.split('.')[1]
    schema = 'p int, c int, v text, w int, PRIMARY KEY (p, c)'
    with new_test_table(cql, test_keyspace, schema) as dst:
        dst_unqualified = dst.split('.')[1]
        with new_cql(cql) as ncql:
            ncql.execute(f"USE {test_keyspace}")
            ncql.execute(f"INSERT INTO {dst_unqualified} SELECT * FROM {src_unqualified} WHERE p=90")
        assert list(cql.execute(f"SELECT p, c, v, w FROM {dst} WHERE p=90")) == [(90, 900, 'u', 9000)]


# --- Explicit target column list, reordering & projection ------------------

def test_copy_with_explicit_columns_and_projection(cql, test_keyspace, src_table):
    cql.execute(f"INSERT INTO {src_table}(p,c,v,w) VALUES (5, 50, 'e', 500)")
    # Project only the key columns and v into a table without w.
    schema = 'p int, c int, v text, PRIMARY KEY (p, c)'
    with new_test_table(cql, test_keyspace, schema) as dst:
        cql.execute(f"INSERT INTO {dst} (p, c, v) SELECT p, c, v FROM {src_table} WHERE p=5")
        assert list(cql.execute(f"SELECT p, c, v FROM {dst} WHERE p=5")) == [(5, 50, 'e')]


def test_copy_remaps_columns_positionally(cql, test_keyspace, src_table):
    # The target column list maps positionally onto the selected columns, so we
    # can route source.w into target.v2 etc.
    cql.execute(f"INSERT INTO {src_table}(p,c,v,w) VALUES (7, 70, 'g', 700)")
    schema = 'p int, c int, v2 int, PRIMARY KEY (p, c)'
    with new_test_table(cql, test_keyspace, schema) as dst:
        cql.execute(f"INSERT INTO {dst} (p, c, v2) SELECT p, c, w FROM {src_table} WHERE p=7")
        assert list(cql.execute(f"SELECT p, c, v2 FROM {dst} WHERE p=7")) == [(7, 70, 700)]


# --- USING TIMESTAMP / TTL propagation -------------------------------------

def test_using_timestamp_applies_to_copied_rows(cql, test_keyspace, src_table):
    cql.execute(f"INSERT INTO {src_table}(p,c,v,w) VALUES (8, 80, 'h', 800)")
    schema = 'p int, c int, v text, w int, PRIMARY KEY (p, c)'
    with new_test_table(cql, test_keyspace, schema) as dst:
        cql.execute(f"INSERT INTO {dst} SELECT * FROM {src_table} WHERE p=8 USING TIMESTAMP 12345")
        ts = list(cql.execute(f"SELECT writetime(v) FROM {dst} WHERE p=8 AND c=80"))
        assert ts == [(12345,)]


# --- Validation errors (enforced at prepare time) --------------------------

def test_missing_partition_key_column_is_rejected(cql, test_keyspace, src_table):
    schema = 'p int, c int, v text, PRIMARY KEY (p, c)'
    with new_test_table(cql, test_keyspace, schema) as dst:
        # SELECT omits the clustering key c, which the target requires.
        with pytest.raises(InvalidRequest, match="clustering-key|missing"):
            cql.execute(f"INSERT INTO {dst} (p, v) SELECT p, v FROM {src_table}")


def test_column_count_mismatch_is_rejected(cql, test_keyspace, src_table):
    schema = 'p int, c int, v text, PRIMARY KEY (p, c)'
    with new_test_table(cql, test_keyspace, schema) as dst:
        with pytest.raises(InvalidRequest, match="count|match"):
            cql.execute(f"INSERT INTO {dst} (p, c, v) SELECT p, c FROM {src_table}")


def test_type_mismatch_is_rejected(cql, test_keyspace, src_table):
    # target v is text; selecting an int into it must fail.
    schema = 'p int, c int, v text, PRIMARY KEY (p, c)'
    with new_test_table(cql, test_keyspace, schema) as dst:
        with pytest.raises(InvalidRequest, match="type|assign"):
            cql.execute(f"INSERT INTO {dst} (p, c, v) SELECT p, c, w FROM {src_table}")


def test_unknown_target_column_is_rejected(cql, test_keyspace, src_table):
    schema = 'p int, c int, v text, PRIMARY KEY (p, c)'
    with new_test_table(cql, test_keyspace, schema) as dst:
        with pytest.raises(InvalidRequest, match="[Uu]nknown"):
            cql.execute(f"INSERT INTO {dst} (p, c, nosuchcol) SELECT p, c, v FROM {src_table}")


def test_duplicate_target_column_is_rejected(cql, test_keyspace, src_table):
    schema = 'p int, c int, v text, PRIMARY KEY (p, c)'
    with new_test_table(cql, test_keyspace, schema) as dst:
        with pytest.raises(InvalidRequest, match="more than once|[Mm]ultiple"):
            cql.execute(f"INSERT INTO {dst} (p, c, v, v) SELECT p, c, v, v FROM {src_table}")


# --- Disallowed contexts ----------------------------------------------------

def test_insert_select_not_allowed_in_batch(cql, test_keyspace, src_table):
    schema = 'p int, c int, v text, w int, PRIMARY KEY (p, c)'
    with new_test_table(cql, test_keyspace, schema) as dst:
        with pytest.raises(InvalidRequest, match="BATCH"):
            cql.execute(
                "BEGIN BATCH "
                f"INSERT INTO {dst} SELECT * FROM {src_table}; "
                "APPLY BATCH")


# Counters are not supported with tablets, so use a vnodes keyspace to actually
# reach (and exercise) the INSERT...SELECT rejection path.
def test_insert_select_into_counter_is_rejected(cql, test_keyspace_vnodes):
    schema = 'p int PRIMARY KEY, c counter'
    with new_test_table(cql, test_keyspace_vnodes, schema) as counter_table:
        with pytest.raises(InvalidRequest, match="counter"):
            cql.execute(f"INSERT INTO {counter_table} SELECT * FROM {counter_table}")
