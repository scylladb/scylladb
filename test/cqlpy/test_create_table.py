# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# Tests for the CREATE TABLE statement

import pytest
import re
from uuid import UUID

from cassandra import AlreadyExists
from cassandra.protocol import ConfigurationException, InvalidRequest, SyntaxException
from .util import new_named_table, unique_name

def test_prepared_create_table_options_validated(cql, test_keyspace):
    """
    CREATE TABLE validates its options when the statement is prepared,
    so nothing is cached before the checks run and a later execution
    cannot skip them.
    """
    with pytest.raises(ConfigurationException, match="min_index_interval"):
        cql.prepare(f"CREATE TABLE {test_keyspace}.{unique_name()} (p int PRIMARY KEY)"
                    " WITH compaction = {'class': 'LeveledCompactionStrategy'} AND min_index_interval = 0")

# Ported from scylla-dtest cql_additional_tests.py::TestCQL::test_table.
# Tables with and without a clustering key, one WITH COMPACT STORAGE and one
# with a trailing comma after its PRIMARY KEY clause, are created. CREATE
# TABLE of a table that already exists fails with AlreadyExists naming the
# keyspace and table; a repeated column and a malformed COMPACT STORAGE
# table are refused. Then a table is dropped, another truncated, and the
# dropped one created again with other columns.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("test_keyspace", ["vnodes"], indirect=True)
def test_create_existing_table(cql, test_keyspace, compact_storage):
    ks = test_keyspace
    try:
        cql.execute(f"""
            CREATE TABLE {ks}.test1 (
                k int PRIMARY KEY,
                c int
            )
        """)
        cql.execute(f"""
            CREATE TABLE {ks}.test2 (
                k int,
                name int,
                value int,
                PRIMARY KEY(k, name)
            ) WITH COMPACT STORAGE
        """)
        cql.execute(f"""
            CREATE TABLE {ks}.test3 (
                k int,
                c int,
                PRIMARY KEY (k),
            )
        """)

        # existing table
        with pytest.raises(AlreadyExists, match=re.escape(f"{ks}.test3")):
            cql.execute(f"CREATE TABLE {ks}.test3 (k int PRIMARY KEY, c int)")
        # repeated column
        with pytest.raises(InvalidRequest, match="code=2200"):
            cql.execute(f"CREATE TABLE {ks}.test4 (k int PRIMARY KEY, c int, k text)")
        # compact storage limitations
        with pytest.raises(SyntaxException):
            cql.execute(f"CREATE TABLE {ks}.test4 (k int, name, int, c1 int, c2 int, PRIMARY KEY(k, name)) WITH COMPACT STORAGE")

        cql.execute(f"DROP TABLE {ks}.test1")
        cql.execute(f"TRUNCATE {ks}.test2")

        cql.execute(f"""
            CREATE TABLE {ks}.test1 (
                k int PRIMARY KEY,
                c1 int,
                c2 int,
            )
        """)
    finally:
        for t in ["test1", "test2", "test3"]:
            cql.execute(f"DROP TABLE IF EXISTS {ks}.{t}")

# Ported from scylla-dtest cql_additional_tests.py::TestCQL::test_static_cf.
# A "static" table (a partition key and regular columns, no clustering key),
# named "users", which is also a CQL keyword: rows written by INSERT, UPDATE
# and a batch mixing INSERT, UPDATE and cell DELETEs read back by key and by
# a full scan.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
def test_static_cf(cql, test_keyspace_vnodes):
    with new_named_table(cql, test_keyspace_vnodes, "users", "userid uuid PRIMARY KEY, firstname text, lastname text, age int") as table:
        cql.execute(f"INSERT INTO {table} (userid, firstname, lastname, age) VALUES (550e8400-e29b-41d4-a716-446655440000, 'Frodo', 'Baggins', 32)")
        cql.execute(f"UPDATE {table} SET firstname = 'Samwise', lastname = 'Gamgee', age = 33 WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479")
        u1 = UUID("550e8400-e29b-41d4-a716-446655440000")
        u2 = UUID("f47ac10b-58cc-4372-a567-0e02b2c3d479")
        assert list(cql.execute(f"SELECT firstname, lastname FROM {table} WHERE userid = 550e8400-e29b-41d4-a716-446655440000")) == [("Frodo", "Baggins")]
        assert list(cql.execute(f"SELECT * FROM {table} WHERE userid = 550e8400-e29b-41d4-a716-446655440000")) == [(u1, 32, "Frodo", "Baggins")]
        assert list(cql.execute(f"SELECT * FROM {table}")) == [(u2, 33, "Samwise", "Gamgee"), (u1, 32, "Frodo", "Baggins")]
        cql.execute(f"""
            BEGIN BATCH
                INSERT INTO {table} (userid, age) VALUES (550e8400-e29b-41d4-a716-446655440000, 36)
                UPDATE {table} SET age = 37 WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479
                DELETE firstname, lastname FROM {table} WHERE userid = 550e8400-e29b-41d4-a716-446655440000
                DELETE firstname, lastname FROM {table} WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479
            APPLY BATCH
        """)
        assert list(cql.execute(f"SELECT * FROM {table}")) == [(u2, 37, None, None), (u1, 36, None, None)]

# Ported from scylla-dtest cql_additional_tests.py::TestCQL::test_noncomposite_static_cf.
# The same as test_static_cf (the dtest body is the same): a table with a
# single-column partition key and no clustering key, written by INSERT,
# UPDATE and a batch, and read back by key and by a full scan.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
def test_noncomposite_static_cf(cql, test_keyspace_vnodes):
    with new_named_table(cql, test_keyspace_vnodes, "users", "userid uuid PRIMARY KEY, firstname text, lastname text, age int") as table:
        cql.execute(f"INSERT INTO {table} (userid, firstname, lastname, age) VALUES (550e8400-e29b-41d4-a716-446655440000, 'Frodo', 'Baggins', 32)")
        cql.execute(f"UPDATE {table} SET firstname = 'Samwise', lastname = 'Gamgee', age = 33 WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479")
        u1 = UUID("550e8400-e29b-41d4-a716-446655440000")
        u2 = UUID("f47ac10b-58cc-4372-a567-0e02b2c3d479")
        assert list(cql.execute(f"SELECT firstname, lastname FROM {table} WHERE userid = 550e8400-e29b-41d4-a716-446655440000")) == [("Frodo", "Baggins")]
        assert list(cql.execute(f"SELECT * FROM {table} WHERE userid = 550e8400-e29b-41d4-a716-446655440000")) == [(u1, 32, "Frodo", "Baggins")]
        assert list(cql.execute(f"SELECT * FROM {table}")) == [(u2, 33, "Samwise", "Gamgee"), (u1, 32, "Frodo", "Baggins")]
        cql.execute(f"""
            BEGIN BATCH
                INSERT INTO {table} (userid, age) VALUES (550e8400-e29b-41d4-a716-446655440000, 36)
                UPDATE {table} SET age = 37 WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479
                DELETE firstname, lastname FROM {table} WHERE userid = 550e8400-e29b-41d4-a716-446655440000
                DELETE firstname, lastname FROM {table} WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479
            APPLY BATCH
        """)
        assert list(cql.execute(f"SELECT * FROM {table}")) == [(u2, 37, None, None), (u1, 36, None, None)]

# A "sparse" table with two clustering columns and two regular columns: rows
# written by INSERT and UPDATE (one setting only one of the columns), read
# by full primary key, by a slice of the second clustering column, and by
# the first clustering column.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
def test_create_sparse_table(cql, test_keyspace_vnodes):
    schema = "userid uuid, posted_month int, posted_day int, body text, posted_by text, PRIMARY KEY (userid, posted_month, posted_day)"
    with new_named_table(cql, test_keyspace_vnodes, "timeline", schema) as table:
        cql.execute(f"INSERT INTO {table} (userid, posted_month, posted_day, body, posted_by) VALUES (550e8400-e29b-41d4-a716-446655440000, 1, 12, 'Something else', 'Frodo Baggins')")
        cql.execute(f"INSERT INTO {table} (userid, posted_month, posted_day, body, posted_by) VALUES (550e8400-e29b-41d4-a716-446655440000, 1, 24, 'Something something', 'Frodo Baggins')")
        cql.execute(f"UPDATE {table} SET body = 'Yo Froddo', posted_by = 'Samwise Gamgee' WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479 AND posted_month = 1 AND posted_day = 3")
        cql.execute(f"UPDATE {table} SET body = 'Yet one more message' WHERE userid = 550e8400-e29b-41d4-a716-446655440000 AND posted_month = 1 and posted_day = 30")
        key = "userid = 550e8400-e29b-41d4-a716-446655440000 AND posted_month = 1"
        assert list(cql.execute(f"SELECT body, posted_by FROM {table} WHERE {key} AND posted_day = 24")) == [
            ("Something something", "Frodo Baggins")]
        assert list(cql.execute(f"SELECT posted_day, body, posted_by FROM {table} WHERE {key} AND posted_day > 12")) == [
            (24, "Something something", "Frodo Baggins"), (30, "Yet one more message", None)]
        assert list(cql.execute(f"SELECT posted_day, body, posted_by FROM {table} WHERE {key}")) == [
            (12, "Something else", "Frodo Baggins"), (24, "Something something", "Frodo Baggins"), (30, "Yet one more message", None)]

# Invalid CREATE TABLE statements: no columns, no primary key, two PRIMARY
# KEY columns, duplicate column names, and a COMPACT STORAGE table with more
# than one regular column next to its clustering column.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
def test_create_table_invalid(cql, test_keyspace_vnodes, compact_storage):
    table = f"{test_keyspace_vnodes}.test"
    with pytest.raises(SyntaxException):
        cql.execute(f"CREATE TABLE {table} ()")
    for schema in ["c1 text, c2 text, c3 text",
                   "key1 text PRIMARY KEY, key2 text PRIMARY KEY",
                   "key text PRIMARY KEY, key int",
                   "key text PRIMARY KEY, c int, c text"]:
        with pytest.raises(InvalidRequest):
            cql.execute(f"CREATE TABLE {table} ({schema})")
    with pytest.raises(InvalidRequest):
        cql.execute(f"CREATE TABLE {table} (key text, key2 text, c int, d text, PRIMARY KEY (key, key2)) WITH COMPACT STORAGE")

# The obsolete CQL2 table property default_validation is a syntax error in
# CREATE TABLE and in ALTER TABLE.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
def test_create_table_obsolete_property(cql, test_keyspace_vnodes):
    with pytest.raises(SyntaxException):
        cql.execute(f"CREATE TABLE {test_keyspace_vnodes}.test (foo text PRIMARY KEY, c int) WITH default_validation=timestamp")
    with new_named_table(cql, test_keyspace_vnodes, "test", "foo text PRIMARY KEY, c int") as table:
        with pytest.raises(SyntaxException):
            cql.execute(f"ALTER TABLE {table} WITH default_validation=int;")

# Unknown compression options are rejected: the CQL2 compression_parameters
# syntax is a syntax error, and an unknown key in the compression map is a
# configuration error (CASSANDRA-4266).
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
def test_create_table_unknown_compression_option(cql, test_keyspace_vnodes):
    with pytest.raises(SyntaxException):
        cql.execute(f"""CREATE TABLE {test_keyspace_vnodes}.users (key varchar PRIMARY KEY, password varchar, gender varchar)
                        WITH compression_parameters:sstable_compressor = 'DeflateCompressor'""")
    with pytest.raises(ConfigurationException):
        cql.execute(f"""CREATE TABLE {test_keyspace_vnodes}.users (key varchar PRIMARY KEY, password varchar, gender varchar)
                        WITH compression = {{ 'sstable_compressor' : 'DeflateCompressor' }}""")
