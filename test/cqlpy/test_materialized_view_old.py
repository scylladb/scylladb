# Copyright 2016-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# These file contains the original functional tests written for materialized
# views, originally written in C++ but translated (see issue #16134) to Python.
# The tests originally in view_schema_test.cc, view_complex_test.cc,
# view_schema_ckey_test.cc and view_schema_pkey_test.cc were moved here.

# The original C++ tests used to wrap reads of a view in an "eventually()"
# retry loop, because in general a materialized view is updated
# asynchronously - some time after the base-table write is acknowledged.
# The tests in this file do *not* need such retries: cqlpy runs against a
# single node, and there both Scylla and Cassandra apply the view update
# synchronously, as part of the base write. In Cassandra this is because
# StorageProxy::mutateMV() has a special case applying the view mutation
# inline when the paired view replica is the local node, instead of going
# through the batchlog and the asynchronous view-mutation stage.
#
# Note that this only holds as long as the base row and its paired view row
# live on the same node. So if we ever want to run these tests against a
# multi-node cluster, we shouldn't add retry loops here - we should instead
# make sure the tests use a keyspace whose data is all replicated on just
# one node (e.g., NetworkTopologyStrategy with RF=1 in a data center that
# has a single node).
#
# What *is* asynchronous even on a single node is the *view build* - the
# backfill of a view created on a table that already has data. Tests which
# need this must wait for the build to complete, and can't just read the
# view immediately after creating it.

import pytest
from cassandra.protocol import InvalidRequest

from .util import new_test_table, new_materialized_view, unique_name

# CQL usually folds identifier names - keyspace, table and column names -
# to lowercase. That is, unless the identifier is enclosed in double
# quotation marks (") then the identifier becomes case sensitive.
# Let's test that case-sensitive (quoted) column names can be used for
# materialized views. Test that data can be inserted and queried, and
# that case sensitive columns in views can be renamed.
# This test reproduces issues #3388 and #3391.
def test_case_sensitivity(cql, test_keyspace):
    schema = '"theKey" int, "theClustering" int, "theValue" int, primary key ("theKey", "theClustering")'
    with new_test_table(cql, test_keyspace, schema) as table:
        where = '"theKey" is not null and "theClustering" is not null'
        pk = '"theKey", "theClustering"'
        with new_materialized_view(cql, table, '*', pk, where) as mv1, \
             new_materialized_view(cql, table, '"theKey", "theClustering", "theValue"', pk, where) as mv2:
            cql.execute(f'insert into {table} ("theKey", "theClustering", "theValue") values (0, 0, 0)')
            for mv in [mv1, mv2]:
                assert [(0, 0, 0)] == list(cql.execute(f'select "theKey", "theClustering", "theValue" from {mv}'))
            cql.execute(f'alter table {table} rename "theClustering" to "Col"')
            for mv in [mv1, mv2]:
                assert [(0, 0, 0)] == list(cql.execute(f'select "theKey", "Col", "theValue" from {mv}'))

# A materialized view is read-only, and its schema is not its own: it can't
# be written to directly, and it can't be modified with ALTER TABLE (only
# with ALTER MATERIALIZED VIEW). Instead, the view's schema follows that of
# its base table - a column added to or renamed in the base table is added
# to or renamed in the view as well.
def test_access_and_schema(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int, c ascii, v bigint, primary key (p, c)') as table:
        with new_materialized_view(cql, table, '*', 'v, p, c',
                'v is not null and p is not null and c is not null') as mv:
            cql.execute(f"insert into {table} (p, c, v) values (0, 'foo', 1)")
            with pytest.raises(InvalidRequest, match='Cannot directly modify a materialized view'):
                cql.execute(f"insert into {mv} (p, c, v) values (1, 'foo', 1)")
            with pytest.raises(InvalidRequest, match='Cannot use ALTER TABLE'):
                cql.execute(f"alter table {mv} add foo text")
            with pytest.raises(InvalidRequest, match='Cannot use ALTER TABLE'):
                cql.execute(f"alter table {mv} with compaction = {{ 'class' : 'LeveledCompactionStrategy' }}")
            cql.execute(f"alter materialized view {mv} with compaction = {{ 'class' : 'LeveledCompactionStrategy' }}")
            cql.execute(f"alter table {table} add foo text")
            cql.execute(f"insert into {table} (p, c, v, foo) values (0, 'foo', 1, 'bar')")
            assert [('bar',)] == list(cql.execute(f"select foo from {mv}"))
            cql.execute(f"alter table {table} rename c to bar")
            assert [('foo',)] == list(cql.execute(f"select bar from {mv}"))

# A base-table column which the view doesn't select can be dropped from the
# base table, and the view continues to work normally.
#
# This is scylla_only, because it is a deliberate Scylla extension: Cassandra
# refuses to drop *any* regular column from a base table that has any
# materialized view at all ("Cannot drop column a on base table ... with
# materialized views", AlterTableStatement.java:516), while Scylla refuses
# only if one of the views actually needs the dropped column - i.e., selects
# it, or depends on its liveness. See issue #4448 and the C++ test
# test_mv_allow_some_column_drops, which is still in view_schema_test.cc and
# covers the rule in more detail.
def test_column_dropped_from_base(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, 'p int, c ascii, a int, v int, primary key (p, c)') as table:
        with new_materialized_view(cql, table, 'p, c, v', 'v, p, c',
                'v is not null and p is not null and c is not null') as mv:
            cql.execute(f"alter table {table} drop a")
            cql.execute(f"insert into {table} (p, c, v) values (0, 'foo', 1)")
            assert [(1,)] == list(cql.execute(f"select v from {mv}"))

# Test that a view row follows the base row it was generated from: when the
# base row is updated so that the view's partition key changes, the old view
# row goes away and a new one appears in its place.
def test_updates(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'k int, v int, primary key (k)') as table:
        with new_materialized_view(cql, table, '*', 'v, k',
                'k is not null and v is not null') as mv:
            cql.execute(f"insert into {table} (k, v) values (0, 0)")
            assert [(0, 0)] == list(cql.execute(f"select k, v from {table} where k = 0"))
            assert [(0, 0)] == list(cql.execute(f"select k, v from {mv} where v = 0"))

            cql.execute(f"insert into {table} (k, v) values (0, 1)")
            assert [(0, 1)] == list(cql.execute(f"select k, v from {table} where k = 0"))
            assert [] == list(cql.execute(f"select k, v from {mv} where v = 0"))
            assert [(0, 1)] == list(cql.execute(f"select k, v from {mv} where v = 1"))

# Like test_updates above, except that here the update leaves the view's key
# columns (k and c) unchanged and modifies only v, which is a regular column
# in both the base table and the view. So the view row is not moved - it is
# just updated in place - which is a different code path, and we check that
# it produces the right result too.
def test_updates_no_read_before_update(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'k int, c int, v int, primary key (k)') as table:
        with new_materialized_view(cql, table, '*', 'k, c',
                'k is not null and c is not null') as mv:
            cql.execute(f"insert into {table} (k, c, v) values (0, 0, 0)")
            assert [(0, 0)] == list(cql.execute(f"select k, v from {table} where k = 0"))
            assert [(0, 0)] == list(cql.execute(f"select k, v from {mv} where k = 0"))

            cql.execute(f"insert into {table} (k, c, v) values (0, 0, 1)")
            assert [(0, 1)] == list(cql.execute(f"select k, v from {table} where k = 0"))
            assert [(0, 1)] == list(cql.execute(f"select k, v from {mv} where k = 0"))

# Test that after a materialized view is dropped, its name can be reused for
# a new view.
def test_reuse_name(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int primary key, v int') as table:
        mv = test_keyspace + '.' + unique_name()
        create = (f"create materialized view {mv} as select * from {table} "
                  "where v is not null and p is not null primary key (v, p)")
        try:
            cql.execute(create)
            cql.execute(f"drop materialized view {mv}")
            cql.execute(create)
        finally:
            cql.execute(f"drop materialized view if exists {mv}")
