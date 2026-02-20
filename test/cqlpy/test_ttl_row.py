# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

#############################################################################
# Functional tests for the per-row TTL feature. Note that this is a distinct
# feature from CQL's traditional per-write (per-cell) TTL, which is tested
# in test_ttl.py.
#
# The per-row TTL feature was requested in issue #13000. It is a feature that
# does not exist in Cassandra - and was inspired by DynamoDB's TTL feature.
# Under the hood it uses the same implementation that we used in Alternator
# to implement the DynamoDB TTL feature.
#############################################################################

import pytest
import time
from cassandra.protocol import InvalidRequest
from .util import new_test_table, new_materialized_view, new_secondary_index, ScyllaMetrics

# All tests in this file check the Scylla-only per-row TTL feature, so
# let's mark them all scylla_only with an autouse fixture:
@pytest.fixture(scope="function", autouse=True)
def all_tests_are_scylla_only(scylla_only):
    pass

# Utility function for finding the current TTL column in the given table -
# using DESC TABLE. Returns the TTL column's name, or None if there is none.
def get_ttl_column(cql, table):
    desc = cql.execute(f'DESC TABLE {table}').one().create_statement
    ttls = []
    for line in desc.splitlines():
        if line.startswith(')'):
            break
        if line.endswith('TTL,'):
            ttls.append(line.lstrip().split(' ', 1)[0].strip('"'))
    if not ttls:
        return None
    assert len(ttls) == 1
    return ttls[0]

# Test that DESC TABLE on a table that doesn't have row TTL enabled doesn't
# list a TTL column.
def test_desc_table_without_ttl_column(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int primary key, v int') as table:
        assert get_ttl_column(cql, table) is None

# Test the ability to create a new table with a TTL column, and to see in
# DESC TABLE that it is indeed the TTL column. We don't check yet that the
# TTL actually works - just that it's stored with the table and can be
# retrieved by DESC TABLE.
def test_create_table_with_ttl_column(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int primary key, v int, expiration timestamp ttl') as table:
        assert get_ttl_column(cql, table) == 'expiration'

# Check that it is not allowed to create a table with more than one column
# designated as "TTL". As The Highlander said, "There can be only one".
def test_create_table_with_two_ttl_column(cql, test_keyspace):
    with pytest.raises(InvalidRequest, match='more than one'):
        with new_test_table(cql, test_keyspace, 'p int primary key, v int, expiration1 timestamp ttl, expiration2 timestamp ttl'):
            pass

# Check that only a column that is of type "timestamp", "bigint" or "int" are
# allowed as the TTL column - other column types are rejected.
def test_create_table_with_bad_ttl_type(cql, test_keyspace):
    # We don't really need to check all possible column types - the
    # implementation will whitelist only specific types and all the rest
    # will just return the same error - but let's check a few types
    # that shouldn't be supported. In particular, `varint` and `decimal`
    # could have been supported but are wasteful. `text` is also not allowed,
    # the time needs to be a number, not a string holding a number.
    for t in ['smallint', 'double', 'varint', 'decimal', 'text']:
        with pytest.raises(InvalidRequest, match='must be of type'):
            with new_test_table(cql, test_keyspace, f'p int primary key, expiration {t} ttl'):
                pass

# Check that the TTL column chosen in CREATE TABLE must be a regular
# column. In other words, it cannot be a primary key column (one of the
# partition key or clustering key columns), or a static column.
#
# Note that there is no inherent reason why we couldn't allow primary
# columns to also be a TTL column. In DynamoDB (and Alternator), this
# is actually allowed. But I think it is not a useful use case, and
# decided not to allow it for CQL's per-row TTL feature.
def test_create_table_ttl_must_be_regular_column(cql, test_keyspace):
    for schema in [
        'e timestamp ttl primary key',
        'e timestamp ttl, c timestamp, primary key(e, c)',
        'p int, e timestamp ttl, primary key(p, e)',
        'p int, c int, e timestamp ttl, primary key(p, c, e)',
        'p int, c int, e timestamp ttl, primary key(p, e, c)',
        'p int, e timestamp ttl, x timestamp, primary key((p, e), x)',
        'p int, e timestamp ttl, x timestamp, primary key((e, p), x)',
        'p int, c int, e timestamp ttl static, primary key(p, c)',
    ]:
        with pytest.raises(InvalidRequest, match='TTL column e'):
            with new_test_table(cql, test_keyspace, schema):
                pass

# In the tests above, we verified that setting a TTL column worked by
# using DESC TABLE and checking that the "TTL" keyword appears for the
# right column. In this test we confirm that DESC KEYSPACE and DESC SCHEMA
# also correctly list the TTL column.
def test_desc_with_ttl_column(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int primary key, v int, expiration timestamp ttl') as table:
        _, table_name = table.split('.')
        for desc_stmt in [f'DESC KEYSPACE {test_keyspace}', 'DESC SCHEMA']:
            success = False
            for entry in cql.execute(desc_stmt):
                if entry.type != 'table' or entry.name != table_name or entry.keyspace_name != test_keyspace:
                    continue
                # Found the test table's description:
                desc = entry.create_statement
                ttls = []
                for line in desc.splitlines():
                    if line.startswith(')'):
                        break
                    if line.endswith('TTL,'):
                        ttls.append(line.lstrip().split(' ', 1)[0])
                assert len(ttls) == 1
                assert ttls[0] == 'expiration'
                # If we got here, the test of this desc_stmt was successful.
                success = True
                break
            if not success:
                pytest.fail(f"couldn't find test table {table} in {desc_stmt}")

# Check that we can add with ALTER TABLE a TTL column to an existing table
# that had none, with the syntax "ALTER TABLE tbl TTL colname".
def test_alter_table_add_ttl(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int primary key, expiration timestamp') as table:
        # At this point, this table has no TTL column
        assert get_ttl_column(cql, table) is None
        # Set the TTL column, and see it happened:
        cql.execute(f'ALTER TABLE {table} TTL expiration')
        assert get_ttl_column(cql, table) == 'expiration'

# Check that the column that we try to make a TTL via ALTER TABLE must
# exist.
def test_alter_table_add_ttl_missing(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int primary key, expiration timestamp') as table:
        with pytest.raises(InvalidRequest, match='does not exist'):
            cql.execute(f'ALTER TABLE {table} TTL nonexistent')
        assert get_ttl_column(cql, table) is None

# Same as we checked above for CREATE TABLE, check that also for ALTER TABLE
# only a column that is of type "timestamp", "bigint" or "int" are allowed as
# the TTL column - other column types are rejected.
def test_alter_table_with_bad_ttl_type(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int primary key, s smallint, d double, v varint, dc decimal, t text') as table:
        for col in ['s', 'd', 'v', 'dc', 't']:
            with pytest.raises(InvalidRequest, match='must be of type'):
                cql.execute(f'ALTER TABLE {table} TTL {col}')

# Same as we checked above for CREATE TABLE, check that also for ALTER TABLE
# only a regular column can be used as a TTL column - not a key column or
# a static column.
def test_alter_table_ttl_must_be_regular_column(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p1 timestamp, p2 timestamp, c1 timestamp, c2 timestamp, s1 timestamp static, s2 timestamp static, e timestamp, primary key ((p1,p2),c1,c2)') as table:
        # Choosing one of the partition key columns, clustering key columns
        # or static columns will fail.
        for col in ['p1', 'p2', 'c1', 'c2', 's1', 's2']:
            with pytest.raises(InvalidRequest, match='Cannot use'):
                cql.execute(f'ALTER TABLE {table} TTL {col}')
        # Choosing "e", a regular column, should work.
        cql.execute(f'ALTER TABLE {table} TTL e')
        assert get_ttl_column(cql, table) == 'e'

# Check that we can't add a second TTL column to a table that already has
# one set. You're not allowed even to set the TTL to the same column that
# is already the TTL.
# Try two ways to create the table that has a TTL set - with CREATE
# TABLE or with ALTER TABLE.
def test_alter_table_second_ttl_column(cql, test_keyspace):
    # First TTL setting is by CREATE TABLE, second by ALTER TABLE:
    with new_test_table(cql, test_keyspace, 'p int primary key, e1 timestamp ttl, e2 timestamp') as table:
        assert get_ttl_column(cql, table) == 'e1'
        with pytest.raises(InvalidRequest, match='already has a TTL column'):
            cql.execute(f'ALTER TABLE {table} TTL e2')
        # Can't even set the TTL to the same column that is already the TTL
        with pytest.raises(InvalidRequest, match='already has a TTL column'):
            cql.execute(f'ALTER TABLE {table} TTL e1')
    # Both first and second TTL setting is by ALTER TABLE:
    with new_test_table(cql, test_keyspace, 'p int primary key, e1 timestamp, e2 timestamp') as table:
        assert get_ttl_column(cql, table) is None
        cql.execute(f'ALTER TABLE {table} TTL e1')
        assert get_ttl_column(cql, table) == 'e1'
        with pytest.raises(InvalidRequest, match='already has a TTL column'):
            cql.execute(f'ALTER TABLE {table} TTL e2')
        # Can't even set the TTL to the same column that is already the TTL
        with pytest.raises(InvalidRequest, match='already has a TTL column'):
            cql.execute(f'ALTER TABLE {table} TTL e1')

# Check that we can remove the TTL designation from an existing column
# with ALTER TABLE, with the syntax "ALTER TABLE tab TTL NULL".
# Again we have two ways of creating a table with TTL (which we want to
# remove) - with CREATE TABLE or with ALTER TABLE.
def test_alter_table_disable_ttl(cql, test_keyspace):
    # TTL setting by CREATE TABLE, disable by ALTER TABLE:
    with new_test_table(cql, test_keyspace, 'p int primary key, e timestamp ttl') as table:
        assert get_ttl_column(cql, table) == 'e'
        cql.execute(f'ALTER TABLE {table} TTL NULL')
        assert get_ttl_column(cql, table) is None
    # Both TTL setting and disable is by ALTER TABLE:
    with new_test_table(cql, test_keyspace, 'p int primary key, e timestamp') as table:
        assert get_ttl_column(cql, table) is None
        cql.execute(f'ALTER TABLE {table} TTL e')
        assert get_ttl_column(cql, table) == 'e'
        cql.execute(f'ALTER TABLE {table} TTL NULL')
        assert get_ttl_column(cql, table) is None

# Check that we can't remove the TTL from a table if it's not already enabled.
def test_alter_table_disable_unset_ttl(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int primary key, e timestamp') as table:
        assert get_ttl_column(cql, table) is None
        with pytest.raises(InvalidRequest, match='does not have a TTL column'):
            cql.execute(f'ALTER TABLE {table} TTL NULL')

# Check that we can change the TTL column from one column to another via
# two separate ALTER TABLE operations - first removing the TTL mark from
# the original column and adding it to the new column.
# Note that there is no syntax for doing this in a single ALTER TABLE
# operation. This is not a big loss - DynamoDB also requires you to disable
# TTL via UpdateTimeToLive before you can re-enable it with a different
# column as the expiration-time column.
def test_alter_table_change_ttl_column(cql, test_keyspace):
    # TTL setting by CREATE TABLE, change by ALTER TABLE:
    with new_test_table(cql, test_keyspace, 'p int primary key, e1 timestamp ttl, e2 timestamp') as table:
        assert get_ttl_column(cql, table) == 'e1'
        cql.execute(f'ALTER TABLE {table} TTL NULL')
        assert get_ttl_column(cql, table) is None
        cql.execute(f'ALTER TABLE {table} TTL e2')
        assert get_ttl_column(cql, table) == 'e2'
    # Both TTL original setting and change is by ALTER TABLE:
    with new_test_table(cql, test_keyspace, 'p int primary key, e1 timestamp, e2 timestamp') as table:
        assert get_ttl_column(cql, table) is None
        cql.execute(f'ALTER TABLE {table} TTL e1')
        assert get_ttl_column(cql, table) == 'e1'
        cql.execute(f'ALTER TABLE {table} TTL NULL')
        assert get_ttl_column(cql, table) is None
        cql.execute(f'ALTER TABLE {table} TTL e2')
        assert get_ttl_column(cql, table) == 'e2'

# Check that quoted and unquoted column names work correctly in CREATE
# TABLE and ALTER TABLE's feature of marking a column as TTL. The column's
# name should be saved correctly in the tag so as to lead DESC TABLE to
# later show this column's name with TTL.
def test_quoted_ttl_column(cql, test_keyspace):
    # The CREATE TABLE statement may have an unquoted mixed-case column
    # name, in which case the real TTL column is the lowercase version.
    with new_test_table(cql, test_keyspace, 'p int primary key, EXpirATION timestamp ttl') as table:
        assert get_ttl_column(cql, table) == 'expiration'
    # If CREATE TABLE chooses a quoted column name, its original case is saved
    with new_test_table(cql, test_keyspace, 'p int primary key, "EXpirATION" timestamp ttl') as table:
        assert get_ttl_column(cql, table) == 'EXpirATION'
    # If a primary column or a static column has a quoted name, the detection
    # that it's a forbidden TTL column still works:
    with new_test_table(cql, test_keyspace, '"dOg" timestamp, "cAt" timestamp, "hEllo" timestamp static, primary key ("dOg", "cAt")') as table:
        assert get_ttl_column(cql, table) is None
        # These statements won't work because they refer to a non-existent
        # column because the command is missing quotes:
        with pytest.raises(InvalidRequest, match='does not exist'):
            cql.execute(f'ALTER TABLE {table} TTL dOg')
        with pytest.raises(InvalidRequest, match='does not exist'):
            cql.execute(f'ALTER TABLE {table} TTL cAt')
        with pytest.raises(InvalidRequest, match='does not exist'):
            cql.execute(f'ALTER TABLE {table} TTL hEllo')
        # These statements won't work because with the quotes, the columns
        # are recognized correctly as non-regular columns (key column or
        # static column)
        with pytest.raises(InvalidRequest, match='Cannot use a primary key'):
            cql.execute(f'ALTER TABLE {table} TTL "dOg"')
        with pytest.raises(InvalidRequest, match='Cannot use a primary key'):
            cql.execute(f'ALTER TABLE {table} TTL "cAt"')
        with pytest.raises(InvalidRequest, match='Cannot use a static column'):
            cql.execute(f'ALTER TABLE {table} TTL "hEllo"')
    # Successful ALTER TABLE TTL, with quoted column names:
    with new_test_table(cql, test_keyspace, 'p int primary key, "dOg" timestamp, "cAt" timestamp') as table:
        assert get_ttl_column(cql, table) is None
        cql.execute(f'ALTER TABLE {table} TTL "dOg"')
        assert get_ttl_column(cql, table) == 'dOg'
        cql.execute(f'ALTER TABLE {table} TTL NULL')
        assert get_ttl_column(cql, table) is None
        cql.execute(f'ALTER TABLE {table} TTL "cAt"')
        assert get_ttl_column(cql, table) == 'cAt'

# Check what happens if a TTL column is deleted from the table with ALTER
# TABLE DROP. If this column was the TTL column, the TTL feature should be
# disabled.
def test_drop_ttl_column(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int primary key, e timestamp ttl, x int') as table:
        assert get_ttl_column(cql, table) == 'e'
        cql.execute(f'ALTER TABLE {table} DROP e')
        assert get_ttl_column(cql, table) is None
        # Unfortunately, the above check is not proof that the TTL tag that
        # used to point to to column 'e' was removed - it might still be
        # there, but DESC TABLE simply doesn't show 'e' since it doesn't
        # exist. Unfortunately, we don't have an API in CQL to read the tag
        # directly. So to be sure the tag was removed, let's re-add column e,
        # and check that even then, it's no longer the TTL column.
        cql.execute(f'ALTER TABLE {table} ADD e timestamp')
        assert get_ttl_column(cql, table) is None

# Check what happens if a table has a TTL column and a *different* column
# is dropped. Nothing should happen - TTL should still be enabled.
def test_ttl_drop_other_column(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int primary key, e timestamp ttl, x int') as table:
        assert get_ttl_column(cql, table) == 'e'
        cql.execute(f'ALTER TABLE {table} DROP x')
        assert get_ttl_column(cql, table) == 'e'

# Check that ALTER TABLE RENAME doesn't cause problems by renaming the
# TTL column. Actually, it turns out that ALTER TABLE RENAME is only allowed
# on clustering key columns - so is NOT allowed on a TTL column (which can't
# be a clustering key). So in that sense we're safe. This test will just
# check that indeed, such rename isn't allowed.
def test_rename_ttl_column(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int primary key, e timestamp ttl, x int') as table:
        with pytest.raises(InvalidRequest, match='Cannot rename'):
            cql.execute(f'ALTER TABLE {table} RENAME e to f')

# Convenience fixture for retrieving the period of the expiration scanning,
# from the "alternator_ttl_period_in_seconds" configuration.
@pytest.fixture(scope='module')
def ttl_period(cql):
    result = list(cql.execute("SELECT value FROM system.config WHERE name='alternator_ttl_period_in_seconds'"))
    assert len(result) == 1
    return float(result[0].value)

# Utility function to convert Python's notion of time (floating-point
# seconds since the Epoch) into what we need to store in TTL's "timestamp"
# (integer milliseconds since the epoch) or "bigint"/"int" (integer seconds
# since the epoch). Pass "timestamp", "bigint" or "int" as typ, and Python's
# time (e.g., the output of time.time()) as tim.
def to_ttl(typ, tim):
    if typ == 'timestamp':
        return int(tim*1000)
    elif typ == 'bigint' or typ == 'int':
        return int(tim)
    else:
        pytest.fail(f'python_to_ttl called for unknown type {typ}')

# All the previous tests checked the ability to enable and disable the
# per-row TTL feature in different ways, but we never checked that
# expiration actually happens. Now we start testing actual expiration.
# The first test is a simple test for an items that do and don't expire.
# The length of this test will be around alternator_ttl_period_in_seconds,
# (retrieved by the fixture "ttl_period"), so configure it low to make this
# test (and the following tests) fast.
# This test is parametrized to run three times, once for the "timestamp" type,
# once for the "bigint" type and once for the not-recommended type "int",
# to see that we understand their correct value - seconds since the epoch for
# bigint/int, milliseconds since the epoch
# for timestamp.
@pytest.mark.parametrize('typ', ['timestamp', 'bigint', 'int'])
def test_row_ttl_expiration(cql, test_keyspace, ttl_period, typ):
    with new_test_table(cql, test_keyspace, f'p int primary key, e {typ}, x int') as table:
        # Row 1 is already-expired item before enabling the TTL, to verify
        # that items that already exist when TTL is enabled also get handled.
        cql.execute(f'INSERT INTO {table} (p, e) values (1, {to_ttl(typ, time.time()-60)})')
        # Enable TTL, using the column "e" for expiration
        cql.execute(f'ALTER TABLE {table} TTL e')
        # Row 2 should never expire, it is missing a value for "e":
        cql.execute(f'INSERT INTO {table} (p, x) values (2, 2)')
        # Row 3 should expire ASAP, as its expiration "e" has already passed,
        # one minute ago.
        cql.execute(f'INSERT INTO {table} (p, e) values (3, {to_ttl(typ, time.time()-60)})')
        # Row 4 has an expiration time more than 5 years in the past (it is
        # my birth date...), it should be ignored and this row will never be
        # expired.
        cql.execute(f'INSERT INTO {table} (p, e) values (4, {to_ttl(typ, 162777600)})')
        # Row 5 has an expiration one second into the future, so should
        # expire by the time the test ends.
        cql.execute(f'INSERT INTO {table} (p, e) values (5, {to_ttl(typ, time.time()+1)})')
        # Row 6 is created with an expiration one second into the future,
        # but immediately, presumably before it can expire, we change its
        # expiration time to never expire.
        cql.execute(f'INSERT INTO {table} (p, e) values (6, {to_ttl(typ, time.time()+1)})')
        cql.execute(f'UPDATE {table} SET e=NULL WHERE p=6')
        # Row 7 is created with an expiration one hour into the future, so
        # it will remain alive until the test ends.
        cql.execute(f'INSERT INTO {table} (p, e) values (7, {to_ttl(typ, time.time()+3600)})')
        # Row 1, 3, and 5 are expected to expire soon: row 5 will take one
        # second, plus maybe an extra two ttl_periods until really expired.
        # Let's use a little higher timeout, just in case. But in the
        # successful case, we won't need to wait so long.
        deadline = time.time() + 3*ttl_period + 2
        while time.time() < deadline:
            p_vals = {row.p for row in cql.execute(f'SELECT p from {table}')}
            if {1, 3, 5}.isdisjoint(p_vals):
                break
            time.sleep(0.1)
        # After expiration, we expect rows 2, 4, 6 and 7 to still be
        # alive - and only them.
        assert p_vals == {2, 4, 6, 7}

# test_row_ttl_expiration above used a table with just a partition key.
# Because the code to *delete* rows in a table is different depending on
# whether the table has clustering keys or not (it needs to either delete
# rows, or partitions), we want to test the case of a table with clustering
# keys as well. This test is simpler than test_row_ttl_expiration because all
# we want to check is that the deletion works - not the various expiration-
# time types and the various cases when rows should not expire.
def test_row_ttl_expiration_composite(cql, test_keyspace, ttl_period):
    typ = "timestamp"
    with new_test_table(cql, test_keyspace, f'p1 int, p2 int, c1 int, c2 int, e {typ} ttl, primary key ((p1,p2),c1,c2)') as table:
        # Row 1,1,0,0 should never expire, it is missing a value for "e":
        cql.execute(f'INSERT INTO {table} (p1, p2, c1, c2) values (1,1,0,0)')
        # Row 1,2,0,0 should expire ASAP
        cql.execute(f'INSERT INTO {table} (p1, p2, c1, c2, e) values (1,2,0,0, {to_ttl(typ, time.time()-60)})')
        # Row 1,3,1,0 should never expire
        cql.execute(f'INSERT INTO {table} (p1, p2, c1, c2) values (1,3,1,0)')
        # Row 1,4,1,1 should expire ASAP
        cql.execute(f'INSERT INTO {table} (p1, p2, c1, c2, e) values (1,4,1,1, {to_ttl(typ, time.time()-60)})')
        deadline = time.time() + 3*ttl_period + 2
        while time.time() < deadline:
            p2_vals = {row.p2 for row in cql.execute(f'SELECT p2 from {table}')}
            if {2, 4}.isdisjoint(p2_vals):
                break
            time.sleep(0.1)
        # After expiration, we expect rows 1 and 3 to still be alive - and
        # only them.
        assert p2_vals == {1, 3}

# Test how in a table with a materialized view or a secondary index,
# expiring an item also removes it from the view and index.
# We already tested above the various reasons for an item to expire or not,
# so we don't need to continue testing these various cases here, and can
# test just one expiring item.
def test_row_ttl_expiration_views(cql, test_keyspace, ttl_period):
    typ = "timestamp"
    with (
        new_test_table(cql, test_keyspace, f'p int primary key, x int, e {typ} ttl') as table,
        new_materialized_view(cql, table, '*', 'x, p', 'x is not null and p is not null') as mv,
        new_secondary_index(cql, table, "x")
    ):
        # Insert a row that doesn't expire (we don't set "e" for it).
        # This allows us to check that the row reached the base table,
        # the view and the index. Note that views and indexes are known
        # to be synchronous on single-node tests, so we don't need to
        # wait until the views are updated.
        cql.execute(f'INSERT INTO {table} (p, x) values (1,2)')
        assert [(2,)] == list(cql.execute(f'SELECT x FROM {table} WHERE p=1'))
        assert [(1,)] == list(cql.execute(f'SELECT p FROM {mv} WHERE x=2'))
        assert [(1,)] == list(cql.execute(f'SELECT p FROM {table} WHERE x=2'))
        # Set the row's expiration column (e) to a minute in the past, so
        # it should expire ASAP
        cql.execute(f'UPDATE {table} SET e={to_ttl(typ, time.time()-60)} WHERE p=1')
        # Wait until the base-table row expires (should be roughly one
        # ttl_period, let's wait a bit more) and verify that the view and
        # index entries are gone too.
        deadline = time.time() + 3*ttl_period + 2
        while time.time() < deadline:
            if [] == list(cql.execute(f'SELECT x FROM {table} WHERE p=1')):
                break
            time.sleep(0.1)
        # Here we can't count on the synchronous view updates, because we
        # didn't wait for the base-table update, so we need to retry a bit.
        deadline = time.time() + 10
        while time.time() < deadline:
            if [] == list(cql.execute(f'SELECT x FROM {table} WHERE p=1')) and\
               [] == list(cql.execute(f'SELECT p FROM {mv} WHERE x=2')) and\
               [] == list(cql.execute(f'SELECT p FROM {table} WHERE x=2')):
                break
            time.sleep(0.1)
        assert [] == list(cql.execute(f'SELECT x FROM {table} WHERE p=1'))
        assert [] == list(cql.execute(f'SELECT p FROM {mv} WHERE x=2'))
        assert [] == list(cql.execute(f'SELECT p FROM {table} WHERE x=2'))

# In test_quoted_ttl_column above we checked that quoted column names can
# be properly set. But let's verify that the expiration scanner can actually
# read these columns and expire the rows.
def test_quoted_ttl_column_expiration(cql, test_keyspace, ttl_period):
    typ = "timestamp"
    with new_test_table(cql, test_keyspace, f'p int primary key, x int, "eXpirAtion" {typ} ttl') as table:
        # Insert a row that doesn't expire (we didn't set the expiration
        # column yet) and check it exists.
        cql.execute(f'INSERT INTO {table} (p, x) values (1,2)')
        assert [(2,)] == list(cql.execute(f'SELECT x FROM {table} WHERE p=1'))
        # Set the row's expiration column "eXpirAtion" to a minute in the
        # past, so it should expire ASAP
        cql.execute(f'UPDATE {table} SET "eXpirAtion"={to_ttl(typ, time.time()-60)} WHERE p=1')
        # Check that soon (should be roughly one ttl_period) the row will
        # disappear
        deadline = time.time() + 3*ttl_period + 2
        while time.time() < deadline:
            if [] == list(cql.execute(f'SELECT x FROM {table} WHERE p=1')):
                break
            time.sleep(0.1)
        assert [] == list(cql.execute(f'SELECT x FROM {table} WHERE p=1'))

# Check that in CDC, an event appears about a row becoming expired.
# This expiration event should have a special `cdc_operation`: -4 for
# partition delete, -3 for row delete. This is the negative of the
# cdc_operation used for a user's delete operation - 3 or 4.
# The CDC event should also contain the appropriate information about the
# expired row - such as its key and even its content when a preimage is
# requested.
# We run this test twice - with and without clustering keys - to see
# the CDC event is slightly different - -3 vs -4.
@pytest.mark.parametrize('has_ck', [True, False])
def test_row_ttl_expiration_cdc_event(cql, test_keyspace, ttl_period, has_ck):
    typ = "bigint"
    # Create a table with CDC enabled with preimage, and a TTL column "e"
    if has_ck:
        schema = f'p int, c int, x int, e {typ} ttl, primary key (p, c)'
    else:
        schema = f'p int, c int, x int, e {typ} ttl, primary key (p)'
    with new_test_table(cql, test_keyspace, schema, "with cdc = {'enabled': true, 'preimage': true}") as table:
        # Insert a row that should expire ASAP (its expiration time is a
        # minute in the past).
        expiration = to_ttl(typ, time.time()-60)
        cql.execute(f'INSERT INTO {table} (p, c, x, e) values (1,2,3,{expiration})')
        # Wait for the row to disappear (should take roughly one ttl_period)
        deadline = time.time() + 3*ttl_period + 2
        while time.time() < deadline:
            if [] == list(cql.execute(f'SELECT p FROM {table} WHERE p=1')):
                break
            time.sleep(0.1)
        assert [] == list(cql.execute(f'SELECT p FROM {table} WHERE p=1'))
        # Check that we can see the rows's creation and expiration events
        # in the CDC log table. We may need to retry for a short time -
        # although the CDC log entry will be written as soon as the row
        # is deleted, if we're quick enough we may happen to catch a
        # glimpse of the row deleted but the CDC event not yet appearing.
        # We wait until *three* rows (an insert, a pre-image and a deletion).
        deadline = time.time() + 10
        while time.time() < deadline:
            cdc_entries = list(cql.execute(f'SELECT * FROM {table}_scylla_cdc_log'))
            if len(cdc_entries) == 3:
                break
            time.sleep(0.1)
        assert len(cdc_entries) == 3
        # The CDC entries (all of them for the same partition) are sorted
        # by time. We expect the first one to be an insert (cdc_operation=2),
        # the second a preimage (0), and the third a special TTL deletion -
        # -3 or -4 depending on has_ck (this is the negative of a normal user
        # deletion).
        # Insert event originally adding the item:
        assert cdc_entries[0].cdc_operation == 2 # insert
        assert cdc_entries[0].p == 1
        assert cdc_entries[0].c == 2
        assert cdc_entries[0].x == 3
        assert cdc_entries[0].e == expiration
        # Preimage for expiration:
        assert cdc_entries[1].cdc_operation == 0 # preimage
        assert cdc_entries[1].p == 1
        assert cdc_entries[1].c == 2
        assert cdc_entries[1].x == 3
        assert cdc_entries[1].e == expiration
        # The expiration event:
        assert cdc_entries[2].cdc_operation == (-3 if has_ck else -4)
        assert cdc_entries[2].p == 1
        if has_ck:
            assert cdc_entries[2].c == 2

# The following is a minimal test for how per-row TTL increases the following
# three metrics:
#  * scylla_expiration_scan_passes
#  * scylla_expiration_scan_table
#  * scylla_expiration_items_deleted
# The metric scylla_expiration_secondary_ranges_scanned is not tested
# here - it can only be tested on a multi-node cluster with some of its
# nodes down.
def test_row_ttl_metrics(cql, test_keyspace, ttl_period):
    typ = 'bigint'
    with new_test_table(cql, test_keyspace, f'p int primary key, e {typ} ttl, x int') as table:
        start = ScyllaMetrics.query(cql)
        # Insert two rows which are already expired (60 seconds ago), so will
        # be deleted soon, and one row that won't expire.
        e = to_ttl(typ, time.time()-60)
        cql.execute(f'INSERT INTO {table} (p, e) values (1, {e})')
        cql.execute(f'INSERT INTO {table} (p, e) values (2, {e})')
        cql.execute(f'INSERT INTO {table} (p) values (3)')
        # Wait for rows 1 and 2 to expire.
        deadline = time.time() + 3*ttl_period + 2
        while time.time() < deadline:
            if len(list(cql.execute(f'SELECT p from {table}'))) == 1:
                break
            time.sleep(0.1)
        assert len(list(cql.execute(f'SELECT p from {table}'))) == 1
        # After expiration, we expect scylla_expiration_scan_passes and
        # scylla_expiration_scan_tables to have increased by at least one -
        # we must have finished at least one scanning pass to perform this
        # expiration.
        end = ScyllaMetrics.query(cql)
        assert end.get('scylla_expiration_scan_passes') > (start.get('scylla_expiration_scan_passes') or 0)
        assert end.get('scylla_expiration_scan_table') > (start.get('scylla_expiration_scan_table') or 0)
        # scylla_expiration_items_deleted should have increased by 2, for
        # the exactly 2 items deleted (we assume that no other TTL activity
        # is running on the same Scylla instance in parallel...)
        assert end.get('scylla_expiration_items_deleted') == (start.get('scylla_expiration_items_deleted') or 0) + 2
