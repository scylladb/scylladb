# -*- coding: utf-8 -*-
# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

#############################################################################
# Tests for configuration of compressed sstables
#############################################################################

import pytest
from . import nodetool
from .util import new_test_table, new_materialized_view, is_scylla
from cassandra.protocol import ConfigurationException, SyntaxException

# In older Cassandra and Scylla, the name of the compression algorithm was
# given as a "sstable_compression" attribute, but newer Cassandra switched
# to "class". Check that we support this new name class.
# Reproduces #8948.
@pytest.mark.xfail(reason="#8948")
def test_compression_class(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int primary key, v int", "with compression = { 'class': 'LZ4Compressor' }") as table:
        pass

# In the following tests, we use the older "sstable_compression" option name
# (instead of the new "class") so we can have passing tests despite #8948.
# When both Scylla and Cassandra support "class", we should modify this variable
# to use it:
sstable_compression = 'sstable_compression'

@pytest.fixture(scope="module")
def table_lz4(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int primary key, v int", "with compression = { '" + sstable_compression + "': 'LZ4Compressor' }") as table:
        yield table

# Test that if we have a table with lz4 compression, it has the expected
# compression "class" in its schema table. Note that even if the older
# "sstable_compression" attribute was used to set the compression class,
# when reading the schema we should see "class".
# Reproduces #8948.
@pytest.mark.xfail(reason="#8948")
def test_read_compression_class(cql, table_lz4):
    [ks, cf] = table_lz4.split('.')
    opts = cql.execute(f"SELECT compression FROM system_schema.tables WHERE keyspace_name='{ks}' AND table_name='{cf}'").one().compression
    assert 'class' in opts
    assert opts['class'] == 'org.apache.cassandra.io.compress.LZ4Compressor'

# When creating a compressed table without specifying chunk_length_in_kb
# explicitly, some default value is nevertheless used, and its value should
# be readable from the schema.
# Reproduces #6442.
@pytest.mark.xfail(reason="#6442")
def test_read_chunk_length(cql, table_lz4):
    [ks, cf] = table_lz4.split('.')
    opts = cql.execute(f"SELECT compression FROM system_schema.tables WHERE keyspace_name='{ks}' AND table_name='{cf}'").one().compression
    assert 'chunk_length_in_kb' in opts

# Both Cassandra and Scylla only allow chunk_length_in_kb to be set a power
# of two.
def test_chunk_length_must_be_power_of_two(cql, test_keyspace):
    with pytest.raises(ConfigurationException, match='power of 2'):
        with new_test_table(cql, test_keyspace, "p int primary key, v int", "with compression = { '" + sstable_compression + "': 'LZ4Compressor', 'chunk_length_in_kb': 100 }") as table:
            pass

# chunk_length_in_kb cannot be zero, negative, null, or non-integer.
# Surprisingly, Scylla allows floating-point numbers (and truncates them).
# It shouldn't, and Cassandra doesn't, so this case is "xfail" below.
@pytest.mark.parametrize("garbage", ["0", "-1", "null", "'dog'",
        pytest.param("1.1", marks=pytest.mark.xfail(reason='Scylla truncates float chunk length'))])
def test_chunk_length_invalid(cql, test_keyspace, garbage):
    # The error should usually be ConfigurationException, but strangely
    # Cassandra throws a SyntaxException in the "null" case.
    with pytest.raises((ConfigurationException, SyntaxException), match='chunk_length_in_kb'):
        with new_test_table(cql, test_keyspace, "p int primary key, v int", "with compression = { '" + sstable_compression + "': 'LZ4Compressor', 'chunk_length_in_kb': " + garbage + " }") as table:
            pass

# If a user is allowed to specify a huge number for chunk_length_in_kb, it can
# result in unbounded allocations and potentially crashing Scylla. Therefore,
# there ought to be some limit for the configured chunk length. Let's check it
# by trying a ridiculously large value, which shouldn't be legal.
# This test fails on Cassandra, which doesn't have protection against huge
# chunk sizes, so the test is marked a "cassandra_bug".
# Reproduces #9933.
def test_huge_chunk_length(cql, test_keyspace, cassandra_bug):
    with pytest.raises(ConfigurationException, match='chunk_length_in_kb'):
        with new_test_table(cql, test_keyspace, "p int primary key, v int", "with compression = { '" + sstable_compression + "': 'LZ4Compressor', 'chunk_length_in_kb': 1048576 }") as table:
            # At this point, the test already failed, as we expected the table
            # creation to have failed with ConfigurationException. But if we
            # reached here, let's really demonstrate the bug - write
            # something and flush it, to have sstable compression actually
            # be used.
            cql.execute(f'INSERT INTO {table} (p, v) VALUES (1, 2)')
            nodetool.flush(cql, table)
    # Also check the same for ALTER TABLE
    with new_test_table(cql, test_keyspace, "p int primary key, v int") as table:
        with pytest.raises(ConfigurationException, match='chunk_length_in_kb'):
            cql.execute("ALTER TABLE " + table + " with compression = { '" + sstable_compression + "': 'LZ4Compressor', 'chunk_length_in_kb': 1048576 }")

# Check that whatever is currently the default sstable compression algorithm
# (for a long time it was LZ4Compressor but issue #26610 changed it to
# LZ4WithDictsCompressor), the base table and all its auxiliary tables -
# materialized view, secondary index and CDC log table - should use the same
# compression. We don't care if this happens because all these tables use
# the same default - or alternatively because the auxiliary tables "inherit"
# the configuration of the base table (#20388). Both are fine for this test.
#
# If the test runs on Cassandra, only the materialized view part of the test
# is done - the CDC and secondary index implementation on Cassandra is
# different and does not have an ordinary table backing it.
# Reproduces issue #26914
def test_aux_tables_compression_parity(cql, test_keyspace):
    extra = "with cdc = {'enabled': true}" if is_scylla(cql) else ""
    with new_test_table(cql, test_keyspace, "p int primary key, v int", extra) as table:
        with new_materialized_view(cql, table, '*', 'v, p', 'v is not null and p is not null') as mv:
            cql.execute(f'CREATE INDEX ON {table}(v)')
            # base table's compression setting:
            ks, cf = table.split('.')
            r = list(cql.execute(f"SELECT compression FROM system_schema.tables WHERE  keyspace_name='{ks}' and table_name='{cf}'"))
            assert len(r) == 1
            base_compression = r[0].compression
            # view table's compression setting:
            _, view = mv.split('.')
            r = list(cql.execute(f"SELECT compression FROM system_schema.views WHERE  keyspace_name='{ks}' and view_name='{view}'"))
            assert len(r) == 1
            view_compression = r[0].compression
            if extra: # Scylla-only part
                # secondary index's backing view's compression setting:
                r = list(cql.execute(f"SELECT compression FROM system_schema.views WHERE  keyspace_name='{ks}' and view_name='{cf}_v_idx_index'"))
                assert len(r) == 1
                index_compression = r[0].compression
                assert view_compression == index_compression
                # CDC log table's compression setting:
                r = list(cql.execute(f"SELECT compression FROM system_schema.tables WHERE  keyspace_name='{ks}' and table_name='{cf}_scylla_cdc_log'"))
                assert len(r) == 1
                cdc_compression = r[0].compression
                assert view_compression == cdc_compression
            assert base_compression == view_compression
