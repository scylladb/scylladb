# -*- coding: utf-8 -*-
# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

#############################################################################
# Tests for configuration of compressed sstables
#############################################################################

import pytest
import nodetool
from util import new_test_table
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
