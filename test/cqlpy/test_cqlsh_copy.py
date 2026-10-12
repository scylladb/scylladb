# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Tests for cqlsh's COPY TO and COPY FROM commands, which export a table to a
# CSV file and import a CSV file into a table. COPY is implemented entirely
# in cqlsh (cqlshlib/copyutil.py), so these tests run the in-tree cqlsh
# (bin/cqlsh) against the server and check the files it writes and the data
# it loads.
#############################################################################

from test import DEBUG_MODES
import csv

import pytest

from .util import new_test_table, run_cqlsh


def write_csv(path, rows, **kwargs):
    with open(path, 'w', newline='', encoding='utf-8') as f:
        csv.writer(f, **kwargs).writerows(rows)


# Run the given cqlsh commands in tmp_path, and check that cqlsh succeeded.
def cqlsh(cql, tmp_path, cmds, **kwargs):
    res = run_cqlsh(cql, cmds, cwd=tmp_path, **kwargs)
    assert res.returncode == 0, f'cqlsh failed:\n{res.stdout}\n{res.stderr}'
    return res


# Test that COPY FROM WITH SKIPCOLS loads only the columns that aren't skipped.
# Loading counters isn't idempotent: each COPY FROM adds the file's values to
# the counters again.
# Reproduces CASSANDRA-9303.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("test_keyspace", ["tablets", "vnodes"], indirect=True)
def test_copy_from_counter_skip_columns(cql, test_keyspace, tmp_path):
    with new_test_table(cql, test_keyspace, 'a int PRIMARY KEY, b counter, c counter, d counter, e counter') as table:
        csv_file = tmp_path / 'in.csv'
        write_csv(csv_file, [(1, 1, 1, 1, 1), (2, 1, 1, 1, 1)])
        def copy_from(skip_cols, expected):
            cqlsh(cql, tmp_path, f"COPY {table} FROM '{csv_file}' WITH SKIPCOLS = '{skip_cols}'")
            assert sorted(cql.execute(f'SELECT a, b, c, d, e FROM {table}')) == expected
        copy_from('c, d, e', [(1, 1, None, None, None), (2, 1, None, None, None)])
        copy_from('b', [(1, 1, 1, 1, 1), (2, 1, 1, 1, 1)])
        copy_from('b', [(1, 1, 2, 2, 2), (2, 1, 2, 2, 2)])
        copy_from('e', [(1, 2, 3, 3, 2), (2, 2, 3, 3, 2)])


# Test that COPY FROM WITH HEADER = true skips the first line of the file.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("test_keyspace", ["vnodes"], indirect=True)
def test_copy_from_with_header(cql, test_keyspace, tmp_path):
    with new_test_table(cql, test_keyspace, 'a int PRIMARY KEY, b int') as table:
        data = [(1, 20), (2, 40), (3, 60), (4, 80)]
        csv_file = tmp_path / 'in.csv'
        write_csv(csv_file, [('a', 'b')] + data)
        cqlsh(cql, tmp_path, f"COPY {table} FROM '{csv_file}' WITH HEADER = true")
        assert sorted(cql.execute(f'SELECT a, b FROM {table}')) == data


# Test that COPY FROM rejects a CSV row with more columns than the table has,
# and that it reports the error and loads nothing.
# Reproduces CASSANDRA-9302.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("test_keyspace", ["vnodes"], indirect=True)
def test_copy_from_wrong_number_of_columns(cql, test_keyspace, tmp_path):
    with new_test_table(cql, test_keyspace, 'a int PRIMARY KEY, b int') as table:
        csv_file = tmp_path / 'in.csv'
        write_csv(csv_file, [(1, 2, 3)])
        res = run_cqlsh(cql, f"COPY {table} FROM '{csv_file}'", cwd=tmp_path)
        assert res.returncode != 0
        assert 'Failed to import 1 rows: ParseError - Invalid row length 3 should be 2' in res.stderr
        assert list(cql.execute(f'SELECT * FROM {table}')) == []
