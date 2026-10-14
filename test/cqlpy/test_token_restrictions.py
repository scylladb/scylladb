# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Tests for restrictions on token() in the WHERE clause, and in particular
# their combination with restrictions on the partition key columns.
# See also test_scan.py, test_allow_filtering.py, test_secondary_index.py,
# test_materialized_view.py and test_lwt.py for more tests involving token
# restrictions.
#############################################################################

import pytest
from .util import new_test_table
from cassandra.protocol import InvalidRequest
from cassandra.query import SimpleStatement

# Tests for combining a restriction on token(p) with restrictions on the
# partition key column p itself, which Cassandra allows since version 2.2
# (CASSANDRA-7016). Reproduces issue #2060.
# When all the partition key columns are restricted by EQ or IN, the
# partitions they name are filtered by the token restrictions:
@pytest.fixture(scope="module")
def table_token(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int, c int, PRIMARY KEY (p, c)') as table:
        for p in range(6):
            for c in range(2):
                # The c values are different in different partitions, so
                # ORDER BY c has no ties.
                cql.execute(f'INSERT INTO {table} (p, c) VALUES ({p}, {p*10+c})')
        # The partitions, sorted by token, and their tokens:
        tokens = sorted({(r[0], r[1]) for r in cql.execute(f'SELECT token(p), p FROM {table}')})
        yield table, [p for t, p in tokens], [t for t, p in tokens]

def test_token_and_partition_key_eq(cql, table_token):
    table, ps, tokens = table_token
    p, t = ps[2], tokens[2]
    assert list(cql.execute(f'SELECT p, c FROM {table} WHERE p = {p} AND token(p) <= {t}')) == [(p, p*10), (p, p*10+1)]
    assert list(cql.execute(f'SELECT p, c FROM {table} WHERE p = {p} AND token(p) < {t}')) == []
    assert list(cql.execute(f'SELECT p, c FROM {table} WHERE p = {p} AND token(p) = {t}')) == [(p, p*10), (p, p*10+1)]
    assert list(cql.execute(f'SELECT p, c FROM {table} WHERE token(p) = token({p}) AND p = {p}')) == [(p, p*10), (p, p*10+1)]
    assert list(cql.execute(f'SELECT p, c FROM {table} WHERE p = {p} AND token(p) > {t}')) == []

def test_token_and_partition_key_in(cql, table_token):
    table, ps, tokens = table_token
    def query(where):
        stmt = SimpleStatement(f'SELECT p, c FROM {table} WHERE p IN (0, 1, 2, 3, 4, 5) AND {where}', fetch_size=None)
        return list(cql.execute(stmt))
    def rows(partitions, c_offsets=(0, 1)):
        return sorted((p, p*10+j) for p in partitions for j in c_offsets)
    assert sorted(query(f'token(p) > {tokens[2]}')) == rows(ps[3:])
    assert sorted(query(f'token(p) >= {tokens[2]}')) == rows(ps[2:])
    assert sorted(query(f'token(p) >= {tokens[1]} AND token(p) <= {tokens[3]}')) == rows(ps[1:4])
    assert sorted(query(f'token(p) > {tokens[1]} AND token(p) < {tokens[3]}')) == rows(ps[2:3])
    # Combined with a restriction on a clustering column, with ORDER BY and
    # with LIMIT:
    assert sorted(query(f'token(p) > {tokens[2]} AND c > 0 AND c < 50')) == [(p, c) for p, c in rows(ps[3:]) if 0 < c < 50]
    assert query(f'token(p) > {tokens[2]} ORDER BY c DESC') == list(reversed(rows(ps[3:])))
    assert query(f'token(p) > {tokens[2]} ORDER BY c ASC LIMIT 3') == rows(ps[3:])[:3]
    # With bind markers:
    stmt = cql.prepare(f'SELECT p, c FROM {table} WHERE p IN (?, ?) AND token(p) > ?')
    assert sorted(cql.execute(stmt, [ps[1], ps[3], tokens[2]])) == rows([ps[3]])

# When the token restrictions contradict each other, no partitions are
# returned - the same as when there are only token restrictions. Cassandra
# 5 is different, it returns partitions with tokens outside the range
# (it was fixed in Cassandra 6).
def test_token_and_partition_key_empty_token_range(cql, table_token, new_to_cassandra_6):
    table, ps, tokens = table_token
    assert list(cql.execute(f'SELECT p, c FROM {table} WHERE token(p) > {tokens[3]} AND token(p) < {tokens[1]}')) == []
    stmt = SimpleStatement(f'SELECT p, c FROM {table} WHERE p IN (0, 1, 2, 3, 4, 5) AND token(p) > {tokens[3]} AND token(p) < {tokens[1]}', fetch_size=None)
    assert list(cql.execute(stmt)) == []

# The same with a partition key with more than one column, with EQ and IN
# restrictions on its columns:
def test_token_and_compound_partition_key(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int, q int, c int, PRIMARY KEY ((p, q), c)') as table:
        for p in range(3):
            for q in range(3):
                cql.execute(f'INSERT INTO {table} (p, q, c) VALUES ({p}, {q}, 0)')
        tokens = {(r[0], r[1]): r[2] for r in cql.execute(f'SELECT p, q, token(p, q) FROM {table}')}
        t = sorted(tokens.values())[4]
        expected = sorted(k for k, v in tokens.items() if v <= t)
        stmt = SimpleStatement(f'SELECT p, q FROM {table} WHERE p IN (0, 1, 2) AND q IN (0, 1, 2) AND token(p, q) <= {t}', fetch_size=None)
        assert sorted(cql.execute(stmt)) == expected
        for (p, q), tok in tokens.items():
            assert list(cql.execute(f'SELECT p, q FROM {table} WHERE p = {p} AND q = {q} AND token(p, q) <= {t}')) == ([(p, q)] if tok <= t else [])

# When not all the partition key columns are restricted by EQ or IN, the
# token restrictions define the range of partitions to scan, and the
# restrictions on the partition key columns are filtered, so ALLOW FILTERING
# is required.
def test_token_and_partial_partition_key(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int, q int, c int, PRIMARY KEY ((p, q), c)') as table:
        for p in range(3):
            for q in range(3):
                cql.execute(f'INSERT INTO {table} (p, q, c) VALUES ({p}, {q}, 0)')
        tokens = {(r[0], r[1]): r[2] for r in cql.execute(f'SELECT p, q, token(p, q) FROM {table}')}
        t = sorted(tokens.values())[4]
        with pytest.raises(InvalidRequest, match='ALLOW FILTERING'):
            cql.execute(f'SELECT p, q FROM {table} WHERE p = 1 AND token(p, q) <= {t}')
        assert sorted(cql.execute(f'SELECT p, q FROM {table} WHERE p = 1 AND token(p, q) <= {t} ALLOW FILTERING')) == \
            sorted(k for k, v in tokens.items() if v <= t and k[0] == 1)
        assert sorted(cql.execute(f'SELECT p, q FROM {table} WHERE p > 0 AND token(p, q) <= {t} ALLOW FILTERING')) == \
            sorted(k for k, v in tokens.items() if v <= t and k[0] > 0)

# Cassandra allows UPDATE and DELETE to restrict the token in addition to
# all the partition key columns, and the write is only done if the token
# restriction is satisfied.
@pytest.mark.xfail(reason="SCYLLADB-5388")
def test_token_and_partition_key_update_delete(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int, c int, v int, PRIMARY KEY (p, c)') as table:
        cql.execute(f'INSERT INTO {table} (p, c, v) VALUES (1, 1, 0)')
        t = list(cql.execute(f'SELECT token(p) FROM {table} WHERE p = 1'))[0][0]
        cql.execute(f'UPDATE {table} SET v = 7 WHERE p = 1 AND c = 1 AND token(p) > {t}')
        assert list(cql.execute(f'SELECT v FROM {table} WHERE p = 1 AND c = 1')) == [(0,)]
        cql.execute(f'UPDATE {table} SET v = 8 WHERE p = 1 AND c = 1 AND token(p) = {t}')
        assert list(cql.execute(f'SELECT v FROM {table} WHERE p = 1 AND c = 1')) == [(8,)]
        cql.execute(f'DELETE FROM {table} WHERE p = 1 AND c = 1 AND token(p) > {t}')
        assert list(cql.execute(f'SELECT v FROM {table} WHERE p = 1 AND c = 1')) == [(8,)]
        cql.execute(f'DELETE FROM {table} WHERE p = 1 AND c = 1 AND token(p) = {t}')
        assert list(cql.execute(f'SELECT v FROM {table} WHERE p = 1 AND c = 1')) == []
