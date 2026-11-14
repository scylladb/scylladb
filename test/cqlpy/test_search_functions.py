# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

###############################################################################
# Tests of what applies to every search function, ANN(), BM25() and their
# families, whatever the family and whether or not there is an index. Tests of
# one family are in test_invalid_ann_queries.py, test_fulltext_index.py and the
# *_with_mock.py files.
###############################################################################

import pytest
import re
from .util import new_test_table
from cassandra.protocol import InvalidRequest


@pytest.fixture(scope="module")
def table(cql, test_keyspace, scylla_only):
    schema = 'p int primary key, f float, i int, t text, h frozen<tuple<float, int>>, m map<int, int>, l list<int>, s set<int>'
    with new_test_table(cql, test_keyspace, schema) as table:
        yield table


# Calls prepare(), not execute(), so a test passes only if preparation rejects
# the statement. The arguments are literals, as a column name is a syntax error
# in these positions.
def assert_rejected(cql, statement, call):
    function_name = call[:call.index('(')]
    with pytest.raises(InvalidRequest, match=re.escape(f"{function_name}() is not supported in the ")):
        cql.prepare(statement)


# Each function written to, and compared with, a column of its return type.
def test_reject_search_functions_as_values(cql, table):
    for call, column in [
            ("BM25('a', 'x')", "h"),
            ("BM25_SCORE('a', 'x')", "f"),
            ("BM25_RANK('a', 'x')", "i"),
            ("BM25_HIGHLIGHT('a', 'x')", "t"),
            ("ANN([1.0, 2.0], [1.0, 2.0])", "h"),
            ("ANN_SCORE([1.0, 2.0], [1.0, 2.0])", "f"),
            ("ANN_RANK([1.0, 2.0], [1.0, 2.0])", "i"),
    ]:
        for statement in [
                f"INSERT INTO {table} (p, {column}) VALUES (1, {call})",
                f"UPDATE {table} SET {column} = {call} WHERE p = 1",
                f"UPDATE {table} SET f = 1.0 WHERE p = 1 IF {column} = {call}",
                f"SELECT * FROM {table} WHERE {column} = {call} ALLOW FILTERING",
                f"BEGIN BATCH INSERT INTO {table} (p, {column}) VALUES (1, {call}); APPLY BATCH",
        ]:
            assert_rejected(cql, statement, call)


# The primary key is an int, so a rank is the search value that fits there.
def test_reject_search_functions_in_primary_key(cql, table):
    for call in ["BM25_RANK('a', 'x')", "ANN_RANK([1.0, 2.0], [1.0, 2.0])"]:
        for statement in [
                f"INSERT INTO {table} (p, f) VALUES ({call}, 1.0)",
                f"UPDATE {table} SET f = 1.0 WHERE p = {call}",
                f"DELETE FROM {table} WHERE p = {call}",
                f"SELECT * FROM {table} WHERE p = {call}",
        ]:
            assert_rejected(cql, statement, call)


# A rank is an int, like the collections' elements and keys.
def test_reject_search_functions_in_collection_operations(cql, table):
    for call in ["BM25_RANK('a', 'x')", "ANN_RANK([1.0, 2.0], [1.0, 2.0])"]:
        for statement in [
                f"UPDATE {table} SET m[{call}] = 1 WHERE p = 1",
                f"UPDATE {table} SET m[1] = {call} WHERE p = 1",
                f"UPDATE {table} SET l[{call}] = 1 WHERE p = 1",
                f"UPDATE {table} SET m = m + {{{call}: 1}} WHERE p = 1",
                f"UPDATE {table} SET l = [{call}] + l WHERE p = 1",
                f"UPDATE {table} SET s = s - {{{call}}} WHERE p = 1",
                f"UPDATE {table} SET m = m - {{{call}}} WHERE p = 1",
                f"DELETE m[{call}] FROM {table} WHERE p = 1",
                f"DELETE l[{call}] FROM {table} WHERE p = 1",
        ]:
            assert_rejected(cql, statement, call)


def test_reject_search_function_in_function_argument(cql, table):
    call = "BM25_RANK('a', 'x')"
    assert_rejected(cql, f"INSERT INTO {table} (p, i) VALUES (1, blobAsInt(intAsBlob({call})))", call)
    call = "BM25('a', 'x')"
    assert_rejected(cql, f"INSERT INTO {table} (p, f) VALUES (1, RRF({call}, ANN([1.0, 2.0], [1.0, 2.0])))", call)


# Only the whole left-hand side may be a search call.
def test_reject_search_function_in_where_subscript(cql, table):
    for call in ["BM25_RANK('a', 'x')", "ANN_RANK([1.0, 2.0], [1.0, 2.0])"]:
        assert_rejected(cql, f"SELECT * FROM {table} WHERE m[{call}] = 1 ALLOW FILTERING", call)


def test_reject_search_function_in_search_relation_rhs(cql, table):
    assert_rejected(cql, f"SELECT * FROM {table} WHERE BM25(t, 'x') > BM25_SCORE('a', 'y')", "BM25_SCORE('a', 'y')")
