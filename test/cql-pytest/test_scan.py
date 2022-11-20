# Copyright 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

#############################################################################
# Tests for scanning SELECT requests (which read many rows and/or many
# partitions).
# We have a separate test file test_filtering.py for scans which also involve
# filtering, and test_allow_filtering.py for checking when "ALLOW FILTERING"
# is needed in scan. test_secondary_index.py also contains tests for scanning
# using a secondary index.
#############################################################################

import pytest
from util import new_test_table
from cassandra.query import SimpleStatement

# Regression test for #9482
def test_scan_ending_with_static_row(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "pk int, ck int, s int STATIC, v int, PRIMARY KEY (pk, ck)") as table:
        stmt = cql.prepare(f"UPDATE {table} SET s = ? WHERE pk = ?")
        for pk in range(100):
            cql.execute(stmt, (0, pk))

        statement = SimpleStatement(f"SELECT * FROM {table}", fetch_size=10)
        # This will trigger an error in either processing or building the query
        # results. The success criteria for this test is the query finishing
        # without errors.
        res = list(cql.execute(statement))


# Test that if we have multi-column restrictions on the clustering key
# and additional filtering on regular columns, both restrictions are obeyed.
# Reproduces #6200.
def test_multi_column_restrictions_and_filtering(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int, c1 int, c2 int, r int, PRIMARY KEY (p, c1, c2)") as table:
        stmt = cql.prepare(f"INSERT INTO {table} (p, c1, c2, r) VALUES (1, ?, ?, ?)")
        for i in range(2):
            for j in range(2):
                cql.execute(stmt, [i, j, j])
        assert list(cql.execute(f"SELECT c1,c2,r FROM {table} WHERE p=1 AND (c1, c2) = (0,1)")) == [(0,1,1)]
        # Since in that result r=1, adding "AND r=1" should return the same
        # result, and adding "AND r=0" should return nothing.
        assert list(cql.execute(f"SELECT c1,c2,r FROM {table} WHERE p=1 AND (c1, c2) = (0,1) AND r=1 ALLOW FILTERING")) == [(0,1,1)]
        # Reproduces #6200:
        assert list(cql.execute(f"SELECT c1,c2,r FROM {table} WHERE p=1 AND (c1, c2) = (0,1) AND r=0 ALLOW FILTERING")) == []

# Test that if we have a range multi-column restrictions on the clustering key
# and additional filtering on regular columns, both restrictions are obeyed.
# Similar to test_multi_column_restrictions_and_filtering, but uses a range
# restriction on the clustering key columns.
# Reproduces #12014, the code is taken from a reproducer provided by a user.
def test_multi_column_range_restrictions_and_filtering(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "pk int, ts timestamp, id int, processed boolean, PRIMARY KEY (pk, ts, id)") as table:
        cql.execute(f"INSERT INTO {table} (pk, ts, id, processed) VALUES (0, currentTimestamp(), 0, true)")
        cql.execute(f"INSERT INTO {table} (pk, ts, id, processed) VALUES (0, currentTimestamp(), 1, true)")
        cql.execute(f"INSERT INTO {table} (pk, ts, id, processed) VALUES (0, currentTimestamp(), 2, false)")
        cql.execute(f"INSERT INTO {table} (pk, ts, id, processed) VALUES (0, currentTimestamp(), 3, false)")
        # This select doesn't use multi-column restrictions, the result shouldn't change when it does.
        rows1 = list(cql.execute(f"SELECT id, processed FROM {table} WHERE pk = 0 AND ts >= 0 AND processed = false ALLOW FILTERING"))
        assert rows1 == [(2, False), (3, False)]
        # Reproduces #12014
        rows2 = list(cql.execute(f"SELECT id, processed FROM {table} WHERE pk = 0 AND (ts, id) >= (0, 0) AND processed = false ALLOW FILTERING"))
        assert rows1 == rows2
