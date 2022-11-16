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
