# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

#############################################################################
# Tests the calculation of partition range and slice.

import pytest
from util import new_test_table

def test_delete_where_empty_IN(cql, test_keyspace):
    """Tests that DELETE FROM t WHERE p IN () is allowed.  See #9311."""
    with new_test_table(cql, test_keyspace, "p int, PRIMARY KEY (p)") as table:
        cql.execute(f"DELETE FROM {table} WHERE p IN ()")
