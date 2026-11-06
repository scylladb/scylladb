# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Tests the calculation of partition range and slice.

import pytest
from cassandra.protocol import InvalidRequest
from .util import new_test_table

def test_delete_where_empty_IN(cql, test_keyspace):
    """Tests that DELETE FROM t WHERE p IN () is allowed.  See #9311."""
    with new_test_table(cql, test_keyspace, "p int, PRIMARY KEY (p)") as table:
        cql.execute(f"DELETE FROM {table} WHERE p IN ()")

# Ported from scylla-dtest deletion_test.py::TestRangeDeletion::test_delete_by_2ck_range_failure.
# A DELETE may not restrict a clustering column after one restricted by a
# slice (non-EQ relation), whatever the relation on the later column, nor
# restrict the second clustering column with the first one unrestricted.
# Like the dtest, for each compaction strategy of the table, in a vnodes
# keyspace.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("test_keyspace", ["vnodes"], indirect=True)
@pytest.mark.parametrize("compaction_strategy", ["LeveledCompactionStrategy", "SizeTieredCompactionStrategy", "TimeWindowCompactionStrategy"])
def test_delete_restricting_clustering_column_after_slice(cql, test_keyspace, compaction_strategy):
    with new_test_table(cql, test_keyspace, "pk1 int, ck1 int, ck2 varchar, v1 int, PRIMARY KEY(pk1, ck1, ck2)",
                        f"WITH compaction = {{'class': '{compaction_strategy}' }}") as table:
        with pytest.raises(InvalidRequest, match='preceding column "?ck1"? is restricted by a non-EQ relation'):
            cql.execute(f"DELETE FROM {table} WHERE pk1=0 and ck1 > 3 and ck2 = 'ck3'")
        with pytest.raises(InvalidRequest, match='cannot be restricted as preceding column "?ck1"? is not restricted'):
            cql.execute(f"DELETE FROM {table} WHERE pk1=0 and ck2 > 'ck3'")
        with pytest.raises(InvalidRequest, match='preceding column "?ck1"? is restricted by a non-EQ relation'):
            cql.execute(f"DELETE FROM {table} WHERE pk1=0 and ck1 > 3 and ck2 > 'ck3'")
        with pytest.raises(InvalidRequest, match='preceding column "?ck1"? is restricted by a non-EQ relation'):
            cql.execute(f"DELETE FROM {table} WHERE pk1=0 and ck1 > 3 and ck2 in ('ck3')")
