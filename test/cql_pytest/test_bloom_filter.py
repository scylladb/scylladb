# Copyright 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import pytest
from . import rest_api
from . import nodetool
from .util import new_test_table

# Test inserts `N` rows into table, flushes it 
# and tries to read `M` non-existing keys.
# Then bloom filter's false-positive ratio is checked.
@pytest.mark.parametrize("N,M,fp_chance", [(500, 1000, 0.1)])
def test_bloom_filter(scylla_only, cql, test_keyspace, N, M, fp_chance):
    with new_test_table(cql, test_keyspace, "a int PRIMARY KEY", 
        f"WITH bloom_filter_fp_chance = {fp_chance}") as table:
        
        stmt = cql.prepare(f"INSERT INTO {table} (a) VALUES(?)")
        for k in range(N):
            cql.execute(stmt, [k])
        nodetool.flush(cql, table)
        
        read_stmt = cql.prepare(f"SELECT * FROM {table} WHERE a = ? BYPASS CACHE")
        for k in range(N, N+M):
            cql.execute(read_stmt, [k])

        fp = rest_api.get_column_family_metric(cql, 
          "bloom_filter_false_positives", table)
        ratio = fp / M
        assert ratio >= fp_chance * 0.7 and ratio <= fp_chance * 1.15
            
