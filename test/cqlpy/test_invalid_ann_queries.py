# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

###############################################################################
# Tests to check if invalid ANN queries are handled correctly.
# These tests ensure that queries that do not meet the requirements for ANN
# indexing throw the appropriate exceptions.
###############################################################################

import pytest
import re
from .util import new_test_table, is_scylla
from cassandra.protocol import InvalidRequest

ANN_REQUIRES_INDEX_MESSAGE = "ANN ordering by vector requires the column to be indexed"
SCYLLA_ANN_REQUIRES_INDEXED_FILTERING_MESSAGE = "ANN ordering by vector does not support filtering"
CASSANDRA_ANN_REQUIRES_INDEXED_FILTERING_MESSAGE = "ANN ordering by vector requires all restricted column(s) to be indexed"


def test_ann_query_without_index(cql, test_keyspace):
    schema = 'p int primary key, v vector<float, 3>'
    with new_test_table(cql, test_keyspace, schema) as table:
        with pytest.raises(InvalidRequest, match=re.escape(ANN_REQUIRES_INDEX_MESSAGE)):
            cql.execute(f"SELECT * FROM {table} ORDER BY v ANN OF [0.1, 0.2, 0.3] LIMIT 5")


@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #16317")]), "vnodes"],
                         indirect=True)
def test_ann_query_with_ck_filtering(cql, test_keyspace):
    ANN_REQUIRES_INDEXED_FILTERING_MESSAGE = (
        SCYLLA_ANN_REQUIRES_INDEXED_FILTERING_MESSAGE if is_scylla(cql) else CASSANDRA_ANN_REQUIRES_INDEXED_FILTERING_MESSAGE
    )
    schema = 'p int, v vector<float, 3>, ck int, primary key (p, ck)'
    with new_test_table(cql, test_keyspace, schema) as table:
        custom_index = 'vector_index' if is_scylla(cql) else 'sai'
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(v) USING '{custom_index}'")
        with pytest.raises(InvalidRequest, match=re.escape(ANN_REQUIRES_INDEXED_FILTERING_MESSAGE)):
            cql.execute(f"SELECT * FROM {table} WHERE ck = 1 ORDER BY v ANN OF [0.1, 0.2, 0.3] LIMIT 5")
        with pytest.raises(InvalidRequest, match=re.escape(ANN_REQUIRES_INDEXED_FILTERING_MESSAGE)):
            cql.execute(f"SELECT * FROM {table} WHERE ck = 1 ORDER BY v ANN OF [0.1, 0.2, 0.3] LIMIT 5 ALLOW FILTERING")


# Although Cassandra allows for such queries, these queries fail with assertion error.
# In Scylla, such queries are not allowed, as it is unclear if the filtering should happen pre or post ANN search.
@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #16317")]), "vnodes"],
                         indirect=True)
def test_ann_query_not_allow_any_filtering(scylla_only, cql, test_keyspace):
    schema = 'p int primary key, c int, v vector<float, 3>'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(v) USING 'vector_index'")

        cql.execute(f"CREATE INDEX ON {table}(c)")
        with pytest.raises(InvalidRequest, match=re.escape(SCYLLA_ANN_REQUIRES_INDEXED_FILTERING_MESSAGE)):
            cql.execute(f"SELECT * FROM {table} WHERE c = 1 ORDER BY v ANN OF [0.1, 0.2, 0.3] LIMIT 5")
        with pytest.raises(InvalidRequest, match=re.escape(SCYLLA_ANN_REQUIRES_INDEXED_FILTERING_MESSAGE)):
            cql.execute(f"SELECT * FROM {table} WHERE c = 1 ORDER BY v ANN OF [0.1, 0.2, 0.3] LIMIT 5 ALLOW FILTERING")
