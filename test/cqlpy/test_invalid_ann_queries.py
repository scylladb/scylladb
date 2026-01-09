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

def test_ann_query_with_null_vector(cql, test_keyspace):
    schema = 'p int primary key, c int, v vector<float, 3>'
    custom_index = 'vector_index' if is_scylla(cql) else 'sai'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(v) USING '{custom_index}'")

        with pytest.raises(InvalidRequest, match="Unsupported null value for column v"):
            cql.execute(f"SELECT * FROM {table} ORDER BY v ANN OF null LIMIT 5")
