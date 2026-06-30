# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

###############################################################################
# Tests for vector search (SELECT with ANN ordering).
#
# These tests use a mock vector store HTTP server to verify that Scylla
# correctly translates CQL queries with ANN ordering into HTTP requests
# for the vector store service and returns the expected results.
###############################################################################

from http import HTTPStatus
from itertools import combinations
import json

import pytest
from cassandra.protocol import InvalidRequest
from cassandra.query import SimpleStatement

from .util import new_test_table, unique_name


# Verify that partition key IN restriction is forwarded to the vector store.
def test_vector_search_ann_with_partition_key_in_restriction(cql, test_keyspace, vector_store_mock, skip_without_tablets):

    schema = "pk1 tinyint, pk2 tinyint, ck1 tinyint, ck2 tinyint, embedding vector<float, 3>, PRIMARY KEY ((pk1, pk2), ck1, ck2)"

    with new_test_table(cql, test_keyspace, schema) as table:
        index_name = unique_name()
        cql.execute(
            f"CREATE CUSTOM INDEX {index_name} ON {table}(embedding) USING 'vector_index'")
        cql.execute(
            f"INSERT INTO {table} (pk1, pk2, ck1, ck2, embedding) VALUES (5, 7, 9, 2, [0.1, 0.2, 0.3])")
        vector_store_mock.set_next_ann_response(200, json.dumps({"primary_keys": {
                                                "pk1": [5], "pk2": [7], "ck1": [9], "ck2": [2]}, "similarity_scores": [0.1]}))

        result = cql.execute(
            f"SELECT pk1, pk2, ck1, ck2 FROM {table} WHERE pk1 IN (5, 6) ORDER BY embedding ANN OF [0.1, 0.2, 0.3] LIMIT 2")

        # Assert CQL SELECT results are returned according to the vector store mock response.
        assert list(result) == [(5, 7, 9, 2)]

        # Assert Scylla sent the expected ANN request to the vector store mock.
        requests = vector_store_mock.ann_requests
        assert len(requests) == 1
        assert requests[0].path == f"/api/v1/indexes/{test_keyspace}/{index_name}/ann"
        assert json.loads(requests[0].body) == {
            "vector": [0.1, 0.2, 0.3],
            "limit": 2,
            "filter": {
                "restrictions": [{"type": "IN", "lhs": "pk1", "rhs": [5, 6]}],
                "allow_filtering": False,
            },
        }


# Verify that HTTP error responses from the vector store are propagated through CQL InvalidRequest.
def test_vector_search_cql_error_contains_http_error_description(cql, test_keyspace, vector_store_mock, skip_without_tablets):

    schema = "pk1 tinyint, pk2 tinyint, ck1 tinyint, ck2 tinyint, embedding vector<float, 3>, PRIMARY KEY ((pk1, pk2), ck1, ck2)"

    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(
            f"CREATE CUSTOM INDEX ON {table}(embedding) USING 'vector_index'")

        vector_store_mock.set_next_ann_response(HTTPStatus.NOT_FOUND, "index does not exist")

        with pytest.raises(InvalidRequest, match="404.*index does not exist"):
            cql.execute(
                f"SELECT * FROM {table} ORDER BY embedding ANN OF [0.1, 0.2, 0.3] LIMIT 5")


# Create a vector index with an additional filtering column.
# Because the local secondary index logic was used to determine the index target column,
# the implementation wrongly selects last column as the target(vectors) column, leading to
# an exception on the SELECT query:
#     ANN ordering by vector requires the column to be indexed using 'vector_index'.
# Reproduces SCYLLADB-635.
def test_vector_search_vector_index_with_additional_filtering_column(cql, test_keyspace, vector_store_mock, skip_without_tablets):

    schema = "pk1 tinyint, pk2 tinyint, ck1 tinyint, ck2 tinyint, embedding vector<float, 3>, PRIMARY KEY ((pk1, pk2), ck1, ck2)"

    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(
            f"CREATE CUSTOM INDEX ON {table}(embedding, ck1) USING 'vector_index'")

        cql.execute(
            f"SELECT * FROM {table} ORDER BY embedding ANN OF [0.1, 0.2, 0.3] LIMIT 5")


def test_vector_search_local_vector_index_create_and_query_do_not_fail(cql, test_keyspace, vector_store_mock, skip_without_tablets):

    schema = "pk1 tinyint, pk2 tinyint, ck1 tinyint, ck2 tinyint, embedding vector<float, 3>, PRIMARY KEY ((pk1, pk2), ck1, ck2)"

    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(
            f"CREATE CUSTOM INDEX ON {table}((pk1, pk2), embedding) USING 'vector_index'")

        cql.execute(
            f"SELECT * FROM {table} WHERE pk1 = 1 AND pk2 = 2 ORDER BY embedding ANN OF [0.1, 0.2, 0.3] LIMIT 5")


# Verify that a paging warning is emitted when page size is smaller than LIMIT.
def test_vector_search_paging_warning_when_page_size_smaller_than_limit(cql, test_keyspace, vector_store_mock, skip_without_tablets):

    schema = "pk1 tinyint, pk2 tinyint, ck1 tinyint, ck2 tinyint, embedding vector<float, 3>, PRIMARY KEY ((pk1, pk2), ck1, ck2)"

    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(
            f"CREATE CUSTOM INDEX ON {table}(embedding) USING 'vector_index'")

        result = cql.execute(SimpleStatement(
            f"SELECT * FROM {table} ORDER BY embedding ANN OF [0.1, 0.2, 0.3] LIMIT 100", fetch_size=5))

        warnings = result.response_future.warnings
        assert warnings
        assert len(warnings) == 1
        assert "Paging is not supported for Vector Search queries. The entire result set has been returned." == warnings[0]


# Verify no paging warning is emitted when paging is disabled (fetch_size=0).
def test_vector_search_no_paging_warning_when_paging_disabled(cql, test_keyspace, vector_store_mock, skip_without_tablets):

    schema = "pk1 tinyint, pk2 tinyint, ck1 tinyint, ck2 tinyint, embedding vector<float, 3>, PRIMARY KEY ((pk1, pk2), ck1, ck2)"

    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(
            f"CREATE CUSTOM INDEX ON {table}(embedding) USING 'vector_index'")

        result = cql.execute(SimpleStatement(
            f"SELECT * FROM {table} ORDER BY embedding ANN OF [0.1, 0.2, 0.3] LIMIT 100", fetch_size=0))

        assert not result.response_future.warnings


# Verify no paging warning is emitted when LIMIT is less than page size.
def test_vector_search_no_paging_warning_when_limit_less_than_page_size(cql, test_keyspace, vector_store_mock, skip_without_tablets):

    schema = "pk1 tinyint, pk2 tinyint, ck1 tinyint, ck2 tinyint, embedding vector<float, 3>, PRIMARY KEY ((pk1, pk2), ck1, ck2)"

    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(
            f"CREATE CUSTOM INDEX ON {table}(embedding) USING 'vector_index'")

        result = cql.execute(SimpleStatement(
            f"SELECT * FROM {table} ORDER BY embedding ANN OF [0.1, 0.2, 0.3] LIMIT 5", fetch_size=100))

        assert not result.response_future.warnings

# Vector Search allows filtering on partition and clustering key columns in any combination.
# It also assumes responsibility for filtering, so post-filtering in the coordinator is not allowed:
# only selected columns are retrieved from the base table, and remaining columns are unavailable for filtering.
# 
# This test verifies all the combinations of partition key and clustering key columns filtering.
# It reproduces bugs from when post filtering was not disabled in general and the normal filtering logic was used.
# In that case some combinations triggered post-filtering, which failed or crashed (on primary key columns)
# due to missing columns in the SELECT result set.
#
# Reproduces SCYLLADB-2737 (all cases without pk1 or pk2)
def test_vector_search_ann_with_restricted_key_column_not_selected(cql, test_keyspace, vector_store_mock, skip_without_tablets):

    schema = "pk1 tinyint, pk2 tinyint, ck1 tinyint, ck2 tinyint, category int, embedding vector<float, 3>, PRIMARY KEY ((pk1, pk2), ck1, ck2)"

    with new_test_table(cql, test_keyspace, schema) as table:
        index_name = unique_name()
        cql.execute(
            f"CREATE CUSTOM INDEX {index_name} ON {table}(embedding) USING 'vector_index'")
        cql.execute(
            f"INSERT INTO {table} (pk1, pk2, ck1, ck2, category, embedding) VALUES (5, 7, 9, 1, 7, [0.1, 0.2, 0.3])")
        cql.execute(
            f"INSERT INTO {table} (pk1, pk2, ck1, ck2, category, embedding) VALUES (5, 7, 9, 2, 8, [0.1, 0.2, 0.3])")
        vector_store_mock.set_next_ann_response(200, json.dumps({
            "primary_keys": {"pk1": [5, 5], "pk2": [7, 7], "ck1": [9, 9], "ck2": [2, 1]},
            "similarity_scores": [0.2, 0.1],
        }))

        restriction_tokens = [
            "pk1 = 5",
            "pk2 = 7",
            "ck1 = 9",
            "ck2 IN (1, 2)",
        ]

        for size in range(1, len(restriction_tokens) + 1):
            for subset in combinations(restriction_tokens, size):
                where_clause = " AND ".join(subset)
                print(f"Testing ANN query with restrictions: {where_clause}")
                result = cql.execute(
                    f"SELECT category FROM {table} WHERE {where_clause} ORDER BY embedding ANN OF [0.1, 0.2, 0.3] LIMIT 2")

                assert list(result) == [(8,), (7,)]


# Regression test: a map subscript restriction in a vector ANN query
# (e.g. WHERE metadata_s['source'] = 'docs') was silently dropped by
# binary_operator_to_prepared instead of raising an error, causing the
# vector store to receive a request with no filter and return results
# that violated the predicate.
def test_vector_search_map_subscript_restriction_raises_error(cql, test_keyspace, vector_store_mock, skip_without_tablets):
    schema = "pk text, metadata_s map<text, text>, embedding vector<float, 3>, PRIMARY KEY (pk)"

    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(embedding) USING 'vector_index'")

        with pytest.raises(InvalidRequest, match="Unsupported restriction"):
            cql.execute(
                f"SELECT pk FROM {table}"
                " WHERE metadata_s['source'] = 'docs'"
                " ORDER BY embedding ANN OF [1.0, 0.0, 0.0]"
                " LIMIT 10 ALLOW FILTERING"
            )
