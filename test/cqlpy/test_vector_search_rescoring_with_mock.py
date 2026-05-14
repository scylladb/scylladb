# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

###############################################################################
# Tests for vector search rescoring and oversampling behavior.
#
# These tests use the vector store mock infrastructure from
# test_vector_search_with_vector_store_mock.py to verify that Scylla correctly
# applies oversampling when querying the vector store and rescores results
# returned by the vector store before returning them to the client.
###############################################################################

import json

import pytest

from .test_vector_search_with_vector_store_mock import vector_store_mock, _vector_store_mock_session
from .util import new_test_table


# Verifies that the LIMIT sent to the vector store is ceil(oversampling * cql_limit).
def test_oversampling_multiplies_limit_for_vector_store_query(cql, test_keyspace, vector_store_mock, skip_without_tablets):
    schema = "id int primary key, embedding vector<float, 3>"
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(embedding) USING 'vector_index' WITH OPTIONS = {{'oversampling': '3.4'}}")

        cql.execute(f"SELECT * FROM {table} ORDER BY embedding ANN OF [0.1, 0.2, 0.3] LIMIT 3")

        requests = vector_store_mock.ann_requests
        assert requests, "Expected at least one ANN request to the vector store"
        # ceil(3.4 * 3) = ceil(10.2) = 11
        assert json.loads(requests[-1].body)["limit"] == 11


# Verifies that when the vector store returns more results than the CQL LIMIT,
# the output is trimmed to the CQL LIMIT.
def test_oversampled_vector_store_results_are_limited_to_cql_limit(cql, test_keyspace, vector_store_mock, skip_without_tablets):
    schema = "id int primary key, embedding vector<float, 3>"
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(embedding) USING 'vector_index' WITH OPTIONS = {{'oversampling': '2'}}")
        cql.execute(f"INSERT INTO {table} (id, embedding) VALUES (1, [1, 1, 1])")
        cql.execute(f"INSERT INTO {table} (id, embedding) VALUES (2, [1, 1, 1])")

        vector_store_mock.set_next_ann_response(200, json.dumps({
            "primary_keys": {"id": [1, 2]},
            "similarity_scores": [0, 0],
        }))
        rows = list(cql.execute(f"SELECT id FROM {table} ORDER BY embedding ANN OF [1, 1, 1] LIMIT 1"))

        assert len(rows) == 1
