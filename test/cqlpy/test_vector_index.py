# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

###############################################################################
# Tests for vector indexes
###############################################################################

import pytest
from .util import new_test_table, is_scylla
from cassandra.protocol import InvalidRequest, ConfigurationException


def test_create_vector_search_index(cql, test_keyspace, scylla_only):
    schema = 'p int primary key, v vector<float, 3>'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(v) USING 'vector_index'")



def test_create_vector_search_index_without_custom_keyword(cql, test_keyspace):
    schema = 'p int primary key, v vector<float, 3>'
    with new_test_table(cql, test_keyspace, schema) as table:
        if is_scylla(cql):
            custom_class = 'vector_index'
        else:
            custom_class =  'sai'
        
        cql.execute(f"CREATE INDEX ON {table}(v) USING '{custom_class}'")

def test_create_custom_index_with_invalid_class(cql, test_keyspace):
    schema = 'p int primary key, v vector<float, 3>'
    with new_test_table(cql, test_keyspace, schema) as table:
        invalid_custom_class = "invalid.custom.class"
        with pytest.raises((InvalidRequest, ConfigurationException), match=r"Non-supported custom class|Unable to find"):
            cql.execute(f"CREATE CUSTOM INDEX ON {table}(v) USING '{invalid_custom_class}'")

def test_create_custom_index_without_custom_class(cql, test_keyspace):
    schema = 'p int primary key, v vector<float, 3>'
    with new_test_table(cql, test_keyspace, schema) as table:
        with pytest.raises((InvalidRequest, ConfigurationException), match=r"CUSTOM index requires specifying|Unable to find"):
            cql.execute(f"CREATE CUSTOM INDEX ON {table}(v)")

def test_create_vector_search_index_on_nonvector_column(cql, test_keyspace, scylla_only):
    schema = 'p int primary key, v int'
    with new_test_table(cql, test_keyspace, schema) as table:
        with pytest.raises(InvalidRequest, match="Vector indexes are only supported on columns of vectors of floats"):
            cql.execute(f"CREATE CUSTOM INDEX ON {table}(v) USING 'vector_index'")

def test_create_vector_search_index_with_bad_options(cql, test_keyspace, scylla_only):
    schema = 'p int primary key, v vector<float, 3>'
    with new_test_table(cql, test_keyspace, schema) as table:
        with pytest.raises(InvalidRequest, match="Unsupported option"):
            cql.execute(f"CREATE CUSTOM INDEX ON {table}(v) USING 'vector_index' WITH OPTIONS = {{'bad_option': 'bad_value'}}")

def test_create_vector_search_index_with_bad_numeric_value(cql, test_keyspace, scylla_only):
    schema = 'p int primary key, v vector<float, 3>'
    with new_test_table(cql, test_keyspace, schema) as table:
        for val in ['-1', '513']:
            with pytest.raises(InvalidRequest, match="out of valid range"):
                cql.execute(f"CREATE CUSTOM INDEX ON {table}(v) USING 'vector_index' WITH OPTIONS = {{'maximum_node_connections': '{val}' }}") 
        for val in ['dog', '123dog']:
            with pytest.raises(InvalidRequest, match="not a valid number"):
                cql.execute(f"CREATE CUSTOM INDEX ON {table}(v) USING 'vector_index' WITH OPTIONS = {{'maximum_node_connections': '{val}' }}") 
        with pytest.raises(InvalidRequest, match="out of valid range"):
            cql.execute(f"CREATE CUSTOM INDEX ON {table}(v) USING 'vector_index' WITH OPTIONS = {{'construction_beam_width': '5000' }}") 

def test_create_vector_search_index_with_bad_similarity_value(cql, test_keyspace, scylla_only):
    schema = 'p int primary key, v vector<float, 3>'
    with new_test_table(cql, test_keyspace, schema) as table:
        with pytest.raises(InvalidRequest, match="Unsupported similarity function"):
            cql.execute(f"CREATE CUSTOM INDEX ON {table}(v) USING 'vector_index' WITH OPTIONS = {{'similarity_function': 'bad_similarity_function'}}") 

def test_create_vector_search_index_on_nonfloat_vector_column(cql, test_keyspace, scylla_only):
    schema = 'p int primary key, v vector<int, 3>'
    with new_test_table(cql, test_keyspace, schema) as table:
        with pytest.raises(InvalidRequest, match="Vector indexes are only supported on columns of vectors of floats"):
            cql.execute(f"CREATE CUSTOM INDEX ON {table}(v) USING 'vector_index'")
        

def test_describe_custom_index(cql, test_keyspace):
    schema = 'p int primary key, v1 vector<float, 3>, v2 vector<float, 3>'
    with new_test_table(cql, test_keyspace, schema) as table:
        # Cassandra inserts a space between the table name and parentheses,
        # Scylla doesn't. This difference doesn't matter because both are
        # valid CQL commands
        # Scylla doesn't support sai custom class.
        if is_scylla(cql):
            maybe_space = ''
            custom_class = 'vector_index'
        else:
            maybe_space = ' '
            custom_class =  'sai'


        create_idx_a = f"CREATE INDEX custom ON {table}(v1) USING '{custom_class}'"
        create_idx_b = f"CREATE CUSTOM INDEX custom1 ON {table}(v2) USING '{custom_class}'"

        cql.execute(create_idx_a)
        cql.execute(create_idx_b)

        a_desc = cql.execute(f"DESC INDEX {test_keyspace}.custom").one().create_statement
        b_desc = cql.execute(f"DESC INDEX {test_keyspace}.custom1").one().create_statement

        assert f"CREATE CUSTOM INDEX custom ON {table}{maybe_space}(v1) USING '{custom_class}'" in a_desc
        assert f"CREATE CUSTOM INDEX custom1 ON {table}{maybe_space}(v2) USING '{custom_class}'" in b_desc