# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

###############################################################################
# Tests for vector indexes
###############################################################################

import pytest
from .util import new_test_table, is_scylla, unique_name
from cassandra.protocol import InvalidRequest, ConfigurationException

@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #16317")]), "vnodes"],
                         indirect=True)
def test_create_vector_search_index(cql, test_keyspace, scylla_only):
    schema = 'p int primary key, v vector<float, 3>'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(v) USING 'vector_index'")


@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #16317")]), "vnodes"],
                         indirect=True)
def test_create_vector_search_index_without_custom_keyword(cql, test_keyspace):
    schema = 'p int primary key, v vector<float, 3>'
    with new_test_table(cql, test_keyspace, schema) as table:
        if is_scylla(cql):
            custom_class = 'vector_index'
        else:
            custom_class =  'sai'
        
        cql.execute(f"CREATE INDEX ON {table}(v) USING '{custom_class}'")

@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #16317")]), "vnodes"],
                         indirect=True)
def test_create_custom_index_with_invalid_class(cql, test_keyspace):
    schema = 'p int primary key, v vector<float, 3>'
    with new_test_table(cql, test_keyspace, schema) as table:
        invalid_custom_class = "invalid.custom.class"
        with pytest.raises((InvalidRequest, ConfigurationException), match=r"Non-supported custom class|Unable to find"):
            cql.execute(f"CREATE CUSTOM INDEX ON {table}(v) USING '{invalid_custom_class}'")

@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #16317")]), "vnodes"],
                         indirect=True)
def test_create_custom_index_without_custom_class(cql, test_keyspace):
    schema = 'p int primary key, v vector<float, 3>'
    with new_test_table(cql, test_keyspace, schema) as table:
        with pytest.raises((InvalidRequest, ConfigurationException), match=r"CUSTOM index requires specifying|Unable to find"):
            cql.execute(f"CREATE CUSTOM INDEX ON {table}(v)")

@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #16317")]), "vnodes"],
                         indirect=True)
def test_create_vector_search_index_on_nonvector_column(cql, test_keyspace, scylla_only):
    schema = 'p int primary key, v int'
    with new_test_table(cql, test_keyspace, schema) as table:
        with pytest.raises(InvalidRequest, match="Vector indexes are only supported on columns of vectors of floats"):
            cql.execute(f"CREATE CUSTOM INDEX ON {table}(v) USING 'vector_index'")

@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #16317")]), "vnodes"],
                         indirect=True)
def test_create_vector_search_index_with_bad_options(cql, test_keyspace, scylla_only):
    schema = 'p int primary key, v vector<float, 3>'
    with new_test_table(cql, test_keyspace, schema) as table:
        with pytest.raises(InvalidRequest, match="Unsupported option"):
            cql.execute(f"CREATE CUSTOM INDEX ON {table}(v) USING 'vector_index' WITH OPTIONS = {{'bad_option': 'bad_value'}}")

@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #16317")]), "vnodes"],
                         indirect=True)
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

@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #16317")]), "vnodes"],
                         indirect=True)
def test_create_vector_search_index_with_bad_similarity_value(cql, test_keyspace, scylla_only):
    schema = 'p int primary key, v vector<float, 3>'
    with new_test_table(cql, test_keyspace, schema) as table:
        with pytest.raises(InvalidRequest, match="Unsupported similarity function"):
            cql.execute(f"CREATE CUSTOM INDEX ON {table}(v) USING 'vector_index' WITH OPTIONS = {{'similarity_function': 'bad_similarity_function'}}") 

@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #16317")]), "vnodes"],
                         indirect=True)
def test_create_vector_search_index_on_nonfloat_vector_column(cql, test_keyspace, scylla_only):
    schema = 'p int primary key, v vector<int, 3>'
    with new_test_table(cql, test_keyspace, schema) as table:
        with pytest.raises(InvalidRequest, match="Vector indexes are only supported on columns of vectors of floats"):
            cql.execute(f"CREATE CUSTOM INDEX ON {table}(v) USING 'vector_index'")

@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #16317")]), "vnodes"],
                         indirect=True)
def test_no_view_for_vector_search_index(cql, test_keyspace, scylla_only):
    schema = 'p int primary key, v vector<float, 3>'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX abc ON {table}(v) USING 'vector_index'")
        result = cql.execute(f"SELECT * FROM system_schema.views WHERE keyspace_name = '{test_keyspace}' AND view_name = 'abc_index'")
        assert len(result.current_rows) == 0, "Vector search index should not create a view in system_schema.views"
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE INDEX def ON {table}(v)")
        result = cql.execute(f"SELECT * FROM system_schema.views WHERE keyspace_name = '{test_keyspace}' AND view_name = 'def_index'")
        assert len(result.current_rows) == 1, "Regular index should create a view in system_schema.views"
        
@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #16317")]), "vnodes"],
                         indirect=True)
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


@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #16317")]), "vnodes"],
                         indirect=True)
def test_vector_index_version_on_recreate(cql, test_keyspace, scylla_only):
    schema = 'p int primary key, v vector<float, 3>'
    with new_test_table(cql, test_keyspace, schema) as table:
        _, table_name = table.split('.')
        base_table_version_query = f"SELECT version FROM system_schema.scylla_tables WHERE keyspace_name = '{test_keyspace}' AND table_name = '{table_name}'"
        index_version_query = f"SELECT * FROM system_schema.indexes WHERE keyspace_name = '{test_keyspace}' AND table_name = '{table_name}' AND index_name = 'abc'"

        # Fetch the base table version.
        version = str(cql.execute(base_table_version_query).one().version)

        # Create the vector index.
        cql.execute(f"CREATE CUSTOM INDEX abc ON {table}(v) USING 'vector_index'")

        # Fetch the index version.
        # It should be the same as the base table version before the index was created.
        result = cql.execute(index_version_query)
        assert len(result.current_rows) == 1
        assert result.current_rows[0].options['index_version'] == version

        # Drop and create new index with the same parameters.
        cql.execute(f"DROP INDEX {test_keyspace}.abc")
        cql.execute(f"CREATE CUSTOM INDEX abc ON {table}(v) USING 'vector_index'")

        # Check if the index version changed.
        result = cql.execute(index_version_query)
        assert len(result.current_rows) == 1
        assert result.current_rows[0].options['index_version'] != version


@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #16317")]), "vnodes"],
                         indirect=True)
def test_vector_index_version_unaffected_by_alter(cql, test_keyspace, scylla_only):
    schema = 'p int primary key, v vector<float, 3>'
    with new_test_table(cql, test_keyspace, schema) as table:
        _, table_name = table.split('.')
        base_table_version_query = f"SELECT version FROM system_schema.scylla_tables WHERE keyspace_name = '{test_keyspace}' AND table_name = '{table_name}'"
        index_version_query = f"SELECT * FROM system_schema.indexes WHERE keyspace_name = '{test_keyspace}' AND table_name = '{table_name}' AND index_name = 'abc'"

        # Fetch the base table version.
        version = str(cql.execute(base_table_version_query).one().version)

        # Create the vector index.
        cql.execute(f"CREATE CUSTOM INDEX abc ON {table}(v) USING 'vector_index'")

        # Fetch the index version.
        # It should be the same as the base table version before the index was created.
        result = cql.execute(index_version_query)
        assert len(result.current_rows) == 1
        assert result.current_rows[0].options['index_version'] == version

        # ALTER the base table.
        cql.execute(f"ALTER TABLE {table} ADD v2 vector<float, 3>")

        # Check if the index version is still the same.
        result = cql.execute(index_version_query)
        assert len(result.current_rows) == 1
        assert result.current_rows[0].options['index_version'] == version


@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #16317")]), "vnodes"],
                         indirect=True)
def test_vector_index_version_fail_given_as_option(cql, test_keyspace, scylla_only):
    schema = 'p int primary key, v vector<float, 3>'
    with new_test_table(cql, test_keyspace, schema) as table:
        # Fail to create vector index with version option given by the user.
        with pytest.raises(InvalidRequest, match="Cannot specify index_version as a CUSTOM option"):
            cql.execute(f"CREATE CUSTOM INDEX abc ON {table}(v) USING 'vector_index' WITH OPTIONS = {{'index_version': '18ad2003-05ea-17d9-1855-0325ac0a755d'}}")


###############################################################################
# Tests for CDC with vector indexes
#
# The following tests verify that the constraints between Vector Search
# and CDC settings are properly enforced.
#
# If a vector index is created, CDC may only be enabled with options meeting
# the Vector Search requirements:
#   - CDC's TTL must be at least 24 hours (86400 seconds) OR set to 0 (infinite).
#   - CDC's delta mode must be set to 'full' or CDC's postimage must be enabled.
#
# We test that:
#   * Enabling CDC with default or valid options succeeds.
#   * Enabling CDC with invalid TTL (< 24h) or invalid delta and postimage
#     disabled is rejected.
#   * Preimage options can be freely toggled.
#   * Disabling CDC is not allowed when a vector index already exists.
#
# We also verify that creating a vector index is forbidden
# if CDC is enabled but uses invalid options or CDC is explicitly disabled
# (set to false by the user), and allowed only when CDC is either undeclared
# or configured to satisfy the minimal Vector Search requirements.
###############################################################################


VS_TTL_SECONDS = 86400  # 24 hours


def alter_cdc(cql, table, options):
    try:
        cql.execute(f"ALTER TABLE {table} WITH cdc = {options}")
    except InvalidRequest as e:
        with pytest.raises(InvalidRequest, match="CDC log must meet the minimal requirements of Vector Search"):
            raise e
        return False
    return True


def create_index(cql, test_keyspace, table, column):
    idx_name = f"{column}_idx_{unique_name()}"
    query = f"CREATE INDEX {idx_name} ON {table} ({column}) USING 'vector_index'"
    try:
        cql.execute(query)
    except InvalidRequest as e:
        with pytest.raises(InvalidRequest, match="CDC log must meet the minimal requirements of Vector Search"):
            raise e
        return False
    cql.execute(f"DROP INDEX {test_keyspace}.{idx_name}")
    return True


@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #16317")]), "vnodes"],
                         indirect=True)
def test_try_create_cdc_with_vector_search_enabled(scylla_only, cql, test_keyspace):
    schema = "pk int primary key, v vector<float, 3>"
    with new_test_table(cql, test_keyspace, schema) as table:
        # The vector index requires CDC to be enabled with specific options:
        # - TTL must be at least 24 hours (86400 seconds)
        # - delta mode must be set to 'full' or postimage must be enabled.

        # Enable Vector Search by creating a vector index.
        cql.execute(f"CREATE INDEX v_idx ON {table} (v) USING 'vector_index'")

        # Allow creating CDC log table with default options.
        assert alter_cdc(cql, table, {'enabled': True})

        # Disallow changing CDC's TTL to less than 24 hours.
        assert not alter_cdc(cql, table, {'enabled': True, 'ttl': 1})
        assert alter_cdc(cql, table, {'enabled': True, 'ttl': 86400})

        # Allow changing CDC's TTL to 0 (infinite).
        assert alter_cdc(cql, table, {'enabled': True, 'ttl': 0})

        # Disallow changing CDC's delta to 'keys' if postimage is disabled.
        assert not alter_cdc(cql, table, {'enabled': True, 'delta': 'keys'})
        assert alter_cdc(cql, table, {'enabled': True, 'delta': 'full'})

        # Allow creating CDC with postimage enabled instead of delta set to 'full'.
        assert alter_cdc(cql, table, {'enabled': True, 'postimage': True, 'delta': 'keys'})

        # Allow changing CDC's preimage and enabling postimage freely.
        assert alter_cdc(cql, table, {'enabled': True, 'preimage': True})
        assert alter_cdc(cql, table, {'enabled': True, 'preimage': False})
        assert alter_cdc(cql, table, {'enabled': True, 'postimage': True})
        assert alter_cdc(cql, table, {'enabled': True, 'preimage': True, 'postimage': True})

        # Disallow changing CDC's postimage to false when delta is 'keys'.
        assert not alter_cdc(cql, table, {'enabled': True, 'delta': 'keys', 'postimage': False})


@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #16317")]), "vnodes"],
                         indirect=True)
def test_try_disable_cdc_with_vector_search_enabled(scylla_only, cql, test_keyspace):
    schema = "pk int primary key, v vector<float, 3>"
    with new_test_table(cql, test_keyspace, schema) as table:
        # Enable Vector Search by creating a vector index.
        cql.execute(f"CREATE INDEX v_idx ON {table} (v) USING 'vector_index'")

        # Disallow disabling CDC when Vector Search is enabled.
        with pytest.raises(InvalidRequest, match="Cannot disable CDC when Vector Search is enabled on the table"):
            cql.execute(f"ALTER TABLE {table} WITH cdc = {{'enabled': False}}")


@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #16317")]), "vnodes"],
                         indirect=True)
def test_try_enable_vector_search_with_cdc_enabled(scylla_only, cql, test_keyspace):
    schema = "pk int primary key, v vector<float, 3>"
    with new_test_table(cql, test_keyspace, schema) as table:
        # The vector index requires CDC to be enabled with specific options:
        # - TTL must be at least 24 hours (86400 seconds)
        # - delta mode must be set to 'full' or postimage must be enabled.

        # Disallow creating the vector index when CDC's TTL is less than 24h.
        assert alter_cdc(cql, table, {'enabled': True, 'ttl': 1})
        assert not create_index(cql, test_keyspace, table, "v")

        # Allow creating the vector index when CDC's TTL is 0 (infinite).
        assert alter_cdc(cql, table, {'enabled': True, 'ttl': 0})
        assert create_index(cql, test_keyspace, table, "v")

        # Disallow creating the vector index when CDC's delta is set to 'keys'.
        assert alter_cdc(cql, table, {'enabled': True, 'delta': 'keys'})
        assert not create_index(cql, test_keyspace, table, "v")

        # Allow creating the vector index when CDC's postimage is enabled instead of delta set to 'full'.
        assert alter_cdc(cql, table, {'enabled': True, 'delta': 'keys', 'postimage': True})
        assert create_index(cql, test_keyspace, table, "v")

        # Allow creating the vector index when CDC's options fulfill the minimal requirements of Vector Search.
        assert alter_cdc(cql, table, {'enabled': True, 'ttl': 172800, 'delta': 'full', 'preimage': True, 'postimage': True})
        assert create_index(cql, test_keyspace, table, "v")


@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #16317")]), "vnodes"],
                         indirect=True)
def test_try_enable_vector_search_with_cdc_disabled(scylla_only, cql, test_keyspace):
    schema = "pk int primary key, v vector<float, 3>"
    with new_test_table(cql, test_keyspace, schema) as table:
        # Disallow creating the vector index when CDC is explicitly disabled.
        assert alter_cdc(cql, table, {'enabled': False, 'ttl': 172800, 'postimage' : True})
        with pytest.raises(InvalidRequest, match="Cannot create the vector index when CDC is explicitly disabled."):
            cql.execute(f"CREATE INDEX v_idx ON {table} (v) USING 'vector_index'")

        # Allow creating the vector index when CDC is enabled again.
        assert alter_cdc(cql, table, {'enabled': True})
        assert create_index(cql, test_keyspace, table, "v")


# This test reproduces VECTOR-179.
# It performs a vector search with tracing enabled. An exception is expected
# because the vector store node is not configured. However, due to the bug,
# Scylla crashes instead of returning an error.
@pytest.mark.parametrize(
    "test_keyspace",
    [
        pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #16317")]),
        "vnodes",
    ],
    indirect=True,
)
def test_vector_search_when_tracing_is_enabled(cql, test_keyspace, scylla_only):
    schema = "p int primary key, v vector<float, 3>"
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(v) USING 'vector_index'")
        with pytest.raises(InvalidRequest, match="Vector Store is disabled"):
            cql.execute(
                f"SELECT * FROM {table} ORDER BY v ANN OF [0.2,0.3,0.4] LIMIT 1",
                trace=True,
            )
