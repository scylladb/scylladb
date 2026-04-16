# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

###############################################################################
# Tests for vector indexes
#
# Index class usage:
#   - Tests that exercise only features common to both Cassandra SAI and
#     ScyllaDB (basic vector creation, similarity_function, IF NOT EXISTS,
#     error on nonexistent columns, etc.) use USING 'sai' so they run
#     against both backends.
#   - Tests that exercise ScyllaDB-specific options or features (quantization,
#     rescoring, oversampling, CDC integration, etc.)
#     use USING 'vector_index' and are marked scylla_only.
#
# Note: pytest does not run the vector search backend, so ANN queries that
# pass CQL validation will fail with "Vector Store is disabled" rather than
# returning results.  Tests use this error to confirm that CQL parsing and
# validation succeeded.
###############################################################################

import pytest
import json
from .util import new_test_table, is_scylla, unique_name
from cassandra.protocol import InvalidRequest, ConfigurationException

supported_filtering_types = [
    'ascii',
    'bigint',
    'blob',
    'boolean',
    'date',
    'decimal',
    'double',
    'float',
    'inet',
    'int',
    'smallint',
    'text',
    'varchar',
    'time',
    'timestamp',
    'timeuuid',
    'tinyint',
    'uuid',
    'varint',
]

unsupported_filtering_types = [
    'duration',
    'map<int, int>',
    'list<int>',
    'set<int>',
    'tuple<int, int>',
    'vector<float, 3>',
    'frozen<map<int, int>>',
    'frozen<list<int>>',
    'frozen<set<int>>',
    'frozen<tuple<int, int>>',
]

def test_create_vector_search_index(cql, test_keyspace, scylla_only, skip_without_tablets):
    """ScyllaDB-only: basic vector index creation using the native
    'vector_index' class (non-SAI) to verify the ScyllaDB-specific path."""
    schema = 'p int primary key, v vector<float, 3>'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(v) USING 'vector_index'")


def test_create_vector_search_index_uppercase_class(cql, test_keyspace, scylla_only, skip_without_tablets):
    """Custom index class name lookup is case-insensitive, so 'VECTOR_INDEX'
    should be accepted just like 'vector_index'."""
    schema = 'p int primary key, v vector<float, 3>'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(v) USING 'VECTOR_INDEX'")


def test_create_vector_search_index_using_sai(cql, test_keyspace, skip_on_scylla_vnodes):
    """SAI class name should be accepted for creating a vector index on a
    VECTOR<FLOAT, N> column.  On ScyllaDB the class is translated to the
    native 'vector_index'; on Cassandra SAI handles it natively."""
    schema = 'p int primary key, v vector<float, 3>'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(v) USING 'sai'")


def test_create_vector_search_index_without_custom_keyword(cql, test_keyspace, skip_on_scylla_vnodes):
    schema = 'p int primary key, v vector<float, 3>'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE INDEX ON {table}(v) USING 'sai'")

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

def test_create_vector_search_index_on_nonvector_column(cql, test_keyspace, scylla_only, skip_without_tablets):
    """ScyllaDB-only: the native vector_index class rejects non-vector columns.
    Cassandra SAI accepts all column types, so this rejection is ScyllaDB-specific."""
    schema = 'p int primary key, v int'
    with new_test_table(cql, test_keyspace, schema) as table:
        with pytest.raises(InvalidRequest, match="Vector indexes are only supported on columns of vectors of floats"):
            cql.execute(f"CREATE CUSTOM INDEX ON {table}(v) USING 'vector_index'")

def test_create_vector_search_global_index_with_filtering_columns(cql, test_keyspace, scylla_only, skip_without_tablets):
    schema = 'p1 int, p2 int, c1 int, c2 int, v vector<float, 3>, f1 int, f2 int, primary key ((p1, p2), c1, c2)'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(v, f1, f2) USING 'vector_index'")

def test_create_vector_search_local_index_with_filtering_columns(cql, test_keyspace, scylla_only, skip_without_tablets):
    schema = 'p1 int, p2 int, c1 int, c2 int, v vector<float, 3>, f1 int, f2 int, primary key ((p1, p2), c1, c2)'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}((p1, p2), v, f1, f2) USING 'vector_index'")

def test_create_vector_search_local_index_with_filtering_columns_on_nonvector_column(cql, test_keyspace, scylla_only, skip_without_tablets):
    schema = 'p1 int, p2 int, c1 int, c2 int, v int, f1 int, f2 int, primary key ((p1, p2), c1, c2)'
    with new_test_table(cql, test_keyspace, schema) as table:
        with pytest.raises(InvalidRequest, match="Vector indexes are only supported on columns of vectors of floats"):
            cql.execute(f"CREATE CUSTOM INDEX ON {table}((p1, p2), v, f1, f2) USING 'vector_index'")

def test_create_vector_search_index_with_supported_and_unsupported_filtering_columns(cql, test_keyspace, scylla_only, skip_without_tablets):
    supported_columns = ', '.join([f's{idx} {typ}' for idx, typ in enumerate(supported_filtering_types)])
    unsupported_columns = ', '.join([f'u{idx} {typ}' for idx, typ in enumerate(unsupported_filtering_types)])
    schema = f'p int, c int, v vector<float, 3>, {supported_columns}, {unsupported_columns}, primary key (p, c)'
    with new_test_table(cql, test_keyspace, schema) as table:
        for idx in range(len(supported_filtering_types)):
            cql.execute(f"CREATE CUSTOM INDEX global_idx ON {table}(v, s{idx}) USING 'vector_index'")
            cql.execute(f"DROP INDEX {test_keyspace}.global_idx")
            cql.execute(f"CREATE CUSTOM INDEX local_idx ON {table}((p), v, s{idx}) USING 'vector_index'")
            cql.execute(f"DROP INDEX {test_keyspace}.local_idx")
        for idx in range(len(unsupported_filtering_types)):
            with pytest.raises(InvalidRequest, match=f"Unsupported vector index filtering column u{idx} type|Secondary indexes are not supported"):
                cql.execute(f"CREATE CUSTOM INDEX global_idx ON {table}(v, u{idx}) USING 'vector_index'")
            with pytest.raises(InvalidRequest, match=f"Unsupported vector index filtering column u{idx} type|Secondary indexes are not supported"):
                cql.execute(f"CREATE CUSTOM INDEX local_idx ON {table}((p), v, u{idx}) USING 'vector_index'")

def test_create_vector_search_local_index_with_unsupported_partition_columns(cql, test_keyspace, scylla_only, skip_without_tablets):
    for filter_type in unsupported_filtering_types:
        schema = f'p {filter_type}, c int, v vector<float, 3>, f int, primary key (p, c)'
        with pytest.raises(InvalidRequest, match="Unsupported|Invalid"):
            with new_test_table(cql, test_keyspace, schema) as table:
                cql.execute(f"CREATE CUSTOM INDEX ON {table}((p), v, f) USING 'vector_index'")

def test_create_vector_search_index_with_duplicated_columns(cql, test_keyspace, scylla_only, skip_without_tablets):
    schema = f'p int, c int, v vector<float, 3>, x int, primary key (p, c)'
    with new_test_table(cql, test_keyspace, schema) as table:
        with pytest.raises(InvalidRequest, match=f"Cannot create secondary index on partition key column p"):
            cql.execute(f"CREATE CUSTOM INDEX global_idx ON {table}(v, p) USING 'vector_index'")
        with pytest.raises(InvalidRequest, match=f"Duplicate column x in index target list"):
            cql.execute(f"CREATE CUSTOM INDEX global_idx ON {table}(v, x, x) USING 'vector_index'")
        with pytest.raises(InvalidRequest, match=f"Cannot create secondary index on partition key column p"):
            cql.execute(f"CREATE CUSTOM INDEX local_idx ON {table}((p), v, p) USING 'vector_index'")
        with pytest.raises(InvalidRequest, match=f"Duplicate column x in index target list"):
            cql.execute(f"CREATE CUSTOM INDEX local_idx ON {table}((p), v, x, x) USING 'vector_index'")

def test_create_vector_search_index_with_bad_options(cql, test_keyspace, skip_on_scylla_vnodes):
    schema = 'p int primary key, v vector<float, 3>'
    with new_test_table(cql, test_keyspace, schema) as table:
        with pytest.raises((InvalidRequest, ConfigurationException), match='Unsupported option|not understood by'):
            cql.execute(f"CREATE CUSTOM INDEX ON {table}(v) USING 'sai' WITH OPTIONS = {{'bad_option': 'bad_value'}}")

def test_create_vector_search_index_with_bad_numeric_value(cql, test_keyspace, skip_on_scylla_vnodes):
    schema = 'p int primary key, v vector<float, 3>'
    with new_test_table(cql, test_keyspace, schema) as table:
        def check_bad_option(option, bad_range_vals):
            for val in bad_range_vals:
                with pytest.raises(InvalidRequest, match='out of valid range|cannot be <= 0'):
                    cql.execute(f"CREATE CUSTOM INDEX ON {table}(v) USING 'sai' WITH OPTIONS = {{'{option}': '{val}' }}")
            for val in ['dog', '123dog']:
                with pytest.raises(InvalidRequest, match='not.*integer'):
                    cql.execute(f"CREATE CUSTOM INDEX ON {table}(v) USING 'sai' WITH OPTIONS = {{'{option}': '{val}' }}")

        check_bad_option('maximum_node_connections', ['-1', '513'])
        check_bad_option('construction_beam_width', ['-1', '5000'])
        # search_beam_width is a ScyllaDB-specific option (not in Cassandra SAI).
        if is_scylla(cql):
            check_bad_option('search_beam_width', ['-1', '5000'])

def test_create_vector_search_index_with_bad_similarity_value(cql, test_keyspace, skip_on_scylla_vnodes):
    schema = 'p int primary key, v vector<float, 3>'
    with new_test_table(cql, test_keyspace, schema) as table:
        with pytest.raises(InvalidRequest, match="Invalid value in option 'similarity_function'|was not recognized"):
            cql.execute(f"CREATE CUSTOM INDEX ON {table}(v) USING 'sai' WITH OPTIONS = {{'similarity_function': 'bad_similarity_function'}}")

def test_create_vector_search_index_on_nonfloat_vector_column(cql, test_keyspace, scylla_only, skip_without_tablets):
    schema = 'p int primary key, v vector<int, 3>'
    with new_test_table(cql, test_keyspace, schema) as table:
        with pytest.raises(InvalidRequest, match="Vector indexes are only supported on columns of vectors of floats"):
            cql.execute(f"CREATE CUSTOM INDEX ON {table}(v) USING 'vector_index'")

def test_no_view_for_vector_search_index(cql, test_keyspace, scylla_only, skip_without_tablets):
    schema = 'p int primary key, v vector<float, 3>'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX abc ON {table}(v) USING 'vector_index'")
        result = cql.execute(f"SELECT * FROM system_schema.views WHERE keyspace_name = '{test_keyspace}' AND view_name = 'abc_index'")
        assert len(result.current_rows) == 0, "Vector search index should not create a view in system_schema.views"
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE INDEX def ON {table}(v)")
        result = cql.execute(f"SELECT * FROM system_schema.views WHERE keyspace_name = '{test_keyspace}' AND view_name = 'def_index'")
        assert len(result.current_rows) == 1, "Regular index should create a view in system_schema.views"
        
def test_describe_custom_index(cql, test_keyspace, skip_on_scylla_vnodes):
    schema = 'p int primary key, v1 vector<float, 3>, v2 vector<float, 3>'
    with new_test_table(cql, test_keyspace, schema) as table:
        # Cassandra inserts a space between the table name and parentheses,
        # Scylla doesn't. This difference doesn't matter because both are
        # valid CQL commands.
        # ScyllaDB translates SAI to vector_index, so DESCRIBE shows
        # 'vector_index'; Cassandra shows 'sai'.
        if is_scylla(cql):
            maybe_space = ''
            described_class = 'vector_index'
        else:
            maybe_space = ' '
            described_class = 'sai'

        create_idx_a = f"CREATE INDEX custom ON {table}(v1) USING 'sai'"
        create_idx_b = f"CREATE CUSTOM INDEX custom1 ON {table}(v2) USING 'sai'"

        cql.execute(create_idx_a)
        cql.execute(create_idx_b)

        a_desc = cql.execute(f"DESC INDEX {test_keyspace}.custom").one().create_statement
        b_desc = cql.execute(f"DESC INDEX {test_keyspace}.custom1").one().create_statement

        assert f"CREATE CUSTOM INDEX custom ON {table}{maybe_space}(v1) USING '{described_class}'" in a_desc
        assert f"CREATE CUSTOM INDEX custom1 ON {table}{maybe_space}(v2) USING '{described_class}'" in b_desc

def test_describe_vector_index_with_filtering_columns(cql, test_keyspace, scylla_only, skip_without_tablets):
    schema = 'p int primary key, v vector<float, 3>, f1 int, f2 int'
    with new_test_table(cql, test_keyspace, schema) as table:
        idx = unique_name()
        cql.execute(f"CREATE CUSTOM INDEX {idx} ON {table}(v, f1, f2) USING 'vector_index'")

        desc = cql.execute(f"DESC INDEX {test_keyspace}.{idx}").one().create_statement

        assert f"CREATE CUSTOM INDEX {idx} ON {table}(v, f1, f2) USING 'vector_index'" in desc

def test_describe_vector_index_local(cql, test_keyspace, scylla_only, skip_without_tablets):
    schema = 'p1 int, p2 int, c int, v vector<float, 3>, PRIMARY KEY ((p1, p2), c)'
    with new_test_table(cql, test_keyspace, schema) as table:
        idx = unique_name()
        cql.execute(f"CREATE CUSTOM INDEX {idx} ON {table}((p1, p2), v) USING 'vector_index'")

        desc = cql.execute(f"DESC INDEX {test_keyspace}.{idx}").one().create_statement

        assert f"CREATE CUSTOM INDEX {idx} ON {table}((p1, p2), v) USING 'vector_index'" in desc

def test_describe_vector_index_local_with_filtering_columns(cql, test_keyspace, scylla_only, skip_without_tablets):
    schema = 'p1 int, p2 int, c int, v vector<float, 3>, f1 text, f2 text, PRIMARY KEY ((p1, p2), c)'
    with new_test_table(cql, test_keyspace, schema) as table:
        idx = unique_name()
        cql.execute(f"CREATE CUSTOM INDEX {idx} ON {table}((p1, p2), v, f1, f2) USING 'vector_index'")

        desc = cql.execute(f"DESC INDEX {test_keyspace}.{idx}").one().create_statement

        assert f"CREATE CUSTOM INDEX {idx} ON {table}((p1, p2), v, f1, f2) USING 'vector_index'" in desc

def test_describe_vector_index_with_options(cql, test_keyspace, scylla_only, skip_without_tablets):
    """Verify that DESCRIBE INDEX includes user-provided options in the
    WITH OPTIONS clause. System-internal keys (target, class_name,
    index_version) must not appear."""
    schema = 'p int primary key, v vector<float, 3>'
    with new_test_table(cql, test_keyspace, schema) as table:
        idx = unique_name()
        cql.execute(
            f"CREATE CUSTOM INDEX {idx} ON {table}(v) USING 'vector_index' "
            f"WITH OPTIONS = {{'similarity_function': 'euclidean'}}"
        )
        desc = cql.execute(f"DESC INDEX {test_keyspace}.{idx}").one().create_statement
        assert "'similarity_function': 'euclidean'" in desc
        # System keys must not leak into WITH OPTIONS.
        assert 'class_name' not in desc
        assert 'target' not in desc.split("USING")[1]  # "target" may appear in column names before USING

def test_vector_index_version_on_recreate(cql, test_keyspace, scylla_only, skip_without_tablets):
    schema = 'p int primary key, v vector<float, 3>'
    with new_test_table(cql, test_keyspace, schema) as table:
        _, table_name = table.split('.')
        index_version_query = f"SELECT * FROM system_schema.indexes WHERE keyspace_name = '{test_keyspace}' AND table_name = '{table_name}' AND index_name = 'abc'"

        # Create the vector index.
        cql.execute(f"CREATE CUSTOM INDEX abc ON {table}(v) USING 'vector_index'")

        # Fetch the index version (a timeuuid generated at index creation time).
        result = cql.execute(index_version_query)
        assert len(result.current_rows) == 1
        version = result.current_rows[0].options['index_version']

        # Drop and create new index with the same parameters.
        cql.execute(f"DROP INDEX {test_keyspace}.abc")
        cql.execute(f"CREATE CUSTOM INDEX abc ON {table}(v) USING 'vector_index'")

        # Check that the index version changed after recreation.
        result = cql.execute(index_version_query)
        assert len(result.current_rows) == 1
        assert result.current_rows[0].options['index_version'] != version


def test_vector_index_version_unaffected_by_alter(cql, test_keyspace, scylla_only, skip_without_tablets):
    schema = 'p int primary key, v vector<float, 3>'
    with new_test_table(cql, test_keyspace, schema) as table:
        _, table_name = table.split('.')
        index_version_query = f"SELECT * FROM system_schema.indexes WHERE keyspace_name = '{test_keyspace}' AND table_name = '{table_name}' AND index_name = 'abc'"

        # Create the vector index.
        cql.execute(f"CREATE CUSTOM INDEX abc ON {table}(v) USING 'vector_index'")

        # Fetch the index version (a timeuuid generated at index creation time).
        result = cql.execute(index_version_query)
        assert len(result.current_rows) == 1
        version = result.current_rows[0].options['index_version']

        # ALTER the base table.
        cql.execute(f"ALTER TABLE {table} ADD v2 vector<float, 3>")

        # Check that the index version is still the same after ALTER.
        result = cql.execute(index_version_query)
        assert len(result.current_rows) == 1
        assert result.current_rows[0].options['index_version'] == version


def test_vector_index_version_fail_given_as_option(cql, test_keyspace, scylla_only):
    schema = 'p int primary key, v vector<float, 3>'
    with new_test_table(cql, test_keyspace, schema) as table:
        # Fail to create vector index with version option given by the user.
        with pytest.raises(InvalidRequest, match="Cannot specify index_version as a CUSTOM option"):
            cql.execute(f"CREATE CUSTOM INDEX abc ON {table}(v) USING 'vector_index' WITH OPTIONS = {{'index_version': '18ad2003-05ea-17d9-1855-0325ac0a755d'}}")

# Test that the "target" option in system_schema.indexes is serialized
# correctly for a vector index on a single vector column. This format is
# critical for backward compatibility, as it's read from disk on startup
# to rebuild indexes. An incompatible change would prevent existing vector
# indexes from being recreated after an upgrade.
# This is also an interface with the vector-store service, which relies on the "target"
# option to identify the target column.
def test_vector_index_target_serialization(cql, test_keyspace, scylla_only, skip_without_tablets):
    schema = 'p int primary key, v vector<float, 3>'
    with new_test_table(cql, test_keyspace, schema) as table:
        index_name = unique_name()
        cql.execute(f"CREATE CUSTOM INDEX {index_name} ON {table}(v) USING 'vector_index'")

        res = [r for r in cql.execute('select * from system_schema.indexes')
               if r.index_name == index_name]

        assert len(res) == 1
        assert res[0].options['target'] == 'v'

# Test "target" option serialization for vector index with filtering columns.
def test_vector_index_target_serialization_filtering_columns(cql, test_keyspace, scylla_only, skip_without_tablets):
    schema = 'p int primary key, v vector<float, 3>, f1 int, f2 int'
    with new_test_table(cql, test_keyspace, schema) as table:
        index_name = unique_name()
        cql.execute(f"CREATE CUSTOM INDEX {index_name} ON {table}(v, f1, f2) USING 'vector_index'")

        res = [r for r in cql.execute('select * from system_schema.indexes')
               if r.index_name == index_name]

        assert len(res) == 1
        assert json.loads(res[0].options['target']) == {"tc": "v", "fc": ["f1", "f2"]}

# Test "target" option serialization for local vector index.
def test_vector_index_target_serialization_local_index(cql, test_keyspace, scylla_only, skip_without_tablets):
    schema = 'p1 int, p2 int, c1 int, c2 int, v vector<float, 3>, PRIMARY KEY ((p1, p2), c1, c2)'
    with new_test_table(cql, test_keyspace, schema) as table:
        index_name = unique_name()
        cql.execute(f"CREATE CUSTOM INDEX {index_name} ON {table}((p1, p2), v) USING 'vector_index'")

        res = [r for r in cql.execute('select * from system_schema.indexes')
               if r.index_name == index_name]

        assert len(res) == 1
        assert json.loads(res[0].options['target']) == {"tc": "v", "pk": ["p1", "p2"]}

# Test "target" option serialization for local vector index with filtering columns.
def test_vector_index_target_serialization_local_index_with_filtering_columns(cql, test_keyspace, scylla_only, skip_without_tablets):
    schema = 'p1 int, p2 int, c1 int, c2 int, v vector<float, 3>, f1 text, f2 text, PRIMARY KEY ((p1, p2), c1, c2)'
    with new_test_table(cql, test_keyspace, schema) as table:
        index_name = unique_name()
        cql.execute(f"CREATE CUSTOM INDEX {index_name} ON {table}((p1, p2), v, f1, f2) USING 'vector_index'")

        res = [r for r in cql.execute('select * from system_schema.indexes')
               if r.index_name == index_name]

        assert len(res) == 1
        assert json.loads(res[0].options['target']) == {"tc": "v", "pk": ["p1", "p2"], "fc": ["f1", "f2"]}

def test_no_duplicate_named_vector_index_on_column(cql, test_keyspace, skip_on_scylla_vnodes):
    schema = "p int primary key, v vector<float, 3>"
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX idx1 ON {table}(v) USING 'sai'")
        with pytest.raises(InvalidRequest, match=r"already exists"):
            cql.execute(f"CREATE CUSTOM INDEX idx1 ON {table}(v) USING 'sai'")


def test_no_duplicate_unnamed_vector_index_on_column(cql, test_keyspace, skip_on_scylla_vnodes):
    schema = "p int primary key, v vector<float, 3>"
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(v) USING 'sai'")
        with pytest.raises(InvalidRequest, match=r"duplicate of existing index"):
            cql.execute(f"CREATE CUSTOM INDEX ON {table}(v) USING 'sai'")


def test_no_duplicate_unnamed_vector_index_with_if_not_exists(cql, test_keyspace, skip_on_scylla_vnodes):
    schema = "p int primary key, v vector<float, 3>"
    with new_test_table(cql, test_keyspace, schema) as table:
        ks, cf = table.split(".")
        cql.execute(f"CREATE CUSTOM INDEX IF NOT EXISTS ON {table}(v) USING 'sai'")
        # Should succeed silently without creating a duplicate
        cql.execute(f"CREATE CUSTOM INDEX IF NOT EXISTS ON {table}(v) USING 'sai'")
        rows = list(cql.execute(f"SELECT index_name FROM system_schema.indexes WHERE keyspace_name='{ks}' AND table_name='{cf}'"))
        index_names = [row.index_name for row in rows]
        assert len(index_names) == 1


def test_no_duplicate_named_vector_index_with_if_not_exists(cql, test_keyspace, skip_on_scylla_vnodes):
    schema = "p int primary key, v vector<float, 3>"
    with new_test_table(cql, test_keyspace, schema) as table:
        ks, cf = table.split(".")
        cql.execute(f"CREATE CUSTOM INDEX IF NOT EXISTS idx1 ON {table}(v) USING 'sai'")
        # Should silently succeed without creating a duplicate
        cql.execute(f"CREATE CUSTOM INDEX IF NOT EXISTS idx1 ON {table}(v) USING 'sai'")
        rows = list(cql.execute(f"SELECT index_name FROM system_schema.indexes WHERE keyspace_name='{ks}' AND table_name='{cf}'"))
        assert len(rows) == 1
        assert rows[0].index_name == 'idx1'


# Scylla allows creating multiple vector indexes with different names on the same column.
# Cassandra does not - it rejects the second index with "Cannot create more than one storage-attached index on the same column".
def test_multiple_vector_indexes_different_names_on_column(cql, test_keyspace, scylla_only, skip_without_tablets):
    schema = "p int primary key, v vector<float, 3>"
    with new_test_table(cql, test_keyspace, schema) as table:
        ks, cf = table.split(".")
        cql.execute(f"CREATE CUSTOM INDEX idx1 ON {table}(v) USING 'vector_index'")
        cql.execute(f"CREATE CUSTOM INDEX idx2 ON {table}(v) USING 'vector_index'")
        # Both indexes should exist in system_schema.indexes
        rows = list(cql.execute(f"SELECT index_name FROM system_schema.indexes WHERE keyspace_name='{ks}' AND table_name='{cf}'"))
        index_names = {row.index_name for row in rows}
        assert 'idx1' in index_names
        assert 'idx2' in index_names


def test_named_vector_index_after_unnamed_on_same_column(cql, test_keyspace, scylla_only, skip_without_tablets):
    schema = "p int primary key, v vector<float, 3>"
    with new_test_table(cql, test_keyspace, schema) as table:
        ks, cf = table.split(".")
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(v) USING 'vector_index'")
        cql.execute(f"CREATE CUSTOM INDEX idx1 ON {table}(v) USING 'vector_index'")
        rows = list(cql.execute(f"SELECT index_name FROM system_schema.indexes WHERE keyspace_name='{ks}' AND table_name='{cf}'"))
        index_names = {row.index_name for row in rows}
        assert 'idx1' in index_names
        assert len(index_names) == 2


def test_unnamed_vector_index_after_named_on_same_column(cql, test_keyspace, skip_on_scylla_vnodes):
    schema = "p int primary key, v vector<float, 3>"
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX idx1 ON {table}(v) USING 'sai'")
        with pytest.raises(InvalidRequest, match=r"duplicate of existing index"):
            cql.execute(f"CREATE CUSTOM INDEX ON {table}(v) USING 'sai'")

# Validates fix for issue #26672
def test_two_same_name_indexes_on_different_tables_with_if_not_exists(cql, test_keyspace, skip_on_scylla_vnodes):
    schema = "p int primary key, v vector<float, 3>"
    with new_test_table(cql, test_keyspace, schema) as table:
        schema = "p int primary key, v vector<float, 3>"
        with new_test_table(cql, test_keyspace, schema) as table2:
            ks, cf1 = table.split(".")
            _, cf2 = table2.split(".")
            cql.execute(f"CREATE CUSTOM INDEX IF NOT EXISTS ann_index ON {table}(v) USING 'sai'")
            # The query below succeeds although silently does not create a new index.
            # This is because the IF NOT EXISTS check looks for an existing index with the same name
            # within the whole keyspace, not just the same table.
            # Issue: VECTOR-641
            cql.execute(f"CREATE CUSTOM INDEX IF NOT EXISTS ann_index ON {table2}(v) USING 'sai'")
            rows1 = list(cql.execute(f"SELECT index_name FROM system_schema.indexes WHERE keyspace_name='{ks}' AND table_name='{cf1}'"))
            rows2 = list(cql.execute(f"SELECT index_name FROM system_schema.indexes WHERE keyspace_name='{ks}' AND table_name='{cf2}'"))
            assert len(rows1) == 1
            assert rows1[0].index_name == 'ann_index'
            assert len(rows2) == 0


def test_two_same_name_indexes_on_different_columns_with_if_not_exists(cql, test_keyspace, skip_on_scylla_vnodes):
    schema = "p int primary key, v vector<float, 3>, v2 vector<float, 3>"
    with new_test_table(cql, test_keyspace, schema) as table:
        ks, cf = table.split(".")
        cql.execute(f"CREATE CUSTOM INDEX IF NOT EXISTS ann_index ON {table}(v) USING 'sai'")
        # The query below succeeds although silently does not create a new index.
        # This is because the IF NOT EXISTS check looks for an existing index with the same name
        # within the table, not just the same column.
        # Issue: VECTOR-641
        cql.execute(f"CREATE CUSTOM INDEX IF NOT EXISTS ann_index ON {table}(v2) USING 'sai'")
        rows = list(cql.execute(f"SELECT index_name FROM system_schema.indexes WHERE keyspace_name='{ks}' AND table_name='{cf}'"))
        assert len(rows) == 1
        assert rows[0].index_name == 'ann_index'


###############################################################################
# SAI (StorageAttachedIndex) compatibility tests
#
# Cassandra SAI is accepted by ScyllaDB for vector columns and translated
# to the native 'vector_index'.  These tests validate all accepted SAI
# class name variants and that non-vector SAI targets are properly rejected.
###############################################################################


def test_sai_vector_index_with_similarity_function(cql, test_keyspace, skip_on_scylla_vnodes):
    """SAI vector index should accept the similarity_function option
    (supported on both ScyllaDB and Cassandra)."""
    schema = 'p int PRIMARY KEY, v vector<float, 3>'
    for func in ['cosine', 'euclidean', 'dot_product']:
        with new_test_table(cql, test_keyspace, schema) as table:
            cql.execute(
                f"CREATE CUSTOM INDEX ON {table}(v) USING 'sai' "
                f"WITH OPTIONS = {{'similarity_function': '{func}'}}"
            )


def test_sai_vector_index_if_not_exists(cql, test_keyspace, skip_on_scylla_vnodes):
    """IF NOT EXISTS should work with SAI class name."""
    schema = 'p int PRIMARY KEY, v vector<float, 3>'
    with new_test_table(cql, test_keyspace, schema) as table:
        idx = unique_name()
        cql.execute(
            f"CREATE CUSTOM INDEX IF NOT EXISTS {idx} ON {table}(v) USING 'sai'"
        )
        # Creating the same index again with IF NOT EXISTS should not fail
        cql.execute(
            f"CREATE CUSTOM INDEX IF NOT EXISTS {idx} ON {table}(v) USING 'sai'"
        )


def test_sai_fully_qualified_class_name(cql, test_keyspace, skip_on_scylla_vnodes):
    """The fully qualified Cassandra SAI class name should be accepted."""
    schema = 'p int PRIMARY KEY, v vector<float, 3>'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(
            f"CREATE CUSTOM INDEX ON {table}(v) "
            f"USING 'org.apache.cassandra.index.sai.StorageAttachedIndex'"
        )


def test_sai_short_class_name(cql, test_keyspace, skip_on_scylla_vnodes):
    """The short class name 'StorageAttachedIndex' should also be accepted."""
    schema = 'p int PRIMARY KEY, v vector<float, 3>'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(v) USING 'StorageAttachedIndex'")


def test_sai_short_class_uppercase_name(cql, test_keyspace, skip_on_scylla_vnodes):
    """The short class name 'StorageAttachedIndex' should also be accepted."""
    schema = 'p int PRIMARY KEY, v vector<float, 3>'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(v) USING 'SAI'")


def test_sai_short_class_name_case_insensitive(cql, test_keyspace, skip_on_scylla_vnodes):
    """Short SAI class name matching should be case-insensitive."""
    schema = 'p int PRIMARY KEY, v vector<float, 3>'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(
            f"CREATE CUSTOM INDEX ON {table}(v) "
            f"USING 'STORAGEATTACHEDINDEX'"
        )


def test_sai_fqn_requires_exact_case(cql, test_keyspace, skip_on_scylla_vnodes):
    """The fully qualified SAI class name requires exact casing.
    Only 'org.apache.cassandra.index.sai.StorageAttachedIndex' is accepted."""
    schema = 'p int PRIMARY KEY, v vector<float, 3>'
    with new_test_table(cql, test_keyspace, schema) as table:
        with pytest.raises((InvalidRequest, ConfigurationException), match=r"Non-supported custom class|Unable to find"):
            cql.execute(
                f"CREATE CUSTOM INDEX ON {table}(v) "
                f"USING 'org.apache.cassandra.index.sai.STORAGEATTACHEDINDEX'"
            )


def test_sai_local_index_with_vector_column(cql, test_keyspace, scylla_only, skip_without_tablets):
    """SAI with a multi-column (local index) target like ((p), v) should
    succeed — vector_index supports local indexes.
    Cassandra SAI does not support this multi-column syntax."""
    schema = 'p int, q int, v vector<float, 3>, PRIMARY KEY (p, q)'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}((p), v) USING 'sai'")


# --- SAI on non-vector columns: rejected by ScyllaDB, accepted by Cassandra ---
#
# Cassandra SAI natively supports indexing regular and collection columns.
# ScyllaDB only accepts SAI for vector columns and rejects non-vector targets
# with an explicit error.  These tests validate the ScyllaDB rejection; they
# would pass on Cassandra but are expected to fail on ScyllaDB (scylla_only).


def test_sai_on_regular_column_rejected(cql, test_keyspace, scylla_only):
    """SAI on a regular (non-vector) column is rejected by ScyllaDB.
    On Cassandra this would succeed since SAI supports arbitrary columns."""
    schema = 'p int PRIMARY KEY, x text'
    with new_test_table(cql, test_keyspace, schema) as table:
        with pytest.raises(InvalidRequest, match='SAI.*only supported on vector columns'):
            cql.execute(f"CREATE CUSTOM INDEX ON {table}(x) USING 'sai'")


def test_sai_entries_on_map_rejected(cql, test_keyspace, scylla_only):
    """SAI ENTRIES index on a MAP column is rejected by ScyllaDB.
    On Cassandra this is a common pattern for metadata filtering."""
    schema = 'p int PRIMARY KEY, metadata_s map<text, text>'
    with new_test_table(cql, test_keyspace, schema) as table:
        with pytest.raises(InvalidRequest, match='SAI.*only supported on vector columns'):
            cql.execute(
                f"CREATE CUSTOM INDEX ON {table}(ENTRIES(metadata_s)) "
                f"USING 'sai'"
            )


def test_sai_on_nonexistent_column(cql, test_keyspace, skip_on_scylla_vnodes):
    """SAI on a non-existent column should fail with an appropriate error."""
    schema = 'p int PRIMARY KEY, v vector<float, 3>'
    with new_test_table(cql, test_keyspace, schema) as table:
        with pytest.raises(InvalidRequest, match='No column definition found|Undefined column name|SAI.*only supported on vector columns'):
            cql.execute(f"CREATE CUSTOM INDEX ON {table}(nonexistent) USING 'sai'")


def test_sai_vector_index_with_source_model(cql, test_keyspace, scylla_only, skip_without_tablets):
    """The source_model option (used by Cassandra client libraries like CassIO
    to tag the embedding model) is rejected by Cassandra SAI as an unrecognized
    property. ScyllaDB accepts and preserves it in DESCRIBE output for
    compatibility with those libraries."""
    schema = 'p int PRIMARY KEY, v vector<float, 3>'
    with new_test_table(cql, test_keyspace, schema) as table:
        idx = unique_name()
        cql.execute(
            f"CREATE CUSTOM INDEX {idx} ON {table}(v) USING 'sai' "
            f"WITH OPTIONS = {{'similarity_function': 'COSINE', "
            f"'source_model': 'ada002'}}"
        )
        desc = cql.execute(f"DESC INDEX {test_keyspace}.{idx}").one().create_statement
        # ScyllaDB preserves all user-provided options in DESCRIBE output.
        assert "'source_model': 'ada002'" in desc
        assert "'similarity_function'" in desc


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


def test_try_create_cdc_with_vector_search_enabled(scylla_only, cql, test_keyspace, skip_without_tablets):
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


def test_try_disable_cdc_with_vector_search_enabled(scylla_only, cql, test_keyspace, skip_without_tablets):
    schema = "pk int primary key, v vector<float, 3>"
    with new_test_table(cql, test_keyspace, schema) as table:
        # Enable Vector Search by creating a vector index.
        cql.execute(f"CREATE INDEX v_idx ON {table} (v) USING 'vector_index'")

        # Disallow disabling CDC when Vector Search is enabled.
        with pytest.raises(InvalidRequest, match="Cannot disable CDC when Vector Search is enabled on the table"):
            cql.execute(f"ALTER TABLE {table} WITH cdc = {{'enabled': False}}")


def test_try_enable_vector_search_with_cdc_enabled(scylla_only, cql, test_keyspace, skip_without_tablets):
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


def test_try_enable_vector_search_with_cdc_disabled(scylla_only, cql, test_keyspace, skip_without_tablets):
    schema = "pk int primary key, v vector<float, 3>"
    with new_test_table(cql, test_keyspace, schema) as table:
        # Disallow creating the vector index when CDC is explicitly disabled.
        assert alter_cdc(cql, table, {'enabled': False, 'ttl': 172800, 'postimage' : True})
        with pytest.raises(InvalidRequest, match="Cannot create the vector index when CDC is explicitly disabled."):
            cql.execute(f"CREATE INDEX v_idx ON {table} (v) USING 'vector_index'")

        # Allow creating the vector index when CDC is enabled again.
        assert alter_cdc(cql, table, {'enabled': True})
        assert create_index(cql, test_keyspace, table, "v")

# When a vector index is created on a table, writes to it should appear in the
# CDC log, even though CDC was never explicitly enabled. This should be true
# both for regular writes and writes using LWT.
# This is a regression test for SCYLLADB-1342, where LWT writes forgot to update
# the CDC log.
def test_vector_index_writes_appear_in_cdc_log(cql, test_keyspace, scylla_only, skip_without_tablets):
    schema = "p int primary key, v vector<float, 3>"
    with new_test_table(cql, test_keyspace, schema) as table:
        _, table_name = table.split('.')
        cdc_log_table = f"{test_keyspace}.{table_name}_scylla_cdc_log"

        # Creating a vector index auto-enables CDC on the table.
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(v) USING 'vector_index'")

        # Confirm that traditional CDC was *not* enabled, but the CDC log
        # table was created (SELECT from it generates no error).
        cdc_opts = cql.execute(f"SELECT cdc FROM system_schema.scylla_tables WHERE keyspace_name = '{test_keyspace}' AND table_name = '{table_name}'").one().cdc
        assert not cdc_opts or cdc_opts.get('enabled') != 'true'
        cql.execute(f"SELECT * FROM {cdc_log_table}")

        # A regular write should appear in the CDC log
        cql.execute(f"INSERT INTO {table} (p, v) VALUES (1, [1.0, 2.0, 3.0])")
        rows_after_regular = list(cql.execute(f"SELECT * FROM {cdc_log_table}"))
        assert len(rows_after_regular) > 0

        # A successful LWT write should appear in the CDC log (reproduces
        # SCYLLADB-1342)
        cql.execute(f"INSERT INTO {table} (p, v) VALUES (2, [4.0, 5.0, 6.0]) IF NOT EXISTS")
        rows_after_lwt = list(cql.execute(f"SELECT * FROM {cdc_log_table} ALLOW FILTERING"))
        assert len(rows_after_lwt) > len(rows_after_regular)

        # An unsuccessful LWT write should not appear in the CDC log.
        # p=2 already exists, so this IF NOT EXISTS will not be applied.
        cql.execute(f"INSERT INTO {table} (p, v) VALUES (2, [7.0, 8.0, 9.0]) IF NOT EXISTS")
        rows_after_failed_lwt = list(cql.execute(f"SELECT * FROM {cdc_log_table} ALLOW FILTERING"))
        assert len(rows_after_failed_lwt) == len(rows_after_lwt)

        # Above we tested sucessful LWT writes using INSERT. Verify the same
        # also for UPDATE and DELETE.
        # A successful LWT UPDATE (p=1 exists) should appear in the CDC log.
        cql.execute(f"UPDATE {table} SET v = [0.1, 0.2, 0.3] WHERE p = 1 IF EXISTS")
        rows_after_lwt_update = list(cql.execute(f"SELECT * FROM {cdc_log_table} ALLOW FILTERING"))
        assert len(rows_after_lwt_update) > len(rows_after_failed_lwt)

        # A successful LWT DELETE (p=2 exists) should appear in the CDC log.
        cql.execute(f"DELETE FROM {table} WHERE p = 2 IF EXISTS")
        rows_after_lwt_delete = list(cql.execute(f"SELECT * FROM {cdc_log_table} ALLOW FILTERING"))
        assert len(rows_after_lwt_delete) > len(rows_after_lwt_update)

# Validates fix for VECTOR-179: a vector search with tracing enabled
# used to crash Scylla instead of returning an error.
def test_vector_search_when_tracing_is_enabled(cql, test_keyspace, scylla_only, skip_without_tablets):
    schema = "p int primary key, v vector<float, 3>"
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(v) USING 'vector_index'")
        with pytest.raises(InvalidRequest, match="Vector Store is disabled"):
            cql.execute(
                f"SELECT * FROM {table} ORDER BY v ANN OF [0.2,0.3,0.4] LIMIT 1",
                trace=True,
            )



# Validates fix for VECTOR-374: ANN query with PK restriction used to be
# rejected even though the restriction is on a partition-key column.
def test_ann_query_with_pk_restriction(cql, test_keyspace, skip_without_tablets):
    schema = 'p int primary key, q int, v vector<float, 3>'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(v) USING 'vector_index'")
