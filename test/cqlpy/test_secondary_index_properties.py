# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

# Tests for properties of secondary indexes.

import itertools
import pytest
import uuid

from cassandra.protocol import SyntaxException, InvalidRequest, ConfigurationException
from test.cqlpy.util import new_test_table, unique_name

# Verify that creating a named index with simple valid view properties finishes successfully,
# and that the options are really applied, which should be reflected in `system_schema.views`.
def test_create_index_simple_valid_view_properties(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, v int, u int") as table:
        index_name = unique_name()
        view_name = f"{index_name}_index"

        def check_for_aux(property, value, proj):

            set_value = proj(value)
            cql.execute(f"CREATE INDEX {index_name} ON {table}(v) WITH {property} = {set_value}")

            row_value = cql.execute(f"SELECT {property} FROM system_schema.views WHERE " \
                                    f"keyspace_name = '{test_keyspace}' AND view_name = '{view_name}'").one()

            assert hasattr(row_value, property)
            assert getattr(row_value, property) == value

            cql.execute(f"DROP INDEX {test_keyspace}.{index_name}")

        def check_for_str(property, value):
            check_for_aux(property, value, lambda val: f"'{val}'")
        def check_for_int(property, value):
            check_for_aux(property, value, lambda val: int(val))
        def check_for_float(property, value):
            check_for_aux(property, value, lambda val: float(val))

        check_for_float("bloom_filter_fp_chance", 0.13)
        check_for_str("comment", "some not really funny comment")

        # FIXME: Once scylladb/scylladb#2431 is resolved, change this to a custom value.
        check_for_float("crc_check_chance", 1)

        check_for_int("gc_grace_seconds", 3)
        check_for_int("max_index_interval", 2013)
        check_for_int("memtable_flush_period_in_ms", 60013)
        check_for_int("min_index_interval", 133)
        check_for_str("speculative_retry", "73.0PERCENTILE")

# Tables and materialized views accept a number of obsolete properties. Although they don't have
# any affect, they're still seen as valid part of the syntax. Since secondary indexes are not bound
# by any contract that would require backward compatibility in that regard, we forbid them.
# Verify that that's what happens indeed.
def test_create_index_obsolete_view_properties(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, v int, u int") as table:
        index_name = unique_name()

        def do_test(property, value):
            with pytest.raises(SyntaxException, match=f"Unknown property '{property}'"):
                cql.execute(f"CREATE INDEX {index_name} ON {table}(v) WITH {property} = {value}")

        # Values should not matter here, so let's use anything.
        do_test("index_interval", 128)
        do_test("replicate_on_write", "true")
        do_test("populate_io_cache_on_flush", "true")
        do_test("read_repair_chance", 0.0)
        do_test("dclocal_read_repair_chance", 0.1)

# Verify that we cannot set a non-zero default TTL when creating an index, just like
# when we're creating a materialized view. Check that we get a proper error message.
def test_create_index_default_ttl(cql, test_keyspace, scylla_only):
    err_msg = "Cannot set or alter default_time_to_live for a materialized view. " \
              "Data in a materialized view always expire at the same time than " \
              "the corresponding data in the parent table."

    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, v int, u int") as table:
        index_name = unique_name()

        with pytest.raises(InvalidRequest, match=err_msg):
            cql.execute(f"CREATE INDEX {index_name} ON {table}(v) WITH default_time_to_live = 13")
        # FIXME: This should also throw an invalid request, but it's a pre-existing problem.
        with pytest.raises(ConfigurationException, match="default_time_to_live cannot be smaller than 0"):
            cql.execute(f"CREATE INDEX {index_name} ON {table}(v) WITH default_time_to_live = -13")
        cql.execute(f"CREATE INDEX {index_name} ON {table}(v) WITH default_time_to_live = 0")

# Verify that we can set the caching property of an index, and that it will be successfully
# applied to the underlying materialized view. That should be reflected in `system_schema.views`.
def test_create_index_caching(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, v int, u int") as table:
        def do_test(enabled, keys, rows_per_partition):
            index_name = unique_name()
            view_name = f"{index_name}_index"
            enabled = str(enabled).lower()

            cql.execute(f"CREATE INDEX {index_name} ON {table}(v) WITH caching = " \
                        f"{{'enabled': {enabled}, 'keys': '{keys}', 'rows_per_partition': '{rows_per_partition}'}}")

            row_value = cql.execute(f"SELECT caching FROM system_schema.views WHERE " \
                                    f"keyspace_name = '{test_keyspace}' AND view_name = '{view_name}'").one()

            assert hasattr(row_value, "caching")
            caching = row_value.caching

            # This is a bit peculiar, but for some reason, Scylla decides to
            # not include this value if caching is enabled.
            if enabled == "false":
                assert "enabled" in caching
                assert caching.get("enabled") == enabled
            else:
                assert "enabled" not in caching

            assert "keys" in caching
            assert caching.get("keys") == keys
            assert "rows_per_partition" in caching
            assert caching.get("rows_per_partition") == rows_per_partition

            cql.execute(f"DROP INDEX {test_keyspace}.{index_name}")

        for e, k, r in itertools.product([True, False], ["ALL", "NONE"], ["ALL", "NONE"]):
            do_test(e, k, r)

# Verify that we can set the compaction property of an index, and that it will be successfully
# applied to the underlying materialized view. That should be reflected in `system_schema.views`.
def test_create_index_compaction(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, v int, u int") as table:
        def do_test(class_opt):
            index_name = unique_name()
            view_name = f"{index_name}_index"

            cql.execute(f"CREATE INDEX {index_name} ON {table}(v) WITH compaction = {{'class': '{class_opt}'}}")

            row_value = cql.execute(f"SELECT compaction FROM system_schema.views WHERE " \
                                    f"keyspace_name = '{test_keyspace}' AND view_name = '{view_name}'").one()

            assert hasattr(row_value, "compaction")
            compaction = row_value.compaction

            assert "class" in compaction
            assert compaction.get("class") == class_opt

            cql.execute(f"DROP INDEX {test_keyspace}.{index_name}")

        for class_opt in ["TimeWindowCompactionStrategy", "IncrementalCompactionStrategy"]:
            do_test(class_opt)

# Verify that we can set the compression property of an index, and that it will be successfully
# applied to the underlying materialized view. That should be reflected in `system_schema.views`.
def test_create_index_compression(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, v int, u int") as table:
        def do_test(compression_opt, chunk_opt):
            index_name = unique_name()
            view_name = f"{index_name}_index"

            opts = [f"'sstable_compression': '{compression_opt}'"]
            if chunk_opt is not None:
                opts.append(f"'chunk_length_in_kb': {chunk_opt}")
            opts = ", ".join(opts)

            cql.execute(f"CREATE INDEX {index_name} ON {table}(v) WITH compression = {{{opts}}}")

            row_value = cql.execute(f"SELECT compression FROM system_schema.views WHERE " \
                                    f"keyspace_name = '{test_keyspace}' AND view_name = '{view_name}'").one()

            assert hasattr(row_value, "compression")
            compression = row_value.compression

            assert "sstable_compression" in compression
            assert compression.get("sstable_compression") == compression_opt

            if chunk_opt:
                assert "chunk_length_in_kb" in compression
                assert compression.get("chunk_length_in_kb") == chunk_opt

            cql.execute(f"DROP INDEX {test_keyspace}.{index_name}")

        compressor_opts = ["org.apache.cassandra.io.compress.LZ4Compressor", "org.apache.cassandra.io.compress.SnappyCompressor"]
        chunk_opts = [None, "4", "8"]

        for compression_opt, chunk_opt in itertools.product(compressor_opts, chunk_opts):
            do_test(compression_opt, chunk_opt)

# Verify that we can set the tombstone_gc property of an index, and that it will be successfully
# applied to the underlying materialized view. That should be reflected in `system_schema.views`.
def test_create_index_extensions(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, v int, u int") as table:
        def do_test(property_value):
            index_name = unique_name()

            cql.execute(f"CREATE INDEX {index_name} ON {table}(v) WITH tombstone_gc = {{{property_value}}}")

            # Unfortunately, we need to use DESCRIBE to confirm the property has been applied.
            # For more context, see issue: scylladb/scylladb#9722.
            result = cql.execute(f"DESC INDEX {test_keyspace}.{index_name} WITH INTERNALS").one()

            assert hasattr(result, "create_statement")
            assert f"tombstone_gc = {{{property_value}}}" in result.create_statement

            cql.execute(f"DROP INDEX {test_keyspace}.{index_name}")

        do_test("'mode': 'timeout', 'propagation_delay_in_seconds': '4200'")
        do_test("'mode': 'disabled', 'propagation_delay_in_seconds': '4200'")

# Verify that we can set the ID of an index, and that it will be successfully applied to
# the underlying materialized view. That should be reflected in `system_schema.views`.
def test_create_index_id(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, v int, u int") as table:
        # It's virtually impossible that Scylla will have a table with this specific UUID.
        # Let's take the risk and verify that we can really set the ID of the underlying MV.
        id = uuid.UUID("018ad550-b25d-09d0-7e90-ea5438411dc7")

        index_name = unique_name()
        view_name = f"{index_name}_index"

        cql.execute(f"CREATE INDEX {index_name} ON {table}(v) WITH ID = {id}")
        row_value = cql.execute(f"SELECT id FROM system_schema.views WHERE " \
                                f"keyspace_name = '{test_keyspace}' AND view_name = '{view_name}'").one()
        assert hasattr(row_value, "id")
        assert row_value.id == id

# Verify that we get an error if we attempt to create an index with an already used ID.
def test_create_index_already_used_id(cql, test_keyspace, scylla_only):
    # It's virtually impossible that Scylla will have a table with this specific UUID.
    id = uuid.UUID("018ad550-b25d-09d0-7e90-ea5438411dc7")

    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, v int, u int", f"WITH ID = {id}") as table:
        index_name = unique_name()
        with pytest.raises(InvalidRequest, match=f"Table with ID {id} already exists: {table}"):
            cql.execute(f"CREATE INDEX {index_name} ON {table}(v) WITH ID = {id}")

# Verify that we cannot use COMPACT STORAGE with an index.
def test_create_index_compact_storage(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, v int, u int") as table:
        index_name = unique_name()
        with pytest.raises(InvalidRequest, match="Cannot use 'COMPACT STORAGE' when defining a materialized view"):
            cql.execute(f"CREATE INDEX {index_name} ON {table}(v) WITH COMPACT STORAGE")

# Verify that indexes do not allow for specifying the clustering order, unlike materialized views.
# FIXME: This is a temporary limitation and should be rid of.
def test_create_index_clustering_order_by(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, v int, u int") as table:
        index_name = unique_name()
        with pytest.raises(InvalidRequest, match="Indexes do not allow for specifying the clustering order"):
            cql.execute(f"CREATE INDEX {index_name} ON {table}(v) WITH CLUSTERING ORDER BY (p ASC)")

# Verify that we can set synchronous updates when creating an index and that it works as intended.
# This test is an adjusted version of a similar one in `cqlpy/test_materialized_view.py`.
def test_create_index_synchronous_updates(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, v int, u int") as table:
        # Synchronous updates.
        s_index_name = unique_name()
        s_view_name = f"{s_index_name}_index"
        # Asynchronous updates.
        as_index_name = unique_name()
        as_view_name = f"{as_index_name}_index"

        cql.execute(f"CREATE INDEX {s_index_name} ON {table}(v) WITH synchronous_updates = true")
        cql.execute(f"CREATE INDEX {as_index_name} ON {table}(u) WITH synchronous_updates = false")

        # Execute a query and inspect its tracing info.
        res = cql.execute(f"INSERT INTO {table} (p, v, u) VALUES (13, 29, 37)", trace=True)
        trace = res.get_query_trace()

        wanted_trace = f"Forcing {test_keyspace}.{s_view_name} view update to be synchronous"
        unwanted_trace = f"Forcing {test_keyspace}.{as_view_name} view update to be synchronous"

        found_wanted_trace = False

        for event in trace.events:
            assert unwanted_trace not in event.description
            if wanted_trace in event.description:
                found_wanted_trace = True

        assert found_wanted_trace

# Verify that we cannot create an index with CDC enabled.
def test_create_index_cdc(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, v int, u int") as table:
        index_name = unique_name()
        with pytest.raises(InvalidRequest, match="Cannot enable CDC for a materialized view"):
            cql.execute(f"CREATE INDEX {index_name} ON {table}(v) WITH cdc = {{'enabled': true}}")

def test_create_index_invalid_simple_property(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, v int, u int") as table:
        index_name = unique_name()

        def do_test(exception_type, property, errmsg):
            with pytest.raises(exception_type, match=errmsg):
                cql.execute(f"CREATE INDEX {index_name} ON {table}(v) WITH {property}")

        def do_syntax_test(property):
            do_test(SyntaxException, property, None)
        def do_configuration_error_test(property, errmsg=None):
            do_test(ConfigurationException, property, errmsg)

        # Invalid type: number instead of a string.
        # FIXME: Numbers are accepted in places of strings, but that shouldn't happen.
        # do_syntax_test("comment = 23")
        # do_syntax_test("speculative_retry = 13")

        # Invalid type: string instead of a number.
        do_syntax_test("bloom_filter_fp_chance = 'not a number'")
        # FIXME: This is accepted. Probably related to scylladb/scylladb#2431.
        # do_syntax_test("crc_check_chance = 'not a number'")
        do_syntax_test("default_time_to_live = 'not a number'")
        do_syntax_test("gc_grace_seconds = 'not a number'")
        do_syntax_test("max_index_interval = 'not a number'")
        do_syntax_test("memtable_flush_period_in_ms = 'not a number'")
        do_syntax_test("min_index_interval = 'not a number'")

        # Invalid number type: double instead of int.
        # FIXME: Currently, Scylla accepts these values, but they should result in a syntax exception.
        # do_syntax_test("default_time_to_live = 1.23")
        # do_syntax_test("gc_grace_seconds = 1.23")
        # do_syntax_test("max_index_interval = 4023.23")
        # do_syntax_test("memtable_flush_period_in_ms = 1.23")
        # do_syntax_test("min_index_interval = 1.23")
        # do_syntax_test("speculative_retry = 1.23")

        # Invalid value: probability out of range [0, 1].
        do_configuration_error_test("bloom_filter_fp_chance = 1.23",
                                    r"bloom_filter_fp_chance must be larger than 6.71e-05 and " \
                                    r"less than or equal to 1.0 \(got 1.23\)")
        do_configuration_error_test("bloom_filter_fp_chance = -0.5",
                                    r"bloom_filter_fp_chance must be larger than 6.71e-05 and " \
                                    r"less than or equal to 1.0 \(got -0.5\)")

# Verify that creating an index with mismatched interval boundaries fails.
def test_create_index_with_invalid_intervals(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, v int, u int") as table:
        index_name = unique_name()
        with pytest.raises(ConfigurationException, match="max_index_interval must be greater than min_index_interval"):
            cql.execute(f"CREATE INDEX {index_name} ON {table}(v) " \
                         "WITH max_index_interval = 2010 AND min_index_interval = 4020")

# Verify that trying to create an index with an invalid memtable_flush_period_in_ms fails.
def test_create_index_with_invalid_memtable_flush_period(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, v int, u int") as table:
        index_name = unique_name()
        errmsg = "memtable_flush_period_in_ms must be 0 or greater than 60000"
        with pytest.raises(ConfigurationException, match=errmsg):
            cql.execute(f"CREATE INDEX {index_name} ON {table}(v) WITH memtable_flush_period_in_ms = -70000")
        with pytest.raises(ConfigurationException, match=errmsg):
            cql.execute(f"CREATE INDEX {index_name} ON {table}(v) WITH memtable_flush_period_in_ms = 30000")

# Vector indexes don't use materialized views. Verify that you cannot specify view properties when creating one.
def test_create_vector_index_with_view_properties(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, v vector<float, 3>") as table:
        index_name = unique_name()
        with pytest.raises(InvalidRequest, match="You cannot use view properties with a vector index"):
            cql.execute(f"CREATE CUSTOM INDEX {index_name} ON {table}(v) USING 'vector_index' WITH gc_grace_seconds = 13")
