# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

from cassandra.protocol import InvalidRequest

from .cassandra_tests.porting import assert_rows_ignoring_order
from .util import new_test_table, unique_name
from .nodetool import flush
import pytest
import time

@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #16317")]), "vnodes"],
                         indirect=True)
def test_vsc_add_drop_index(scylla_only, cql, test_keyspace):
    schema = "pk int primary key, v1 vector<float, 3>, v2 vector<float, 3>"
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE INDEX ON {table} (v1) USING 'vector_index'")
        with pytest.raises(InvalidRequest, match="Unrecognized name v2"):
            cql.execute(f"SELECT pk, v1, v2 FROM {table}_scylla_vsc_log")
        cql.execute(f"SELECT pk, v1 FROM {table}_scylla_vsc_log")

        cql.execute(f"CREATE INDEX ON {table} (v2) USING 'vector_index'")
        cql.execute(f"SELECT pk, v1, v2 FROM {table}_scylla_vsc_log")

        cql.execute(f"DROP INDEX {table}_v1_idx")
        with pytest.raises(InvalidRequest, match="Unrecognized name v1"):
            cql.execute(f"SELECT pk, v1, v2 FROM {table}_scylla_vsc_log")
        cql.execute(f"SELECT pk, v2 FROM {table}_scylla_vsc_log")

# For a table named "xyz", the VSC table is always named "xyz_scylla_vsc_log".
# Check what happens if a table called "xyz_scylla_vsc_log" already exists
# (as a normal table), and then we try to create index on "xyz"'s vector column
# using 'vector_index' custom index class.
# Unlike the secondary-index code which tries to find a different name to
# use for its backing view, the VSC code doesn't do that, but creating the
# index using 'vector_index' custom class should fail gracefully with a clear
# error message, and this test verifies that.
@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #16317")]), "vnodes"],
                         indirect=True)
def test_vsc_taken_log_name(scylla_only, cql, test_keyspace):
    name = test_keyspace + "." + unique_name()
    cql.execute(f"CREATE TABLE {name}_scylla_vsc_log (p int PRIMARY KEY)")
    try:
        schema = "pk int primary key, v vector<float, 3>"
        # We can't create an index using 'vector_index' on vector column of table {name}:
        with pytest.raises(InvalidRequest, match=f"{name}_scylla_vsc_log already exists"):
            cql.execute(f"CREATE TABLE {name} ({schema})")
            cql.execute(f"CREATE INDEX ON {name} (v) USING 'vector_index'")
            cql.execute(f"DROP TABLE {name}")
    finally:
        cql.execute(f"DROP TABLE {name}_scylla_vsc_log")

@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #16317")]), "vnodes"],
                         indirect=True)
def test_both_cdc_vsc_enabled(scylla_only, cql, test_keyspace):
    schema = "pk int primary key, v vector<float, 3>"
    extra = " with cdc = {'enabled': true}"
    with new_test_table(cql, test_keyspace, schema, extra) as table:
        cql.execute(f"CREATE INDEX ON {table} (v) USING 'vector_index'")

        # Log to both tables
        cql.execute(f"INSERT INTO {table} (pk, v) VALUES (1, [1.0, 2.0, 3.0])")
        cdc_results = cql.execute(f"SELECT \"cdc$operation\", pk, v FROM {table}_scylla_cdc_log")
        vsc_results = cql.execute(f"SELECT \"cdc$operation\", pk, v FROM {table}_scylla_vsc_log")
        assert_rows_ignoring_order(cdc_results,
            [2, 1, [1.0, 2.0, 3.0]],
        )
        assert_rows_ignoring_order(vsc_results,
            [2, 1, [1.0, 2.0, 3.0]],
        )

        # Change CDC options and check if it affects VSC

        # Disable CDC
        cql.execute(f"ALTER TABLE {table} WITH cdc = {{'enabled' : false}}")
        cql.execute(f"INSERT INTO {table} (pk, v) VALUES (2, [4.0, 5.0, 6.0])")
        cdc_results = cql.execute(f"SELECT \"cdc$operation\", pk, v FROM {table}_scylla_cdc_log")
        vsc_results = cql.execute(f"SELECT \"cdc$operation\", pk, v FROM {table}_scylla_vsc_log")
        assert_rows_ignoring_order(cdc_results,
            [2, 1, [1.0, 2.0, 3.0]],
        )
        assert_rows_ignoring_order(vsc_results,
            [2, 1, [1.0, 2.0, 3.0]],
            [2, 2, [4.0, 5.0, 6.0]],
        )
        # Enable CDC with TTL set to 2 seconds
        cql.execute(f"ALTER TABLE {table} WITH cdc = {{'enabled' : true, 'ttl' : 2}}")
        cql.execute(f"INSERT INTO {table} (pk, v) VALUES (3, [7.0, 8.0, 9.0])")
        cdc_results = cql.execute(f"SELECT \"cdc$operation\", pk, v FROM {table}_scylla_cdc_log")
        vsc_results = cql.execute(f"SELECT \"cdc$operation\", pk, v FROM {table}_scylla_vsc_log")
        assert_rows_ignoring_order(cdc_results,
            [2, 1, [1.0, 2.0, 3.0]],
            [2, 3, [7.0, 8.0, 9.0]],
        )
        assert_rows_ignoring_order(vsc_results,
            [2, 1, [1.0, 2.0, 3.0]],
            [2, 2, [4.0, 5.0, 6.0]],
            [2, 3, [7.0, 8.0, 9.0]],
        )
        # Wait for TTL to expire
        time.sleep(3)
        cdc_results = cql.execute(f"SELECT \"cdc$operation\", pk, v FROM {table}_scylla_cdc_log")
        vsc_results = cql.execute(f"SELECT \"cdc$operation\", pk, v FROM {table}_scylla_vsc_log")
        assert_rows_ignoring_order(cdc_results,
            [2, 1, [1.0, 2.0, 3.0]],
        )
        assert_rows_ignoring_order(vsc_results,
            [2, 1, [1.0, 2.0, 3.0]],
            [2, 2, [4.0, 5.0, 6.0]],
            [2, 3, [7.0, 8.0, 9.0]],
        )
        # Enable CDC with delta set to keys only
        cql.execute(f"ALTER TABLE {table} WITH cdc = {{'enabled' : true, 'delta' : 'keys'}}")
        cql.execute(f"INSERT INTO {table} (pk, v) VALUES (4, [10.0, 11.0, 12.0])")
        cdc_results = cql.execute(f"SELECT \"cdc$operation\", pk, v FROM {table}_scylla_cdc_log")
        vsc_results = cql.execute(f"SELECT \"cdc$operation\", pk, v FROM {table}_scylla_vsc_log")
        assert_rows_ignoring_order(cdc_results,
            [2, 1, [1.0, 2.0, 3.0]],
            [2, 4, None],
        )
        assert_rows_ignoring_order(vsc_results,
            [2, 1, [1.0, 2.0, 3.0]],
            [2, 2, [4.0, 5.0, 6.0]],
            [2, 3, [7.0, 8.0, 9.0]],
            [2, 4, [10.0, 11.0, 12.0]],
        )
        # Enable CDC with preimage
        cql.execute(f"ALTER TABLE {table} WITH cdc = {{'enabled' : true, 'preimage' : true}}")
        cql.execute(f"INSERT INTO {table} (pk, v) VALUES (1, [0.0, 0.0, 0.0])")
        cdc_results = cql.execute(f"SELECT \"cdc$operation\", pk, v FROM {table}_scylla_cdc_log")
        vsc_results = cql.execute(f"SELECT \"cdc$operation\", pk, v FROM {table}_scylla_vsc_log")
        assert_rows_ignoring_order(cdc_results,
            [2, 1, [1.0, 2.0, 3.0]],
            [2, 4, None],
            [0, 1, [1.0, 2.0, 3.0]],
            [2, 1, [0.0, 0.0, 0.0]],
        )
        assert_rows_ignoring_order(vsc_results,
            [2, 1, [1.0, 2.0, 3.0]],
            [2, 2, [4.0, 5.0, 6.0]],
            [2, 3, [7.0, 8.0, 9.0]],
            [2, 4, [10.0, 11.0, 12.0]],
            [2, 1, [0.0, 0.0, 0.0]],
        )
        # Enable CDC with postimage
        cql.execute(f"ALTER TABLE {table} WITH cdc = {{'enabled' : true, 'postimage' : true}}")
        cql.execute(f"INSERT INTO {table} (pk, v) VALUES (1, [1.0, 2.0, 3.0])")
        cdc_results = cql.execute(f"SELECT \"cdc$operation\", pk, v FROM {table}_scylla_cdc_log")
        vsc_results = cql.execute(f"SELECT \"cdc$operation\", pk, v FROM {table}_scylla_vsc_log")
        assert_rows_ignoring_order(cdc_results,
            [2, 1, [1.0, 2.0, 3.0]],
            [2, 4, None],
            [0, 1, [1.0, 2.0, 3.0]],
            [2, 1, [0.0, 0.0, 0.0]],
            [2, 1, [1.0, 2.0, 3.0]],
            [9, 1, [1.0, 2.0, 3.0]],
        )
        assert_rows_ignoring_order(vsc_results,
            [2, 1, [1.0, 2.0, 3.0]],
            [2, 2, [4.0, 5.0, 6.0]],
            [2, 3, [7.0, 8.0, 9.0]],
            [2, 4, [10.0, 11.0, 12.0]],
            [2, 1, [0.0, 0.0, 0.0]],
            [2, 1, [1.0, 2.0, 3.0]],
        )
        # Alter back to default cdc options
        cql.execute(f"ALTER TABLE {table} WITH cdc = {{'enabled' : true}}")

        # Drop index
        cql.execute(f"DROP INDEX {table}_v_idx")
        cql.execute(f"INSERT INTO {table} (pk, v) VALUES (5, [13.0, 14.0, 15.0])")
        cdc_results = cql.execute(f"SELECT \"cdc$operation\", pk, v FROM {table}_scylla_cdc_log")
        vsc_results = cql.execute(f"SELECT \"cdc$operation\", pk, v FROM {table}_scylla_vsc_log")
        assert_rows_ignoring_order(cdc_results,
            [2, 1, [1.0, 2.0, 3.0]],
            [2, 4, None],
            [0, 1, [1.0, 2.0, 3.0]],
            [2, 1, [0.0, 0.0, 0.0]],
            [2, 1, [1.0, 2.0, 3.0]],
            [9, 1, [1.0, 2.0, 3.0]],
            [2, 5, [13.0, 14.0, 15.0]],
        )
        assert_rows_ignoring_order(vsc_results,
            [2, 1, [1.0, 2.0, 3.0]],
            [2, 2, [4.0, 5.0, 6.0]],
            [2, 3, [7.0, 8.0, 9.0]],
            [2, 4, [10.0, 11.0, 12.0]],
            [2, 1, [0.0, 0.0, 0.0]],
            [2, 1, [1.0, 2.0, 3.0]],
        )
