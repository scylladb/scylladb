# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

import collections.abc
import contextlib
import re
from cassandra.cluster import ConsistencyLevel
from cassandra.query import SimpleStatement
from cassandra.protocol import InvalidRequest
from cassandra.util import SortedSet

from .util import new_test_table, unique_name, keyspace_has_tablets
from .nodetool import flush
import pytest
import time

# Waits until at least one CDC generation is published to system_distributed.cdc_generation_timestamps
# and system_distributed.cdc_streams_descriptions_v2. It may happen after the first node bootstraps.
def wait_for_first_cdc_generation(cql, timeout):
    query = SimpleStatement(
            "select time from system_distributed.cdc_generation_timestamps where key = 'timestamps'",
            consistency_level=ConsistencyLevel.ONE)
    deadline = time.time() + timeout
    while len(list(cql.execute(query))) == 0:
        assert time.time() < deadline, "Timed out waiting for the first CDC generation"
        time.sleep(1)

@pytest.mark.parametrize("test_keyspace", ["tablets", "vnodes"], indirect=True)
def test_cdc_log_entries_use_cdc_streams(scylla_only, cql, test_keyspace):
    '''Test that the stream IDs chosen for CDC log entries come from the CDC generation
    whose streams are listed in the streams description table. Since this test is executed
    on a single-node cluster, there is only one generation.'''

    has_tablets = keyspace_has_tablets(cql, test_keyspace)

    if not has_tablets:
        wait_for_first_cdc_generation(cql, 60)

    stream_ids = set()

    schema = "pk int primary key"
    extra = " with cdc = {'enabled': true}"
    with new_test_table(cql, test_keyspace, schema, extra) as table:
        stmt = cql.prepare(f"insert into {table} (pk) values (?) using timeout 5m")
        for i in range(100):
            cql.execute(stmt, [i])

        log_stream_ids = set(r[0] for r in cql.execute(f'select "cdc$stream_id" from {table}_scylla_cdc_log'))

        if has_tablets:
            streams_desc = cql.execute(SimpleStatement(
                    'select stream_id from system.cdc_streams',
                    consistency_level=ConsistencyLevel.ONE))
            for entry in streams_desc:
                stream_ids.add(entry.stream_id)

    # There should be exactly one generation, so we just select the streams
    if not has_tablets:
        streams_desc = cql.execute(SimpleStatement(
                'select streams from system_distributed.cdc_streams_descriptions_v2',
                consistency_level=ConsistencyLevel.ONE))
        for entry in streams_desc:
            stream_ids.update(entry.streams)

    assert(log_stream_ids.issubset(stream_ids))


# Test for #10473 - reading logs (from sstable) after dropping
# column in base.
def test_cdc_alter_table_drop_column(scylla_only, cql, test_keyspace):
    schema = "pk int primary key, v int"
    extra = " with cdc = {'enabled': true}"
    with new_test_table(cql, test_keyspace, schema, extra) as table:
        cql.execute(f"insert into {table} (pk, v) values (0, 0)")
        cql.execute(f"insert into {table} (pk, v) values (1, null)")
        flush(cql, table)
        flush(cql, table + "_scylla_cdc_log")
        cql.execute(f"alter table {table} drop v")
        cql.execute(f"select * from {table}_scylla_cdc_log")

# Regression test for #12098 - check that LWT inserts don't observe
# themselves inside preimages
def test_cdc_with_lwt_preimage(scylla_only, cql, test_keyspace):
    schema = "pk int primary key"
    extra = " with cdc = {'enabled': true, 'preimage':true}"
    with new_test_table(cql, test_keyspace, schema, extra) as table:
        stmt = cql.prepare(f"insert into {table} (pk) values (?) if not exists")
        for pk in range(500):
            cql.execute(stmt, (pk,))
        rs = cql.execute(f"select \"cdc$operation\" from {table}_scylla_cdc_log")
        # There should be no preimages because no keys were overwritten;
        # `cdc$operation` should only be `2` in all CDC log rows (denoting INSERT)
        assert all(r[0] == 2 for r in rs)

# For a table named "xyz", the CDC table is always named "xyz_scylla_cdc_log".
# Check what happens if a table called "xyz_scylla_cdc_log" already exists
# (as a normal table), and we then try to create "xyz" with CDC enabled,
# or create "xyz" without CDC and then try to enable it.
# Unlike the secondary-index code which tries to find a different name to
# use for its backing view, the CDC code doesn't do that, but creating the
# table with CDC (or enabling CDC) should fail gracefully with a clear
# error message, and this test verifies that.
def test_cdc_taken_log_name(scylla_only, cql, test_keyspace):
    name = test_keyspace + "." + unique_name()
    cql.execute(f"CREATE TABLE {name}_scylla_cdc_log (p int PRIMARY KEY)")
    try:
        schema = "pk int primary key, v int"
        extra = " with cdc = {'enabled': true}"
        # We can't create a table {name} with CDC enabled:
        with pytest.raises(InvalidRequest, match=f"{name}_scylla_cdc_log already exists"):
            cql.execute(f"CREATE TABLE {name} ({schema}) {extra}")
            cql.execute(f"DROP TABLE {name}")
        # We can create a table {name} *without* CDC enabled, but then we
        # can't enable CDC:
        try:
            cql.execute(f"CREATE TABLE {name} ({schema})")
            with pytest.raises(InvalidRequest, match=f"{name}_scylla_cdc_log already exists"):
                cql.execute(f"ALTER TABLE {name} {extra}")
        finally:
            cql.execute(f"DROP TABLE {name}")
    finally:
        cql.execute(f"DROP TABLE {name}_scylla_cdc_log")

def test_alter_column_of_cdc_log_table(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, v int, u int", "with cdc = {'enabled': true}") as table:
        cdc_log_table_name = f"{table}_scylla_cdc_log"
        errmsg = "You cannot modify the set of columns of a CDC log table directly. " \
                 "Modify the base table instead."

        with pytest.raises(InvalidRequest, match=errmsg):
            cql.execute(f"ALTER TABLE {cdc_log_table_name} ADD c int")
        with pytest.raises(InvalidRequest, match=errmsg):
            cql.execute(f"ALTER TABLE {cdc_log_table_name} DROP u")
        with pytest.raises(InvalidRequest, match=errmsg):
            cql.execute(f'ALTER TABLE {cdc_log_table_name} DROP "cdc$stream_id"')
        with pytest.raises(InvalidRequest, match=errmsg):
            cql.execute(f"ALTER TABLE {cdc_log_table_name} ALTER u TYPE float")
        with pytest.raises(InvalidRequest, match=errmsg):
            cql.execute(f'ALTER TABLE {cdc_log_table_name} ALTER "cdc$stream_id" TYPE float')

        cql.execute(f"ALTER TABLE {table} DROP u")
        with pytest.raises(InvalidRequest, match=errmsg):
            cql.execute(f'ALTER TABLE {cdc_log_table_name} DROP "cdc$deleted_u"')

def test_rename_column_of_cdc_log_table(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, v int, u int", "with cdc = {'enabled': true}") as table:
        cdc_log_table_name = f"{table}_scylla_cdc_log"
        errmsg = "Cannot rename a column of a CDC log table."
        with pytest.raises(InvalidRequest, match=errmsg):
            cql.execute(f"ALTER TABLE {cdc_log_table_name} RENAME u TO c")
        with pytest.raises(InvalidRequest, match=errmsg):
            cql.execute(f'ALTER TABLE {cdc_log_table_name} RENAME "cdc$stream_id" TO c')
        with pytest.raises(InvalidRequest, match=errmsg):
            cql.execute(f'ALTER TABLE {cdc_log_table_name} RENAME "cdc$stream_id" TO "cdc$c"')

        cql.execute(f"ALTER TABLE {table} DROP u")
        with pytest.raises(InvalidRequest, match=errmsg):
            cql.execute(f'ALTER TABLE {cdc_log_table_name} RENAME "cdc$deleted_u" TO c')

# Verify that you cannot modify the set of columns on a CDC log table, even when it stops being active.
def test_alter_column_of_inactive_cdc_log_table(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, v int, u int", "with cdc = {'enabled': true}") as table:
        cdc_log_table_name = f"{table}_scylla_cdc_log"

        # Insert some data just so we don't work an empty table. This shouldn't
        # have ANY impact on how the test should behave, but let's make do it anyway.
        cql.execute(f"INSERT INTO {table}(p, v, u) VALUES (1, 2, 3)")
        # Detach the log table.
        cql.execute(f"ALTER TABLE {table} WITH cdc = {{'enabled': false}}")

        errmsg = "You cannot modify the set of columns of a CDC log table directly. " \
                 "Although the base table has deactivated CDC, this table will continue being " \
                 "a CDC log table until it is dropped. If you want to modify the columns in it, " \
                 "you can only do that by reenabling CDC on the base table, which will reattach " \
                 "this log table. Then you will be able to modify the columns in the base table, " \
                 "and that will have effect on the log table too. Modifying the columns of a CDC " \
                 "log table directly is never allowed."

        with pytest.raises(InvalidRequest, match=errmsg):
            cql.execute(f"ALTER TABLE {cdc_log_table_name} ADD c int")
        with pytest.raises(InvalidRequest, match=errmsg):
            cql.execute(f"ALTER TABLE {cdc_log_table_name} DROP u")
        with pytest.raises(InvalidRequest, match=errmsg):
            cql.execute(f'ALTER TABLE {cdc_log_table_name} DROP "cdc$stream_id"')
        with pytest.raises(InvalidRequest, match=errmsg):
            cql.execute(f"ALTER TABLE {cdc_log_table_name} ALTER u TYPE float")
        with pytest.raises(InvalidRequest, match=errmsg):
            cql.execute(f'ALTER TABLE {cdc_log_table_name} ALTER "cdc$stream_id" TYPE float')

# Verify that the set of columnfs of a table whose name resembles that of a CDC log table is possible.
def test_alter_column_of_fake_cdc_log_table(cql, test_keyspace, scylla_only):
    name = unique_name()
    fake_cdc_log_table_name = f"{name}_scylla_cdc_log"

    try:
        cql.execute(f"CREATE TABLE {test_keyspace}.{fake_cdc_log_table_name} (p int PRIMARY KEY, v int)")
        cql.execute(f"ALTER TABLE {test_keyspace}.{fake_cdc_log_table_name} DROP v")
    finally:
        cql.execute(f"DROP TABLE IF EXISTS {test_keyspace}.{fake_cdc_log_table_name}")

# Verify that you cannot rename a column of a CDC log table, even when it stops being active.
def test_rename_column_of_inactive_cdc_log_table(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, v int, u int", "with cdc = {'enabled': true}") as table:
        cdc_log_table_name = f"{table}_scylla_cdc_log"

        # Insert some data just so we don't work an empty table. This shouldn't
        # have ANY impact on how the test should behave, but let's make do it anyway.
        cql.execute(f"INSERT INTO {table}(p, v, u) VALUES (1, 2, 3)")
        # Detach the log table.
        cql.execute(f"ALTER TABLE {table} WITH cdc = {{'enabled': false}}")

        errmsg = "You cannot rename a column of a CDC log table. Although the base table " \
                 "has deactivated CDC, this table will continue being a CDC log table until it " \
                 "is dropped."

        with pytest.raises(InvalidRequest, match=errmsg):
            cql.execute(f"ALTER TABLE {cdc_log_table_name} RENAME u TO c")
        with pytest.raises(InvalidRequest, match=errmsg):
            cql.execute(f'ALTER TABLE {cdc_log_table_name} RENAME "cdc$stream_id" TO c')
        with pytest.raises(InvalidRequest, match=errmsg):
            cql.execute(f'ALTER TABLE {cdc_log_table_name} RENAME "cdc$stream_id" TO "cdc$c"')

# Verify that you can rename a column in a table whose name resembles that of a CDC log table
# but that is NOT a CDC log table.
def test_rename_column_of_fake_cdc_log_table(cql, test_keyspace, scylla_only):
    name = unique_name()
    fake_cdc_log_table_name = f"{name}_scylla_cdc_log"

    try:
        cql.execute(f"CREATE TABLE {test_keyspace}.{fake_cdc_log_table_name} (p int PRIMARY KEY, v int)")
        cql.execute(f"ALTER TABLE {test_keyspace}.{fake_cdc_log_table_name} RENAME p TO q")
    finally:
        cql.execute(f"DROP TABLE IF EXISTS {test_keyspace}.{fake_cdc_log_table_name}")

# When the user disables CDC on a table, the CDC log table is not removed. Instead, it's detached from the base table,
# and it functions as a normal table (with some differences). If that log table lives up to the point when the user
# re-enabled CDC on the base table, instead of creating a new log table, the old one is re-attached to the base.
#
# Verify that changes in the base table after disabling CDC are reflected on the log table after re-enabling CDC.
# Verify that we can perform basic operations on the log table without running into problems.
@pytest.mark.parametrize("test_keyspace", ["tablets", "vnodes"], indirect=True)
def test_reattach_cdc_log_table_after_altering_base(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "p int, v int, u int, a int, PRIMARY KEY (p, v)", "WITH cdc = {'enabled': true}") as table:
        cdc_log_table = f"{table}_scylla_cdc_log"

        # Prefill the table with some data. It shouldn't matter, but let's do it anyway.
        cql.execute(f"INSERT INTO {table} (p, v, u, a) VALUES (1, 2, 3, 4)")
        # Detach the log table.
        cql.execute(f"ALTER TABLE {table} WITH cdc = {{'enabled': false}}")

        # Change the base table.
        cql.execute(f"ALTER TABLE {table} DROP a")
        cql.execute(f"ALTER TABLE {table} ADD b double")
        cql.execute(f"ALTER TABLE {table} RENAME v TO v2")

        # Make sure that the detached log table still exists and is not affected by any changes in the base table.
        cql.execute(f"INSERT INTO {table} (p, v2, u, b) VALUES (5, 6, 7, 9.2)")
        result = cql.execute(f"SELECT * FROM {cdc_log_table} WHERE p = 5 ALLOW FILTERING").all()
        assert len(result) == 0

        # Reattach the log table.
        cql.execute(f"ALTER TABLE {table} WITH cdc = {{'enabled': true}}")

        # Confirm the definition of the log table has been updated.
        expected_definition = f'CREATE TABLE {cdc_log_table} (\n' \
                               '    "cdc$stream_id" blob,\n' \
                               '    "cdc$time" timeuuid,\n' \
                               '    "cdc$batch_seq_no" int,\n' \
                               '    b double,\n' \
                               '    "cdc$deleted_b" boolean,\n' \
                               '    "cdc$deleted_u" boolean,\n' \
                               '    "cdc$end_of_batch" boolean,\n' \
                               '    "cdc$operation" tinyint,\n' \
                               '    "cdc$ttl" bigint,\n' \
                               '    p int,\n' \
                               '    u int,\n' \
                               '    v2 int,\n' \
                               '    v int,\n' \
                               '    "cdc$deleted_a" boolean,\n' \
                               '    a int,\n' \
                               '    PRIMARY KEY ("cdc$stream_id", "cdc$time", "cdc$batch_seq_no")\n' \
                               ')'
        describe_result = cql.execute(f"DESC TABLE {cdc_log_table} WITH INTERNALS").one()
        assert hasattr(describe_result, "create_statement")

        assert expected_definition in describe_result.create_statement
        assert f"ALTER TABLE {cdc_log_table} DROP v" in describe_result.create_statement
        assert f'ALTER TABLE {cdc_log_table} DROP "cdc$deleted_a"' in describe_result.create_statement
        assert f"ALTER TABLE {cdc_log_table} DROP a" in describe_result.create_statement

        # Changes on the base table should be reflected on the log table again.
        cql.execute(f"INSERT INTO {table} (p, v2, u, b) VALUES (10, 11, 12, 13.3)")
        result = cql.execute(f"SELECT * FROM {cdc_log_table} WHERE p = 10 ALLOW FILTERING").all()
        assert len(result) > 0

# When the user disables CDC on a table, the CDC log table is not removed. Instead, it's detached from the base table,
# and it functions as a normal table (with some differences). If that log table lives up to the point when the user
# re-enabled CDC on the base table, instead of creating a new log table, the old one is re-attached to the base.
#
# Verify that changes in the base table after disabling CDC are reflected on the log table after re-enabling CDC,
# and that the log table ends up with the same definition as if it had never been detached.
@pytest.mark.parametrize("test_keyspace", ["tablets", "vnodes"], indirect=True)
def test_reattach_cdc_log_table_after_altering_base_schema(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "p int, v int, u int, a int, PRIMARY KEY (p, v)", "WITH cdc = {'enabled': true}") as t1, \
          new_test_table(cql, test_keyspace, "p int, v int, u int, a int, PRIMARY KEY (p, v)", "WITH cdc = {'enabled': true}") as t2:
        # We'll be performing the same operations on the tables: one with disabled CDC, the other with enabled CDC.
        cql.execute(f"ALTER TABLE {t2} WITH cdc = {{'enabled': false}}")

        for table in [t1, t2]:
            cql.execute(f"ALTER TABLE {table} DROP a")
            cql.execute(f"ALTER TABLE {table} ADD b double")
            cql.execute(f"ALTER TABLE {table} RENAME v TO v2")

        cql.execute(f"ALTER TABLE {t2} WITH cdc = {{'enabled': true}}")

        cdc_log_t1 = f"{t1}_scylla_cdc_log"
        cdc_log_t2 = f"{t2}_scylla_cdc_log"

        # Note that we cannot use `WITH INTERNALS` here because it'll also include the UUIDs
        # of the tables, which will obviously be different.
        desc_t1 = cql.execute(f"DESC TABLE {cdc_log_t1}").one()
        assert hasattr(desc_t1, "create_statement")
        create_stmt_t1 = desc_t1.create_statement

        desc_t2 = cql.execute(f"DESC TABLE {cdc_log_t2}").one()
        assert hasattr(desc_t2, "create_statement")
        create_stmt_t2 = desc_t2.create_statement

        # We need to change the name of one of the tables.
        create_stmt_t2 = create_stmt_t2.replace(t2, t1)

        assert create_stmt_t1 == create_stmt_t2

# Verify that the `tombstone_gc` property is present after creating a CDC log table.
# Reproducer of scylladb/scylladb#25187.
def test_log_table_tombstone_gc_present(cql, test_keyspace, scylla_only):
    def get_log_alter_stmt(log_table_name: str):
        descs_it = cql.execute(f"DESCRIBE KEYSPACE {test_keyspace} WITH INTERNALS")
        [log_desc] = filter(lambda desc: desc.name == log_table_name, descs_it)
        assert hasattr(log_desc, "create_statement")
        return log_desc.create_statement

    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY", "WITH cdc = {'enabled': true}") as table:
        _, table_name = table.split(".")
        cdc_log_table = f"{table_name}_scylla_cdc_log"

        log_stmt = get_log_alter_stmt(cdc_log_table)
        # Doesn't matter what the value is. Just check the property is present.
        assert "tombstone_gc = {" in log_stmt

# Verify that performing a no-op ALTER TABLE statement (changing a property to the same value)
# on the log table doesn't change anything.
# Reproducer of scylladb/scylladb#25187.
def test_desc_log_table_properties_preserved_after_noop_alter(cql, test_keyspace, scylla_only):
    def get_log_alter_stmt(log_table_name: str):
        descs_it = cql.execute(f"DESCRIBE KEYSPACE {test_keyspace} WITH INTERNALS")
        [log_desc] = filter(lambda desc: desc.name == log_table_name, descs_it)
        assert hasattr(log_desc, "create_statement")
        return log_desc.create_statement

    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY", "WITH cdc = {'enabled': true}") as table:
        _, table_name = table.split(".")
        cdc_log_table = f"{table_name}_scylla_cdc_log"

        old_log_stmt = get_log_alter_stmt(cdc_log_table)
        # No-op.
        cql.execute(old_log_stmt)
        new_log_stmt = get_log_alter_stmt(cdc_log_table)

        assert old_log_stmt == new_log_stmt

# Verify that after detaching or reattaching a CDC log table, its properties
# are preserved.
#
# Reproduces scylladb/scylladb#25523.
def test_log_table_preserves_properties_after_reattachment(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY", "WITH cdc = {'enabled': true}") as table:
        cdc_log_table = f"{table}_scylla_cdc_log"

        # Every property here should have a non-custom value.
        properties = [
            ("bloom_filter_fp_chance", "0.13"),
            ("caching", "{'keys': 'ALL', 'rows_per_partition': 'NONE'}"),
            ("comment", "'some not very interesting custom comment'"),
            ("compaction", "{'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': '37', 'compaction_window_unit': 'MINUTES', 'expired_sstable_check_frequency_seconds': '1351'}"),
            ("compression", "{'sstable_compression': 'org.apache.cassandra.io.compress.SnappyCompressor'}"),

            # FIXME: Due to scylladb/scylladb#2431, we cannot change this property.
            # ("crc_check_chance", "1"),

            ("default_time_to_live", "2"),
            ("gc_grace_seconds", "3"),
            ("max_index_interval", "2371"),
            ("memtable_flush_period_in_ms", "60013"),
            ("min_index_interval", "137"),
            ("speculative_retry", "'95.0PERCENTILE'"),
            ("tombstone_gc", "{'mode': 'timeout', 'propagation_delay_in_seconds': '3719'}"),
        ]

        def validate_properties():
            desc_row = cql.execute(f"DESC TABLE {cdc_log_table} WITH INTERNALS").one()
            create_statement = desc_row.create_statement

            for property, value in properties:
                assert f"{property} = {value}" in create_statement

        for property, value in properties:
            cql.execute(f"ALTER TABLE {cdc_log_table} WITH {property} = {value}")

        # Step 1. Make sure that we really altered the CDC log table.
        validate_properties()

        # Step 2. Make sure that after detaching the log table, the properties are preserved.
        cql.execute(f"ALTER TABLE {table} WITH cdc = {{'enabled': false}}")
        validate_properties()

        # Step 3. Make sure that after reattaching the log table, the properties are preserved.
        cql.execute(f"ALTER TABLE {table} WITH cdc = {{'enabled': true}}")
        validate_properties()

# Verify that the ID of the log table doesn't change due to detaching or reattaching
# it via executing `ALTER TABLE <base_table> WITH cdc = {'enabled': <enabled>}`.
def test_log_table_preserves_id_after_reattachment(cql, test_keyspace, scylla_only):
    def get_definition(table_name: str):
        desc_row = cql.execute(f"DESC TABLE {table_name} WITH INTERNALS").one()
        return desc_row.create_statement

    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int", "WITH cdc = {'enabled': true}") as table:
        cdc_log_table = f"{table}_scylla_cdc_log"
        desc = get_definition(cdc_log_table)

        # String of form `ID = <ID>`
        match = re.search(r"(ID = \S+)", desc)
        id = match[1]

        # The ID should not change after detaching the log table.
        cql.execute(f"ALTER TABLE {table} WITH cdc = {{'enabled': false}}")
        assert id in get_definition(cdc_log_table)

        # The ID shouldn't change after reattaching the log table either.
        cql.execute(f"ALTER TABLE {table} WITH cdc = {{'enabled': true}}")
        assert id in get_definition(cdc_log_table)

def test_create_if_not_exists_with_cdc(scylla_only, cql, test_keyspace):
    table_name = f"{test_keyspace}.{unique_name()}"
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} (p int PRIMARY KEY) WITH cdc = {{ 'enabled': true }}"
    cql.execute(create_table_query)
    try:
        # Creating again should be a no-op and not fail.
        cql.execute(create_table_query)
    finally:
        cql.execute(f"DROP TABLE IF EXISTS {table_name}")

# CDC cannot accept a write with a timestamp far in the past (#24430). It
# should refuse it with a clear InvalidRequest error, which
# explains the problem. With vnodes, it did. But with tablets, the write
# failed with a generic server error, saying only that no CDC streams were
# found for the timestamp. Reproduces SCYLLADB-5228.
# We check different kinds of writes: INSERT, UPDATE, DELETE and BATCH.
@pytest.mark.parametrize("test_keyspace", ["tablets", "vnodes"], indirect=True)
def test_cdc_write_with_old_timestamp(scylla_only, cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, v int", "WITH cdc = {'enabled': true}") as table:
        for stmt in [f"INSERT INTO {table} (p, v) VALUES (1, 1) USING TIMESTAMP 1",
                     f"UPDATE {table} USING TIMESTAMP 1 SET v = 1 WHERE p = 1",
                     f"DELETE FROM {table} USING TIMESTAMP 1 WHERE p = 1",
                     f"BEGIN BATCH USING TIMESTAMP 1 INSERT INTO {table} (p, v) VALUES (1, 1); APPLY BATCH"]:
            # With tablets, the error also says which timestamps are accepted.
            match = 'from the past.*The oldest timestamp CDC accepts' if keyspace_has_tablets(cql, test_keyspace) else 'from the past'
            with pytest.raises(InvalidRequest, match=match):
                cql.execute(stmt)


# Test that the CDC log rows - delta rows as well as preimage and postimage
# rows - expire after the table's CDC 'ttl' option, independently of the TTL
# (or lack of one) of the base-table write, and that the base table only
# follows its own TTL. Both INSERT and UPDATE, with a regular and a
# non-frozen collection column, are checked.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("test_keyspace", ["vnodes"], indirect=True)
def test_cdc_log_ttl_expiry(scylla_only, cql, test_keyspace):
    cdc_ttl = 6
    schema = 'p int, c int, t text, l list<text>, primary key (p, c)'
    extra = f" with cdc = {{'enabled': true, 'preimage': true, 'postimage': true, 'ttl': {cdc_ttl}}}"
    with new_test_table(cql, test_keyspace, schema, extra) as table:
        log = f'{table}_scylla_cdc_log'
        # One partition per (operation, base TTL). Base TTL 1 expires before
        # the log, 1000 after it. Each row is written twice, so that the second
        # write has a preimage.
        base_ttls = {}
        for op in ['insert', 'update']:
            for base_ttl in [None, 1, 1000]:
                p = len(base_ttls)
                base_ttls[p] = base_ttl
                using = f' USING TTL {base_ttl}' if base_ttl else ''
                for i in range(2):
                    if op == 'insert':
                        cql.execute(f"INSERT INTO {table} (p, c, t, l) VALUES ({p}, 0, 't{i}', ['a{i}', 'b{i}']){using}")
                    else:
                        cql.execute(f"UPDATE {table}{using} SET t = 't{i}', l = ['a{i}', 'b{i}'] WHERE p = {p} AND c = 0")

        # Every log row carrying t - preimage (0), delta (1 = update,
        # 2 = insert) and postimage (9) - has the CDC TTL, not the base one.
        # The delta rows record the base TTL in cdc$ttl.
        log_rows = list(cql.execute(f'SELECT p, "cdc$operation", "cdc$ttl", ttl(t) FROM {log}'))
        for p, base_ttl in base_ttls.items():
            rows = [r for r in log_rows if r[0] == p and r[3] is not None]
            delta_op = 2 if p < 3 else 1
            assert sorted(r[1] for r in rows) == [0, delta_op, delta_op, 9, 9]
            for r in rows:
                assert 0 < r[3] <= cdc_ttl
                assert r[2] == (base_ttl if r[1] == delta_op else None)

        def wait_until(cond):
            deadline = time.time() + 60
            while not cond():
                assert time.time() < deadline, 'Timed out waiting for TTL expiration'
                time.sleep(0.1)

        def base_partitions():
            return {r[0] for r in cql.execute(f'SELECT p FROM {table}')}

        def log_partitions():
            return {r[0] for r in cql.execute(f'SELECT p FROM {log}')}

        # The base rows with TTL 1 expire, but their log rows stay.
        short = {p for p, ttl in base_ttls.items() if ttl == 1}
        wait_until(lambda: not (short & base_partitions()))
        assert log_partitions() == set(base_ttls)

        # Then the whole log expires, while the remaining base rows stay intact.
        wait_until(lambda: not log_partitions())
        rest = set(base_ttls) - short
        assert sorted(cql.execute(f'SELECT p, t, l FROM {table}')) == [(p, 't1', ['a1', 'b1']) for p in sorted(rest)]
        for p in rest:
            ttl = cql.execute(f'SELECT ttl(t) FROM {table} WHERE p = {p} AND c = 0').one()[0]
            if base_ttls[p] is None:
                assert ttl is None
            else:
                assert base_ttls[p] - 100 < ttl <= base_ttls[p]

# CDC log operation codes, see cdc::operation.
PREIMAGE, UPDATE, INSERT, POSTIMAGE = 0, 1, 2, 9
# Expected CDC log of writes to a table whose regular columns are named
# cval<i> and static columns stval<i>, all of one type, and of the clustering
# key ck. The writes of one change of a partition - a statement, or the
# statements of a batch for that partition - are given together: their
# log rows are the preimages of all changed rows (the static row first,
# then the clustering rows in order), then the deltas, then the postimages.
# Rows are (cdc$operation, ck, values), values as in log_rows_after().
class CdcLogModel:
    def __init__(self, preimage, postimage, static_columns, regular_columns, non_frozen):
        self.preimage = preimage
        self.postimage = postimage
        self.columns = {True: static_columns, False: regular_columns}
        self.non_frozen = non_frozen
        self.rows = {}  # (pk, ck), ck None for the static row -> {column: value}
        self.markers = set()  # (pk, ck) of rows with a live row marker

    # writes: list of (pk, ck, {column: value or None}, is_insert), all with
    # one timestamp. Returns {pk: expected log rows}.
    def change(self, writes):
        parts = {}  # (pk, ck) -> (values, is_insert)
        for pk, ck, values, insert in writes:
            static = {c: v for c, v in values.items() if c.startswith('st')}
            regular = {c: v for c, v in values.items() if not c.startswith('st')}
            for key, vals, ins in [((pk, None), static, False), ((pk, ck), regular, insert)]:
                if vals or (ins and key[1] is not None):
                    old_vals, old_ins = parts.get(key, ({}, False))
                    parts[key] = (old_vals | vals, old_ins or ins)
        ret = {}
        for pk in sorted({pk for pk, _ in parts}):
            keys = sorted((k for k in parts if k[0] == pk), key=lambda k: (k[1] is not None, k[1]))
            pres, deltas, posts = [], [], []
            for key in keys:
                values, insert = parts[key]
                static = key[1] is None
                old = self.rows.get(key, {})
                if self.preimage != 'false' and (old or key in self.markers):
                    image_columns = self.columns[static] if self.preimage == 'full' else values
                    pres.append((PREIMAGE, key[1], {c: old[c] for c in image_columns if c in old} |
                                 {f'deleted_{c}': True for c in image_columns if c not in old}))
                deltas.append((INSERT if insert else UPDATE, key[1], {c: v for c, v in values.items() if v is not None} |
                               {f'deleted_{c}': True for c, v in values.items() if v is None or c in self.non_frozen}))
                new = {c: v for c, v in (old | values).items() if v is not None}
                self.rows[key] = new
                if insert:
                    self.markers.add(key)
                posts.append((POSTIMAGE, key[1], new))
            ret[pk] = pres + deltas + (posts if self.postimage == 'true' else [])
        return ret


# Reads the CDC log of `table` at consistency level ALL, like the CDC
# dtests: the whole log, or - given the "cdc$time" of the changes read
# last - only the rows newer than all of them. Returns the rows in log
# order as {pk: [(cdc$operation, ck, values)]}, where `values` maps the
# names of the cval/stval columns to their non-null values in the log row
# ("cdc$deleted_x" flags as "deleted_x"; lists in `list_columns` - logged
# as maps from timeuuid - as lists of their values), and the "cdc$time" of
# each change read.
def log_rows_after(cql, table, last_times, list_columns=()):
    where = ' AND '.join(f'"cdc$time" > {t}' for t in last_times)
    query = f'SELECT * FROM {table}_scylla_cdc_log' + (f' WHERE {where} ALLOW FILTERING' if where else '')
    ret, times = {}, set()
    for r in cql.execute(SimpleStatement(query, consistency_level=ConsistencyLevel.ALL)):
        times.add(r.cdc_time)
        values = {}
        for name, value in r._asdict().items():
            if value is None or not ('val' in name):
                continue
            column = name.removeprefix('cdc_deleted_')
            if column in list_columns and not name.startswith('cdc_deleted_'):
                value = list(value.values())
            elif isinstance(value, collections.abc.Mapping):
                value = dict(value)
            elif isinstance(value, (collections.abc.Set, SortedSet)):
                value = set(value)
            values[name.removeprefix('cdc_')] = value
        ret.setdefault(r.pk, []).append((r.cdc_operation, r.ck, values))
    return ret, sorted(times, key=lambda t: t.time)


# Values of the CDC static row and batch dtests' data generators: value i of
# type typ, where seed() is the dtests' DataGenerator.seed - a property that
# takes the next number of a counter each time it is read, so that the two
# halves of a collection value get consecutive seeds.
def typed_value(typ, i, seed):
    typ = typ.replace(' ', '')
    if typ == 'int':
        return i + seed()
    if typ == 'bigint':
        return 32000 + i + seed()
    if typ in ('text', 'varchar'):
        return f'{typ}{i + seed()}'
    if typ == 'frozen<set<text>>':
        return {f'frozenset{i + seed()}', f'frozenset{i + seed() + 10}'}
    if typ in ('frozen<set<int>>', 'set<int>'):
        return {i + seed(), i + seed() + 10}
    if typ in ('frozen<list<int>>', 'list<int>'):
        return [i + seed(), i + seed() + 10]
    if typ == 'map<int,int>':
        return {i + seed(): i + seed() + 10}
    if typ == 'map<text,blob>':
        return {f'file{i + seed()}': b'\x00' * (i + seed())}
    raise ValueError(typ)


# The dtests' DataGenerator: a new data set of n values, value i being
# typed_value(typ, i) with the shared seed counter. A data set is the same
# for all the rows written.
class DataGenerator:
    def __init__(self, typ):
        self.typ = typ
        self.counter = 0

    def seed(self):
        self.counter += 1
        return self.counter

    def generate(self, n):
        return [typed_value(self.typ, i, self.seed) for i in range(n)]


# The dtests' test id of a type name.
def mkident(s):
    s = s.replace(' ', '')
    for c in '<>,':
        s = s.replace(c, '_')
    return s.rstrip('_')


# Creates the table of the CDC static row and batch dtests, with n_static
# static and n_regular regular columns of type typ and the cdc option
# `cdc_options` (as the dtest formats it), and returns it with the model of
# its log, for the given normalized preimage ('false', 'true' or 'full') and
# postimage ('false' or 'true').
def new_cdc_table_of_type(stack, cql, keyspace, typ, n_static, n_regular, cdc_options, preimage, postimage):
    static = [f'stval{i}' for i in range(n_static)]
    regular = [f'cval{i}' for i in range(n_regular)]
    schema = ', '.join(['pk bigint', 'ck bigint'] + [f'{c} {typ} STATIC' for c in static] + [f'{c} {typ}' for c in regular]) + ', PRIMARY KEY (pk, ck)'
    table = stack.enter_context(new_test_table(cql, keyspace, schema, f" WITH cdc={{'enabled': true{cdc_options}}}"))
    non_frozen = set(static + regular) if typ.split('<')[0] in ('map', 'set', 'list') else set()
    lists = set(static + regular) if typ.startswith('list') else set()
    return table, CdcLogModel(preimage, postimage, static, regular, non_frozen), lists


# Runs `writes` (as for CdcLogModel.change(), one statement each, values
# formatted into the statements by the driver) and checks the log rows
# written since the changes whose times are `last_times`. Returns the times
# of the new changes.
def check_cdc_statements(cql, table, model, writes, last_times, list_columns):
    expected = {}
    for pk, ck, values, insert in writes:
        cols = list(values)
        if insert:
            keys = ['pk'] + (['ck'] if ck is not None else [])
            cql.execute(f"INSERT INTO {table} ({', '.join(keys + cols)}) VALUES ({', '.join(['%s'] * (len(keys) + len(cols)))})",
                        [pk] + ([ck] if ck is not None else []) + [values[c] for c in cols])
        else:
            where = 'pk = %s' + (' AND ck = %s' if ck is not None else '')
            cql.execute(f"UPDATE {table} SET {', '.join(f'{c} = %s' for c in cols)} WHERE {where}",
                        [values[c] for c in cols] + [pk] + ([ck] if ck is not None else []))
        for pk, rows in model.change([(pk, ck, values, insert)]).items():
            expected.setdefault(pk, []).extend(rows)
    actual, times = log_rows_after(cql, table, last_times, list_columns)
    assert actual == expected
    return times


# The column types of cdc_static_row_test.py, in the dtest's spelling.
static_row_types = [pytest.param(t, id=mkident(t)) for t in
                    ['int', 'bigint', 'text', 'map<int,int>', 'varchar', 'frozen<set<int>>', 'frozen<set<text>>',
                     'frozen<list<int>>', 'set<int>', 'list<int>']]


# The cdc option of the static row dtests' tables: the preimage and
# postimage values are formatted in quotes, so True becomes 'True'.
def static_row_cdc_options(preimage_enable, postimage_enable):
    ret = ''
    if preimage_enable:
        ret += f", 'preimage': '{preimage_enable}'"
    if postimage_enable:
        ret += f", 'postimage': '{postimage_enable}'"
    return ret

def normalized_images(preimage_enable, postimage_enable):
    preimage = 'full' if preimage_enable == 'full' else ('true' if preimage_enable else 'false')
    return preimage, 'true' if postimage_enable else 'false'
# cdc_static_row_test.py's check_single_row_static(): the dtest's INSERT,
# UPDATE, DELETE and UPDATE again of the static columns (only) of each row
# of the data set (num_of_partitions partitions of one row), one statement
# per row. Each change is logged as an UPDATE of the static row (null
# clustering key), with preimage and postimage rows as enabled; the static
# row is empty after the DELETE, so the last UPDATE has no preimage. The log
# is read as the dtest did: whole, then the rows newer than the changes read
# last, at consistency level ALL.
def check_single_row_static(cql, keyspace, typ, num_of_partitions=1, num_of_rows=1, num_of_static=2,
                            preimage_enable=False, postimage_enable=False):
    with contextlib.ExitStack() as stack:
        table, model, lists = new_cdc_table_of_type(stack, cql, keyspace, typ, num_of_static, 0,
                                                    static_row_cdc_options(preimage_enable, postimage_enable),
                                                    *normalized_images(preimage_enable, postimage_enable))
        data = DataGenerator(typ)
        rows = [(pk, ck) for pk in range(num_of_partitions) for ck in range(num_of_rows)]

        def statics(values):
            return {f'stval{k}': None if values is None else values[k] for k in range(num_of_static)}
        ins = data.generate(num_of_static)
        times = check_cdc_statements(cql, table, model, [(pk, None, statics(ins), True) for pk, _ in rows], [], lists)
        upd = data.generate(num_of_static)
        times = check_cdc_statements(cql, table, model, [(pk, None, statics(upd), False) for pk, _ in rows], times, lists)
        expected = {}
        for pk, _ in rows:
            cql.execute(f"DELETE {', '.join(statics(None))} FROM {table} WHERE pk = %s", [pk])
            for p, log in model.change([(pk, None, statics(None), False)]).items():
                expected.setdefault(p, []).extend(log)
        actual, times = log_rows_after(cql, table, times, lists)
        assert actual == expected
        upd = data.generate(num_of_static)
        check_cdc_statements(cql, table, model, [(pk, None, statics(upd), False) for pk, _ in rows], times, lists)

# cdc_static_row_test.py's check_static_common_field(): INSERT, UPDATE,
# UPDATE to null and UPDATE again of all static and regular columns of each
# row of the data set (num_of_partitions partitions of num_of_rows rows),
# one statement per row. Each statement's change is logged as an UPDATE of
# the static row (null clustering key) and an INSERT or UPDATE of the
# clustering row, with their preimage and postimage rows as enabled.
def check_static_common_field(cql, keyspace, typ, num_of_partitions, num_of_rows, num_of_columns, num_of_static=2,
                              preimage_enable=False, postimage_enable=False):
    with contextlib.ExitStack() as stack:
        table, model, lists = new_cdc_table_of_type(stack, cql, keyspace, typ, num_of_static, num_of_columns,
                                                    static_row_cdc_options(preimage_enable, postimage_enable),
                                                    *normalized_images(preimage_enable, postimage_enable))
        data = DataGenerator(typ)
        rows = [(pk, ck) for pk in range(num_of_partitions) for ck in range(num_of_rows)]

        def values(data_set):
            return {f'{c}{k}': None if data_set is None else data_set[k] for c, n in (('cval', num_of_columns), ('stval', num_of_static)) for k in range(n)}
        n = num_of_columns + num_of_static
        ins = data.generate(n)
        times = check_cdc_statements(cql, table, model, [(pk, ck, values(ins), True) for pk, ck in rows], [], lists)
        upd = data.generate(n)
        times = check_cdc_statements(cql, table, model, [(pk, ck, values(upd), False) for pk, ck in rows], times, lists)
        times = check_cdc_statements(cql, table, model, [(pk, ck, values(None), False) for pk, ck in rows], times, lists)
        upd = data.generate(n)
        check_cdc_statements(cql, table, model, [(pk, ck, values(upd), False) for pk, ck in rows], times, lists)


STATIC_ROW_GATE = pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")


# Ported from scylla-dtest cdc_static_row_test.py::TestCDCStaticRow::test_single_static_row.
# Writes of the two static columns of one partition, without images.
@pytest.mark.tier3
@STATIC_ROW_GATE
@pytest.mark.parametrize('typ', static_row_types)
def test_single_static_row(scylla_only, cql, test_keyspace_vnodes, typ):
    check_single_row_static(cql, test_keyspace_vnodes, typ)
# Ported from scylla-dtest cdc_static_row_test.py::TestCDCStaticRow::test_single_static_row_preimage.
@pytest.mark.tier3
@STATIC_ROW_GATE
@pytest.mark.parametrize('typ', static_row_types)
def test_single_static_row_preimage(scylla_only, cql, test_keyspace_vnodes, typ):
    check_single_row_static(cql, test_keyspace_vnodes, typ, preimage_enable=True)

# Ported from scylla-dtest cdc_static_row_test.py::TestCDCStaticRow::test_single_static_row_postimage.
@pytest.mark.tier3
@STATIC_ROW_GATE
@pytest.mark.parametrize('typ', static_row_types)
def test_single_static_row_postimage(scylla_only, cql, test_keyspace_vnodes, typ):
    check_single_row_static(cql, test_keyspace_vnodes, typ, postimage_enable=True)
# Ported from scylla-dtest cdc_static_row_test.py::TestCDCStaticRow::test_single_static_row_preimage_postimage.
@pytest.mark.tier3
@STATIC_ROW_GATE
@pytest.mark.parametrize('typ', static_row_types)
def test_single_static_row_preimage_postimage(scylla_only, cql, test_keyspace_vnodes, typ):
    check_single_row_static(cql, test_keyspace_vnodes, typ, preimage_enable=True, postimage_enable=True)
# Ported from scylla-dtest cdc_static_row_test.py::TestCDCStaticRow::test_several_partitions_with_single_static_row_preimage_postimage.
# The same with five partitions.
@pytest.mark.tier3
@STATIC_ROW_GATE
@pytest.mark.parametrize('typ', static_row_types)
def test_several_partitions_with_single_static_row_preimage_postimage(scylla_only, cql, test_keyspace_vnodes, typ):
    check_single_row_static(cql, test_keyspace_vnodes, typ, num_of_partitions=5, preimage_enable=True, postimage_enable=True)
# Ported from scylla-dtest cdc_static_row_test.py::TestCDCStaticRow::test_several_partitions_with_single_static_row.
# Ten partitions, without images.
@pytest.mark.tier3
@STATIC_ROW_GATE
@pytest.mark.parametrize('typ', static_row_types)
def test_several_partitions_with_single_static_row(scylla_only, cql, test_keyspace_vnodes, typ):
    check_single_row_static(cql, test_keyspace_vnodes, typ, num_of_partitions=10)
# Ported from scylla-dtest cdc_static_row_test.py::TestCDCStaticRow::test_several_partitions_with_single_static_row_preimage.
@pytest.mark.tier3
@STATIC_ROW_GATE
@pytest.mark.parametrize('typ', static_row_types)
def test_several_partitions_with_single_static_row_preimage(scylla_only, cql, test_keyspace_vnodes, typ):
    check_single_row_static(cql, test_keyspace_vnodes, typ, num_of_partitions=5, preimage_enable=True)
# Ported from scylla-dtest cdc_static_row_test.py::TestCDCStaticRow::test_several_partitions_with_single_static_row_postimage.
@pytest.mark.tier3
@STATIC_ROW_GATE
@pytest.mark.parametrize('typ', static_row_types)
def test_several_partitions_with_single_static_row_postimage(scylla_only, cql, test_keyspace_vnodes, typ):
    check_single_row_static(cql, test_keyspace_vnodes, typ, num_of_partitions=3, postimage_enable=True)
# Ported from scylla-dtest cdc_static_row_test.py::TestCDCStaticRow::test_several_partitions_with_single_static_row_full_postimage.
# Three partitions, with full preimage and postimage.
@pytest.mark.tier3
@STATIC_ROW_GATE
@pytest.mark.parametrize('typ', static_row_types)
def test_several_partitions_with_single_static_row_full_postimage(scylla_only, cql, test_keyspace_vnodes, typ):
    check_single_row_static(cql, test_keyspace_vnodes, typ, num_of_partitions=3, preimage_enable='full', postimage_enable=True)
# Ported from scylla-dtest cdc_static_row_test.py::TestCDCStaticRow::test_operations_with_static_row_and_regular_columns_preimage_postimage.
# Writes of two static and two regular columns of three rows in each of
# three partitions, with preimage and postimage.
@pytest.mark.tier3
@STATIC_ROW_GATE
@pytest.mark.parametrize('typ', static_row_types)
def test_operations_with_static_row_and_regular_columns_preimage_postimage(scylla_only, cql, test_keyspace_vnodes, typ):
    check_static_common_field(cql, test_keyspace_vnodes, typ, 3, 3, 2, preimage_enable=True, postimage_enable=True)
# Ported from scylla-dtest cdc_static_row_test.py::TestCDCStaticRow::test_operations_with_static_row_and_regular_columns_preimage.
@pytest.mark.tier3
@STATIC_ROW_GATE
@pytest.mark.parametrize('typ', static_row_types)
def test_operations_with_static_row_and_regular_columns_preimage(scylla_only, cql, test_keyspace_vnodes, typ):
    check_static_common_field(cql, test_keyspace_vnodes, typ, 3, 3, 2, preimage_enable=True)
# Ported from scylla-dtest cdc_static_row_test.py::TestCDCStaticRow::test_operations_with_static_row_and_regular_columns_postimage.
@pytest.mark.tier3
@STATIC_ROW_GATE
@pytest.mark.parametrize('typ', static_row_types)
def test_operations_with_static_row_and_regular_columns_postimage(scylla_only, cql, test_keyspace_vnodes, typ):
    check_static_common_field(cql, test_keyspace_vnodes, typ, 3, 3, 2, postimage_enable=True)
# Ported from scylla-dtest cdc_static_row_test.py::TestCDCStaticRow::test_operations_with_static_row_and_regular_columns.
@pytest.mark.tier3
@STATIC_ROW_GATE
@pytest.mark.parametrize('typ', static_row_types)
def test_operations_with_static_row_and_regular_columns(scylla_only, cql, test_keyspace_vnodes, typ):
    check_static_common_field(cql, test_keyspace_vnodes, typ, 3, 3, 2)
# Ported from scylla-dtest cdc_static_row_test.py::TestCDCStaticRow::test_operations_with_large_number_of_static_and_regular_columns_postimage_preimage.
# Ten static and ten regular columns, with preimage and postimage.
@pytest.mark.tier3
@STATIC_ROW_GATE
@pytest.mark.parametrize('typ', static_row_types)
def test_operations_with_large_number_of_static_and_regular_columns_postimage_preimage(scylla_only, cql, test_keyspace_vnodes, typ):
    check_static_common_field(cql, test_keyspace_vnodes, typ, 3, 3, 10, num_of_static=10, preimage_enable=True, postimage_enable=True)
# Ported from scylla-dtest cdc_static_row_test.py::TestCDCStaticRow::test_operations_with_large_number_of_static_and_regular_columns.
@pytest.mark.tier3
@STATIC_ROW_GATE
@pytest.mark.parametrize('typ', static_row_types)
def test_operations_with_large_number_of_static_and_regular_columns(scylla_only, cql, test_keyspace_vnodes, typ):
    check_static_common_field(cql, test_keyspace_vnodes, typ, 3, 3, 10, num_of_static=10)
