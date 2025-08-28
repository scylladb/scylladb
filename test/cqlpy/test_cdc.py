# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

from cassandra.cluster import ConsistencyLevel
from cassandra.query import SimpleStatement
from cassandra.protocol import InvalidRequest

from .util import new_test_table, unique_name
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

@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #16317")]), "vnodes"],
                         indirect=True)
def test_cdc_log_entries_use_cdc_streams(scylla_only, cql, test_keyspace):
    '''Test that the stream IDs chosen for CDC log entries come from the CDC generation
    whose streams are listed in the streams description table. Since this test is executed
    on a single-node cluster, there is only one generation.'''

    wait_for_first_cdc_generation(cql, 60)

    schema = "pk int primary key"
    extra = " with cdc = {'enabled': true}"
    with new_test_table(cql, test_keyspace, schema, extra) as table:
        stmt = cql.prepare(f"insert into {table} (pk) values (?) using timeout 5m")
        for i in range(100):
            cql.execute(stmt, [i])

        log_stream_ids = set(r[0] for r in cql.execute(f'select "cdc$stream_id" from {table}_scylla_cdc_log'))

    # There should be exactly one generation, so we just select the streams
    streams_desc = cql.execute(SimpleStatement(
            'select streams from system_distributed.cdc_streams_descriptions_v2',
            consistency_level=ConsistencyLevel.ONE))
    stream_ids = set()
    for entry in streams_desc:
        stream_ids.update(entry.streams)

    assert(log_stream_ids.issubset(stream_ids))


# Test for #10473 - reading logs (from sstable) after dropping
# column in base.
@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #16317")]), "vnodes"],
                         indirect=True)
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
@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #16317")]), "vnodes"],
                         indirect=True)
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
@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #16317")]), "vnodes"],
                         indirect=True)
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

@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #16317")]), "vnodes"],
                         indirect=True)
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

@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #16317")]), "vnodes"],
                         indirect=True)
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
@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #16317")]), "vnodes"],
                         indirect=True)
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
@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #16317")]), "vnodes"],
                         indirect=True)
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
@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #16317")]), "vnodes"],
                         indirect=True)
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
@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #16317")]), "vnodes"],
                         indirect=True)
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
@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #16317")]), "vnodes"],
                         indirect=True)
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
@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #16317")]), "vnodes"],
                         indirect=True)
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
