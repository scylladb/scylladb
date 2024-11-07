# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

from cassandra.cluster import ConsistencyLevel
from cassandra.query import SimpleStatement
from cassandra.protocol import InvalidRequest

from util import new_test_table, unique_name
from nodetool import flush
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
