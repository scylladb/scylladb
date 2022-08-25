# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

from cassandra.cluster import ConsistencyLevel
from cassandra.query import SimpleStatement

from util import new_test_table
from nodetool import flush

def test_cdc_log_entries_use_cdc_streams(scylla_only, cql, test_keyspace):
    '''Test that the stream IDs chosen for CDC log entries come from the CDC generation
    whose streams are listed in the streams description table. Since this test is executed
    on a single-node cluster, there is only one generation.'''

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
