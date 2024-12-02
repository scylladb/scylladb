# -*- coding: utf-8 -*-
# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

# Tests concerning the SELECT * FROM MUTATION_FRAGMENTS($table) statement, which allows dumping
# the underlying mutation fragment data stream, for a table.


import cassandra.protocol
import cassandra.query
import glob
import json
import os
import nodetool
import pytest
import requests
import subprocess
import util


@pytest.fixture(scope="module")
def test_table(cql, test_keyspace):
    """ Prepares a table for the mutation dump tests to work with."""
    with util.new_test_table(cql, test_keyspace, 'pk1 int, pk2 int, ck1 int, ck2 int, v text, s text static, PRIMARY KEY ((pk1, pk2), ck1, ck2)',
                             "WITH compaction = {'class':'NullCompactionStrategy'}") as table:
        yield table


@pytest.mark.parametrize("test_keyspace", ["tablets", "vnodes"], indirect=True)
def test_smoke(cql, test_table, scylla_only):
    """ Simple smoke tests, this should fail first if something is very wrong. """
    partitions = {}
    for i in range(0, 1):
        pk1 = util.unique_key_int()
        pk2 = 0
        cql.execute(f"DELETE FROM {test_table} WHERE pk1 = {pk1} AND pk2 = {pk2}")
        cql.execute(f"UPDATE {test_table} SET s = 'static val' WHERE pk1 = {pk1} AND pk2 = {pk2}")
        cql.execute(f"DELETE FROM {test_table} WHERE pk1 = {pk1} AND pk2 = {pk2} AND ck1 = 0 AND ck2 > 0 AND ck2 < 3")
        cql.execute(f"INSERT INTO {test_table} (pk1, pk2, ck1, ck2, v) VALUES ({pk1}, {pk2}, 0, 1, 'regular val')")
        cql.execute(f"INSERT INTO {test_table} (pk1, pk2, ck1, ck2, v) VALUES ({pk1}, {pk2}, 0, 2, 'regular val')")
        partitions[(pk1, pk2)] = [
                (pk1, pk2, 'sstable:', 0, None, None, None, 'partition start'),
                (pk1, pk2, 'sstable:', 1, None, None, None, 'static row'),
                (pk1, pk2, 'sstable:', 2, 0   , 0   , 1   , 'range tombstone change'),
                (pk1, pk2, 'sstable:', 2, 0   , 1   , 0   , 'clustering row'),
                (pk1, pk2, 'sstable:', 2, 0   , 2   , 0   , 'clustering row'),
                (pk1, pk2, 'sstable:', 2, 0   , 3   , -1  , 'range tombstone change'),
                (pk1, pk2, 'sstable:', 3, None, None, None, 'partition end'),
        ]

    nodetool.flush(cql, f"{test_table}")

    col_names = ('pk1', 'pk2', 'mutation_source', 'partition_region', 'ck1', 'ck2', 'position_weight', 'mutation_fragment_kind')

    def check_partition_rows(rows, expected_rows):
        assert len(rows) == len(expected_rows)

        for expected_col_values, row in zip(expected_rows, rows):
            for col_name, col_value in zip(col_names, expected_col_values):
                assert hasattr(row, col_name)
                if col_name == 'mutation_source':
                    assert getattr(row, col_name).startswith(col_value)
                else:
                    assert getattr(row, col_name) == col_value

    # Point queries
    for (pk1, pk2), expected_rows in partitions.items():
        rows = list(cql.execute(f"SELECT * FROM MUTATION_FRAGMENTS({test_table}) WHERE pk1 = {pk1} AND pk2 = {pk2} AND mutation_source > 'sstable:'"))
        check_partition_rows(rows, expected_rows)

    # Range scan
    all_rows = list(cql.execute(f"SELECT * FROM MUTATION_FRAGMENTS({test_table}) WHERE mutation_source > 'sstable:' ALLOW FILTERING"))
    for (pk1, pk2), expected_rows in partitions.items():
        rows = [r for r in all_rows if r.pk1 == pk1 and r.pk2 == pk2]
        check_partition_rows(rows, expected_rows)


@pytest.mark.parametrize("test_keyspace", ["tablets", "vnodes"], indirect=True)
def test_order_by(cql, test_table, scylla_only):
    """ ORDER BY is not allowed """
    pk1 = util.unique_key_int()
    pk2 = 0
    cql.execute(f"INSERT INTO {test_table} (pk1, pk2, ck1, ck2, v) VALUES ({pk1}, {pk2}, 0, 0, 'vv')")

    with pytest.raises(cassandra.protocol.InvalidRequest, match="ORDER BY is not supported in SELECT FROM MUTATION_FRAGMENTS\\(\\) statements"):
        cql.execute(f"SELECT * FROM MUTATION_FRAGMENTS({test_table}) WHERE pk1 = {pk1} AND pk2 = {pk2} ORDER BY mutation_source DESC")


@pytest.mark.parametrize("test_keyspace", ["tablets", "vnodes"], indirect=True)
def test_mutation_source(cql, test_table, scylla_only):
    """ Manipulate where the data is located in the node, and check that the corred mutation source is reported. """
    pk1 = util.unique_key_int()
    pk2 = util.unique_key_int()

    def expect_sources(*expected_sources):
        for src in ('memtable', 'row-cache', 'sstable'):
            rows = list(cql.execute(f"SELECT * FROM MUTATION_FRAGMENTS({test_table}) WHERE pk1 = {pk1} AND pk2 = {pk2} AND mutation_source >= '{src}' AND mutation_source < '{src};'"))
            if src in expected_sources:
                assert len(rows) == 3 # partition-start, clustering-row, partition-end
            else:
                assert len(rows) == 0

    # Start with an empty memtable.
    nodetool.flush(cql, f"{test_table}")

    cql.execute(f"INSERT INTO {test_table} (pk1, pk2, ck1, ck2, v) VALUES ({pk1}, {pk2}, 0, 0, 'vv')")
    expect_sources('memtable')

    nodetool.flush(cql, f"{test_table}")
    expect_sources('row-cache', 'sstable')

    requests.post(f'{nodetool.rest_api_url(cql)}/system/drop_sstable_caches')
    expect_sources('sstable')

    assert list(cql.execute(f"SELECT v FROM {test_table} WHERE pk1={pk1} AND pk2={pk2} BYPASS CACHE"))[0].v == 'vv'
    expect_sources('sstable')

    assert list(cql.execute(f"SELECT v FROM {test_table} WHERE pk1={pk1} AND pk2={pk2}"))[0].v == 'vv'
    expect_sources('row-cache', 'sstable')

    cql.execute(f"INSERT INTO {test_table} (pk1, pk2, ck1, ck2, v) VALUES ({pk1}, {pk2}, 0, 0, 'vv')")
    expect_sources('memtable', 'row-cache', 'sstable')


@pytest.mark.parametrize("test_keyspace", ["tablets", "vnodes"], indirect=True)
def test_mutation_dump_range_tombstone_changes(cql, test_table, scylla_only):
    """
    Range tombstones can share the same position.
    This doesn't seem to happen in practice, but this test still tries to produce
    such range tombstone and checks that they are handled correctly.
    """
    pk1 = util.unique_key_int()
    pk2 = util.unique_key_int()
    cql.execute(f"INSERT INTO {test_table} (pk1, pk2, ck1, ck2, v) VALUES ({pk1}, {pk2}, 0, 0, 'vv')")

    rts = 4

    for ck in range(44, 44 - rts, -1):
        cql.execute(f"DELETE FROM {test_table} WHERE pk1={pk1} AND pk2={pk2} AND ck1=0 AND ck2>30 AND ck2<{ck}")
        nodetool.flush(cql, f"{test_table}")

    res = list(cql.execute(f"SELECT * FROM MUTATION_FRAGMENTS({test_table}) WHERE pk1 = {pk1} AND pk2 = {pk2} AND mutation_source > 'sstable' AND mutation_source < 'sstable;' AND partition_region = 2 ALLOW FILTERING"))
    # row + 2 * range-tombstone-change
    assert len(res) == 2 * rts + 1


@pytest.mark.parametrize("test_keyspace", ["tablets", "vnodes"], indirect=True)
def test_count(cql, test_table, scylla_only):
    """ Test aggregation (COUNT). """
    pk1 = util.unique_key_int()
    pk2 = util.unique_key_int()

    cql.execute(f"UPDATE {test_table} SET s = 'static val' WHERE pk1 = {pk1} AND pk2 = {pk2}")

    for ck in range(0, 10):
        cql.execute(f"INSERT INTO {test_table} (pk1, pk2, ck1, ck2, v) VALUES ({pk1}, {pk2}, 0, {ck}, 'vv')")

    for ck in range(10, 20):
        cql.execute(f"DELETE FROM {test_table} WHERE pk1 = {pk1} AND pk2 = {pk2} AND ck1 = 1 AND ck2 > {ck} AND ck2 < 100")

    def check_count(kind, expected_count):
        res = list(cql.execute(f"SELECT COUNT(*) FROM MUTATION_FRAGMENTS({test_table}) WHERE pk1 = {pk1} AND pk2 = {pk2} AND mutation_fragment_kind = '{kind}' ALLOW FILTERING"))
        assert res[0].count == expected_count

    check_count('partition start', 1)
    check_count('static row', 1)
    check_count('clustering row', 10)
    check_count('range tombstone change', 11)
    check_count('partition end', 1)


@pytest.mark.parametrize("test_keyspace", ["tablets", "vnodes"], indirect=True)
def test_many_partition_scan(cql, test_keyspace, scylla_only):
    """
    Full scans work like secondary-index based scans. First, a query is
    issued to obtain partition-keys, then each partition is read individually.
    The former uses paging, reading 1000 partition keys in a page. Stress this
    logic a bit.
    """
    with util.new_test_table(cql, test_keyspace, 'pk int PRIMARY KEY, v text') as test_table:
        insert_stmt = cql.prepare(f"INSERT INTO {test_table} (pk, v) VALUES (?, ?)")
        delete_stmt = cql.prepare(f"DELETE FROM {test_table} WHERE pk = ?")
        partitions = []
        # the scan algorithm reads 1000 partition / page, so have enough partitions for at least 2 pages
        partition_count = 1312
        for pk in range(0, partition_count):
            cql.execute(insert_stmt, (pk, 'v'))
            if pk % 3 == 0:
                cql.execute(delete_stmt, (pk,))
                partitions.append((pk, False))
            else:
                partitions.append((pk, True))

        nodetool.flush(cql, f"{test_table}")
        requests.post(f'{nodetool.rest_api_url(cql)}/system/drop_sstable_caches')

        # the real test here is that this scan completes without problems
        # since this scan is very slow, we create an extra patient cql connection,
        # with an abundant 10 minutes timeout
        ep = cql.hosts[0].endpoint
        with util.cql_session(ep.address, ep.port, False, 'cassandra', 'cassandra', 600) as patient_cql:
            res_all = list(patient_cql.execute(f"SELECT pk, mutation_source, mutation_fragment_kind, metadata FROM MUTATION_FRAGMENTS({test_table});"))

        actual_partitions = []
        for r in res_all:
            if r.mutation_fragment_kind == "partition start":
                actual_partitions.append((r.pk, len(json.loads(r.metadata)["tombstone"]) == 0))

        actual_partitions = sorted(actual_partitions)

        assert len(actual_partitions) == len(partitions)
        assert actual_partitions == partitions


@pytest.mark.parametrize("test_keyspace", ["tablets", "vnodes"], indirect=True)
def test_metadata_and_value(cql, test_keyspace, scylla_path, scylla_data_dir, scylla_only):
    """
    Test that metadata + value columns allow reconstructing a full sstable dump.
    Meaning that their json representation of metadata and value is the same.
    """
    with util.new_test_table(cql, test_keyspace, 'pk int, ck int, v1 text, v2 map<int, text>, v3 tuple<int, text, boolean>, s text static, PRIMARY KEY (pk, ck)') as test_table:
        insert_stmt = cql.prepare(f"INSERT INTO {test_table} (pk, ck, v1, v2, v3, s) VALUES (?, ?, ?, ?, ?, ?)")
        delete_row_stmt = cql.prepare(f"DELETE FROM {test_table} WHERE pk = ? AND ck = ?")
        delete_row_range_stmt = cql.prepare(f"DELETE FROM {test_table} WHERE pk = ? AND ck > ? AND CK < ?")
        delete_partition_stmt = cql.prepare(f"DELETE FROM {test_table} WHERE pk = ?")
        for pk in range(0, 2):
            for ck in range(0, 10):
                if ck % 4:
                    cql.execute(delete_row_stmt, (pk, ck))
                else:
                    cql.execute(insert_stmt, (pk, ck, 'v1_val', {0: '0_val', 1: '1_val'}, (4, 'tuple_val', ck % 2), 'static_val'))
            cql.execute(delete_row_range_stmt, (pk, 100, 200))
        cql.execute(delete_partition_stmt, (100,))

        nodetool.flush(cql, f"{test_table}")
        nodetool.flush_keyspace(cql, "system_schema")
        requests.post(f'{nodetool.rest_api_url(cql)}/system/drop_sstable_caches')

        table_name = test_table.split('.')[1]
        sstables = glob.glob(os.path.join(scylla_data_dir, test_keyspace, f"{table_name}-*", "*-Data.db"))

        with nodetool.no_autocompaction_context(cql, "system", "system_schema"):
            res = subprocess.check_output([scylla_path, "sstable", "dump-data", "--merge"] + sstables)

        reference_dump = json.loads(res)["sstables"]["anonymous"]

        for partition in reference_dump:
            del partition["key"]["token"]
            del partition["key"]["raw"]
            for ce in partition.get("clustering_elements", {}):
                del ce["key"]["raw"]

        def merged_value_into_metadata(metadata, value):
            value = json.loads(value)
            for col_name, col_value in metadata.items():
                if "cells" in col_value:
                    for i, cell_value in enumerate(col_value["cells"]):
                        cell_value["value"]["value"] = value[col_name][int(i)]["value"]
                else:
                    col_value["value"] = value[col_name]
            return metadata

        res = cql.execute(f"SELECT * FROM MUTATION_FRAGMENTS({test_table})")
        reconstructed_dump = []
        partition = {}
        for row in res:
            kind = row.mutation_fragment_kind
            if kind == "partition start":
                partition = {"key": {"value": str(row.pk)}}
                metadata = json.loads(row.metadata)
                tombstone = metadata["tombstone"]
                if tombstone:
                    partition["tombstone"] = tombstone
            elif kind == "static row":
                partition["static_row"] = json.loads(row.metadata)
                merged_value_into_metadata(partition["static_row"], row.value)
            elif kind == "clustering row":
                cr = {"type": "clustering-row", "key": {"value": str(row.ck)}}
                cr.update(json.loads(row.metadata))
                merged_value_into_metadata(cr["columns"], row.value)
                if "clustering_elements" in partition:
                    partition["clustering_elements"].append(cr)
                else:
                    partition["clustering_elements"] = [cr]
            elif kind == "range tombstone change":
                rtc = {"type": "range-tombstone-change", "key": {"value": str(row.ck)}, "weight": row.position_weight}
                rtc.update(json.loads(row.metadata))
                if "clustering_elements" in partition:
                    partition["clustering_elements"].append(rtc)
                else:
                    partition["clustering_elements"] = [rtc]
            else:
                assert kind == "partition end"
                reconstructed_dump.append(partition)

        reference_dump_json = json.dumps(reference_dump, indent=4, sort_keys=True)
        reconstructed_dump_json = json.dumps(reconstructed_dump, indent=4, sort_keys=True)

        assert reference_dump_json == reconstructed_dump_json


@pytest.mark.parametrize("test_keyspace", ["tablets", "vnodes"], indirect=True)
def test_paging(cql, test_table, scylla_only):
    """ Test that paging works properly. """
    pk1 = util.unique_key_int()
    pk2 = util.unique_key_int()

    insert_stmt = cql.prepare(f"INSERT INTO {test_table} (pk1, pk2, ck1, ck2, v) VALUES (?, ?, ?, ?, ?)")
    num_rows = 43
    expected_mutation_fragments = num_rows + 2
    page_size = 10
    for ck in range(0, num_rows):
        cql.execute(insert_stmt, (pk1, pk2, 0, ck, 'asdasd'))

    nodetool.flush(cql, f"{test_table}")

    read_stmt = cassandra.query.SimpleStatement(
            f"""SELECT * FROM mutation_fragments({test_table})
            WHERE
                pk1 = {pk1} AND
                pk2 = {pk2} AND
                mutation_source > 'sstable:'""",
            fetch_size=page_size)
    result = cql.execute(read_stmt)

    remaining = expected_mutation_fragments
    while remaining:
        rows = list(result.current_rows)
        print(f"rows({len(rows)}): {rows}")
        current_page_size = min(page_size, remaining)
        assert len(rows) == current_page_size
        remaining -= current_page_size
        if remaining:
            assert result.has_more_pages
            result.fetch_next_page()


@pytest.mark.parametrize("test_keyspace", ["tablets", "vnodes"], indirect=True)
def test_slicing_rows(cql, test_table, scylla_only):
    """ Test that slicing rows from underlying works. """
    pk1 = util.unique_key_int()
    pk2 = util.unique_key_int()

    cql.execute(f"INSERT INTO {test_table} (pk1, pk2, ck1, ck2, v) VALUES ({pk1}, {pk2}, 0, 0, 'vv')")
    cql.execute(f"INSERT INTO {test_table} (pk1, pk2, ck1, ck2, v) VALUES ({pk1}, {pk2}, 0, 20, 'vv')")
    cql.execute(f"INSERT INTO {test_table} (pk1, pk2, ck1, ck2, v) VALUES ({pk1}, {pk2}, 1, 20, 'vv')")

    nodetool.flush(cql, f"{test_table}")

    all_rows = list(cql.execute(f"""SELECT * FROM MUTATION_FRAGMENTS({test_table})
            WHERE
                pk1 = {pk1} AND
                pk2 = {pk2} AND
                mutation_source > 'sstable:'"""))
    mutation_source = all_rows[0].mutation_source

    def check_slice(ck1, ck2_start_inclusive, ck2_end_exclusive):
        res = list(cql.execute(f"""SELECT * FROM MUTATION_FRAGMENTS({test_table})
            WHERE
                pk1 = {pk1} AND
                pk2 = {pk2} AND
                mutation_source = '{mutation_source}' AND
                partition_region = 2 AND
                ck1 = {ck1} AND
                ck2 >= {ck2_start_inclusive} AND
                ck2 < {ck2_end_exclusive}
            """))
        expected_rows = [r for r in all_rows if r.ck1 == ck1 and r.ck2 >= ck2_start_inclusive and r.ck2 < ck2_end_exclusive]
        assert res == expected_rows

    check_slice(0, 0, 1)
    check_slice(0, 1, 10)
    check_slice(0, 1, 20)
    check_slice(0, 1, 21)
    check_slice(0, 20, 21)
    check_slice(0, 21, 210)
    check_slice(1, 0, 2)
    check_slice(1, 0, 20)
    check_slice(1, 0, 22)
    check_slice(2, 0, 100)


@pytest.mark.parametrize("test_keyspace", ["tablets", "vnodes"], indirect=True)
def test_slicing_range_tombstone_changes(cql, test_table, scylla_only):
    """ Test that slicing range-tombstone-changes from underlying works. """
    pk1 = util.unique_key_int()
    pk2 = util.unique_key_int()
    ck1 = 0

    cql.execute(f"DELETE FROM {test_table} WHERE pk1 = {pk1} AND pk2 = {pk2} AND ck1 = {ck1}")
    cql.execute(f"DELETE FROM {test_table} WHERE pk1 = {pk1} AND pk2 = {pk2} AND ck1 = {ck1} AND ck2 > 10 AND ck2 < 20")
    cql.execute(f"DELETE FROM {test_table} WHERE pk1 = {pk1} AND pk2 = {pk2} AND ck1 = {ck1} AND ck2 > 20 AND ck2 < 30")

    nodetool.flush(cql, f"{test_table}")

    sample_row = list(cql.execute(f"""SELECT * FROM MUTATION_FRAGMENTS({test_table})
        WHERE
            pk1 = {pk1} AND
            pk2 = {pk2} AND
            mutation_source > 'sstable:'
        LIMIT 1"""))
    mutation_source = sample_row[0].mutation_source

    def check_slice_ck1_fixed(ck2_start_inclusive, ck2_end_exclusive, expected_rtcs):
        res = list(cql.execute(f"""SELECT * FROM MUTATION_FRAGMENTS({test_table})
            WHERE
                pk1 = {pk1} AND
                pk2 = {pk2} AND
                mutation_source = '{mutation_source}' AND
                partition_region = 2 AND
                ck1 = {ck1} AND
                ck2 >= {ck2_start_inclusive} AND
                ck2 < {ck2_end_exclusive}
            """))
        assert len(res) == len(expected_rtcs)
        for row, rtc in zip(res, expected_rtcs):
            assert row.ck1 == ck1
            assert row.ck2 == rtc

    check_slice_ck1_fixed(0, 8, [0, 8])
    check_slice_ck1_fixed(1, 10, [1, 10])
    check_slice_ck1_fixed(10, 28, [10, 10, 20, 20, 28])
    check_slice_ck1_fixed(30, 38, [30, 30, 38])


@pytest.mark.parametrize("test_keyspace", ["tablets", "vnodes"], indirect=True)
def test_ck_in_query(cql, test_table, scylla_only):
    pk1 = util.unique_key_int()
    pk2 = util.unique_key_int()

    cql.execute(f"INSERT INTO {test_table} (pk1, pk2, ck1, ck2, v) VALUES ({pk1}, {pk2}, 0, 0, 'vv')")
    cql.execute(f"INSERT INTO {test_table} (pk1, pk2, ck1, ck2, v) VALUES ({pk1}, {pk2}, 1, 1, 'vv')")
    nodetool.flush(cql, f"{test_table}")
    cql.execute(f"INSERT INTO {test_table} (pk1, pk2, ck1, ck2, v) VALUES ({pk1}, {pk2}, 0, 0, 'vv')")

    sources_res = list(cql.execute(f"SELECT * FROM MUTATION_FRAGMENTS({test_table}) WHERE pk1 = {pk1} AND pk2 = {pk2}"))

    sources = {r.mutation_source.split(":")[0]: r.mutation_source for r in sources_res}
    assert len(sources) == 3
    assert "memtable" in sources
    assert "row-cache" in sources
    assert "sstable" in sources

    res = list(cql.execute(f"""SELECT * FROM MUTATION_FRAGMENTS({test_table})
        WHERE
            pk1 = {pk1} AND
            pk2 = {pk2} AND
            (mutation_source, partition_region, ck1, ck2, position_weight) IN (
                ('{sources["memtable"]}', 2, 0, 0, 0),
                ('{sources["row-cache"]}', 2, 0, 0, 0),
                ('{sources["sstable"]}', 2, 0, 0, 0),
                ('{sources["sstable"]}', 2, 1, 1, 0))
        """))

    columns = ("mutation_source", "partition_region", "ck1", "ck2", "position_weight")
    expected_results = [
            (sources["memtable"], 2, 0, 0, 0),
            (sources["row-cache"], 2, 0, 0, 0),
            (sources["sstable"], 2, 0, 0, 0),
            (sources["sstable"], 2, 1, 1, 0),
    ]

    assert len(res) == len(expected_results)
    for row, expected_row in zip(res, expected_results):
        for col_name, expected_value in zip(columns, expected_row):
            assert hasattr(row, col_name)
            assert getattr(row, col_name) == expected_value


@pytest.mark.parametrize("test_keyspace", ["tablets", "vnodes"], indirect=True)
def test_many_partitions(cql, test_keyspace, scylla_only):
    num_partitions = 5000
    with util.new_test_table(cql, test_keyspace, 'pk int PRIMARY KEY, v int') as table:
        delete_id = cql.prepare(f"DELETE FROM {table} WHERE pk = ?")
        for pk in range(num_partitions):
            cql.execute(delete_id, (pk,))

        nodetool.flush(cql, f"{table}")

        # since this scan is very slow, we create an extra patient cql connection,
        # with an abundant 10 minutes timeout
        ep = cql.hosts[0].endpoint
        with util.cql_session(ep.address, ep.port, False, 'cassandra', 'cassandra', 600) as patient_cql:
            res = list(patient_cql.execute(
                f"SELECT * FROM MUTATION_FRAGMENTS({table}) WHERE mutation_source > 'sstable:' ALLOW FILTERING"))
        pks = set()
        partition_starts = 0
        partition_ends = 0
        for row in res:
            assert row.pk >= 0 and row.pk < num_partitions
            if row.mutation_fragment_kind == "partition start":
                partition_starts += 1
                pks.add(row.pk)
            elif row.mutation_fragment_kind == "partition end":
                partition_ends += 1
                assert row.pk in pks
            else:
                pytest.fail(f"Unexpected mutation fragment kind: {row.mutation_fragment_kind}")

        assert partition_starts == num_partitions
        assert partition_ends == num_partitions
        assert len(pks) == num_partitions


@pytest.mark.xfail(reason="issue #18768; token() filtering doesn't work with MUTATION_FRAGMENTS")
def test_mutation_fragments_vs_token(cql, test_keyspace, scylla_only):
    with util.new_test_table(cql, test_keyspace, 'pk int PRIMARY KEY, c int') as table:
        print(f'INSERT INFO {table} (pk, c) VALUES (0, 0)')
        cql.execute(f'INSERT INTO {table} (pk, c) VALUES (0, 0)')
        # FIXME add some reasonable validation of selected keys vs tokens
        cql.execute(f'SELECT * FROM MUTATION_FRAGMENTS({table}) WHERE token(pk) <= -1')
