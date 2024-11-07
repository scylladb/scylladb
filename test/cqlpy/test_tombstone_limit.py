# Copyright 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

#############################################################################
# Various tests for query_tombstone_page_limit
#
# Replicas are supposed to stop after seeing query_tombstone_page_limit amount of
# tombstones (partition, row or range tombstones) and cut the page even if not
# full or even empty.
# These tests verify that this is the case for both tombstone prefixes and spans,
# for all supported tombstones.
#############################################################################

from util import new_test_table, config_value_context, unique_key_int
from conftest import driver_bug_1
from cassandra.query import SimpleStatement
import pytest


# Tests that need a single partition can co-locate their data in a single table.
@pytest.fixture(scope="module")
def table(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'pk int, ck int, v int, PRIMARY KEY (pk, ck)') as table:
        yield table


@pytest.fixture(scope="module")
def lowered_tombstone_limit(cql):
    with config_value_context(cql, 'query_tombstone_page_limit', '10'):
        yield


def fetch_all_pages(results, col):
    pages = []
    while True:
        pages.append([getattr(r, col) for r in list(results.current_rows)])
        print('fetched page: {}'.format(pages[-1]))
        if results.has_more_pages:
            results.fetch_next_page()
        else:
            break

    return pages


def check_pages_single_partition(results, expected_pages):
    actual_pages = fetch_all_pages(results, 'ck')
    assert actual_pages == expected_pages


def test_row_tombstone_prefix(cql, table, lowered_tombstone_limit):
    delete_row_id = cql.prepare(f"DELETE FROM {table} WHERE pk = ? AND ck = ?")
    insert_row_id = cql.prepare(f"INSERT INTO {table} (pk, ck, v) VALUES (?, ?, ?)")

    pk = unique_key_int()

    for ck in range(0, 30):
        cql.execute(delete_row_id, (pk, ck))

    cql.execute(insert_row_id, (pk, 31, 0))

    statement = SimpleStatement(f"SELECT * FROM {table} WHERE pk = {pk}", fetch_size=10)
    check_pages_single_partition(cql.execute(statement), [
        [],
        [],
        [],
        [31]
    ])

    statement = SimpleStatement(f"SELECT * FROM {table} WHERE pk = {pk} AND v = 0 ALLOW FILTERING", fetch_size=10)
    check_pages_single_partition(cql.execute(statement), [
        [],
        [],
        [],
        [31]
    ])


def test_row_tombstone_span(cql, table, lowered_tombstone_limit, driver_bug_1):
    delete_row_id = cql.prepare(f"DELETE FROM {table} WHERE pk = ? AND ck = ?")
    insert_row_id = cql.prepare(f"INSERT INTO {table} (pk, ck, v) VALUES (?, ?, ?)")

    pk = unique_key_int()

    for ck in range(0, 11):
        cql.execute(insert_row_id, (pk, ck, 0))

    for ck in range(30, 50):
        cql.execute(delete_row_id, (pk, ck))

    cql.execute(insert_row_id, (pk, 51, 0))

    statement = SimpleStatement(f"SELECT * FROM {table} WHERE pk = {pk}", fetch_size=10)
    check_pages_single_partition(cql.execute(statement), [
        list(range(0, 10)),
        [10],
        [],
        [51]
    ])

    statement = SimpleStatement(f"SELECT * FROM {table} WHERE pk = {pk} AND v = 0 ALLOW FILTERING", fetch_size=10)
    check_pages_single_partition(cql.execute(statement), [
        list(range(0, 10)),
        [10],
        [],
        [51]
    ])


def test_range_tombstone_prefix(cql, table, lowered_tombstone_limit, driver_bug_1):
    delete_row_id = cql.prepare(f"DELETE FROM {table} WHERE pk = ? AND ck > ? AND ck < ?")
    insert_row_id = cql.prepare(f"INSERT INTO {table} (pk, ck, v) VALUES (?, ?, ?)")

    pk = unique_key_int()

    # generates 12 range tombstones -> 24 range tombstone changes
    for ck in range(0, 12 * 4, 4):
        cql.execute(delete_row_id, (pk, ck, ck + 3))

    cql.execute(insert_row_id, (pk, 2000, 0))

    statement = SimpleStatement(f"SELECT * FROM {table} WHERE pk = {pk}", fetch_size=10)
    check_pages_single_partition(cql.execute(statement), [
        [],
        [],
        [2000]
    ])

    statement = SimpleStatement(f"SELECT * FROM {table} WHERE pk = {pk} AND v = 0 ALLOW FILTERING", fetch_size=10)
    check_pages_single_partition(cql.execute(statement), [
        [],
        [],
        [2000]
    ])


def test_range_tombstone_span(cql, table, lowered_tombstone_limit, driver_bug_1):
    delete_row_id = cql.prepare(f"DELETE FROM {table} WHERE pk = ? AND ck > ? AND ck < ?")
    insert_row_id = cql.prepare(f"INSERT INTO {table} (pk, ck, v) VALUES (?, ?, ?)")

    pk = unique_key_int()

    for ck in range(0, 12):
        cql.execute(insert_row_id, (pk, ck, 0))

    # generates 10 range tombstones -> 20 range tombstone changes
    for ck in range(32, 32 + 10 * 4, 4):
        cql.execute(delete_row_id, (pk, ck, ck + 3))

    cql.execute(insert_row_id, (pk, 2000, 0))

    statement = SimpleStatement(f"SELECT * FROM {table} WHERE pk = {pk}", fetch_size=10)
    check_pages_single_partition(cql.execute(statement), [
        list(range(0, 10)),
        [10, 11],
        [],
        [2000]
    ])

    statement = SimpleStatement(f"SELECT * FROM {table} WHERE pk = {pk} AND v = 0 ALLOW FILTERING", fetch_size=10)
    check_pages_single_partition(cql.execute(statement), [
        list(range(0, 10)),
        [10, 11],
        [],
        [2000]
    ])


def test_empty_row_prefix(cql, table, lowered_tombstone_limit, driver_bug_1):
    # Use update to avoid creating a row-marker ...
    upsert_row_id = cql.prepare(f"UPDATE {table} SET v = ? WHERE pk = ? AND ck = ?")
    # ... so deleting the only live cell in the row makes it empty.
    delete_row_id = cql.prepare(f"DELETE v FROM {table} WHERE pk = ? AND ck = ?")

    pk = unique_key_int()

    for ck in range(0, 20):
        cql.execute(upsert_row_id, (0, pk, ck))

    for ck in range(0, 16):
        cql.execute(delete_row_id, (pk, ck))

    statement = SimpleStatement(f"SELECT * FROM {table} WHERE pk = {pk}", fetch_size=10)
    check_pages_single_partition(cql.execute(statement), [
        [],
        list(range(16, 20)),
    ])

    statement = SimpleStatement(f"SELECT * FROM {table} WHERE pk = {pk} AND v = 0 ALLOW FILTERING", fetch_size=10)
    check_pages_single_partition(cql.execute(statement), [
        [],
        list(range(16, 20)),
    ])



def test_empty_row_span(cql, table, lowered_tombstone_limit, driver_bug_1):
    # Use update to avoid creating a row-marker so that ...
    upsert_row_id = cql.prepare(f"UPDATE {table} SET v = ? WHERE pk = ? AND ck = ?")
    # ... deleting the only live cell in the row makes it empty.
    delete_row_id = cql.prepare(f"DELETE v FROM {table} WHERE pk = ? AND ck = ?")

    pk = unique_key_int()

    for ck in range(0, 30):
        cql.execute(upsert_row_id, (0, pk, ck))

    for ck in range(5, 28):
        cql.execute(delete_row_id, (pk, ck))

    statement = SimpleStatement(f"SELECT * FROM {table} WHERE pk = {pk}", fetch_size=10)
    check_pages_single_partition(cql.execute(statement), [
        list(range(0, 5)),
        [],
        list(range(28, 30)),
    ])

    statement = SimpleStatement(f"SELECT * FROM {table} WHERE pk = {pk} AND v = 0 ALLOW FILTERING", fetch_size=10)
    check_pages_single_partition(cql.execute(statement), [
        list(range(0, 5)),
        [],
        list(range(28, 30)),
    ])


def get_all_pks(cql, table):
    res = cql.execute(f"SELECT DISTINCT pk FROM {table}")
    return [r.pk for r in res]


# It is very hard to predict the number of pages as it depends on the
# distribution of partitions across vnodes.
# So we just check that we have the expected number of non-empty pages and at
# least one empty page.
def check_pages_many_partitions(results, expected):
    pages = fetch_all_pages(results, 'pk')

    expected_pages = {}
    for i, pk in expected.items():
        if i < 0:
            i = len(pages) + i
        expected_pages[i] = [pk]

    assert len(pages) >= (len(expected_pages) + 1) # at least one empty page
    for i, p in enumerate(pages):
        assert p == expected_pages.get(i, [])


@pytest.mark.parametrize("test_keyspace", ["tablets", "vnodes"], indirect=True)
def test_partition_tombstone_prefix(cql, test_keyspace, lowered_tombstone_limit, driver_bug_1):
    with new_test_table(cql, test_keyspace, 'pk int, ck int, v int, PRIMARY KEY (pk, ck)') as table:
        insert_row_id = cql.prepare(f"INSERT INTO {table} (pk, ck, v) VALUES (?, ?, ?)")
        delete_partition_id = cql.prepare(f"DELETE FROM {table} WHERE pk = ?")

        for pk in range(0, 400):
            cql.execute(insert_row_id, (pk, 0, 0))

        all_pks = get_all_pks(cql, table)

        for pk in all_pks[:-1]:
            cql.execute(delete_partition_id, (pk,))

        statement = SimpleStatement(f"SELECT * FROM {table}", fetch_size=10)
        check_pages_many_partitions(cql.execute(statement), {-1: all_pks[-1]})

        statement = SimpleStatement(f"SELECT * FROM {table} WHERE v = 0 ALLOW FILTERING", fetch_size=10)
        check_pages_many_partitions(cql.execute(statement), {-1: all_pks[-1]})


@pytest.mark.parametrize("test_keyspace", ["tablets", "vnodes"], indirect=True)
def test_partition_tombstone_span(cql, test_keyspace, lowered_tombstone_limit, driver_bug_1):
    with new_test_table(cql, test_keyspace, 'pk int, ck int, v int, PRIMARY KEY (pk, ck)') as table:
        insert_row_id = cql.prepare(f"INSERT INTO {table} (pk, ck, v) VALUES (?, ?, ?)")
        delete_partition_id = cql.prepare(f"DELETE FROM {table} WHERE pk = ?")

        for pk in range(0, 400):
            cql.execute(insert_row_id, (pk, 0, 0))

        all_pks = get_all_pks(cql, table)

        for pk in all_pks[1:-1]:
            cql.execute(delete_partition_id, (pk,))

        statement = SimpleStatement(f"SELECT * FROM {table}", fetch_size=10)
        check_pages_many_partitions(cql.execute(statement), {0: all_pks[0], -1: all_pks[-1]})

        statement = SimpleStatement(f"SELECT * FROM {table} WHERE v = 0 ALLOW FILTERING", fetch_size=10)
        check_pages_many_partitions(cql.execute(statement), {0: all_pks[0], -1: all_pks[-1]})


@pytest.mark.parametrize("test_keyspace", ["tablets", "vnodes"], indirect=True)
def test_static_row_tombstone_prefix(cql, test_keyspace, lowered_tombstone_limit, driver_bug_1):
    with new_test_table(cql, test_keyspace, 'pk int, ck int, v int, s int static, PRIMARY KEY (pk, ck)') as table:
        upsert_row_id = cql.prepare(f"UPDATE {table} SET s = ? WHERE pk = ?")
        delete_partition_id = cql.prepare(f"DELETE FROM {table} WHERE pk = ?")

        for pk in range(0, 400):
            cql.execute(upsert_row_id, (0, pk))

        all_pks = get_all_pks(cql, table)

        for pk in all_pks[:-1]:
            cql.execute(delete_partition_id, (pk,))

        statement = SimpleStatement(f"SELECT * FROM {table}", fetch_size=10)
        check_pages_many_partitions(cql.execute(statement), {-1: all_pks[-1]})

        statement = SimpleStatement(f"SELECT * FROM {table} WHERE v = 0 ALLOW FILTERING", fetch_size=10)
        check_pages_many_partitions(cql.execute(statement), {-1: all_pks[-1]})


@pytest.mark.parametrize("test_keyspace", ["tablets", "vnodes"], indirect=True)
def test_static_row_tombstone_span(cql, test_keyspace, lowered_tombstone_limit, driver_bug_1):
    with new_test_table(cql, test_keyspace, 'pk int, ck int, v int, s int static, PRIMARY KEY (pk, ck)') as table:
        upsert_row_id = cql.prepare(f"UPDATE {table} SET s = ? WHERE pk = ?")
        delete_partition_id = cql.prepare(f"DELETE FROM {table} WHERE pk = ?")

        for pk in range(0, 400):
            cql.execute(upsert_row_id, (0, pk))

        all_pks = get_all_pks(cql, table)

        for pk in all_pks[1:-1]:
            cql.execute(delete_partition_id, (pk,))

        statement = SimpleStatement(f"SELECT * FROM {table}", fetch_size=10)
        check_pages_many_partitions(cql.execute(statement), {0: all_pks[0], -1: all_pks[-1]})

        statement = SimpleStatement(f"SELECT * FROM {table} WHERE v = 0 ALLOW FILTERING", fetch_size=10)
        check_pages_many_partitions(cql.execute(statement), {0: all_pks[0], -1: all_pks[-1]})


# Sanity check that empty pages support didn't mess up truly empty results.
def test_empty_table(cql, test_keyspace, lowered_tombstone_limit, driver_bug_1):
    with new_test_table(cql, test_keyspace, 'pk int, ck int, v int, PRIMARY KEY (pk, ck)') as table:
        assert list(cql.execute(f"SELECT * FROM {table}")) == []
        assert list(cql.execute(f"SELECT * FROM {table} WHERE pk = 0")) == []
        assert list(cql.execute(f"SELECT * FROM {table} WHERE v = 0 ALLOW FILTERING")) == []


# Unpaged query should be not affected
def test_unpaged_query(cql, table, lowered_tombstone_limit, driver_bug_1):
    # Use update to avoid creating a row-marker ...
    upsert_row_id = cql.prepare(f"UPDATE {table} SET v = ? WHERE pk = ? AND ck = ?")
    # ... so deleting the only live cell in the row makes it empty.
    delete_row_id = cql.prepare(f"DELETE v FROM {table} WHERE pk = ? AND ck = ?")

    pk = unique_key_int()

    for ck in range(0, 20):
        cql.execute(upsert_row_id, (0, pk, ck))

    for ck in range(0, 16):
        cql.execute(delete_row_id, (pk, ck))

    statement = SimpleStatement(f"SELECT * FROM {table} WHERE pk = {pk}", fetch_size=None)
    rows = list(cql.execute(statement))
    assert len(rows) == 4
