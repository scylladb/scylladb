# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Tests for filtering with the LIKE operator: '%' and '_' wildcards and
# their escaping, on partition key, clustering key, regular and static
# columns of the string types, combined with other restrictions and LIMIT,
# on materialized views, and the rejection of LIKE on non-string types.
#
# Each test creates its own RF=1 vnodes keyspace, like the dtests these
# tests come from. The tests of static columns and of views end with a
# flush of all tables, as a dtest node flushes its memtables when it stops.

import contextlib
import time

import pytest
from cassandra.protocol import InvalidRequest

from .nodetool import flush_all
from .util import new_test_keyspace


@contextlib.contextmanager
def like_keyspace(cql):
    with new_test_keyspace(cql, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} "
                                "AND tablets = {'enabled': false}") as ks:
        yield ks


def rows(cql, query):
    return [list(r) for r in cql.execute(query)]


def assert_all(cql, query, expected, ignore_order=False):
    res = rows(cql, query)
    if ignore_order:
        assert sorted(res, key=repr) == sorted(expected, key=repr), query
    else:
        assert res == expected, query


def assert_one(cql, query, expected):
    assert rows(cql, query) == [expected], query


def assert_none(cql, query):
    assert rows(cql, query) == [], query


def simple_table(cql, ks, cl_type="text"):
    cql.execute(f"CREATE TABLE {ks}.test (pk {cl_type}, ck {cl_type}, test {cl_type}, PRIMARY KEY (pk, ck))")


def populate_basic_data(cql, ks):
    all_data = [[f"teststring{i}", f"{i}teststring", f"test{i}string"] for i in range(5)]
    for d in all_data:
        cql.execute(f"INSERT INTO {ks}.test (pk, ck, test) VALUES ('{d[0]}', '{d[1]}', '{d[2]}')")
    return all_data


def populate_several_partitions(cql, ks, num_partitions=5, rows_per_partition=2):
    all_data = [[f"teststring{j}", f"{i}teststring", f"test{i}string"] for j in range(num_partitions) for i in range(rows_per_partition)]
    for d in all_data:
        cql.execute(f"INSERT INTO {ks}.test (pk, ck, test) VALUES ('{d[0]}', '{d[1]}', '{d[2]}')")
    return all_data


special_values = "!@#$%^&*()-_<>?.,/\\ "


def populate_specific_data(cql, ks):
    for i in range(5):
        cql.execute(f"INSERT INTO {ks}.test (pk, ck, test) VALUES ('TEST%STRING{i}', '{i}Test_String', '_{{}}%')")
    for char in special_values:
        cql.execute(f"INSERT INTO {ks}.test (pk, ck, test) VALUES ('TEST%STRING7', 'cluster_{char}', '{char}')")
    return rows(cql, f"SELECT * FROM {ks}.test")


# Queries of the basic data set, for each column and wildcard: (kind, query, expected row index).
basic_queries = {
    ("pk", "percent"): [
        ("all", "pk LIKE 'test%'"), ("all", "pk LIKE '%string%'"), ("all", "pk LIKE '%'"),
        (3, "pk LIKE '%string3'"), (3, "pk LIKE 'te%ng3'"), (1, "pk LIKE 't%1'"),
        (None, "pk LIKE 'TEST%'"), (None, "pk LIKE '%STR%'"), (None, "pk LIKE '%strinG3'"),
    ],
    ("ck", "percent"): [
        ("all", "ck LIKE '%ing'"), ("all", "ck LIKE '%test%'"), ("all", "ck LIKE '%'"),
        (3, "ck LIKE '3%'"), (1, "ck LIKE '1te%ng'"), (2, "ck LIKE '2%g'"),
        (None, "ck LIKE '%STRING'"), (None, "ck LIKE '%EST%'"), (None, "ck LIKE '3%strinG'"),
    ],
    ("cl", "percent"): [
        ("all", "test LIKE '%ing'"), ("all", "test LIKE '%test%'"), ("all", "test LIKE 'test%'"),
        ("all", "test LIKE 'test%string'"), ("all", "test LIKE '%'"),
        (4, "test LIKE 'test4%'"), (0, "test LIKE '%0string'"), (2, "test LIKE '%2%'"),
        (None, "test LIKE 'Test%STRING'"), (None, "test LIKE 'tesT%'"), (None, "test LIKE '3%strinG'"),
    ],
    ("pk", "underscore"): [
        ("all", "pk LIKE 'teststring_'"), ("all", "pk LIKE '_eststring_'"), ("all", "pk LIKE 'tes_string_'"),
        ("all", "pk LIKE '___________'"),
        (3, "pk LIKE '_eststring3'"), (3, "pk LIKE 'tes__tring3'"), (1, "pk LIKE 't_stst___g1'"),
        (None, "pk LIKE 'TESTSTRING_'"), (None, "pk LIKE '_EstSTRing_'"), (None, "pk LIKE '_strinG3'"),
        (None, "pk LIKE '_teststring3'"), (None, "pk LIKE 'teststring3_'"), (None, "pk LIKE 'test_string3'"),
    ],
    ("ck", "underscore"): [
        ("all", "ck LIKE '_teststring'"), ("all", "ck LIKE '_teststrin_'"), ("all", "ck LIKE '_tes_str_n_'"),
        ("all", "ck LIKE '___________'"),
        (2, "ck LIKE '2teststrin_'"), (3, "ck LIKE '3tes__tring'"), (1, "ck LIKE '1t_stst___g'"),
        (None, "ck LIKE '_TESTSTRING'"), (None, "ck LIKE '__EstSTRin_'"), (None, "ck LIKE '3_strinG'"),
        (None, "ck LIKE '_3teststring'"), (None, "ck LIKE '3teststring_'"), (None, "ck LIKE 'test_string3'"),
    ],
    ("cl", "underscore"): [
        ("all", "test LIKE 'test_string'"), ("all", "test LIKE '_est_strin_'"), ("all", "test LIKE '___________'"),
        (0, "test LIKE 'test0strin_'"), (3, "test LIKE 'te__3_tring'"), (1, "test LIKE 't_st1st___g'"),
        (None, "test LIKE 'TEST_STRING'"), (None, "test LIKE '_Est1STRin_'"), (None, "test LIKE 'Test_strinG'"),
        (None, "test LIKE '_test1string'"), (None, "test LIKE 'test_string_'"), (None, "test LIKE 'test_1string'"),
    ],
}


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("cl_type", ["text", "varchar", "ascii"])
@pytest.mark.parametrize("column,sign", list(basic_queries))
def test_filtering_of_string_type_with_wildcard(cql, column, sign, cl_type):
    with like_keyspace(cql) as ks:
        simple_table(cql, ks, cl_type)
        all_data = populate_basic_data(cql, ks)
        queries = list(basic_queries[(column, sign)])
        if (column, sign, cl_type) == ("ck", "underscore", "ascii"):
            queries.append((None, "ck LIKE '_' and ck = '1teststring'"))
        for kind, where in queries:
            q = f"SELECT * FROM {ks}.test WHERE {where} ALLOW FILTERING"
            if kind == "all":
                assert_all(cql, q, all_data, ignore_order=True)
            elif kind is None:
                assert_none(cql, q)
            else:
                assert_one(cql, q, all_data[kind])


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
def test_multiple_like_operator_on_same_column(cql):
    with like_keyspace(cql) as ks:
        simple_table(cql, ks)
        all_data = populate_basic_data(cql, ks)
        assert_one(cql, f"SELECT * FROM {ks}.test WHERE pk LIKE '%4' and pk LIKE '%' ALLOW FILTERING", all_data[4])
        assert_none(cql, f"SELECT * FROM {ks}.test WHERE ck LIKE '%4%' and ck LIKE '1teststring' ALLOW FILTERING")
        assert_one(cql, f"SELECT * FROM {ks}.test WHERE test LIKE '%4%' and test LIKE '%' ALLOW FILTERING", all_data[4])


unsupported_column_types_and_values = {
    "bigint": ("10000", "1", "2"),
    "boolean": ("true", "true", "false"),
    "blob": ("textAsBlob('1234567890qwertyuiop')", "textAsBlob('a')", "bigintAsBlob(1)"),
    "date": ("currentDate()", "currentDate()", "currentDate()"),
    "decimal": ("10.1", "11.1", "12.2"),
    "double": ("10.1000001", "11.111111", "22.22222"),
    "duration": ("89h1m48s", "11h11m11s", "22h22m22s"),
    "float": ("10.10001", "33.33", "44.44"),
    "inet": ("'1.1.1.1'", "'1.1.1.1'", "'2.2.2.2'"),
    "int": ("100001", "1", "2"),
    "smallint": ("1", "1", "2"),
    "time": ("currentTime()", "currentTime()", "currentTime()"),
    "timestamp": ("currentTimestamp()", "currentTimestamp()", "currentTimestamp()"),
    "timeuuid": ("currentTimeUUID()", "currentTimeUUID()", "currentTimeUUID()"),
    "tinyint": ("1", "2", "5"),
    "uuid": ("uuid()", "uuid()", "uuid()"),
    "varint": ("1", "4", "5"),
}


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
def test_unsupported_data_types(cql):
    names = list(unsupported_column_types_and_values)
    # The first type is the partition key.
    columns = [f"pk_{names[0]}"] + [f"cl_{t}" for t in names[1:]]
    types = [f"{c} {t}" for c, t in zip(columns, names)]
    values = [v[0] for v in unsupported_column_types_and_values.values()]
    with like_keyspace(cql) as ks:
        cql.execute(f"CREATE TABLE {ks}.test ({','.join(types)}, PRIMARY KEY ({columns[0]}))")
        cql.execute(f"INSERT INTO {ks}.test ({','.join(columns)}) VALUES ({','.join(values)})")
        for column in columns:
            with pytest.raises(InvalidRequest, match="LIKE is allowed only on string type"):
                cql.execute(f"SELECT {column} FROM {ks}.test WHERE {column} LIKE '%'")
            with pytest.raises(InvalidRequest, match="LIKE is allowed only on string type"):
                cql.execute(f"SELECT {column} FROM {ks}.test WHERE {column} LIKE '_'")


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
def test_unsupported_data_types_and_supported_data_types(cql):
    with like_keyspace(cql) as ks:
        for t in unsupported_column_types_and_values:
            cql.execute(f"CREATE TABLE {ks}.t_{t} (pk text, cl_{t} {t}, cl_text text, PRIMARY KEY (pk))")
        for t, values in unsupported_column_types_and_values.items():
            for i, value in enumerate(values[1:]):
                cql.execute(f"INSERT INTO {ks}.t_{t} (pk, cl_{t}, cl_text) VALUES ('text_{i}', {value}, 'text%{i}')")
        for t in unsupported_column_types_and_values:
            for where in [f"cl_{t} LIKE '%'", f"cl_{t} LIKE '_'", f"cl_{t} LIKE '_' AND cl_text LIKE '%1%'"]:
                with pytest.raises(InvalidRequest, match="LIKE is allowed only on string types"):
                    cql.execute(f"SELECT * FROM {ks}.t_{t} WHERE {where} ALLOW FILTERING")
            assert_one(cql, f"SELECT pk FROM {ks}.t_{t} WHERE cl_text LIKE '%1%' ALLOW FILTERING", ["text_1"])
            assert_all(cql, f"SELECT cl_text FROM {ks}.t_{t} WHERE pk LIKE 'text%' ALLOW FILTERING", [["text%0"], ["text%1"]], ignore_order=True)


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
def test_filtering_with_like_operator_by_static_field(cql):
    with like_keyspace(cql) as ks:
        cql.execute(f"CREATE TABLE {ks}.test (pk ascii, ck text, test varchar, cl_static text STATIC, PRIMARY KEY (pk, ck))")
        all_data = [[f"teststring{i}", f"{i}teststring", f"test{i}string", "static teststring"] for i in range(5)]
        for d in all_data:
            cql.execute(f"INSERT INTO {ks}.test (pk, ck, test, cl_static) VALUES ('{d[0]}', '{d[1]}', '{d[2]}', '{d[3]}')")
        select = f"SELECT pk, ck, test, cl_static FROM {ks}.test WHERE"
        for pattern in ["%", "static%teststring", "static teststring", "static_teststring", "% %"]:
            assert_all(cql, f"{select} cl_static LIKE '{pattern}' ALLOW FILTERING", all_data, ignore_order=True)
        assert_one(cql, f"{select} test LIKE 'test0string' ALLOW FILTERING", all_data[0])
        assert_none(cql, f"{select} cl_static LIKE 'static\\_teststring' ALLOW FILTERING")
        assert_none(cql, f"{select} cl_static LIKE 'static\\%teststring' ALLOW FILTERING")
        flush_all(cql)


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
def test_filtering_query_limiting(cql):
    with like_keyspace(cql) as ks:
        simple_table(cql, ks)
        all_data = populate_several_partitions(cql, ks)
        for where in ["pk LIKE 'test%' LIMIT 1", "ck LIKE '%string' LIMIT 1", "test LIKE 'test_string' LIMIT 1"]:
            row = rows(cql, f"SELECT * FROM {ks}.test WHERE {where} ALLOW FILTERING")
            assert len(row) == 1
            assert row[0] in all_data
        for limit, count in [(1, 5), (2, 10)]:
            res = rows(cql, f"SELECT * FROM {ks}.test WHERE test LIKE 'test%' PER PARTITION LIMIT {limit} ALLOW FILTERING")
            assert len(res) == count
            for row in res:
                assert row in all_data


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("cl_type", ["text", "ascii", "varchar"])
def test_advanced_matching(cql, cl_type):
    with like_keyspace(cql) as ks:
        simple_table(cql, ks, cl_type)
        all_data = populate_specific_data(cql, ks)
        special_data = [x for x in all_data if "cluster" in x[1]]
        select = f"SELECT * FROM {ks}.test WHERE"

        # Special chars, escaped, with LIKE operators.
        for char in special_values:
            row = ["TEST%STRING7", f"cluster_{char}", char]
            # Filter by column.
            assert_one(cql, f"{select} test LIKE '\\{char}' ALLOW FILTERING", row)
            assert_one(cql, f"{select} test LIKE '\\_' ALLOW FILTERING", ["TEST%STRING7", "cluster__", "_"])
            assert_one(cql, f"{select} test LIKE '\\%' ALLOW FILTERING", ["TEST%STRING7", "cluster_%", "%"])
            assert_all(cql, f"{select} test LIKE '_' ALLOW FILTERING", special_data, ignore_order=True)
            assert_all(cql, f"{select} test LIKE '%' ALLOW FILTERING", all_data, ignore_order=True)
            # Filter by clustering key.
            assert_one(cql, f"{select} ck LIKE 'cluster\\_\\{char}' ALLOW FILTERING", row)
            # An unescaped % or _ matches every special row.
            if char not in ["%", "_"]:
                assert_one(cql, f"{select} ck LIKE 'cluster\\_{char}' ALLOW FILTERING", row)
            else:
                assert_all(cql, f"{select} ck LIKE 'cluster\\_{char}' ALLOW FILTERING", special_data)
            assert_one(cql, f"{select} ck LIKE 'cluster_\\{char}' ALLOW FILTERING", row)
            assert_one(cql, f"{select} ck LIKE 'cluster%\\{char}' ALLOW FILTERING", row)

        # Case sensitivity and combinations on the partition key.
        for pattern in ["TEST%", "TEST%STRING%", "TEST%NG_"]:
            assert_all(cql, f"{select} pk LIKE '{pattern}' ALLOW FILTERING", all_data, ignore_order=True)
        assert_none(cql, f"{select} pk LIKE 'test%' ALLOW FILTERING")
        assert_one(cql, f"{select} pk LIKE 'TEST\\%STRING0' ALLOW FILTERING", ["TEST%STRING0", "0Test_String", "_{}%"])
        assert_one(cql, f"{select} pk LIKE 'TEST_STRING1' ALLOW FILTERING", ["TEST%STRING1", "1Test_String", "_{}%"])

        # Case sensitivity and combinations on the clustering key.
        expected_rows = [x for x in all_data if "String" in x[1]]
        for pattern in ["%Test%Str%", "_Test\\_S%", "%_\\_String"]:
            assert_all(cql, f"{select} ck LIKE '{pattern}' ALLOW FILTERING", expected_rows, ignore_order=True)
        assert_none(cql, f"{select} ck LIKE '%_\\_s%' ALLOW FILTERING")
        assert_none(cql, f"{select} ck LIKE '_test_string' ALLOW FILTERING")
        assert_one(cql, f"{select} ck LIKE '0Test\\_String' ALLOW FILTERING", ["TEST%STRING0", "0Test_String", "_{}%"])
        assert_one(cql, f"{select} ck LIKE '1Test_String' ALLOW FILTERING", ["TEST%STRING1", "1Test_String", "_{}%"])

        # Escaping of the LIKE operators.
        expected_rows = [x for x in all_data if "_{}%" in x[2]]
        for pattern in ["_{}%", "\\_{}\\%", "\\_\\{\\}\\%"]:
            assert_all(cql, f"{select} test like '{pattern}' ALLOW FILTERING", expected_rows)
        assert_all(cql, f"{select} test like '%' ALLOW FILTERING", all_data)
        assert_none(cql, f"{select} test like '' ALLOW FILTERING")


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
def test_filtering_combinations_of_fields_and_wc(cql):
    with like_keyspace(cql) as ks:
        simple_table(cql, ks)
        all_data = populate_several_partitions(cql, ks)
        select = f"SELECT * FROM {ks}.test WHERE"

        expected_rows = [x for x in all_data if "0" in x[1]]
        assert_all(cql, f"{select} pk LIKE '_e%t%g_' AND ck LIKE '0%' ALLOW FILTERING", expected_rows, ignore_order=True)

        expected_rows = [x for x in all_data if "1" in x[2]]
        assert_all(cql, f"{select} ck LIKE '_test%' and test LIKE '%1%' ALLOW FILTERING", expected_rows, ignore_order=True)
        assert_one(cql, f"{select} pk LIKE 'teststring3' AND ck LIKE '1teststring' AND test LIKE 'test1string' ALLOW FILTERING",
                   ["teststring3", "1teststring", "test1string"])
        assert_none(cql, f"{select} pk LIKE 'teststring3' AND ck LIKE '0teststring' AND test LIKE 'test1string' ALLOW FILTERING")
        assert_none(cql, f"{select} pk LIKE 'teststring_' and test LIKE 'test\\_string' ALLOW FILTERING")

        expected_rows = [x for x in all_data if "1" in x[1] and "1" in x[2]]
        assert_all(cql, f"{select} test LIKE '%1%' AND ck = '1teststring' ALLOW FILTERING", expected_rows, ignore_order=True)
        assert_one(cql, f"{select} pk LIKE '%1' AND test LIKE 'tes_1_tring' ALLOW FILTERING", ["teststring1", "1teststring", "test1string"])
        assert_none(cql, f"{select} pk LIKE '%1' AND test = 'tes_1_tring' ALLOW FILTERING")
        assert_none(cql, f"{select} pk in ('teststring1', 'teststring0') AND ck > '0teststring' AND test LIKE 'test0string' ALLOW FILTERING")

        expected_rows = [x for x in all_data if ("0" in x[0] or "1" in x[0]) and "1" in x[1] and ("1" in x[2] or "0" in x[2])]
        assert_all(cql, f"{select} pk in ('teststring1', 'teststring0') AND ck > '0teststring' AND test LIKE 'test_string' ALLOW FILTERING",
                   expected_rows, ignore_order=True)
        assert_none(cql, f"{select} pk LIKE '%4' AND ck LIKE '_teststring' AND test LIKE '%Test%String%' ALLOW FILTERING")
        assert_all(cql, f"{select} pk LIKE '%%' and pk in ('teststring1', 'teststring2') ALLOW FILTERING",
                   [[f"teststring{p}", f"{c}teststring", f"test{c}string"] for p in (1, 2) for c in (0, 1)], ignore_order=True)


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
def test_filtering_after_update_values(cql):
    with like_keyspace(cql) as ks:
        simple_table(cql, ks)
        populate_specific_data(cql, ks)

        cql.execute(f"INSERT INTO {ks}.test (pk, ck, test) VALUES ('TEST%STRING6', ' ', '')")
        all_data = rows(cql, f"SELECT * FROM {ks}.test")
        assert_one(cql, f"SELECT * FROM {ks}.test WHERE test LIKE '' ALLOW FILTERING", ["TEST%STRING6", " ", ""])

        cql.execute(f"UPDATE {ks}.test SET test='!@#$%^&*()?:\"\\' WHERE pk = 'TEST%STRING6' and ck = ' '")
        assert_one(cql, f"SELECT * FROM {ks}.test WHERE pk = 'TEST%STRING6' and ck = ' ' ALLOW FILTERING", ["TEST%STRING6", " ", '!@#$%^&*()?:"\\'])
        assert_none(cql, f"SELECT * FROM {ks}.test WHERE test LIKE '' ALLOW FILTERING")
        assert_all(cql, f"SELECT pk FROM {ks}.test WHERE ck LIKE '%' and test LIKE '%' ALLOW FILTERING", [[r[0]] for r in all_data], ignore_order=True)

        cql.execute(f"DELETE FROM {ks}.test WHERE pk in ('TEST%STRING0', 'TEST%STRING1', 'TEST%STRING2')")
        all_data = rows(cql, f"SELECT * FROM {ks}.test")
        assert_all(cql, f"SELECT pk FROM {ks}.test WHERE pk LIKE 'TEST\\%STRING_' ALLOW FILTERING", [[r[0]] for r in all_data], ignore_order=True)

        for i in range(3):
            cql.execute(f"INSERT INTO {ks}.test (pk, ck, test) VALUES ('qwErty{i}', 'ytrewq{i}', '')")
        all_data = rows(cql, f"SELECT * FROM {ks}.test")
        assert_all(cql, f"SELECT pk FROM {ks}.test WHERE pk LIKE '%E%_' ALLOW FILTERING", [[r[0]] for r in all_data], ignore_order=True)


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("column,patterns", [
    ("city", ["y%", "%q%", "%", "%2", "ytrewq3", "ytrewq_", "ytr_wq4", "", "Y%q_"]),
    ("name", ["q%", "%w%", "%", "%2", "qwerty3", "qwerty_", "qw_rty4", "", "q%T_"]),
], ids=["new_primary", "new_column"])
def test_filtering_mv(cql, column, patterns):
    with like_keyspace(cql) as ks:
        cql.execute(f"CREATE TABLE {ks}.buildings (name text, city text, PRIMARY KEY (name))")
        cql.execute(f"CREATE MATERIALIZED VIEW {ks}.building_by_city AS SELECT * FROM {ks}.buildings "
                    "WHERE city IS NOT NULL PRIMARY KEY (city, name)")
        for i in range(5):
            cql.execute(f"INSERT INTO {ks}.buildings (name, city) VALUES ('qwerty{i}', 'ytrewq{i}')")
        expected = [[f"ytrewq{i}", f"qwerty{i}"] for i in range(5)]
        for _ in range(50):
            if len(rows(cql, f"SELECT * FROM {ks}.building_by_city")) == 5:
                break
            time.sleep(0.1)
        select = f"SELECT * FROM {ks}.building_by_city WHERE {column} LIKE"
        p_all1, p_all2, p_all3, p2, p3, p_all4, p4, p_none1, p_none2 = patterns
        for p in [p_all1, p_all2, p_all3]:
            assert_all(cql, f"{select} '{p}' ALLOW FILTERING", expected, ignore_order=True)
        assert_one(cql, f"{select} '{p2}' ALLOW FILTERING", expected[2])
        assert_one(cql, f"{select} '{p3}' ALLOW FILTERING", expected[3])
        assert_all(cql, f"{select} '{p_all4}' ALLOW FILTERING", expected, ignore_order=True)
        assert_one(cql, f"{select} '{p4}' ALLOW FILTERING", expected[4])
        assert_none(cql, f"{select} '{p_none1}' ALLOW FILTERING")
        assert_none(cql, f"{select} '{p_none2}' ALLOW FILTERING")
        flush_all(cql)
