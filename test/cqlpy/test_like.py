# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Tests for the LIKE operator in SELECT filtering (with ALLOW FILTERING).
#
# Scylla allows LIKE in filtering on any column of a string type - text,
# varchar or ascii - with SQL-like patterns: "%" matches any sequence of
# characters and "_" any single character, anywhere in the pattern, and a
# backslash escapes them. Cassandra before version 6 allows LIKE only on
# columns with a SASI index. Cassandra 6 also allows it in filtering, but
# supports only patterns of the form "abc%", "%abc", "%abc%" or "abc" -
# with a "%" in the middle of the pattern, or with "_", it silently matches
# nothing, and it rejects the pattern "%" - and doesn't support escapes.
# Most tests in this file check Scylla's more general patterns, so they are
# scylla_only. test_like_common_patterns checks the patterns supported by
# both.
#
# Except test_like_common_patterns, these tests were translated from
# scylla-dtest's like_operator_test.py.
#############################################################################

import pytest
from cassandra.protocol import InvalidRequest

from .test_secondary_index import wait_for_index
from .util import new_materialized_view, new_test_table, unique_name


# The rows of the "basic" table used by many tests below, with columns
# (pk, ck, test) and the given type for all three columns.
BASIC_ROWS = [(f'teststring{i}', f'{i}teststring', f'test{i}string') for i in range(5)]

def create_basic_table(cql, keyspace, column_type):
    return new_test_table(cql, keyspace, f'pk {column_type}, ck {column_type}, test {column_type}, PRIMARY KEY (pk, ck)')

def insert_rows(cql, table, rows):
    for row in rows:
        cql.execute(f"INSERT INTO {table} (pk, ck, test) VALUES ('{row[0]}', '{row[1]}', '{row[2]}')")

# A table, for each of the string types, holding BASIC_ROWS. The tests using
# it only read it.
@pytest.fixture(scope='module')
def basic_tables(cql, test_keyspace):
    with create_basic_table(cql, test_keyspace, 'text') as text_table, \
         create_basic_table(cql, test_keyspace, 'ascii') as ascii_table:
        tables = {'text': text_table, 'ascii': ascii_table}
        for table in tables.values():
            insert_rows(cql, table, BASIC_ROWS)
        yield tables

def select(cql, table, where, columns='*'):
    return list(cql.execute(f'SELECT {columns} FROM {table} WHERE {where} ALLOW FILTERING'))

# Check a list of (where, expected) pairs on a table with BASIC_ROWS, where
# expected is either 'ALL' - all rows, in any order - or the list of indexes
# in BASIC_ROWS of the single row expected, or an empty list.
def check_basic_filtering(cql, table, checks):
    for where, expected in checks:
        if expected == 'ALL':
            assert sorted(select(cql, table, where)) == BASIC_ROWS
        else:
            assert select(cql, table, where) == [BASIC_ROWS[i] for i in expected]

# The following six tests check LIKE with the "%" wildcard (any sequence of
# characters, including an empty one) or the "_" wildcard (exactly one
# character) on the partition key, the clustering key and a regular column,
# of each of the string types: wildcards at the beginning, end, middle and
# several places of the pattern, a pattern of only wildcards, and that
# matching is case sensitive.
# Translated from scylla-dtest's like_operator_test.py::TestLikeOperatorForBaseTable
# tests test_{pk,ck,cl}_filtering_of_{text,varchar,ascii}_type_with_{percent,underscore}_sign,
# which had the same checks for each of the three types, except that only
# test_ck_filtering_of_ascii_type_with_underscore_sign had the check of
# "ck LIKE '_' AND ck = '1teststring'", which here is done for all types.
# The tests here don't check varchar, because it is just another name for
# the text type.
string_types = ['text', 'ascii']

@pytest.mark.parametrize('column_type', string_types)
def test_like_partition_key_percent(cql, basic_tables, column_type, scylla_only):
    check_basic_filtering(cql, basic_tables[column_type], [
        ("pk LIKE 'test%'", 'ALL'),
        ("pk LIKE '%string%'", 'ALL'),
        ("pk LIKE '%'", 'ALL'),
        ("pk LIKE '%string3'", [3]),
        ("pk LIKE 'te%ng3'", [3]),
        ("pk LIKE 't%1'", [1]),
        # Matching is case sensitive
        ("pk LIKE 'TEST%'", []),
        ("pk LIKE '%STR%'", []),
        ("pk LIKE '%strinG3'", []),
    ])

@pytest.mark.parametrize('column_type', string_types)
def test_like_partition_key_underscore(cql, basic_tables, column_type, scylla_only):
    check_basic_filtering(cql, basic_tables[column_type], [
        ("pk LIKE 'teststring_'", 'ALL'),
        ("pk LIKE '_eststring_'", 'ALL'),
        ("pk LIKE 'tes_string_'", 'ALL'),
        ("pk LIKE '___________'", 'ALL'),
        ("pk LIKE '_eststring3'", [3]),
        ("pk LIKE 'tes__tring3'", [3]),
        ("pk LIKE 't_stst___g1'", [1]),
        # Matching is case sensitive
        ("pk LIKE 'TESTSTRING_'", []),
        ("pk LIKE '_EstSTRing_'", []),
        ("pk LIKE '_strinG3'", []),
        # "_" matches exactly one character
        ("pk LIKE '_teststring3'", []),
        ("pk LIKE 'teststring3_'", []),
        ("pk LIKE 'test_string3'", []),
    ])

@pytest.mark.parametrize('column_type', string_types)
def test_like_clustering_key_percent(cql, basic_tables, column_type, scylla_only):
    check_basic_filtering(cql, basic_tables[column_type], [
        ("ck LIKE '%ing'", 'ALL'),
        ("ck LIKE '%test%'", 'ALL'),
        ("ck LIKE '%'", 'ALL'),
        ("ck LIKE '3%'", [3]),
        ("ck LIKE '1te%ng'", [1]),
        ("ck LIKE '2%g'", [2]),
        # Matching is case sensitive
        ("ck LIKE '%STRING'", []),
        ("ck LIKE '%EST%'", []),
        ("ck LIKE '3%strinG'", []),
    ])

@pytest.mark.parametrize('column_type', string_types)
def test_like_clustering_key_underscore(cql, basic_tables, column_type, scylla_only):
    check_basic_filtering(cql, basic_tables[column_type], [
        ("ck LIKE '_teststring'", 'ALL'),
        ("ck LIKE '_teststrin_'", 'ALL'),
        ("ck LIKE '_tes_str_n_'", 'ALL'),
        ("ck LIKE '___________'", 'ALL'),
        ("ck LIKE '2teststrin_'", [2]),
        ("ck LIKE '3tes__tring'", [3]),
        ("ck LIKE '1t_stst___g'", [1]),
        # Matching is case sensitive
        ("ck LIKE '_TESTSTRING'", []),
        ("ck LIKE '__EstSTRin_'", []),
        ("ck LIKE '3_strinG'", []),
        # "_" matches exactly one character
        ("ck LIKE '_3teststring'", []),
        ("ck LIKE '3teststring_'", []),
        ("ck LIKE 'test_string3'", []),
        ("ck LIKE '_' AND ck = '1teststring'", []),
    ])

@pytest.mark.parametrize('column_type', string_types)
def test_like_regular_column_percent(cql, basic_tables, column_type, scylla_only):
    check_basic_filtering(cql, basic_tables[column_type], [
        ("test LIKE '%ing'", 'ALL'),
        ("test LIKE '%test%'", 'ALL'),
        ("test LIKE 'test%'", 'ALL'),
        ("test LIKE 'test%string'", 'ALL'),
        ("test LIKE '%'", 'ALL'),
        ("test LIKE 'test4%'", [4]),
        ("test LIKE '%0string'", [0]),
        ("test LIKE '%2%'", [2]),
        # Matching is case sensitive
        ("test LIKE 'Test%STRING'", []),
        ("test LIKE 'tesT%'", []),
        ("test LIKE '3%strinG'", []),
    ])

@pytest.mark.parametrize('column_type', string_types)
def test_like_regular_column_underscore(cql, basic_tables, column_type, scylla_only):
    check_basic_filtering(cql, basic_tables[column_type], [
        ("test LIKE 'test_string'", 'ALL'),
        ("test LIKE '_est_strin_'", 'ALL'),
        ("test LIKE '___________'", 'ALL'),
        ("test LIKE 'test0strin_'", [0]),
        ("test LIKE 'te__3_tring'", [3]),
        ("test LIKE 't_st1st___g'", [1]),
        # Matching is case sensitive
        ("test LIKE 'TEST_STRING'", []),
        ("test LIKE '_Est1STRin_'", []),
        ("test LIKE 'Test_strinG'", []),
        # "_" matches exactly one character
        ("test LIKE '_test1string'", []),
        ("test LIKE 'test_string_'", []),
        ("test LIKE 'test_1string'", []),
    ])

# Test the LIKE patterns which both Scylla and Cassandra 6 support - a
# prefix ("abc%"), a suffix ("%abc"), a substring ("%abc%") or a whole
# value ("abc") - on the partition key, the clustering key and a regular
# column, and that matching is case sensitive.
@pytest.mark.parametrize('column_type', string_types)
def test_like_common_patterns(cql, basic_tables, column_type, new_to_cassandra_6):
    check_basic_filtering(cql, basic_tables[column_type], [
        ("pk LIKE 'test%'", 'ALL'),
        ("pk LIKE '%string3'", [3]),
        ("pk LIKE '%string%'", 'ALL'),
        ("pk LIKE 'teststring1'", [1]),
        ("pk LIKE 'TEST%'", []),
        ("ck LIKE '3%'", [3]),
        ("ck LIKE '%ing'", 'ALL'),
        ("ck LIKE '%2test%'", [2]),
        ("ck LIKE '0teststring'", [0]),
        ("ck LIKE '%STRING'", []),
        ("test LIKE 'test4%'", [4]),
        ("test LIKE '%0string'", [0]),
        ("test LIKE '%t2s%'", [2]),
        ("test LIKE 'test3string'", [3]),
        ("test LIKE '%Test%'", []),
    ])

# Test that LIKE on the partition key without ALLOW FILTERING, and LIKE with
# a pattern which is a function returning a non-string type, are rejected.
# Translated from scylla-dtest's like_operator_test.py::TestLikeOperatorForBaseTable::
# test_invalid_queries_with_like_operator.
def test_like_invalid_queries(cql, basic_tables, scylla_only):
    table = basic_tables['text']
    for query, message in [(f"SELECT * FROM {table} WHERE pk LIKE 'teststring%'", 'ALLOW FILTERING'),
                           (f'SELECT * FROM {table} WHERE ck LIKE uuid() ALLOW FILTERING', 'type uuid'),
                           (f'SELECT * FROM {table} WHERE ck LIKE currentDate() ALLOW FILTERING', 'type date'),
                           (f'SELECT * FROM {table} WHERE ck LIKE currentTime() ALLOW FILTERING', 'type time')]:
        with pytest.raises(InvalidRequest, match=message):
            cql.execute(query)

# Test two LIKE restrictions on the same column.
# Translated from scylla-dtest's like_operator_test.py::TestLikeOperatorForBaseTable::
# test_multiple_like_operator_on_same_column.
def test_like_twice_on_same_column(cql, basic_tables, scylla_only):
    table = basic_tables['text']
    assert select(cql, table, "pk LIKE '%4' AND pk LIKE '%'") == [BASIC_ROWS[4]]
    assert select(cql, table, "ck LIKE '%4%' AND ck LIKE '1teststring'") == []
    assert select(cql, table, "test LIKE '%4%' AND test LIKE '%'") == [BASIC_ROWS[4]]

# Types on which LIKE isn't allowed, with three values of each type.
UNSUPPORTED_TYPES_AND_VALUES = {
    'bigint': ('10000', '1', '2'),
    'boolean': ('true', 'true', 'false'),
    'blob': ("textAsBlob('1234567890qwertyuiop')", "textAsBlob('a')", 'bigintAsBlob(1)'),
    'date': ('currentDate()', 'currentDate()', 'currentDate()'),
    'decimal': ('10.1', '11.1', '12.2'),
    'double': ('10.1000001', '11.111111', '22.22222'),
    'duration': ('89h1m48s', '11h11m11s', '22h22m22s'),
    'float': ('10.10001', '33.33', '44.44'),
    'inet': ("'1.1.1.1'", "'1.1.1.1'", "'2.2.2.2'"),
    'int': ('100001', '1', '2'),
    'smallint': ('1', '1', '2'),
    'time': ('currentTime()', 'currentTime()', 'currentTime()'),
    'timestamp': ('currentTimestamp()', 'currentTimestamp()', 'currentTimestamp()'),
    'timeuuid': ('currentTimeUUID()', 'currentTimeUUID()', 'currentTimeUUID()'),
    'tinyint': ('1', '2', '5'),
    'uuid': ('uuid()', 'uuid()', 'uuid()'),
    'varint': ('1', '4', '5'),
}

# Test that LIKE is rejected on columns of non-string types - on the
# partition key (of type bigint) and on regular columns of all other types.
# Translated from scylla-dtest's like_operator_test.py::TestLikeOperatorForBaseTable::
# test_unsupported_data_types.
def test_like_unsupported_types(cql, test_keyspace, scylla_only):
    types = list(UNSUPPORTED_TYPES_AND_VALUES)
    columns = [f'pk_{types[0]}'] + [f'cl_{t}' for t in types[1:]]
    schema = ', '.join(f'{c} {t}' for c, t in zip(columns, types)) + f', PRIMARY KEY ({columns[0]})'
    with new_test_table(cql, test_keyspace, schema) as table:
        values = [v[0] for v in UNSUPPORTED_TYPES_AND_VALUES.values()]
        cql.execute(f'INSERT INTO {table} ({", ".join(columns)}) VALUES ({", ".join(values)})')
        for column in columns:
            for pattern in ['%', '_']:
                with pytest.raises(InvalidRequest, match='LIKE is allowed only on string type'):
                    cql.execute(f"SELECT {column} FROM {table} WHERE {column} LIKE '{pattern}'")

# Test that LIKE on a column of a non-string type is rejected also when the
# query has another LIKE on a text column, and that LIKE on the text columns
# works in a table that also has a column of each non-string type.
# Translated from scylla-dtest's like_operator_test.py::TestLikeOperatorForBaseTable::
# test_unsupported_data_types_and_supported_data_types, which created a
# separate table for each non-string type; this test uses one table with a
# column of each non-string type, to be faster, and runs the queries that
# don't involve those columns only once.
def test_like_unsupported_and_supported_types(cql, test_keyspace, scylla_only):
    types = list(UNSUPPORTED_TYPES_AND_VALUES)
    schema = 'pk text PRIMARY KEY, cl_text text, ' + ', '.join(f'cl_{t} {t}' for t in types)
    with new_test_table(cql, test_keyspace, schema) as table:
        for i in range(2):
            columns = ', '.join(f'cl_{t}' for t in types)
            values = ', '.join(v[i + 1] for v in UNSUPPORTED_TYPES_AND_VALUES.values())
            cql.execute(f"INSERT INTO {table} (pk, cl_text, {columns}) VALUES ('text_{i}', 'text%{i}', {values})")
        for t in types:
            for where in [f"cl_{t} LIKE '%'", f"cl_{t} LIKE '_'", f"cl_{t} LIKE '_' AND cl_text LIKE '%1%'"]:
                with pytest.raises(InvalidRequest, match='LIKE is allowed only on string types'):
                    select(cql, table, where)
        assert select(cql, table, "cl_text LIKE '%1%'", 'pk') == [('text_1',)]
        assert sorted(select(cql, table, "pk LIKE 'text%'", 'cl_text')) == [('text%0',), ('text%1',)]

# Test LIKE on a static column, including escaped wildcards which must match
# the "_" or "%" character itself.
# Translated from scylla-dtest's like_operator_test.py::TestLikeOperatorForBaseTable::
# test_filtering_with_like_operator_by_static_field.
def test_like_static_column(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, 'pk ascii, ck text, test varchar, cl_static text STATIC, PRIMARY KEY (pk, ck)') as table:
        rows = [(f'teststring{i}', f'{i}teststring', f'test{i}string', 'static teststring') for i in range(5)]
        for row in rows:
            cql.execute(f"INSERT INTO {table} (pk, ck, test, cl_static) VALUES ('{row[0]}', '{row[1]}', '{row[2]}', '{row[3]}')")
        columns = 'pk, ck, test, cl_static'
        for pattern in ['%', 'static%teststring', 'static teststring', 'static_teststring', '% %']:
            assert sorted(select(cql, table, f"cl_static LIKE '{pattern}'", columns)) == rows
        assert select(cql, table, "test LIKE 'test0string'", columns) == [rows[0]]
        assert select(cql, table, "cl_static LIKE 'static\\_teststring'", columns) == []
        assert select(cql, table, "cl_static LIKE 'static\\%teststring'", columns) == []

# Test LIMIT and PER PARTITION LIMIT on queries filtering with LIKE.
# Translated from scylla-dtest's like_operator_test.py::TestLikeOperatorForBaseTable::
# test_filtering_query_limiting.
def test_like_limit(cql, test_keyspace, scylla_only):
    with create_basic_table(cql, test_keyspace, 'text') as table:
        rows = [(f'teststring{j}', f'{i}teststring', f'test{i}string') for j in range(5) for i in range(2)]
        insert_rows(cql, table, rows)
        for where in ["pk LIKE 'test%' LIMIT 1", "ck LIKE '%string' LIMIT 1", "test LIKE 'test_string' LIMIT 1"]:
            result = select(cql, table, where)
            assert len(result) == 1
            assert result[0] in rows
        result = select(cql, table, "test LIKE 'test%' PER PARTITION LIMIT 1")
        assert len(result) == 5
        assert all(row in rows for row in result)
        result = select(cql, table, "test LIKE 'test%' PER PARTITION LIMIT 2")
        assert len(result) == 10
        assert all(row in rows for row in result)

# Characters used in the values and patterns of the test below.
SPECIAL_CHARS = '!@#$%^&*()-_<>?.,/\\ '

# Insert the data used by the test below: values with the wildcard
# characters "%" and "_" in them, and values with each of SPECIAL_CHARS.
def insert_special_data(cql, table):
    for i in range(5):
        cql.execute(f"INSERT INTO {table} (pk, ck, test) VALUES ('TEST%STRING{i}', '{i}Test_String', '_{{}}%')")
    for char in SPECIAL_CHARS:
        cql.execute(f"INSERT INTO {table} (pk, ck, test) VALUES ('TEST%STRING7', 'cluster_{char}', '{char}')")

# Test patterns with escaped wildcards and other special characters ("\_"
# matches only "_", and "\%" only "%"), combinations of wildcards, escaped
# wildcards and case-sensitive letters, and the empty pattern.
# Translated from scylla-dtest's like_operator_test.py::TestLikeOperatorForBaseTable
# tests test_advanced_matching_{text,ascii,varchar}_type, which had the same
# checks for each of the three types (this test doesn't check varchar,
# because it is just another name for text). Those tests also repeated four checks
# that don't depend on the special character in each iteration of the loop
# over the special characters; this test does them once.
@pytest.mark.parametrize('column_type', string_types)
def test_like_special_characters(cql, test_keyspace, column_type, scylla_only):
    with create_basic_table(cql, test_keyspace, column_type) as table:
        insert_special_data(cql, table)
        all_data = list(cql.execute(f'SELECT * FROM {table}'))
        special_data = [row for row in all_data if 'cluster' in row[1]]
        def special_row(char):
            return [('TEST%STRING7', f'cluster_{char}', char)]

        assert select(cql, table, "test LIKE '\\_'") == special_row('_')
        assert select(cql, table, "test LIKE '\\%'") == special_row('%')
        assert sorted(select(cql, table, "test LIKE '_'")) == sorted(special_data)
        assert sorted(select(cql, table, "test LIKE '%'")) == sorted(all_data)
        for char in SPECIAL_CHARS:
            # Filter by the regular column
            assert select(cql, table, f"test LIKE '\\{char}'") == special_row(char)
            # Filter by the clustering key
            assert select(cql, table, f"ck LIKE 'cluster\\_\\{char}'") == special_row(char)
            if char not in ['%', '_']:
                assert select(cql, table, f"ck LIKE 'cluster\\_{char}'") == special_row(char)
            else:
                # An unescaped wildcard matches all these rows
                assert select(cql, table, f"ck LIKE 'cluster\\_{char}'") == special_data
            assert select(cql, table, f"ck LIKE 'cluster_\\{char}'") == special_row(char)
            assert select(cql, table, f"ck LIKE 'cluster%\\{char}'") == special_row(char)

        # Case sensitivity and combinations on the partition key
        for pattern in ['TEST%', 'TEST%STRING%', 'TEST%NG_']:
            assert sorted(select(cql, table, f"pk LIKE '{pattern}'")) == sorted(all_data)
        assert select(cql, table, "pk LIKE 'test%'") == []
        assert select(cql, table, "pk LIKE 'TEST\\%STRING0'") == [('TEST%STRING0', '0Test_String', '_{}%')]
        assert select(cql, table, "pk LIKE 'TEST_STRING1'") == [('TEST%STRING1', '1Test_String', '_{}%')]

        # Case sensitivity and combinations on the clustering key
        expected_rows = sorted(row for row in all_data if 'String' in row[1])
        for pattern in ['%Test%Str%', '_Test\\_S%', '%_\\_String']:
            assert sorted(select(cql, table, f"ck LIKE '{pattern}'")) == expected_rows
        assert select(cql, table, "ck LIKE '%_\\_s%'") == []
        assert select(cql, table, "ck LIKE '_test_string'") == []
        assert select(cql, table, "ck LIKE '0Test\\_String'") == [('TEST%STRING0', '0Test_String', '_{}%')]
        assert select(cql, table, "ck LIKE '1Test_String'") == [('TEST%STRING1', '1Test_String', '_{}%')]

        # Escaped wildcards in the regular column
        expected_rows = [row for row in all_data if '_{}%' in row[2]]
        assert select(cql, table, "test like '_{}%'") == expected_rows
        assert select(cql, table, "test like '\\_{}\\%'") == expected_rows
        assert select(cql, table, "test like '\\_\\{\\}\\%'") == expected_rows
        assert select(cql, table, "test like '%'") == all_data
        assert select(cql, table, "test like ''") == []

# Test LIKE restrictions on several columns at once, and together with
# other restrictions: =, IN and >.
# Translated from scylla-dtest's like_operator_test.py::TestLikeOperatorForBaseTable::
# test_filtering_combinations_of_fields_and_wc.
def test_like_on_several_columns(cql, test_keyspace, scylla_only):
    with create_basic_table(cql, test_keyspace, 'text') as table:
        rows = [(f'teststring{j}', f'{i}teststring', f'test{i}string') for j in range(5) for i in range(2)]
        insert_rows(cql, table, rows)
        def matching(condition):
            return sorted(row for row in rows if condition(row))
        assert sorted(select(cql, table, "pk LIKE '_e%t%g_' AND ck LIKE '0%'")) == matching(lambda r: '0' in r[1])
        assert sorted(select(cql, table, "ck LIKE '_test%' AND test LIKE '%1%'")) == matching(lambda r: '1' in r[2])
        assert select(cql, table, "pk LIKE 'teststring3' AND ck LIKE '1teststring' AND test LIKE 'test1string'") == [('teststring3', '1teststring', 'test1string')]
        assert select(cql, table, "pk LIKE 'teststring3' AND ck LIKE '0teststring' AND test LIKE 'test1string'") == []
        assert select(cql, table, "pk LIKE 'teststring_' AND test LIKE 'test\\_string'") == []
        assert sorted(select(cql, table, "test LIKE '%1%' AND ck = '1teststring'")) == matching(lambda r: '1' in r[1] and '1' in r[2])
        assert select(cql, table, "pk LIKE '%1' AND test LIKE 'tes_1_tring'") == [('teststring1', '1teststring', 'test1string')]
        assert select(cql, table, "pk LIKE '%1' AND test = 'tes_1_tring'") == []
        assert select(cql, table, "pk IN ('teststring1', 'teststring0') AND ck > '0teststring' AND test LIKE 'test0string'") == []
        assert sorted(select(cql, table, "pk IN ('teststring1', 'teststring0') AND ck > '0teststring' AND test LIKE 'test_string'")) == \
            matching(lambda r: ('0' in r[0] or '1' in r[0]) and '1' in r[1] and ('1' in r[2] or '0' in r[2]))
        assert select(cql, table, "pk LIKE '%4' AND ck LIKE '_teststring' AND test LIKE '%Test%String%'") == []
        assert sorted(select(cql, table, "pk LIKE '%%' AND pk IN ('teststring1', 'teststring2')")) == [
            ('teststring1', '0teststring', 'test0string'),
            ('teststring1', '1teststring', 'test1string'),
            ('teststring2', '0teststring', 'test0string'),
            ('teststring2', '1teststring', 'test1string'),
        ]

# Test LIKE after inserting, updating and deleting rows: the empty pattern
# matches only an empty value, and stops matching it after it is updated to
# a non-empty value; deleted rows aren't matched, and newly inserted ones are.
# Translated from scylla-dtest's like_operator_test.py::TestLikeOperatorForBaseTable::
# test_filtering_after_update_values.
def test_like_after_updates(cql, test_keyspace, scylla_only):
    with create_basic_table(cql, test_keyspace, 'text') as table:
        insert_special_data(cql, table)
        cql.execute(f"INSERT INTO {table} (pk, ck, test) VALUES ('TEST%STRING6', ' ', '')")
        all_data = list(cql.execute(f'SELECT * FROM {table}'))
        assert select(cql, table, "test LIKE ''") == [('TEST%STRING6', ' ', '')]

        cql.execute(f"UPDATE {table} SET test='!@#$%^&*()?:\"\\' WHERE pk = 'TEST%STRING6' and ck = ' '")
        assert select(cql, table, "pk = 'TEST%STRING6' AND ck = ' '") == [('TEST%STRING6', ' ', '!@#$%^&*()?:"\\')]
        assert select(cql, table, "test LIKE ''") == []
        assert sorted(select(cql, table, "ck LIKE '%' AND test LIKE '%'", 'pk')) == sorted((row[0],) for row in all_data)

        cql.execute(f"DELETE FROM {table} WHERE pk IN ('TEST%STRING0', 'TEST%STRING1', 'TEST%STRING2')")
        all_data = list(cql.execute(f'SELECT * FROM {table}'))
        assert sorted(select(cql, table, "pk LIKE 'TEST\\%STRING_'", 'pk')) == sorted((row[0],) for row in all_data)

        for i in range(3):
            cql.execute(f"INSERT INTO {table} (pk, ck, test) VALUES ('qwErty{i}', 'ytrewq{i}', '')")
        all_data = list(cql.execute(f'SELECT * FROM {table}'))
        assert sorted(select(cql, table, "pk LIKE '%E%_'", 'pk')) == sorted((row[0],) for row in all_data)

# Test LIKE on long values (64 KB), with the matching part of the value at
# its beginning or its end.
# Translated from scylla-dtest's like_operator_test.py::TestLikeOperatorForBaseTable::
# test_filtering_with_long_values.
def test_like_long_values(cql, test_keyspace, scylla_only):
    with create_basic_table(cql, test_keyspace, 'text') as table:
        insert_rows(cql, table, BASIC_ROWS)
        assert sorted(select(cql, table, "pk LIKE 't%ts%g%'")) == BASIC_ROWS
        for i in range(5):
            if i % 2 == 0:
                value = 'a' * 65520 + 'findme'
            else:
                value = 'findme' + 'a' * 65520
            cql.execute(f"INSERT INTO {table} (pk, ck, test) VALUES ('teststring{i}', '{i}teststring', '{value}')")
        assert sorted(select(cql, table, "test LIKE '%me'", 'pk')) == [('teststring0',), ('teststring2',), ('teststring4',)]
        assert sorted(select(cql, table, "test LIKE '_indme%'", 'pk')) == [('teststring1',), ('teststring3',)]

# A table "buildings" with columns name (the key) and city, and five rows.
def create_buildings_table(cql, keyspace):
    return new_test_table(cql, keyspace, 'name text, city text, PRIMARY KEY (name)')

def insert_buildings(cql, table):
    for i in range(5):
        cql.execute(f"INSERT INTO {table} (name, city) VALUES ('qwerty{i}', 'ytrewq{i}')")

# The following two tests check LIKE on a materialized view of the
# buildings table whose partition key is city, on that partition key and on
# the view's clustering key, name.
# Translated from scylla-dtest's like_operator_test.py::TestLikeOperatorForMV
# tests test_filtering_mv_new_primary and test_filtering_mv_new_column.
def test_like_view_partition_key(cql, test_keyspace, scylla_only):
    with create_buildings_table(cql, test_keyspace) as table:
        with new_materialized_view(cql, table, '*', 'city, name', 'city IS NOT NULL') as view:
            insert_buildings(cql, table)
            all_rows = [(f'ytrewq{i}', f'qwerty{i}') for i in range(5)]
            for pattern in ['y%', '%q%', '%', 'ytrewq_']:
                assert sorted(select(cql, view, f"city LIKE '{pattern}'")) == all_rows
            assert select(cql, view, "city LIKE '%2'") == [all_rows[2]]
            assert select(cql, view, "city LIKE 'ytrewq3'") == [all_rows[3]]
            assert select(cql, view, "city LIKE 'ytr_wq4'") == [all_rows[4]]
            assert select(cql, view, "city LIKE ''") == []
            assert select(cql, view, "city LIKE 'Y%q_'") == []

def test_like_view_clustering_key(cql, test_keyspace, scylla_only):
    with create_buildings_table(cql, test_keyspace) as table:
        with new_materialized_view(cql, table, '*', 'city, name', 'city IS NOT NULL') as view:
            insert_buildings(cql, table)
            all_rows = [(f'ytrewq{i}', f'qwerty{i}') for i in range(5)]
            for pattern in ['q%', '%w%', '%', 'qwerty_']:
                assert sorted(select(cql, view, f"name LIKE '{pattern}'")) == all_rows
            assert select(cql, view, "name LIKE '%2'") == [all_rows[2]]
            assert select(cql, view, "name LIKE 'qwerty3'") == [all_rows[3]]
            assert select(cql, view, "name LIKE 'qw_rty4'") == [all_rows[4]]
            assert select(cql, view, "name LIKE ''") == []
            assert select(cql, view, "name LIKE 'q%T_'") == []

# The checks of the following two tests, of LIKE on an indexed column.
# Secondary indexes don't support LIKE, so these queries must filter the
# base table instead of reading the index. The tests wait for the indexes to
# be built, so that the queries run while the index is usable.
def check_like_on_indexed_city(cql, table):
    all_rows = [(f'qwerty{i}', f'ytrewq{i}') for i in range(5)]
    assert select(cql, table, "city LIKE '%2'") == [all_rows[2]]
    for pattern in ['%', 'y%', '%r%', '_trewq_']:
        assert sorted(select(cql, table, f"city LIKE '{pattern}'")) == all_rows
    assert select(cql, table, "city LIKE '_trewq2'") == [all_rows[2]]
    assert select(cql, table, "city LIKE 'ytrewq0'") == [all_rows[0]]
    assert select(cql, table, "city LIKE 'Ytrewq_'") == []
    assert select(cql, table, "city LIKE '_T%_'") == []

# Test LIKE on a column with both a global and a local secondary index.
# Translated from scylla-dtest's like_operator_test.py::TestIndexFilteringWithLike::
# test_filter_index.
def test_like_global_index(cql, test_keyspace, scylla_only):
    with create_buildings_table(cql, test_keyspace) as table:
        insert_buildings(cql, table)
        global_index, local_index = unique_name(), unique_name()
        cql.execute(f'CREATE INDEX {global_index} ON {table} (city)')
        cql.execute(f'CREATE INDEX {local_index} ON {table} ((name), city)')
        wait_for_index(cql, test_keyspace, global_index)
        wait_for_index(cql, test_keyspace, local_index)
        check_like_on_indexed_city(cql, table)

# Test LIKE on a column with a local secondary index.
# Translated from scylla-dtest's like_operator_test.py::TestIndexFilteringWithLike::
# test_filter_local_index.
def test_like_local_index(cql, test_keyspace, scylla_only):
    with create_buildings_table(cql, test_keyspace) as table:
        insert_buildings(cql, table)
        local_index = unique_name()
        cql.execute(f'CREATE INDEX {local_index} ON {table} ((name), city)')
        wait_for_index(cql, test_keyspace, local_index)
        check_like_on_indexed_city(cql, table)
