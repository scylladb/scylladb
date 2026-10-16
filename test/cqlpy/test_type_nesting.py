# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Tests that a type cannot nest collections, tuples, vectors and user types
# deeper than 32 levels. Code that walks a type recursively overflows the
# stack on a deep enough type.
#############################################################################

import pytest
from cassandra.protocol import InvalidRequest, SyntaxException
from .util import config_value_context, new_function, new_test_table, new_type

def frozen_lists(depth, inner='int'):
    """Build frozen<list<frozen<list<... inner ...>>>>, nested depth levels"""
    return 'frozen<list<' * depth + inner + '>>' * depth

# Every level has its own frozen<>, the spelling that nests deepest in the
# parser. The parser's own limit must still accept it.
def test_type_nesting_at_limit(cql, test_keyspace):
    lists_32 = frozen_lists(32)
    with new_test_table(cql, test_keyspace, f'k int PRIMARY KEY, v {lists_32}') as table:
        value = 1
        for _ in range(32):
            value = [value]
        cql.execute(cql.prepare(f'INSERT INTO {table} (k, v) VALUES (0, ?)'), [value])
        assert list(cql.execute(f'SELECT v FROM {table}')) == [(value,)]

# With a single frozen<>, 33 lists stay within the parser's own limit, so the
# check after parsing is the one that rejects this type.
def test_type_nesting_over_limit(cql, test_keyspace, scylla_only):
    lists_33 = 'frozen<' + 'list<' * 33 + 'int' + '>' * 33 + '>'
    with pytest.raises(InvalidRequest, match='nested too deeply: 33 levels, the limit is 32'):
        with new_test_table(cql, test_keyspace, f'k int PRIMARY KEY, v {lists_33}'):
            pass

# A type nested this deep overflows the parser's stack unless the parser stops
# early.
def test_type_nesting_huge(cql, test_keyspace, scylla_only):
    lists_100000 = 'list<' * 100000 + 'int' + '>' * 100000
    with pytest.raises(SyntaxException, match='type nested too deeply'):
        with new_test_table(cql, test_keyspace, f'k int PRIMARY KEY, v {lists_100000}'):
            pass

# A user type adds a level, so a chain of user types can nest deeply without
# any deep statement. The limit must cover the whole type, user types included.
def test_user_type_nesting_limit(cql, test_keyspace, scylla_only):
    too_deep = 'nested too deeply: 33 levels, the limit is 32'
    with new_type(cql, test_keyspace, '(a int)') as t:
        field = frozen_lists(30, f'frozen<{t}>')  # t + 30 lists = 31 levels
        with new_type(cql, test_keyspace, f'(a {field})') as u:  # + the user type = 32
            # A column of type u has 32 levels: allowed.
            with new_test_table(cql, test_keyspace, f'k int PRIMARY KEY, v frozen<{u}>'):
                pass
            # A user type around u has 33 levels: rejected.
            with pytest.raises(InvalidRequest, match=too_deep):
                with new_type(cql, test_keyspace, f'(a frozen<{u}>)'):
                    pass
            # A list around u has 33 levels: rejected.
            with pytest.raises(InvalidRequest, match=too_deep):
                with new_test_table(cql, test_keyspace, f'k int PRIMARY KEY, v frozen<list<frozen<{u}>>>'):
                    pass

# An ALTER TYPE that makes a type deeper must keep it within the limit, and also
# every type, table and function that contains it, since they get deeper with it.
# Otherwise a chain of user types altered one by one could nest without limit.
def test_alter_type_nesting_limit(cql, test_keyspace, scylla_only):
    # A frozen<list<int>> field gives t a second level, so everything that
    # contains t gets one level deeper.
    def add_level_to(t):
        cql.execute(f'ALTER TYPE {t} ADD b frozen<list<int>>')

    # Altered type nesting goes from 1 to 33 levels: rejected.
    with new_type(cql, test_keyspace, '(a int)') as t:
        field = frozen_lists(32)  # 32 levels
        error = f'Cannot alter type {t}: type {t} would be nested too deeply: 33 levels'
        with pytest.raises(InvalidRequest, match=error):
            cql.execute(f'ALTER TYPE {t} ADD b {field}')

    # Dependent type nesting goes from 31 to 32 levels: allowed.
    with new_type(cql, test_keyspace, '(a int)') as t:
        field = frozen_lists(29, f'frozen<{t}>')  # t + 29 lists = 30 levels
        with new_type(cql, test_keyspace, f'(a {field})'):  # + the user type = 31
            add_level_to(t)

    # Dependent type nesting goes from 32 to 33 levels: rejected.
    with new_type(cql, test_keyspace, '(a int)') as t:
        field = frozen_lists(30, f'frozen<{t}>')  # t + 30 lists = 31 levels
        with new_type(cql, test_keyspace, f'(a {field})') as dependent:  # + the user type = 32
            error = f'Cannot alter type {t}: type {dependent} would be nested too deeply: 33 levels'
            with pytest.raises(InvalidRequest, match=error):
                add_level_to(t)

    # Dependent column nesting goes from 32 to 33 levels: rejected.
    with new_type(cql, test_keyspace, '(a int)') as t:
        column_type = frozen_lists(31, f'frozen<{t}>')  # t + 31 lists = 32 levels
        with new_test_table(cql, test_keyspace, f'k int PRIMARY KEY, v {column_type}') as table:
            error = f'Cannot alter type {t}: column v of table {table} would be nested too deeply: 33 levels'
            with pytest.raises(InvalidRequest, match=error):
                add_level_to(t)

    # Dependent function argument nesting goes from 32 to 33 levels: rejected.
    with new_type(cql, test_keyspace, '(a int)') as t:
        arg_type = frozen_lists(31, f'frozen<{t}>')  # t + 31 lists = 32 levels
        function = f"(x {arg_type}) CALLED ON NULL INPUT RETURNS int LANGUAGE lua AS 'return 1;'"
        with new_function(cql, test_keyspace, function) as function_name:
            error = f'Cannot alter type {t}: function {test_keyspace}.{function_name} would be nested too deeply: 33 levels'
            with pytest.raises(InvalidRequest, match=error):
                add_level_to(t)

    # Dependent function return type nesting goes from 32 to 33 levels: rejected.
    with new_type(cql, test_keyspace, '(a int)') as t:
        return_type = frozen_lists(31, f'frozen<{t}>')  # t + 31 lists = 32 levels
        function = f"(x int) CALLED ON NULL INPUT RETURNS {return_type} LANGUAGE lua AS 'return {{}};'"
        with new_function(cql, test_keyspace, function) as function_name:
            error = f'Cannot alter type {t}: function {test_keyspace}.{function_name} would be nested too deeply: 33 levels'
            with pytest.raises(InvalidRequest, match=error):
                add_level_to(t)

# An ALTER TYPE that makes nothing deeper works, even at the limit.
def test_alter_type_at_limit(cql, test_keyspace):
    with new_type(cql, test_keyspace, '(a int)') as t:
        field = frozen_lists(30, f'frozen<{t}>')  # t + 30 lists = 31 levels
        with new_type(cql, test_keyspace, f'(a {field})') as u:  # + the user type = 32
            cql.execute(f'ALTER TYPE {t} ADD b int')
            cql.execute(f'ALTER TYPE {u} ADD b int')
            cql.execute(f'ALTER TYPE {u} RENAME a TO c')

# The limit is the max_type_nesting option, so a node can accept a deeper type,
# for example to recreate a schema created before the limit.
def test_type_nesting_limit_option(cql, test_keyspace, scylla_only):
    with config_value_context(cql, 'max_type_nesting', '33'):
        # Every level has its own frozen<>, so the parser's limit must follow too.
        lists_33 = frozen_lists(33)
        with new_test_table(cql, test_keyspace, f'k int PRIMARY KEY, v {lists_33}'):
            pass
        lists_34 = 'frozen<' + 'list<' * 34 + 'int' + '>' * 34 + '>'
        with pytest.raises(InvalidRequest, match='nested too deeply: 34 levels, the limit is 33'):
            with new_test_table(cql, test_keyspace, f'k int PRIMARY KEY, v {lists_34}'):
                pass
