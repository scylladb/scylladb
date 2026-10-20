# -*- coding: utf-8 -*-
# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Tests for user-defined functions (UDF)
#############################################################################

import pytest
from cassandra.protocol import InvalidRequest

from .util import new_test_keyspace, new_test_table, new_type, unique_name, new_function

# Unfortunately, while ScyllaDB and Cassandra support the same UDF
# feature, each supports a different choice of languages. In particular,
# Cassandra supports Java and ScyllaDB doesn't - but ScyllaDB supports
# Lua. The following fixture can be used to check if the server supports
# Java UDFs - and if it doesn't, the test should use Lua.
@pytest.fixture(scope="module")
def has_java_udf(cql, test_keyspace):
    try:
        with new_function(cql, test_keyspace, "(i int) CALLED ON NULL INPUT RETURNS int LANGUAGE java AS 'return 42;'"):
            return True
    except InvalidRequest as e:
        # When the language "java" is not supported, Scylla prints the
        # error message "Language 'java' is not supported". Cassandra
        # isn't expected to fail here because it does support Java.
        # Any other error here indicates an unexpected bug.
        assert "Language 'java' is not supported" in str(e)
        return False

# Test that if we have an overloaded function (i.e., two functions with the
# same name, just with different parameters), DROP FUNCTION requires
# disambiguating which function you want to drop. Without parameters, the
# DROP will be an error. On the other hand, when a function is not
# overloaded and has a unique name, it's not necessary to list the
# parameters in the DROP command.
def test_drop_overloaded_udf(cql, test_keyspace, has_java_udf):
    if has_java_udf:
        body1 = "(i int) CALLED ON NULL INPUT RETURNS bigint LANGUAGE java AS 'return 42L;'"
        body2 = "(i int, j int) CALLED ON NULL INPUT RETURNS bigint LANGUAGE java AS 'return 42L;'"
    else:
        body1 = "(i int) CALLED ON NULL INPUT RETURNS bigint LANGUAGE lua AS 'return 42;'"
        body2 = "(i int, j int) CALLED ON NULL INPUT RETURNS bigint LANGUAGE lua AS 'return 42;'"
    args1 = "int"
    args2 = "int, int"

    # Create two different functions with the same name fun, but a
    # different signature (different parameters):
    fun = unique_name()
    with new_function(cql, test_keyspace, body1, name=fun, args=args1):
        with new_function(cql, test_keyspace, body2, name=fun, args=args2):
            # Shouldn't be able to DROP "fun" without parameters, because
            # it's ambiguous.
            with pytest.raises(InvalidRequest, match="multiple function"):
                cql.execute(f"DROP FUNCTION {test_keyspace}.{fun}")
            # Should be ok to DROP one of the "fun" instances by specifying
            # its parameters explicitly
            cql.execute(f"DROP FUNCTION {test_keyspace}.{fun}({args2})")
            # Now there's only one function left with that name, so it
            # should be fine to drop "fun" without explicit parameters:
            cql.execute(f"DROP FUNCTION {test_keyspace}.{fun}")
            # The new_function() context manager will expect the functions
            # to exist when it ends, to drop them. But we just dropped them.
            # So let's recreate them to pacify new_function():
            cql.execute(f"CREATE FUNCTION {test_keyspace}.{fun} {body1}")
            cql.execute(f"CREATE FUNCTION {test_keyspace}.{fun} {body2}")

# Test that a UDF can take a frozen UDT argument and that the UDT cannot
# be dropped while the function is in use.
# Ported from scylla-dtest user_functions_test.py::test_udf_with_udt (SCYLLADB-1928).
def test_udf_with_udt(cql, test_keyspace, has_java_udf):
    with new_type(cql, test_keyspace, "(a text, b int)") as udt:
        _, udt_name = udt.split('.')
        with new_test_table(cql, test_keyspace,
                            f"key int PRIMARY KEY, udt frozen<{udt_name}>") as table:
            cql.execute(f"INSERT INTO {table} (key, udt) VALUES (1, {{a: 'un', b: 1}})")
            cql.execute(f"INSERT INTO {table} (key, udt) VALUES (2, {{a: 'deux', b: 2}})")
            cql.execute(f"INSERT INTO {table} (key, udt) VALUES (3, {{a: 'trois', b: 3}})")

            if has_java_udf:
                func_body = f"(udt {udt_name}) CALLED ON NULL INPUT RETURNS int LANGUAGE java AS 'return udt.getInt(\"b\");'"
            else:
                func_body = f"(udt {udt_name}) CALLED ON NULL INPUT RETURNS int LANGUAGE lua AS 'return udt.b'"
            with new_function(cql, test_keyspace, func_body) as funk:
                result = list(cql.execute(f"SELECT sum({test_keyspace}.{funk}(udt)) FROM {table}"))
                assert len(result) == 1
                assert result[0][0] == 6

                # UDT should not be droppable while function references it
                with pytest.raises(InvalidRequest, match="is still used by"):
                    cql.execute(f"DROP TYPE {udt}")

# Test that a UDF cannot reference a UDT from another keyspace.
# Ported from scylla-dtest user_functions_test.py::test_udf_with_udt_keyspace_isolation (SCYLLADB-1928).
# Reproduces CASSANDRA-9409.
def test_udf_with_udt_keyspace_isolation(cql, test_keyspace, has_java_udf):
    with new_type(cql, test_keyspace, "(a text, b int)") as udt:
        _, udt_name = udt.split('.')
        with new_test_keyspace(cql, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as other_ks:
            if has_java_udf:
                lang_arg = "LANGUAGE java AS 'return \"f1\";'"
                lang_ret = "LANGUAGE java AS 'return null;'"
            else:
                lang_arg = "LANGUAGE lua AS 'return \"f1\"'"
                lang_ret = "LANGUAGE lua AS 'return nil'"
            # Cannot use UDT from test_keyspace as function arg in other_ks
            with pytest.raises(InvalidRequest, match="cannot refer to a user type in keyspace"):
                cql.execute(
                    f"CREATE FUNCTION {other_ks}.overloaded(v {test_keyspace}.{udt_name}) "
                    f"CALLED ON NULL INPUT RETURNS text {lang_arg}")
            # Cannot use UDT from test_keyspace as return type in other_ks
            with pytest.raises(InvalidRequest, match="cannot refer to a user type in keyspace"):
                cql.execute(
                    f"CREATE FUNCTION {other_ks}.testfun(v text) "
                    f"CALLED ON NULL INPUT RETURNS {test_keyspace}.{udt_name} {lang_ret}")


# The name of the result column of a function call with a constant argument
# includes the argument, printed as a CQL literal. For a UDT value, a field
# name which needs quoting (e.g., a case-sensitive name) must be quoted, as in
# any CQL literal printed by Scylla. This test is scylla_only because
# Cassandra prints the literal differently, as {A: 1, b: 2}.
# Tests the fix of SCYLLADB-5270 (printing UDT values as CQL literals didn't
# quote field names).
def test_udf_udt_literal_argument_column_name(cql, test_keyspace, scylla_only):
    with new_type(cql, test_keyspace, '("A" int, b int)') as udt:
        with new_test_table(cql, test_keyspace, 'p int PRIMARY KEY') as table:
            cql.execute(f'INSERT INTO {table} (p) VALUES (1)')
            with new_function(cql, test_keyspace, f"(x {udt}) CALLED ON NULL INPUT RETURNS int LANGUAGE lua AS 'return 7'") as f:
                rs = cql.execute(f'SELECT {test_keyspace}.{f}({{"A": 1, b: 2}}) FROM {table}')
                assert rs.column_names == [f'{test_keyspace}.{f}({{"A":1, b:2}})']
                assert list(rs) == [(7,)]

# Test that after ALTER TYPE adds a field to a UDT used by a function, the
# function can still be called, and sees the new field (here, in the UDT it
# returns). Also check that the function can then be dropped and re-created.
# Reproduces #12787 (the function kept the old version of the UDT, so it
# couldn't be called, and DROP FUNCTION left it behind in memory).
def test_udf_after_alter_type(cql, test_keyspace, has_java_udf):
    body = "LANGUAGE java AS 'return x;'" if has_java_udf else "LANGUAGE lua AS 'return x'"
    with new_type(cql, test_keyspace, '(a int)') as udt:
        with new_test_table(cql, test_keyspace, f'p int PRIMARY KEY, u frozen<{udt}>') as table:
            fun = f'{test_keyspace}.{unique_name()}'
            create = f'CREATE FUNCTION {fun}(x {udt}) CALLED ON NULL INPUT RETURNS {udt} {body}'
            cql.execute(create)
            try:
                cql.execute(f'INSERT INTO {table} (p, u) VALUES (1, {{a: 1}})')
                assert [tuple(r[0]) for r in cql.execute(f'SELECT {fun}(u) FROM {table}')] == [(1,)]
                cql.execute(f'ALTER TYPE {udt} ADD b text')
                cql.execute(f"INSERT INTO {table} (p, u) VALUES (1, {{a: 2, b: 'x'}})")
                assert [tuple(r[0]) for r in cql.execute(f'SELECT {fun}(u) FROM {table}')] == [(2, 'x')]
                cql.execute(f'DROP FUNCTION {fun}')
                with pytest.raises(InvalidRequest, match='nknown function'):
                    cql.execute(f'SELECT {fun}(u) FROM {table}')
                cql.execute(create)
                assert [tuple(r[0]) for r in cql.execute(f'SELECT {fun}(u) FROM {table}')] == [(2, 'x')]
            finally:
                cql.execute(f'DROP FUNCTION IF EXISTS {fun}')

# Like test_udf_after_alter_type, but the function's parameter is a UDT
# which has a field of the altered UDT.
# Reproduces #12787.
def test_udf_after_alter_nested_type(cql, test_keyspace, has_java_udf):
    body = "LANGUAGE java AS 'return x;'" if has_java_udf else "LANGUAGE lua AS 'return x'"
    with new_type(cql, test_keyspace, '(a int)') as inner:
        with new_type(cql, test_keyspace, f'(n frozen<{inner}>)') as outer:
            with new_test_table(cql, test_keyspace, f'p int PRIMARY KEY, u frozen<{outer}>') as table:
                with new_function(cql, test_keyspace, f'(x {outer}) CALLED ON NULL INPUT RETURNS {outer} {body}') as f:
                    cql.execute(f'ALTER TYPE {inner} ADD b text')
                    cql.execute(f"INSERT INTO {table} (p, u) VALUES (1, {{n: {{a: 2, b: 'x'}}}})")
                    [result] = [r[0] for r in cql.execute(f'SELECT {test_keyspace}.{f}(u) FROM {table}')]
                    assert tuple(result.n) == (2, 'x')

# Test that after ALTER TYPE renames a field of a UDT which a function returns,
# but doesn't take as an argument, the function returns the new version of the
# type.
# Reproduces #12787.
def test_udf_returning_udt_after_alter_type(cql, test_keyspace, has_java_udf):
    if has_java_udf:
        body = "LANGUAGE java AS 'UDTValue v = udfContext.newReturnUDTValue(); v.setInt(0, x); return v;'"
    else:
        # Creating the function doesn't check the field name c, and the
        # function is only called after the field is renamed to c.
        body = "LANGUAGE lua AS 'return {c = x}'"
    with new_type(cql, test_keyspace, '(a int)') as udt:
        with new_test_table(cql, test_keyspace, 'p int PRIMARY KEY') as table:
            cql.execute(f'INSERT INTO {table} (p) VALUES (7)')
            with new_function(cql, test_keyspace, f'(x int) CALLED ON NULL INPUT RETURNS {udt} {body}') as f:
                cql.execute(f'ALTER TYPE {udt} RENAME a TO c')
                [result] = [r[0] for r in cql.execute(f'SELECT {test_keyspace}.{f}(p) FROM {table}')]
                assert tuple(result) == (7,)
                assert result._fields == ('c',)

# Test that a prepared statement calling a function uses the new version of
# the function after CREATE OR REPLACE FUNCTION replaced it.
# Reproduces SCYLLADB-5272 (the prepared statement kept using the old version).
def test_udf_prepared_after_replace(cql, test_keyspace, has_java_udf):
    def body(n):
        return f"LANGUAGE java AS 'return {n};'" if has_java_udf else f"LANGUAGE lua AS 'return {n}'"
    with new_test_table(cql, test_keyspace, 'p int PRIMARY KEY') as table:
        cql.execute(f'INSERT INTO {table} (p) VALUES (1)')
        with new_function(cql, test_keyspace, f'(x int) CALLED ON NULL INPUT RETURNS int {body(10)}') as f:
            stmt = cql.prepare(f'SELECT {test_keyspace}.{f}(p) FROM {table}')
            assert [r[0] for r in cql.execute(stmt)] == [10]
            cql.execute(f'CREATE OR REPLACE FUNCTION {test_keyspace}.{f}(x int) CALLED ON NULL INPUT RETURNS int {body(20)}')
            assert [r[0] for r in cql.execute(stmt)] == [20]

# Test that a prepared statement calling a function fails after the function
# was dropped.
# Reproduces SCYLLADB-5272 (the prepared statement kept using the dropped
# function).
def test_udf_prepared_after_drop(cql, test_keyspace, has_java_udf):
    body = "LANGUAGE java AS 'return 10;'" if has_java_udf else "LANGUAGE lua AS 'return 10'"
    with new_test_table(cql, test_keyspace, 'p int PRIMARY KEY') as table:
        cql.execute(f'INSERT INTO {table} (p) VALUES (1)')
        fun = f'{test_keyspace}.{unique_name()}'
        cql.execute(f'CREATE FUNCTION {fun}(x int) CALLED ON NULL INPUT RETURNS int {body}')
        try:
            stmt = cql.prepare(f'SELECT {fun}(p) FROM {table}')
            assert [r[0] for r in cql.execute(stmt)] == [10]
            cql.execute(f'DROP FUNCTION {fun}')
            with pytest.raises(InvalidRequest, match='nknown function'):
                cql.execute(stmt)
        finally:
            cql.execute(f'DROP FUNCTION IF EXISTS {fun}')

# Test that after ALTER TYPE renames a field of a UDT which a function
# returns, a prepared statement calling the function uses the new version of
# the type. The statement's table has no column of this type, so altering the
# type doesn't invalidate the statement because of the table.
# Reproduces SCYLLADB-5272 (the prepared statement kept using the old
# version of the function, with the old version of the type).
def test_udf_prepared_after_alter_type(cql, test_keyspace, has_java_udf):
    if has_java_udf:
        body = "LANGUAGE java AS 'UDTValue v = udfContext.newReturnUDTValue(); v.setInt(0, x); return v;'"
    else:
        # The function is only called after the field is renamed to c.
        body = "LANGUAGE lua AS 'return {c = x}'"
    with new_type(cql, test_keyspace, '(a int)') as udt:
        with new_test_table(cql, test_keyspace, 'p int PRIMARY KEY') as table:
            cql.execute(f'INSERT INTO {table} (p) VALUES (7)')
            with new_function(cql, test_keyspace, f'(x int) CALLED ON NULL INPUT RETURNS {udt} {body}') as f:
                stmt = cql.prepare(f'SELECT {test_keyspace}.{f}(p) FROM {table}')
                cql.execute(f'ALTER TYPE {udt} RENAME a TO c')
                [result] = [r[0] for r in cql.execute(stmt)]
                assert tuple(result) == (7,)
                assert result._fields == ('c',)
