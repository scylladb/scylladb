# -*- coding: utf-8 -*-
# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Tests for user-defined functions (UDF)
#############################################################################

import pytest
from cassandra.protocol import InvalidRequest
from cassandra.query import BoundStatement

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

# An int column can hold an "empty" (zero-length) value, which is not the
# same as null. When such an empty value is passed to a user-defined
# function, the function should receive it as null - and a function declared
# RETURNS NULL ON NULL INPUT should return null without being called. The
# same applies to other types for which the empty value isn't an ordinary
# value of the type, but not to text, where the empty value is an ordinary
# empty string. Reproduces SCYLLADB-5183 (passing an empty value of type int
# to a function caused a server error).
# Note that the Python driver returns None both for a null int and for an
# empty int, so we don't check the function's int results directly: The
# functions below return text, and to check an int result we convert it to a
# blob with intAsBlob(), which is empty for an empty int and null for a null.
@pytest.mark.parametrize("typ", ["int", "bigint", "varint", "float", "double", "decimal", "boolean", "timestamp", "uuid", "timeuuid", "inet", "text"])
def test_udf_empty_argument(cql, test_keyspace, has_java_udf, typ):
    if has_java_udf:
        is_null_body = "LANGUAGE java AS 'return val == null ? \"null\" : \"not null\";'"
        called_body = "LANGUAGE java AS 'return \"called\";'"
        return_val_body = "LANGUAGE java AS 'return val;'"
    else:
        is_null_body = "LANGUAGE lua AS 'if val == nil then return \"null\" else return \"not null\" end'"
        called_body = "LANGUAGE lua AS 'return \"called\"'"
        return_val_body = "LANGUAGE lua AS 'return val'"
    with new_test_table(cql, test_keyspace, f"p int PRIMARY KEY, v {typ}") as table:
        blob_as = "blobAs" + typ.capitalize()
        cql.execute(f"INSERT INTO {table} (p, v) VALUES (1, {blob_as}(0x))")
        # Check that v is really empty, not null.
        assert list(cql.execute(f"SELECT {typ}AsBlob(v) FROM {table}")) == [(b'',)]
        # For text, the empty value is an ordinary value, not a null.
        expect_null = typ != "text"
        with new_function(cql, test_keyspace, f"(val {typ}) CALLED ON NULL INPUT RETURNS text {is_null_body}") as is_null, \
             new_function(cql, test_keyspace, f"(val {typ}) RETURNS NULL ON NULL INPUT RETURNS text {called_body}") as called, \
             new_function(cql, test_keyspace, f"(val {typ}) CALLED ON NULL INPUT RETURNS {typ} {return_val_body}") as return_val:
            assert list(cql.execute(f"SELECT {test_keyspace}.{is_null}(v) FROM {table}")) == [("null" if expect_null else "not null",)]
            assert list(cql.execute(f"SELECT {test_keyspace}.{called}(v) FROM {table}")) == [(None if expect_null else "called",)]
            assert list(cql.execute(f"SELECT {typ}AsBlob({test_keyspace}.{return_val}(v)) FROM {table}")) == [(None if expect_null else b'',)]

# Like test_udf_empty_argument, but for a counter argument. A counter column
# can't hold an empty value, so we bind an empty value to the function's
# argument instead.
# Reproduces SCYLLADB-5183 (passing an empty counter value caused a server
# error).
def test_udf_empty_counter_argument(cql, test_keyspace, has_java_udf):
    if has_java_udf:
        is_null_body = "LANGUAGE java AS 'return val == null ? \"null\" : \"not null\";'"
    else:
        is_null_body = "LANGUAGE lua AS 'if val == nil then return \"null\" else return \"not null\" end'"
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, c counter") as table:
        cql.execute(f"UPDATE {table} SET c = c + 1 WHERE p = 1")
        with new_function(cql, test_keyspace, f"(val counter) CALLED ON NULL INPUT RETURNS text {is_null_body}") as is_null:
            stmt = cql.prepare(f"SELECT {test_keyspace}.{is_null}(?) FROM {table}")
            # The Python driver can't send an empty value for a counter bind
            # variable, so we send the raw serialized value ourselves.
            bound = BoundStatement(stmt)
            bound.values = [b'']
            assert list(cql.execute(bound)) == [("null",)]

# Like test_udf_empty_argument, but the empty int is nested in the function's
# argument - as a list element, a tuple component or a map value. The
# function should receive it as a null element, as Cassandra does. The test
# also checks that the element before it, which isn't empty, isn't null.
# Reproduces SCYLLADB-5183 (an empty int nested in a function's argument
# caused a server error).
@pytest.mark.parametrize('table_type,fn_type,value,first,second', [
    ('list<int>', 'list<int>', '[1, blobAsInt(0x), 3]',
     ('val.get(0) == null', 'val[1] == nil'), ('val.get(1) == null', 'val[2] == nil')),
    ('frozen<tuple<int, int>>', 'tuple<int, int>', '(1, blobAsInt(0x))',
     ('val.getObject(0) == null', 'val[1] == nil'), ('val.getObject(1) == null', 'val[2] == nil')),
    ('map<int, int>', 'map<int, int>', '{1: 1, 2: blobAsInt(0x)}',
     ('val.get(1) == null', 'val[1] == nil'), ('val.get(2) == null', 'val[2] == nil')),
], ids=['list', 'tuple', 'map_value'])
def test_udf_empty_nested_argument(cql, test_keyspace, has_java_udf, table_type, fn_type, value, first, second):
    def is_null_body(cond):
        if has_java_udf:
            return f"LANGUAGE java AS 'return {cond[0]} ? \"null\" : \"not null\";'"
        return f"LANGUAGE lua AS 'if {cond[1]} then return \"null\" else return \"not null\" end'"
    with new_test_table(cql, test_keyspace, f'p int PRIMARY KEY, v {table_type}') as table:
        cql.execute(f'INSERT INTO {table} (p, v) VALUES (1, {value})')
        with new_function(cql, test_keyspace, f'(val {fn_type}) CALLED ON NULL INPUT RETURNS text {is_null_body(first)}') as first_is_null, \
             new_function(cql, test_keyspace, f'(val {fn_type}) CALLED ON NULL INPUT RETURNS text {is_null_body(second)}') as second_is_null:
            assert list(cql.execute(f'SELECT {test_keyspace}.{first_is_null}(v), {test_keyspace}.{second_is_null}(v) FROM {table}')) == [('not null', 'null')]

# An empty int can also be a set element or a map key. A Lua function receives
# a set or a map as a Lua table whose keys are these values, but nil can't be
# a key of a Lua table, so such an element is left out of the table. This test
# is scylla_only because it checks the behavior of Lua functions - in
# Cassandra, a Java function receives a set or map with a null element.
# Reproduces SCYLLADB-5183 (an empty int nested in a function's argument
# caused a server error).
@pytest.mark.parametrize('typ,value', [
    ('set<int>', '{1, blobAsInt(0x)}'),
    ('map<int, int>', '{blobAsInt(0x): 1, 2: 2}'),
], ids=['set', 'map_key'])
def test_udf_empty_nested_set_element_or_map_key(cql, test_keyspace, scylla_only, typ, value):
    count_body = "LANGUAGE lua AS 'local n = 0 for k, v in pairs(val) do n = n + 1 end return n'"
    with new_test_table(cql, test_keyspace, f'p int PRIMARY KEY, v {typ}') as table:
        cql.execute(f'INSERT INTO {table} (p, v) VALUES (1, {value})')
        with new_function(cql, test_keyspace, f'(val {typ}) CALLED ON NULL INPUT RETURNS int {count_body}') as count:
            assert list(cql.execute(f'SELECT {test_keyspace}.{count}(v) FROM {table}')) == [(1,)]
