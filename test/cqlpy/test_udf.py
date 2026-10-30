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

# Test that a frozen collection column can be passed to a function whose
# parameter is a (non-frozen) collection of the same type. The serialized
# value of a frozen and a non-frozen collection is the same, so there is no
# reason to refuse this. Note that an empty frozen collection is not the
# same as null, and should be passed to the function as an empty collection.
# Reproduces #13855. The translated Cassandra tests which cite #13855 (in
# aggregation_test.py and uf_types_test.py) also need other issues fixed,
# so they don't check #13855 alone.
@pytest.mark.parametrize("typ,value,empty", [("list<text>", "['a', 'b']", "[]"), ("set<text>", "{'a', 'b'}", "{}"), ("map<text, text>", "{'a': 'x', 'b': 'y'}", "{}")])
def test_udf_frozen_collection_argument(cql, test_keyspace, has_java_udf, typ, value, empty):
    if has_java_udf:
        size_body = "LANGUAGE java AS 'return val == null ? \"null\" : \"size \" + val.size();'"
    else:
        size_body = "LANGUAGE lua AS 'if val == nil then return \"null\" end local n = 0 for _ in pairs(val) do n = n + 1 end return \"size \" .. n'"
    with new_test_table(cql, test_keyspace, f"p int PRIMARY KEY, v frozen<{typ}>") as table:
        cql.execute(f"INSERT INTO {table} (p, v) VALUES (1, {value})")
        cql.execute(f"INSERT INTO {table} (p, v) VALUES (2, {empty})")
        cql.execute(f"INSERT INTO {table} (p) VALUES (3)")
        with new_function(cql, test_keyspace, f"(val {typ}) CALLED ON NULL INPUT RETURNS text {size_body}") as size:
            assert list(cql.execute(f"SELECT {test_keyspace}.{size}(v) FROM {table} WHERE p = 1")) == [("size 2",)]
            assert list(cql.execute(f"SELECT {test_keyspace}.{size}(v) FROM {table} WHERE p = 2")) == [("size 0",)]
            assert list(cql.execute(f"SELECT {test_keyspace}.{size}(v) FROM {table} WHERE p = 3")) == [("null",)]

# The opposite of test_udf_frozen_collection_argument: a non-frozen collection
# column passed to a function whose parameter is a frozen collection of the
# same type. This test is scylla_only because Cassandra doesn't allow a
# frozen<...> parameter type at all (see #13866).
# Reproduces #13855.
@pytest.mark.parametrize("typ,value", [("list<text>", "['a', 'b']"), ("set<text>", "{'a', 'b'}"), ("map<text, text>", "{'a': 'x', 'b': 'y'}")])
def test_udf_nonfrozen_collection_to_frozen_parameter(scylla_only, cql, test_keyspace, typ, value):
    size_body = "LANGUAGE lua AS 'if val == nil then return \"null\" end local n = 0 for _ in pairs(val) do n = n + 1 end return \"size \" .. n'"
    with new_test_table(cql, test_keyspace, f"p int PRIMARY KEY, v {typ}") as table:
        cql.execute(f"INSERT INTO {table} (p, v) VALUES (1, {value})")
        cql.execute(f"INSERT INTO {table} (p) VALUES (2)")
        with new_function(cql, test_keyspace, f"(val frozen<{typ}>) CALLED ON NULL INPUT RETURNS text {size_body}") as size:
            assert list(cql.execute(f"SELECT {test_keyspace}.{size}(v) FROM {table} WHERE p = 1")) == [("size 2",)]
            assert list(cql.execute(f"SELECT {test_keyspace}.{size}(v) FROM {table} WHERE p = 2")) == [("null",)]

# Test that a non-frozen UDT column can be passed to a function whose
# parameter is that UDT. Scylla treats a UDT parameter as frozen, so this is
# the same problem as #13855 (a frozen collection passed to a non-frozen
# collection parameter), in the opposite direction.
def test_udf_nonfrozen_udt_argument(cql, test_keyspace, has_java_udf):
    with new_type(cql, test_keyspace, "(a int, b text)") as udt:
        if has_java_udf:
            body = "LANGUAGE java AS 'return val == null ? \"null\" : val.getInt(\"a\") + val.getString(\"b\");'"
        else:
            body = "LANGUAGE lua AS 'if val == nil then return \"null\" end return val.a .. val.b'"
        with new_test_table(cql, test_keyspace, f"p int PRIMARY KEY, v {udt}") as table:
            cql.execute(f"INSERT INTO {table} (p, v) VALUES (1, {{a: 7, b: 'x'}})")
            cql.execute(f"INSERT INTO {table} (p) VALUES (2)")
            with new_function(cql, test_keyspace, f"(val {udt}) CALLED ON NULL INPUT RETURNS text {body}") as f:
                assert list(cql.execute(f"SELECT {test_keyspace}.{f}(v) FROM {table} WHERE p = 1")) == [("7x",)]
                assert list(cql.execute(f"SELECT {test_keyspace}.{f}(v) FROM {table} WHERE p = 2")) == [("null",)]

# Scylla (unlike Cassandra) allows a function's parameter to be a frozen
# collection, so a function can be overloaded with both a frozen and a
# non-frozen collection parameter. Since a frozen collection can be passed to
# a non-frozen parameter and vice versa (#13855), check that such a call still
# chooses the overload whose parameter type matches the argument exactly,
# instead of failing as ambiguous.
def test_udf_overload_frozen_and_nonfrozen_parameter(scylla_only, cql, test_keyspace):
    name = unique_name()
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, l list<text>, fl frozen<list<text>>") as table:
        cql.execute(f"INSERT INTO {table} (p, l, fl) VALUES (1, ['a'], ['a'])")
        with new_function(cql, test_keyspace, "(val list<text>) CALLED ON NULL INPUT RETURNS text LANGUAGE lua AS 'return \"nonfrozen\"'", name=name, args="list<text>"), \
             new_function(cql, test_keyspace, "(val frozen<list<text>>) CALLED ON NULL INPUT RETURNS text LANGUAGE lua AS 'return \"frozen\"'", name=name, args="frozen<list<text>>"):
            assert list(cql.execute(f"SELECT {test_keyspace}.{name}(l) FROM {table}")) == [("nonfrozen",)]
            assert list(cql.execute(f"SELECT {test_keyspace}.{name}(fl) FROM {table}")) == [("frozen",)]

# Like test_udf_frozen_collection_argument and
# test_udf_nonfrozen_collection_to_frozen_parameter, but the collection
# passed to the function is the result of another function call, rather than
# a column: one function returns a frozen (or non-frozen) list, and is called
# inside a function whose parameter is a non-frozen (or frozen) list. This
# test is scylla_only because Cassandra doesn't allow a frozen<...> type in a
# function's signature at all (see #13866).
# Reproduces #13855.
@pytest.mark.parametrize('inner_type,outer_type', [
    ('frozen<list<int>>', 'list<int>'),
    ('list<int>', 'frozen<list<int>>'),
], ids=['frozen_to_nonfrozen', 'nonfrozen_to_frozen'])
def test_udf_frozen_collection_nested_call(scylla_only, cql, test_keyspace, inner_type, outer_type):
    with new_test_table(cql, test_keyspace, 'p int PRIMARY KEY, v int') as table:
        cql.execute(f'INSERT INTO {table} (p, v) VALUES (1, 7)')
        with new_function(cql, test_keyspace, f"(x int) CALLED ON NULL INPUT RETURNS {inner_type} LANGUAGE lua AS 'return {{x, x}}'") as make_list, \
             new_function(cql, test_keyspace, f"(l {outer_type}) CALLED ON NULL INPUT RETURNS int LANGUAGE lua AS 'return #l'") as size:
            assert list(cql.execute(f'SELECT {test_keyspace}.{size}({test_keyspace}.{make_list}(v)) FROM {table}')) == [(2,)]
