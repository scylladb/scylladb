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
