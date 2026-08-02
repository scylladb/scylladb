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

# Test what happens when a UDF has the same name as a native (system-keyspace)
# function, such as tounixtimestamp(timeuuid) or token() and used unqualified
# (without an explict keyspace). The first example tounixtimestamp() is a
# "declared" builtin (stored in _declared) and the latter is a "special-cased"
# builtin (its parameters are different in every keyspace). In both cases, The
# UDF should always win - this is not considered an abiguity error, to be
# compatible with how Cassandra behaves. See SCYLLADB-2799.
# The system builtin is still reachable via system.name().

@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    schema = 'p int primary key, t timeuuid, s text'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"INSERT INTO {table} (p, t, s) VALUES (1, now(), 'hello')")
        yield table

# Case 1: UDF has the same name and same argument types as a declared builtin.
# Cassandra decided that this case is not called out as ambigous - and the UDF
# is selected over the builtin. We also check that the user can access the
# builtin via the "system" keyspace.
def test_udf_shadows_builtin_1(cql, test_keyspace, table1, has_java_udf):
    if has_java_udf:
        body = "(t timeuuid) CALLED ON NULL INPUT RETURNS bigint LANGUAGE java AS 'return 0L;'"
    else:
        body = "(t timeuuid) CALLED ON NULL INPUT RETURNS bigint LANGUAGE lua AS 'return 0'"
    with new_function(cql, test_keyspace, body, name="tounixtimestamp", args="timeuuid"):
        # Unqualified call: UDF wins, returns 0 (not the real unix timestamp)
        rows = list(cql.execute(f"SELECT tounixtimestamp(t) FROM {table1}"))
        assert rows[0][0] == 0
        # Explicit "system." reaches the builtin, and returns a non-zero timestamp
        rows = list(cql.execute(f"SELECT system.tounixtimestamp(t) FROM {table1}"))
        assert rows[0][0] != 0
        # Explicit "{test_keyspace}." qualification reaches the UDF, which returns 0
        rows = list(cql.execute(f"SELECT {test_keyspace}.tounixtimestamp(t) FROM {table1}"))
        assert rows[0][0] == 0

# Case 2: UDF has the same name as a declared builtin but with a different
# (incompatible) argument type. Either the builtin or the UDF match, depending
# on the actual argument types. An ambiguity error is not reported.
def test_udf_shadows_builtin_2(cql, test_keyspace, table1, has_java_udf):
    # text is not a valid argument for the native tounixtimestamp(timeuuid)
    if has_java_udf:
        body = "(s text) CALLED ON NULL INPUT RETURNS bigint LANGUAGE java AS 'return 0L;'"
    else:
        body = "(s text) CALLED ON NULL INPUT RETURNS bigint LANGUAGE lua AS 'return 0'"
    with new_function(cql, test_keyspace, body, name="tounixtimestamp", args="text"):
        # UDF wins for text argument, returns 0
        rows = list(cql.execute(f"SELECT tounixtimestamp(s) FROM {table1}"))
        assert rows[0][0] == 0
        # For timeuuid argument the UDF doesn't match, so the builtin is used
        rows = list(cql.execute(f"SELECT tounixtimestamp(t) FROM {table1}"))
        assert rows[0][0] != 0

# Case 3: UDF has the same name as a declared builtin but with a different
# argument count. Depending on the number of arguments actually passed,
# either the builtin or the UDF match. An ambiguity error is not reported.
def test_udf_shadows_builtin_3(cql, test_keyspace, table1, has_java_udf):
    # native tounixtimestamp takes one argument; UDF takes two
    if has_java_udf:
        body = "(t timeuuid, i int) CALLED ON NULL INPUT RETURNS bigint LANGUAGE java AS 'return 0L;'"
    else:
        body = "(t timeuuid, i int) CALLED ON NULL INPUT RETURNS bigint LANGUAGE lua AS 'return 0'"
    with new_function(cql, test_keyspace, body, name="tounixtimestamp", args="timeuuid, int"):
        # UDF wins and returns 0; the one-argument builtin would not match two args
        rows = list(cql.execute(f"SELECT tounixtimestamp(t, 0) FROM {table1}"))
        assert rows[0][0] == 0
        # With one argument the UDF doesn't match, so the builtin is used
        rows = list(cql.execute(f"SELECT tounixtimestamp(t) FROM {table1}"))
        assert rows[0][0] != 0

# Case 4: Like Case 1, but with a "special-cased" builtin token() instead of a
# "declared" builtin tounixtimestamp(). Also in this case the UDF should win
# when called unqualified with a keyspace name (it's not considered ambigous)
# and the builtin is still reachable via system.token().
def test_udf_shadows_builtin_4(cql, test_keyspace, table1, has_java_udf):
    if has_java_udf:
        body = "(p int) CALLED ON NULL INPUT RETURNS bigint LANGUAGE java AS 'return 42L;'"
    else:
        body = "(p int) CALLED ON NULL INPUT RETURNS bigint LANGUAGE lua AS 'return 42'"
    # Read the real partition token before the UDF shadows the name
    real_token = list(cql.execute(f"SELECT token(p) FROM {table1}"))[0][0]
    with new_function(cql, test_keyspace, body, name="token", args="int"):
        # Unqualified token(p): UDF wins, returns 42
        rows = list(cql.execute(f"SELECT token(p) FROM {table1}"))
        assert rows[0][0] == 42
        # Explicit ks. qualification also reaches the UDF, returns 42
        rows = list(cql.execute(f"SELECT {test_keyspace}.token(p) FROM {table1}"))
        assert rows[0][0] == 42
        # system.token(p) reaches the builtin, returns the real partition token
        rows = list(cql.execute(f"SELECT system.token(p) FROM {table1}"))
        assert rows[0][0] == real_token

# Case 5: The builtin is an EXACT_MATCH and the UDF is only WEAKLY_ASSIGNABLE —
# in this case the builtin must win, not the UDF.
# "Weak matching" (WEAKLY_ASSIGNABLE) arises when an argument is assignable to a
# parameter type, but not an exact match.  A typed column value is EXACT for its
# own type and WEAK for a value-compatible type.
# In this test we'll have a UDF tounixtimestamp(uuid) and builtin
# tounixtimestamp(timeuuid). The types uuid and timeuuid are value-compatible
# (both share the same wire encoding), so passing a timeuuid column to a
# uuid parameter yields WEAKLY_ASSIGNABLE, while passing it to a timeuuid
# parameter yields EXACT_MATCH.
def test_udf_shadows_builtin_5(cql, test_keyspace, table1, has_java_udf):
    if has_java_udf:
        body = "(u uuid) CALLED ON NULL INPUT RETURNS bigint LANGUAGE java AS 'return 99L;'"
    else:
        body = "(u uuid) CALLED ON NULL INPUT RETURNS bigint LANGUAGE lua AS 'return 99'"
    with new_function(cql, test_keyspace, body, name="tounixtimestamp", args="uuid"):
        # Since t is timeuuid, udf(t) is WEAKLY_ASSIGNABLE, builtin(t) is
        # EXACT_MATCH.  Builtin wins; it returns the real unix timestamp, not 99.
        rows = list(cql.execute(f"SELECT tounixtimestamp(t) FROM {table1}"))
        assert rows[0][0] != 99

# Case 6: Both the builtin and UDF are only WEAKLY_ASSIGNABLE - the call
# should be deemed ambiguous and refused.  This is in contrast to the EXACT
# match case (test 1) where the UDF wins without ambiguity.
# We use NULL as the argument: null is WEAKLY_ASSIGNABLE to any non-counter
# type in both Cassandra and Scylla, so both intasblob(int) and intasblob(bigint)
# are weak candidates - so we get an ambiguity error.
# Note: an integer literal like 42 would NOT work for this test, because
# Cassandra assigns it the preferred type int (EXACT for the builtin), so the
# builtin would win without ambiguity. Scylla lacks the preferred-type mechanism
# so 42 would be WEAK for both there.
def test_udf_shadows_builtin_6(cql, test_keyspace, table1, has_java_udf):
    if has_java_udf:
        body = "(x bigint) CALLED ON NULL INPUT RETURNS blob LANGUAGE java AS 'return null;'"
    else:
        body = "(x bigint) CALLED ON NULL INPUT RETURNS blob LANGUAGE lua AS 'return nil'"
    with new_function(cql, test_keyspace, body, name="intasblob", args="bigint"):
        with pytest.raises(InvalidRequest, match="Ambiguous"):
            cql.execute(f"SELECT intasblob(null) FROM {table1}")

# Case 7: The UDF does not match because of wrong argument count, and the builtin
# does not match because of wrong argument type.  The error should say "none of its
# type signatures match", not "Invalid number of arguments" (which would only be
# correct if there were a single candidate and it had the wrong count).
def test_udf_shadows_builtin_7(cql, test_keyspace, table1, has_java_udf):
    # UDF tounixtimestamp(timeuuid, int) takes two arguments; builtin takes
    # one (there are actually three variants of this builtin, taking type
    # timeuuid, timestamp, or date). If we call this function with a single
    # text column, neither the UDF nor the builtin should match. The error
    # should just generally say "none of its type signatures match" (not
    # something more specific like "Invalid number of arguments" because the
    # two non-matches happened because of different causes).
    if has_java_udf:
        body = "(t timeuuid, i int) CALLED ON NULL INPUT RETURNS bigint LANGUAGE java AS 'return 0L;'"
    else:
        body = "(t timeuuid, i int) CALLED ON NULL INPUT RETURNS bigint LANGUAGE lua AS 'return 0'"
    with new_function(cql, test_keyspace, body, name="tounixtimestamp", args="timeuuid, int"):
        with pytest.raises(InvalidRequest, match="none of its type signatures match"):
            cql.execute(f"SELECT tounixtimestamp(s) FROM {table1}")

# Case 8: The special-cased builtin token(int) is EXACT_MATCH for an int
# partition key column, but a UDF token(blob) is only WEAKLY_ASSIGNABLE (because
# blob is value-compatible with every type). In this case the builtin should win
# over the UDF, just as a declared builtin exact match beats a UDF weak match
# (see test 5).
# Unfortunately, this obscure case currently fails on Scylla. We deliberately
# decided that a weakly matching UDF should win over a special-cased builtin,
# even when the builtin is an exact match - even though this is not what
# Cassandra does - and this is what causes this test to fail.
# Why did we deviate from Cassandra here? Because we wanted the Cassandra test
#  testCreatingUDFWithSameNameAsBuiltin_PrefersCompatibleArgs_SameKeyspace
# to pass. That test uses token(10) with a CQL constant. In Cassandra, the
# CQL constant is considered an exact match for the UDF token(double), while in
# Scylla it's considered a weak match. If we want this weak match to win over
# the builtin, we need to deviate from Cassandra's behavior :-(
@pytest.mark.xfail(reason="Scylla lets a weakly-matching UDF beat an exact-match special-cased builtin (token)")
def test_udf_shadows_builtin_8(cql, test_keyspace, table1, has_java_udf):
    if has_java_udf:
        body = "(b blob) CALLED ON NULL INPUT RETURNS bigint LANGUAGE java AS 'return 99L;'"
    else:
        body = "(b blob) CALLED ON NULL INPUT RETURNS bigint LANGUAGE lua AS 'return 99'"
    real_token = list(cql.execute(f"SELECT token(p) FROM {table1}"))[0][0]
    with new_function(cql, test_keyspace, body, name="token", args="blob"):
        # UDF token(blob) is WEAKLY_ASSIGNABLE for int column p.
        # Builtin token(int) is EXACT_MATCH for p.
        # The builtin should win, returning the real partition token, not 99.
        rows = list(cql.execute(f"SELECT token(p) FROM {table1}"))
        assert rows[0][0] == real_token

