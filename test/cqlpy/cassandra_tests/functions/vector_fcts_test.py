# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 4ab8bac4a51f8aef0d55b2497699e1291baeda4b
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0
#
# Modifications: Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

from ..porting import *
import re

# The Java test is parameterized by the similarity function, and checks the
# results against the implementation of the same functions in the jvector
# library (VectorSimilarityFunction.compare()). We loop over the functions
# instead, and compute their results here with the same formulas:
# cosine:      (1 + cos(a, b)) / 2
# euclidean:   1 / (1 + |a - b|^2)
# dot product: (1 + a.b) / 2
# The functions return a 32-bit float.
def cosine(a, b):
    dot = sum(x * y for x, y in zip(a, b))
    norms = (sum(x * x for x in a) * sum(y * y for y in b)) ** 0.5
    return to_float((1 + dot / norms) / 2)

def euclidean(a, b):
    return to_float(1 / (1 + sum((x - y) ** 2 for x, y in zip(a, b))))

def dot_product(a, b):
    return to_float((1 + sum(x * y for x, y in zip(a, b))) / 2)

# Some error messages are worded slightly differently in Scylla and in
# Cassandra, so we accept both:
# Cassandra: "Function f requires a float vector argument, but found argument l of type list<float>"
# Scylla:    "Function f requires a float vector argument, but found l of type list<float>"
def wrong_type(function, what):
    return re.escape("Function " + function + " requires a float vector argument, but found ") + "(argument )?" + re.escape(what)

# Cassandra: "Cannot infer type of argument NULL in call to function f"
# Scylla:    "Cannot infer type of argument null for function f(vector<float, n>, vector<float, n>)"
def cannot_infer(function, arg):
    return "Cannot infer type of argument (" + re.escape(arg) + "|" + re.escape(arg.lower()) + ") (in call to|for) function " + re.escape(function)

# Cassandra: "Type error: ['a', 'b'] cannot be passed as argument 1"
# Scylla:    "Function f requires a float vector argument, but found ['a', 'b'] of type frozen<list<text>>"
def not_assignable(function, n):
    return re.escape("Type error: ['a', 'b'] cannot be passed as argument " + str(n)) + "|" + wrong_type(function, "['a', 'b'] of type frozen<list<text>>")

# Cassandra: "Function f requires a float vector argument, but found argument v_int of type vector<int, 2>"
# Scylla:    "Type error: v_int cannot be passed as argument 0 of function f of type vector<float, 2>"
def wrong_vector_type(function, column, typ, n):
    return wrong_type(function, column + " of type " + typ) + "|" + re.escape("Type error: " + column + " cannot be passed as argument " + str(n) + " of function " + function)

def testVectorSimilarityFunction(cql, test_keyspace):
    for function, luceneFunction in [("system.similarity_cosine", cosine),
                                     ("system.similarity_euclidean", euclidean),
                                     ("system.similarity_dot_product", dot_product)]:
        with create_table(cql, test_keyspace, "(pk int PRIMARY KEY, value vector<float, 2>, " +
                              "l list<float>, " + # lists shouldn't be accepted by the functions
                              "fl frozen<list<float>>, " + # frozen lists shouldn't be accepted by the functions
                              "v1 vector<float, 1>, " + # 1-dimension vector to test missmatching dimensions
                              "v_int vector<int, 2>, " + # int vectors shouldn't be accepted by the functions
                              "v_double vector<double, 2>)") as table: # double vectors shouldn't be accepted by the functions

            values = [1.0, 2.0]
            vector = values
            similarity = row(luceneFunction(values, values))

            # basic functionality
            execute(cql, table, "INSERT INTO %s (pk, value, l, fl, v1, v_int, v_double) VALUES (0, ?, ?, ?, ?, ?, ?)",
                    vector, [1.0, 2.0], [1.0, 2.0], [1.0], [1, 2], [1.0, 2.0])
            assertRows(execute(cql, table, "SELECT " + function + "(value, value) FROM %s"), similarity)

            # literals
            assertRows(execute(cql, table, "SELECT " + function + "(value, [1, 2]) FROM %s"), similarity)
            assertRows(execute(cql, table, "SELECT " + function + "([1, 2], value) FROM %s"), similarity)
            assertRows(execute(cql, table, "SELECT " + function + "([1, 2], [1, 2]) FROM %s"), similarity)

            # bind markers
            assertRows(execute(cql, table, "SELECT " + function + "(value, ?) FROM %s", vector), similarity)
            assertRows(execute(cql, table, "SELECT " + function + "(?, value) FROM %s", vector), similarity)
            assertInvalidMessage(cql, table, "Cannot infer type of argument ?",
                                 "SELECT " + function + "(?, ?) FROM %s", vector, vector)

            # The checks of "bind markers with type hints" are in
            # testVectorSimilarityFunctionWithTypeHints below.

            # bind markers and literals
            assertRows(execute(cql, table, "SELECT " + function + "([1, 2], ?) FROM %s", vector), similarity)
            assertRows(execute(cql, table, "SELECT " + function + "(?, [1, 2]) FROM %s", vector), similarity)
            assertRows(execute(cql, table, "SELECT " + function + "([1, 2], ?) FROM %s", vector), similarity)

            # wrong column types with columns
            assertInvalidMessageRE(cql, table, wrong_type(function, "l of type list<float>"),
                                 "SELECT " + function + "(l, value) FROM %s")
            assertInvalidMessageRE(cql, table, wrong_type(function, "fl of type frozen<list<float>>"),
                                 "SELECT " + function + "(fl, value) FROM %s")
            assertInvalidMessageRE(cql, table, wrong_type(function, "l of type list<float>"),
                                 "SELECT " + function + "(value, l) FROM %s")
            assertInvalidMessageRE(cql, table, wrong_type(function, "fl of type frozen<list<float>>"),
                                 "SELECT " + function + "(value, fl) FROM %s")

            # wrong column types with columns and literals
            assertInvalidMessageRE(cql, table, wrong_type(function, "l of type list<float>"),
                                 "SELECT " + function + "(l, [1, 2]) FROM %s")
            assertInvalidMessageRE(cql, table, wrong_type(function, "fl of type frozen<list<float>>"),
                                 "SELECT " + function + "(fl, [1, 2]) FROM %s")
            assertInvalidMessageRE(cql, table, wrong_type(function, "l of type list<float>"),
                                 "SELECT " + function + "([1, 2], l) FROM %s")
            assertInvalidMessageRE(cql, table, wrong_type(function, "fl of type frozen<list<float>>"),
                                 "SELECT " + function + "([1, 2], fl) FROM %s")

            # The checks of "wrong column types with cast literals" are in
            # testVectorSimilarityFunctionWithTypeHints below.

            # wrong non-float vectors
            assertInvalidMessageRE(cql, table, wrong_vector_type(function, "v_int", "vector<int, 2>", 0),
                                 "SELECT " + function + "(v_int, [1, 2]) FROM %s")
            assertInvalidMessageRE(cql, table, wrong_vector_type(function, "v_double", "vector<double, 2>", 0),
                                 "SELECT " + function + "(v_double, [1, 2]) FROM %s")
            assertInvalidMessageRE(cql, table, wrong_vector_type(function, "v_int", "vector<int, 2>", 1),
                                 "SELECT " + function + "([1, 2], v_int) FROM %s")
            assertInvalidMessageRE(cql, table, wrong_vector_type(function, "v_double", "vector<double, 2>", 1),
                                 "SELECT " + function + "([1, 2], v_double) FROM %s")

            # mismatching dimensions with literals
            # (the Java test passes an unused bind value to these queries,
            # which the Python driver doesn't allow, so we don't)
            assertInvalidMessage(cql, table, "All arguments must have the same vector dimensions",
                                 "SELECT " + function + "([1, 2], [3]) FROM %s")
            assertInvalidMessage(cql, table, "All arguments must have the same vector dimensions",
                                 "SELECT " + function + "(value, [1]) FROM %s")
            assertInvalidMessage(cql, table, "All arguments must have the same vector dimensions",
                                 "SELECT " + function + "([1], value) FROM %s")

            # The checks of "mismatching dimensions with bind markers" are in
            # testVectorSimilarityFunctionWithTypeHints below.

            # mismatching dimensions with columns
            assertInvalidMessage(cql, table, "All arguments must have the same vector dimensions",
                                 "SELECT " + function + "(value, v1) FROM %s")
            assertInvalidMessage(cql, table, "All arguments must have the same vector dimensions",
                                 "SELECT " + function + "(v1, value) FROM %s")

            # null arguments with literals
            assertRows(execute(cql, table, "SELECT " + function + "(value, null) FROM %s"), row(None))
            assertRows(execute(cql, table, "SELECT " + function + "(null, value) FROM %s"), row(None))
            assertInvalidMessageRE(cql, table, cannot_infer(function, "NULL"),
                                 "SELECT " + function + "(null, null) FROM %s")

            # null arguments with bind markers
            assertRows(execute(cql, table, "SELECT " + function + "(value, ?) FROM %s", None), row(None))
            assertRows(execute(cql, table, "SELECT " + function + "(?, value) FROM %s", None), row(None))
            assertInvalidMessageRE(cql, table, cannot_infer(function, "?"),
                                 "SELECT " + function + "(?, ?) FROM %s", None, None)

            # test all-zero vectors, only cosine similarity should reject them
            if luceneFunction == cosine:
                # Scylla deliberately returns NaN for the cosine similarity
                # of an all-zero vector, instead of an error - see
                # test_vector_similarity.py::test_vector_similarity_cosine_with_zero_vectors
                # So these checks are commented out.
                #expected = "Function " + function + " doesn't support all-zero vectors"
                #assertInvalidMessage(cql, table, expected, "SELECT " + function + "(value, [0, 0]) FROM %s")
                #assertInvalidMessage(cql, table, expected, "SELECT " + function + "([0, 0], value) FROM %s")
                pass
            else:
                expected = luceneFunction(values, [0, 0])
                assertRows(execute(cql, table, "SELECT " + function + "(value, [0, 0]) FROM %s"), row(expected))
                assertRows(execute(cql, table, "SELECT " + function + "([0, 0], value) FROM %s"), row(expected))

            # not-assignable element types
            assertInvalidMessageRE(cql, table, not_assignable(function, 1),
                                 "SELECT " + function + "(value, ['a', 'b']) FROM %s WHERE pk=0")
            assertInvalidMessageRE(cql, table, not_assignable(function, 0),
                                 "SELECT " + function + "(['a', 'b'], value) FROM %s WHERE pk=0")
            assertInvalidMessageRE(cql, table, not_assignable(function, 0),
                                 "SELECT " + function + "(['a', 'b'], ['a', 'b']) FROM %s WHERE pk=0")

# The steps of the Java test that use type hints, such as
# "(vector<float, 2>) ?" and "(List<Float>)[1, 2]", in the arguments of the
# similarity functions. Scylla doesn't support type hints for non-native
# types in the selection clause yet, so these steps are in a separate test,
# which is xfail, while the rest of testVectorSimilarityFunction runs.
# Reproduces #5411 (terms, such as type hints, in the selection clause)
@pytest.mark.xfail(reason="#5411")
def testVectorSimilarityFunctionWithTypeHints(cql, test_keyspace):
    for function, luceneFunction in [("system.similarity_cosine", cosine),
                                     ("system.similarity_euclidean", euclidean),
                                     ("system.similarity_dot_product", dot_product)]:
        with create_table(cql, test_keyspace, "(pk int PRIMARY KEY, value vector<float, 2>, " +
                              "l list<float>, " + # lists shouldn't be accepted by the functions
                              "fl frozen<list<float>>, " + # frozen lists shouldn't be accepted by the functions
                              "v1 vector<float, 1>, " + # 1-dimension vector to test missmatching dimensions
                              "v_int vector<int, 2>, " + # int vectors shouldn't be accepted by the functions
                              "v_double vector<double, 2>)") as table: # double vectors shouldn't be accepted by the functions

            values = [1.0, 2.0]
            vector = values
            similarity = row(luceneFunction(values, values))

            execute(cql, table, "INSERT INTO %s (pk, value, l, fl, v1, v_int, v_double) VALUES (0, ?, ?, ?, ?, ?, ?)",
                    vector, [1.0, 2.0], [1.0, 2.0], [1.0], [1, 2], [1.0, 2.0])

            # bind markers with type hints
            assertRows(execute(cql, table, "SELECT " + function + "((vector<float, 2>) ?, ?) FROM %s", vector, vector), similarity)
            assertRows(execute(cql, table, "SELECT " + function + "(?, (vector<float, 2>) ?) FROM %s", vector, vector), similarity)
            assertRows(execute(cql, table, "SELECT " + function + "((vector<float, 2>) ?, (vector<float, 2>) ?) FROM %s", vector, vector), similarity)

            # wrong column types with cast literals
            assertInvalidMessageRE(cql, table, wrong_type(function, "(list<float>)[1, 2] of type frozen<list<float>>"),
                                 "SELECT " + function + "((List<Float>)[1, 2], [3, 4]) FROM %s")
            assertInvalidMessageRE(cql, table, wrong_type(function, "(list<float>)[1, 2] of type frozen<list<float>>"),
                                 "SELECT " + function + "((List<Float>)[1, 2], (List<Float>)[3, 4]) FROM %s")
            assertInvalidMessageRE(cql, table, wrong_type(function, "(list<float>)[3, 4] of type frozen<list<float>>"),
                                 "SELECT " + function + "([1, 2], (List<Float>)[3, 4]) FROM %s")

            # mismatching dimensions with bind markers
            assertInvalidMessage(cql, table, "All arguments must have the same vector dimensions",
                                 "SELECT " + function + "((vector<float, 1>) ?, value) FROM %s", [1.0])
            assertInvalidMessage(cql, table, "All arguments must have the same vector dimensions",
                                 "SELECT " + function + "(value, (vector<float, 1>) ?) FROM %s", [1.0])
            assertInvalidMessage(cql, table, "All arguments must have the same vector dimensions",
                                 "SELECT " + function + "((vector<float, 2>) ?, (vector<float, 1>) ?) FROM %s", [1.0, 2.0], [1.0])
