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
from cassandra.util import Duration
import time

# Notes on the translation of expected results with nested collections:
# The driver returns set and map columns as special types, which assert_rows
# converts to Python sets and dicts, converting their elements (or keys) to
# hashable types: a list becomes a tuple, a set or a map becomes a frozenset
# (a map becomes a frozenset of its key-value pairs), and a UDT becomes a
# tuple. So the expected results below use these types inside sets, and for
# map keys.

# Scylla's error message for a term whose type can't be inferred is "Could not
# infer type of ...", so we accept either message.
CANNOT_INFER = "Cannot infer type for term|Could not infer type of"

def fz(*items):
    return frozenset(items)

# Helper method for testSelectLiteral()
def assertConstantResult(result, constant):
    assert_rows(result,
                row(1, "one", constant),
                row(2, "two", constant),
                row(3, "three", constant))

# Reproduces #5411 (terms, such as type hints and collection literals, in the
# selection clause)
@pytest.mark.xfail(reason="#5411")
def testSelectLiteral(cql, test_keyspace):
    timestampInMicros = int(time.time() * 1000) * 1000
    with create_table(cql, test_keyspace, "(pk int, ck int, t text, PRIMARY KEY (pk, ck) )") as table:
        execute(cql, table, "INSERT INTO %s (pk, ck, t) VALUES (?, ?, ?) USING TIMESTAMP ?", 1, 1, "one", timestampInMicros)
        execute(cql, table, "INSERT INTO %s (pk, ck, t) VALUES (?, ?, ?) USING TIMESTAMP ?", 1, 2, "two", timestampInMicros)
        execute(cql, table, "INSERT INTO %s (pk, ck, t) VALUES (?, ?, ?) USING TIMESTAMP ?", 1, 3, "three", timestampInMicros)

        # Scylla deliberately infers the type of a literal in the selection
        # clause (SCYLLADB-1229, see test_selector_literals.py::
        # test_simple_literal_type_inference), so it doesn't reject it like
        # Cassandra does:
        # assert_invalid_message_re(cql, table, CANNOT_INFER, "SELECT ck, t, 'a const' FROM %s")
        assertConstantResult(execute(cql, table, "SELECT ck, t, (text)'a const' FROM %s"), "a const")

        # Scylla deliberately infers the type of a literal in the selection
        # clause (SCYLLADB-1229, see test_selector_literals.py::
        # test_simple_literal_type_inference), so it doesn't reject it like
        # Cassandra does:
        # assert_invalid_message_re(cql, table, CANNOT_INFER, "SELECT ck, t, 42 FROM %s")
        assertConstantResult(execute(cql, table, "SELECT ck, t, (smallint)42 FROM %s"), 42)

        # Scylla deliberately infers the type of a literal in the selection
        # clause (SCYLLADB-1229, see test_selector_literals.py::
        # test_simple_literal_type_inference), so it doesn't reject it like
        # Cassandra does:
        # assert_invalid_message_re(cql, table, CANNOT_INFER, "SELECT ck, t, (1, 'foo') FROM %s")
        assertConstantResult(execute(cql, table, "SELECT ck, t, (tuple<int, text>)(1, 'foo') FROM %s"), (1, "foo"))

        # Scylla deliberately infers the type of a literal in the selection
        # clause (SCYLLADB-1229, see test_selector_literals.py::
        # test_simple_literal_type_inference), so it doesn't reject it like
        # Cassandra does:
        # assert_invalid_message(cql, table, "Cannot infer type for term ((1)) in selection clause", "SELECT ck, t, ((1)) FROM %s")
        # We cannot differentiate a tuple containing a tuple from a tuple between parentheses.
        # (Scylla also infers the type of this term, tuple<tuple<int>>.)
        # assert_invalid_message(cql, table, "Cannot infer type for term ((tuple<int>)(1))", "SELECT ck, t, ((tuple<int>)(1)) FROM %s")
        assertConstantResult(execute(cql, table, "SELECT ck, t, (tuple<tuple<int>>)((1)) FROM %s"), ((1,),))

        # Scylla deliberately infers the type of a literal in the selection
        # clause (SCYLLADB-1229, see test_selector_literals.py::
        # test_simple_literal_type_inference), so it doesn't reject it like
        # Cassandra does:
        # assert_invalid_message_re(cql, table, CANNOT_INFER, "SELECT ck, t, [1, 2, 3] FROM %s")
        assertConstantResult(execute(cql, table, "SELECT ck, t, (list<int>)[1, 2, 3] FROM %s"), [1, 2, 3])

        # Scylla deliberately infers the type of a literal in the selection
        # clause (SCYLLADB-1229, see test_selector_literals.py::
        # test_simple_literal_type_inference), so it doesn't reject it like
        # Cassandra does:
        # assert_invalid_message_re(cql, table, CANNOT_INFER, "SELECT ck, t, {1, 2, 3} FROM %s")
        assertConstantResult(execute(cql, table, "SELECT ck, t, (set<int>){1, 2, 3} FROM %s"), {1, 2, 3})

        # Scylla deliberately infers the type of a literal in the selection
        # clause (SCYLLADB-1229, see test_selector_literals.py::
        # test_simple_literal_type_inference), so it doesn't reject it like
        # Cassandra does:
        # assert_invalid_message_re(cql, table, CANNOT_INFER, "SELECT ck, t, {1: 'foo', 2: 'bar', 3: 'baz'} FROM %s")
        assertConstantResult(execute(cql, table, "SELECT ck, t, (map<int, text>){1: 'foo', 2: 'bar', 3: 'baz'} FROM %s"), {1: "foo", 2: "bar", 3: "baz"})

        assert_invalid_message_re(cql, table, CANNOT_INFER, "SELECT ck, t, {} FROM %s")
        assertConstantResult(execute(cql, table, "SELECT ck, t, (map<int, text>){} FROM %s"), {})
        assertConstantResult(execute(cql, table, "SELECT ck, t, (set<int>){} FROM %s"), set())

        with original_column_names(cql):
            assert_column_names(execute(cql, table, "SELECT ck, t, (int)42, (int)43 FROM %s"), "ck", "t", "(int)42", "(int)43")
        assert_rows(execute(cql, table, "SELECT ck, t, (int) 42, (int) 43 FROM %s"),
                    row(1, "one", 42, 43),
                    row(2, "two", 42, 43),
                    row(3, "three", 42, 43))

        assert_rows(execute(cql, table, "SELECT min(ck), max(ck), [min(ck), max(ck)] FROM %s"), row(1, 3, [1, 3]))
        assert_rows(execute(cql, table, "SELECT [min(ck), max(ck)] FROM %s"), row([1, 3]))
        assert_rows(execute(cql, table, "SELECT {min(ck), max(ck)} FROM %s"), row({1, 3}))

        # We need to use a cast to differentiate between a map and an UDT
        assert_invalid_message_re(cql, table, re.escape("Cannot infer type for term {'min': system.min(ck), 'max': system.max(ck)}") + "|Could not infer type of",
                               "SELECT {'min' : min(ck), 'max' : max(ck)} FROM %s")
        assert_rows(execute(cql, table, "SELECT (map<text, int>){'min' : min(ck), 'max' : max(ck)} FROM %s"), row({"min": 1, "max": 3}))

        assert_rows(execute(cql, table, "SELECT [1, min(ck), max(ck)] FROM %s"), row([1, 1, 3]))
        assert_rows(execute(cql, table, "SELECT {1, min(ck), max(ck)} FROM %s"), row({1, 1, 3}))
        assert_rows(execute(cql, table, "SELECT (map<text, int>) {'litteral' : 1, 'min' : min(ck), 'max' : max(ck)} FROM %s"), row({"litteral": 1, "min": 1, "max": 3}))

        # Test List nested within Lists
        assert_rows(execute(cql, table, "SELECT [[], [min(ck), max(ck)]] FROM %s"),
                    row([[], [1, 3]]))
        assert_rows(execute(cql, table, "SELECT [[], [CAST(pk AS BIGINT), CAST(ck AS BIGINT), WRITETIME(t)]] FROM %s"),
                    row([[], [1, 1, timestampInMicros]]),
                    row([[], [1, 2, timestampInMicros]]),
                    row([[], [1, 3, timestampInMicros]]))
        assert_rows(execute(cql, table, "SELECT [[min(ck)], [max(ck)]] FROM %s"),
                    row([[1], [3]]))
        assert_rows(execute(cql, table, "SELECT [[min(ck)], ([max(ck)])] FROM %s"),
                    row([[1], [3]]))
        assert_rows(execute(cql, table, "SELECT [[pk], [ck]] FROM %s"),
                    row([[1], [1]]),
                    row([[1], [2]]),
                    row([[1], [3]]))
        assert_rows(execute(cql, table, "SELECT [[pk], [ck]] FROM %s WHERE pk = 1 ORDER BY ck DESC"),
                    row([[1], [3]]),
                    row([[1], [2]]),
                    row([[1], [1]]))

        # Test Sets nested within Lists
        assert_rows(execute(cql, table, "SELECT [{}, {min(ck), max(ck)}] FROM %s"),
                    row([set(), {1, 3}]))
        assert_rows(execute(cql, table, "SELECT [{}, {CAST(pk AS BIGINT), CAST(ck AS BIGINT), WRITETIME(t)}] FROM %s"),
                    row([set(), {1, 1, timestampInMicros}]),
                    row([set(), {1, 2, timestampInMicros}]),
                    row([set(), {1, 3, timestampInMicros}]))
        assert_rows(execute(cql, table, "SELECT [{min(ck)}, {max(ck)}] FROM %s"),
                    row([{1}, {3}]))
        assert_rows(execute(cql, table, "SELECT [{min(ck)}, ({max(ck)})] FROM %s"),
                    row([{1}, {3}]))
        assert_rows(execute(cql, table, "SELECT [{pk}, {ck}] FROM %s"),
                    row([{1}, {1}]),
                    row([{1}, {2}]),
                    row([{1}, {3}]))
        assert_rows(execute(cql, table, "SELECT [{pk}, {ck}] FROM %s WHERE pk = 1 ORDER BY ck DESC"),
                    row([{1}, {3}]),
                    row([{1}, {2}]),
                    row([{1}, {1}]))

        # Test Maps nested within Lists
        assert_rows(execute(cql, table, "SELECT [{}, (map<text, int>){'min' : min(ck), 'max' : max(ck)}] FROM %s"),
                    row([{}, {"min": 1, "max": 3}]))
        assert_rows(execute(cql, table, "SELECT [{}, (map<text, bigint>){'pk' : CAST(pk AS BIGINT), 'ck' : CAST(ck AS BIGINT), 'writetime' : WRITETIME(t)}] FROM %s"),
                    row([{}, {"pk": 1, "ck": 1, "writetime": timestampInMicros}]),
                    row([{}, {"pk": 1, "ck": 2, "writetime": timestampInMicros}]),
                    row([{}, {"pk": 1, "ck": 3, "writetime": timestampInMicros}]))
        assert_rows(execute(cql, table, "SELECT [{}, (map<text, int>){'pk' : pk, 'ck' : ck}] FROM %s WHERE pk = 1 ORDER BY ck DESC"),
                    row([{}, {"pk": 1, "ck": 3}]),
                    row([{}, {"pk": 1, "ck": 2}]),
                    row([{}, {"pk": 1, "ck": 1}]))

        # Test Tuples nested within Lists
        assert_rows(execute(cql, table, "SELECT [(pk, ck, WRITETIME(t))] FROM %s"),
                    row([(1, 1, timestampInMicros)]),
                    row([(1, 2, timestampInMicros)]),
                    row([(1, 3, timestampInMicros)]))
        assert_rows(execute(cql, table, "SELECT [(min(ck), max(ck))] FROM %s"),
                    row([(1, 3)]))
        # The following check from the Java test was not translated, because
        # of a Cassandra bug: The elements of this list literal are tuples of
        # different types (bigint, bigint) and (text, bigint), and Cassandra
        # accepts it - but describes the result's type by the first element,
        # list<frozen<tuple<bigint, bigint>>>, so the driver fails to decode
        # the second element, and closes the connection. The Java test reads
        # the result inside Cassandra, so it doesn't notice. This is
        # CASSANDRA-21736.
        # assert_rows(execute(cql, table, "SELECT [(CAST(pk AS BIGINT), CAST(ck AS BIGINT)), (t, WRITETIME(t))] FROM %s"),
        #             row([(1, 1), ("one", timestampInMicros)]),
        #             row([(1, 2), ("two", timestampInMicros)]),
        #             row([(1, 3), ("three", timestampInMicros)]))

        # Test UDTs nested within Lists
        with create_type(cql, test_keyspace, "(a int, b int, c bigint)") as type:
            assert_rows(execute(cql, table, "SELECT [(" + type + "){a : min(ck), b: max(ck)}] FROM %s"),
                        row([user_type("a", 1, "b", 3, "c", None)]))
            assert_rows(execute(cql, table, "SELECT [(" + type + "){a : pk, b : ck, c : WRITETIME(t)}] FROM %s"),
                        row([user_type("a", 1, "b", 1, "c", timestampInMicros)]),
                        row([user_type("a", 1, "b", 2, "c", timestampInMicros)]),
                        row([user_type("a", 1, "b", 3, "c", timestampInMicros)]))
            assert_rows(execute(cql, table, "SELECT [(" + type + "){a : pk, b : ck, c : WRITETIME(t)}] FROM %s WHERE pk = 1 ORDER BY ck DESC"),
                        row([user_type("a", 1, "b", 3, "c", timestampInMicros)]),
                        row([user_type("a", 1, "b", 2, "c", timestampInMicros)]),
                        row([user_type("a", 1, "b", 1, "c", timestampInMicros)]))

            # Test Lists nested within Sets
            assert_rows(execute(cql, table, "SELECT {[], [min(ck), max(ck)]} FROM %s"),
                        row({(), (1, 3)}))
            assert_rows(execute(cql, table, "SELECT {[], [pk, ck]} FROM %s LIMIT 2"),
                        row({(), (1, 1)}),
                        row({(), (1, 2)}))
            assert_rows(execute(cql, table, "SELECT {[], [pk, ck]} FROM %s WHERE pk = 1 ORDER BY ck DESC LIMIT 2"),
                        row({(), (1, 3)}),
                        row({(), (1, 2)}))
            assert_rows(execute(cql, table, "SELECT {[min(ck)], ([max(ck)])} FROM %s"),
                        row({(1,), (3,)}))
            assert_rows(execute(cql, table, "SELECT {[pk], ([ck])} FROM %s"),
                        row({(1,), (1,)}),
                        row({(1,), (2,)}),
                        row({(1,), (3,)}))
            assert_rows(execute(cql, table, "SELECT {([min(ck)]), [max(ck)]} FROM %s"),
                        row({(1,), (3,)}))

            # Test Sets nested within Sets
            assert_rows(execute(cql, table, "SELECT {{}, {min(ck), max(ck)}} FROM %s"),
                        row({fz(), fz(1, 3)}))
            assert_rows(execute(cql, table, "SELECT {{}, {pk, ck}} FROM %s LIMIT 2"),
                        row({fz(), fz(1, 1)}),
                        row({fz(), fz(1, 2)}))
            assert_rows(execute(cql, table, "SELECT {{}, {pk, ck}} FROM %s WHERE pk = 1 ORDER BY ck DESC LIMIT 2"),
                        row({fz(), fz(1, 3)}),
                        row({fz(), fz(1, 2)}))
            assert_rows(execute(cql, table, "SELECT {{min(ck)}, ({max(ck)})} FROM %s"),
                        row({fz(1), fz(3)}))
            assert_rows(execute(cql, table, "SELECT {{pk}, ({ck})} FROM %s"),
                        row({fz(1), fz(1)}),
                        row({fz(1), fz(2)}),
                        row({fz(1), fz(3)}))
            assert_rows(execute(cql, table, "SELECT {({min(ck)}), {max(ck)}} FROM %s"),
                        row({fz(1), fz(3)}))

            # Test Maps nested within Sets
            assert_rows(execute(cql, table, "SELECT {{}, (map<text, int>){'min' : min(ck), 'max' : max(ck)}} FROM %s"),
                        row({fz(), fz(("min", 1), ("max", 3))}))
            assert_rows(execute(cql, table, "SELECT {{}, (map<text, int>){'pk' : pk, 'ck' : ck}} FROM %s"),
                        row({fz(), fz(("pk", 1), ("ck", 1))}),
                        row({fz(), fz(("pk", 1), ("ck", 2))}),
                        row({fz(), fz(("pk", 1), ("ck", 3))}))

            # Test Tuples nested within Sets
            assert_rows(execute(cql, table, "SELECT {(pk, ck, WRITETIME(t))} FROM %s"),
                        row({(1, 1, timestampInMicros)}),
                        row({(1, 2, timestampInMicros)}),
                        row({(1, 3, timestampInMicros)}))
            assert_rows(execute(cql, table, "SELECT {(min(ck), max(ck))} FROM %s"),
                        row({(1, 3)}))

            # Test UDTs nested within Sets
            assert_rows(execute(cql, table, "SELECT {(" + type + "){a : min(ck), b: max(ck)}} FROM %s"),
                        row({(1, 3, None)}))
            assert_rows(execute(cql, table, "SELECT {(" + type + "){a : pk, b : ck, c : WRITETIME(t)}} FROM %s"),
                        row({(1, 1, timestampInMicros)}),
                        row({(1, 2, timestampInMicros)}),
                        row({(1, 3, timestampInMicros)}))
            assert_rows(execute(cql, table, "SELECT {(" + type + "){a : pk, b : ck, c : WRITETIME(t)}} FROM %s WHERE pk = 1 ORDER BY ck DESC"),
                        row({(1, 3, timestampInMicros)}),
                        row({(1, 2, timestampInMicros)}),
                        row({(1, 1, timestampInMicros)}))

            # Test Lists nested within Maps
            assert_rows(execute(cql, table, "SELECT (map<frozen<list<int>>, frozen<list<int>>>){[min(ck)]:[max(ck)]} FROM %s"),
                        row({(1,): [3]}))
            assert_rows(execute(cql, table, "SELECT (map<frozen<list<int>>, frozen<list<int>>>){[pk]: [ck]} FROM %s"),
                        row({(1,): [1]}),
                        row({(1,): [2]}),
                        row({(1,): [3]}))

            # Test Sets nested within Maps
            assert_rows(execute(cql, table, "SELECT (map<frozen<set<int>>, frozen<set<int>>>){{min(ck)} : {max(ck)}} FROM %s"),
                        row({fz(1): {3}}))
            assert_rows(execute(cql, table, "SELECT (map<frozen<set<int>>, frozen<set<int>>>){{pk} : {ck}} FROM %s"),
                        row({fz(1): {1}}),
                        row({fz(1): {2}}),
                        row({fz(1): {3}}))

            # Test Maps nested within Maps
            assert_rows(execute(cql, table, "SELECT (map<frozen<map<text, int>>, frozen<map<text, int>>>){{'min' : min(ck)} : {'max' : max(ck)}} FROM %s"),
                        row({fz(("min", 1)): {"max": 3}}))
            assert_rows(execute(cql, table, "SELECT (map<frozen<map<text, int>>, frozen<map<text, int>>>){{'pk' : pk} : {'ck' : ck}} FROM %s"),
                        row({fz(("pk", 1)): {"ck": 1}}),
                        row({fz(("pk", 1)): {"ck": 2}}),
                        row({fz(("pk", 1)): {"ck": 3}}))

            # Test Tuples nested within Maps
            assert_rows(execute(cql, table, "SELECT (map<frozen<tuple<int, int>>, frozen<tuple<bigint>>>){(pk, ck) : (WRITETIME(t))} FROM %s"),
                        row({(1, 1): (timestampInMicros,)}),
                        row({(1, 2): (timestampInMicros,)}),
                        row({(1, 3): (timestampInMicros,)}))
            assert_rows(execute(cql, table, "SELECT (map<frozen<tuple<int>> , frozen<tuple<int>>>){(min(ck)) : (max(ck))} FROM %s"),
                        row({(1,): (3,)}))

            # Test UDTs nested within Maps
            assert_rows(execute(cql, table, "SELECT (map<int, frozen<" + type + ">>){ck : {a : min(ck), b: max(ck)}} FROM %s"),
                        row({1: user_type("a", 1, "b", 3, "c", None)}))
            assert_rows(execute(cql, table, "SELECT (map<int, frozen<" + type + ">>){ck : {a : pk, b : ck, c : WRITETIME(t)}} FROM %s"),
                        row({1: user_type("a", 1, "b", 1, "c", timestampInMicros)}),
                        row({2: user_type("a", 1, "b", 2, "c", timestampInMicros)}),
                        row({3: user_type("a", 1, "b", 3, "c", timestampInMicros)}))
            assert_rows(execute(cql, table, "SELECT (map<int, frozen<" + type + ">>){ck : {a : pk, b : ck, c : WRITETIME(t)}} FROM %s WHERE pk = 1 ORDER BY ck DESC"),
                        row({3: user_type("a", 1, "b", 3, "c", timestampInMicros)}),
                        row({2: user_type("a", 1, "b", 2, "c", timestampInMicros)}),
                        row({1: user_type("a", 1, "b", 1, "c", timestampInMicros)}))

            # Test Lists nested within Tuples
            assert_rows(execute(cql, table, "SELECT ([min(ck)], [max(ck)]) FROM %s"),
                        row(([1], [3])))
            assert_rows(execute(cql, table, "SELECT ([pk], [ck]) FROM %s"),
                        row(([1], [1])),
                        row(([1], [2])),
                        row(([1], [3])))

            # Test Sets nested within Tuples
            assert_rows(execute(cql, table, "SELECT ({min(ck)}, {max(ck)}) FROM %s"),
                        row(({1}, {3})))
            assert_rows(execute(cql, table, "SELECT ({pk}, {ck}) FROM %s"),
                        row(({1}, {1})),
                        row(({1}, {2})),
                        row(({1}, {3})))

            # Test Maps nested within Tuples
            assert_rows(execute(cql, table, "SELECT ((map<text, int>){'min' : min(ck)}, (map<text, int>){'max' : max(ck)}) FROM %s"),
                        row(({"min": 1}, {"max": 3})))
            assert_rows(execute(cql, table, "SELECT ((map<text, int>){'pk' : pk}, (map<text, int>){'ck' : ck}) FROM %s"),
                        row(({"pk": 1}, {"ck": 1})),
                        row(({"pk": 1}, {"ck": 2})),
                        row(({"pk": 1}, {"ck": 3})))

            # Test Tuples nested within Tuples
            assert_rows(execute(cql, table, "SELECT (tuple<tuple<int, int, bigint>>)((pk, ck, WRITETIME(t))) FROM %s"),
                        row(((1, 1, timestampInMicros),)),
                        row(((1, 2, timestampInMicros),)),
                        row(((1, 3, timestampInMicros),)))
            assert_rows(execute(cql, table, "SELECT (tuple<tuple<int, int, bigint>>)((min(ck), max(ck))) FROM %s"),
                        row(((1, 3, None),)))

            assert_rows(execute(cql, table, "SELECT ((t, WRITETIME(t)), (CAST(pk AS BIGINT), CAST(ck AS BIGINT))) FROM %s"),
                        row((("one", timestampInMicros), (1, 1))),
                        row((("two", timestampInMicros), (1, 2))),
                        row((("three", timestampInMicros), (1, 3))))

            # Test UDTs nested within Tuples
            assert_rows(execute(cql, table, "SELECT (tuple<" + type + ">)({a : min(ck), b: max(ck)}) FROM %s"),
                        row((user_type("a", 1, "b", 3, "c", None),)))
            assert_rows(execute(cql, table, "SELECT (tuple<" + type + ">)({a : pk, b : ck, c : WRITETIME(t)}) FROM %s"),
                        row((user_type("a", 1, "b", 1, "c", timestampInMicros),)),
                        row((user_type("a", 1, "b", 2, "c", timestampInMicros),)),
                        row((user_type("a", 1, "b", 3, "c", timestampInMicros),)))
            assert_rows(execute(cql, table, "SELECT (tuple<" + type + ">)({a : pk, b : ck, c : WRITETIME(t)}) FROM %s WHERE pk = 1 ORDER BY ck DESC"),
                        row((user_type("a", 1, "b", 3, "c", timestampInMicros),)),
                        row((user_type("a", 1, "b", 2, "c", timestampInMicros),)),
                        row((user_type("a", 1, "b", 1, "c", timestampInMicros),)))

            # Test Lists nested within UDTs
            with create_type(cql, test_keyspace, "(l list<int>)") as containerType:
                assert_rows(execute(cql, table, "SELECT (" + containerType + "){l : [min(ck), max(ck)]} FROM %s"),
                            row(user_type("l", [1, 3])))
                assert_rows(execute(cql, table, "SELECT (" + containerType + "){l : [pk, ck]} FROM %s"),
                            row(user_type("l", [1, 1])),
                            row(user_type("l", [1, 2])),
                            row(user_type("l", [1, 3])))

            # Test Sets nested within UDTs
            with create_type(cql, test_keyspace, "(s set<int>)") as containerType:
                assert_rows(execute(cql, table, "SELECT (" + containerType + "){s : {min(ck), max(ck)}} FROM %s"),
                            row(user_type("s", {1, 3})))
                assert_rows(execute(cql, table, "SELECT (" + containerType + "){s : {pk, ck}} FROM %s"),
                            row(user_type("s", {1})),
                            row(user_type("s", {1, 2})),
                            row(user_type("s", {1, 3})))

            # Test Maps nested within UDTs
            with create_type(cql, test_keyspace, "(m map<text, int>)") as containerType:
                assert_rows(execute(cql, table, "SELECT (" + containerType + "){m : {'min' : min(ck), 'max' : max(ck)}} FROM %s"),
                            row(user_type("m", {"min": 1, "max": 3})))
                assert_rows(execute(cql, table, "SELECT (" + containerType + "){m : {'pk' : pk, 'ck' : ck}} FROM %s"),
                            row(user_type("m", {"pk": 1, "ck": 1})),
                            row(user_type("m", {"pk": 1, "ck": 2})),
                            row(user_type("m", {"pk": 1, "ck": 3})))

            # Test Tuples nested within UDTs
            with create_type(cql, test_keyspace, "(t tuple<int, int>, w tuple<bigint>)") as containerType:
                assert_rows(execute(cql, table, "SELECT (" + containerType + "){t : (pk, ck), w : (WRITETIME(t))} FROM %s"),
                            row(user_type("t", (1, 1), "w", (timestampInMicros,))),
                            row(user_type("t", (1, 2), "w", (timestampInMicros,))),
                            row(user_type("t", (1, 3), "w", (timestampInMicros,))))

            # Test UDTs nested within Maps
            with create_type(cql, test_keyspace, "(t frozen<" + type + ">)") as containerType:
                assert_rows(execute(cql, table, "SELECT (" + containerType + "){t : {a : min(ck), b: max(ck)}} FROM %s"),
                            row(user_type("t", user_type("a", 1, "b", 3, "c", None))))
                assert_rows(execute(cql, table, "SELECT (" + containerType + "){t : {a : pk, b : ck, c : WRITETIME(t)}} FROM %s"),
                            row(user_type("t", user_type("a", 1, "b", 1, "c", timestampInMicros))),
                            row(user_type("t", user_type("a", 1, "b", 2, "c", timestampInMicros))),
                            row(user_type("t", user_type("a", 1, "b", 3, "c", timestampInMicros))))
                assert_rows(execute(cql, table, "SELECT (" + containerType + "){t : {a : pk, b : ck, c : WRITETIME(t)}} FROM %s WHERE pk = 1 ORDER BY ck DESC"),
                            row(user_type("t", user_type("a", 1, "b", 3, "c", timestampInMicros))),
                            row(user_type("t", user_type("a", 1, "b", 2, "c", timestampInMicros))),
                            row(user_type("t", user_type("a", 1, "b", 1, "c", timestampInMicros))))

        # Test Litteral Set with Duration elements
        assert_invalid_message(cql, table, "Durations are not allowed inside sets: set<duration>",
                               "SELECT pk, ck, (set<duration>){2d, 1mo} FROM %s")

        # Scylla's message doesn't have the "system." prefix
        assert_invalid_message_re(cql, table, re.escape("Invalid field selection: ") + "(system.)?" + re.escape("min(ck) of type int is not a user type"),
                               "SELECT min(ck).min FROM %s")
        assert_invalid_message(cql, table, "Invalid field selection: (map<text, int>){'min': system.min(ck), 'max': system.max(ck)} of type frozen<map<text, int>> is not a user type",
                               "SELECT (map<text, int>) {'min' : min(ck), 'max' : max(ck)}.min FROM %s")

# Reproduces #5411 (terms, such as type hints and collection literals, in the
# selection clause)
@pytest.mark.xfail(reason="#5411")
def testCollectionLiteralsWithDurations(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pk int, ck int, d1 duration, d2 duration, PRIMARY KEY (pk, ck) )") as table:
        execute(cql, table, "INSERT INTO %s (pk, ck, d1, d2) VALUES (1, 1, 15h, 13h)")
        execute(cql, table, "INSERT INTO %s (pk, ck, d1, d2) VALUES (1, 2, 10h, 12h)")
        execute(cql, table, "INSERT INTO %s (pk, ck, d1, d2) VALUES (1, 3, 11h, 13h)")

        h = 3600 * 1000000000   # nanoseconds per hour
        assert_rows(execute(cql, table, "SELECT [d1, d2] FROM %s"),
                    row([Duration(0, 0, 15 * h), Duration(0, 0, 13 * h)]),
                    row([Duration(0, 0, 10 * h), Duration(0, 0, 12 * h)]),
                    row([Duration(0, 0, 11 * h), Duration(0, 0, 13 * h)]))

        assert_invalid_message(cql, table, "Durations are not allowed inside sets: frozen<set<duration>>", "SELECT {d1, d2} FROM %s")

        assert_rows(execute(cql, table, "SELECT (map<int, duration>){ck : d1} FROM %s"),
                    row({1: Duration(0, 0, 15 * h)}),
                    row({2: Duration(0, 0, 10 * h)}),
                    row({3: Duration(0, 0, 11 * h)}))

        assert_invalid_message(cql, table, "Durations are not allowed as map keys: map<duration, int>",
                               "SELECT (map<duration, int>){d1 : ck, d2 :ck} FROM %s")

# Reproduces #5411 (terms, such as type hints and collection literals, in the
# selection clause). Moreover, since Scylla infers the types of tuple literals
# in the selection clause (SCYLLADB-1229), it interprets the parenthesized
# ((type){...}) as a tuple with one element, so it can't select a field of it.
@pytest.mark.xfail(reason="#5411")
def testSelectUDTLiteral(cql, test_keyspace):
    with create_type(cql, test_keyspace, "(a int, b text)") as type:
        with create_table(cql, test_keyspace, "(k int PRIMARY KEY, v " + type + ")") as table:
            execute(cql, table, "INSERT INTO %s(k, v) VALUES (?, ?)", 0, user_type("a", 3, "b", "foo"))

            assert_invalid_message_re(cql, table, CANNOT_INFER, "SELECT { a: 4, b: 'bar'} FROM %s")

            assert_rows(execute(cql, table, "SELECT k, v, (" + type + "){ a: 4, b: 'bar'} FROM %s"),
                        row(0, user_type("a", 3, "b", "foo"), user_type("a", 4, "b", "bar")))

            assert_rows(execute(cql, table, "SELECT k, v, (" + type + ")({ a: 4, b: 'bar'}) FROM %s"),
                        row(0, user_type("a", 3, "b", "foo"), user_type("a", 4, "b", "bar")))

            assert_rows(execute(cql, table, "SELECT k, v, ((" + type + "){ a: 4, b: 'bar'}).a FROM %s"),
                        row(0, user_type("a", 3, "b", "foo"), 4))

            assert_rows(execute(cql, table, "SELECT k, v, (" + type + "){ a: 4, b: 'bar'}.a FROM %s"),
                        row(0, user_type("a", 3, "b", "foo"), 4))

            assert_invalid_message_re(cql, table, CANNOT_INFER, "SELECT { a: 4} FROM %s")

            assert_rows(execute(cql, table, "SELECT k, v, (" + type + "){ a: 4} FROM %s"),
                        row(0, user_type("a", 3, "b", "foo"), user_type("a", 4, "b", None)))

            assert_rows(execute(cql, table, "SELECT k, v, (" + type + "){ b: 'bar'} FROM %s"),
                        row(0, user_type("a", 3, "b", "foo"), user_type("a", None, "b", "bar")))

            execute(cql, table, "INSERT INTO %s(k, v) VALUES (?, ?)", 1, user_type("a", 5, "b", "foo"))
            assert_rows(execute(cql, table, "SELECT (" + type + "){ a: max(v.a) , b: 'max'} FROM %s"),
                        row(user_type("a", 5, "b", "max")))
            assert_rows(execute(cql, table, "SELECT (" + type + "){ a: min(v.a) , b: 'min'} FROM %s"),
                        row(user_type("a", 3, "b", "min")))
