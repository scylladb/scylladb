# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 6ca34f81386dc8f6020cdf2ea4246bca2a0896c5
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0

from cassandra_tests.porting import *
from cassandra.query import UNSET_VALUE
from datetime import datetime

def testTuplePutAndGet(cql, test_keyspace):
    for valueType in ["frozen<tuple<int, text, double>>", "tuple<int, text, double>"]:
        with create_table(cql, test_keyspace, f"(k int PRIMARY KEY, t {valueType})") as table:
            execute(cql, table, "INSERT INTO %s (k, t) VALUES (?, ?)", 0, (3, "foo", 3.4))
            execute(cql, table, "INSERT INTO %s (k, t) VALUES (?, ?)", 1, (8, "bar", 0.2))
            assert_all_rows(cql, table, [1, (8, "bar", 0.2)],
                          [0, (3, "foo", 3.4)])

            # nulls
            execute(cql, table, "INSERT INTO %s (k, t) VALUES (?, ?)", 2, (5, None, 3.4))
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k=?", 2),
                       [2, (5, None, 3.4)])

            # incomplete tuple
            execute(cql, table, "INSERT INTO %s (k, t) VALUES (?, ?)", 3, (5, "bar"))
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k=?", 3),
                       [3, (5, "bar", None)])


def testNestedTuple(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, t frozen<tuple<int, tuple<text, double>>>)") as table:
        execute(cql, table, "INSERT INTO %s (k, t) VALUES (?, ?)", 0, (3, ("foo", 3.4)))
        execute(cql, table, "INSERT INTO %s (k, t) VALUES (?, ?)", 1, (8, ("bar", 0.2)))
        assert_all_rows(cql, table, [1, (8, ("bar", 0.2))], [0, (3, ("foo", 3.4))])

def testTupleInPartitionKey(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(t frozen<tuple<int, text>> PRIMARY KEY)") as table:
        execute(cql, table, "INSERT INTO %s (t) VALUES (?)", (3, "foo"))
        assert_all_rows(cql, table, [(3, "foo")])

def testTupleInClusteringKey(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int, t frozen<tuple<int, text>>, PRIMARY KEY (k, t))") as table:
        execute(cql, table, "INSERT INTO %s (k, t) VALUES (?, ?)", 0, (5, "bar"))
        execute(cql, table, "INSERT INTO %s (k, t) VALUES (?, ?)", 0, (3, "foo"))
        execute(cql, table, "INSERT INTO %s (k, t) VALUES (?, ?)", 0, (6, "bar"))
        execute(cql, table, "INSERT INTO %s (k, t) VALUES (?, ?)", 0, (5, "foo"))

        assert_all_rows(cql, table,
            [0, (3, "foo")],
            [0, (5, "bar")],
            [0, (5, "foo")],
            [0, (6, "bar")]
        )

def testTupleFromString(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int, c int, t frozen<tuple<int, text>>, PRIMARY KEY (k, c))") as table:
        execute(cql, table, "INSERT INTO %s (k, c, t) VALUES (0, 0, '0:0')")
        execute(cql, table, "INSERT INTO %s (k, c, t) VALUES (0, 1, '0:1')")
        execute(cql, table, "INSERT INTO %s (k, c, t) VALUES (0, 2, '1')")
        execute(cql, table, "INSERT INTO %s (k, c, t) VALUES (0, 3, '1:1\\:1')")
        execute(cql, table, "INSERT INTO %s (k, c, t) VALUES (0, 4, '@:1')")
        assert_all_rows(cql, table,
            [0, 0, (0, "0")],
            [0, 1, (0, "1")],
            [0, 2, (1, None)],
            [0, 3, (1, "1:1")],
            [0, 4, (None, "1")]
        )

        assert_invalid_message(cql, table, "Invalid tuple literal: too many elements. Type frozen<tuple<int, text>> expects 2 but got 3",
                             "INSERT INTO %s(k, t) VALUES (1,'1:2:3')")

def testInvalidQueries(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, t frozen<tuple<int, text, double>>)") as table:
        assert_invalid_syntax(cql, table, "INSERT INTO %s (k, t) VALUES (0, ())")

        assert_invalid_message(cql, table, "Invalid tuple literal for t: too many elements. Type frozen<tuple<int, text, double>> expects 3 but got 4",
                             "INSERT INTO %s (k, t) VALUES (0, (2, 'foo', 3.1, 'bar'))")
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, t frozen<tuple<int, tuple<int, text, double>>>)") as table:
        # The Python driver doesn't let us send a wrong-lengthed tuple as a
        # prepared statement parameter. FIXME: figure out how to monkey-patching
        # the driver or another trick to make the following work (i.e., pass the
        # invalid tuple to the server, and let the server - not the driver - fail.
        #assert_invalid_message(cql, table, "Invalid remaining data after end of tuple value",
        #                     "INSERT INTO %s (k, t) VALUES (0, ?)",
        #                     (1, (1, "1", 1.0, 1)))

        assert_invalid_message(cql, table, "Invalid tuple literal for t: component 1 is not of type frozen<tuple<int, text, double>>",
                             "INSERT INTO %s (k, t) VALUES (0, (1, (1, '1', 1.0, 1)))")

def testTupleWithUnsetValues(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, t tuple<int, text, double>)") as table:
        # invalid positional field substitution
        assert_invalid_message(cql, table, "unset",
                             "INSERT INTO %s (k, t) VALUES(0, (3, ?, 2.1))", UNSET_VALUE)

        #FIXME: The Python driver doesn't agree to send such a command to the server,
        #so I had to comment out this test.
        #execute(cql, table, "CREATE INDEX tuple_index ON %s (t)")
        # select using unset
        #assert_invalid_message("Invalid unset value for tuple field number 0", "SELECT * FROM %s WHERE k = ? and t = (?,?,?)", UNSET_VALUE, UNSET_VALUE, UNSET_VALUE, UNSET_VALUE)

# Test the syntax introduced by CASSANDRA-4851,
# migrated from cql_tests.py:TestCQL.tuple_notation_test()
def testTupleNotation(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int, v1 int, v2 int, v3 int, PRIMARY KEY (k, v1, v2, v3))") as table:
        for i in range(2):
            for j in range(2):
                for k in range(2):
                    execute(cql, table, "INSERT INTO %s (k, v1, v2, v3) VALUES (0, ?, ?, ?)", i, j, k)

        assert_rows(execute(cql, table, "SELECT v1, v2, v3 FROM %s WHERE k = 0"),
                   [0, 0, 0],
                   [0, 0, 1],
                   [0, 1, 0],
                   [0, 1, 1],
                   [1, 0, 0],
                   [1, 0, 1],
                   [1, 1, 0],
                   [1, 1, 1])

        assert_rows(execute(cql, table, "SELECT v1, v2, v3 FROM %s WHERE k = 0 AND (v1, v2, v3) >= (1, 0, 1)"),
                   [1, 0, 1],
                   [1, 1, 0],
                   [1, 1, 1])
        assert_rows(execute(cql, table, "SELECT v1, v2, v3 FROM %s WHERE k = 0 AND (v1, v2) >= (1, 1)"),
                   [1, 1, 0],
                   [1, 1, 1])

        assert_rows(execute(cql, table, "SELECT v1, v2, v3 FROM %s WHERE k = 0 AND (v1, v2) > (0, 1) AND (v1, v2, v3) <= (1, 1, 0)"),
                   [1, 0, 0],
                   [1, 0, 1],
                   [1, 1, 0])

        assert_invalid(cql, table, "SELECT v1, v2, v3 FROM %s WHERE k = 0 AND (v1, v3) > (1, 0)")

# Test for CASSANDRA-8062,
# migrated from cql_tests.py:TestCQL.test_v2_protocol_IN_with_tuples()
def testSelectInStatementWithTuples(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int, c1 int, c2 text, PRIMARY KEY (k, c1, c2))") as table:
        # the dtest was using v2 protocol
        execute(cql, table, "INSERT INTO %s (k, c1, c2) VALUES (0, 0, 'a')")
        execute(cql, table, "INSERT INTO %s (k, c1, c2) VALUES (0, 0, 'b')")
        execute(cql, table, "INSERT INTO %s (k, c1, c2) VALUES (0, 0, 'c')")

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k=0 AND (c1, c2) IN ((0, 'b'), (0, 'c'))"),
                   [0, 0, "b"],
                   [0, 0, "c"])

# Note translated because the Python driver refuses to send incorrect
# parameters for prepared statements.
#    @Test
#    public void testInvalidInputForTuple() throws Throwable
#    {
#        createTable("CREATE TABLE %s(pk int PRIMARY KEY, t tuple<text, text>)");
#        assertInvalidMessage("Not enough bytes to read 0th component",
#                             "INSERT INTO %s (pk, t) VALUES (?, ?)", 1, "test");
#        assertInvalidMessage("Not enough bytes to read 0th component",
#                             "INSERT INTO %s (pk, t) VALUES (?, ?)", 1, Long.MAX_VALUE);
#    }

@pytest.mark.xfail(reason="Cassandra 3.10's += syntax not yet supported. Issue #7735")
def testTupleModification(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pk int PRIMARY KEY, value tuple<int, int>)") as table:
        assert_invalid_message(cql, table, "Invalid operation (value = value + (1, 1)) for tuple column value",
                             "UPDATE %s SET value += (1, 1) WHERE k=0;")

# CASSANDRA-13717
def testReversedTypeTuple(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(id int, tdemo frozen<tuple<timestamp, varchar>>, primary key (id, tdemo)) with clustering order by (tdemo desc)") as table:
        execute(cql, table, "INSERT INTO %s (id, tdemo) VALUES (1, ('2017-02-03 03:05+0000','Europe'))")
        assert_rows(execute(cql, table, "SELECT tdemo FROM %s"),
            [(datetime(2017, 2, 3, 3, 5, 0), "Europe")])

# The following two tests were written using the Java "QuickTheories" library
# (https://github.com/quicktheories/QuickTheories), a property-based testing
# library. These are the only two tests in Cassandra's test suite to use this
# library, and I'm too lazy to learn this framework and translate it into
# Python just to convert these tests.
'''
    @Test
    public void tuplePartitionReadWrite()
    {
        qt().withExamples(100).withShrinkCycles(0).forAll(typesAndRowsGen()).checkAssert(orFail(testcase -> {
            TupleType tupleType = testcase.type;
            createTable("CREATE TABLE %s (id " + toCqlType(tupleType) + ", value int, PRIMARY KEY(id))");
            SortedMap<ByteBuffer, Integer> map = new TreeMap<>(Comparator.comparing(currentTableMetadata().partitioner::decorateKey));
            int count = 0;
            for (ByteBuffer value : testcase.uniqueRows)
            {
                map.put(value, count);
                ByteBuffer[] tupleBuffers = tupleType.split(value);

                // use cast to avoid warning
                execute("INSERT INTO %s (id, value) VALUES (?, ?)", tuple((Object[]) tupleBuffers), count);

                assertRows(execute("SELECT * FROM %s WHERE id = ?", tuple((Object[]) tupleBuffers)),
                           row(tuple((Object[]) tupleBuffers), count));
                count++;
            }
            assertRows(execute("SELECT * FROM %s LIMIT 100"),
                       map.entrySet().stream().map(e -> row(e.getKey(), e.getValue())).toArray(Object[][]::new));
        }));
    }

    @Test
    public void tupleCkReadWriteAsc()
    {
        tupleCkReadWrite(Order.ASC);
    }

    @Test
    public void tupleCkReadWriteDesc()
    {
        tupleCkReadWrite(Order.DESC);
    }

    private void tupleCkReadWrite(Order order)
    {
        // for some reason this test is much slower than the partition key test: with 100 examples partition key is 6s and these tests were 20-30s
        qt().withExamples(50).withShrinkCycles(0).forAll(typesAndRowsGen()).checkAssert(orFail(testcase -> {
            TupleType tupleType = testcase.type;
            createTable("CREATE TABLE %s (pk int, ck " + toCqlType(tupleType) + ", value int, PRIMARY KEY(pk, ck))" +
                        " WITH CLUSTERING ORDER BY (ck "+order.name()+")");
            String cql = SchemaCQLHelper.getTableMetadataAsCQL(currentTableMetadata(), false, false, false);
            SortedMap<ByteBuffer, Integer> map = new TreeMap<>(order.apply(tupleType));
            int count = 0;
            for (ByteBuffer value : testcase.uniqueRows)
            {
                map.put(value, count);
                ByteBuffer[] tupleBuffers = tupleType.split(value);

                // use cast to avoid warning
                execute("INSERT INTO %s (pk, ck, value) VALUES (?, ?, ?)", 1, tuple((Object[]) tupleBuffers), count);

                assertRows(execute("SELECT * FROM %s WHERE pk = ? AND ck = ?", 1, tuple((Object[]) tupleBuffers)),
                           row(1, tuple((Object[]) tupleBuffers), count));
                count++;
            }
            UntypedResultSet results = execute("SELECT * FROM %s LIMIT 100");
            assertRows(results,
                       map.entrySet().stream().map(e -> row(1, e.getKey(), e.getValue())).toArray(Object[][]::new));
        }));
    }

    private static final class TypeAndRows
    {
        TupleType type;
        List<ByteBuffer> uniqueRows;
    }

    private static Gen<TypeAndRows> typesAndRowsGen()
    {
        return typesAndRowsGen(10);
    }

    private static Gen<TypeAndRows> typesAndRowsGen(int numRows)
    {
        Gen<TupleType> typeGen = tupleTypeGen(primitiveTypeGen(), SourceDSL.integers().between(1, 10));
        Set<ByteBuffer> distinctRows = new HashSet<>(numRows); // reuse the memory
        Gen<TypeAndRows> gen = rnd -> {
            TypeAndRows c = new TypeAndRows();
            c.type = typeGen.generate(rnd);
            TypeSupport<ByteBuffer> support = getTypeSupport(c.type);
            Gen<ByteBuffer> valueGen = filter(support.valueGen, b -> b.remaining() <= Short.MAX_VALUE);
            valueGen = filter(valueGen, 20, v -> !distinctRows.contains(v));

            distinctRows.clear();
            for (int i = 0; i < numRows; i++)
            {
                try
                {
                    assert distinctRows.add(valueGen.generate(rnd)) : "unable to add distinct row";
                }
                catch (IllegalStateException e)
                {
                    // gave up trying to find values... so just try with how ever many rows we could
                    logger.warn("Unable to generate enough distinct rows; using {} rows", distinctRows.size());
                    break;
                }
            }
            c.uniqueRows = new ArrayList<>(distinctRows);
            return c;
        };
        gen = gen.describedAs(c -> c.type.asCQL3Type().toString());
        return gen;
    }

    private enum Order {
        ASC
        {
            <T> Comparator<T> apply(Comparator<T> c)
            {
                return c;
            }
        },
        DESC
        {
            <T> Comparator<T> apply(Comparator<T> c)
            {
                return c.reversed();
            }
        };

        abstract <T> Comparator<T> apply(Comparator<T> c);
    }

    private static List<Object[]> toObjects(UntypedResultSet results)
    {
        List<Object[]> rows = new ArrayList<>(results.size());
        for (UntypedResultSet.Row row : results)
            rows.add(results.metadata().stream().map(c -> c.type.compose(row.getBlob(c.name.toString()))).toArray());
        return rows;
    }
}
'''
