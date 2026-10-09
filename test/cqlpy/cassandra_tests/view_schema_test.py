# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 4ab8bac4a51f8aef0d55b2497699e1291baeda4b
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0
#
# Modifications: Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

from .porting import *
from contextlib import ExitStack
from uuid import UUID
from decimal import Decimal
from datetime import datetime, timezone
from cassandra.util import Date, Time

def testCaseSensitivity(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(\"theKey\" int, \"theClustering\" int, \"theValue\" int, PRIMARY KEY (\"theKey\", \"theClustering\"))") as table:
        execute(cql, table, "INSERT INTO %s (\"theKey\", \"theClustering\", \"theValue\") VALUES (?, ?, ?)", 0, 0, 0)

        # The Java test's views also have the restriction
        # '"theValue" IS NOT NULL'. Cassandra silently ignores an IS NOT NULL
        # restriction on a column outside the view's primary key, but Scylla
        # deliberately rejects it (see #10365, and
        # test_materialized_view.py::test_is_not_null_forbidden_in_filter), so
        # we removed it - this doesn't change the test on Cassandra.
        with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                     "WHERE \"theKey\" IS NOT NULL AND \"theClustering\" IS NOT NULL " +
                                     "PRIMARY KEY (\"theKey\", \"theClustering\")") as mv1, \
             create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT \"theKey\", \"theClustering\", \"theValue\" FROM %s " +
                                     "WHERE \"theKey\" IS NOT NULL AND \"theClustering\" IS NOT NULL " +
                                     "PRIMARY KEY (\"theKey\", \"theClustering\")") as mv2:

            for mvname in [mv1, mv2]:
                assert_rows(cql.execute("SELECT \"theKey\", \"theClustering\", \"theValue\" FROM " + mvname),
                            row(0, 0, 0))

            execute(cql, table, "ALTER TABLE %s RENAME \"theClustering\" TO \"Col\"")

            for mvname in [mv1, mv2]:
                assert_rows(cql.execute("SELECT \"theKey\", \"Col\", \"theValue\" FROM " + mvname),
                            row(0, 0, 0))

# The Java test checks that the view's schema has a column, using Cassandra's
# internal schema objects. We check the view's columns in system_schema
# instead.
def viewHasColumn(cql, view, column):
    ks, name = view.split('.')
    return list(cql.execute("SELECT column_name FROM system_schema.columns WHERE keyspace_name = %s AND table_name = %s AND column_name = %s",
                            [ks, name, column])) != []

# Cassandra's message is "Cannot use ALTER TABLE on a materialized view; use
# ALTER MATERIALIZED VIEW instead", Scylla's is "Cannot use ALTER TABLE on
# Materialized View. (Did you mean ALTER MATERIALIZED VIEW)?".
ALTER_TABLE_ON_VIEW = "Cannot use ALTER TABLE on (a materialized view|Materialized View)"

def testAccessAndSchema(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(" +
                      "k int, " +
                      "asciival ascii, " +
                      "bigintval bigint, " +
                      "PRIMARY KEY((k, asciival)))") as table:

        with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                     "WHERE bigintval IS NOT NULL AND k IS NOT NULL AND asciival IS NOT NULL " +
                                     "PRIMARY KEY (bigintval, k, asciival)") as view:
            execute(cql, table, "INSERT INTO %s(k,asciival,bigintval)VALUES(?,?,?)", 0, "foo", 1)

            # Shouldn't be able to modify a MV directly
            with pytest.raises(InvalidRequest, match="Cannot directly modify a materialized view"):
                execute(cql, view, "INSERT INTO %s(k,asciival,bigintval) VALUES(?,?,?)", 1, "foo", 2)

            # Should not be able to use alter table with MV
            with pytest.raises(InvalidRequest, match=ALTER_TABLE_ON_VIEW):
                execute(cql, view, "ALTER TABLE %s ADD foo text")

            # Should not be able to use alter table with MV
            with pytest.raises(InvalidRequest, match=ALTER_TABLE_ON_VIEW):
                execute(cql, view, "ALTER TABLE %s WITH compaction = { 'class' : 'LeveledCompactionStrategy' }")

            execute(cql, view, "ALTER MATERIALIZED VIEW %s WITH compaction = { 'class' : 'LeveledCompactionStrategy' }")

            #Test alter add
            execute(cql, table, "ALTER TABLE %s ADD foo text")
            assert viewHasColumn(cql, view, "foo")

            execute(cql, table, "INSERT INTO %s(k,asciival,bigintval,foo)VALUES(?,?,?,?)", 0, "foo", 1, "bar")
            assert_rows(execute(cql, table, "SELECT foo from %s"), row("bar"))

            #Test alter rename
            execute(cql, table, "ALTER TABLE %s RENAME asciival TO bar")

            assert_rows(execute(cql, table, "SELECT bar from %s"), row("foo"))
            assert viewHasColumn(cql, view, "bar")

def testTwoTablesOneView(cql, test_keyspace):
    # The Java test names the tables dummy_table and real_base
    with create_table(cql, test_keyspace, "(" +
                      "j int, " +
                      "intval int, " +
                      "PRIMARY KEY (j))") as dummy_table, \
         create_table(cql, test_keyspace, "(" +
                      "k int, " +
                      "intval int, " +
                      "PRIMARY KEY (k))") as real_base:

        with create_view(cql, real_base, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE k IS NOT NULL AND intval IS NOT NULL PRIMARY KEY (intval, k)") as mv, \
             create_view(cql, dummy_table, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE j IS NOT NULL AND intval IS NOT NULL PRIMARY KEY (intval, j)"):

            execute(cql, real_base, "INSERT INTO %s (k, intval) VALUES (?, ?)", 0, 0)
            assert_rows(execute(cql, real_base, "SELECT k, intval FROM %s WHERE k = ?", 0), row(0, 0))
            assert_rows(execute(cql, mv, "SELECT k, intval from %s WHERE intval = ?", 0), row(0, 0))

            execute(cql, real_base, "INSERT INTO %s (k, intval) VALUES (?, ?)", 0, 1)
            assert_rows(execute(cql, real_base, "SELECT k, intval FROM %s WHERE k = ?", 0), row(0, 1))
            assert_rows(execute(cql, mv, "SELECT k, intval from %s WHERE intval = ?", 1), row(0, 1))

            assert_rows(execute(cql, real_base, "SELECT k, intval FROM %s WHERE k = ?", 0), row(0, 1))
            assert_rows(execute(cql, mv, "SELECT k, intval from %s WHERE intval = ?", 1), row(0, 1))

            execute(cql, dummy_table, "INSERT INTO %s (j, intval) VALUES(?, ?)", 0, 1)
            assert_rows(execute(cql, dummy_table, "SELECT j, intval FROM %s WHERE j = ?", 0), row(0, 1))
            assert_rows(execute(cql, mv, "SELECT k, intval from %s WHERE intval = ?", 1), row(0, 1))

def testReuseName(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(" +
                      "k int, " +
                      "intval int, " +
                      "PRIMARY KEY (k))") as table:

        with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                     "WHERE k IS NOT NULL AND intval IS NOT NULL " +
                                     "PRIMARY KEY (intval, k)") as view:

            execute(cql, table, "INSERT INTO %s (k, intval) VALUES (?, ?)", 0, 0)
            assert_rows(execute(cql, table, "SELECT k, intval FROM %s WHERE k = ?", 0), row(0, 0))
            assert_rows(execute(cql, view, "SELECT k, intval from %s WHERE intval = ?", 0), row(0, 0))

        # Leaving the "with" above dropped the view.
        with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                     "WHERE k IS NOT NULL AND intval IS NOT NULL " +
                                     "PRIMARY KEY (intval, k)", name=view.split('.')[1]):

            execute(cql, table, "INSERT INTO %s (k, intval) VALUES (?, ?)", 0, 1)
            assert_rows(execute(cql, table, "SELECT k, intval FROM %s WHERE k = ?", 0), row(0, 1))
            assert_rows(execute(cql, view, "SELECT k, intval from %s WHERE intval = ?", 1), row(0, 1))

# Reproduces SCYLLADB-5141 (snake_case names of native functions - here,
# from_json()) and #7954 (from_json() can't set a tuple element to null).
@pytest.mark.xfail(reason="SCYLLADB-5141, #7954")
def testAllTypes(cql, test_keyspace):
    with create_type(cql, test_keyspace, "(a int, b uuid, c set<text>)") as myType, \
         create_table(cql, test_keyspace, "(" +
                      "k int PRIMARY KEY, " +
                      "asciival ascii, " +
                      "bigintval bigint, " +
                      "blobval blob, " +
                      "booleanval boolean, " +
                      "dateval date, " +
                      "decimalval decimal, " +
                      "doubleval double, " +
                      "floatval float, " +
                      "inetval inet, " +
                      "intval int, " +
                      "textval text, " +
                      "timeval time, " +
                      "timestampval timestamp, " +
                      "timeuuidval timeuuid, " +
                      "uuidval uuid," +
                      "varcharval varchar, " +
                      "varintval varint, " +
                      "listval list<int>, " +
                      "frozenlistval frozen<list<int>>, " +
                      "setval set<uuid>, " +
                      "frozensetval frozen<set<uuid>>, " +
                      "mapval map<ascii, int>," +
                      "frozenmapval frozen<map<ascii, int>>," +
                      "tupleval frozen<tuple<int, ascii, uuid>>," +
                      "udtval frozen<" + myType + ">)") as table, \
         ExitStack() as stack:

        # The Java test iterates over the table's columns from its metadata.
        # We read them from system_schema.columns instead.
        ks, cf = table.split('.')
        mv = {}
        for c in cql.execute("SELECT column_name, kind, type FROM system_schema.columns WHERE keyspace_name = %s AND table_name = %s", [ks, cf]):
            isMultiCell = c.type.startswith(("list<", "set<", "map<"))
            isPartitionKey = c.kind == "partition_key"
            try:
                mv[c.column_name] = stack.enter_context(create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE " + c.column_name + " IS NOT NULL AND k IS NOT NULL PRIMARY KEY (" + c.column_name + ",k)", wait=False, name="mv_" + c.column_name))

                if isMultiCell:
                    pytest.fail("MV on a multicell should fail " + c.column_name)

                if isPartitionKey:
                    pytest.fail("MV on partition key should fail " + c.column_name)
            except Exception:
                if not isMultiCell and not isPartitionKey:
                    pytest.fail("MV creation failed on " + c.column_name)
        # To make the test faster, we didn't wait for each view to be built
        # after creating it, and wait for all of them together here.
        for view in mv.values():
            wait_for_view_built(cql, view)

        # from_json() can only be used when the receiver type is known
        # Scylla's message is "...() can only be called if receiver type is
        # known".
        assert_invalid_message_re(cql, table, r"from_json\(\) cannot be used in the selection clause|can only be called if receiver type is known", "SELECT from_json(asciival) FROM %s", 0, 0)

        # The Java test also creates two Java UDFs here, but never uses them,
        # so we didn't translate their creation.

        # ================ ascii ================
        execute(cql, table, "INSERT INTO %s (k, asciival) VALUES (?, from_json(?))", 0, "\"ascii text\"")
        assert_rows(execute(cql, table, "SELECT k, asciival FROM %s WHERE k = ?", 0), row(0, "ascii text"))

        execute(cql, table, "INSERT INTO %s (k, asciival) VALUES (?, from_json(?))", 0, "\"ascii \\\" text\"")
        assert_rows(execute(cql, table, "SELECT k, asciival FROM %s WHERE k = ?", 0), row(0, "ascii \" text"))

        # test that we can use from_json() in other valid places in queries
        assert_rows(execute(cql, table, "SELECT asciival FROM %s WHERE k = from_json(?)", "0"), row("ascii \" text"))

        #Check the MV
        assert_empty(execute(cql, mv["asciival"], "SELECT k, udtval from %s WHERE asciival = ?", "ascii text"))
        assert_rows(execute(cql, mv["asciival"], "SELECT k, udtval from %s WHERE asciival = ?", "ascii \" text"), row(0, None))

        execute(cql, table, "UPDATE %s SET asciival = from_json(?) WHERE k = from_json(?)", "\"ascii \\\" text\"", "0")
        assert_rows(execute(cql, mv["asciival"], "SELECT k, udtval from %s WHERE asciival = ?", "ascii \" text"), row(0, None))

        execute(cql, table, "DELETE FROM %s WHERE k = from_json(?)", "0")
        assert_empty(execute(cql, table, "SELECT k, asciival FROM %s WHERE k = ?", 0))
        assert_empty(execute(cql, mv["asciival"], "SELECT k, udtval from %s WHERE asciival = ?", "ascii \" text"))

        execute(cql, table, "INSERT INTO %s (k, asciival) VALUES (?, from_json(?))", 0, "\"ascii text\"")
        assert_rows(execute(cql, mv["asciival"], "SELECT k, udtval from %s WHERE asciival = ?", "ascii text"), row(0, None))

        # ================ bigint ================
        execute(cql, table, "INSERT INTO %s (k, bigintval) VALUES (?, from_json(?))", 0, "123123123123")
        assert_rows(execute(cql, table, "SELECT k, bigintval FROM %s WHERE k = ?", 0), row(0, 123123123123))
        assert_rows(execute(cql, mv["bigintval"], "SELECT k, asciival from %s WHERE bigintval = ?", 123123123123), row(0, "ascii text"))

        # ================ blob ================
        execute(cql, table, "INSERT INTO %s (k, blobval) VALUES (?, from_json(?))", 0, "\"0x00000001\"")
        assert_rows(execute(cql, table, "SELECT k, blobval FROM %s WHERE k = ?", 0), row(0, b"\x00\x00\x00\x01"))
        assert_rows(execute(cql, mv["blobval"], "SELECT k, asciival from %s WHERE blobval = ?", b"\x00\x00\x00\x01"), row(0, "ascii text"))

        # ================ boolean ================
        execute(cql, table, "INSERT INTO %s (k, booleanval) VALUES (?, from_json(?))", 0, "true")
        assert_rows(execute(cql, table, "SELECT k, booleanval FROM %s WHERE k = ?", 0), row(0, True))
        assert_rows(execute(cql, mv["booleanval"], "SELECT k, asciival from %s WHERE booleanval = ?", True), row(0, "ascii text"))

        execute(cql, table, "INSERT INTO %s (k, booleanval) VALUES (?, from_json(?))", 0, "false")
        assert_rows(execute(cql, table, "SELECT k, booleanval FROM %s WHERE k = ?", 0), row(0, False))
        assert_empty(execute(cql, mv["booleanval"], "SELECT k, asciival from %s WHERE booleanval = ?", True))
        assert_rows(execute(cql, mv["booleanval"], "SELECT k, asciival from %s WHERE booleanval = ?", False), row(0, "ascii text"))

        # ================ date ================
        execute(cql, table, "INSERT INTO %s (k, dateval) VALUES (?, from_json(?))", 0, "\"1987-03-23\"")
        assert_rows(execute(cql, table, "SELECT k, dateval FROM %s WHERE k = ?", 0), row(0, Date("1987-03-23")))
        assert_rows(execute(cql, mv["dateval"], "SELECT k, asciival from %s WHERE dateval = from_json(?)", "\"1987-03-23\""), row(0, "ascii text"))

        # ================ decimal ================
        execute(cql, table, "INSERT INTO %s (k, decimalval) VALUES (?, from_json(?))", 0, "123123.123123")
        assert_rows(execute(cql, table, "SELECT k, decimalval FROM %s WHERE k = ?", 0), row(0, Decimal("123123.123123")))
        assert_rows(execute(cql, mv["decimalval"], "SELECT k, asciival from %s WHERE decimalval = from_json(?)", "123123.123123"), row(0, "ascii text"))

        execute(cql, table, "INSERT INTO %s (k, decimalval) VALUES (?, from_json(?))", 0, "123123")
        assert_rows(execute(cql, table, "SELECT k, decimalval FROM %s WHERE k = ?", 0), row(0, Decimal("123123")))
        assert_empty(execute(cql, mv["decimalval"], "SELECT k, asciival from %s WHERE decimalval = from_json(?)", "123123.123123"))
        assert_rows(execute(cql, mv["decimalval"], "SELECT k, asciival from %s WHERE decimalval = from_json(?)", "123123"), row(0, "ascii text"))

        # accept strings for numbers that cannot be represented as doubles
        execute(cql, table, "INSERT INTO %s (k, decimalval) VALUES (?, from_json(?))", 0, "\"123123.123123\"")
        assert_rows(execute(cql, table, "SELECT k, decimalval FROM %s WHERE k = ?", 0), row(0, Decimal("123123.123123")))

        execute(cql, table, "INSERT INTO %s (k, decimalval) VALUES (?, from_json(?))", 0, "\"-1.23E-12\"")
        assert_rows(execute(cql, table, "SELECT k, decimalval FROM %s WHERE k = ?", 0), row(0, Decimal("-1.23E-12")))
        assert_rows(execute(cql, mv["decimalval"], "SELECT k, asciival from %s WHERE decimalval = from_json(?)", "\"-1.23E-12\""), row(0, "ascii text"))

        # ================ double ================
        execute(cql, table, "INSERT INTO %s (k, doubleval) VALUES (?, from_json(?))", 0, "123123.123123")
        assert_rows(execute(cql, table, "SELECT k, doubleval FROM %s WHERE k = ?", 0), row(0, 123123.123123))
        assert_rows(execute(cql, mv["doubleval"], "SELECT k, asciival from %s WHERE doubleval = from_json(?)", "123123.123123"), row(0, "ascii text"))

        execute(cql, table, "INSERT INTO %s (k, doubleval) VALUES (?, from_json(?))", 0, "123123")
        assert_rows(execute(cql, table, "SELECT k, doubleval FROM %s WHERE k = ?", 0), row(0, 123123.0))
        assert_rows(execute(cql, mv["doubleval"], "SELECT k, asciival from %s WHERE doubleval = from_json(?)", "123123"), row(0, "ascii text"))

        # ================ float ================
        execute(cql, table, "INSERT INTO %s (k, floatval) VALUES (?, from_json(?))", 0, "123123.123123")
        assert_rows(execute(cql, table, "SELECT k, floatval FROM %s WHERE k = ?", 0), row(0, to_float(123123.123123)))
        assert_rows(execute(cql, mv["floatval"], "SELECT k, asciival from %s WHERE floatval = from_json(?)", "123123.123123"), row(0, "ascii text"))

        execute(cql, table, "INSERT INTO %s (k, floatval) VALUES (?, from_json(?))", 0, "123123")
        assert_rows(execute(cql, table, "SELECT k, floatval FROM %s WHERE k = ?", 0), row(0, 123123.0))
        assert_rows(execute(cql, mv["floatval"], "SELECT k, asciival from %s WHERE floatval = from_json(?)", "123123"), row(0, "ascii text"))

        # ================ inet ================
        execute(cql, table, "INSERT INTO %s (k, inetval) VALUES (?, from_json(?))", 0, "\"127.0.0.1\"")
        assert_rows(execute(cql, table, "SELECT k, inetval FROM %s WHERE k = ?", 0), row(0, "127.0.0.1"))
        assert_rows(execute(cql, mv["inetval"], "SELECT k, asciival from %s WHERE inetval = from_json(?)", "\"127.0.0.1\""), row(0, "ascii text"))

        execute(cql, table, "INSERT INTO %s (k, inetval) VALUES (?, from_json(?))", 0, "\"::1\"")
        assert_rows(execute(cql, table, "SELECT k, inetval FROM %s WHERE k = ?", 0), row(0, "::1"))
        assert_empty(execute(cql, mv["inetval"], "SELECT k, asciival from %s WHERE inetval = from_json(?)", "\"127.0.0.1\""))
        assert_rows(execute(cql, mv["inetval"], "SELECT k, asciival from %s WHERE inetval = from_json(?)", "\"::1\""), row(0, "ascii text"))

        # ================ int ================
        execute(cql, table, "INSERT INTO %s (k, intval) VALUES (?, from_json(?))", 0, "123123")
        assert_rows(execute(cql, table, "SELECT k, intval FROM %s WHERE k = ?", 0), row(0, 123123))
        assert_rows(execute(cql, mv["intval"], "SELECT k, asciival from %s WHERE intval = from_json(?)", "123123"), row(0, "ascii text"))

        # ================ text (varchar) ================
        execute(cql, table, "INSERT INTO %s (k, textval) VALUES (?, from_json(?))", 0, "\"some \\\" text\"")
        assert_rows(execute(cql, table, "SELECT k, textval FROM %s WHERE k = ?", 0), row(0, "some \" text"))

        execute(cql, table, "INSERT INTO %s (k, textval) VALUES (?, from_json(?))", 0, "\"\\u2013\"")
        assert_rows(execute(cql, table, "SELECT k, textval FROM %s WHERE k = ?", 0), row(0, "\u2013"))
        assert_rows(execute(cql, mv["textval"], "SELECT k, asciival from %s WHERE textval = from_json(?)", "\"\\u2013\""), row(0, "ascii text"))

        execute(cql, table, "INSERT INTO %s (k, textval) VALUES (?, from_json(?))", 0, "\"abcd\"")
        assert_rows(execute(cql, table, "SELECT k, textval FROM %s WHERE k = ?", 0), row(0, "abcd"))
        assert_rows(execute(cql, mv["textval"], "SELECT k, asciival from %s WHERE textval = from_json(?)", "\"abcd\""), row(0, "ascii text"))

        # ================ time ================
        execute(cql, table, "INSERT INTO %s (k, timeval) VALUES (?, from_json(?))", 0, "\"07:35:07.000111222\"")
        assert_rows(execute(cql, table, "SELECT k, timeval FROM %s WHERE k = ?", 0), row(0, Time("07:35:07.000111222")))
        assert_rows(execute(cql, mv["timeval"], "SELECT k, asciival from %s WHERE timeval = from_json(?)", "\"07:35:07.000111222\""), row(0, "ascii text"))

        # ================ timestamp ================
        execute(cql, table, "INSERT INTO %s (k, timestampval) VALUES (?, from_json(?))", 0, "123123123123")
        assert_rows(execute(cql, table, "SELECT k, timestampval FROM %s WHERE k = ?", 0), row(0, datetime.fromtimestamp(123123123.123, timezone.utc).replace(tzinfo=None)))
        assert_rows(execute(cql, mv["timestampval"], "SELECT k, asciival from %s WHERE timestampval = from_json(?)", "123123123123"), row(0, "ascii text"))

        execute(cql, table, "INSERT INTO %s (k, timestampval) VALUES (?, from_json(?))", 0, "\"2014-01-01\"")
        # A timestamp without a timezone is parsed in the server's timezone,
        # which the Java test assumes is the test's local timezone. That isn't
        # true when the server runs in a docker container (in UTC), so we
        # accept midnight in either UTC or the local timezone.
        assert execute(cql, table, "SELECT timestampval FROM %s WHERE k = ?", 0).one().timestampval in [
            datetime(2014, 1, 1),
            datetime.fromtimestamp(datetime(2014, 1, 1).timestamp(), timezone.utc).replace(tzinfo=None)]
        assert_rows(execute(cql, mv["timestampval"], "SELECT k, asciival from %s WHERE timestampval = from_json(?)", "\"2014-01-01\""), row(0, "ascii text"))

        # ================ timeuuid ================
        execute(cql, table, "INSERT INTO %s (k, timeuuidval) VALUES (?, from_json(?))", 0, "\"6bddc89a-5644-11e4-97fc-56847afe9799\"")
        assert_rows(execute(cql, table, "SELECT k, timeuuidval FROM %s WHERE k = ?", 0), row(0, UUID("6bddc89a-5644-11e4-97fc-56847afe9799")))

        execute(cql, table, "INSERT INTO %s (k, timeuuidval) VALUES (?, from_json(?))", 0, "\"6BDDC89A-5644-11E4-97FC-56847AFE9799\"")
        assert_rows(execute(cql, table, "SELECT k, timeuuidval FROM %s WHERE k = ?", 0), row(0, UUID("6bddc89a-5644-11e4-97fc-56847afe9799")))
        assert_rows(execute(cql, mv["timeuuidval"], "SELECT k, asciival from %s WHERE timeuuidval = from_json(?)", "\"6BDDC89A-5644-11E4-97FC-56847AFE9799\""), row(0, "ascii text"))

        # ================ uuidval ================
        execute(cql, table, "INSERT INTO %s (k, uuidval) VALUES (?, from_json(?))", 0, "\"6bddc89a-5644-11e4-97fc-56847afe9799\"")
        assert_rows(execute(cql, table, "SELECT k, uuidval FROM %s WHERE k = ?", 0), row(0, UUID("6bddc89a-5644-11e4-97fc-56847afe9799")))

        execute(cql, table, "INSERT INTO %s (k, uuidval) VALUES (?, from_json(?))", 0, "\"6BDDC89A-5644-11E4-97FC-56847AFE9799\"")
        assert_rows(execute(cql, table, "SELECT k, uuidval FROM %s WHERE k = ?", 0), row(0, UUID("6bddc89a-5644-11e4-97fc-56847afe9799")))
        assert_rows(execute(cql, mv["uuidval"], "SELECT k, asciival from %s WHERE uuidval = from_json(?)", "\"6BDDC89A-5644-11E4-97FC-56847AFE9799\""), row(0, "ascii text"))

        # ================ varint ================
        execute(cql, table, "INSERT INTO %s (k, varintval) VALUES (?, from_json(?))", 0, "123123123123")
        assert_rows(execute(cql, table, "SELECT k, varintval FROM %s WHERE k = ?", 0), row(0, 123123123123))
        assert_rows(execute(cql, mv["varintval"], "SELECT k, asciival from %s WHERE varintval = from_json(?)", "123123123123"), row(0, "ascii text"))

        # accept strings for numbers that cannot be represented as longs
        execute(cql, table, "INSERT INTO %s (k, varintval) VALUES (?, from_json(?))", 0, "\"1234567890123456789012345678901234567890\"")
        assert_rows(execute(cql, table, "SELECT k, varintval FROM %s WHERE k = ?", 0), row(0, 1234567890123456789012345678901234567890))
        assert_rows(execute(cql, mv["varintval"], "SELECT k, asciival from %s WHERE varintval = from_json(?)", "\"1234567890123456789012345678901234567890\""), row(0, "ascii text"))

        # ================ lists ================
        execute(cql, table, "INSERT INTO %s (k, listval) VALUES (?, from_json(?))", 0, "[1, 2, 3]")
        assert_rows(execute(cql, table, "SELECT k, listval FROM %s WHERE k = ?", 0), row(0, [1, 2, 3]))
        assert_rows(execute(cql, mv["textval"], "SELECT k, listval from %s WHERE textval = from_json(?)", "\"abcd\""), row(0, [1, 2, 3]))

        execute(cql, table, "INSERT INTO %s (k, listval) VALUES (?, from_json(?))", 0, "[1]")
        assert_rows(execute(cql, table, "SELECT k, listval FROM %s WHERE k = ?", 0), row(0, [1]))
        assert_rows(execute(cql, mv["textval"], "SELECT k, listval from %s WHERE textval = from_json(?)", "\"abcd\""), row(0, [1]))

        execute(cql, table, "UPDATE %s SET listval = listval + from_json(?) WHERE k = ?", "[2]", 0)
        assert_rows(execute(cql, table, "SELECT k, listval FROM %s WHERE k = ?", 0), row(0, [1, 2]))
        assert_rows(execute(cql, mv["textval"], "SELECT k, listval from %s WHERE textval = from_json(?)", "\"abcd\""), row(0, [1, 2]))

        execute(cql, table, "UPDATE %s SET listval = from_json(?) + listval WHERE k = ?", "[0]", 0)
        assert_rows(execute(cql, table, "SELECT k, listval FROM %s WHERE k = ?", 0), row(0, [0, 1, 2]))
        assert_rows(execute(cql, mv["textval"], "SELECT k, listval from %s WHERE textval = from_json(?)", "\"abcd\""), row(0, [0, 1, 2]))

        execute(cql, table, "UPDATE %s SET listval[1] = from_json(?) WHERE k = ?", "10", 0)
        assert_rows(execute(cql, table, "SELECT k, listval FROM %s WHERE k = ?", 0), row(0, [0, 10, 2]))
        assert_rows(execute(cql, mv["textval"], "SELECT k, listval from %s WHERE textval = from_json(?)", "\"abcd\""), row(0, [0, 10, 2]))

        execute(cql, table, "DELETE listval[1] FROM %s WHERE k = ?", 0)
        assert_rows(execute(cql, table, "SELECT k, listval FROM %s WHERE k = ?", 0), row(0, [0, 2]))
        assert_rows(execute(cql, mv["textval"], "SELECT k, listval from %s WHERE textval = from_json(?)", "\"abcd\""), row(0, [0, 2]))

        execute(cql, table, "INSERT INTO %s (k, listval) VALUES (?, from_json(?))", 0, "[]")
        assert_rows(execute(cql, table, "SELECT k, listval FROM %s WHERE k = ?", 0), row(0, None))
        assert_rows(execute(cql, mv["textval"], "SELECT k, listval from %s WHERE textval = from_json(?)", "\"abcd\""), row(0, None))

        # frozen
        execute(cql, table, "INSERT INTO %s (k, frozenlistval) VALUES (?, from_json(?))", 0, "[1, 2, 3]")
        assert_rows(execute(cql, table, "SELECT k, frozenlistval FROM %s WHERE k = ?", 0), row(0, [1, 2, 3]))
        assert_rows(execute(cql, mv["textval"], "SELECT k, frozenlistval from %s WHERE textval = from_json(?)", "\"abcd\""), row(0, [1, 2, 3]))
        assert_rows(execute(cql, mv["frozenlistval"], "SELECT k, textval from %s where frozenlistval = from_json(?)", "[1, 2, 3]"), row(0, "abcd"))

        execute(cql, table, "INSERT INTO %s (k, frozenlistval) VALUES (?, from_json(?))", 0, "[3, 2, 1]")
        assert_rows(execute(cql, table, "SELECT k, frozenlistval FROM %s WHERE k = ?", 0), row(0, [3, 2, 1]))
        assert_empty(execute(cql, mv["frozenlistval"], "SELECT k, textval from %s where frozenlistval = from_json(?)", "[1, 2, 3]"))
        assert_rows(execute(cql, mv["frozenlistval"], "SELECT k, textval from %s where frozenlistval = from_json(?)", "[3, 2, 1]"), row(0, "abcd"))
        assert_rows(execute(cql, mv["textval"], "SELECT k, frozenlistval from %s WHERE textval = from_json(?)", "\"abcd\""), row(0, [3, 2, 1]))

        execute(cql, table, "INSERT INTO %s (k, frozenlistval) VALUES (?, from_json(?))", 0, "[]")
        assert_rows(execute(cql, table, "SELECT k, frozenlistval FROM %s WHERE k = ?", 0), row(0, []))
        assert_rows(execute(cql, mv["textval"], "SELECT k, frozenlistval from %s WHERE textval = from_json(?)", "\"abcd\""), row(0, []))

        # ================ sets ================
        execute(cql, table, "INSERT INTO %s (k, setval) VALUES (?, from_json(?))",
                0, "[\"6bddc89a-5644-11e4-97fc-56847afe9798\", \"6bddc89a-5644-11e4-97fc-56847afe9799\"]")
        assert_rows(execute(cql, table, "SELECT k, setval FROM %s WHERE k = ?", 0),
                    row(0, {UUID("6bddc89a-5644-11e4-97fc-56847afe9798"), UUID("6bddc89a-5644-11e4-97fc-56847afe9799")}))
        assert_rows(execute(cql, mv["textval"], "SELECT k, setval from %s WHERE textval = from_json(?)", "\"abcd\""),
                    row(0, {UUID("6bddc89a-5644-11e4-97fc-56847afe9798"), UUID("6bddc89a-5644-11e4-97fc-56847afe9799")}))

        # duplicates are okay, just like in CQL
        execute(cql, table, "INSERT INTO %s (k, setval) VALUES (?, from_json(?))",
                0, "[\"6bddc89a-5644-11e4-97fc-56847afe9798\", \"6bddc89a-5644-11e4-97fc-56847afe9798\", \"6bddc89a-5644-11e4-97fc-56847afe9799\"]")
        assert_rows(execute(cql, table, "SELECT k, setval FROM %s WHERE k = ?", 0),
                    row(0, {UUID("6bddc89a-5644-11e4-97fc-56847afe9798"), UUID("6bddc89a-5644-11e4-97fc-56847afe9799")}))
        assert_rows(execute(cql, mv["textval"], "SELECT k, setval from %s WHERE textval = from_json(?)", "\"abcd\""),
                    row(0, {UUID("6bddc89a-5644-11e4-97fc-56847afe9798"), UUID("6bddc89a-5644-11e4-97fc-56847afe9799")}))

        execute(cql, table, "UPDATE %s SET setval = setval + from_json(?) WHERE k = ?", "[\"6bddc89a-5644-0000-97fc-56847afe9799\"]", 0)
        assert_rows(execute(cql, table, "SELECT k, setval FROM %s WHERE k = ?", 0),
                    row(0, {UUID("6bddc89a-5644-0000-97fc-56847afe9799"), UUID("6bddc89a-5644-11e4-97fc-56847afe9798"), UUID("6bddc89a-5644-11e4-97fc-56847afe9799")}))
        assert_rows(execute(cql, mv["textval"], "SELECT k, setval from %s WHERE textval = from_json(?)", "\"abcd\""),
                    row(0, {UUID("6bddc89a-5644-0000-97fc-56847afe9799"), UUID("6bddc89a-5644-11e4-97fc-56847afe9798"), UUID("6bddc89a-5644-11e4-97fc-56847afe9799")}))

        execute(cql, table, "UPDATE %s SET setval = setval - from_json(?) WHERE k = ?", "[\"6bddc89a-5644-0000-97fc-56847afe9799\"]", 0)
        assert_rows(execute(cql, table, "SELECT k, setval FROM %s WHERE k = ?", 0),
                    row(0, {UUID("6bddc89a-5644-11e4-97fc-56847afe9798"), UUID("6bddc89a-5644-11e4-97fc-56847afe9799")}))
        assert_rows(execute(cql, mv["textval"], "SELECT k, setval from %s WHERE textval = from_json(?)", "\"abcd\""),
                    row(0, {UUID("6bddc89a-5644-11e4-97fc-56847afe9798"), UUID("6bddc89a-5644-11e4-97fc-56847afe9799")}))

        execute(cql, table, "INSERT INTO %s (k, setval) VALUES (?, from_json(?))", 0, "[]")
        assert_rows(execute(cql, table, "SELECT k, setval FROM %s WHERE k = ?", 0), row(0, None))
        assert_rows(execute(cql, mv["textval"], "SELECT k, setval from %s WHERE textval = from_json(?)", "\"abcd\""),
                    row(0, None))


        # frozen
        execute(cql, table, "INSERT INTO %s (k, frozensetval) VALUES (?, from_json(?))",
                0, "[\"6bddc89a-5644-11e4-97fc-56847afe9798\", \"6bddc89a-5644-11e4-97fc-56847afe9799\"]")
        assert_rows(execute(cql, table, "SELECT k, frozensetval FROM %s WHERE k = ?", 0),
                    row(0, {UUID("6bddc89a-5644-11e4-97fc-56847afe9798"), UUID("6bddc89a-5644-11e4-97fc-56847afe9799")}))
        assert_rows(execute(cql, mv["textval"], "SELECT k, frozensetval from %s WHERE textval = from_json(?)", "\"abcd\""),
                    row(0, {UUID("6bddc89a-5644-11e4-97fc-56847afe9798"), UUID("6bddc89a-5644-11e4-97fc-56847afe9799")}))

        execute(cql, table, "INSERT INTO %s (k, frozensetval) VALUES (?, from_json(?))",
                0, "[\"6bddc89a-0000-11e4-97fc-56847afe9799\", \"6bddc89a-5644-11e4-97fc-56847afe9798\"]")
        assert_rows(execute(cql, table, "SELECT k, frozensetval FROM %s WHERE k = ?", 0),
                    row(0, {UUID("6bddc89a-0000-11e4-97fc-56847afe9799"), UUID("6bddc89a-5644-11e4-97fc-56847afe9798")}))
        assert_rows(execute(cql, mv["textval"], "SELECT k, frozensetval from %s WHERE textval = from_json(?)", "\"abcd\""),
                    row(0, {UUID("6bddc89a-0000-11e4-97fc-56847afe9799"), UUID("6bddc89a-5644-11e4-97fc-56847afe9798")}))

        # ================ maps ================
        execute(cql, table, "INSERT INTO %s (k, mapval) VALUES (?, from_json(?))", 0, "{\"a\": 1, \"b\": 2}")
        assert_rows(execute(cql, table, "SELECT k, mapval FROM %s WHERE k = ?", 0), row(0, {"a": 1, "b": 2}))
        assert_rows(execute(cql, mv["textval"], "SELECT k, mapval from %s WHERE textval = from_json(?)", "\"abcd\""), row(0, {"a": 1, "b": 2}))

        execute(cql, table, "UPDATE %s SET mapval[?] = ?  WHERE k = ?", "c", 3, 0)
        assert_rows(execute(cql, table, "SELECT k, mapval FROM %s WHERE k = ?", 0),
                    row(0, {"a": 1, "b": 2, "c": 3}))
        assert_rows(execute(cql, mv["textval"], "SELECT k, mapval from %s WHERE textval = from_json(?)", "\"abcd\""),
                    row(0, {"a": 1, "b": 2, "c": 3}))

        execute(cql, table, "UPDATE %s SET mapval[?] = ?  WHERE k = ?", "b", 10, 0)
        assert_rows(execute(cql, table, "SELECT k, mapval FROM %s WHERE k = ?", 0),
                    row(0, {"a": 1, "b": 10, "c": 3}))
        assert_rows(execute(cql, mv["textval"], "SELECT k, mapval from %s WHERE textval = from_json(?)", "\"abcd\""),
                    row(0, {"a": 1, "b": 10, "c": 3}))

        execute(cql, table, "DELETE mapval[?] FROM %s WHERE k = ?", "b", 0)
        assert_rows(execute(cql, table, "SELECT k, mapval FROM %s WHERE k = ?", 0),
                    row(0, {"a": 1, "c": 3}))
        assert_rows(execute(cql, mv["textval"], "SELECT k, mapval from %s WHERE textval = from_json(?)", "\"abcd\""),
                    row(0, {"a": 1, "c": 3}))

        execute(cql, table, "INSERT INTO %s (k, mapval) VALUES (?, from_json(?))", 0, "{}")
        assert_rows(execute(cql, table, "SELECT k, mapval FROM %s WHERE k = ?", 0), row(0, None))
        assert_rows(execute(cql, mv["textval"], "SELECT k, mapval from %s WHERE textval = from_json(?)", "\"abcd\""),
                    row(0, None))

        # frozen
        execute(cql, table, "INSERT INTO %s (k, frozenmapval) VALUES (?, from_json(?))", 0, "{\"a\": 1, \"b\": 2}")
        assert_rows(execute(cql, table, "SELECT k, frozenmapval FROM %s WHERE k = ?", 0), row(0, {"a": 1, "b": 2}))
        assert_rows(execute(cql, mv["frozenmapval"], "SELECT k, textval FROM %s WHERE frozenmapval = from_json(?)", "{\"a\": 1, \"b\": 2}"), row(0, "abcd"))

        execute(cql, table, "INSERT INTO %s (k, frozenmapval) VALUES (?, from_json(?))", 0, "{\"b\": 2, \"a\": 3}")
        assert_rows(execute(cql, table, "SELECT k, frozenmapval FROM %s WHERE k = ?", 0), row(0, {"a": 3, "b": 2}))
        assert_rows(execute(cql, table, "SELECT k, frozenmapval FROM %s WHERE k = ?", 0), row(0, {"a": 3, "b": 2}))

        # ================ tuples ================
        execute(cql, table, "INSERT INTO %s (k, tupleval) VALUES (?, from_json(?))", 0, "[1, \"foobar\", \"6bddc89a-5644-11e4-97fc-56847afe9799\"]")
        assert_rows(execute(cql, table, "SELECT k, tupleval FROM %s WHERE k = ?", 0),
                    row(0, (1, "foobar", UUID("6bddc89a-5644-11e4-97fc-56847afe9799"))))
        assert_rows(execute(cql, mv["tupleval"], "SELECT k, textval FROM %s WHERE tupleval = ?", (1, "foobar", UUID("6bddc89a-5644-11e4-97fc-56847afe9799"))),
                    row(0, "abcd"))

        execute(cql, table, "INSERT INTO %s (k, tupleval) VALUES (?, from_json(?))", 0, "[1, null, \"6bddc89a-5644-11e4-97fc-56847afe9799\"]")
        assert_rows(execute(cql, table, "SELECT k, tupleval FROM %s WHERE k = ?", 0),
                    row(0, (1, None, UUID("6bddc89a-5644-11e4-97fc-56847afe9799"))))
        assert_empty(execute(cql, mv["tupleval"], "SELECT k, textval FROM %s WHERE tupleval = ?", (1, "foobar", UUID("6bddc89a-5644-11e4-97fc-56847afe9799"))))
        assert_rows(execute(cql, mv["tupleval"], "SELECT k, textval FROM %s WHERE tupleval = ?", (1, None, UUID("6bddc89a-5644-11e4-97fc-56847afe9799"))),
                    row(0, "abcd"))

        # ================ UDTs ================
        execute(cql, table, "INSERT INTO %s (k, udtval) VALUES (?, from_json(?))", 0, "{\"a\": 1, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\", \"c\": [\"foo\", \"bar\"]}")
        assert_rows(execute(cql, table, "SELECT k, udtval.a, udtval.b, udtval.c FROM %s WHERE k = ?", 0),
                    row(0, 1, UUID("6bddc89a-5644-11e4-97fc-56847afe9799"), {"bar", "foo"}))
        assert_rows(execute(cql, mv["udtval"], "SELECT k, textval FROM %s WHERE udtval = from_json(?)", "{\"a\": 1, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\", \"c\": [\"foo\", \"bar\"]}"),
                    row(0, "abcd"))

        # order of fields shouldn't matter
        execute(cql, table, "INSERT INTO %s (k, udtval) VALUES (?, from_json(?))", 0, "{\"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\", \"a\": 1, \"c\": [\"foo\", \"bar\"]}")
        assert_rows(execute(cql, table, "SELECT k, udtval.a, udtval.b, udtval.c FROM %s WHERE k = ?", 0),
                    row(0, 1, UUID("6bddc89a-5644-11e4-97fc-56847afe9799"), {"bar", "foo"}))
        assert_rows(execute(cql, mv["udtval"], "SELECT k, textval FROM %s WHERE udtval = from_json(?)", "{\"a\": 1, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\", \"c\": [\"foo\", \"bar\"]}"),
                    row(0, "abcd"))

        # test nulls
        execute(cql, table, "INSERT INTO %s (k, udtval) VALUES (?, from_json(?))", 0, "{\"a\": null, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\", \"c\": [\"foo\", \"bar\"]}")
        assert_rows(execute(cql, table, "SELECT k, udtval.a, udtval.b, udtval.c FROM %s WHERE k = ?", 0),
                    row(0, None, UUID("6bddc89a-5644-11e4-97fc-56847afe9799"), {"bar", "foo"}))
        assert_empty(execute(cql, mv["udtval"], "SELECT k, textval FROM %s WHERE udtval = from_json(?)", "{\"a\": 1, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\", \"c\": [\"foo\", \"bar\"]}"))
        assert_rows(execute(cql, mv["udtval"], "SELECT k, textval FROM %s WHERE udtval = from_json(?)", "{\"a\": null, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\", \"c\": [\"foo\", \"bar\"]}"),
                    row(0, "abcd"))

        # test missing fields
        execute(cql, table, "INSERT INTO %s (k, udtval) VALUES (?, from_json(?))", 0, "{\"a\": 1, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\"}")
        assert_rows(execute(cql, table, "SELECT k, udtval.a, udtval.b, udtval.c FROM %s WHERE k = ?", 0),
                    row(0, 1, UUID("6bddc89a-5644-11e4-97fc-56847afe9799"), None))
        assert_empty(execute(cql, mv["udtval"], "SELECT k, textval FROM %s WHERE udtval = from_json(?)", "{\"a\": null, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\", \"c\": [\"foo\", \"bar\"]}"))
        assert_rows(execute(cql, mv["udtval"], "SELECT k, textval FROM %s WHERE udtval = from_json(?)", "{\"a\": 1, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\"}"),
                    row(0, "abcd"))

def testDropTableWithMV(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(" +
                      "a int," +
                      "b int," +
                      "c int," +
                      "d int," +
                      "PRIMARY KEY (a, b, c))") as table:

        with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE a IS NOT NULL AND b IS NOT NULL AND c IS NOT NULL PRIMARY KEY (a, b, c)") as mv:

            # Cassandra's message is "Cannot use DROP TABLE on a materialized
            # view. Please use DROP MATERIALIZED VIEW instead.", Scylla's is
            # "Cannot use DROP TABLE on Materialized View. (Did you mean DROP
            # MATERIALIZED VIEW)?"
            with pytest.raises(InvalidRequest, match="Cannot use DROP TABLE on (a materialized view|Materialized View)"):
                cql.execute("DROP TABLE " + mv)

def testCreateMVWithFilteringOnNonPkColumn(cql, test_keyspace):
    # SEE CASSANDRA-13798, we cannot properly support non-pk base column filtering for mv without huge storage
    # format changes.
    with create_table(cql, test_keyspace, "( a int, b int, c int, d int, PRIMARY KEY (a, b, c))") as table:

        # Scylla's message is "Non-primary key columns cannot be restricted
        # in the SELECT statement used for materialized view ... creation".
        assert_invalid_message_re(cql, table, "Non-primary key columns (can only|cannot) be restricted",
                               "CREATE MATERIALIZED VIEW " + test_keyspace + "." + unique_name() + " AS SELECT * FROM %s "
                               + "WHERE b IS NOT NULL AND c IS NOT NULL AND a IS NOT NULL "
                               + "AND d = 1 PRIMARY KEY (c, b, a)")

def testViewTokenRestrictions(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, PRIMARY KEY(a))") as table:

        execute(cql, table, "INSERT into %s (a,b,c,d) VALUES (?,?,?,?)", 1, 2, 3, 4)

        # Cassandra rejects a token restriction in a view's WHERE clause
        # (CASSANDRA-13464), with the message "Cannot use token relation when
        # defining a materialized view". Scylla allows it, and the view then
        # holds only the base rows matching it. So we check that the view is
        # either rejected or holds the right rows.
        view = test_keyspace + "." + unique_name()
        try:
            cql.execute("CREATE MATERIALIZED VIEW " + view + " AS SELECT a,b,c FROM " + table + " WHERE a IS NOT NULL and b IS NOT NULL and token(a) = token(1) PRIMARY KEY(b,a)")
        except InvalidRequest as e:
            assert "Cannot use token relation when defining a materialized view" in str(e)
            return
        try:
            wait_for_view_built(cql, view)
            assert_rows(cql.execute("SELECT * FROM " + view), row(2, 1, 3))
            execute(cql, table, "INSERT into %s (a,b,c,d) VALUES (?,?,?,?)", 1, 5, 6, 7)
            execute(cql, table, "INSERT into %s (a,b,c,d) VALUES (?,?,?,?)", 2, 8, 9, 10)
            assert_rows(cql.execute("SELECT * FROM " + view), row(5, 1, 6))
        finally:
            cql.execute("DROP MATERIALIZED VIEW " + view)

def testCreateViewWithClusteringOrderOnMvOnly(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(" +
                      "pk int, " +
                      "c1 int," +
                      "c2 int," +
                      "c3 int," +
                      "v int, " +
                      "PRIMARY KEY (pk, c1, c2, c3))") as table:

        with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE pk IS NOT NULL AND c1 IS NOT NULL AND c2 IS NOT NULL and c3 IS NOT NULL PRIMARY KEY (pk, c2, c1, c3) WITH CLUSTERING ORDER BY (c2 DESC, c1 ASC, c3 ASC)") as mv1, \
             create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE pk IS NOT NULL AND c1 IS NOT NULL AND c2 IS NOT NULL and c3 IS NOT NULL PRIMARY KEY (pk, c2, c1, c3) WITH CLUSTERING ORDER BY (c2 ASC, c1 DESC, c3 DESC)") as mv2:

            execute(cql, table, "INSERT INTO %s (pk, c1, c2, c3, v) VALUES (?, ?, ?, ?, ?)", 0, 0, 0, 0, 0)
            execute(cql, table, "INSERT INTO %s (pk, c1, c2, c3, v) VALUES (?, ?, ?, ?, ?)", 0, 0, 0, 1, 1)
            execute(cql, table, "INSERT INTO %s (pk, c1, c2, c3, v) VALUES (?, ?, ?, ?, ?)", 0, 0, 0, 2, 2)
            execute(cql, table, "INSERT INTO %s (pk, c1, c2, c3, v) VALUES (?, ?, ?, ?, ?)", 0, 0, 1, 0, 3)
            execute(cql, table, "INSERT INTO %s (pk, c1, c2, c3, v) VALUES (?, ?, ?, ?, ?)", 0, 0, 1, 1, 4)
            execute(cql, table, "INSERT INTO %s (pk, c1, c2, c3, v) VALUES (?, ?, ?, ?, ?)", 0, 0, 1, 2, 5)
            execute(cql, table, "INSERT INTO %s (pk, c1, c2, c3, v) VALUES (?, ?, ?, ?, ?)", 0, 1, 1, 1, 6)
            execute(cql, table, "INSERT INTO %s (pk, c1, c2, c3, v) VALUES (?, ?, ?, ?, ?)", 0, 1, 2, 1, 7)
            execute(cql, table, "INSERT INTO %s (pk, c1, c2, c3, v) VALUES (?, ?, ?, ?, ?)", 0, 2, 1, 1, 8)

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = ?", 0),
                        row(0, 0, 0, 0, 0),
                        row(0, 0, 0, 1, 1),
                        row(0, 0, 0, 2, 2),
                        row(0, 0, 1, 0, 3),
                        row(0, 0, 1, 1, 4),
                        row(0, 0, 1, 2, 5),
                        row(0, 1, 1, 1, 6),
                        row(0, 1, 2, 1, 7),
                        row(0, 2, 1, 1, 8))

            assert_rows(execute(cql, mv1, "SELECT * FROM %s WHERE pk = ?", 0),
                        row(0, 2, 1, 1, 7),
                        row(0, 1, 0, 0, 3),
                        row(0, 1, 0, 1, 4),
                        row(0, 1, 0, 2, 5),
                        row(0, 1, 1, 1, 6),
                        row(0, 1, 2, 1, 8),
                        row(0, 0, 0, 0, 0),
                        row(0, 0, 0, 1, 1),
                        row(0, 0, 0, 2, 2))

            assert_rows(execute(cql, mv2, "SELECT * FROM %s WHERE pk = ?", 0),
                        row(0, 0, 0, 2, 2),
                        row(0, 0, 0, 1, 1),
                        row(0, 0, 0, 0, 0),
                        row(0, 1, 2, 1, 8),
                        row(0, 1, 1, 1, 6),
                        row(0, 1, 0, 2, 5),
                        row(0, 1, 0, 1, 4),
                        row(0, 1, 0, 0, 3),
                        row(0, 2, 1, 1, 7))

# Reproduces #12308 (a view without its own CLUSTERING ORDER BY should get
# the base table's clustering order for its clustering columns)
@pytest.mark.xfail(reason="#12308")
def testCreateViewWithClusteringOrderOnBaseTableAndMv(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(" +
                      "pk int, " +
                      "c1 int," +
                      "c2 int," +
                      "c3 int," +
                      "v int, " +
                      "PRIMARY KEY (pk, c1, c2, c3)) WITH CLUSTERING ORDER BY (c1 DESC, c2 ASC, c3 DESC)") as table:

        with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE pk IS NOT NULL AND c1 IS NOT NULL AND c2 IS NOT NULL and c3 IS NOT NULL PRIMARY KEY (pk, c2, c1, c3)") as mv1, \
             create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE pk IS NOT NULL AND c1 IS NOT NULL AND c2 IS NOT NULL and c3 IS NOT NULL PRIMARY KEY (pk, c2, c1, c3) WITH CLUSTERING ORDER BY (c2 DESC, c1 ASC, c3 ASC)") as mv2, \
             create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE pk IS NOT NULL AND c1 IS NOT NULL AND c2 IS NOT NULL and c3 IS NOT NULL PRIMARY KEY (pk, c2, c1, c3) WITH CLUSTERING ORDER BY (c2 ASC, c1 DESC, c3 DESC)") as mv3:

            execute(cql, table, "INSERT INTO %s (pk, c1, c2, c3, v) VALUES (?, ?, ?, ?, ?)", 0, 0, 0, 0, 0)
            execute(cql, table, "INSERT INTO %s (pk, c1, c2, c3, v) VALUES (?, ?, ?, ?, ?)", 0, 0, 0, 1, 1)
            execute(cql, table, "INSERT INTO %s (pk, c1, c2, c3, v) VALUES (?, ?, ?, ?, ?)", 0, 0, 0, 2, 2)
            execute(cql, table, "INSERT INTO %s (pk, c1, c2, c3, v) VALUES (?, ?, ?, ?, ?)", 0, 0, 1, 0, 3)
            execute(cql, table, "INSERT INTO %s (pk, c1, c2, c3, v) VALUES (?, ?, ?, ?, ?)", 0, 0, 1, 1, 4)
            execute(cql, table, "INSERT INTO %s (pk, c1, c2, c3, v) VALUES (?, ?, ?, ?, ?)", 0, 0, 1, 2, 5)
            execute(cql, table, "INSERT INTO %s (pk, c1, c2, c3, v) VALUES (?, ?, ?, ?, ?)", 0, 1, 1, 1, 6)
            execute(cql, table, "INSERT INTO %s (pk, c1, c2, c3, v) VALUES (?, ?, ?, ?, ?)", 0, 1, 2, 1, 7)
            execute(cql, table, "INSERT INTO %s (pk, c1, c2, c3, v) VALUES (?, ?, ?, ?, ?)", 0, 2, 1, 1, 8)

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = ?", 0),
                        row(0, 2, 1, 1, 8),
                        row(0, 1, 1, 1, 6),
                        row(0, 1, 2, 1, 7),
                        row(0, 0, 0, 2, 2),
                        row(0, 0, 0, 1, 1),
                        row(0, 0, 0, 0, 0),
                        row(0, 0, 1, 2, 5),
                        row(0, 0, 1, 1, 4),
                        row(0, 0, 1, 0, 3))

            assert_rows(execute(cql, mv1, "SELECT * FROM %s WHERE pk = ?", 0),
                        row(0, 0, 0, 2, 2),
                        row(0, 0, 0, 1, 1),
                        row(0, 0, 0, 0, 0),
                        row(0, 1, 2, 1, 8),
                        row(0, 1, 1, 1, 6),
                        row(0, 1, 0, 2, 5),
                        row(0, 1, 0, 1, 4),
                        row(0, 1, 0, 0, 3),
                        row(0, 2, 1, 1, 7))

            assert_rows(execute(cql, mv2, "SELECT * FROM %s WHERE pk = ?", 0),
                        row(0, 2, 1, 1, 7),
                        row(0, 1, 0, 0, 3),
                        row(0, 1, 0, 1, 4),
                        row(0, 1, 0, 2, 5),
                        row(0, 1, 1, 1, 6),
                        row(0, 1, 2, 1, 8),
                        row(0, 0, 0, 0, 0),
                        row(0, 0, 0, 1, 1),
                        row(0, 0, 0, 2, 2))

            assert_rows(execute(cql, mv3, "SELECT * FROM %s WHERE pk = ?", 0),
                        row(0, 0, 0, 2, 2),
                        row(0, 0, 0, 1, 1),
                        row(0, 0, 0, 0, 0),
                        row(0, 1, 2, 1, 8),
                        row(0, 1, 1, 1, 6),
                        row(0, 1, 0, 2, 5),
                        row(0, 1, 0, 1, 4),
                        row(0, 1, 0, 0, 3),
                        row(0, 2, 1, 1, 7))

# The Java test checks the CQL that Cassandra generates for a view's schema,
# through Cassandra's internal SchemaCQLHelper. We check the output of
# DESCRIBE MATERIALIZED VIEW instead, which is generated by the same code in
# Cassandra. The Java test's expected CQL starts with "CREATE MATERIALIZED
# VIEW IF NOT EXISTS" and includes "WITH ID = ...", which DESCRIBE doesn't
# print, so we removed them. Scylla's and Cassandra's DESCRIBE differ in
# whitespace and in the case of keywords and of "null", so we compare the
# outputs after normalizing them.
def viewMetadataCQL(cql, test_keyspace, createBase, createView, viewSnapshotSchema):
    with create_table(cql, test_keyspace, createBase) as base, \
         create_view(cql, base, createView) as view:
        description = cql.execute("DESCRIBE MATERIALIZED VIEW " + view).one().create_statement
        def normalize(s):
            return " ".join(s.split()).lower()
        expected = normalize(viewSnapshotSchema % (view, base))
        assert normalize(description)[:len(expected)] == expected

def testViewMetadataCQLNotIncludeAllColumn(cql, test_keyspace):
    createBase = ("(" +
                  "pk1 int," +
                  "pk2 int," +
                  "ck1 int," +
                  "ck2 int," +
                  "reg1 int," +
                  "reg2 list<int>," +
                  "reg3 int," +
                  "PRIMARY KEY ((pk1, pk2), ck1, ck2)) WITH " +
                  "CLUSTERING ORDER BY (ck1 ASC, ck2 ASC);")

    createView = ("CREATE MATERIALIZED VIEW IF NOT EXISTS %s AS SELECT pk1, pk2, ck1, ck2, reg1, reg2 FROM %s "
                  + "WHERE pk2 IS NOT NULL AND pk1 IS NOT NULL AND ck2 IS NOT NULL AND ck1 IS NOT NULL PRIMARY KEY((pk2, pk1), ck2, ck1)")

    expectedViewSnapshot = ("CREATE MATERIALIZED VIEW %s AS\n" +
                            "    SELECT pk2, pk1, ck2, ck1, reg1, reg2\n" +
                            "    FROM %s\n" +
                            "    WHERE pk2 IS NOT NULL AND pk1 IS NOT NULL AND ck2 IS NOT NULL AND ck1 IS NOT NULL\n" +
                            "    PRIMARY KEY ((pk2, pk1), ck2, ck1)\n" +
                            " WITH CLUSTERING ORDER BY (ck2 ASC, ck1 ASC)")

    viewMetadataCQL(cql, test_keyspace,
                    createBase,
                    createView,
                    expectedViewSnapshot)

# Reproduces #12308 (a view without its own CLUSTERING ORDER BY should get
# the base table's clustering order for its clustering columns)
@pytest.mark.xfail(reason="#12308")
def testViewMetadataCQLIncludeAllColumn(cql, test_keyspace):
    createBase = ("(" +
                  "pk1 int," +
                  "pk2 int," +
                  "ck1 int," +
                  "ck2 int," +
                  "reg1 int," +
                  "reg2 list<int>," +
                  "reg3 int," +
                  "PRIMARY KEY ((pk1, pk2), ck1, ck2)) WITH " +
                  "CLUSTERING ORDER BY (ck1 ASC, ck2 DESC);")

    createView = ("CREATE MATERIALIZED VIEW IF NOT EXISTS %s AS SELECT * FROM %s "
                  + "WHERE pk2 IS NOT NULL AND pk1 IS NOT NULL AND ck2 IS NOT NULL AND ck1 IS NOT NULL PRIMARY KEY((pk2, pk1), ck2, ck1)")

    expectedViewSnapshot = ("CREATE MATERIALIZED VIEW %s AS\n" +
                            "    SELECT *\n" +
                            "    FROM %s\n" +
                            "    WHERE pk2 IS NOT NULL AND pk1 IS NOT NULL AND ck2 IS NOT NULL AND ck1 IS NOT NULL\n" +
                            "    PRIMARY KEY ((pk2, pk1), ck2, ck1)\n" +
                            " WITH CLUSTERING ORDER BY (ck2 DESC, ck1 ASC)")

    viewMetadataCQL(cql, test_keyspace,
                    createBase,
                    createView,
                    expectedViewSnapshot)

# Reproduces SCYLLADB-5144 (ALTER ... IF EXISTS)
@pytest.mark.xfail(reason="SCYLLADB-5144")
def testAlterViewIfExists(cql, test_keyspace):
    cql.execute("ALTER MATERIALIZED VIEW IF EXISTS " + test_keyspace + "." + unique_name() + " WITH compaction = { 'class' : 'LeveledCompactionStrategy' }")
