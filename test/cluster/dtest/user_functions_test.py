import math
import re
import time

import pytest
from cassandra import FunctionFailure

from dtest_class import Tester, create_ks
from dtest_setup_overrides import DTestSetupOverrides
from tools.assertions import assert_invalid, assert_one
from tools.misc import ImmutableMapping


@pytest.mark.dtest_full
@pytest.mark.next_gating
class TestUserFunctions(Tester):
    @pytest.fixture(scope="function", autouse=True)
    def fixture_dtest_setup_overrides(self, dtest_config):
        dtest_setup_overrides = DTestSetupOverrides()
        if dtest_config.is_scylla:
            dtest_setup_overrides.cluster_options = ImmutableMapping(
                {
                    "experimental_features": ["udf"],
                    "enable_user_defined_functions": "true",
                }
            )
        elif dtest_config.cassandra_version_from_build >= "3.0":
            dtest_setup_overrides.cluster_options = ImmutableMapping(
                {
                    "enable_user_defined_functions": "true",
                    "enable_scripted_user_defined_functions": "true",
                }
            )
        else:
            dtest_setup_overrides.cluster_options = ImmutableMapping(
                {
                    "enable_user_defined_functions": "true",
                }
            )
        return dtest_setup_overrides

    def prepare(self, create_keyspace=True, nodes=1, rf=1):
        cluster = self.cluster

        cluster.populate(nodes).start()
        node1 = cluster.nodelist()[0]
        time.sleep(0.2)

        session = self.patient_cql_connection(node1)
        if create_keyspace:
            create_ks(session, "ks", rf)
        return session

    def test_migration(self):
        """Test migration of user functions"""
        cluster = self.cluster

        # Uses 3 nodes just to make sure function mutations are correctly serialized
        cluster.populate(3).start()
        node1 = cluster.nodelist()[0]
        node2 = cluster.nodelist()[1]
        node3 = cluster.nodelist()[2]
        time.sleep(0.2)

        session1 = self.patient_exclusive_cql_connection(node1)
        session2 = self.patient_exclusive_cql_connection(node2)
        session3 = self.patient_exclusive_cql_connection(node3)
        create_ks(session1, "ks", 1)
        session2.execute("use ks")
        session3.execute("use ks")

        session1.execute(
            """
            CREATE TABLE udf_kv (
                key    int primary key,
                value  double
            );
        """
        )
        time.sleep(1)

        session1.execute("INSERT INTO udf_kv (key, value) VALUES (%d, %d)" % (1, 1))
        session1.execute("INSERT INTO udf_kv (key, value) VALUES (%d, %d)" % (2, 2))
        session1.execute("INSERT INTO udf_kv (key, value) VALUES (%d, %d)" % (3, 3))

        session1.execute(
            """
            create or replace function x_2 ( input double ) called on null input
            returns double language lua as 'return (input ~= nil) and input * 2.0 or nil'
            """
        )
        session2.execute(
            """
            create or replace function x_3 ( input double ) called on null input
            returns double language lua as 'return (input ~= nil) and input * 3.0 or nil'
            """
        )
        session3.execute(
            """
            create or replace function x_4 ( input double ) called on null input
            returns double language lua as 'return (input ~= nil) and input * 4.0 or nil'
            """
        )

        time.sleep(1)

        assert_one(session1, "SELECT key, value, x_2(value), x_3(value), x_4(value) FROM udf_kv where key = 1", [1, 1.0, 2.0, 3.0, 4.0])

        assert_one(session2, "SELECT key, value, x_2(value), x_3(value), x_4(value) FROM udf_kv where key = 2", [2, 2.0, 4.0, 6.0, 8.0])

        assert_one(session3, "SELECT key, value, x_2(value), x_3(value), x_4(value) FROM udf_kv where key = 3", [3, 3.0, 6.0, 9.0, 12.0])

        # try giving existing function bad input, should error
        assert_invalid(session1, "SELECT key, value, x_2(key) FROM udf_kv where key = 1", re.escape("Type error: key cannot be passed as argument 0 of function ks.x_2 of type double"))

        session2.execute("drop function x_2")
        session3.execute("drop function x_3")
        session1.execute("drop function x_4")

        assert_invalid(session1, "SELECT key, value, x_2(value), x_3(value), x_4(value) FROM udf_kv where key = 1")
        assert_invalid(session2, "SELECT key, value, x_2(value), x_3(value), x_4(value) FROM udf_kv where key = 1")
        assert_invalid(session3, "SELECT key, value, x_2(value), x_3(value), x_4(value) FROM udf_kv where key = 1")

    @pytest.mark.single_node
    def test_udf_overload_test(self):
        session = self.prepare()

        session.execute("CREATE TABLE tab (v varchar PRIMARY KEY, i int, t text, a ascii)")
        session.execute("INSERT INTO tab (v, i, t, a) VALUES ('foo', 1, 'foo', 'foo');")

        # create overloaded udfs
        session.execute("CREATE FUNCTION overloaded(v varchar) called on null input RETURNS text LANGUAGE lua AS 'return \"f1\"'")
        session.execute("CREATE OR REPLACE FUNCTION overloaded(i int) called on null input RETURNS text LANGUAGE lua AS 'return \"f2\"'")
        session.execute("CREATE OR REPLACE FUNCTION overloaded(t text) called on null input RETURNS text LANGUAGE lua AS 'return \"f3\"'")
        session.execute("CREATE OR REPLACE FUNCTION overloaded(a ascii) called on null input RETURNS text LANGUAGE lua AS 'return \"f4\"'")

        # ensure that works with correct specificity
        assert_invalid(
            session,
            "SELECT v FROM tab WHERE t = overloaded('foo')",
            "Ambiguous call to function overloaded|Cannot execute this query as it might involve data filtering",
        )
        assert_one(session, "SELECT v, overloaded(v) FROM tab", ["foo", "f3"])  # varchar is the same as text
        assert_one(session, "SELECT i, overloaded(i) FROM tab", [1, "f2"])
        assert_one(session, "SELECT t, overloaded(t) FROM tab", ["foo", "f3"])
        assert_one(session, "SELECT a, overloaded(a) FROM tab", ["foo", "f4"])

        # try non-existent functions
        assert_invalid(session, "DROP FUNCTION overloaded(boolean)")
        assert_invalid(session, "DROP FUNCTION overloaded(bigint)")

        # try dropping overloaded - should fail because ambiguous
        assert_invalid(session, "DROP FUNCTION overloaded")

        # varchar is the same as text here too.
        session.execute("DROP FUNCTION overloaded(varchar)")
        assert_invalid(session, "DROP FUNCTION overloaded(text)", re.escape("User function ks.overloaded(text) doesn't exist"))

        session.execute("DROP FUNCTION overloaded(ascii)")

        # should now work - unambiguous
        session.execute("DROP FUNCTION overloaded")

    @pytest.mark.skip(reason="Language 'javascript' is not supported")
    @pytest.mark.single_node
    def test_udf_scripting(self):
        session = self.prepare()
        session.execute("create table nums (key int primary key, val double);")

        for x in range(1, 4):
            session.execute("INSERT INTO nums (key, val) VALUES (%d, %d)" % (x, float(x)))

        session.execute("CREATE FUNCTION x_sin(val double) called on null input returns double language javascript as 'Math.sin(val)'")

        assert_one(session, "SELECT key, val, x_sin(val) FROM nums where key = %d" % 1, [1, 1.0, math.sin(1.0)])
        assert_one(session, "SELECT key, val, x_sin(val) FROM nums where key = %d" % 2, [2, 2.0, math.sin(2.0)])
        assert_one(session, "SELECT key, val, x_sin(val) FROM nums where key = %d" % 3, [3, 3.0, math.sin(3.0)])

        session.execute("create function y_sin(val double) called on null input returns double language javascript as 'Math.sin(val).toString()'")

        assert_invalid(session, "select y_sin(val) from nums where key = 1", expected=FunctionFailure)

        assert_invalid(session, "create function compilefail(key int) called on null input returns double language javascript as 'foo bar';")

        session.execute("create function plustwo(key int) called on null input returns double language javascript as 'key+2'")

        assert_one(session, "select plustwo(key) from nums where key = 3", [5])

    @pytest.mark.single_node
    def test_default_aggregate(self):
        session = self.prepare()
        session.execute("create table nums (key int primary key, val double);")

        for x in range(1, 10):
            session.execute("INSERT INTO nums (key, val) VALUES (%d, %d)" % (x, float(x)))

        assert_one(session, "SELECT min(key) FROM nums", [1])
        assert_one(session, "SELECT max(val) FROM nums", [9.0])
        assert_one(session, "SELECT sum(key) FROM nums", [45])
        assert_one(session, "SELECT avg(val) FROM nums", [5.0])
        assert_one(session, "SELECT count(*) FROM nums", [9])

    @pytest.mark.single_node
    def test_aggregate_udf(self):
        session = self.prepare()
        session.execute("create table nums (key int primary key, val int);")

        for x in range(1, 4):
            session.execute("INSERT INTO nums (key, val) VALUES (%d, %d)" % (x, x))
        session.execute("create function plus(key int, val int) called on null input returns int language lua as 'return key + val'")
        session.execute("create function stri(key int) called on null input returns text language lua as 'return tostring(key)'")
        session.execute("create aggregate suma (int) sfunc plus stype int finalfunc stri initcond 10")

        assert_one(session, "select suma(val) from nums", ["16"])

        session.execute("create function test(a int, b double) called on null input returns int language lua as 'return a + b'")
        session.execute("create aggregate aggy(double) sfunc test stype int")

        assert_invalid(session, "create aggregate aggtwo(int) sfunc aggy stype int")

        assert_invalid(session, "create aggregate aggthree(int) sfunc test stype int finalfunc aggtwo")

    def test_restart(self):
        """Ensure that UDA survives server restart"""
        session = self.prepare(nodes=3)

        # Prepare a table with some data
        session.execute("CREATE TABLE tab (pk int PRIMARY KEY, t text)")
        for x in range(10):
            session.execute(f"INSERT INTO tab (pk, t) VALUES ({x}, '" + "a" * x + "');")

        # Create a UDA that sums integers from all rows
        session.execute("CREATE FUNCTION plus(acc int, val int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE lua AS 'return acc + val'")
        session.execute("CREATE FUNCTION return_int(acc int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE lua AS 'return acc'")
        session.execute("CREATE AGGREGATE suma(int) SFUNC plus STYPE int FINALFUNC return_int INITCOND 0")
        assert_one(session, "SELECT suma(pk) FROM tab", [45])

        # Create a variant of the UDA that uses a REDUCEFUNC
        session.execute("CREATE AGGREGATE sum_reduce(int) SFUNC plus STYPE int REDUCEFUNC plus FINALFUNC return_int INITCOND 0")
        assert_one(session, "SELECT sum_reduce(pk) FROM tab", [45])

        # Create a UDA that concatenates strings from all rows
        session.execute("CREATE FUNCTION concat(acc text, val text) RETURNS NULL ON NULL INPUT RETURNS text LANGUAGE lua AS 'return acc..val'")
        session.execute("CREATE FUNCTION return_string(acc text) RETURNS NULL ON NULL INPUT RETURNS text LANGUAGE lua AS 'return acc'")
        session.execute("CREATE AGGREGATE suma(text) SFUNC concat STYPE text FINALFUNC return_string INITCOND ''")
        assert_one(session, "SELECT suma(t) FROM tab WHERE pk IN (1,3,5)", ["aaaaaaaaa"])

        # Create a variant of the UDA that uses a REDUCEFUNC
        session.execute("CREATE AGGREGATE sum_reduce(text) SFUNC concat STYPE text REDUCEFUNC concat FINALFUNC return_string INITCOND ''")
        assert_one(session, "SELECT sum_reduce(t) FROM tab WHERE pk IN (1,3,5)", ["aaaaaaaaa"])

        # Stop and start the cluster
        cluster = self.cluster
        cluster.stop()

        cluster.start(wait_other_notice=True)
        node = cluster.nodelist()[0]
        time.sleep(1)

        session = self.patient_cql_connection(node)
        session.execute("use ks")

        # Ensure that the UDAs still work after restart
        assert_one(session, "SELECT plus(pk, pk) FROM tab WHERE pk = 1", [2])
        assert_one(session, "SELECT concat(t, t) FROM tab WHERE pk = 1", ["aa"])
        assert_one(session, "SELECT suma(pk) FROM tab", [45])
        assert_one(session, "SELECT suma(t) FROM tab WHERE pk in (1,3,5)", ["aaaaaaaaa"])
        assert_one(session, "SELECT sum_reduce(pk) FROM tab", [45])
        assert_one(session, "SELECT sum_reduce(t) FROM tab WHERE pk in (1,3,5)", ["aaaaaaaaa"])

        # Ensure that the UDAs are still tracked:
        # - they can't be overwritten
        assert_invalid(session, "CREATE AGGREGATE suma(int) SFUNC plus STYPE int")
        assert_invalid(session, "CREATE AGGREGATE suma(text) SFUNC concat STYPE text")
        assert_invalid(session, "CREATE AGGREGATE sum_reduce(int) SFUNC plus STYPE int")
        assert_invalid(session, "CREATE AGGREGATE sum_reduce(text) SFUNC concat STYPE text")

        # - they can be dropped
        session.execute("DROP AGGREGATE suma(int)")
        session.execute("DROP AGGREGATE suma(text)")
        session.execute("DROP AGGREGATE sum_reduce(int)")
        session.execute("DROP AGGREGATE sum_reduce(text)")
