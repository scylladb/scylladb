#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Ported from scylla-dtest user_functions_test.py: Lua user functions and
# aggregates. Like the dtests' nodes, the nodes have UDFs enabled
# (experimental feature "udf"), use the SimpleSnitch (ccm's populate) and
# vnodes.

import asyncio
import re
import time

import pytest
from cassandra.protocol import InvalidRequest

from test import DEBUG_MODES
from test.cluster.dtest_helpers import create_ks, start_node_wait_cql
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for_cql_and_get_hosts

UDF_CONFIG = {"experimental_features": ["udf"], "enable_user_defined_functions": True,
              "tablets_mode_for_new_keyspaces": "disabled", "num_tokens": 256, "endpoint_snitch": "SimpleSnitch"}
PROPERTY_FILE = {"dc": "datacenter1", "rack": "rack1"}


def use_ks(cql, name: str, rf: int) -> None:
    """The dtests' create_ks(): the keyspace, schema agreement and USE."""
    create_ks(cql, name, rf)
    cql.cluster.control_connection.wait_for_schema_agreement(wait_time=120)
    cql.execute(f"USE {name}")


def assert_one(session, query, expected) -> None:
    rows = [list(r) for r in session.execute(query)]
    assert rows == [expected], f"{query}: {rows}"


def assert_invalid(session, query, matching=None) -> None:
    with pytest.raises(InvalidRequest) as e:
        session.execute(query)
    if matching:
        assert re.search(matching, str(e.value)), f"{query}: {e.value}"


async def prepare(manager: ScyllaClusterManager, nodes: int = 1, rf: int = 1):
    """TestUserFunctions.prepare(): populate(nodes), and keyspace ks."""
    servers = [await manager.server_add(config=UDF_CONFIG, property_file=PROPERTY_FILE) for _ in range(nodes)]
    await asyncio.sleep(0.2)
    cql = manager.get_cql()
    use_ks(cql, "ks", rf)
    return servers, cql


# Ported from scylla-dtest user_functions_test.py::TestUserFunctions::test_migration.
# On three nodes, Lua functions created through each node can be called
# through every node; a call with an argument of the wrong type fails; once
# dropped (through other nodes), no node can call them.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_migration(manager: ScyllaClusterManager):
    servers = [await manager.server_add(config=UDF_CONFIG, property_file=PROPERTY_FILE) for _ in range(3)]
    await asyncio.sleep(0.2)
    session1, session2, session3 = [await manager.get_cql_exclusive(s) for s in servers]
    use_ks(session1, "ks", 1)
    session2.execute("use ks")
    session3.execute("use ks")

    session1.execute("CREATE TABLE udf_kv (key int primary key, value double);")
    await asyncio.sleep(1)
    for k in (1, 2, 3):
        session1.execute(f"INSERT INTO udf_kv (key, value) VALUES ({k}, {k})")
    for session, factor in [(session1, 2), (session2, 3), (session3, 4)]:
        session.execute(f"create or replace function x_{factor} ( input double ) called on null input "
                        f"returns double language lua as 'return (input ~= nil) and input * {factor}.0 or nil'")
    await asyncio.sleep(1)

    assert_one(session1, "SELECT key, value, x_2(value), x_3(value), x_4(value) FROM udf_kv where key = 1", [1, 1.0, 2.0, 3.0, 4.0])
    assert_one(session2, "SELECT key, value, x_2(value), x_3(value), x_4(value) FROM udf_kv where key = 2", [2, 2.0, 4.0, 6.0, 8.0])
    assert_one(session3, "SELECT key, value, x_2(value), x_3(value), x_4(value) FROM udf_kv where key = 3", [3, 3.0, 6.0, 9.0, 12.0])

    # A function given an input of the wrong type fails.
    assert_invalid(session1, "SELECT key, value, x_2(key) FROM udf_kv where key = 1",
                   re.escape("Type error: key cannot be passed as argument 0 of function ks.x_2 of type double"))

    session2.execute("drop function x_2")
    session3.execute("drop function x_3")
    session1.execute("drop function x_4")
    for session in (session1, session2, session3):
        assert_invalid(session, "SELECT key, value, x_2(value), x_3(value), x_4(value) FROM udf_kv where key = 1")


# Ported from scylla-dtest user_functions_test.py::TestUserFunctions::test_udf_overload_test.
# Overloads of a Lua function by argument type: a call picks the overload
# by the argument's type (varchar is text), an ambiguous call or DROP fails,
# DROP of a missing signature fails, and once one overload is left, DROP
# without arguments drops it.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_udf_overload_test(manager: ScyllaClusterManager):
    _, session = await prepare(manager)
    session.execute("CREATE TABLE tab (v varchar PRIMARY KEY, i int, t text, a ascii)")
    session.execute("INSERT INTO tab (v, i, t, a) VALUES ('foo', 1, 'foo', 'foo');")

    session.execute("CREATE FUNCTION overloaded(v varchar) called on null input RETURNS text LANGUAGE lua AS 'return \"f1\"'")
    session.execute("CREATE OR REPLACE FUNCTION overloaded(i int) called on null input RETURNS text LANGUAGE lua AS 'return \"f2\"'")
    session.execute("CREATE OR REPLACE FUNCTION overloaded(t text) called on null input RETURNS text LANGUAGE lua AS 'return \"f3\"'")
    session.execute("CREATE OR REPLACE FUNCTION overloaded(a ascii) called on null input RETURNS text LANGUAGE lua AS 'return \"f4\"'")

    assert_invalid(session, "SELECT v FROM tab WHERE t = overloaded('foo')",
                   "Ambiguous call to function overloaded|Cannot execute this query as it might involve data filtering")
    assert_one(session, "SELECT v, overloaded(v) FROM tab", ["foo", "f3"])  # varchar is the same as text
    assert_one(session, "SELECT i, overloaded(i) FROM tab", [1, "f2"])
    assert_one(session, "SELECT t, overloaded(t) FROM tab", ["foo", "f3"])
    assert_one(session, "SELECT a, overloaded(a) FROM tab", ["foo", "f4"])

    assert_invalid(session, "DROP FUNCTION overloaded(boolean)")
    assert_invalid(session, "DROP FUNCTION overloaded(bigint)")
    # Ambiguous.
    assert_invalid(session, "DROP FUNCTION overloaded")
    # varchar is the same as text here too.
    session.execute("DROP FUNCTION overloaded(varchar)")
    assert_invalid(session, "DROP FUNCTION overloaded(text)", re.escape("User function ks.overloaded(text) doesn't exist"))
    session.execute("DROP FUNCTION overloaded(ascii)")
    # Unambiguous now.
    session.execute("DROP FUNCTION overloaded")


# Ported from scylla-dtest user_functions_test.py::TestUserFunctions::test_default_aggregate.
# The built-in min, max, sum, avg and count aggregates over int and double
# columns.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_default_aggregate(manager: ScyllaClusterManager):
    _, session = await prepare(manager)
    session.execute("create table nums (key int primary key, val double);")
    for x in range(1, 10):
        session.execute(f"INSERT INTO nums (key, val) VALUES ({x}, {float(x)})")
    assert_one(session, "SELECT min(key) FROM nums", [1])
    assert_one(session, "SELECT max(val) FROM nums", [9.0])
    assert_one(session, "SELECT sum(key) FROM nums", [45])
    assert_one(session, "SELECT avg(val) FROM nums", [5.0])
    assert_one(session, "SELECT count(*) FROM nums", [9])


# Ported from scylla-dtest user_functions_test.py::TestUserFunctions::test_restart.
# On three nodes, Lua aggregates (with and without REDUCEFUNC, overloaded
# by argument type) still work after the whole cluster restarts, can't be
# created again and can be dropped.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_restart(manager: ScyllaClusterManager):
    servers, session = await prepare(manager, nodes=3)
    session.execute("CREATE TABLE tab (pk int PRIMARY KEY, t text)")
    for x in range(10):
        session.execute(f"INSERT INTO tab (pk, t) VALUES ({x}, '" + "a" * x + "');")

    session.execute("CREATE FUNCTION plus(acc int, val int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE lua AS 'return acc + val'")
    session.execute("CREATE FUNCTION return_int(acc int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE lua AS 'return acc'")
    session.execute("CREATE AGGREGATE suma(int) SFUNC plus STYPE int FINALFUNC return_int INITCOND 0")
    assert_one(session, "SELECT suma(pk) FROM tab", [45])
    session.execute("CREATE AGGREGATE sum_reduce(int) SFUNC plus STYPE int REDUCEFUNC plus FINALFUNC return_int INITCOND 0")
    assert_one(session, "SELECT sum_reduce(pk) FROM tab", [45])
    session.execute("CREATE FUNCTION concat(acc text, val text) RETURNS NULL ON NULL INPUT RETURNS text LANGUAGE lua AS 'return acc..val'")
    session.execute("CREATE FUNCTION return_string(acc text) RETURNS NULL ON NULL INPUT RETURNS text LANGUAGE lua AS 'return acc'")
    session.execute("CREATE AGGREGATE suma(text) SFUNC concat STYPE text FINALFUNC return_string INITCOND ''")
    assert_one(session, "SELECT suma(t) FROM tab WHERE pk IN (1,3,5)", ["aaaaaaaaa"])
    session.execute("CREATE AGGREGATE sum_reduce(text) SFUNC concat STYPE text REDUCEFUNC concat FINALFUNC return_string INITCOND ''")
    assert_one(session, "SELECT sum_reduce(t) FROM tab WHERE pk IN (1,3,5)", ["aaaaaaaaa"])

    # cluster.stop() and cluster.start(wait_other_notice=True).
    manager.driver_close()
    await asyncio.gather(*(manager.server_stop_gracefully(s.server_id) for s in servers))
    for s in servers:
        await start_node_wait_cql(manager, s)
    await manager.servers_see_each_other(servers)
    await asyncio.sleep(1)
    await manager.driver_connect()
    session = manager.get_cql()
    await wait_for_cql_and_get_hosts(session, servers, time.time() + 60)
    session.execute("use ks")

    assert_one(session, "SELECT plus(pk, pk) FROM tab WHERE pk = 1", [2])
    assert_one(session, "SELECT concat(t, t) FROM tab WHERE pk = 1", ["aa"])
    assert_one(session, "SELECT suma(pk) FROM tab", [45])
    assert_one(session, "SELECT suma(t) FROM tab WHERE pk in (1,3,5)", ["aaaaaaaaa"])
    assert_one(session, "SELECT sum_reduce(pk) FROM tab", [45])
    assert_one(session, "SELECT sum_reduce(t) FROM tab WHERE pk in (1,3,5)", ["aaaaaaaaa"])

    # They can't be overwritten, and can be dropped.
    assert_invalid(session, "CREATE AGGREGATE suma(int) SFUNC plus STYPE int")
    assert_invalid(session, "CREATE AGGREGATE suma(text) SFUNC concat STYPE text")
    assert_invalid(session, "CREATE AGGREGATE sum_reduce(int) SFUNC plus STYPE int")
    assert_invalid(session, "CREATE AGGREGATE sum_reduce(text) SFUNC concat STYPE text")
    session.execute("DROP AGGREGATE suma(int)")
    session.execute("DROP AGGREGATE suma(text)")
    session.execute("DROP AGGREGATE sum_reduce(int)")
    session.execute("DROP AGGREGATE sum_reduce(text)")
