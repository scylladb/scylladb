#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
"scylla sstable dump-data --merge" of a node's flushed tables, ported from
scylla-dtest scylla_sstable_test.py, one test per dtest: the values it
prints for a table of regular columns, for a counter table and for a table
of (almost) all CQL types are the values written through CQL. Like the
dtests' populate(1), a single node with the SimpleSnitch and vnodes; the
sstables are dumped from a snapshot, like ccm's run_scylla_sstable().
"""

import datetime
import json
import logging
import math
import uuid
from decimal import Decimal

import pytest
from cassandra import ConsistencyLevel
from cassandra.cluster import EXEC_PROFILE_DEFAULT
from cassandra.util import OrderedMap

from test import DEBUG_MODES
from test.cluster.util import dump_sstables, run_nodetool, stop_servers_gracefully
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

CONFIG = {"tablets_mode_for_new_keyspaces": "disabled", "endpoint_snitch": "SimpleSnitch"}
GATE = pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")


@pytest.fixture(autouse=True)
async def stop_nodes_gracefully(manager: ScyllaClusterManager):
    """Like the dtests' teardown, stop the nodes gracefully at the end of
    each test: shutdown flushes the memtables and closes the sstables."""
    yield
    await stop_servers_gracefully(manager)


async def start_node(manager: ScyllaClusterManager):
    """populate(1).start() and a session whose statements default to CL ONE."""
    server = await manager.server_add(config=CONFIG)
    cql = manager.get_cql()
    cql.get_execution_profile(EXEC_PROFILE_DEFAULT).consistency_level = ConsistencyLevel.ONE
    return server, cql


async def dump_data(manager: ScyllaClusterManager, server, table: str) -> list:
    """The dtests' _dump_data(): flush, then dump-data --merge of all the
    table's sstables in one batch."""
    await run_nodetool(manager, server, "flush")
    return await dump_sstables(manager, server, "ks", table)


def compare_data(src, dst):
    """The dtests' ScyllaSstable._compare_data()."""
    symmetric_diff = set(src) ^ set(dst)
    assert len(symmetric_diff) == 0, f"Destination data set is not same as source. Found difference:\n{symmetric_diff} "


# Ported from scylla-dtest scylla_sstable_test.py::TestScyllaSstableDumpData::test_scylla_sstable_basic.
# Four rows of a (name text, value_one int, value_two int) table.
@GATE
async def test_scylla_sstable_basic(manager: ScyllaClusterManager):
    server, cql = await start_node(manager)
    cql.execute("CREATE KEYSPACE ks WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 };")
    cql.execute("CREATE TABLE ks.cf (name text PRIMARY KEY, value_one int, value_two int);")
    values_list = [("mary", 1, 12), ("sara", 2, 24), ("mike", 3, 36), ("ted", 4, 48)]
    for values in values_list:
        cql.execute(f"INSERT INTO ks.cf (name, value_one, value_two) VALUES {values};")
    data = await dump_data(manager, server, "cf")
    json_values = []
    for partition in data:
        values = [int(v["value"]) for v in partition["clustering_elements"][0]["columns"].values()]
        json_values.append(tuple([partition["key"]["value"], *values]))
    compare_data(values_list, json_values)


# Ported from scylla-dtest scylla_sstable_test.py::TestScyllaSstableDumpData::test_sstabledump_counter_basic.
# Nine counters, each incremented by its key and by 10^18: the shards
# dumped for each add up to the value read through CQL.
@GATE
async def test_sstabledump_counter_basic(manager: ScyllaClusterManager):
    server, cql = await start_node(manager)
    cql.execute("CREATE KEYSPACE ks WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 };")
    cql.execute("CREATE TABLE ks.cf (pk INT PRIMARY KEY, cnt COUNTER);")
    for i in range(1, 10):
        cql.execute(f"UPDATE ks.cf SET cnt = cnt + {i} WHERE pk = {i};")
        val = int(math.pow(10, 18))
        cql.execute(f"UPDATE ks.cf SET cnt = cnt + {val} WHERE pk = {i};")
    values_list = [tuple(row) for row in cql.execute("SELECT * FROM ks.cf;")]
    data = await dump_data(manager, server, "cf")
    json_values = []
    for partition in data:
        values = [sum(int(shard["value"]) for shard in v["value"]) for v in partition["clustering_elements"][0]["columns"].values()]
        json_values.append(tuple([int(partition["key"]["value"]), *values]))
    compare_data(values_list, json_values)


def parse_timestamp(v: str) -> datetime.datetime:
    """dateutil's parse() of a dumped timestamp, as the dtest compares it
    (with or without the trailing Z): a UTC datetime."""
    v = v.replace("Z", "+00:00")
    dt = datetime.datetime.fromisoformat(v)
    return dt if dt.tzinfo else dt.replace(tzinfo=datetime.timezone.utc)


# Ported from scylla-dtest scylla_sstable_test.py::TestScyllaSstableDumpataAllDatatypes::test_sstabledump_all_datatypes.
# The dtests' testdatatype table (cqlsh_copy_tests' all_datatypes_prepare())
# without its two UDT columns (dropped, scylla-tools-java#24): one row of
# all the other types; the dumped values of all but the last two columns
# are the ones written.
@GATE
async def test_sstabledump_all_datatypes(manager: ScyllaClusterManager):
    server, cql = await start_node(manager)
    cql.execute("DROP KEYSPACE IF EXISTS ks")
    cql.execute("CREATE KEYSPACE ks WITH replication={'class':'NetworkTopologyStrategy', 'replication_factor':1}")
    cql.execute("USE ks")
    cql.execute("CREATE TYPE name_type (firstname text, lastname text)")
    cql.execute("CREATE TYPE address_type (name frozen<name_type>, number int, street text, phones set<text>)")
    cql.execute("""CREATE TABLE testdatatype (
                a ascii PRIMARY KEY, b bigint, c blob, d boolean, e decimal, f double, g float, h inet, i int, j text,
                k timestamp, l timeuuid, m uuid, n varchar, o varint, p list<int>, q set<text>, r map<timestamp, text>,
                s tuple<int, text, boolean>, t frozen<address_type>, u frozen<list<list<address_type>>>,
                v frozen<map<map<int,int>,set<text>>>, w frozen<set<set<inet>>>,
            )""")
    date1 = datetime.datetime(2005, 7, 14, 12, 30, 0, 0, datetime.timezone.utc)
    date2 = datetime.datetime(2005, 7, 14, 13, 30, 0, 0, datetime.timezone.utc)
    # all_datatypes_prepare()'s data without t and u (data[:19] + data[21:]).
    data = (
        "ascii", 2**40, bytearray.fromhex("beef"), True, Decimal("3.14"), 2.444, 1.1, "127.0.0.1", 25, "ヽ(`ー`)/",
        date1, uuid.uuid1(), uuid.uuid4(), "asdf", 2**65, [1, 2, 3], {"3", "2", "1"}, {date1: "1", date2: "2"}, (1, "1", True),
        OrderedMap([(OrderedMap([(1, 1), (2, 2)]), {"1", "2", "3"})]),
        {frozenset(["127.0.0.1"]), frozenset(["127.0.0.1", "127.0.0.2"])},
    )
    cql.execute("ALTER TABLE ks.testdatatype DROP t")
    cql.execute("ALTER TABLE ks.testdatatype DROP u")
    insert = cql.prepare("""INSERT INTO testdatatype (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, v, w)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""")
    cql.execute(insert, data)
    logger.debug(list(cql.execute("SELECT * FROM testdatatype")))

    dumped = await dump_data(manager, server, "testdatatype")
    logger.debug(json.dumps(dumped, indent=2))

    # The dtest's _fetch_data_from_json().
    p, q, r = [], [], {}
    format_val = {
        "b": int,
        "c": bytearray.fromhex,
        "d": lambda v: json.loads(v) if isinstance(v, str) else v,
        "e": Decimal,
        "f": float,
        "g": float,
        "i": int,
        "j": lambda v: v,
        "k": parse_timestamp,
        "l": uuid.UUID,
        "m": uuid.UUID,
        "o": int,
        "p": lambda v: p.append(int(v["value"]["value"])),
        "q": lambda v: q.append(v["key"]),
        "r": lambda v: r.update({parse_timestamp(v["key"]): v["value"]["value"]}),
        "s": lambda v: tuple(json.loads(val) if i != 1 else val for i, val in enumerate(v.split(":"))) if isinstance(v, str) else tuple(v),
    }
    res = []
    for name, item in dumped[0]["clustering_elements"][0]["columns"].items():
        if name not in ("p", "q", "r"):
            value = item["value"]
            if name in format_val:
                value = format_val[name](value)
            res.append(value)
        else:
            for cell in item["cells"]:
                format_val[name](cell)
    res.insert(14, p)
    res.insert(15, set(q))
    res.insert(16, r)
    res.insert(0, dumped[0]["key"]["value"])

    # The dtest's _compare_data(): all but the last two columns (v, w).
    for i in range(len(data) - 2):
        src, dst = data[i], res[i]
        if isinstance(dst, dict):
            assert dict(src) == dst, f"Destination data: {dst} is not same as source: {dict(src)}"
        if isinstance(src, str) and isinstance(dst, bytes):
            assert src == dst.decode("utf-8"), f"Destination data: {dst.decode('utf-8')} is not same as source: {src}"
        else:
            assert src == dst, f"Destination data: {dst} is not same as source: {src}"
