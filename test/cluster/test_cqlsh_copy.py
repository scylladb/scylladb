#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
cqlsh COPY FROM and COPY TO against Scylla: CSV files loaded into tables
and exported from them, with delimiters, null indicators, headers, counters,
TTLs, column orders, quoted names, invalid values, every CQL type, SOURCE,
failures injected into cqlsh's child processes, and round trips (COPY TO,
TRUNCATE, COPY FROM, COPY TO) of tables filled by cassandra-stress.

Ported from scylla-dtest cqlsh_tests/cqlsh_copy_tests.py. cqlsh is the
client, as in the dtests (run as ccm's node.run_cqlsh() did, see
test_cqlsh.Node). Like the dtests' nodes, the nodes use vnodes (256
per node), auto_snapshot on and truncates may take 60 s; the blogposts profile is the
dtests', in test/resource/cassandra-stress. The nodes are stopped
gracefully at the end of each test, as the dtests' teardown did.
"""

import asyncio
import csv
import datetime
import json
import logging
import os
import re
from collections import namedtuple
from decimal import Decimal
from tempfile import NamedTemporaryFile
from uuid import uuid1, uuid4

import pytest
from cassandra.concurrent import execute_concurrent_with_args

from test import DEBUG_MODES
from test.cluster.test_cqlsh import Node
from test.cluster.util import cassandra_stress, stop_servers_gracefully
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

# auto_snapshot is on in the dtests' nodes (test.py turns it off), so TRUNCATE
# takes a snapshot.
CONFIG = {'tablets_mode_for_new_keyspaces': 'disabled', 'num_tokens': 256, 'auto_snapshot': True}
PROFILES = os.path.join(os.path.dirname(__file__), "..", "resource", "cassandra-stress")


@pytest.fixture(autouse=True)
async def stop_nodes_gracefully(manager: ScyllaClusterManager):
    """Like the dtests' teardown, stop the nodes gracefully at the end of
    each test: shutdown flushes the memtables and closes the sstables."""
    yield
    await stop_servers_gracefully(manager)


async def prepare(manager: ScyllaClusterManager, nodes: int = 1, config: dict | None = None):
    """The dtests' CqlshPrepare.prepare(): the nodes, and an empty ks."""
    servers = await manager.servers_add(nodes, config={**CONFIG, **(config or {})})
    cql = manager.get_cql()
    await cql.run_async("DROP KEYSPACE IF EXISTS ks")
    await cql.run_async("CREATE KEYSPACE ks WITH replication={'class':'NetworkTopologyStrategy', 'replication_factor':1}")
    # create_ks() also switches the session to ks.
    await cql.run_async("USE ks")
    exe = await manager.server_get_exe(servers[0].server_id)
    return Node(servers[0], exe), servers[0], cql


# A CSV row of two ints is loaded by COPY FROM into an int table.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_read_valid_data(manager: ScyllaClusterManager, tmp_path):
    node1, _, cql = await prepare(manager)
    await cql.run_async("CREATE TABLE ks.testvalidate (a int PRIMARY KEY, b int)")
    path = tmp_path / "data.csv"
    with open(path, "w", newline="") as f:
        csv.writer(f).writerows([[1, 2]])
    await asyncio.to_thread(node1.run_cqlsh, f"COPY ks.testvalidate (a, b) FROM '{path}'", None, True)
    results = list(await cql.run_async("SELECT * FROM ks.testvalidate"))
    with open(path) as f:
        assert len(list(csv.reader(f))) == len(results)


async def bulk_round_trip(manager: ScyllaClusterManager, num_operations: int, config: dict | None = None,
                          profile: str | None = None, stress_table: str = "keyspace1.standard1") -> None:
    """The dtests' _test_bulk_round_trip(): cassandra-stress writes at ALL,
    COPY TO a CSV of all the records, TRUNCATE, COPY FROM it, COPY TO a
    second CSV of as many lines."""
    node1, server, cql = await prepare(manager, nodes=3, config={"truncate_request_timeout_in_ms": 60000, **(config or {})})
    if profile:
        await cassandra_stress(manager, server, "user", f"profile={profile}", "ops(insert=1)", f"n={num_operations}", "cl=ALL",
                               "no-warmup", "-rate", "threads=50", create_keyspace=False)
    else:
        await cassandra_stress(manager, server, "write", f"n={num_operations}", "cl=ALL", "no-warmup", "-rate", "threads=50",
                               create_keyspace=False)
    num_records = list(await cql.run_async(f"SELECT COUNT(*) FROM {stress_table}"))[0][0]
    assert num_records >= num_operations, "cassandra-stress did not import enough records"
    tempfile1 = NamedTemporaryFile(mode="w+", delete=False, encoding="utf-8")
    tempfile2 = NamedTemporaryFile(mode="w+", delete=False, encoding="utf-8")
    try:
        await asyncio.to_thread(node1.run_cqlsh, f"CONSISTENCY ALL; COPY {stress_table} TO '{tempfile1.name}'")
        with open(tempfile1.name, encoding="utf-8", newline="") as f:
            assert num_records == sum(1 for _ in csv.reader(f, quotechar='"', escapechar="\\"))
        await cql.run_async(f"TRUNCATE {stress_table}")
        await asyncio.to_thread(node1.run_cqlsh, f"COPY {stress_table} FROM '{tempfile1.name}'")
        await asyncio.to_thread(node1.run_cqlsh, f"CONSISTENCY ALL; COPY {stress_table} TO '{tempfile2.name}'")
        with open(tempfile1.name, encoding="utf-8") as f1, open(tempfile2.name, encoding="utf-8") as f2:
            assert sum(1 for _ in f1) == sum(1 for _ in f2)
    finally:
        os.unlink(tempfile1.name)
        os.unlink(tempfile2.name)


# 100,000 cassandra-stress rows round-trip through CSV files.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_bulk_round_trip_default(manager: ScyllaClusterManager):
    await bulk_round_trip(manager, 100000)


# 10,000 operations of the blogposts profile (clustering rows) round-trip,
# with batch_size_warn_threshold_in_kb 10, as in the dtest.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_bulk_round_trip_blogposts(manager: ScyllaClusterManager):
    await bulk_round_trip(manager, 10000, config={"batch_size_warn_threshold_in_kb": "10"},
                          profile=os.path.join(PROFILES, "blogposts.yaml"), stress_table="stresscql.blogposts")


############################################################################
# One test per dtest of TestCqlshCopy. The dtests' session is in keyspace
# ks; the queries here name it.

SKIP_DEV_DEBUG = pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
# cqlsh's error reports of a failed COPY FROM / COPY TO, as the dtests match them.
COPY_FROM_FAILURE_RE = re.compile(r"[1-9]\d* (?:parse|insert) error\(s\) encountered|Failed to (?:import|process)|COPY FROM error")


async def run_cqlsh(node1: Node, cmds: str, return_output: bool = False):
    """ccm's node1.run_cqlsh(cmds), off the event loop."""
    return await asyncio.to_thread(node1.run_cqlsh, cmds, None, return_output)


def temp_csv() -> str:
    """The dtests' NamedTemporaryFile(mode="w+", delete=False)."""
    f = NamedTemporaryFile(mode="w+", delete=False, encoding="utf-8")
    f.close()
    return f.name


def write_rows_to_csv(filename: str, data) -> None:
    with open(filename, "w", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        for row in data:
            writer.writerow(row)


def assert_csv_result_equal(csv_filename: str, results) -> None:
    """The dtests' assert_csv_result_equal(): the CSV file has as many rows
    as the query result."""
    with open(csv_filename) as f:
        csv_file = list(csv.reader(f))
    assert len(csv_file) == len(results), f"Expected {len(results)}, got {len(csv_file)}"


def assert_csvs_items_equal(filename1: str, filename2: str) -> None:
    with open(filename1) as x, open(filename2) as y:
        n1, n2 = len(x.readlines()), len(y.readlines())
    assert n1 == n2, f"Lines amount in the {filename1} is {n1} and in the {filename2} is {n2}"


def rows_to_list(rows) -> list:
    return [list(r) for r in rows]


async def select_list(cql, query: str) -> list:
    return rows_to_list(await cql.run_async(query))


async def insert_concurrent(cql, query: str, args) -> None:
    await asyncio.to_thread(execute_concurrent_with_args, cql, cql.prepare(query), args)


class FrozenDict(dict):
    """A hashable dict, for maps used as map keys (the dtest's ImmutableDict)."""
    def __hash__(self):
        return hash(tuple(sorted(self.items(), key=repr)))


Name = namedtuple("Name", ("firstname", "lastname"))
Address = namedtuple("Address", ("name", "number", "street", "phones"))


async def all_datatypes_prepare(manager: ScyllaClusterManager):
    """The dtests' all_datatypes_prepare(): prepare(), the UDTs and the
    testdatatype table of every type (named unqualified, after USE, as
    the dtest does), and a row of data for it."""
    node1, server, cql = await prepare(manager)
    await cql.run_async("CREATE TYPE name_type (firstname text, lastname text)")
    await cql.run_async("CREATE TYPE address_type (name frozen<name_type>, number int, street text, phones set<text>)")
    await cql.run_async("""
        CREATE TABLE testdatatype (
            a ascii PRIMARY KEY,
            b bigint,
            c blob,
            d boolean,
            e decimal,
            f double,
            g float,
            h inet,
            i int,
            j text,
            k timestamp,
            l timeuuid,
            m uuid,
            n varchar,
            o varint,
            p list<int>,
            q set<text>,
            r map<timestamp, text>,
            s tuple<int, text, boolean>,
            t frozen<address_type>,
            u frozen<list<list<address_type>>>,
            v frozen<map<map<int,int>,set<text>>>,
            w frozen<set<set<inet>>>,
        )""")
    cql.cluster.register_user_type("ks", "name_type", Name)
    cql.cluster.register_user_type("ks", "address_type", Address)
    date1 = datetime.datetime(2005, 7, 14, 12, 30, 0, 0, datetime.UTC)
    date2 = datetime.datetime(2005, 7, 14, 13, 30, 0, 0, datetime.UTC)
    addr1 = Address(Name("name1", "last1"), 1, "street 1", frozenset(["1111 2222", "3333 4444"]))
    addr2 = Address(Name("name2", "last2"), 2, "street 2", frozenset(["5555 6666", "7777 8888"]))
    addr3 = Address(Name("name3", "last3"), 3, "street 3", frozenset(["1111 2222", "3333 4444"]))
    addr4 = Address(Name("name4", "last4"), 4, "street 4", frozenset(["5555 6666", "7777 8888"]))
    data = (
        "ascii", 2**40, bytearray.fromhex("beef"), True, Decimal("3.14"), 2.444, 1.1, "127.0.0.1", 25, "ヽ(`ー`)/", date1,
        uuid1(), uuid4(), "asdf", 2**65, [1, 2, 3], frozenset(["3", "2", "1"]), {date1: "1", date2: "2"}, (1, "1", True),
        addr1, [[addr1, addr2], [addr3, addr4]], {FrozenDict({1: 1, 2: 2}): frozenset(["1", "2", "3"])},
        frozenset([frozenset(["127.0.0.1"]), frozenset(["127.0.0.1", "127.0.0.2"])]),
    )
    return node1, server, cql, data


async def non_default_delimiter(manager: ScyllaClusterManager, delimiter: str) -> None:
    """The dtests' non_default_delimiter_template(): 10000 ints exported
    with the given delimiter."""
    node1, _, cql = await prepare(manager)
    await cql.run_async("CREATE TABLE ks.testdelimiter (a int primary key)")
    await insert_concurrent(cql, "INSERT INTO ks.testdelimiter (a) VALUES (?)", [(i,) for i in range(10000)])
    results = await select_list(cql, "SELECT * FROM ks.testdelimiter")
    tempfile = temp_csv()
    try:
        await run_cqlsh(node1, f"COPY ks.testdelimiter TO '{tempfile}' WITH DELIMITER = '{delimiter}'")
        assert_csv_result_equal(tempfile, results)
    finally:
        os.unlink(tempfile)


# Ported from scylla-dtest cqlsh_tests/cqlsh_copy_tests.py::TestCqlshCopy::test_colon_delimiter.
@SKIP_DEV_DEBUG
async def test_colon_delimiter(manager: ScyllaClusterManager):
    await non_default_delimiter(manager, ":")


# Ported from scylla-dtest cqlsh_tests/cqlsh_copy_tests.py::TestCqlshCopy::test_letter_delimiter.
@SKIP_DEV_DEBUG
async def test_letter_delimiter(manager: ScyllaClusterManager):
    await non_default_delimiter(manager, "a")


# Ported from scylla-dtest cqlsh_tests/cqlsh_copy_tests.py::TestCqlshCopy::test_number_delimiter.
@SKIP_DEV_DEBUG
async def test_number_delimiter(manager: ScyllaClusterManager):
    await non_default_delimiter(manager, "1")


async def custom_null_indicator(manager: ScyllaClusterManager, indicator: str) -> None:
    """The dtests' custom_null_indicator_template(): rows with null values
    exported WITH NULL = <indicator>."""
    node1, _, cql, _ = await all_datatypes_prepare(manager)
    await cql.run_async("CREATE TABLE ks.testnullindicator (a int primary key, b text)")
    await insert_concurrent(cql, "INSERT INTO ks.testnullindicator (a, b) VALUES (?, ?)", [(1, "eggs"), (100, "sausage")])
    await insert_concurrent(cql, "INSERT INTO ks.testnullindicator (a) VALUES (?)", [(2,), (200,)])
    tempfile = temp_csv()
    try:
        await run_cqlsh(node1, f"COPY ks.testnullindicator TO '{tempfile}' WITH NULL = '{indicator}'")
        results = await select_list(cql, "SELECT a, b FROM ks.testnullindicator")
        results = [[indicator if value is None else value for value in row] for row in results]
        assert_csv_result_equal(tempfile, results)
    finally:
        os.unlink(tempfile)


# Ported from scylla-dtest cqlsh_tests/cqlsh_copy_tests.py::TestCqlshCopy::test_undefined_as_null_indicator.
@SKIP_DEV_DEBUG
async def test_undefined_as_null_indicator(manager: ScyllaClusterManager):
    await custom_null_indicator(manager, "undefined")


# Ported from scylla-dtest cqlsh_tests/cqlsh_copy_tests.py::TestCqlshCopy::test_null_as_null_indicator.
@SKIP_DEV_DEBUG
async def test_null_as_null_indicator(manager: ScyllaClusterManager):
    await custom_null_indicator(manager, "null")


# Ported from scylla-dtest cqlsh_tests/cqlsh_copy_tests.py::TestCqlshCopy::test_writing_use_header.
# COPY TO WITH HEADER = true writes the column names first.
@SKIP_DEV_DEBUG
async def test_writing_use_header(manager: ScyllaClusterManager):
    node1, _, cql = await prepare(manager)
    await cql.run_async("CREATE TABLE ks.testheader (a int primary key, b int)")
    await insert_concurrent(cql, "INSERT INTO ks.testheader (a, b) VALUES (?, ?)", [(1, 10), (2, 20), (3, 30)])
    tempfile = temp_csv()
    try:
        await run_cqlsh(node1, f"COPY ks.testheader TO '{tempfile}' WITH HEADER = true")
        with open(tempfile) as csvfile:
            csv_values = rows_to_list(csv.reader(csvfile))
    finally:
        os.unlink(tempfile)
    expected = [["a", "b"], ["1", "10"], ["2", "20"], ["3", "30"]]
    assert sorted(csv_values) == sorted(expected), f"Data after table copying not as expected. Expected: {expected}.\nGot: {csv_values}"


async def reading_counter(manager: ScyllaClusterManager, copy_options: dict | None = None) -> None:
    """The dtests' _test_reading_counter_template(): a CSV of counter values
    with a header is loaded by COPY FROM."""
    node1, _, cql = await prepare(manager)
    await cql.run_async("CREATE TABLE IF NOT EXISTS ks.testcounter (a int, b text, c counter, PRIMARY KEY (a, b))")
    tempfile = temp_csv()
    data = [[1, "1", 20], [2, "2", 40], [3, "3", 60], [4, "4", 80]]
    with open(tempfile, "w") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=["a", "b", "c"])
        writer.writeheader()
        for a, b, c in data:
            writer.writerow({"a": a, "b": b, "c": c})
    await cql.run_async("TRUNCATE TABLE ks.testcounter")
    cmds = f"COPY ks.testcounter FROM '{tempfile}' WITH HEADER = true"
    for opt, val in (copy_options or {}).items():
        cmds += f" AND {opt} = {val}"
    await run_cqlsh(node1, cmds)
    assert sorted(await select_list(cql, "SELECT * FROM ks.testcounter")) == sorted(data)


# Ported from scylla-dtest cqlsh_tests/cqlsh_copy_tests.py::TestCqlshCopy::test_reading_counter.
# CASSANDRA-9043.
@SKIP_DEV_DEBUG
async def test_reading_counter(manager: ScyllaClusterManager):
    await reading_counter(manager)


# Ported from scylla-dtest cqlsh_tests/cqlsh_copy_tests.py::TestCqlshCopy::test_reading_counter_without_batching.
# CASSANDRA-11474.
@SKIP_DEV_DEBUG
async def test_reading_counter_without_batching(manager: ScyllaClusterManager):
    await reading_counter(manager, {"MAXBATCHSIZE": "1"})


# Ported from scylla-dtest cqlsh_tests/cqlsh_copy_tests.py::TestCqlshCopy::test_writing_with_timeformat.
# COPY TO WITH DATETIMEFORMAT writes timestamps in that format (CASSANDRA-10633).
@SKIP_DEV_DEBUG
async def test_writing_with_timeformat(manager: ScyllaClusterManager):
    node1, _, cql = await prepare(manager)
    await cql.run_async("CREATE TABLE ks.testtimeformat (a int primary key, b timestamp)")
    utc = datetime.UTC
    await insert_concurrent(cql, "INSERT INTO ks.testtimeformat (a, b) VALUES (?, ?)",
                            [(1, datetime.datetime(2015, 1, 1, 7, 00, 0, 0, utc)), (2, datetime.datetime(2015, 6, 10, 12, 30, 30, 500, utc)),
                             (3, datetime.datetime(2015, 12, 31, 23, 59, 59, 999, utc))])
    tempfile = temp_csv()
    try:
        await run_cqlsh(node1, f"COPY ks.testtimeformat TO '{tempfile}' WITH DATETIMEFORMAT = '%Y/%m/%d %H:%M'")
        with open(tempfile) as csvfile:
            csv_values = list(csv.reader(csvfile))
    finally:
        os.unlink(tempfile)
    expected = [["1", "2015/01/01 07:00"], ["2", "2015/06/10 12:30"], ["3", "2015/12/31 23:59"]]
    assert sorted(csv_values, key=lambda x: x[0]) == expected, f"Actual value \"{csv_values}\" is not as expected '{expected}'"


# Ported from scylla-dtest cqlsh_tests/cqlsh_copy_tests.py::TestCqlshCopy::test_reading_with_ttl.
# COPY FROM WITH TTL writes rows that expire (CASSANDRA-9494): the rows
# are there with a TTL, then polled until they expired.
@SKIP_DEV_DEBUG
async def test_reading_with_ttl(manager: ScyllaClusterManager):
    node1, _, cql = await prepare(manager)
    await cql.run_async("CREATE TABLE ks.testttl (a int primary key, b int)")
    tempfile = temp_csv()
    data = [[1, 20], [2, 40], [3, 60], [4, 80]]
    with open(tempfile, "w") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=["a", "b"])
        for a, b in data:
            writer.writerow({"a": a, "b": b})
    # cqlsh's COPY waits up to 5 s for its worker processes to exit after the
    # rows are written; under load it does, so a TTL of the dtest's 5 s could
    # expire the rows before they are read back.
    ttl = 15
    try:
        await run_cqlsh(node1, f"COPY ks.testttl FROM '{tempfile}' WITH TTL = '{ttl}'")
    finally:
        os.unlink(tempfile)
    # The remaining TTL bounds the time since the write: fail clearly if the
    # rows are about to expire.
    remaining = [row[0] for row in await select_list(cql, "SELECT TTL(b) FROM ks.testttl")]
    assert len(remaining) == len(data) and all(r is not None for r in remaining), f"expected TTL on all {len(data)} imported rows, got {remaining}"
    assert min(remaining) >= 1, f"rows too close to expiry (remaining TTL {remaining}, TTL {ttl}s) for the 'not yet expired' check to be meaningful"
    assert sorted(await select_list(cql, "SELECT * FROM ks.testttl")) == sorted(data)
    # Poll until expired, at most ttl + 10 s, like the dtest's assert_all().
    for _ in range(2 * (ttl + 10)):
        if await select_list(cql, "SELECT * FROM ks.testttl") == []:
            break
        await asyncio.sleep(0.5)
    assert await select_list(cql, "SELECT * FROM ks.testttl") == []


# Ported from scylla-dtest cqlsh_tests/cqlsh_copy_tests.py::TestCqlshCopy::test_explicit_column_order_writing.
# COPY TO with the columns in another order than the table's.
@SKIP_DEV_DEBUG
async def test_explicit_column_order_writing(manager: ScyllaClusterManager):
    node1, _, cql = await prepare(manager)
    await cql.run_async("CREATE TABLE ks.testorder (a int primary key, b int, c text)")
    data = [[1, 20, "ham"], [2, 40, "eggs"], [3, 60, "beans"], [4, 80, "toast"]]
    await insert_concurrent(cql, "INSERT INTO ks.testorder (a, b, c) VALUES (?, ?, ?)", data)
    tempfile, reference_file = temp_csv(), temp_csv()
    try:
        await run_cqlsh(node1, f"COPY ks.testorder (a, c, b) TO '{tempfile}'")
        write_rows_to_csv(reference_file, [[a, c, b] for a, b, c in data])
        assert_csvs_items_equal(tempfile, reference_file)
    finally:
        os.unlink(tempfile)
        os.unlink(reference_file)


# Ported from scylla-dtest cqlsh_tests/cqlsh_copy_tests.py::TestCqlshCopy::test_explicit_column_order_reading.
# COPY FROM with the columns in another order than the table's.
@SKIP_DEV_DEBUG
async def test_explicit_column_order_reading(manager: ScyllaClusterManager):
    node1, _, cql = await prepare(manager)
    await cql.run_async("CREATE TABLE ks.testorder (a int primary key, b text, c int)")
    data = [[1, 20, "ham"], [2, 40, "eggs"], [3, 60, "beans"], [4, 80, "toast"]]
    tempfile, reference_file = temp_csv(), temp_csv()
    try:
        write_rows_to_csv(tempfile, data)
        await run_cqlsh(node1, f"COPY ks.testorder (a, c, b) FROM '{tempfile}'")
        results = await select_list(cql, "SELECT * FROM ks.testorder")
        write_rows_to_csv(reference_file, [[a, c, b] for a, b, c in data])
        assert_csv_result_equal(reference_file, results)
    finally:
        os.unlink(tempfile)
        os.unlink(reference_file)


async def quoted_column_names_reading(manager: ScyllaClusterManager, specify_column_names: bool) -> None:
    """The dtests' quoted_column_names_reading_template()."""
    node1, _, cql = await prepare(manager)
    await cql.run_async('CREATE TABLE ks.testquoted ("IdNumber" int PRIMARY KEY, "select" text)')
    data = [[1, "no"], [2, "Yes"], [3, "True"], [4, "false"]]
    tempfile = temp_csv()
    try:
        write_rows_to_csv(tempfile, data)
        columns = ' ("IdNumber", "select")' if specify_column_names else ""
        await run_cqlsh(node1, f"COPY ks.testquoted{columns} FROM '{tempfile}'")
        assert_csv_result_equal(tempfile, await select_list(cql, "SELECT * FROM ks.testquoted"))
    finally:
        os.unlink(tempfile)


# Ported from scylla-dtest cqlsh_tests/cqlsh_copy_tests.py::TestCqlshCopy::test_quoted_column_names_reading_specify_names.
@SKIP_DEV_DEBUG
async def test_quoted_column_names_reading_specify_names(manager: ScyllaClusterManager):
    await quoted_column_names_reading(manager, True)


# Ported from scylla-dtest cqlsh_tests/cqlsh_copy_tests.py::TestCqlshCopy::test_quoted_column_names_reading_dont_specify_names.
@SKIP_DEV_DEBUG
async def test_quoted_column_names_reading_dont_specify_names(manager: ScyllaClusterManager):
    await quoted_column_names_reading(manager, False)


async def quoted_column_names_writing(manager: ScyllaClusterManager, specify_column_names: bool) -> None:
    """The dtests' quoted_column_names_writing_template()."""
    node1, _, cql = await prepare(manager)
    await cql.run_async('CREATE TABLE ks.testquoted ("IdNumber" int PRIMARY KEY, "select" text)')
    data = [[1, "no"], [2, "Yes"], [3, "True"], [4, "false"]]
    await insert_concurrent(cql, 'INSERT INTO ks.testquoted ("IdNumber", "select") VALUES (?, ?)', data)
    tempfile, reference_file = temp_csv(), temp_csv()
    try:
        columns = ' ("IdNumber", "select")' if specify_column_names else ""
        await run_cqlsh(node1, f"COPY ks.testquoted{columns} TO '{tempfile}'")
        write_rows_to_csv(reference_file, data)
        assert_csvs_items_equal(tempfile, reference_file)
    finally:
        os.unlink(tempfile)
        os.unlink(reference_file)


# Ported from scylla-dtest cqlsh_tests/cqlsh_copy_tests.py::TestCqlshCopy::test_quoted_column_names_writing_specify_names.
@SKIP_DEV_DEBUG
async def test_quoted_column_names_writing_specify_names(manager: ScyllaClusterManager):
    await quoted_column_names_writing(manager, True)


# Ported from scylla-dtest cqlsh_tests/cqlsh_copy_tests.py::TestCqlshCopy::test_quoted_column_names_writing_dont_specify_names.
@SKIP_DEV_DEBUG
async def test_quoted_column_names_writing_dont_specify_names(manager: ScyllaClusterManager):
    await quoted_column_names_writing(manager, False)


async def read_invalid(manager: ScyllaClusterManager, load_as_int) -> None:
    """The dtests' data_validation_on_read_template(expect_invalid=True): a
    value that is not an int fails COPY FROM into an int column, and loads
    nothing (CASSANDRA-9302)."""
    node1, _, cql = await prepare(manager)
    await cql.run_async("CREATE TABLE ks.testvalidate (a int PRIMARY KEY, b int)")
    tempfile = temp_csv()
    try:
        write_rows_to_csv(tempfile, [[1, load_as_int]])
        out, err = await run_cqlsh(node1, f"COPY ks.testvalidate (a, b) FROM '{tempfile}'", return_output=True)
    finally:
        os.unlink(tempfile)
    results = await select_list(cql, "SELECT * FROM ks.testvalidate")
    copy_output = out + err
    assert COPY_FROM_FAILURE_RE.search(copy_output), f"No COPY FROM failure reported by cqlsh: '{copy_output}'"
    assert not results, "Unexpected data found in the 'testvalidate' table"


# Ported from scylla-dtest cqlsh_tests/cqlsh_copy_tests.py::TestCqlshCopy::test_read_invalid_float.
@SKIP_DEV_DEBUG
async def test_read_invalid_float(manager: ScyllaClusterManager):
    await read_invalid(manager, 2.14)


# Ported from scylla-dtest cqlsh_tests/cqlsh_copy_tests.py::TestCqlshCopy::test_read_invalid_uuid.
@SKIP_DEV_DEBUG
async def test_read_invalid_uuid(manager: ScyllaClusterManager):
    await read_invalid(manager, uuid4())


# Ported from scylla-dtest cqlsh_tests/cqlsh_copy_tests.py::TestCqlshCopy::test_read_invalid_text.
@SKIP_DEV_DEBUG
async def test_read_invalid_text(manager: ScyllaClusterManager):
    await read_invalid(manager, "test")


# Ported from scylla-dtest cqlsh_tests/cqlsh_copy_tests.py::TestCqlshCopy::test_all_datatypes_round_trip.
# A row of every CQL type round-trips through COPY TO, TRUNCATE and COPY FROM
# (CASSANDRA-9302).
@SKIP_DEV_DEBUG
async def test_all_datatypes_round_trip(manager: ScyllaClusterManager):
    node1, _, cql, data = await all_datatypes_prepare(manager)
    insert = cql.prepare("INSERT INTO ks.testdatatype (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w) "
                         "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
    await cql.run_async(insert, data)
    tempfile = temp_csv()
    try:
        await run_cqlsh(node1, f"COPY ks.testdatatype TO '{tempfile}'")
        exported_results = await select_list(cql, "SELECT * FROM ks.testdatatype")
        await cql.run_async("TRUNCATE ks.testdatatype")
        await run_cqlsh(node1, f"COPY ks.testdatatype FROM '{tempfile}'")
    finally:
        os.unlink(tempfile)
    assert await select_list(cql, "SELECT * FROM ks.testdatatype") == exported_results


# Ported from scylla-dtest cqlsh_tests/cqlsh_copy_tests.py::TestCqlshCopy::test_round_trip_murmur3.
# 10000 rows round-trip through COPY TO, TRUNCATE and COPY FROM on three nodes.
@SKIP_DEV_DEBUG
async def test_round_trip_murmur3(manager: ScyllaClusterManager):
    num_records = 10000
    node1, _, cql = await prepare(manager, nodes=3)
    await cql.run_async("CREATE TABLE ks.testcopyto (a text PRIMARY KEY, b int, c float, d uuid)")
    await insert_concurrent(cql, "INSERT INTO ks.testcopyto (a, b, c, d) VALUES (?, ?, ?, ?)",
                            [(str(i), i, float(i) + 0.5, uuid4()) for i in range(num_records)])
    results = await select_list(cql, "SELECT * FROM ks.testcopyto")
    tempfile = temp_csv()
    try:
        await run_cqlsh(node1, f"COPY ks.testcopyto TO '{tempfile}'", return_output=True)
        with open(tempfile) as f:
            lines_num = sum(1 for _ in f)
        assert num_records == lines_num, f"Expected exported records: {num_records}, actual exported: {lines_num}"
        await cql.run_async("TRUNCATE ks.testcopyto")
        await run_cqlsh(node1, f"COPY ks.testcopyto FROM '{tempfile}'", return_output=True)
    finally:
        os.unlink(tempfile)
    assert await select_list(cql, "SELECT * FROM ks.testcopyto") == results


# Ported from scylla-dtest cqlsh_tests/cqlsh_copy_tests.py::TestCqlshCopy::test_source_copy_round_trip.
# The same, with the COPY commands run by SOURCE (CASSANDRA-9083).
@SKIP_DEV_DEBUG
async def test_source_copy_round_trip(manager: ScyllaClusterManager):
    node1, _, cql = await prepare(manager)
    await cql.run_async("CREATE TABLE ks.testcopyto (a int, b text, c float, d uuid, PRIMARY KEY (a, b))")
    await insert_concurrent(cql, "INSERT INTO ks.testcopyto (a, b, c, d) VALUES (?, ?, ?, ?)",
                            [(i, str(i), float(i) + 0.5, uuid4()) for i in range(1000)])
    results = await select_list(cql, "SELECT * FROM ks.testcopyto")
    tempfile = temp_csv()
    try:
        for command in (f"COPY ks.testcopyto TO '{tempfile}' WITH HEADER=false;", f"COPY ks.testcopyto FROM '{tempfile}' WITH HEADER=false;"):
            if "FROM" in command:
                await cql.run_async("TRUNCATE ks.testcopyto")
            commandfile = temp_csv()
            with open(commandfile, "w", encoding="utf-8") as f:
                f.write("USE ks;\n")
                f.write(command)
            try:
                await run_cqlsh(node1, f"SOURCE '{commandfile}'")
            finally:
                os.unlink(commandfile)
    finally:
        os.unlink(tempfile)
    assert await select_list(cql, "SELECT * FROM ks.testcopyto") == results


async def stress_table_and_csv(manager: ScyllaClusterManager, num_records: int, export: bool = True):
    """prepare() and cassandra-stress writes into keyspace1.standard1, then
    (with export) COPY TO a CSV file and TRUNCATE, as the failure dtests do."""
    node1, server, cql = await prepare(manager)
    await cassandra_stress(manager, server, "write", f"n={num_records}", "-rate", "threads=50", create_keyspace=False)
    tempfile = temp_csv()
    if export:
        await run_cqlsh(node1, f"COPY keyspace1.standard1 TO '{tempfile}'")
        await cql.run_async("TRUNCATE keyspace1.standard1")
    return node1, cql, tempfile


async def count_standard1(cql) -> int:
    return (await cql.run_async("SELECT COUNT(*) FROM keyspace1.standard1"))[0][0]


# Ported from scylla-dtest cqlsh_tests/cqlsh_copy_tests.py::TestCqlshCopy::test_copy_to_with_child_process_crashing.
# A COPY TO child process exits (CQLSH_COPY_TEST_FAILURES exit_range): COPY
# TO reports a failure and exports fewer rows (CASSANDRA-9304).
@pytest.mark.tier3
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest runs it nightly in release mode only")
async def test_copy_to_with_child_process_crashing(manager: ScyllaClusterManager, monkeypatch):
    num_records = 100000
    node1, _, tempfile = await stress_table_and_csv(manager, num_records, export=False)
    try:
        monkeypatch.setenv("CQLSH_COPY_TEST_FAILURES", json.dumps({"exit_range": {"start": 0, "end": 5000000000000000000}}))
        out, err = await run_cqlsh(node1, f"COPY keyspace1.standard1 TO '{tempfile}'", return_output=True)
        assert err.strip(), f"No COPY TO failure reported by cqlsh, stdout: '{out}'"
        with open(tempfile) as f:
            lines_num = len(f.readlines())
    finally:
        os.unlink(tempfile)
    assert lines_num < num_records, f"Expected that lines in the file after copy is less than {num_records}, but got {lines_num}"


# Ported from scylla-dtest cqlsh_tests/cqlsh_copy_tests.py::TestCqlshCopy::test_copy_from_with_more_failures_than_max_attempts.
# A COPY FROM batch that fails more times than MAXATTEMPTS fails the import,
# which loads fewer rows (CASSANDRA-9302).
@SKIP_DEV_DEBUG
async def test_copy_from_with_more_failures_than_max_attempts(manager: ScyllaClusterManager, monkeypatch):
    num_records = 1000
    node1, cql, tempfile = await stress_table_and_csv(manager, num_records)
    try:
        monkeypatch.setenv("CQLSH_COPY_TEST_FAILURES", json.dumps({"failing_batch": {"id": 30, "failures": 5}}))
        out, err = await run_cqlsh(node1, f"COPY keyspace1.standard1 FROM '{tempfile}' WITH CHUNKSIZE='1' AND MAXATTEMPTS='3'",
                                   return_output=True)
    finally:
        os.unlink(tempfile)
    copy_output = out + err
    assert COPY_FROM_FAILURE_RE.search(copy_output), f"No COPY FROM failure reported by cqlsh: '{copy_output}'"
    assert await count_standard1(cql) < num_records


# Ported from scylla-dtest cqlsh_tests/cqlsh_copy_tests.py::TestCqlshCopy::test_copy_from_with_child_process_crashing.
# A COPY FROM child process exits (exit_batch): COPY FROM reports an error
# and loads fewer rows (CASSANDRA-9302).
@SKIP_DEV_DEBUG
async def test_copy_from_with_child_process_crashing(manager: ScyllaClusterManager, monkeypatch):
    num_records = 1000
    node1, cql, tempfile = await stress_table_and_csv(manager, num_records)
    try:
        monkeypatch.setenv("CQLSH_COPY_TEST_FAILURES", json.dumps({"exit_batch": {"id": 30}}))
        _, err = await run_cqlsh(node1, f"COPY keyspace1.standard1 FROM '{tempfile}' WITH CHUNKSIZE='1'", return_output=True)
    finally:
        os.unlink(tempfile)
    assert err, "no error was found"
    assert await count_standard1(cql) < num_records
