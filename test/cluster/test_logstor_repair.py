#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import json
import re
import time
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass, field, replace
from typing import Any, cast

import pytest

from cassandra import WriteFailure, WriteTimeout
from cassandra.cluster import ConsistencyLevel
from cassandra.query import SimpleStatement

from test.cluster.util import new_test_keyspace
from test.pylib.async_cql import _wrap_future
from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import inject_error
from test.pylib.util import wait_for

# Counts the writes a node refused, which under the database_apply injection is
# exactly the writes it was made to suppress.
FAILED_WRITES = 'scylla_database_total_writes_failed'


async def create_logstor_cluster(manager: ManagerClient, *, nodes: int = 2, smp: int = 1) -> tuple[list[Any], Any, list[Any]]:
    """Start a logstor cluster and return its servers, a cql session and its hosts.

    Hinted handoff is disabled so that a write suppressed on a replica by
    writes_suppressed_on() is never delivered to it later, which would converge
    the replicas before repair gets to run. Read balancing is disabled so that a
    host-pinned CL=ONE read is answered by the node it is sent to.
    """
    cmdline = ['--logger-log-level', 'logstor=debug', '--hinted-handoff-enabled', '0',
               '--cache-hit-rate-read-balancing', '0']
    if smp != 1:
        cmdline.append(f'--smp={smp}')
    cfg = {'experimental_features': ['logstor']}
    servers = await manager.servers_add(nodes, cmdline=cmdline, config=cfg, auto_rack_dc="dc1")
    cql, hosts = await manager.get_ready_cql(servers)
    # get_ready_cql() returns the driver's host list, which is not ordered like
    # `servers`. Order it so that hosts[i] is always the host of servers[i].
    by_address = {host.address: host for host in hosts}
    return servers, cast(Any, cql), [by_address[str(server.rpc_address)] for server in servers]


@asynccontextmanager
async def writes_suppressed_on(manager: ManagerClient, servers: list[Any], ks: str, table: str):
    """Make writes to `ks`.`table` fail on `servers` for the duration of the block.

    A CL=ONE write issued inside the block is stored only by the replicas that
    are not suppressed, which is how every test here diverges the replicas. This
    replaces stopping a node for the write: it needs no restart, and it leaves
    every node up so per-node state can be read with MUTATION_FRAGMENTS()
    throughout.
    """
    for server in servers:
        await manager.api.enable_injection(server.ip_addr, "database_apply", one_shot=False,
                parameters={"ks_name": ks, "cf_name": table, "what": "throw"})
    try:
        yield
    finally:
        for server in servers:
            await manager.api.disable_injection(server.ip_addr, "database_apply")


async def write_only_on(manager: ManagerClient, servers: list[Any], cql: Any, hosts: list[Any],
                        ks: str, table: str, index: int, statements: list[str]) -> None:
    """Run `statements` so that only the replica `index` ends up storing them.

    The writes go out at CL=ALL and each one is expected to fail, but that alone
    does not mean the suppressed replicas are done with it: the coordinator
    answers as soon as the consistency level becomes unreachable, which the first
    refusal already decides, so another suppressed replica can still be holding
    the mutation. Applied after the injection is lifted, it would silently undo
    the divergence. So wait until every suppressed replica has refused every
    statement before letting writes through again.
    """
    others = [server for i, server in enumerate(servers) if i != index]
    async with writes_suppressed_on(manager, others, ks, table):
        before = [await manager.metrics.query(server.ip_addr) for server in others]
        for statement in statements:
            try:
                await cql.run_async(SimpleStatement(statement, consistency_level=ConsistencyLevel.ALL), host=hosts[index])
            except (WriteFailure, WriteTimeout):
                pass  # Expected: the suppressed replicas refused the write.
            else:
                pytest.fail(f"the write was not suppressed on {[server.ip_addr for server in others]}: {statement}")
        # The injection is the only thing failing writes in these tests, so the
        # counter grows by exactly one per statement per suppressed replica.
        for server, metrics_before in zip(others, before):
            async def refused_every_statement(server=server, metrics_before=metrics_before):
                metrics = await manager.metrics.query(server.ip_addr)
                return metric_delta(metrics_before, metrics, FAILED_WRITES) >= len(statements) or None
            await wait_for(refused_every_statement, time.time() + 60)


async def token_order(cql: Any, ks: str, keys: list[int]) -> list[int]:
    """Return `keys` in ring order, which is the order a scan visits them in.

    The token of a partition key does not depend on the table it is written to,
    so the order is determined with a throwaway table. That lets a test know
    which key is where in the ring before it writes anything to the table under
    test, which is what a test that sets up a scan page boundary needs.
    """
    probe = f"{ks}.token_probe"
    await cql.run_async(f"CREATE TABLE {probe} (pk int PRIMARY KEY)")
    try:
        for key in keys:
            await cql.run_async(f"INSERT INTO {probe} (pk) VALUES ({key})")
        rows = await cql.run_async(f"SELECT pk, token(pk) AS tok FROM {probe}")
        return [row.pk for row in sorted(rows, key=lambda row: row.tok)]
    finally:
        await cql.run_async(f"DROP TABLE {probe}")


# Two bigint partition keys whose murmur3 hashes collide on the token
# 6874760189787677834 (the same pair test_conflicting_keys_read_repair.py uses).
# The keys of a logstor table are ordered by token first, so these two keys are
# adjacent in every scan, and only the full decorated key distinguishes them.
COLLIDING_PK1 = -4818441857111425024
COLLIDING_PK2 = -8686612841249112064


async def assert_colliding_tokens(cql: Any, ks: str) -> None:
    """Assert the two colliding keys really share one token.

    The same-token tests are built entirely on this property, and would silently
    degenerate into ordinary multi-key tests if the partitioner setup ever
    changed, so it is asserted rather than assumed.
    """
    probe = f"{ks}.collision_probe"
    await cql.run_async(f"CREATE TABLE {probe} (pk bigint PRIMARY KEY)")
    try:
        for key in (COLLIDING_PK1, COLLIDING_PK2):
            await cql.run_async(f"INSERT INTO {probe} (pk) VALUES ({key})")
        rows = await cql.run_async(f"SELECT pk, token(pk) AS tok FROM {probe}")
        tokens = {row.pk: row.tok for row in rows}
        assert tokens[COLLIDING_PK1] == tokens[COLLIDING_PK2], \
            f"the keys no longer share a token: {tokens}"
    finally:
        await cql.run_async(f"DROP TABLE {probe}")


#############################################################################
# Reading stored logstor records with SELECT ... FROM MUTATION_FRAGMENTS().
#
# A MUTATION_FRAGMENTS() query is answered from the coordinator's own storage
# and never goes through the storage proxy, so sending it to a specific host
# shows what that replica really stores, regardless of consistency level and of
# which other nodes are up. That is what makes the assertions below possible: a
# plain CQL read reflects a reconciling read of whichever replica answered it,
# while these helpers compare the records themselves - fragment kinds,
# tombstone, row marker and per-cell timestamps - across replicas.
#
# The helpers assume the table's partition key is a single column named `pk`,
# which is true for every table in this file.
#############################################################################

# Fragment kinds, as reported in the mutation_fragment_kind column.
PARTITION_START = 'partition start'
CLUSTERING_ROW = 'clustering row'
PARTITION_END = 'partition end'

# The source holding the on-disk log record is named
# "logstor-log:<file path>:<segment>:<offset>", so it identifies the record's
# location and changes if and only if the record is rewritten.
LOG_SOURCE_PREFIX = 'logstor-log:'
CACHE_SOURCE = 'logstor-cache'


@dataclass(frozen=True)
class Record:
    """The logstor record of one partition, as stored on one node.

    Two replicas are converged when their records compare equal, so the fields
    that are node-local by nature - where the record lives and whether it
    happens to be cached - are excluded from comparison.
    """
    kinds: tuple[str, ...]
    # (timestamp, deletion_time) of the partition tombstone, None if there is none.
    partition_tombstone: tuple[int, str] | None
    # Row marker timestamp, None if the record has no live row.
    marker_timestamp: int | None
    # (column, value, timestamp) per cell, sorted by column. value is None for a dead cell.
    cells: tuple[tuple[str, Any, int], ...]
    location: str = field(compare=False)
    # The same partition as currently held in the logstor cache, if it is cached.
    cached: 'Record | None' = field(compare=False, default=None)

    def cell_values(self) -> dict[str, Any]:
        return {name: value for name, value, _ in self.cells}

    def cell_timestamps(self) -> dict[str, int]:
        return {name: timestamp for name, _, timestamp in self.cells}

    def tombstone_timestamp(self) -> int | None:
        return self.partition_tombstone[0] if self.partition_tombstone else None


def _parse_source(rows: list[Any], source: str) -> Record:
    """Build a Record out of the fragments of a single mutation source.

    Rows arrive in clustering order, which for the MUTATION_FRAGMENTS() schema
    is mutation source, then partition region, so they are in fragment order.
    """
    tombstone = None
    marker_timestamp = None
    cells: dict[str, tuple[Any, int]] = {}
    for row in rows:
        if row.mutation_fragment_kind == PARTITION_START:
            metadata = json.loads(row.metadata)['tombstone']
            if metadata:
                tombstone = (metadata['timestamp'], metadata['deletion_time'])
        elif row.mutation_fragment_kind == CLUSTERING_ROW:
            metadata = json.loads(row.metadata)
            marker_timestamp = metadata.get('marker', {}).get('timestamp')
            values = json.loads(row.value)
            for name, cell in metadata['columns'].items():
                cells[name] = (values.get(name), cell['timestamp'])
    return Record(
        kinds=tuple(row.mutation_fragment_kind for row in rows),
        partition_tombstone=tombstone,
        marker_timestamp=marker_timestamp,
        cells=tuple(sorted((name, value, timestamp) for name, (value, timestamp) in cells.items())),
        location=source,
    )


def _record_from_rows(rows: list[Any]) -> Record:
    by_source: dict[str, list[Any]] = {}
    for row in rows:
        by_source.setdefault(row.mutation_source, []).append(row)
    log_sources = [source for source in by_source if source.startswith(LOG_SOURCE_PREFIX)]
    assert len(log_sources) == 1, f"expected exactly one log record source, got {sorted(by_source)}"
    record = _parse_source(by_source[log_sources[0]], log_sources[0])
    if CACHE_SOURCE in by_source:
        record = replace(record, cached=_parse_source(by_source[CACHE_SOURCE], CACHE_SOURCE))
    return record


async def read_record(cql: Any, host: Any, table: str, pk: int) -> Record | None:
    """Read the record of one partition as stored on `host`, None if the node has no record for it."""
    rows = await cql.run_async(f"SELECT * FROM MUTATION_FRAGMENTS({table}) WHERE pk = {pk}", host=host)
    return _record_from_rows(rows) if rows else None


async def read_records(cql: Any, host: Any, table: str) -> dict[int, Record]:
    """Read every record stored on `host`, keyed by partition key.

    The key set is the one `host` holds locally, so a partition missing from a
    node is missing from its result.
    """
    rows = await cql.run_async(f"SELECT * FROM MUTATION_FRAGMENTS({table})", host=host)
    by_pk: dict[int, list[Any]] = {}
    for row in rows:
        by_pk.setdefault(row.pk, []).append(row)
    return {pk: _record_from_rows(pk_rows) for pk, pk_rows in by_pk.items()}


async def read_all_records(cql: Any, hosts: list[Any], table: str) -> list[dict[int, Record]]:
    """Read the records stored on every host, one dict per host, in `hosts` order."""
    return [await read_records(cql, host, table) for host in hosts]


async def read_locally(cql: Any, host: Any, query: str) -> list[Any]:
    """Run `query` on `host` and assert `host` answered it out of its own storage.

    Read balancing is disabled in create_logstor_cluster() and the read is pinned
    with CL=LOCAL_ONE, but only the query trace proves the coordinator did not
    forward the read to the other replica - which matters when the point of the
    read is the side effect it has on the local node, such as populating the
    logstor cache.
    """
    result = cql.execute_async(SimpleStatement(query, consistency_level=ConsistencyLevel.LOCAL_ONE), host=host, trace=True)
    rows = await _wrap_future(result)
    traces = await asyncio.to_thread(result.get_all_query_traces, max_wait_per=60)
    for trace in traces:
        for event in trace.events:
            assert event.source == host.address, \
                f"the read was served by {event.source}, not by {host.address}"
    return rows


def check_record(record: Record) -> None:
    """Assert the invariants that must hold for any stored logstor record.

    A logstor partition is a partition start, at most one clustering row (at the
    empty clustering key), and a partition end. A static row, a range tombstone
    change or several rows would mean the repair apply path produced a partition
    logstor is not supposed to store.

    Also checks that a cached copy of the partition, if there is one, agrees with
    the stored record: logstor evicts the cached mutation when a record is
    overwritten, and repair writes through the same path, so a disagreement means
    a node would keep serving its pre-repair value.
    """
    assert record.kinds[0] == PARTITION_START, f"unexpected first fragment: {record.kinds}"
    assert record.kinds[-1] == PARTITION_END, f"unexpected last fragment: {record.kinds}"
    rows = record.kinds[1:-1]
    assert all(kind == CLUSTERING_ROW for kind in rows), f"unexpected fragments: {record.kinds}"
    assert len(rows) <= 1, f"more than one row in a logstor partition: {record.kinds}"
    assert record.cached is None or record.cached == record, \
        f"stale logstor cache: cached {record.cached}, stored {record}"


def check_deleted_record(record: Record, timestamp: int) -> None:
    """Assert `record` is a partition deleted at `timestamp`.

    A converged record may still physically contain the row the tombstone
    deletes, because merging applies the tombstone but does not purge the data it
    shadows. So what has to hold is that the tombstone is stored, and that
    nothing in the record is newer than it - at equal timestamps the tombstone
    wins.
    """
    assert record.tombstone_timestamp() == timestamp, f"expected a tombstone at {timestamp}: {record}"
    assert (record.marker_timestamp or 0) <= timestamp, f"row marker survived the tombstone: {record}"
    for name, cell_timestamp in record.cell_timestamps().items():
        assert cell_timestamp <= timestamp, f"cell {name} survived the tombstone: {record}"


@dataclass(frozen=True)
class Deleted:
    """The expected state of a partition that converged to a deletion at `timestamp`."""
    timestamp: int


class _Live:
    """The expected state of a live partition whose cells the caller checks itself."""

    def __repr__(self) -> str:
        return 'LIVE'


LIVE = _Live()

# What a partition is expected to hold after the tested operation: the exact cell
# values of its converged record, Deleted(timestamp), or LIVE.
Expected = dict[int, dict[str, Any] | Deleted | _Live]


async def check_converged(cql: Any, hosts: list[Any], table: str, expected: Expected) -> dict[int, Record]:
    """Assert every replica stores identical records holding exactly `expected`, and return them.

    This is the verification step of every test in this file. It reads what each
    node stores rather than running a plain SELECT because a SELECT is answered
    by the storage proxy coordinator, which is free to serve it from either
    replica and may itself read-repair the very divergence the test is about. It
    is also strictly stronger than comparing query results: it sees the partition
    tombstone, the row marker and the per-cell timestamps, so a replica that ends
    up with the right values but a different timestamp, or that received only the
    cells it was missing instead of the whole converged partition, is caught.

    `expected` must name every partition key the replicas hold, so a record that
    should have been deleted, or one that should never have been created, is
    caught by the key set alone.
    """
    records = check_records_converged(hosts, await read_all_records(cql, hosts, table))
    assert set(records) == set(expected), \
        f"the replicas hold keys {sorted(records)}, expected {sorted(expected)}"
    for pk, want in expected.items():
        if isinstance(want, Deleted):
            check_deleted_record(records[pk], want.timestamp)
        elif want is not LIVE:
            assert records[pk].cell_values() == want, f"pk {pk}: expected {want}, got {records[pk]}"
    return records


def check_records_converged(hosts: list[Any], per_host: list[dict[int, Record]]) -> dict[int, Record]:
    """Assert every replica stores identical records, and return them.

    Stricter than comparing query results: it compares the stored records
    including tombstone, row marker and per-cell timestamps, so a replica that
    ends up with the right values but a different timestamp, or that received
    only the rows it was missing instead of the whole converged partition, is
    caught.
    """
    for records in per_host:
        for record in records.values():
            check_record(record)
    reference = per_host[0]
    for host, records in zip(hosts[1:], per_host[1:]):
        assert set(records) == set(reference), \
            f"host {host} holds keys {sorted(records)}, host {hosts[0]} holds {sorted(reference)}"
        for pk, record in records.items():
            assert record == reference[pk], \
                f"pk {pk} differs: host {hosts[0]} has {reference[pk]}, host {host} has {record}"
    return reference


def locations(records: dict[int, Record]) -> dict[int, str]:
    return {pk: record.location for pk, record in records.items()}


# Scalar counters of the row level algorithm, as logged by the repair master when
# it finishes a repair. The two fast path counters count sync windows that were
# found to be in sync - the first when the replicas reported the same sync
# boundary and the same combined hash, the second when the combined hashes of the
# negotiated window matched - and the slow path counter counts the windows that
# needed full row hashes and row transfers.
REPAIR_STATS_FIELDS = (
    'round_nr',
    'round_nr_fast_path_already_synced',
    'round_nr_fast_path_same_combined_hashes',
    'round_nr_slow_path',
    'tx_row_nr',
    'rx_row_nr',
)

REPAIR_STATS_LINE = r'repair\[.*\]: stats: '


async def read_repair_stats(log: Any, from_mark: int) -> dict[str, int]:
    """Sum the repair stats logged since `from_mark`.

    One line is logged per repair task, so a repair of a tablet keyspace logs one
    per tablet, on the shard that owns it. They all have to be summed to describe
    the whole repair.
    """
    await log.wait_for(REPAIR_STATS_LINE, from_mark=from_mark, timeout=60)
    matches = await log.grep(REPAIR_STATS_LINE, from_mark=from_mark)
    stats = dict.fromkeys(REPAIR_STATS_FIELDS, 0)
    for line, _ in matches:
        for name in REPAIR_STATS_FIELDS:
            # The names are matched with a trailing '=' so that, for example,
            # tx_row_nr does not match the tx_row_nr_peer map on the same line.
            match = re.search(rf'\b{name}=(\d+)', line)
            assert match, f"{name} missing from the repair stats line: {line}"
            stats[name] += int(match.group(1))
    return stats


def metric_delta(before: Any, after: Any, name: str) -> int:
    """Growth of a repair counter. The counters are absent until the first repair runs."""
    return int((after.get(name) or 0) - (before.get(name) or 0))


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_repair_missing_partition(manager: ManagerClient):
    """Test that repair syncs a partition that exists on one node but is missing from another node.

    The write to node2 is suppressed, so node1 stores the row and node2 does not.
    After repair, node2 should have the row with the correct value.
    """
    servers, cql, hosts = await create_logstor_cluster(manager)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v text) WITH storage_engine = 'logstor'")

        await write_only_on(manager, servers, cql, hosts, ks, "test", 0,
                            [f"INSERT INTO {ks}.test (pk, v) VALUES (1, 'a')"])

        # The record is missing from node2's own storage, which a reconciling
        # read could not show while both nodes are up.
        assert await read_record(cql, hosts[1], f"{ks}.test", 1) is None

        await manager.api.repair(servers[0].ip_addr, ks, "test")

        records = await check_converged(cql, hosts, f"{ks}.test", {1: {'v': 'a'}})
        assert records[1].partition_tombstone is None


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_repair_same_timestamp_conflict(manager: ManagerClient):
    """Test that repair reconciles two rows with the same primary key, same timestamp, but different values.

    Node1 has value 'a', node2 has value 'b', both at timestamp 1000.
    After repair, both nodes converge to the same value (the one that wins the
    cell-level tiebreaker — largest value wins at equal timestamp).
    """
    servers, cql, hosts = await create_logstor_cluster(manager)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v text) WITH storage_engine = 'logstor'")

        await write_only_on(manager, servers, cql, hosts, ks, "test", 0,
                            [f"INSERT INTO {ks}.test (pk, v) VALUES (1, 'a') USING TIMESTAMP 1000"])
        await write_only_on(manager, servers, cql, hosts, ks, "test", 1,
                            [f"INSERT INTO {ks}.test (pk, v) VALUES (1, 'b') USING TIMESTAMP 1000"])

        # The stored records show the divergence the test set up, which a
        # reconciling read cannot show.
        before = await read_all_records(cql, hosts, f"{ks}.test")
        assert before[0][1].cell_values() == {'v': 'a'}
        assert before[1][1].cell_values() == {'v': 'b'}

        await manager.api.repair(servers[0].ip_addr, ks, "test")

        # At equal timestamps the lexicographically larger cell value wins. Both
        # nodes must store the same record, with the original timestamp: the merge
        # picks a winning cell, it does not rewrite timestamps.
        records = await check_converged(cql, hosts, f"{ks}.test", {1: {'v': 'b'}})
        assert records[1].cell_timestamps() == {'v': 1000}
        assert records[1].marker_timestamp == 1000


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_repair_same_timestamp_tombstone_wins(manager: ManagerClient):
    """Test that a tombstone beats a live cell when both have the same timestamp.

    Node1 has a live row (v=100), node2 has a tombstone deleting pk=1, both at timestamp 1000.
    After repair, both nodes converge to the tombstone (row deleted).
    """
    servers, cql, hosts = await create_logstor_cluster(manager)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v int) WITH storage_engine = 'logstor'")

        await write_only_on(manager, servers, cql, hosts, ks, "test", 0,
                            [f"INSERT INTO {ks}.test (pk, v) VALUES (1, 100) USING TIMESTAMP 1000"])
        await write_only_on(manager, servers, cql, hosts, ks, "test", 1,
                            [f"DELETE FROM {ks}.test USING TIMESTAMP 1000 WHERE pk = 1"])

        before = await read_all_records(cql, hosts, f"{ks}.test")
        assert before[0][1].cell_values() == {'v': '100'}
        assert before[1][1].tombstone_timestamp() == 1000

        await manager.api.repair(servers[0].ip_addr, ks, "test")

        # The converged record must store the deletion itself, and the live row it
        # shadows must not survive it.
        await check_converged(cql, hosts, f"{ks}.test", {1: Deleted(1000)})


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_repair_multishard_routing(manager: ManagerClient):
    """Test that repair works correctly with multiple shards and tablets.

    Use smp=2 and 2 tablets. Insert disjoint partition sets on each node
    (pk 0-15 on node1, pk 16-31 on node2). After repair, both nodes should
    have all 32 rows.
    """
    servers, cql, hosts = await create_logstor_cluster(manager, smp=2)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2} AND tablets = {'initial': 2}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v int) WITH storage_engine = 'logstor'")

        await write_only_on(manager, servers, cql, hosts, ks, "test", 0,
                            [f"INSERT INTO {ks}.test (pk, v) VALUES ({pk}, {pk + 100})" for pk in range(16)])
        await write_only_on(manager, servers, cql, hosts, ks, "test", 1,
                            [f"INSERT INTO {ks}.test (pk, v) VALUES ({pk}, {pk + 100})" for pk in range(16, 32)])

        await manager.api.repair(servers[0].ip_addr, ks, "test")

        # Every record must be present in each node's own storage: a scan would go
        # through the read path, where a range can be served by the other replica,
        # so it would not prove the rows were applied on the right node.
        await check_converged(cql, hosts, f"{ks}.test", {pk: {'v': str(pk + 100)} for pk in range(32)})


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_repair_mixed_outcomes(manager: ManagerClient):
    """Test repair with a mix of conflict types in a single repair run.

    Scenarios covered on pk 1-6:
      pk=1: identical on both nodes — no-op.
      pk=2: same timestamp, different values — tiebreaker picks larger.
      pk=3: live row vs tombstone (same timestamp) — tombstone wins.
      pk=4: different timestamps — newer wins.
      pk=5: only on node1 — synced to node2.
      pk=6: only on node2 — synced to node1.
    After repair, both nodes have identical data matching the expected outcome for each case.
    """
    servers, cql, hosts = await create_logstor_cluster(manager)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v text) WITH storage_engine = 'logstor'")

        await write_only_on(manager, servers, cql, hosts, ks, "test", 0, [
            f"INSERT INTO {ks}.test (pk, v) VALUES ({pk}, '{v}') USING TIMESTAMP 1000"
            for pk, v in [(1, 'a'), (2, 'b'), (3, 'd'), (4, 'e'), (5, 'g')]])
        await write_only_on(manager, servers, cql, hosts, ks, "test", 1, [
            f"INSERT INTO {ks}.test (pk, v) VALUES (2, 'c') USING TIMESTAMP 1000",
            f"DELETE FROM {ks}.test USING TIMESTAMP 1000 WHERE pk = 3",
            f"INSERT INTO {ks}.test (pk, v) VALUES (4, 'f') USING TIMESTAMP 900",
            f"INSERT INTO {ks}.test (pk, v) VALUES (6, 'h') USING TIMESTAMP 1000"])

        await manager.api.repair(servers[0].ip_addr, ks, "test")

        # Both nodes must store the same record for every key, each with the
        # expected shape: a live row, or the tombstone record for the deleted pk=3.
        # check_converged() also checks the record shape invariants, so a repair
        # apply that produced, say, a static row is caught here.
        await check_converged(cql, hosts, f"{ks}.test", {
            1: {'v': 'a'},
            2: {'v': 'c'},
            3: Deleted(1000),
            4: {'v': 'e'},
            5: {'v': 'g'},
            6: {'v': 'h'},
        })


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_repair_mutation_merge_conflicting_rows_with_same_timestamp_multicell(manager: ManagerClient):
    """Test that repair correctly merges multi-column rows with same-timestamp column-level conflicts.

    Node1 has (a='a', b='b'), node2 has (a='b', b='a'), both at timestamp 1000.
    After repair, each column is reconciled independently: the larger value wins
    per-column, so the result is (a='b', b='b').
    """
    servers, cql, hosts = await create_logstor_cluster(manager)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, a text, b text) WITH storage_engine = 'logstor'")

        await write_only_on(manager, servers, cql, hosts, ks, "test", 0,
                            [f"INSERT INTO {ks}.test (pk, a, b) VALUES (1, 'a', 'b') USING TIMESTAMP 1000"])
        await write_only_on(manager, servers, cql, hosts, ks, "test", 1,
                            [f"INSERT INTO {ks}.test (pk, a, b) VALUES (1, 'b', 'a') USING TIMESTAMP 1000"])

        before = await read_all_records(cql, hosts, f"{ks}.test")
        assert before[0][1].cell_values() == {'a': 'a', 'b': 'b'}
        assert before[1][1].cell_values() == {'a': 'b', 'b': 'a'}

        await manager.api.repair(servers[0].ip_addr, ks, "test")

        # Each column is reconciled independently but keeps its own timestamp.
        records = await check_converged(cql, hosts, f"{ks}.test", {1: {'a': 'b', 'b': 'b'}})
        assert records[1].cell_timestamps() == {'a': 1000, 'b': 1000}


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_repair_three_replica_multicell_merge(manager: ManagerClient):
    """Test that repair converges a partition whose columns are split across three replicas.

    Each replica holds a disjoint column of pk=1 at the same timestamp (node1 a='x',
    node2 b='y', node3 c='z'), because the other replicas were down when each write
    happened and hinted handoff is disabled. Repair must gather all versions on the
    master and push the fully merged partition to every replica, so each node ends up
    with a='x', b='y', c='z'. This exercises the logstor whole-partition convergence:
    sending only the differing rows would drop the columns held by other replicas.
    """
    servers, cql, hosts = await create_logstor_cluster(manager, nodes=3)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, a text, b text, c text) WITH storage_engine = 'logstor'")

        # Write a disjoint column on each node, suppressed on the other two.
        for idx, col, val in [(0, 'a', 'x'), (1, 'b', 'y'), (2, 'c', 'z')]:
            await write_only_on(manager, servers, cql, hosts, ks, "test", idx,
                                [f"INSERT INTO {ks}.test (pk, {col}) VALUES (1, '{val}') USING TIMESTAMP 1000"])

        await manager.api.repair(servers[0].ip_addr, ks, "test")

        # All three replicas must store the same merged record, with each column
        # keeping the timestamp it was written with. A replica that received only
        # the rows it was missing would store a partial partition here.
        records = await check_converged(cql, hosts, f"{ks}.test", {1: {'a': 'x', 'b': 'y', 'c': 'z'}})
        assert records[1].cell_timestamps() == {'a': 1000, 'b': 1000, 'c': 1000}
        assert records[1].marker_timestamp == 1000


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_repair_three_replica_conflicting_versions(manager: ManagerClient):
    """Test that repair converges three replicas that hold conflicting versions.

    In test_repair_three_replica_multicell_merge the three versions are disjoint,
    so merging them cannot lose anything. Here every replica holds a genuinely
    conflicting version of the same partition, and one of them is a delete:

      pk=1: node1 v='a', node2 v='b', node3 v='c', all at ts=1000
            - the cell tiebreaker picks the largest value, 'c'.
      pk=2: node1 v='old' ts=1000, node2 v='new' ts=3000, node3 delete ts=2000
            - v='new' wins, over a tombstone that is newer than node1's version.
      pk=3: only on node3 - synced to the other two.

    With three replicas the master merges versions pulled from two different
    followers into one partition before flushing it and sending it on, and it
    copies the working row buffer instead of moving it (the _nr_peer_nodes == 1
    shortcut in get_row_diff()). A two node cluster exercises neither.
    """
    servers, cql, hosts = await create_logstor_cluster(manager, nodes=3)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v text) WITH storage_engine = 'logstor'")
        table = f"{ks}.test"

        await write_only_on(manager, servers, cql, hosts, ks, "test", 0, [
            f"INSERT INTO {table} (pk, v) VALUES (1, 'a') USING TIMESTAMP 1000",
            f"INSERT INTO {table} (pk, v) VALUES (2, 'old') USING TIMESTAMP 1000"])
        await write_only_on(manager, servers, cql, hosts, ks, "test", 1, [
            f"INSERT INTO {table} (pk, v) VALUES (1, 'b') USING TIMESTAMP 1000",
            f"INSERT INTO {table} (pk, v) VALUES (2, 'new') USING TIMESTAMP 3000"])
        await write_only_on(manager, servers, cql, hosts, ks, "test", 2, [
            f"INSERT INTO {table} (pk, v) VALUES (1, 'c') USING TIMESTAMP 1000",
            f"DELETE FROM {table} USING TIMESTAMP 2000 WHERE pk = 2",
            f"INSERT INTO {table} (pk, v) VALUES (3, 'only') USING TIMESTAMP 1000"])

        before = await read_all_records(cql, hosts, table)
        assert [sorted(records) for records in before] == [[1, 2], [1, 2], [1, 2, 3]]
        assert [records[1].cell_values()['v'] for records in before] == ['a', 'b', 'c']
        assert before[0][2].cell_timestamps() == {'v': 1000}, f"node1 pk=2: {before[0][2]}"
        assert before[1][2].cell_timestamps() == {'v': 3000}, f"node2 pk=2: {before[1][2]}"
        assert before[2][2].tombstone_timestamp() == 2000, f"node3 pk=2: {before[2][2]}"

        await manager.api.repair(servers[0].ip_addr, ks, "test")

        records = await check_converged(cql, hosts, table,
                                       {1: {'v': 'c'}, 2: {'v': 'new'}, 3: {'v': 'only'}})
        assert records[1].cell_timestamps() == {'v': 1000}
        assert records[2].cell_timestamps() == {'v': 3000}
        # The winning version is newer than the tombstone, so the partition stays
        # live, but the converged record must still carry the tombstone: dropping
        # it would let node1's older version resurrect on a later repair.
        assert records[2].tombstone_timestamp() == 2000


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_repair_delete_newer_than_live_no_resurrection(manager: ManagerClient):
    """Test that a delete newer than a live row converges to deleted and cannot be resurrected.

    Node1 has a live row (v=100) at timestamp 1000, node2 deletes pk=1 at timestamp 2000.
    After repair both nodes must be deleted, and the converged record must carry the
    tombstone timestamp (2000). A subsequent write at an older timestamp (1500) must
    therefore be rejected on every node, leaving the partition deleted. Without deriving
    the record timestamp from the max of the row marker and partition tombstone, the
    converged record would keep timestamp 1000 and the older write would resurrect it.
    """
    servers, cql, hosts = await create_logstor_cluster(manager)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v int) WITH storage_engine = 'logstor'")

        await write_only_on(manager, servers, cql, hosts, ks, "test", 0,
                            [f"INSERT INTO {ks}.test (pk, v) VALUES (1, 100) USING TIMESTAMP 1000"])
        await write_only_on(manager, servers, cql, hosts, ks, "test", 1,
                            [f"DELETE FROM {ks}.test USING TIMESTAMP 2000 WHERE pk = 1"])

        await manager.api.repair(servers[0].ip_addr, ks, "test")

        # The converged record must carry the deletion timestamp, and nothing in it
        # may be newer than the tombstone. A record that kept only the row marker's
        # older timestamp is what would let the older write below resurrect it.
        await check_converged(cql, hosts, f"{ks}.test", {1: Deleted(2000)})

        # A write older than the tombstone must not resurrect the partition. It
        # goes out at CL=ALL so that both replicas are known to have seen it and
        # rejected it by the time the records are read.
        await cql.run_async(SimpleStatement(f"INSERT INTO {ks}.test (pk, v) VALUES (1, 200) USING TIMESTAMP 1500",
                                            consistency_level=ConsistencyLevel.ALL))

        await check_converged(cql, hosts, f"{ks}.test", {1: Deleted(2000)})


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_repair_window_never_splits_partition(manager: ManagerClient):
    """Test that a repair sync window never splits a logstor partition.

    Logstor repair converges whole partitions, so all fragments of a partition
    must land in the same sync window. The repair_tiny_max_row_buf_size injection
    shrinks the repair row buffer below a single fragment, which is what makes
    the window boundaries observable at all. Keeping windows partition-aligned
    then takes two things, and the two key groups below cover one each:

    - read_rows_from_disk() reads past the byte budget until the open partition
      is complete, so the row buffer never ends inside one. The two_fragment
      keys need this: a record written by a batch that combines a partition
      delete with a newer insert carries a partition tombstone and a row, and a
      buffer that stopped between them would converge half a partition.

    - get_sync_boundary() reports the boundary at after_all_clustered_rows()
      rather than at the last row's own position. The tombstone_only keys need
      this: on node1 the partition is a partition tombstone and nothing else, so
      its only repair row sits at the partition-start position. Reported as-is,
      that boundary is the minimum across the replicas and cuts node2's row for
      the same partition into the next window.

    Both groups end up with the tombstone lost from the converged record and the
    cells it deleted resurrected, which is what the assertions below catch.

    Both groups converge to the same value, and differ only in which replica
    holds the tombstone and in the fragment shape that produces the boundary:

      two_fragment keys   node1: batch DELETE ts=2000 + INSERT v='new' ts=3000
                          node2: INSERT v='old', w='stale' ts=1000
      tombstone_only keys node1: DELETE ts=2000, nothing else
                          node2: batch INSERT w='stale' ts=1000 + v='new' ts=3000

      converged, both:    live, v='new', w killed by the tombstone at 2000

    Note the second group needs node2's surviving cell to be *newer* than the
    tombstone. With an older one a split does no visible damage: logstor keeps
    the record with the highest timestamp, so the older row could never replace
    the tombstone that was converged without it.
    """
    servers, cql, hosts = await create_logstor_cluster(manager)

    two_fragment_keys = range(10)
    tombstone_only_keys = range(10, 20)
    keys = list(two_fragment_keys) + list(tombstone_only_keys)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v text, w text) WITH storage_engine = 'logstor'")

        await write_only_on(manager, servers, cql, hosts, ks, "test", 0, [
            f"BEGIN UNLOGGED BATCH "
            f"DELETE FROM {ks}.test USING TIMESTAMP 2000 WHERE pk = {pk}; "
            f"INSERT INTO {ks}.test (pk, v) VALUES ({pk}, 'new') USING TIMESTAMP 3000; "
            f"APPLY BATCH" for pk in two_fragment_keys] + [
            f"DELETE FROM {ks}.test USING TIMESTAMP 2000 WHERE pk = {pk}" for pk in tombstone_only_keys])
        await write_only_on(manager, servers, cql, hosts, ks, "test", 1, [
            f"INSERT INTO {ks}.test (pk, v, w) VALUES ({pk}, 'old', 'stale') USING TIMESTAMP 1000"
            for pk in two_fragment_keys] + [
            f"BEGIN UNLOGGED BATCH "
            f"INSERT INTO {ks}.test (pk, w) VALUES ({pk}, 'stale') USING TIMESTAMP 1000; "
            f"INSERT INTO {ks}.test (pk, v) VALUES ({pk}, 'new') USING TIMESTAMP 3000; "
            f"APPLY BATCH" for pk in tombstone_only_keys])

        async with AsyncExitStack() as stack:
            for server in servers:
                await stack.enter_async_context(inject_error(manager.api, server.ip_addr, "repair_tiny_max_row_buf_size"))
            await manager.api.repair(servers[0].ip_addr, ks, "test")

        # The cells are checked below rather than through `expected`: w may
        # legitimately still be present in the record, shadowed by the tombstone,
        # so the converged cell set is not fixed.
        records = await check_converged(cql, hosts, f"{ks}.test", {pk: LIVE for pk in keys})
        # The converged record must still carry the partition tombstone next to
        # the newer row. A record that only kept the row reads the same to a
        # client, but has lost the deletion: a later write of w at a timestamp
        # between 1000 and 2000 would then resurrect it.
        for pk, record in records.items():
            assert record.kinds == (PARTITION_START, CLUSTERING_ROW, PARTITION_END), f"pk {pk}: {record}"
            assert record.tombstone_timestamp() == 2000, f"pk {pk}: tombstone lost from the converged record: {record}"
            assert record.cell_values()['v'] == 'new', f"pk {pk}: {record}"
            # w may still be present in the record, shadowed by the tombstone,
            # but it must never be newer than the tombstone that deleted it.
            assert record.cell_timestamps().get('w', 0) < 2000, f"pk {pk}: w survived the tombstone: {record}"


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_repair_same_token_missing_keys(manager: ManagerClient):
    """Test that repair syncs two partitions whose keys share one token.

    The logstor primary index and range reader order same-token keys by the full
    decorated key, and repair compares sync boundaries and groups its convergence
    work by the full key as well. A token-only comparison anywhere in that chain
    would order the two keys differently on different replicas, sync the wrong
    record, or treat the two partitions as one. So the colliding keys diverge in
    opposite directions - each replica misses one of them - and repair must end
    with both replicas holding both records, next to plain keys that make the
    colliding run part of a larger scan.

    The second repair, run from the other node as master, asserts the repaired
    range hashes clean: converged same-token records must compare equal on the
    next run, with no rows transferred and no records rewritten. Replicas that
    emitted or hashed the same-token run inconsistently would show up here as
    spurious diffs on every repair.
    """
    servers, cql, hosts = await create_logstor_cluster(manager)

    plain_keys = range(4)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk bigint PRIMARY KEY, v text) WITH storage_engine = 'logstor'")
        table = f"{ks}.test"
        await assert_colliding_tokens(cql, ks)

        await write_only_on(manager, servers, cql, hosts, ks, "test", 0,
                            [f"INSERT INTO {table} (pk, v) VALUES ({COLLIDING_PK1}, 'a') USING TIMESTAMP 1000"] +
                            [f"INSERT INTO {table} (pk, v) VALUES ({pk}, 'v{pk}') USING TIMESTAMP 1000"
                             for pk in plain_keys[:2]])
        await write_only_on(manager, servers, cql, hosts, ks, "test", 1,
                            [f"INSERT INTO {table} (pk, v) VALUES ({COLLIDING_PK2}, 'b') USING TIMESTAMP 1000"] +
                            [f"INSERT INTO {table} (pk, v) VALUES ({pk}, 'v{pk}') USING TIMESTAMP 1000"
                             for pk in plain_keys[2:]])

        # Each node misses the colliding key written to the other one.
        assert await read_record(cql, hosts[1], table, COLLIDING_PK1) is None
        assert await read_record(cql, hosts[0], table, COLLIDING_PK2) is None

        await manager.api.repair(servers[0].ip_addr, ks, "test")

        expected = {COLLIDING_PK1: {'v': 'a'}, COLLIDING_PK2: {'v': 'b'}}
        expected |= {pk: {'v': f'v{pk}'} for pk in plain_keys}
        records = await check_converged(cql, hosts, table, expected)
        assert records[COLLIDING_PK1].marker_timestamp == 1000
        assert records[COLLIDING_PK2].marker_timestamp == 1000

        # A second repair, from the other node as master, must find everything in
        # sync: no slow-path windows, no rows moved, no records rewritten.
        before = await read_all_records(cql, hosts, table)
        log = await manager.server_open_log(servers[1].server_id)
        mark = await log.mark()

        await manager.api.repair(servers[1].ip_addr, ks, "test")

        stats = await read_repair_stats(log, mark)
        assert stats['round_nr_slow_path'] == 0, \
            f"the converged same-token records did not hash equal across the replicas: {stats}"
        assert stats['tx_row_nr'] == 0 and stats['rx_row_nr'] == 0, f"repair transferred rows: {stats}"

        after = await read_all_records(cql, hosts, table)
        for host, old, new in zip(hosts, before, after):
            assert locations(new) == locations(old), f"host {host} rewrote records that were already in sync"


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_repair_same_token_window_boundary(manager: ManagerClient):
    """Test repair sync windows that cut between two keys sharing one token.

    The repair_tiny_max_row_buf_size injection shrinks the repair row buffer
    below a single fragment, so every sync window holds exactly one partition and
    a window boundary necessarily falls between the two colliding keys. Unlike a
    split inside a partition, a cut between two keys at an equal token is legal -
    but only if every step uses the full decorated key: the boundary each replica
    reports (with its position forced to after_all_clustered_rows), the common
    boundary the master picks as the minimum across the replicas, and the split
    of the row buffer at that boundary. Comparing by token alone would put the
    second key on different sides of the boundary on different replicas, or
    converge the two partitions as one.

    Both colliding keys carry a conflict that makes such a mixup visible:

      COLLIDING_PK1  node1: INSERT v='a...' ts=1000  node2: INSERT v='b...' ts=1000
                     -> converges by the equal-timestamp cell tiebreak to 'b...',
                        and must not pick up the other key's tombstone

      COLLIDING_PK2  node1: DELETE ts=2000, nothing else
                     node2: batch INSERT w='stale' ts=1000 + v='new...' ts=3000
                     -> converges to a live row v='new...' still carrying the
                        tombstone that kills w; on node1 its only repair row
                        sits at the partition-start position, the shape that
                        makes the reported boundary position matter

    Every live value is padded above the injected byte budget, so a window that
    read a live row is full once the partition completes. On node2 both colliding
    partitions are live, so each fills a window by itself and node2 reports a
    boundary exactly between the two keys - the common boundary the master picks
    is the minimum across the replicas and windows only advance, so that cut is
    reached whatever node1's windows look like. A run of plain keys diverges
    alongside them so the repair processes many windows; the round count checks
    the padding really kept the windows at one live partition each (only node1's
    tombstone-only record, which no padding can reach, may share a window with
    its ring-order successor).
    """
    servers, cql, hosts = await create_logstor_cluster(manager)

    plain_keys = range(8)
    # repair_tiny_max_row_buf_size caps the row buffer at 100 bytes; a 128-byte
    # value keeps every live row fragment alone above the cap.
    pad = 'x' * 128

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk bigint PRIMARY KEY, v text, w text) WITH storage_engine = 'logstor'")
        table = f"{ks}.test"
        await assert_colliding_tokens(cql, ks)

        await write_only_on(manager, servers, cql, hosts, ks, "test", 0,
                            [f"INSERT INTO {table} (pk, v) VALUES ({COLLIDING_PK1}, 'a{pad}') USING TIMESTAMP 1000",
                             f"DELETE FROM {table} USING TIMESTAMP 2000 WHERE pk = {COLLIDING_PK2}"] +
                            [f"INSERT INTO {table} (pk, v) VALUES ({pk}, 'v{pk}{pad}') USING TIMESTAMP 1000"
                             for pk in plain_keys])
        await write_only_on(manager, servers, cql, hosts, ks, "test", 1,
                            [f"INSERT INTO {table} (pk, v) VALUES ({COLLIDING_PK1}, 'b{pad}') USING TIMESTAMP 1000",
                             f"BEGIN UNLOGGED BATCH "
                             f"INSERT INTO {table} (pk, w) VALUES ({COLLIDING_PK2}, 'stale') USING TIMESTAMP 1000; "
                             f"INSERT INTO {table} (pk, v) VALUES ({COLLIDING_PK2}, 'new{pad}') USING TIMESTAMP 3000; "
                             f"APPLY BATCH"])

        log = await manager.server_open_log(servers[0].server_id)
        mark = await log.mark()

        async with AsyncExitStack() as stack:
            for server in servers:
                await stack.enter_async_context(inject_error(manager.api, server.ip_addr, "repair_tiny_max_row_buf_size"))
            await manager.api.repair(servers[0].ip_addr, ks, "test")

        # The master holds one live padded partition per plain key plus
        # COLLIDING_PK1, each needing a window of its own; only its tombstone-only
        # COLLIDING_PK2 record may share a window. A common boundary can cut a
        # window shorter but never merge two, so this lower bound is exact enough
        # to catch the padding failing to keep windows at one partition.
        stats = await read_repair_stats(log, mark)
        assert stats['round_nr'] >= len(plain_keys) + 1, f"a sync window held more than one live partition: {stats}"

        expected = {COLLIDING_PK1: {'v': f'b{pad}'}, COLLIDING_PK2: LIVE}
        expected |= {pk: {'v': f'v{pk}{pad}'} for pk in plain_keys}
        records = await check_converged(cql, hosts, table, expected)

        # The equal-timestamp conflict converges by the cell tiebreak (largest
        # value wins) with the original timestamp, and must not have picked up
        # the tombstone of the other key at the same token.
        assert records[COLLIDING_PK1].cell_timestamps() == {'v': 1000}
        assert records[COLLIDING_PK1].partition_tombstone is None, \
            f"the tombstone of the key sharing the token leaked into this record: {records[COLLIDING_PK1]}"

        # The deleted-then-rewritten key must keep the newer row and the
        # tombstone next to it, and w must not have survived the deletion.
        record = records[COLLIDING_PK2]
        assert record.tombstone_timestamp() == 2000, f"tombstone lost from the converged record: {record}"
        assert record.cell_values()['v'] == f'new{pad}', f"{record}"
        assert record.cell_timestamps().get('w', 0) < 2000, f"w survived the tombstone: {record}"


async def test_repair_consistent_nodes_transfers_nothing(manager: ManagerClient):
    """Test that repair of already consistent replicas finds them equal and applies nothing.

    This is the case every periodic repair of a healthy cluster hits, and for
    logstor it is the case that has to stay cheap: a repair that failed to see two
    identical partitions as identical would converge and rewrite every record in
    the range, and one that skipped the comparison altogether would silently stop
    repairing.

    So the test asserts all three properties, from three independent angles:

      - repair really compared the data and found it equal: every sync window took
        one of the two fast paths and none took the slow path that fetches full row
        hashes and rows;
      - nothing moved between the nodes: no rows were sent or received, while rows
        were in fact read from disk and hashed, which is what makes the previous
        assertion meaningful;
      - nothing was applied: every record is still at the log location it had
        before the repair.
    """
    servers, cql, hosts = await create_logstor_cluster(manager)

    keys = range(32)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v text) WITH storage_engine = 'logstor'")
        table = f"{ks}.test"

        # No write is suppressed, so both replicas store the same records.
        for pk in keys:
            await cql.run_async(f"INSERT INTO {table} (pk, v) VALUES ({pk}, 'v{pk}') USING TIMESTAMP 1000")

        before = await read_all_records(cql, hosts, table)
        reference = check_records_converged(hosts, before)
        assert set(reference) == set(keys)

        metrics_before = [await manager.metrics.query(server.ip_addr) for server in servers]
        log = await manager.server_open_log(servers[0].server_id)
        mark = await log.mark()

        await manager.api.repair(servers[0].ip_addr, ks, "test")

        stats = await read_repair_stats(log, mark)
        assert stats['round_nr'] > 0, f"repair did no rounds at all: {stats}"
        assert stats['round_nr_slow_path'] == 0, \
            f"repair failed to see the identical replicas as in sync and took the slow path: {stats}"
        assert stats['round_nr_fast_path_already_synced'] + stats['round_nr_fast_path_same_combined_hashes'] > 0, \
            f"no sync window was resolved by comparing hashes: {stats}"
        assert stats['tx_row_nr'] == 0 and stats['rx_row_nr'] == 0, f"repair transferred rows: {stats}"

        metrics_after = [await manager.metrics.query(server.ip_addr) for server in servers]
        for server, m_before, m_after in zip(servers, metrics_before, metrics_after):
            assert metric_delta(m_before, m_after, 'scylla_repair_tx_row_nr') == 0, f"{server.ip_addr} sent rows"
            assert metric_delta(m_before, m_after, 'scylla_repair_rx_row_nr') == 0, f"{server.ip_addr} received rows"
            # Both replicas read and hashed their own copy of every partition, so
            # the comparison above was made on the real data.
            assert metric_delta(m_before, m_after, 'scylla_repair_row_from_disk_nr') >= len(keys), \
                f"{server.ip_addr} did not read the rows it compared"

        after = await read_all_records(cql, hosts, table)
        for host, old, new in zip(hosts, before, after):
            assert locations(new) == locations(old), f"host {host} rewrote records that were already in sync"
        assert check_records_converged(hosts, after) == reference


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_repair_does_not_rewrite_clean_partitions(manager: ManagerClient):
    """Test that repair rewrites only the logstor records of partitions that differ.

    Logstor repair converges and flushes whole partitions, but only the ones a
    peer disagreed about: flush_rows() skips rows that are not dirty, and a
    follower only applies what it was sent. A repair that rewrote clean
    partitions would be correct but would turn every repair into a full rewrite
    of the range.

    The record's log location, visible in the MUTATION_FRAGMENTS() source name,
    changes if and only if the record was rewritten, which makes this observable:
    repair a synced range and no location may move, diverge one key and only that
    key's record may move.
    """
    servers, cql, hosts = await create_logstor_cluster(manager)

    keys = range(8)
    diverged = 0

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v text) WITH storage_engine = 'logstor'")
        table = f"{ks}.test"

        # No write is suppressed, so both replicas store the identical record.
        for pk in keys:
            await cql.run_async(f"INSERT INTO {table} (pk, v) VALUES ({pk}, 'v{pk}') USING TIMESTAMP 1000")

        before = await read_all_records(cql, hosts, table)
        for records in before:
            assert set(records) == set(keys)

        await manager.api.repair(servers[0].ip_addr, ks, "test")

        after = await read_all_records(cql, hosts, table)
        for host, old, new in zip(hosts, before, after):
            assert locations(new) == locations(old), f"host {host} rewrote records while everything was in sync"

        # Diverge a single key on the repair master.
        await write_only_on(manager, servers, cql, hosts, ks, "test", 0,
                            [f"INSERT INTO {table} (pk, v) VALUES ({diverged}, 'new') USING TIMESTAMP 2000"])

        before = await read_all_records(cql, hosts, table)
        await manager.api.repair(servers[0].ip_addr, ks, "test")
        after = await read_all_records(cql, hosts, table)

        for host, old, new in zip(hosts, before, after):
            for pk in keys:
                if pk == diverged:
                    continue
                assert new[pk].location == old[pk].location, f"host {host} rewrote clean pk {pk}"

        # The follower had the stale version, so its record must have been replaced.
        # Nothing is asserted about the master's own record: it holds the winning
        # version, but it also merges and reflushes the partition it pulled a
        # differing version for.
        assert after[1][diverged].location != before[1][diverged].location, \
            "the follower's stale record was not replaced"

        await check_converged(cql, hosts, table,
                              {pk: {'v': 'new' if pk == diverged else f'v{pk}'} for pk in keys})

        # Everything is in sync again, so a second repair rewrites nothing.
        before = after
        await manager.api.repair(servers[0].ip_addr, ks, "test")
        after = await read_all_records(cql, hosts, table)
        for host, old, new in zip(hosts, before, after):
            assert locations(new) == locations(old), f"host {host} rewrote records in a repair with nothing to do"


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_repair_overwrites_stale_cached_partition(manager: ManagerClient):
    """Test that repair does not leave a node serving a stale cached partition.

    Logstor answers reads from a cache keyed by primary index entry, and
    overwriting a record evicts the cached mutation (primary_index::insert).
    Repair applies through the same table write path, so the same eviction has to
    happen there: otherwise a node whose partition was read (and thus cached)
    before repair keeps serving the pre-repair value even though its record on
    disk was replaced.

    Node2 holds v=100 and has it cached, node1 holds the newer v=200. After
    repair, node2's record and its cached copy must both be the new value.
    """
    servers, cql, hosts = await create_logstor_cluster(manager)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v int)"
                            " WITH storage_engine = 'logstor' AND speculative_retry = 'NONE'")
        table = f"{ks}.test"

        await cql.run_async(f"INSERT INTO {table} (pk, v) VALUES (1, 100) USING TIMESTAMP 1000")

        # Write the newer value on node1 only.
        await write_only_on(manager, servers, cql, hosts, ks, "test", 0,
                            [f"INSERT INTO {table} (pk, v) VALUES (1, 200) USING TIMESTAMP 2000"])

        # Populate node2's logstor cache with its stale version. read_locally()
        # checks the read really was answered by node2: served from node1 it
        # would leave node2's cache empty and the test would prove nothing.
        rows = await read_locally(cql, hosts[1], f"SELECT v FROM {table} WHERE pk = 1")
        assert rows[0].v == 100
        record = await read_record(cql, hosts[1], table, 1)
        assert record is not None and record.cached is not None, \
            f"the read did not populate the logstor cache: {record}"

        await manager.api.repair(servers[0].ip_addr, ks, "test")

        # check_converged() asserts that a cached copy, if the overwrite left one
        # behind, agrees with the stored record.
        records = await check_converged(cql, hosts, table, {1: {'v': '200'}})
        assert records[1].cell_timestamps() == {'v': 2000}

        # And node2 must serve the new value out of its own storage.
        rows = await read_locally(cql, hosts[1], f"SELECT v FROM {table} WHERE pk = 1")
        assert rows[0].v == 200, "node2 served a stale cached value after repair"


#############################################################################
# Read repair on logstor tables.
#
# Read repair is the coordinator-side reconciliation that runs when a digest read
# detects that the replicas disagree: the coordinator reads the full data from all
# the replicas, merges the versions, and writes back to each divergent replica
# before returning the reconciled result to the client.
#
# For logstor tables the write-back must carry the full reconciled partition
# rather than the usual cell-level diff, because a logstor write replaces the
# replica's whole partition record. See the "Master-side whole-partition
# convergence" section of docs/dev/row_level_repair.md for the same reasoning
# applied to background repair.
#
# These tests trigger read repair with a CL=ALL read of divergent replicas and
# then assert, with MUTATION_FRAGMENTS() reads of each node's storage, that the
# replicas converged on the reconciled records.
#############################################################################

FOREGROUND_READ_REPAIRS = 'scylla_storage_proxy_coordinator_foreground_read_repairs'


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_read_repair_converges_divergent_replicas(manager: ManagerClient):
    """Test that a reconciling read converges divergent logstor replicas.

    Node2 misses three writes: a new partition, a newer value for an existing
    partition, and a partition delete. A CL=ALL read of each partition detects
    the digest mismatch and read-repairs it, so afterwards both nodes must store
    the reconciled record.
    """
    servers, cql, hosts = await create_logstor_cluster(manager)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v text) WITH storage_engine = 'logstor'")
        table = f"{ks}.test"

        # No write is suppressed, so both replicas store pk 2 and pk 3.
        await cql.run_async(SimpleStatement(f"INSERT INTO {table} (pk, v) VALUES (2, 'old') USING TIMESTAMP 1000", consistency_level=ConsistencyLevel.ALL))
        await cql.run_async(SimpleStatement(f"INSERT INTO {table} (pk, v) VALUES (3, 'c') USING TIMESTAMP 1000", consistency_level=ConsistencyLevel.ALL))

        await write_only_on(manager, servers, cql, hosts, ks, "test", 0, [
            f"INSERT INTO {table} (pk, v) VALUES (1, 'a') USING TIMESTAMP 2000",
            f"INSERT INTO {table} (pk, v) VALUES (2, 'new') USING TIMESTAMP 2000",
            f"DELETE FROM {table} USING TIMESTAMP 2000 WHERE pk = 3"])

        # The stored records still show the divergence the test set up.
        before = await read_all_records(cql, hosts, table)
        assert set(before[0]) == {1, 2, 3} and set(before[1]) == {2, 3}
        assert before[0][2].cell_values() == {'v': 'new'}
        assert before[1][2].cell_values() == {'v': 'old'}
        assert before[0][3].tombstone_timestamp() is not None
        assert before[1][3].tombstone_timestamp() is None

        metrics_before = await manager.metrics.query(servers[0].ip_addr)

        # Each read goes to both replicas, sees the mismatch and repairs it
        # before returning the reconciled result.
        rows = await cql.run_async(SimpleStatement(f"SELECT pk, v FROM {table} WHERE pk = 1", consistency_level=ConsistencyLevel.ALL), host=hosts[0])
        assert [(row.pk, row.v) for row in rows] == [(1, 'a')]
        rows = await cql.run_async(SimpleStatement(f"SELECT pk, v FROM {table} WHERE pk = 2", consistency_level=ConsistencyLevel.ALL), host=hosts[0])
        assert [(row.pk, row.v) for row in rows] == [(2, 'new')]
        rows = await cql.run_async(SimpleStatement(f"SELECT pk, v FROM {table} WHERE pk = 3", consistency_level=ConsistencyLevel.ALL), host=hosts[0])
        assert rows == []

        metrics_after = await manager.metrics.query(servers[0].ip_addr)
        assert metric_delta(metrics_before, metrics_after, FOREGROUND_READ_REPAIRS) >= 3, \
            "the reads did not go through foreground read repair"

        records = await check_converged(cql, hosts, table,
                                       {1: {'v': 'a'}, 2: {'v': 'new'}, 3: Deleted(2000)})
        assert records[1].marker_timestamp == 2000
        assert records[2].marker_timestamp == 2000

        # For pk 1 and pk 2 the reconciled partition equals what node1 already
        # stores, so read repair must not have written to node1 at all.
        after_node1 = await read_records(cql, hosts[0], table)
        for pk in (1, 2):
            assert locations(after_node1)[pk] == locations(before[0])[pk], \
                f"read repair rewrote node1's already-reconciled record for pk {pk}"


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_read_repair_same_timestamp_conflict(manager: ManagerClient):
    """Test read repair of two values with the same timestamp on a logstor table.

    Node1 has value 'a', node2 has value 'b', both at timestamp 1000. The
    reconciled value is 'b' (largest value wins the cell tiebreaker at equal
    timestamp), and the coordinator's diff for node1 is that cell alone -
    without a row marker, since the markers tie. A logstor replica cannot apply
    such a partial diff: its write path replaces the whole partition record and
    rejects a mutation with no row marker and no partition tombstone. Read
    repair therefore has to send the full reconciled partition, and this test
    fails with a read error if it does not.
    """
    servers, cql, hosts = await create_logstor_cluster(manager)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v text) WITH storage_engine = 'logstor'")
        table = f"{ks}.test"

        await write_only_on(manager, servers, cql, hosts, ks, "test", 0,
                            [f"INSERT INTO {table} (pk, v) VALUES (1, 'a') USING TIMESTAMP 1000"])
        await write_only_on(manager, servers, cql, hosts, ks, "test", 1,
                            [f"INSERT INTO {table} (pk, v) VALUES (1, 'b') USING TIMESTAMP 1000"])

        before = await read_all_records(cql, hosts, table)
        assert before[0][1].cell_values() == {'v': 'a'}
        assert before[1][1].cell_values() == {'v': 'b'}

        rows = await cql.run_async(SimpleStatement(f"SELECT pk, v FROM {table} WHERE pk = 1", consistency_level=ConsistencyLevel.ALL), host=hosts[0])
        assert [(row.pk, row.v) for row in rows] == [(1, 'b')]

        records = await check_converged(cql, hosts, table, {1: {'v': 'b'}})
        assert records[1].marker_timestamp == 1000
        assert records[1].partition_tombstone is None

        # Node2 already stored the winning value, so read repair must have
        # written only to node1, leaving node2's record where it was.
        after_node2 = await read_records(cql, hosts[1], table)
        assert locations(after_node2)[1] == locations(before[1])[1], \
            "read repair rewrote node2's record although it already held the reconciled value"


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_read_repair_scan_leaves_partitions_beyond_the_page(manager: ManagerClient):
    """Test that a scan does not read-repair a partition its page did not reach.

    Every replica fills a scan page up to the row limit on its own, so a replica
    that holds more live partitions than the others stops at a lower key. The
    reconciliation still sees the partitions the other replicas reported past
    that key, and the replica that stopped short reports no version for them -
    which at the coordinator is indistinguishable from not holding them.

    Sending such a replica the whole reconciled partition would replace a record
    the reconciliation never saw. Here node1's page ends at `mid`, which only
    node1 holds, so it reports nothing for `high` although its own record for
    `high` carries the only copy of that partition's v2 cell.
    """
    servers, cql, hosts = await create_logstor_cluster(manager)

    # A single tablet, so the whole ring is scanned as one range read whose row
    # limit is shared by the three partitions.
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2} "
                                          "AND tablets = {'initial': 1}") as ks:
        low, mid, high = await token_order(cql, ks, [1, 2, 3])

        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v1 text, v2 text) WITH storage_engine = 'logstor'")
        table = f"{ks}.test"

        # Both replicas store `low` and the first version of `high`.
        await cql.run_async(SimpleStatement(f"INSERT INTO {table} (pk, v1, v2) VALUES ({low}, 'x', 'x2') USING TIMESTAMP 1000",
                                            consistency_level=ConsistencyLevel.ALL))
        await cql.run_async(SimpleStatement(f"INSERT INTO {table} (pk, v1, v2) VALUES ({high}, 'old', 'keep') USING TIMESTAMP 1000",
                                            consistency_level=ConsistencyLevel.ALL))
        # Only node2 gets the newer version of `high`. A logstor write replaces
        # the whole record, so node2's copy has no v2 at all and node1's older
        # record is the only place v2 is stored.
        await write_only_on(manager, servers, cql, hosts, ks, "test", 1,
                            [f"INSERT INTO {table} (pk, v1) VALUES ({high}, 'new') USING TIMESTAMP 2000"])
        # Only node1 gets `mid`, so below `high` node1 has one live partition
        # more than node2.
        await write_only_on(manager, servers, cql, hosts, ks, "test", 0,
                            [f"INSERT INTO {table} (pk, v1) VALUES ({mid}, 'm') USING TIMESTAMP 1000"])

        before = await read_all_records(cql, hosts, table)
        assert set(before[0]) == {low, mid, high} and set(before[1]) == {low, high}
        assert before[0][high].cell_values() == {'v1': 'old', 'v2': 'keep'}
        assert before[1][high].cell_values() == {'v1': 'new'}

        metrics_before = await manager.metrics.query(servers[0].ip_addr)

        # A page of two rows: node1 fills it with `low` and `mid` and stops
        # before `high`, node2 fills it with `low` and `high`. The digests
        # differ, so the page is read-repaired before it is returned.
        statement = SimpleStatement(f"SELECT pk, v1, v2 FROM {table}", consistency_level=ConsistencyLevel.ALL, fetch_size=2)
        page = await cql.run_async(statement, all_pages=False, host=hosts[0])
        assert [row.pk for row in page] == [low, mid]

        metrics_after = await manager.metrics.query(servers[0].ip_addr)
        assert metric_delta(metrics_before, metrics_after, FOREGROUND_READ_REPAIRS) >= 1, \
            "the page did not go through foreground read repair"

        after = await read_all_records(cql, hosts, table)
        # `mid` is the partition node2 really is missing, and repairing it is
        # what the page is supposed to do.
        assert after[1][mid].cell_values() == {'v1': 'm'}
        # `high` is past node1's part of the page, so node1's record for it must
        # be left alone: replacing it drops the only copy of v2.
        assert after[0][high].cell_values() == {'v1': 'old', 'v2': 'keep'}, \
            "read repair overwrote node1's record for a partition its page never reached"
        assert locations(after[0])[high] == locations(before[0])[high], \
            "read repair rewrote node1's record for a partition its page never reached"

        # The rest of the scan does reach `high`, where both replicas report
        # their version, and converges them on the merged record.
        rows = await cql.run_async(statement, all_pages=True, host=hosts[0])
        assert [(row.pk, row.v1, row.v2) for row in rows] == [(low, 'x', 'x2'), (mid, 'm', None), (high, 'new', 'keep')]

        records = await check_converged(cql, hosts, table, {
            low: {'v1': 'x', 'v2': 'x2'},
            mid: {'v1': 'm'},
            high: {'v1': 'new', 'v2': 'keep'},
        })
        assert records[high].marker_timestamp == 2000
