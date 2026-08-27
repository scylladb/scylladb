#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
#############################################################################
# Multi-node and restart coverage for the `storage_format` table property.
# See docs/dev/parquet-storage-format.md, §9.6a and §11.1.
#
# WHY THIS FILE EXISTS SEPARATELY FROM test/cqlpy/test_parquet_storage_format.py.
# That file is the CQL-level suite and it is thorough, but `test/cqlpy` runs
# against a single node that it cannot restart. Two classes of claim are
# therefore unreachable from it, and both are places where a bug would be
# invisible on a single-node test:
#
#   1. A `pq` sstable written before a restart must still be readable after
#      it, and the table's `storage_format` must survive schema reload. A
#      footer, a metadata cache or a schema-column round trip that dropped
#      the format would look perfectly healthy until the node came back.
#
#   2. With RF > 1 the format decision has to hold on *every* replica and on
#      every path that writes an sstable -- flush, streaming, repair. It is
#      decided per-node from the local schema, so "the coordinator wrote
#      Parquet" says nothing about the other two.
#
# THREE TRAPS THAT MAKE TESTS IN THIS AREA SILENTLY VACUOUS.
#
# 1. "Only one node ever wrote." A convergence test that flushes and then
#    asserts "every sstable I found is pq" passes trivially if two of the
#    three replicas hold no sstables at all -- an empty set satisfies every
#    universally-quantified claim. Every assertion below therefore goes
#    through assert_format_on_every_replica(), which first checks how many
#    nodes hold *any* sstable and fails as a setup problem if too few do.
#
# 2. "The receiving node flushed instead of streaming." The streaming and
#    repair tests below must not flush the receiving node before asserting.
#    `make_streaming_consumer` writes an sstable directly, so if the format
#    came from streaming the file is already there; but if the data had
#    arrived as mutations into a memtable instead, a flush would then produce
#    a `pq` file via the *flush* path and the test would pass without the
#    streaming path ever being consulted. Not flushing is what makes these
#    tests streaming tests. For the same reason hinted handoff is disabled in
#    the repair test: a hint replay is a memtable write, and it would supply
#    the rows that repair was supposed to supply.
#
# 3. RF changes tombstone behaviour. A table created without an explicit
#    `tombstone_gc` gets mode `repair`, and at RF=1 that short-circuits
#    `gc_before` to *now*, so every dead cell is purgeable the moment it is
#    written and `gc_grace_seconds` is never consulted. At RF=3 they are
#    retained. The restart test below keeps a tombstone across the restart
#    and runs at RF=1, so it sets `tombstone_gc = {'mode': 'timeout'}`
#    explicitly; without it the deletion could legitimately vanish and the
#    assertion would be testing nothing.
#
# A fourth, inherited from the cqlpy file: a read served from the memtable or
# the row cache never touches the sstable. Every read below that is meant to
# prove something about Parquet uses BYPASS CACHE.
#############################################################################

import asyncio
import glob
import logging
import os
import time

import pytest
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from test.cluster.util import new_test_keyspace
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for

logger = logging.getLogger(__name__)


# See trap 3. Only needed where a test keeps a tombstone.
TOMBSTONE_GC = "tombstone_gc = {'mode': 'timeout'}"

TWCS = ("compaction = {'class': "
        "'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy'}")
ICS = "compaction = {'class': 'IncrementalCompactionStrategy'}"

RF3 = "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}"
RF1 = "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"


###########################################################################
# Reading the format off disk.
#
# Deliberately a re-read of the filesystem rather than the
# `column_family/storage_format/{name}` REST endpoint: the endpoint reports
# what the node *believes* it wrote, and a format bug that also confused the
# accounting would agree with itself. The filenames are the ground truth.
#
# These mirror test/cqlpy/test_parquet_storage_format.py's helpers, with two
# differences forced by the cluster harness: the path acquires a `data`
# element because it starts from a server workdir rather than a data
# directory, and an *empty* result is a legitimate answer here (a replica may
# hold none of the data) so sstable_versions() does not assert non-empty.
# Callers that need sstables to exist say so, and say it as a setup failure.
###########################################################################

def table_dir(workdir, table):
    """The on-disk directory of `table` ("keyspace.name") on one node.

    The 32-char UUID glob is not decoration: without it, `t-*` would also
    match a `t_other-...` directory, and silently globbing two tables would
    make the format assertions depend on an unrelated one.
    """
    keyspace, name = table.split('.')
    pattern = os.path.join(workdir, "data", keyspace, f"{name}-" + "?" * 32)
    dirs = glob.glob(pattern)
    assert len(dirs) == 1, \
        f"expected exactly one data directory matching {pattern}, found {dirs}"
    return dirs[0]


def data_components(workdir, table):
    """Basenames of the sealed Data components of `table` on one node.

    Empty is a legitimate answer, so this does not assert. Anything under
    snapshots/, staging/ or upload/ is excluded by construction: the glob does
    not recurse. An sstable whose TOC is still a .tmp is mid-write, and
    counting it would make the assertions racy.
    """
    d = table_dir(workdir, table)
    names = [os.path.basename(p) for p in glob.glob(os.path.join(d, "*-Data.db"))]

    def sealed(name):
        prefix = "-".join(name.split("-")[:-1])
        return not os.path.exists(os.path.join(d, prefix + "-TOC.txt.tmp"))

    return sorted(n for n in names if sealed(n))


def sstable_versions(workdir, table):
    """The set of sstable version prefixes present for `table` on one node.

    Filenames are `<version>-<generation>-<format>-<component>`, so
    `pq-3-big-Data.db` is Parquet and `me-3-big-Data.db` is the native format.
    """
    return {n.split("-", 1)[0] for n in data_components(workdir, table)}


async def workdirs_of(manager, servers):
    return await asyncio.gather(
        *[manager.server_get_workdir(s.server_id) for s in servers])


def versions_by_node(workdirs, table):
    return {wd: sstable_versions(wd, table) for wd in workdirs}


def assert_format_on_every_replica(workdirs, table, expect_parquet, min_holders):
    """The load-bearing assertion of this file.

    `min_holders` is trap 1 made explicit. It is a *setup* precondition, not a
    product claim: if fewer than `min_holders` nodes hold any sstable at all
    then the flush or the stream did not happen where we thought it did, and
    the format claim below would be vacuously true. It is asserted first and
    worded as staleness so a failure reads as "the test stopped exercising the
    thing" rather than "the product regressed".
    """
    per_node = versions_by_node(workdirs, table)
    holders = {wd: v for wd, v in per_node.items() if v}
    assert len(holders) >= min_holders, (
        f"SETUP WENT STALE: expected at least {min_holders} of {len(workdirs)} "
        f"nodes to hold sstables for {table}, but only {len(holders)} do. "
        f"The format assertion that follows would be vacuous. Per node: {per_node}")

    if expect_parquet:
        bad = {wd: v for wd, v in holders.items() if v != {"pq"}}
        assert not bad, (
            f"expected only Parquet sstables for {table} on every replica; "
            f"these nodes have other formats: {bad}. Per node: {per_node}")
    else:
        bad = {wd: v for wd, v in holders.items() if "pq" in v}
        assert not bad, (
            f"expected no Parquet sstables for {table} on any replica; "
            f"these nodes have some: {bad}. Per node: {per_node}")


def scylla_tables_storage_format(cql, table):
    """The persisted value of the property, or None if the table never set it."""
    keyspace, name = table.split('.')
    return cql.execute(
        "SELECT storage_format FROM system_schema.scylla_tables "
        f"WHERE keyspace_name = '{keyspace}' AND table_name = '{name}'"
    ).one().storage_format


def describe(cql, table):
    return cql.execute(f"DESCRIBE TABLE {table}").one().create_statement


async def insert_rows(cql, table, keys, consistency=ConsistencyLevel.ALL):
    """Rows wide enough to be worth encoding, at a consistency level that
    forces every replica to take the write.

    `pk` and `ck` are `bigint` on purpose: those are the columns
    schema_mapping.cc delta-encodes, and they are what the bit-packing
    data-loss bug of §9.6b corrupted. A test that only used `int` keys would
    not have noticed it.
    """
    stmt = cql.prepare(
        f"INSERT INTO {table} (pk, ck, v) VALUES (?, ?, ?)")
    stmt.consistency_level = consistency
    await asyncio.gather(*[
        cql.run_async(stmt, (k, k * 1000, f"value-{k}")) for k in keys])


def expected_rows(keys):
    return sorted((k, k * 1000, f"value-{k}") for k in keys)


async def read_all(cql, table, consistency=ConsistencyLevel.ALL, host=None):
    """Read every row from the sstables.

    BYPASS CACHE is what makes this a Parquet read rather than a row-cache
    read. At CL=ALL it is also the strongest available cross-replica check:
    every replica has to answer from its own files, so a replica whose `pq`
    file were unreadable would fail the query rather than being covered for
    by the other two.
    """
    stmt = SimpleStatement(
        f"SELECT pk, ck, v FROM {table} BYPASS CACHE",
        consistency_level=consistency)
    rows = await cql.run_async(stmt, host=host)
    return sorted((r.pk, r.ck, r.v) for r in rows)


###########################################################################
# 1. Restart survival.
###########################################################################

async def test_parquet_survives_restart(manager: ScyllaClusterManager) -> None:
    """A `pq` sstable written before a restart is still readable after it, the
    files are still `pq`, and the table is still declared `parquet`.

    Three distinct claims, and they fail independently:

      * the *data* survives -- the footer, the page index and the deletion
        channel all have to be re-read from a file this process did not write;
      * the *files* are untouched -- asserted by name, so a node that quietly
        rewrote them as native on load would be caught even though the rows
        would still read back correctly;
      * the *property* survives schema reload -- both in
        `system_schema.scylla_tables` and, separately, in the reloaded schema
        object, which is what `DESCRIBE` renders and what the write path asks.

    The last one is why the test flushes again at the end. The schema row and
    the in-memory schema are two different things: a build that persisted
    `storage_format` but dropped it when reading the row back would keep
    answering `'parquet'` from `scylla_tables` while writing native sstables
    for the rest of the node's life.
    """
    server = await manager.server_add()
    workdir = await manager.server_get_workdir(server.server_id)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, RF1) as ks:
        table = f"{ks}.t"
        await cql.run_async(
            f"CREATE TABLE {table} (pk bigint, ck bigint, v text, "
            f"PRIMARY KEY (pk, ck)) "
            f"WITH storage_format = 'parquet' AND {TOMBSTONE_GC}")

        # Four flushed sstables is exactly the STCS compaction threshold, so
        # without this the four would be merged back into one by background
        # compaction -- undoing the staging below and reintroducing the very
        # trap it exists to avoid. Disabling autocompaction is not persisted
        # across a restart, so the boot path still gets to reshape.
        await manager.api.disable_autocompaction(server.ip_addr, ks, "t")

        # Four separate flushes rather than one, for two reasons that survive
        # scrutiny. First, the post-restart read then has to *merge* several
        # `pq` files written by the previous process instead of reading one,
        # which is a strictly harder thing to get right. Second, four is the
        # STCS compaction threshold, so the restart is followed by a real
        # compaction of pre-restart Parquet files, whose output the assertions
        # below also cover.
        #
        # It was originally four in order to give reshape-on-load something to
        # do. That reasoning turned out to be wrong -- see the note after the
        # restart -- but the staging is worth keeping on the two grounds above.
        keys = list(range(200))
        batches = [keys[i::4] for i in range(4)]
        for batch in batches:
            await insert_rows(cql, table, batch, ConsistencyLevel.ONE)
            await manager.api.keyspace_flush(server.ip_addr, ks, "t")

        # A deletion, kept alive by TOMBSTONE_GC (trap 3). This is the part of
        # the file most recently reworked -- the folded deletion channel of
        # §10.28 -- and a restart is the only way to prove it round-trips
        # through a footer written by a previous process.
        deleted = keys[0]
        await cql.run_async(f"DELETE FROM {table} WHERE pk = {deleted}")
        live = [k for k in keys if k != deleted]

        await manager.api.keyspace_flush(server.ip_addr, ks, "t")

        # Preconditions. Without these the restart proves nothing: if nothing
        # was flushed, everything below reads out of the commitlog replay.
        before = data_components(workdir, table)
        assert before, (
            f"SETUP WENT STALE: nothing was flushed for {table}, so the "
            f"restart below would exercise commitlog replay rather than a "
            f"Parquet file")
        assert len(before) >= 4, (
            f"SETUP WENT STALE: only {len(before)} sstables were staged "
            f"({before}); the post-restart read is then a single-file read "
            f"rather than a merge, and no post-restart compaction of "
            f"pre-restart Parquet files will happen")
        assert sstable_versions(workdir, table) == {"pq"}, (
            f"SETUP WENT STALE: expected the pre-restart flush to write "
            f"Parquet, got {sstable_versions(workdir, table)}")
        assert await read_all(cql, table, ConsistencyLevel.ONE) == expected_rows(live)

        logger.info(f"Restarting {server}; {len(before)} pq sstable(s) on disk")
        await manager.server_restart(server.server_id)
        cql, _ = await manager.get_ready_cql([server])

        # Every sstable is still Parquet. Deliberately *not* asserted by
        # filename: autocompaction is re-enabled by the restart (the disable
        # above is not persisted), so the staged sstables are merged by
        # ordinary compaction shortly after boot -- measured here as 17 files
        # becoming 4. Whether this runs before or after the assertion is a
        # race, so the assertion is written to hold either way: the staged
        # files are `pq` and the compacted output is `pq` too.
        #
        # Note what this does *not* reach, because it looked like it did.
        # Reshape-on-load -- the boot-time write path where the
        # `distributed_loader.cc` format defect lived -- does not run here at
        # all: `reshape_mode::relaxed` only rewrites sstables that violate the
        # strategy's layout goal, and normally-flushed ones do not. Confirmed
        # twice: no "Reshap" line appears in the node log across the restart,
        # and a build with `version_for_rewrite_on_load()` forced to the
        # native version passes this test unchanged. That path is covered by
        # `test_reshape_on_load_writes_parquet_for_hybrid_twcs` and
        # `test_storage_format_honoured_by_refresh_reshape` in test/boost,
        # which stage sstables that do need reshaping.
        after = data_components(workdir, table)
        assert sstable_versions(workdir, table) == {"pq"}, (
            f"the sstables are no longer Parquet after the restart: "
            f"{sstable_versions(workdir, table)} (files: {after})")

        # The data, out of the file written by the previous process.
        assert await read_all(cql, table, ConsistencyLevel.ONE) == expected_rows(live)
        # And the tombstone specifically: still dead, not resurrected.
        gone = await cql.run_async(
            SimpleStatement(f"SELECT pk FROM {table} WHERE pk = {deleted} BYPASS CACHE",
                            consistency_level=ConsistencyLevel.ONE))
        assert list(gone) == [], \
            f"the deleted partition {deleted} came back after the restart"

        # The property, in the schema table and in the reloaded schema object.
        assert scylla_tables_storage_format(cql, table) == 'parquet'
        assert "storage_format = 'parquet'" in describe(cql, table), \
            f"DESCRIBE lost storage_format after the restart: {describe(cql, table)}"

        # And it still drives the write path. This is the assertion that
        # separates "the row is still in scylla_tables" from "the reloaded
        # schema still means it".
        await insert_rows(cql, table, range(200, 300), ConsistencyLevel.ONE)
        await manager.api.keyspace_flush(server.ip_addr, ks, "t")
        grew = data_components(workdir, table)
        # A *new filename*, not a larger count: background compaction can
        # merge files away between the two readings, so counting would flake
        # even though the flush happened. Any file the flush produced is a
        # name that was not there before, and if compaction immediately
        # merges it the merged output is also a new name.
        assert set(grew) - set(after), (
            f"SETUP WENT STALE: the post-restart flush added no sstable "
            f"(was={after}, now={grew}), so the format assertion that "
            f"follows would be about the pre-restart files")
        assert sstable_versions(workdir, table) == {"pq"}, (
            f"a flush after the restart wrote a non-Parquet sstable: "
            f"{sstable_versions(workdir, table)} -- the reloaded schema lost "
            f"storage_format even though scylla_tables still has it")


###########################################################################
# 2. Convergence across replicas at RF=3.
###########################################################################

# `expect_parquet` is the whole point of the matrix. Without the two False
# rows a build in which *everything* flushed as `pq` would pass, and without
# the hybrid rows the TWCS short-circuit inside
# `writes_parquet_unconditionally()` would be untested on a cluster -- it is
# statically true there, so it is the one hybrid case with a definite answer.
CONVERGENCE_MATRIX = [
    ('parquet', '',    True),
    ('hybrid',  TWCS,  True),
    ('hybrid',  ICS,   False),
    ('sstable', '',    False),
]


@pytest.mark.parametrize("fmt,extra,expect_parquet", CONVERGENCE_MATRIX)
async def test_storage_format_converges_on_every_replica(
        manager: ScyllaClusterManager, fmt, extra, expect_parquet) -> None:
    """Every replica of an RF=3 table independently agrees on the format.

    The decision is made per-node from the local schema, so this is not
    implied by the single-node flush tests: three nodes reading the same
    schema could still disagree if any of them consulted something local, and
    a table whose replicas disagreed would never converge -- each node's
    compactions would keep undoing the others'.

    `hybrid` + ICS is the discriminating case in the other direction: hybrid
    means Parquet only where the strategy has no rewritten levels to protect,
    so under ICS a *flush* must stay native. Only the flush is asserted --
    which format a later compaction picks for a hybrid table is a
    data-dependent policy decision that weighs a measured size gain, and
    pinning it here would be pinning the policy, not the format plumbing.
    """
    servers = await manager.servers_add(3, auto_rack_dc="dc1")
    cql, hosts = await manager.get_ready_cql(servers)
    workdirs = await workdirs_of(manager, servers)

    async with new_test_keyspace(manager, RF3) as ks:
        table = f"{ks}.t"
        props = f"storage_format = '{fmt}'"
        if extra:
            props += f" AND {extra}"
        await cql.run_async(
            f"CREATE TABLE {table} (pk bigint, ck bigint, v text, "
            f"PRIMARY KEY (pk, ck)) WITH {props}")

        keys = list(range(300))
        # CL=ALL: every replica must take the write, so every replica has
        # something to flush. Trap 1 in its most likely form is a write that
        # only reached the coordinator.
        await insert_rows(cql, table, keys, ConsistencyLevel.ALL)

        await asyncio.gather(*[
            manager.api.keyspace_flush(s.ip_addr, ks, "t") for s in servers])

        assert_format_on_every_replica(
            workdirs, table, expect_parquet=expect_parquet, min_holders=3)

        # CL=ALL + BYPASS CACHE: all three replicas answer from their own
        # files. A replica whose file were unreadable fails the query here
        # rather than being covered for by the other two.
        assert await read_all(cql, table, ConsistencyLevel.ALL) == expected_rows(keys)

        # And once per replica, pinned, so a single unreadable file cannot
        # hide behind the others' agreement.
        for host in hosts:
            assert await read_all(cql, table, ConsistencyLevel.ONE, host=host) \
                == expected_rows(keys), f"replica {host} disagreed"


###########################################################################
# 3. Repair: the streaming write path, on a node that missed the writes.
###########################################################################

async def test_repair_writes_parquet_on_the_receiving_node(
        manager: ScyllaClusterManager) -> None:
    """Sstables produced by repair on the receiving node are `pq`.

    This is the multi-node analogue of the boot and refresh defects already
    fixed in `replica/distributed_loader.cc`: a write path that does not go
    through compaction, and so had its own idea of which version to write.
    Repair reaches it through `make_streaming_consumer` ->
    `table::make_streaming_sstable_for_write()`.

    Three things make this test rather than a green tick:

      * hinted handoff is off. A hint replay would deliver the missing rows
        as mutations into a memtable, and the subsequent sstable would come
        from the *flush* path -- correct format, wrong path, test proves
        nothing (trap 2).
      * the receiving node is asserted to hold *nothing* before the repair.
        Otherwise "it has pq sstables afterwards" could be describing files
        it already had.
      * the receiving node is never flushed. `make_streaming_consumer` writes
        an sstable directly, so if repair delivered the rows the file is
        already on disk. Flushing first would let a memtable-delivered row
        produce a `pq` file via the flush path and mask the difference.
    """
    cfg = {'hinted_handoff_enabled': False}
    servers = await manager.servers_add(3, auto_rack_dc="dc1", config=cfg)
    cql, _ = await manager.get_ready_cql(servers)
    workdirs = await workdirs_of(manager, servers)

    # One tablet keeps the repair deterministic: every key is in the same
    # tablet, so a single repair covers all of them.
    async with new_test_keyspace(manager, RF3 + " AND tablets = {'initial': 1}") as ks:
        table = f"{ks}.t"
        await cql.run_async(
            f"CREATE TABLE {table} (pk bigint, ck bigint, v text, "
            f"PRIMARY KEY (pk, ck)) WITH storage_format = 'parquet'")

        victim, live_servers = servers[2], servers[:2]
        victim_workdir = workdirs[2]

        logger.info(f"Stopping {victim} so it misses the writes")
        await manager.server_stop_gracefully(victim.server_id)

        keys = list(range(300))
        # QUORUM, not ALL: ALL would fail with a replica down.
        await insert_rows(cql, table, keys, ConsistencyLevel.QUORUM)
        await asyncio.gather(*[
            manager.api.keyspace_flush(s.ip_addr, ks, "t") for s in live_servers])
        assert_format_on_every_replica(
            [workdirs[0], workdirs[1]], table, expect_parquet=True, min_holders=2)

        await manager.server_start(victim.server_id)
        cql, _ = await manager.get_ready_cql(servers)

        # Off-strategy compaction is why this line exists, and it is the
        # subtlest trap in the file. Repair-received sstables land in the
        # maintenance set, and the compaction manager then runs an
        # *off-strategy* compaction to fold them into the main set. That
        # compaction goes through the normal compaction path, which asks
        # `writes_parquet_unconditionally()` and so rewrites them as `pq` --
        # repairing, in passing, exactly the defect this test is looking for.
        # Whether we observe the file repair wrote or the file compaction
        # replaced it with is then a race, and the test would flake green.
        # Holding compaction still makes the observation deterministic and
        # keeps this a test of the *streaming* write.
        await manager.api.disable_autocompaction(victim.ip_addr, ks, "t")

        # Precondition: the node that was down holds nothing for this table.
        stale = data_components(victim_workdir, table)
        assert stale == [], (
            f"SETUP WENT STALE: {victim} already holds sstables for {table} "
            f"before the repair ({stale}); the assertion after the repair "
            f"would not be about repair's output")

        # api.repair() rather than api.repair_and_wait(): the latter always
        # posts to /storage_service/repair_async, which refuses a tablet
        # keyspace with HTTP 403 ("Use /storage_service/tablets/repair").
        # api.repair() looks the keyspace up and picks the right endpoint.
        logger.info(f"Repairing {ks}.t from {victim}")
        await manager.api.repair(victim.ip_addr, ks, "t")

        # No flush here, deliberately -- see the docstring.
        produced = data_components(victim_workdir, table)
        assert produced, (
            f"SETUP WENT STALE: repair produced no sstable on {victim}, so "
            f"there is no format to check. Either the rows were already "
            f"there or the repair did no work.")
        versions = sstable_versions(victim_workdir, table)
        assert versions == {"pq"}, (
            f"repair wrote non-Parquet sstables on {victim}: {versions} "
            f"(files: {produced}) -- the streaming write path did not "
            f"consult storage_format")

        # The repaired replica can serve every row from what repair wrote.
        assert await read_all(cql, table, ConsistencyLevel.ALL) == expected_rows(keys)


###########################################################################
# 4. Bootstrap: the other consumer of the streaming write path.
###########################################################################

async def test_bootstrap_converges_to_parquet_on_the_new_node(
        manager: ScyllaClusterManager) -> None:
    """A node bootstrapped into a cluster holding `pq` data ends up all-`pq`.

    Read the name carefully: this is a *convergence* test, not a test of the
    streaming format decision, and the difference was found the hard way.

    Bootstrap here is a **vnode** keyspace, so the data is reconstructed from
    mutations through `make_streaming_consumer` ->
    `table::make_streaming_sstable_for_write()` -- the path that has to ask
    `storage_format`. But the streamed sstables land in the maintenance set,
    and the compaction manager immediately runs an **off-strategy**
    compaction over them (observed in the node log: "Starting off-strategy
    compaction ... 15 candidates were found"). That compaction goes through
    the normal compaction path, which asks the same predicate and rewrites
    the lot as `pq`.

    So on this path the format is decided *twice*, by two independent
    mechanisms, and the second one repairs the first. Verified: a build whose
    streaming creator ignored `storage_format` entirely -- the exact
    historical drift -- still passes this test, because off-strategy
    compaction converted the native files before the assertion could see
    them. Only mutating streaming *and* compaction together makes it fail.

    That is worth knowing rather than working around: it is why a streaming
    format bug is invisible in practice on the bootstrap path, and it means
    this test cannot stand in for a test of the streaming decision. The
    streaming decision is pinned by
    `test_repair_writes_parquet_on_the_receiving_node` above, which holds
    compaction still so that the file repair wrote is the file it checks.

    RF=1 so the ranges the new node takes over are ranges it must receive:
    at RF=1 every token has exactly one owner.
    """
    first = await manager.server_add()
    cql = manager.get_cql()

    async with new_test_keyspace(
            manager, RF1 + " AND tablets = {'enabled': false}") as ks:
        table = f"{ks}.t"
        await cql.run_async(
            f"CREATE TABLE {table} (pk bigint, ck bigint, v text, "
            f"PRIMARY KEY (pk, ck)) WITH storage_format = 'parquet'")

        # The path is asserted rather than assumed.
        # `tablets_mode_for_new_keyspaces: enabled` in
        # test/cluster/test_config.yaml means a keyspace is a tablet keyspace
        # unless it opts out, and a tablet keyspace here would silently turn
        # this into a second copy of test_tablet_migration_preserves_parquet:
        # file-based streaming preserves `pq` by copying bytes, so nothing
        # would be reconstructed from mutations at all.
        vnode_keyspaces = await manager.api.client.get_json(
            "/storage_service/keyspaces", host=first.ip_addr,
            params={"replication": "vnodes"})
        assert ks in vnode_keyspaces, (
            f"SETUP WENT STALE: {ks} is not a vnode keyspace "
            f"(vnode keyspaces: {vnode_keyspaces}), so bootstrap would move "
            f"whole sstable files instead of reconstructing them from "
            f"mutations, and this test would be a duplicate of the tablet "
            f"migration one")

        # Enough distinct partitions that the token ranges the new node takes
        # over are very unlikely to be empty. A handful of keys could all land
        # on the node that already owns them, and the test would then be
        # asserting over an empty set.
        keys = list(range(500))
        await insert_rows(cql, table, keys, ConsistencyLevel.ONE)
        await manager.api.keyspace_flush(first.ip_addr, ks, "t")

        first_workdir = await manager.server_get_workdir(first.server_id)
        assert sstable_versions(first_workdir, table) == {"pq"}, (
            f"SETUP WENT STALE: the source node's sstables are not Parquet "
            f"({sstable_versions(first_workdir, table)}), so what streams "
            f"from it proves nothing about the format")

        logger.info("Bootstrapping a second node into a cluster holding pq data")
        second = await manager.server_add()
        cql, _ = await manager.get_ready_cql([first, second])
        second_workdir = await manager.server_get_workdir(second.server_id)

        # No flush on the new node: everything here arrived over the wire,
        # either as the streamed sstable or as off-strategy compaction's
        # rewrite of it. Flushing would let a memtable-delivered row produce a
        # `pq` file via the flush path and mask both.
        streamed = data_components(second_workdir, table)
        assert streamed, (
            f"SETUP WENT STALE: bootstrap brought no sstable for {table} to "
            f"{second}, so there is no format to check. With RF=1 and 500 "
            f"partitions the new node should have taken over a non-empty "
            f"range.")
        versions = sstable_versions(second_workdir, table)
        assert versions == {"pq"}, (
            f"the bootstrapped node {second} holds non-Parquet sstables: "
            f"{versions} (files: {streamed}) -- neither the streaming write "
            f"path nor off-strategy compaction converged it on the table's "
            f"declared format")

        # Every row is still there and readable, from both nodes' files.
        assert await read_all(cql, table, ConsistencyLevel.ONE) == expected_rows(keys)


async def test_tablet_migration_preserves_parquet(manager: ScyllaClusterManager) -> None:
    """Tablet bootstrap and decommission keep `pq` data in `pq` files.

    Worth stating what this does and does not prove. Tablet migration uses
    *file*-based streaming (`streaming/stream_blob.cc`), which recreates the
    sstable from the sender's own entry descriptor -- `desc.version` -- and
    copies the bytes. So the format is preserved by construction rather than
    by a decision, and there is no predicate here to get wrong. That makes
    this a regression test against a future migration path that *did*
    re-encode, and a check that a `pq` file is transportable between nodes at
    all; it is not a test of the format decision. The decision on the
    streaming path is covered by the vnode test above and by the repair test.

    Both directions are exercised, because they are different code: the new
    node receives on bootstrap, and the remaining node receives on
    decommission.
    """
    first = await manager.server_add()
    cql = manager.get_cql()

    async with new_test_keyspace(manager, RF1 + " AND tablets = {'initial': 8}") as ks:
        table = f"{ks}.t"
        await cql.run_async(
            f"CREATE TABLE {table} (pk bigint, ck bigint, v text, "
            f"PRIMARY KEY (pk, ck)) WITH storage_format = 'parquet'")

        keys = list(range(500))
        await insert_rows(cql, table, keys, ConsistencyLevel.ONE)
        await manager.api.keyspace_flush(first.ip_addr, ks, "t")

        first_workdir = await manager.server_get_workdir(first.server_id)
        assert sstable_versions(first_workdir, table) == {"pq"}, (
            f"SETUP WENT STALE: the source node's sstables are not Parquet "
            f"({sstable_versions(first_workdir, table)})")

        logger.info("Bootstrapping a second node; tablets should migrate to it")
        second = await manager.server_add()
        cql, _ = await manager.get_ready_cql([first, second])
        second_workdir = await manager.server_get_workdir(second.server_id)

        # Tablet migration is driven by the load balancer and completes
        # asynchronously after the node joins, so this has to be waited for
        # rather than asserted outright -- the first version of this test
        # asserted immediately and failed its own precondition with an empty
        # file list. A timeout here means no tablet ever moved, which is a
        # stale setup rather than a format bug, so it is worded that way.
        await manager.enable_tablet_balancing()

        async def some_tablet_arrived():
            return data_components(second_workdir, table) or None

        migrated = await wait_for(
            some_tablet_arrived, time.time() + 120,
            label=f"a tablet of {table} to migrate to {second}")
        logger.info(f"{len(migrated)} sstable(s) migrated to {second}")
        assert sstable_versions(second_workdir, table) == {"pq"}, (
            f"tablet migration onto {second} produced non-Parquet sstables: "
            f"{sstable_versions(second_workdir, table)} (files: {migrated})")
        assert await read_all(cql, table, ConsistencyLevel.ONE) == expected_rows(keys)

        logger.info(f"Decommissioning {second}; tablets migrate back to {first}")
        await manager.decommission_node(second.server_id)
        cql, _ = await manager.get_ready_cql([first])

        assert sstable_versions(first_workdir, table) == {"pq"}, (
            f"after decommission the surviving node holds non-Parquet "
            f"sstables: {sstable_versions(first_workdir, table)}")
        assert await read_all(cql, table, ConsistencyLevel.ONE) == expected_rows(keys)
