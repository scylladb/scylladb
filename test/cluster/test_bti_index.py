#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
import pytest
import asyncio
import random
import os
import glob
import logging
from typing import Any, Optional
from test.cluster.conftest import skip_mode
from test.pylib.internal_types import ServerInfo
from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import ScyllaMetrics

# main logger
logger = logging.getLogger(__name__)

async def get_metrics(manager: ManagerClient, servers: list[ServerInfo]) -> list[ScyllaMetrics]:
    return await asyncio.gather(*[manager.metrics.query(s.ip_addr) for s in servers])
def get_io_read_ops(metrics: list[ScyllaMetrics]) -> int:
    return int(sum([m.get("scylla_io_queue_total_read_ops") for m in metrics]))

async def live_update_config(manager: ManagerClient, servers: list[ServerInfo], key: str, value: Any):
    cql, hosts = await manager.get_ready_cql(servers)
    await asyncio.gather(*[manager.server_update_config(s.server_id, key, value) for s in servers])
    stmt = cql.prepare("UPDATE system.config SET value=? WHERE name=?")
    await asyncio.gather(*[cql.run_async(stmt, [value, key], host=host) for host in hosts])

async def get_sstable_files_for_server(data_dir, ks, cf):
    base_pattern = os.path.join(data_dir, "data", f"{ks}", f"{cf}-????????????????????????????????", "*.*")
    sstables = glob.glob(base_pattern)
    return sstables

def sstable_version_of(filename: str) -> str:
    # SSTable filenames look like `<version>-<generation>-<format>-<component>.db`,
    # e.g. `me-3-big-Data.db`, `ms-1-big-Index.db`, `mt-7-big-Partitions.db`.
    base = os.path.basename(filename)
    return base.split('-', 1)[0]

async def check_output_format(workdirs: list[str], ks: str, cf: str, expected_version: str):
    # `expected_version` is one of "me", "ms", "mt".
    # ME uses Index.db/Summary.db; MS and MT use Partitions.db/Rows.db.
    sstable_sets = await asyncio.gather(*[get_sstable_files_for_server(wd, ks, cf) for wd in workdirs])
    bti_expected = expected_version in ("ms", "mt")
    for sstable_set in sstable_sets:
        partitions = [x for x in sstable_set if x.endswith('Partitions.db')]
        rows = [x for x in sstable_set if x.endswith('Rows.db')]
        index = [x for x in sstable_set if x.endswith('Index.db')]
        summary = [x for x in sstable_set if x.endswith('Summary.db')]
        data = [x for x in sstable_set if x.endswith('Data.db')]
        if bti_expected:
            assert len(partitions) > 0
            assert len(rows) > 0
            assert len(index) == 0
            assert len(summary) == 0
        else:
            assert len(partitions) == 0
            assert len(rows) == 0
            assert len(index) > 0
            assert len(summary) > 0
        assert data, "no Data.db files found"
        versions = {sstable_version_of(d) for d in data}
        assert versions == {expected_version}, (
            f"expected sstables of version {expected_version!r}, got {versions!r}"
        )

async def set_suppressions(manager: ManagerClient, servers: list[ServerInfo],
                           ms_unsuppressed: bool, mt_unsuppressed: bool):
    suppressed = []
    if not ms_unsuppressed:
        suppressed.append("MS_SSTABLE_FORMAT")
    if not mt_unsuppressed:
        suppressed.append("MT_SSTABLE_FORMAT")
    if suppressed:
        injections = [{'name': 'suppress_features', 'value': ';'.join(suppressed)}]
    else:
        injections = []
    await asyncio.gather(*[manager.server_update_config(s.server_id, "error_injections_at_startup", injections) for s in servers])

# Matrix of expected output sstable formats.
#
# `chosen` is the value passed to the `sstable_format` config. `None` means
# "don't set it"; in our test setup this is equivalent to the default `mt`,
# since test/pylib/scylla_cluster.py sets sstable_format=mt.
#
# Rows are ordered so that `ms_unsuppressed` and `mt_unsuppressed` only ever go
# from False to True as we move down the table: suppressions can be lifted but
# never re-added, and the older feature (MS) is lifted before the newer (MT).
# `chosen` cycles freely within each suppression block since the format choice
# can be changed live.
#
# `chosen=None` means "don't touch the sstable_format config".
# This only makes sense in the first iteration. After we start updating the scylla.yaml,
# the original value of the option of `sstable_format` will be gone.
#
#   chosen   ms_unsuppressed   mt_unsuppressed   ->   expected output
SSTABLE_FORMAT_MATRIX: list[tuple[Optional[str], bool, bool, str]] = [
    ("me",   False, False, "me"),
    ("ms",   False, False, "me"),
    ("mt",   False, False, "me"),
    ("me",   True,  False, "me"),
    ("ms",   True,  False, "ms"),
    ("mt",   True,  False, "me"),
    ("me",   True,  True,  "me"),
    ("ms",   True,  True,  "mt"),
    ("mt",   True,  True,  "mt"),
]

@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_bti_index_output_format(manager: ManagerClient) -> None:
    """Checks that the sstable format written by Scylla is consistent with the
    current set of enabled cluster features and the chosen `sstable_format` config.
    """
    cassandra_logger = logging.getLogger('cassandra')
    cassandra_logger.setLevel(logging.INFO)

    ks = "ks"
    cf = "t"

    servers = await manager.servers_add(1, config={
        'error_injections_at_startup': [
            {
                'name': 'suppress_features',
                'value': 'MS_SSTABLE_FORMAT;MT_SSTABLE_FORMAT',
            }
        ],
        'column_index_size_in_kb': 1,
    })
    cql = manager.get_cql()
    workdirs = await asyncio.gather(*[manager.server_get_workdir(s.server_id) for s in servers])

    await cql.run_async(
        f"CREATE KEYSPACE {ks} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}};"
    )
    await cql.run_async(f"CREATE TABLE {ks}.{cf} (pk int, ck int, v blob, primary key (pk, ck));")

    # Insert some content into the table, doesn't matter what it is.
    insert = cql.prepare(f"INSERT INTO {ks}.{cf} (pk, ck, v) VALUES (?, ?, ?);")
    for pk in range(3):
        for ck in range(32):
            await cql.run_async(insert, (pk, ck, random.randbytes(1024)))
    await asyncio.gather(*[manager.api.keyspace_flush(s.ip_addr, ks, cf) for s in servers])

    prev_suppressions: Optional[tuple[bool, bool]] = None
    for chosen, ms_unsuppressed, mt_unsuppressed, expected in SSTABLE_FORMAT_MATRIX:
        # Suppressions only take effect at startup, so restart whenever they change.
        if prev_suppressions != (ms_unsuppressed, mt_unsuppressed):
            if prev_suppressions is not None:
                logger.info(
                    f"Lifting suppressions to ms_unsuppressed={ms_unsuppressed}, "
                    f"mt_unsuppressed={mt_unsuppressed}"
                )
                await set_suppressions(manager, servers, ms_unsuppressed, mt_unsuppressed)
                manager.driver_close()
                await manager.rolling_restart(servers)
                await manager.driver_connect()
                await manager.get_ready_cql(servers)
            prev_suppressions = (ms_unsuppressed, mt_unsuppressed)

        logger.info(f"chosen={chosen!r} -> expected output format {expected!r}")
        await live_update_config(manager, servers, 'sstable_format', chosen)
        await asyncio.gather(*[manager.api.keyspace_upgrade_sstables(s.ip_addr, ks) for s in servers])
        await check_output_format(workdirs, ks, cf, expected)

    manager.driver_close()

@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_bti_index_read_path(manager: ManagerClient) -> None:
    """Checks that the read path uses the right index components for each sstable
    format (Index.db for ME; Partitions.db/Rows.db for MS and MT), both with and
    without the cache. The output format is also checked for sanity.
    """
    cassandra_logger = logging.getLogger('cassandra')
    cassandra_logger.setLevel(logging.INFO)

    ks = "ks"
    cf = "t"

    # Start with MT suppressed so we can produce ms-format sstables when MS is lifted.
    # MT gets lifted last so we exercise ms before mt.
    #
    # `--smp=1` because this test uses CQL tracing. Trace events are written to
    # trace tables asynchronously w.r.t. the traced statements, and AFAIU there's
    # currently no way (other than polling) to wait for them to appear. The Python
    # driver's polling mechanism for traces is only reliable if the entire statement
    # runs on a single shard (it only waits for the coordinator, not replicas, to
    # write their events). We aren't testing any multi-shard mechanisms here anyway.
    servers = await manager.servers_add(1, cmdline=['--smp=1'], config={
        'error_injections_at_startup': [
            {'name': 'suppress_features', 'value': 'MT_SSTABLE_FORMAT'},
        ],
        'column_index_size_in_kb': 1,
    })
    cql = manager.get_cql()
    workdirs = await asyncio.gather(*[manager.server_get_workdir(s.server_id) for s in servers])

    await cql.run_async(
        f"CREATE KEYSPACE {ks} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}};"
    )
    await cql.run_async(f"CREATE TABLE {ks}.{cf} (pk int, ck int, v blob, primary key (pk, ck));")

    insert = cql.prepare(f"INSERT INTO {ks}.{cf} (pk, ck, v) VALUES (?, ?, ?);")
    n_pks = 3
    n_cks = 32
    pks = list(range(n_pks))
    cks = list(range(n_cks))
    vs = [random.randbytes(1024) for _ in range(n_cks)]
    for pk in pks:
        for ck, v in zip(cks, vs):
            await cql.run_async(insert, (pk, ck, v))
    await asyncio.gather(*[manager.api.keyspace_flush(s.ip_addr, ks, cf) for s in servers])

    # An arbitrary choice of a row to query.
    chosen_pk = pks[len(pks) // 2]
    chosen_ck_idx = len(cks) // 2
    chosen_ck = cks[chosen_ck_idx]
    chosen_v = vs[chosen_ck_idx]

    # (chosen sstable_format, (ms_unsuppressed, mt_unsuppressed))
    cases: list[tuple[str, tuple[bool, bool]]] = [
        ("me", (True, False)),
        ("ms", (True, False)),
        ("mt", (True, True)),
    ]

    async def check_read_path(cql, expected_version: str, use_cache: bool):
        # ME → reads go through Index.db. MS and MT → reads go through Partitions.db/Rows.db.
        should_use_bti = expected_version in ("ms", "mt")
        if use_cache:
            select = cql.prepare(f"SELECT pk, ck, v FROM {ks}.{cf} WHERE pk=? and ck=?;")
        else:
            select = cql.prepare(f"SELECT pk, ck, v FROM {ks}.{cf} WHERE pk=? and ck=? BYPASS CACHE;")
        metrics_before = await get_metrics(manager, servers)
        select_result = cql.execute(select, (chosen_pk, chosen_ck), trace=True)
        metrics_after = await get_metrics(manager, servers)
        row = select_result.one()
        assert row.pk == chosen_pk
        assert row.ck == chosen_ck
        assert row.v == chosen_v

        trace = select_result.get_query_trace()
        seen_index = False
        seen_partitions = False
        seen_rows = False
        for event in trace.events:
            logger.debug(f"Trace event: {event.description}")
            seen_partitions = seen_partitions or "Partitions.db" in event.description
            seen_rows = seen_rows or "Rows.db" in event.description
            seen_index = seen_index or "Index.db" in event.description

        if not use_cache:
            if should_use_bti:
                assert not seen_index, "Index.db was used despite BTI preference"
                assert seen_partitions, "Partitions.db was not used despite BTI preference"
                assert seen_rows, "Rows.db was not used despite BTI preference"
            else:
                assert seen_index, "Index.db was not used despite BIG preference"
                assert not seen_partitions, "Partitions.db was used despite BIG preference"
                assert not seen_rows, "Rows.db was used despite BIG preference"

            # Test that BYPASS CACHE does force disk reads.
            io_read_ops = get_io_read_ops(metrics_after) - get_io_read_ops(metrics_before)
            # At least one read for each of the three primary on-disk components
            # (Index.db main + Index.db promoted + Data.db for BIG;
            # Partitions.db + Rows.db + Data.db for BTI).
            assert io_read_ops >= 3

    prev_suppressions: Optional[tuple[bool, bool]] = None
    for chosen, (ms_unsuppressed, mt_unsuppressed) in cases:
        if prev_suppressions != (ms_unsuppressed, mt_unsuppressed):
            if prev_suppressions is not None:
                logger.info(
                    f"Setting suppressions to ms_unsuppressed={ms_unsuppressed}, "
                    f"mt_unsuppressed={mt_unsuppressed}"
                )
                await set_suppressions(manager, servers, ms_unsuppressed, mt_unsuppressed)
                manager.driver_close()
                await manager.rolling_restart(servers)
                await manager.driver_connect()
                cql, _ = await manager.get_ready_cql(servers)
            prev_suppressions = (ms_unsuppressed, mt_unsuppressed)

        logger.info(f"chosen={chosen!r}")
        await live_update_config(manager, servers, 'sstable_format', chosen)
        await asyncio.gather(*[manager.api.keyspace_upgrade_sstables(s.ip_addr, ks) for s in servers])
        await check_output_format(workdirs, ks, cf, chosen)
        await check_read_path(cql, chosen, use_cache=False)
        await check_read_path(cql, chosen, use_cache=True)

    manager.driver_close()
