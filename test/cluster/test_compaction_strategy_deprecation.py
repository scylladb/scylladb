#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import logging

import pytest

from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.cluster.util import new_test_keyspace

logger = logging.getLogger(__name__)

# max_threshold is lowered so that a wrongly grouped layout crosses the reshape
# threshold with a handful of SSTables rather than 32 of them, and
# min_sstable_size is lowered so that SSTables this small are still bucketed by
# size instead of all being lumped together as "small".
MAX_THRESHOLD = 4
MIN_THRESHOLD = 4
COMPACTION = ("{'class': 'SizeTieredCompactionStrategy', 'min_sstable_size': '1', "
              f"'min_threshold': '{MIN_THRESHOLD}', 'max_threshold': '{MAX_THRESHOLD}'}}")

# Minor compaction would otherwise rewrite the layout as soon as the node comes
# back up - disable_autocompaction() doesn't survive a restart - and the check
# below couldn't tell that apart from a reshape. With min_threshold enforced, a
# tier under it is left alone, so anything that does change is the reshape.
CONFIG = {'compaction_enforce_min_threshold': True}

RESHAPE_LOG = "found SSTables that need reshape"


async def sstable_generations(manager: ScyllaClusterManager, server, ks: str, table: str) -> set[str]:
    info = await manager.api.get_sstable_info(server.ip_addr, ks, table)
    return {sst['generation'] for shard in info for sst in shard['sstables']}


@pytest.mark.parametrize("in_strategy", [True, False])
async def test_size_tiered_layout_across_restart(manager: ScyllaClusterManager, in_strategy: bool):
    """
    A table configured with the deprecated SizeTieredCompactionStrategy is
    compacted by IncrementalCompactionStrategy, which it is now an alias of.
    Its existing SSTables are not ICS runs - each one is a run of its own - so
    a healthy size-tiered layout must not be mistaken for an off-strategy one
    and reshaped on the next boot, which is what an upgraded cluster does first.

    With in_strategy, the SSTables are laid out in size tiers that are each
    below max_threshold, as STCS leaves them, and must come back from the
    restart untouched. Without it, they form one tier over max_threshold, which
    is off-strategy for real and must be reshaped - so that the case above can't
    pass just because nothing is ever reshaped on boot.
    """
    server = await manager.server_add(config=CONFIG)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY, v text) WITH compaction = {COMPACTION}")
        # Compaction would otherwise merge the layout away before the restart.
        await manager.api.disable_autocompaction(server.ip_addr, ks)

        pk = 0
        async def write_sstable(rows: int) -> None:
            nonlocal pk
            await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.t (pk, v) VALUES ({pk + i}, '{'x' * 1024}')")
                                   for i in range(rows)])
            pk += rows
            await manager.api.keyspace_flush(server.ip_addr, ks, "t")

        if in_strategy:
            # Two size tiers, each below max_threshold, that together exceed it,
            # so a layout wrongly grouped into one bucket would be reshaped.
            for rows_per_sstable in [1, 200]:
                for _ in range(MAX_THRESHOLD - 1):
                    await write_sstable(rows_per_sstable)
        else:
            for _ in range(MAX_THRESHOLD + 1):
                await write_sstable(1)

        before = await sstable_generations(manager, server, ks, "t")
        assert before, "expected the flushes to have produced SSTables"

        log = await manager.server_open_log(server.server_id)
        mark = await log.mark()

        await manager.server_restart(server.server_id)
        await manager.driver_connect(server=server)

        after = await sstable_generations(manager, server, ks, "t")
        reshaped = await log.grep(RESHAPE_LOG, from_mark=mark)
        if in_strategy:
            assert not reshaped, f"restart reshaped an in-strategy size-tiered layout: {reshaped}"
            assert after == before, \
                f"restart rewrote an in-strategy size-tiered layout: {before} -> {after}"
        else:
            # The off-strategy case shows the boot-time reshape does fire here,
            # so the assertions above aren't passing for want of a mechanism.
            assert reshaped, "expected an off-strategy layout to be reshaped on restart"
            assert after != before, \
                f"expected the reshape to replace the off-strategy SSTables: {before}"
