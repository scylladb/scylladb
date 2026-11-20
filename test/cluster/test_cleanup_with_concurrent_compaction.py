#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import glob
import logging
import os
import re

import pytest

from test.cluster.util import new_test_keyspace
from test.pylib.rest_client import inject_error_one_shot
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)


async def write_sstables(manager, server, ks, first, count):
    """Writes `count` sstables, numbered from `first`, to table ks.test, and
    returns the names of the Data.db files of all its sstables."""
    cql = manager.get_cql()
    # The node owns all the tokens, but cleanup still rewrites an sstable
    # whose token range doesn't fit in one of the node's vnode ranges.
    # So write rows with tokens spread over the ring to each sstable.
    for i in range(first, first + count):
        await asyncio.gather(*[cql.run_async(f'INSERT INTO {ks}.test (pk, v) VALUES ({pk}, {pk})')
                               for pk in range(20 * i, 20 * (i + 1))])
        await manager.api.keyspace_flush(server.ip_addr, ks, 'test')
    workdir = await manager.server_get_workdir(server.server_id)
    [table_dir] = glob.glob(f'{workdir}/data/{ks}/test-*')
    return {os.path.basename(f) for f in glob.glob(f'{table_dir}/*-Data.db')}


def sstables_in(line):
    """Returns the names of the Data.db files in a compaction log line."""
    return set(re.findall(r'[^/,\[\]]+-Data\.db', line))


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_regular_compaction_skips_sstables_waiting_for_cleanup(manager: ScyllaClusterManager):
    """
    Cleanup selects all the sstables it will rewrite before it starts
    rewriting them. Regular compaction, which may run in the meantime, must
    not pick the sstables that cleanup selected: the same data would be
    compacted twice, and the sstables of whichever compaction finishes last
    are already deleted.
    The test pauses cleanup before it starts rewriting, gives regular
    compaction new sstables to compact, and checks that the regular
    compaction doesn't take any of the sstables that cleanup selected.
    Reproduces #8155.
    """
    server = await manager.server_add(cmdline=['--smp', '1', '--logger-log-level', 'compaction=debug'])
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'enabled': false}") as ks:
        await cql.run_async(f'CREATE TABLE {ks}.test (pk int PRIMARY KEY, v int)')
        await manager.api.disable_autocompaction(server.ip_addr, ks, 'test')
        # Regular compaction compacts at least 4 (min_threshold) sstables of
        # similar size. The sstables for cleanup and the new ones are all
        # tiny, so if regular compaction could pick the former, it would take
        # them too.
        sstables_for_cleanup = await write_sstables(manager, server, ks, 0, 4)
        assert len(sstables_for_cleanup) == 4

        await inject_error_one_shot(manager.api, server.ip_addr, 'sstable_cleanup_wait')
        cleanup = asyncio.create_task(manager.api.cleanup_keyspace(server.ip_addr, ks))
        try:
            await manager.api.wait_for_injection_enter(server.ip_addr, 'sstable_cleanup_wait')

            await write_sstables(manager, server, ks, 4, 4)
            log = await manager.server_open_log(server.server_id)
            mark = await log.mark()
            await manager.api.enable_autocompaction(server.ip_addr, ks, 'test')
            # Wait less than sstable_cleanup_wait's 60 seconds, whose expiry is
            # an internal error, which aborts Scylla in tests.
            _, [(line, _)] = await log.wait_for(rf'\[Compact {ks}\.test [^\]]*\] Compacting \[', from_mark=mark, timeout=30)
            compacted = sstables_in(line)
            logger.info(f'Regular compaction took {compacted}, cleanup selected {sstables_for_cleanup}')
            # Check that regular compaction took only the new sstables.
            assert compacted and not compacted & sstables_for_cleanup
        finally:
            await manager.api.message_injection(server.ip_addr, 'sstable_cleanup_wait')
            await cleanup
