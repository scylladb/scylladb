#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
import asyncio
import logging
import os
import subprocess
from typing import Callable

import psutil
import pytest

from test.cluster.util import batch_cql, get_coordinator_host, new_test_keyspace, new_test_table
from test.pylib.manager_client import ManagerClient
from test.cluster.storage.conftest import space_limited_servers

logger = logging.getLogger(__name__)

MAX_IMBALANCE_PERCENTAGE = 0.05  # maximum allowed imbalance between nodes (5%)


@pytest.mark.asyncio
@pytest.mark.nightly
@pytest.mark.parametrize("strategy", ["LeveledCompactionStrategy", "SizeTieredCompactionStrategy", "TimeWindowCompactionStrategy", "IncrementalCompactionStrategy"])
async def test_data_distribution_balance(manager: ManagerClient, volumes_factory: Callable, strategy: str) -> None:
    """
    Test that data is evenly distributed across nodes after writes and compaction.

    The test creates a cluster with nodes that each have a different disk space size.
    It then creates a keyspace and a table with the given compaction strategy,
    and inserts a large amount of data, setup in such a way that there are multiple partitions of varying sizes.

    After flushing and compacting the data, it checks that the disk usage across nodes is balanced within the defined threshold.
    """
    config = {
        "commitlog_segment_size_in_mb": 4,
        "schema_commitlog_segment_size_in_mb": 8,
        "commitlog_total_space_in_mb": 20,  # limit commitlog so it has a smaller impact on disk space
        "minimal_tablet_size_for_balancing": 1,  # tablets are small in this test
        "tablet_load_stats_refresh_interval_in_seconds": 1,  # default is 60, lowering it to make test faster
    }
    cmdline = ["--logger-log-level=load_balancer=debug"]

    # Generate a topology where nodes have different disk space sizes
    topology_sizes = {"dc1": {"r1": ["250M", "300M", "350M", "400M"]}}

    async with space_limited_servers(manager, volumes_factory, topology_sizes, config=config, cmdline=cmdline) as servers:
        cql, _ = await manager.get_ready_cql(servers)

        async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 256}") as ks:
            # if compaction happens when checking disk space
            # it may double count both the source sstables that are being compacted and temporary sstables that are being written by compaction
            # to counteract this, disable auto compaction
            for server in servers:
                await manager.api.disable_autocompaction(server.ip_addr, ks)

            async with new_test_table(manager, ks, "pk int, ck int, v blob, PRIMARY KEY(pk, ck)", f"WITH compaction={{'class': '{strategy}'}}") as cf:
                stmt = cql.prepare(f"INSERT INTO {cf} (pk, ck, v) VALUES (?, ?, ?)")
                # Will insert:
                # 1000 partitions with 100 clustering rows each
                # 40 partitions with 2500 clustering rows each
                # 10 partitions with 10000 clustering rows each
                # Total 300000 rows
                for p_keys, c_rows in [(1000, 100), (40, 2500), (10, 10000)]:
                    parameters = []
                    for p in range(p_keys):
                        pk = p_keys + p  # avoid duplicate partition keys
                        parameters += [(pk, ck, os.urandom(1000)) for ck in range(c_rows)]

                    logger.debug(f"Will insert {p_keys} partitions with {c_rows} clustering rows each, total: {len(parameters)} rows")
                    await batch_cql(cql, stmt, parameters, batch_size=800)

                    # Flush often to not hit auto-flush due to small commitlog size, it may timeout the writes
                    await asyncio.gather(*(manager.api.flush_keyspace(server.ip_addr, ks) for server in servers))

                # Trigger compaction
                await asyncio.gather(*(manager.api.keyspace_compaction(server.ip_addr, ks) for server in servers))

                # Wait for next tablet refresh
                coordinator = await get_coordinator_host(manager)
                log = await manager.server_open_log(coordinator.server_id)
                mark = await log.mark()
                await log.wait_for(r"raft_topology - raft topology: Refreshed table load stats for all DC\(s\)\.", from_mark=mark)

                # Wait for tablet migration to finish
                for _ in range(3):
                    await asyncio.gather(*(manager.api.quiesce_topology(server.ip_addr) for server in servers))

                usages = []
                for server in servers:
                    # disk usage is data / capacity; and capacity = data + free space
                    workdir = await manager.server_get_workdir(server.server_id)
                    data_dir = os.path.join(workdir, "data")
                    free_bytes = psutil.disk_usage(workdir).free
                    res = subprocess.run(["du", "-B1", "-s", data_dir], capture_output=True, check=False, encoding="utf-8")
                    data_bytes = int(res.stdout.strip().split()[0])
                    capacity = data_bytes + free_bytes
                    disk_usage = data_bytes / capacity
                    usages.append(disk_usage)

                    # debug logging for exact disk usage
                    host_id = await manager.get_host_id(server.server_id)
                    logger.debug(f"Server {server.server_id} {host_id}: {disk_usage=:.2%} ({data_bytes=} {free_bytes=} {capacity=})")
                    res = subprocess.run(["du", "-B1", "-d1", workdir], capture_output=True, check=False, encoding="utf-8")
                    logger.debug(f"`du` of workdir :\n{res.stdout.strip()}")

                balance = max(usages) - min(usages)
                assert balance <= MAX_IMBALANCE_PERCENTAGE, f"Data distribution is {balance:.2%} imbalanced: {usages}"
