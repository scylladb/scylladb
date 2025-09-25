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

from test.storage.conftest import space_limited_servers
from test.cluster.util import batch_cql, get_coordinator_host, new_test_keyspace, new_test_table
from test.pylib.manager_client import ManagerClient

logger = logging.getLogger(__name__)

MAX_IMBALANCE_PERCENTAGE = 5  # maximum allowed imbalance between nodes in percentage


@pytest.mark.asyncio
@pytest.mark.parametrize("num_nodes", [3, 4, 6])
@pytest.mark.parametrize("strategy", ["LeveledCompactionStrategy", "SizeTieredCompactionStrategy", "TimeWindowCompactionStrategy", "IncrementalCompactionStrategy"])
async def test_data_distribution_balance(manager: ManagerClient, volumes_factory: Callable, num_nodes: int, strategy: str) -> None:
    """
    Test that data is evenly distributed across nodes after writes and compaction.

    The test creates a cluster with the given number of nodes, each with a limited disk space.  
    It then creates a keyspace and a table with the given compaction strategy, and inserts
    a large amount of data, setup in such a way that there are multiple partitions of varying sizes.

    After flushing and compacting the data, it checks that the disk usage across nodes is balanced within the defined threshold.
    """
    config = {
        "commitlog_segment_size_in_mb": 2, 
        "commitlog_total_space_in_mb": 10,  # limit commitlog so it has a smaller impact on disk space and balancing
        "size_based_balance_threshold_percentage": 3,  # lower than MAX_IMBALANCE_PERCENTAGE to make balancing more aggressive
        "tablet_load_stats_refresh_interval_in_seconds": 10  # default is 60, lowering it to make test faster
    }

    # Generate a topology with the specified number of nodes
    # Total size is fixed at 1000MB
    topology_sizes = {
        3: {"dc1": {"r1": ["250M", "250M", "500M"]}},
        4: {"dc1": {"r1": ["200M", "200M", "300M", "300M"]}},
        6: {"dc1": {"r1": ["150M", "150M", "150M", "150M", "200M", "200M"]}},
    }[num_nodes]

    async with space_limited_servers(manager, volumes_factory, topology_sizes, config=config, cmdline=["--logger-log-level=load_balancer=debug"]) as servers:
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

                # wait for tablet refresh
                coordinator = await get_coordinator_host(manager)
                log = await manager.server_open_log(coordinator.server_id)
                await log.wait_for(r"raft_topology - raft topology: Refreshed table load stats for all DC\(s\)\.")

                # Wait for tablet migration to finish
                await asyncio.gather(*(manager.api.quiesce_topology(server.ip_addr) for server in servers))

                usages = []
                for server in servers:
                    workdir = await manager.server_get_workdir(server.server_id)
                    disk_info = psutil.disk_usage(workdir)
                    usages.append(disk_info.percent)
                    logger.info(f"Disk usage for server {server.server_id}: {disk_info}")
                    res = subprocess.run(["du", "-h", "-BM", "-d", "1", workdir], capture_output=True, check=False, encoding="utf-8")
                    logger.debug(f"Disk space per folder:\n{res.stdout.strip()}")

                assert max(usages) - min(usages) <= MAX_IMBALANCE_PERCENTAGE, f"Data distribution is {max(usages) - min(usages):<.2f}% imbalanced: {usages}"
