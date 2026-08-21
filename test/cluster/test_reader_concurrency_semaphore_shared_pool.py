#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import logging
import time

from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel

from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for

from .util import new_test_keyspace, new_test_table


logger = logging.getLogger(__name__)

SHARED_POOL_FRACTION = "reader_concurrency_semaphore_shared_pool_fraction"

TOTAL_MEMORY_METRIC = "scylla_database_reads_shared_pool_total_memory"
AVAILABLE_MEMORY_METRIC = "scylla_database_reads_shared_pool_available_memory"
BORROWED_MEMORY_METRIC = "scylla_database_reads_memory_borrowed_from_shared_pool"
MAIN_POOL_LABELS = {"class": "unnamed"}


async def _pool_metric(manager: ManagerClient, ip_addr: str, name: str) -> int:
    metrics = await manager.metrics.query(ip_addr)
    return int(metrics.get(name, MAIN_POOL_LABELS) or 0)


async def test_shared_pool_live_resize_under_reads(manager: ManagerClient):
    """
    Change reader_concurrency_semaphore_shared_pool_fraction live (0.2 -> 0 -> 0.5)
    while a continuous stream of reads is in flight, verifying via metrics that
    the shared pool is resized correctly and reads keep succeeding. Finally drive
    the pool to 0.99 so the tiny dedicated budget is exhausted and confirm reads
    borrow from the shared pool.

    The node runs with a small memory budget so the reader semaphore's dedicated
    memory (a fixed 2% fraction of node memory) is small enough to exercise the
    shared pool with ordinary reads.
    """
    server = await manager.server_add(cmdline=["--smp", "1"])
    cql, hosts = await manager.get_ready_cql([server])
    ip = server.ip_addr
    host = hosts[0]

    rows = 10
    blob_size = 100 * 1024
    concurrency = 50

    async with new_test_keyspace(manager, "WITH REPLICATION = {'replication_factor': 1}", host) as ks:
        async with new_test_table(manager, ks, "pk int, ck int, v blob, PRIMARY KEY (pk, ck)", host=host) as tbl:
            await manager.server_update_config(server.server_id, SHARED_POOL_FRACTION, 0.2)
            insert = cql.prepare(f"INSERT INTO {tbl} (pk, ck, v) VALUES (0, ?, ?)")
            blob = bytes(blob_size)
            await asyncio.gather(*(cql.run_async(insert, [i, blob]) for i in range(rows)))
            await manager.api.flush_keyspace(ip, ks)
            logger.info(f"Inserted {rows} rows of {blob_size} bytes into a single partition")

            stop_event = asyncio.Event()
            # Unpaged reverse-order full-partition scan, kept running in the
            # background for the whole test so the semaphore is always under load
            # while the shared pool fraction is changed.
            select = SimpleStatement(f"SELECT * FROM {tbl} WHERE pk = 0 ORDER BY ck DESC",
                                     consistency_level=ConsistencyLevel.ONE,
                                     fetch_size=None)
            read_state = {"count": 0, "max_borrowed": 0}

            async def do_reads():
                while not stop_event.is_set():
                    await cql.run_async(select, host=host)
                    read_state["count"] += 1

            # The borrow gauge is a gauge that drops back to 0 once a read repays
            # the pool, so it must be sampled continuously while reads are in
            # flight. Sum across all per-semaphore (per-scheduling-group) series.
            async def sample_borrowed():
                while not stop_event.is_set():
                    metrics = await manager.metrics.query(ip)
                    borrowed = int(metrics.get(BORROWED_MEMORY_METRIC) or 0)
                    read_state["max_borrowed"] = max(read_state["max_borrowed"], borrowed)

            async def reads_advanced(baseline: int):
                # Truthy once enough new reads have completed since the baseline.
                done = read_state["count"] - baseline
                return done if done > 50 else None

            read_tasks = [asyncio.create_task(do_reads()) for _ in range(concurrency)]
            sampler_task = asyncio.create_task(sample_borrowed())

            try:
                # Make sure reads are actively running so the live config changes
                # below happen while the semaphore is under load.
                await wait_for(lambda: reads_advanced(0), time.time() + 60, label="reads start")

                # The shared pool holds ~0.2 of the group.
                total_20 = await _pool_metric(manager, ip, TOTAL_MEMORY_METRIC)
                available_20 = await _pool_metric(manager, ip, AVAILABLE_MEMORY_METRIC)
                logger.info(f"fraction=0.2: total={total_20} available={available_20}")
                assert total_20 > 0
                assert 0 <= available_20 <= total_20

                # Live update to 0: the shared pool is emptied.
                await manager.server_update_config(server.server_id, SHARED_POOL_FRACTION, 0)

                async def pool_emptied():
                    return (await _pool_metric(manager, ip, TOTAL_MEMORY_METRIC)) == 0 or None
                await wait_for(pool_emptied, time.time() + 60, label="pool total drops to 0")
                assert (await _pool_metric(manager, ip, AVAILABLE_MEMORY_METRIC)) == 0
                logger.info("fraction=0: shared pool emptied")

                # Reads must keep succeeding with the pool disabled.
                reads_at_zero = read_state["count"]
                await wait_for(lambda: reads_advanced(reads_at_zero), time.time() + 60,
                               label="reads progress with pool disabled")

                # Live update to 0.5: the shared pool grows to ~half the group.
                await manager.server_update_config(server.server_id, SHARED_POOL_FRACTION, 0.5)

                async def pool_grew():
                    total = await _pool_metric(manager, ip, TOTAL_MEMORY_METRIC)
                    # 0.5 must be clearly larger than the 0.2 total.
                    return total if total > total_20 * 2 else None
                total_50 = await wait_for(pool_grew, time.time() + 60, label="pool total grows")
                available_50 = await _pool_metric(manager, ip, AVAILABLE_MEMORY_METRIC)
                logger.info(f"fraction=0.5: total={total_50} available={available_50}")
                assert 0 <= available_50 <= total_50

                # Drive the dedicated budget almost to zero (0.99 shared) so the
                # in-flight reads immediately exhaust it and borrow from the
                # shared pool. Sample until a non-zero borrow is observed.
                await manager.server_update_config(server.server_id, SHARED_POOL_FRACTION, 0.99)

                async def borrowed_seen():
                    return read_state["max_borrowed"] or None
                await wait_for(borrowed_seen, time.time() + 60, label="reads borrow from shared pool")
                logger.info(f"fraction=0.99: peak memory borrowed from shared pool = {read_state['max_borrowed']}")
            finally:
                stop_event.set()
                sampler_task.cancel()
                await asyncio.gather(*read_tasks, sampler_task, return_exceptions=True)

            logger.info(f"Total successful reads during test: {read_state['count']}")
            assert read_state["count"] > 0
