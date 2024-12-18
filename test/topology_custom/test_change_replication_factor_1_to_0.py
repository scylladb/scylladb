#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
import logging
import pytest
import asyncio
import time

from cassandra import ConsistencyLevel  # type: ignore
from cassandra.query import SimpleStatement  # type: ignore
from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for_cql_and_get_hosts


logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "use_tablets",
    [
        pytest.param(False, id="vnodes"),
        pytest.param(True, id="tablets", marks=pytest.mark.xfail(reason="issue #20282")),
    ],
)
@pytest.mark.asyncio
async def test_change_replication_factor_1_to_0(request: pytest.FixtureRequest, manager: ManagerClient, use_tablets: bool) -> None:
    CONFIG = {"endpoint_snitch": "GossipingPropertyFileSnitch", "enable_tablets": str(use_tablets)}
    logger.info("Creating a new cluster")
    for i in range(2):
        await manager.server_add(
            config=CONFIG,
            property_file={'dc': f'dc{i}', 'rack': f'myrack{i}'})

    cql = manager.get_cql()
    await cql.run_async("create keyspace ks with replication = {'class': 'NetworkTopologyStrategy', 'dc0': 1, 'dc1': 1}")
    await cql.run_async("create table ks.t (pk int primary key)")

    srvs = await manager.running_servers()
    await wait_for_cql_and_get_hosts(cql, srvs, time.time() + 60)

    stmt = cql.prepare(f"SELECT * FROM ks.t where pk = ?")
    stmt.consistency_level = ConsistencyLevel.LOCAL_QUORUM

    stop_event = asyncio.Event()

    async def do_reads() -> None:
        iteration = 0
        while not stop_event.is_set():
            start_time = time.time()
            try:
                await cql.run_async(stmt, [0])
            except Exception as e:
                logger.error(f"Read started {time.time() - start_time}s ago failed: {e}")
                raise
            iteration += 1
            await asyncio.sleep(0.01)
        logger.info(f"Finishing with iter {iteration}")

    tasks = [asyncio.create_task(do_reads()) for _ in range(3)]

    await cql.run_async("alter keyspace ks with replication = {'class': 'NetworkTopologyStrategy', 'dc0': 1, 'dc1': 0}")

    await asyncio.sleep(1)
    stop_event.set()
    await asyncio.gather(*tasks)
