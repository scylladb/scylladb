# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

from __future__ import annotations

import time
import random
import logging
import itertools
from typing import TYPE_CHECKING

import psutil
import pytest
from cassandra.cluster import NoHostAvailable

from test.pylib.util import wait_for, wait_for_cql_and_get_hosts
from test.topology.util import wait_for_token_ring_and_group0_consistency
from test.pylib.internal_types import ServerState
from test.topology_experimental_raft.cluster_events import CLUSTER_EVENTS
from test.topology_experimental_raft.error_injections import ERROR_INJECTIONS

if TYPE_CHECKING:
    from typing import Optional
    from collections.abc import Awaitable, Callable

    from test.pylib.random_tables import RandomTables
    from test.pylib.internal_types import ServerNum
    from test.pylib.manager_client import ManagerClient


LOGGER = logging.getLogger(__name__)


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    names = []
    values = []

    if "error_injection" in metafunc.fixturenames:
        names.append("error_injection")
        values.append(ERROR_INJECTIONS[:metafunc.config.getoption("--raft-errors-last-index")])

    if "cluster_event" in metafunc.fixturenames:
        names.append("cluster_event")
        values.append(CLUSTER_EVENTS[:metafunc.config.getoption("--raft-events-last-index")])

    if names:
        tests = list(itertools.product(*values))
        random.Random(metafunc.config.getoption("--raft-failure-injections-seed")).shuffle(tests)
        metafunc.parametrize(names, tests[:metafunc.config.getoption("--raft-failure-injections-tests-count")])


@pytest.fixture
async def three_nodes_cluster(manager: ManagerClient) -> list[ServerNum]:
    LOGGER.info(f"Booting initial 3-node cluster")
    servers = [(await manager.server_add()).server_id for _ in range(3)]
    await wait_for_token_ring_and_group0_consistency(manager=manager, deadline=time.time() + 30)
    return servers


@pytest.mark.usefixtures("three_nodes_cluster")
@pytest.mark.asyncio
async def test_random_failures(manager: ManagerClient,
                               random_tables: RandomTables,
                               error_injection: str,
                               cluster_event: Callable[[ManagerClient, RandomTables], Awaitable[None]]) -> None:
    table = await random_tables.add_table(ncolumns=5)
    await table.insert_seq()

    # Add an index for drop_index cluster event.
    await table.add_index(table.columns[-2], "test_random_failures_index_to_drop")

    # Add another table for drop_table cluster event.
    await random_tables.add_table(ncolumns=5)

    # Add a keyspace for drop_keyspace cluster event.
    await manager.cql.run_async(
        "CREATE KEYSPACE test_random_failures_ks_to_drop"
        " WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 3 }"
    )

    await random_tables.add_udt("test_random_failures_udt_to_drop", "(a text, b int)")

    s_info = await manager.server_add(
        config={"error_injections_at_startup": [error_injection]},
        expected_server_state=ServerState.PROCESS_STARTED,
    )
    await manager.wait_for_scylla_process_status(
        server_id=s_info.server_id,
        expected_status=psutil.STATUS_STOPPED,
    )

    await cluster_event(manager, random_tables)

    LOGGER.info("Unpause the server and wait till it wake up and become connectable using CQL")
    await manager.server_unpause(server_id=s_info.server_id)

    async def driver_connect() -> Optional[bool]:
        try:
            await manager.driver_connect(server=s_info)
        except NoHostAvailable:
            LOGGER.info("Driver not connected to %s yet", s_info.ip_addr)
            return None
        return True

    await wait_for(pred=driver_connect, deadline=time.time() + 90)
    await wait_for_cql_and_get_hosts(cql=manager.cql, servers=[s_info], deadline=time.time() + 30)

    LOGGER.info("Check for the cluster's health")
    await manager.driver_connect()
    await wait_for_token_ring_and_group0_consistency(manager=manager, deadline=time.time() + 30)
    await table.add_column()
    await random_tables.verify_schema()
