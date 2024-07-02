# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

from __future__ import annotations

import sys
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
from test.topology.conftest import skip_mode
from test.pylib.internal_types import ServerState
from test.topology_random_failures.cluster_events import CLUSTER_EVENTS
from test.topology_random_failures.error_injections import ERROR_INJECTIONS

if TYPE_CHECKING:
    from test.pylib.random_tables import RandomTables
    from test.pylib.internal_types import ServerNum
    from test.pylib.manager_client import ManagerClient
    from test.topology_random_failures.cluster_events import ClusterEventType


LOGGER = logging.getLogger(__name__)


# Parametrize the test and limit the number of tests to run.
#
# Following pytest options can be used to control the number and a sequence of the tests:
#
#   `--raft-failure-injections-tests-count' to limit total count of tests from the whole matrix
#   `--raft-failure-injections-seed' to use some stable order
#
# Also, to be able to run same sequence of tests if some new error injection and/or cluster events will be added
# there are options to exclude these new items:
#
#   `--raft-errors-last-index' to limit number of error injections
#   `--raft-events-last-index' to limit number of cluster events
#
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
        seed = metafunc.config.getoption("--raft-failure-injections-seed")
        if seed is None:
            seed = random.randrange(sys.maxsize)
        LOGGER.info("Effective --raft-failure-injections-seed is %s", seed)

        tests = list(itertools.product(*values))
        random.Random(seed).shuffle(tests)
        metafunc.parametrize(names, tests[:metafunc.config.getoption("--raft-failure-injections-tests-count")])


@pytest.fixture
async def four_nodes_cluster(manager: ManagerClient) -> list[ServerNum]:
    LOGGER.info("Booting initial 4-node cluster")
    servers = [(await manager.server_add()).server_id for _ in range(4)]
    await wait_for_token_ring_and_group0_consistency(manager=manager, deadline=time.time() + 30)
    return servers


@pytest.mark.usefixtures("four_nodes_cluster")
@pytest.mark.asyncio
@skip_mode("release", "error injections are not supported in release mode")
async def test_random_failures(manager: ManagerClient,
                               random_tables: RandomTables,
                               error_injection: str,
                               cluster_event: ClusterEventType) -> None:
    table = await random_tables.add_table(ncolumns=5)
    await table.insert_seq()

    cluster_event_steps = cluster_event(manager, random_tables, error_injection)

    LOGGER.info("Run preparation step of the cluster event")
    await anext(cluster_event_steps)

    if error_injection == "stop_after_updating_cdc_generation":
        # This error injection is a special one and should be handled on a coordinator node:
        #   1. Enable the injection on a coordinator node
        #   2. Bootstrap a new node
        #   3. Wait till the injection handler will print the message to the log
        #   4. Pause the bootstrapping node
        #   5. Send the message to the injection handler to continue
        coordinator = (await manager.running_servers())[0]
        await manager.api.enable_injection(node_ip=coordinator.ip_addr, injection=error_injection, one_shot=True)
        coordinator_log = await manager.server_open_log(server_id=coordinator.server_id)
        coordinator_log_mark = await coordinator_log.mark()
        s_info = await manager.server_add(expected_server_state=ServerState.PROCESS_STARTED)
        await coordinator_log.wait_for(
            pattern="stop_after_updating_cdc_generation: wait for message for 5 minutes",
            from_mark=coordinator_log_mark,
        )
        await manager.server_pause(server_id=s_info.server_id)
        await manager.api.message_injection(coordinator.ip_addr, error_injection)
    else:
        s_info = await manager.server_add(
            config={"error_injections_at_startup": [{"name": error_injection, "one_shot": True}]},
            expected_server_state=ServerState.PROCESS_STARTED,
        )

    LOGGER.info("Wait till the bootstrapping node will be paused")
    await manager.wait_for_scylla_process_status(
        server_id=s_info.server_id,
        expected_statuses=[
            psutil.STATUS_STOPPED,
        ],
    )

    LOGGER.info("Run the cluster event main step")
    try:
        await anext(cluster_event_steps)
    finally:
        LOGGER.info("Unpause the server and wait till it wake up and become connectable using CQL")
        await manager.server_unpause(server_id=s_info.server_id)

    logs = await manager.server_open_log(s_info.server_id)

    LOGGER.info("Wait until the new node initialization completes or fails")
    await logs.wait_for("init - (Startup failed:|Scylla version .* initialization completed)", timeout=120)
    time.sleep(1)

    LOGGER.info("Check that the new node is running or dead")
    scylla_process_status = await manager.wait_for_scylla_process_status(
        server_id=s_info.server_id,
        expected_statuses=[
            psutil.STATUS_RUNNING,
            psutil.STATUS_SLEEPING,
            psutil.STATUS_DEAD,
        ],
    )

    if scylla_process_status in (psutil.STATUS_RUNNING, psutil.STATUS_SLEEPING):
        LOGGER.info("The new node is running.  Check if we can connect to it.")

        async def driver_connect() -> bool | None:
            try:
                await manager.driver_connect(server=s_info)
            except NoHostAvailable:
                LOGGER.info("Driver not connected to %s yet", s_info.ip_addr)
                return None
            return True

        LOGGER.info("Check for the new node's health")
        await wait_for(pred=driver_connect, deadline=time.time() + 90)
        await wait_for_cql_and_get_hosts(cql=manager.cql, servers=[s_info], deadline=time.time() + 30)
    else:
        LOGGER.info("The new node is dead.  Check if it failed to startup.")
        assert await logs.grep("init - Startup failed:")
        await manager.server_stop(server_id=s_info.server_id)  # remove the node from the list of running servers

    LOGGER.info("Check for the cluster's health")
    await manager.driver_connect()
    await wait_for_token_ring_and_group0_consistency(manager=manager, deadline=time.time() + 90)
    await table.add_column()
    await random_tables.verify_schema()
    await anext(cluster_event_steps, None)  # use default value to suppress StopIteration
    await random_tables.verify_schema()
