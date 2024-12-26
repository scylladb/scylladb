#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from __future__ import annotations

import sys
import time
import random
import logging
import itertools
from typing import TYPE_CHECKING
from contextlib import suppress

import psutil
import pytest
from cassandra.cluster import NoHostAvailable

from test.pylib.util import wait_for, wait_for_cql_and_get_hosts
from test.topology.util import wait_for_token_ring_and_group0_consistency, get_coordinator_host
from test.topology.conftest import skip_mode
from test.pylib.internal_types import ServerUpState
from test.topology_random_failures.cluster_events import CLUSTER_EVENTS, TOPOLOGY_TIMEOUT
from test.topology_random_failures.error_injections import ERROR_INJECTIONS, ERROR_INJECTIONS_NODE_MAY_HANG

if TYPE_CHECKING:
    from test.pylib.random_tables import RandomTables
    from test.pylib.manager_client import ManagerClient
    from test.topology_random_failures.cluster_events import ClusterEventType


TESTS_COUNT = 1  # number of tests from the whole matrix to run, None to run the full matrix.

# Following parameters can be adjusted to run same sequence of tests from a previous run.  Look at logs for the values.
# Also see `pytest_generate_tests()` below for details.
TESTS_SHUFFLE_SEED = random.randrange(sys.maxsize)  # seed for the tests order randomization
ERROR_INJECTIONS_COUNT = len(ERROR_INJECTIONS)  # change it to limit number of error injections
CLUSTER_EVENTS_COUNT = len(CLUSTER_EVENTS)  # change it to limit number of cluster events

WAIT_FOR_IP_TIMEOUT = 30  # seconds

LOGGER = logging.getLogger(__name__)


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    error_injections = ERROR_INJECTIONS[:ERROR_INJECTIONS_COUNT]
    cluster_events = CLUSTER_EVENTS[:CLUSTER_EVENTS_COUNT]
    tests = list(itertools.product(error_injections, cluster_events))

    random.Random(TESTS_SHUFFLE_SEED).shuffle(tests)

    # Deselect unsupported combinations.  Do it after the shuffle to have the stable order.
    tests = [
        (inj, event) for inj, event in tests if inj not in getattr(event, "deselected_random_failures", {})
    ]

    metafunc.parametrize(["error_injection", "cluster_event"], tests[:TESTS_COUNT])


@pytest.fixture
async def four_nodes_cluster(manager: ManagerClient) -> None:
    LOGGER.info("Booting initial 4-node cluster.")
    for _ in range(4):
        server = await manager.server_add()
        await manager.api.enable_injection(
            node_ip=server.ip_addr,
            injection="raft_server_set_snapshot_thresholds",
            one_shot=True,
            parameters={
                "snapshot_threshold": "3",
                "snapshot_trailing": "1",
            }
        )
    await wait_for_token_ring_and_group0_consistency(manager=manager, deadline=time.time() + 30)


@pytest.mark.usefixtures("four_nodes_cluster")
@pytest.mark.asyncio
@skip_mode("release", "error injections are not supported in release mode")
async def test_random_failures(manager: ManagerClient,
                               random_tables: RandomTables,
                               error_injection: str,
                               cluster_event: ClusterEventType) -> None:
    LOGGER.info(
        "To repeat this run set TESTS_COUNT to %s, TESTS_SHUFFLE_SEED to %s, ERROR_INJECTIONS_COUNT to %s,"
        " and CLUSTER_EVENTS_COUNT to %s",
        TESTS_COUNT, TESTS_SHUFFLE_SEED, ERROR_INJECTIONS_COUNT, CLUSTER_EVENTS_COUNT,
    )

    table = await random_tables.add_table(ncolumns=5)
    await table.insert_seq()

    cluster_event_steps = cluster_event(manager, random_tables, error_injection)

    LOGGER.info("Run preparation step of the cluster event.")
    await anext(cluster_event_steps)

    if error_injection == "stop_after_updating_cdc_generation":
        # This error injection is a special one and should be handled on a coordinator node:
        #   1. Enable `topology_coordinator_pause_after_updating_cdc_generation` injection on a coordinator node
        #   2. Bootstrap a new node
        #   3. Wait till the injection handler will print the message to the log
        #   4. Pause the bootstrapping node
        #   5. Send the message to the injection handler to continue
        coordinator = await get_coordinator_host(manager=manager)
        await manager.api.enable_injection(
            node_ip=coordinator.ip_addr,
            injection="topology_coordinator_pause_after_updating_cdc_generation",
            one_shot=True,
        )
        coordinator_log = await manager.server_open_log(server_id=coordinator.server_id)
        coordinator_log_mark = await coordinator_log.mark()
        s_info = await manager.server_add(expected_server_up_state=ServerUpState.PROCESS_STARTED)
        await coordinator_log.wait_for(
            pattern="topology_coordinator_pause_after_updating_cdc_generation: waiting",
            from_mark=coordinator_log_mark,
        )
        await manager.server_pause(server_id=s_info.server_id)
        await manager.api.message_injection(
            node_ip=coordinator.ip_addr,
            injection="topology_coordinator_pause_after_updating_cdc_generation",
        )
    else:
        s_info = await manager.server_add(
            config={"error_injections_at_startup": [{"name": error_injection, "one_shot": True}]},
            expected_server_up_state=ServerUpState.PROCESS_STARTED,
        )

    LOGGER.info("Wait till the bootstrapping node will be paused.")
    await manager.wait_for_scylla_process_status(
        server_id=s_info.server_id,
        expected_statuses=[
            psutil.STATUS_STOPPED,
        ],
        deadline=time.time() + 180,
    )

    LOGGER.info("Run the cluster event main step.")
    try:
        cluster_event_start = time.perf_counter()
        await anext(cluster_event_steps)
        cluster_event_duration = time.perf_counter() - cluster_event_start
        LOGGER.info("Cluster event `%s' took %.1fs", cluster_event.__name__, cluster_event_duration)
    finally:
        LOGGER.info("Unpause the server and wait till it wake up and become connectable using CQL.")
        await manager.server_unpause(server_id=s_info.server_id)

    server_log = await manager.server_open_log(server_id=s_info.server_id)

    if cluster_event_duration + 1 >= WAIT_FOR_IP_TIMEOUT and error_injection in ERROR_INJECTIONS_NODE_MAY_HANG:
        LOGGER.info("Expecting the added node can hang and we'll have a message in the coordinator's log.  See #18638.")
        coordinator = await get_coordinator_host(manager=manager)
        coordinator_log = await manager.server_open_log(server_id=coordinator.server_id)
        coordinator_log_pattern = r"The node may hang\. It's safe to shut it down manually now\."
        if matches := await server_log.grep(r"init - Setting local host id to (?P<hostid>[0-9a-f-]+)"):
            line, match = matches[-1]
            LOGGER.info("Found following message in the coordinator's log:\n\t%s", line)
            coordinator_log_pattern += (
                rf"|updating topology state: rollback {match.group('hostid')} after bootstrapping failure, moving"
                rf" transition state to left token ring and setting cleanup flag"
            )
        if matches := await coordinator_log.grep(coordinator_log_pattern):
            LOGGER.info("Found following message in the coordinator's log:\n\t%s", matches[-1][0])
            await manager.server_stop(server_id=s_info.server_id)

    if s_info in await manager.running_servers():
        LOGGER.info("Wait until the new node initialization completes or fails.")
        await server_log.wait_for("init - (Startup failed:|Scylla version .* initialization completed)", timeout=120)

        if await server_log.grep("init - Startup failed:"):
            LOGGER.info("Check that the new node is dead.")
            expected_statuses = [psutil.STATUS_DEAD]
        else:
            LOGGER.info("Check that the new node is running.")
            expected_statuses = [psutil.STATUS_RUNNING, psutil.STATUS_SLEEPING]

        scylla_process_status = await manager.wait_for_scylla_process_status(
            server_id=s_info.server_id,
            expected_statuses=expected_statuses,
        )
    else:
        scylla_process_status = psutil.STATUS_DEAD

    if scylla_process_status in (psutil.STATUS_RUNNING, psutil.STATUS_SLEEPING):
        LOGGER.info("The new node is running.  Check if we can connect to it.")

        async def driver_connect() -> bool | None:
            try:
                await manager.driver_connect(server=s_info)
            except NoHostAvailable:
                LOGGER.info("Driver not connected to %s yet", s_info.ip_addr)
                return None
            return True

        LOGGER.info("Check for the new node's health.")
        await wait_for(pred=driver_connect, deadline=time.time() + 90)
        await wait_for_cql_and_get_hosts(cql=manager.cql, servers=[s_info], deadline=time.time() + 30)
    else:
        if s_info in await manager.running_servers():
            LOGGER.info("The new node is dead.  Check if it failed to startup.")
            assert await server_log.grep("init - Startup failed:")
            await manager.server_stop(server_id=s_info.server_id)  # remove the node from the list of running servers

        LOGGER.info("Try to remove the dead new node from the cluster.")
        with suppress(Exception):
            await manager.remove_node(
                initiator_id=(await manager.running_servers())[0].server_id,
                server_id=s_info.server_id,
                wait_removed_dead=False,
                timeout=TOPOLOGY_TIMEOUT,
            )

    LOGGER.info("Check for the cluster's health.")
    await manager.driver_connect()
    await wait_for_token_ring_and_group0_consistency(manager=manager, deadline=time.time() + 90)
    await table.add_column()
    await random_tables.verify_schema()
    await anext(cluster_event_steps, None)  # use default value to suppress StopIteration
    await random_tables.verify_schema()
