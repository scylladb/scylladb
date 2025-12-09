#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import inject_error
from test.pylib.util import wait_for_cql_and_get_hosts
from test.cluster.util import wait_for_cdc_generations_publishing

import time
import logging

logger = logging.getLogger(__name__)


async def test_group0_recovers_after_partial_command_application(manager: ManagerClient):
    """
    Reproducer for scylladb/scylladb#26945.

    This test simulates a situation where a node crashes after partially
    applying a group0 command. Because of the partial application, the tables
    managed by group0 are left in an inconsistent state. Specifically, we apply
    a mutation that deletes data of a CDC generation but does not apply
    a mutation that removes this CDC generation from the list of committed
    generations.

    The test verifies that group0 will recover from this by re-applying commands
    that could have been partially applied before reloading the in-memory state.
    Otherwise the CDC logic will trip on an on_internal_error check - it will
    notice a committed CDC generation without any data.
    """

    # This injection increases the leeway significantly, making sure that
    # the generation will not be removed until we enable another error injection
    # which tells the fiber to ignore the leeway.
    config = {'error_injections_at_startup': ['increase_cdc_generation_leeway']}

    logger.info("Setting up first node")
    srv1 = await manager.server_add(config=config)

    cql = manager.get_cql()
    [host1] = await wait_for_cql_and_get_hosts(cql, [srv1], time.time() + 60)

    logger.info("Waiting until the first generation is published")
    await wait_for_cdc_generations_publishing(cql, [host1], time.time() + 60)

    # This error injection will cause the generation published fiber to pause its
    # execution, after being woken up, at a specific point at the beginning of its
    # loop. Sending a message to this injection later allows us to control when
    # the generation cleaning up logic will be triggered.
    async with inject_error(manager.api, srv1.ip_addr, "cdc_generation_publisher_fiber") as handler:
        # This error injection will cause the fiber to disregard the generation leeway
        # while determining which generations can be removed, so the first generation
        # will be considered for removal next time the fiber wakes up.
        await manager.api.enable_injection(srv1.ip_addr, "clean_obsolete_cdc_generations_change_ts_ub", one_shot=False)

        # Adding a second node results in a second generation appearing for publishing.
        # This triggers the CDC generation publisher fiber, which stops
        # at the cdc_generation_publisher_fiber error injection.
        logger.info("Adding the second node")
        await manager.server_add(config=config)

        # Prepare the injection which causes the command which deletes a cdc generation
        # to be partially applied, i.e. only the generation data is deleted
        # from cdc_generations_v3 but the generation's ID is not removed
        # from system.topology.
        # Sending the message unstucks the CDC generation publisher fiber.
        await manager.api.enable_injection(srv1.ip_addr, "group0_simulate_partial_application_of_cdc_generation_deletion", one_shot=False)
        await handler.message()

        logger.info("Waiting until the cdc generation clearing command is partially applied")
        log = await manager.server_open_log(srv1.server_id)
        await log.wait_for("group0 has hung, waiting for abort")

        # Restart to get it unstuck
        logger.info("Kill and restart the node")
        await manager.server_stop(srv1.server_id)
        await manager.server_start(srv1.server_id)

        # Restarting should have succeeded, without the fix it would fail
        # on an assertion
        logger.info("Test succeeded, the node was successfully restarted")
