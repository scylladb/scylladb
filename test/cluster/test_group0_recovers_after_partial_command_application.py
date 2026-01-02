#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
import pytest

from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import inject_error
from test.cluster.conftest import skip_mode
from test.cluster.util import wait_for_cdc_generations_publishing

import time
import logging

logger = logging.getLogger(__name__)


@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
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

    cql, [host1] = await manager.get_ready_cql([srv1])

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
        await log.wait_for("group0 has hung on error injection, waiting for the process to be killed")

        # The error injection enters an infinite loop, so kill the process.
        logger.info("Kill the first node")
        await manager.server_stop(srv1.server_id)

        # Before restarting, enable an error injection which causes the state
        # machine to load the group0 state before (re)applying any group0
        # commands. This will simulate the behaviour without the fix
        # and will crash the node on a failed on_internal_error check.
        #
        # This is done to ensure that the test is being useful - it depends
        # on implementation details of CDC generation publishing, specifically
        # that the check is there and will fail if we have a regression. If the
        # check is removed, the test will need to be rewritten to rely on some
        # other check in group0.
        logger.info("Restart the first node with an error injection that simulates the bug, expecting a crash")
        config["error_injections_at_startup"].append("group0_enable_sm_immediately")
        cdc_generation_not_present_error = "data for CDC generation [a-f0-9\\-]* not present"
        manager.ignore_cores_log_patterns.extend(cdc_generation_not_present_error)
        await manager.server_update_config(srv1.server_id, config_options=config)
        await manager.server_start(srv1.server_id, expected_error=cdc_generation_not_present_error)

        # Now, restart the node normally
        logger.info("Restart the first node normally, expecting everything to be fine")
        config["error_injections_at_startup"].remove("group0_enable_sm_immediately")
        await manager.server_update_config(srv1.server_id, config_options=config)
        await manager.server_start(srv1.server_id)

        # Restarting should have succeeded, without the fix it would fail
        # on an assertion
        logger.info("Test succeeded, the node was successfully restarted")
