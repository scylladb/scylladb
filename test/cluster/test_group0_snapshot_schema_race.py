#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import logging
import pytest

from test.pylib.manager_client import ManagerClient

logger = logging.getLogger(__name__)


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_snapshot_transfer_before_enable_sm_causes_no_such_keyspace(manager: ManagerClient) -> None:
    """Verify that a joining node handles schema correctly when transfer_snapshot()
    runs before enable_in_memory_state_machine().

    Reproduces a race condition where:
    1. Raft server starts on the joining node (_in_memory_state_machine_enabled = false)
    2. Leader sends install_snapshot, triggering transfer_snapshot()
    3. transfer_snapshot() acquires _read_apply_mutex before enable_in_memory_state_machine
    4. Since the flag is false, schema is written to system_schema tables via
       write_mutations_to_database() but NOT loaded into memory
    5. enable_in_memory_state_machine() sets flag=true, calls reload_state() which
       does NOT reload schema
    6. Next Raft apply() with flag=true tries to ALTER system_distributed via
       merge_schema_from() -> merge_keyspaces() -> prepare_update_keyspace_on_all_shards()
       -> find_keyspace() which fails because the keyspace is on disk but not in
       the in-memory _keyspaces map
    """
    # Start node 1 -- it creates system_distributed during its normal startup
    srv1 = await manager.server_add()
    logger.info("Leader node %s started", srv1.server_id)

    # Create node 2 with injection that will delay enable_in_memory_state_machine.
    # This forces transfer_snapshot() to run first with the flag=false.
    injection = "delay_before_enable_in_memory_state_machine"
    srv2 = await manager.server_add(start=False,
                                    config={'error_injections_at_startup': [injection]})
    logger.info("Created node %s, starting with injection", srv2.server_id)

    # Start node 2 as a background task.
    # We expect it to join successfully (after the fix).
    # Without the fix it will crash with no_such_keyspace.
    task = asyncio.create_task(manager.server_start(srv2.server_id))

    # Open node 2's log and wait for the injection to fire
    log = await manager.server_open_log(srv2.server_id)
    await log.wait_for(f"{injection}: waiting for message", timeout=60)
    logger.info("Node %s hit the injection, transfer_snapshot should run now", srv2.server_id)

    # Wait for transfer_snapshot to complete. The INFO-level log message
    # "transfer snapshot from <id> index <N> snp id <id> completed" is emitted
    # at the very end of transfer_snapshot() after all data (schema, topology,
    # auth) has been persisted locally.
    await log.wait_for("transfer snapshot from .* completed", timeout=60)
    logger.info("transfer_snapshot completed on node %s, releasing injection", srv2.server_id)

    # Release the injection -- enable_in_memory_state_machine() will now proceed
    await manager.api.message_injection(srv2.ip_addr, injection)

    # Wait for node 2 to finish joining.
    # Without the fix: crashes with "no_such_keyspace (Can't find a keyspace system_distributed)"
    # With the fix: joins successfully
    await task
    logger.info("Node %s joined successfully", srv2.server_id)
