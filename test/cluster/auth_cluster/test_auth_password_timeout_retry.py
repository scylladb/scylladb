#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import pytest
import logging

from test.pylib.manager_client import ManagerClient
from test.cluster.auth_cluster import extra_scylla_config_options as auth_config


logger = logging.getLogger(__name__)


@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injection is disabled in release mode')
async def test_auth_password_timeout_retry(manager: ManagerClient) -> None:
    """Regression test for SCYLLADB-1371: raft timeout during default password
    creation should be retried by do_after_system_ready, not silently swallowed.

    Uses a one-shot fault injection to throw raft_operation_timeout_error in
    maybe_create_default_password() before the raft start_operation() call.

    With the bug: the timeout is caught in maybe_create_default_password_with_retries()
    and silently co_return'd, so do_after_system_ready considers the operation successful
    and never retries. The default password is never created.

    With the fix: the timeout is re-thrown, do_after_system_ready catches the failure
    and retries with exponential backoff. On the second attempt (injection is one-shot,
    already consumed), the password is created successfully.
    """
    config = {
        **auth_config,
        'authenticator': 'com.scylladb.auth.TransitionalAuthenticator',
        'error_injections_at_startup': [
            {'name': 'maybe_create_default_password_timeout', 'one_shot': True},
        ],
    }
    server = await manager.server_add(config=config)

    server_log = await manager.server_open_log(server.server_id)

    # Verify the injection fired — the raft timeout error was hit.
    matches = await server_log.grep("Failed to create default superuser password due to raft timeout, will retry")
    assert len(matches) > 0, (
        "Injection did not fire — expected raft timeout warning in log"
    )
    logger.info("Confirmed injection fired: %s", matches[0][0].strip())

    # The password should have been created on retry (do_after_system_ready retries on failure).
    # BUG (SCYLLADB-1371): the raft_operation_timeout_error is caught and co_return'd in
    # maybe_create_default_password_with_retries(), so do_after_system_ready never retries,
    # and this message never appears.
    matches = await server_log.grep("Created default superuser authentication record")
    assert len(matches) > 0, (
        "BUG SCYLLADB-1371: 'Created default superuser authentication record' was never logged. "
        "The raft_operation_timeout_error was silently swallowed in "
        "maybe_create_default_password_with_retries(), preventing do_after_system_ready "
        "from retrying."
    )

