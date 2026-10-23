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
async def test_audit_write_timeout_follows_write_request_timeout(manager: ManagerClient):
    """An audit write gets as long as a client write before it times out (SCYLLADB-5625)."""
    server = await manager.server_add(config={
        "audit": "table",
        "audit_categories": "DCL,DDL,AUTH,ADMIN",
        "authenticator": "AllowAllAuthenticator",
        "authorizer": "AllowAllAuthorizer",
        "write_request_timeout_in_ms": 20000,
    })
    cql, _ = await manager.get_ready_cql([server])

    async def write_timeouts() -> float:
        metrics = await manager.metrics.query(server.ip_addr)
        return metrics.get("scylla_storage_proxy_coordinator_write_timeouts") or 0

    timeouts_before = await write_timeouts()

    await manager.api.enable_injection(server.ip_addr, "database_apply", one_shot=False,
                                       parameters={"ks_name": "audit", "cf_name": "audit_log", "what": "wait"})
    try:
        statement = cql.run_async("LIST ALL SERVICE LEVELS")
        logger.info("Holding the audit write past the 5s audit write timeout floor")
        await asyncio.sleep(6)
        # the statement can't finish while the injection holds its audit write
        assert not statement.done()
    finally:
        await manager.api.disable_injection(server.ip_addr, "database_apply")
    await statement
    assert await write_timeouts() == timeouts_before
