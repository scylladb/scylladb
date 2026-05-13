#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import logging
import pytest
import time
import asyncio

from cassandra import WriteFailure
from cassandra.protocol import ConfigurationException

from test.pylib.manager_client import ManagerClient

logger = logging.getLogger(__name__)

REJECT_MSG_RE = "(?i)Write rejected.*(partition size|partition row count)"


async def _make_oversized_partition(cql, tbl, pk, num_rows, value_size_bytes):
    """Insert `num_rows` rows of `value_size_bytes` blobs under partition key `pk`."""
    insert = cql.prepare(f"INSERT INTO {tbl} (pk, ck, v) VALUES (?, ?, ?)")
    for ck in range(num_rows):
        await cql.run_async(insert, [pk, ck, bytes(value_size_bytes)])


@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_large_data_guardrails_rolling_upgrade(manager: ManagerClient):
    """Verify that large_data_guardrails_enabled is properly gated by the
    LARGE_DATA_GUARDRAILS cluster feature during a simulated rolling upgrade:

    1. Start a 2-node cluster where one node suppresses the feature.
    2. Verify that CREATE TABLE WITH large_data_guardrails_enabled = true
       is rejected (even on the upgraded node).
    3. "Upgrade" the old node (remove suppress_features, restart).
    4. Verify that CREATE TABLE WITH large_data_guardrails_enabled = true
       now succeeds.
    """
    cfg_base = {
        "compaction_large_partition_warning_threshold_mb": 1,
        "large_partition_fail_threshold_mb": 1,
    }
    cfg_old = {
        **cfg_base,
        "error_injections_at_startup": [
            {"name": "suppress_features", "value": "LARGE_DATA_GUARDRAILS"},
        ],
    }

    servers = []
    servers.append(await manager.server_add(config=cfg_old))   # "old" node
    servers.append(await manager.server_add(config=cfg_base))  # "new" node

    # Connect to the "new" node — it supports the feature but the cluster
    # as a whole does not (because the old node doesn't advertise it).
    cql = await manager.get_cql_exclusive(servers[1])

    await cql.run_async(
        "CREATE KEYSPACE ks_upgrade_test WITH REPLICATION = "
        "{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"
    )

    # Feature not yet cluster-wide — enabling guardrails must be rejected.
    with pytest.raises(ConfigurationException, match="cannot be used until all nodes"):
        await cql.run_async(
            "CREATE TABLE ks_upgrade_test.tbl1 "
            "(pk int, ck int, v blob, PRIMARY KEY (pk, ck)) "
            "WITH large_data_guardrails_enabled = true"
        )

    # "Upgrade" the old node: remove the injection, restart.
    await manager.server_stop_gracefully(servers[0].server_id)
    await manager.server_remove_config_option(servers[0].server_id, "error_injections_at_startup")
    await manager.server_start(servers[0].server_id)

    # Wait for the upgraded node to realize the feature is now cluster-wide.
    timeout = time.time() + 60
    success = False
    while not success and time.time() < timeout:
        try:
            await cql.run_async(
                "CREATE TABLE ks_upgrade_test.tbl1 "
                "(pk int, ck int, v blob, PRIMARY KEY (pk, ck)) "
                "WITH large_data_guardrails_enabled = true"
            )
            success = True
        except ConfigurationException as e:
            assert "cannot be used until all nodes" in str(e)
            await asyncio.sleep(0.5)
    assert success, "Feature was not enabled cluster-wide within timeout"

    # Verify the guardrail actually works on the newly-created table.
    await _make_oversized_partition(cql, "ks_upgrade_test.tbl1", pk=1, num_rows=2, value_size_bytes=600 * 1024)
    # Flush on both servers — with RF=1 we don't know which owns pk=1.
    for srv in servers:
        await manager.api.keyspace_flush(srv.ip_addr, "ks_upgrade_test", "tbl1")
    insert = cql.prepare("INSERT INTO ks_upgrade_test.tbl1 (pk, ck, v) VALUES (?, ?, ?)")
    with pytest.raises(WriteFailure, match=REJECT_MSG_RE):
        await cql.run_async(insert, [1, 99, b"\x00"])
