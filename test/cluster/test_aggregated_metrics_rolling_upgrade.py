#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import logging
import pytest
import time

from cassandra.protocol import ConfigurationException

from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_aggregated_metrics_rolling_upgrade(manager: ScyllaClusterManager):
    """Verify that the per-table `aggregated_metrics` WITH-clause property is
    gated by the PER_TABLE_AGGREGATED_METRICS cluster feature during a
    simulated rolling upgrade, mirroring
    test_large_data_guardrails_rolling_upgrade in
    test_large_partition_guardrail.py:

    1. Start a 2-node cluster where one node suppresses the feature.
    2. Verify that CREATE TABLE WITH aggregated_metrics = ... is rejected
       (even on the upgraded node), because the cluster as a whole doesn't
       support the property yet.
    3. "Upgrade" the old node (remove suppress_features, restart).
    4. Verify that CREATE TABLE WITH aggregated_metrics = ... now succeeds.
    """
    cfg_old = {
        "error_injections_at_startup": [
            {"name": "suppress_features", "value": "PER_TABLE_AGGREGATED_METRICS"},
        ],
    }

    servers = []
    servers.append(await manager.server_add(config=cfg_old))   # "old" node
    servers.append(await manager.server_add())                 # "new" node

    # Connect to the "new" node — it supports the feature but the cluster
    # as a whole does not (because the old node doesn't advertise it).
    cql = await manager.get_cql_exclusive(servers[1])

    await cql.run_async(
        "CREATE KEYSPACE ks_aggregated_metrics_upgrade_test WITH REPLICATION = "
        "{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"
    )

    # Feature not yet cluster-wide — the property must be rejected.
    with pytest.raises(ConfigurationException, match="cannot be used until all nodes"):
        await cql.run_async(
            "CREATE TABLE ks_aggregated_metrics_upgrade_test.tbl1 "
            "(p int PRIMARY KEY) WITH aggregated_metrics = false"
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
                "CREATE TABLE ks_aggregated_metrics_upgrade_test.tbl1 "
                "(p int PRIMARY KEY) WITH aggregated_metrics = false"
            )
            success = True
        except ConfigurationException as e:
            assert "cannot be used until all nodes" in str(e)
            await asyncio.sleep(0.5)
    assert success, "Feature was not enabled cluster-wide within timeout"
