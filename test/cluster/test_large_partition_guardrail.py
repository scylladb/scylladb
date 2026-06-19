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
from cassandra.cluster import ConsistencyLevel
from cassandra.protocol import ConfigurationException
from cassandra.query import SimpleStatement

from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for_cql_and_get_hosts

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


@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_read_repair_skips_large_data_guardrails(manager: ManagerClient):
    """Read repair must bypass large-data guardrails.

    The data already exists on at least one replica; rejecting the
    read-repair write would leave replicas permanently inconsistent,
    which is worse than having an oversized partition.

    Scenario:
    1. Build a partition that exceeds the fail threshold on both replicas.
    2. Verify that a normal write to that partition is rejected.
    3. Use error injection to make one node miss an additional write,
       creating a digest mismatch.
    4. Read at CL=ALL — triggers read repair.  The repair write must
       succeed even though the partition already exceeds the guardrail.
    """
    cfg = {
        "compaction_large_partition_warning_threshold_mb": 1,
        "large_partition_fail_threshold_mb": 1,
    }
    cmdline = ["--hinted-handoff-enabled", "0"]

    nodes = await manager.servers_add(2, cmdline=cmdline, config=cfg, auto_rack_dc="dc1")
    node1, node2 = nodes

    cql = manager.get_cql()
    await wait_for_cql_and_get_hosts(cql, nodes, time.time() + 60)

    ks = "ks_rr_guardrail"
    await cql.run_async(
        f"CREATE KEYSPACE {ks} WITH REPLICATION = "
        f"{{'class': 'NetworkTopologyStrategy', 'replication_factor': 2}}"
    )
    await cql.run_async(
        f"CREATE TABLE {ks}.tbl (pk int, ck int, v blob, PRIMARY KEY (pk, ck)) "
        f"WITH large_data_guardrails_enabled = true"
    )

    # Build an oversized partition (> 1 MB) on both replicas.
    await _make_oversized_partition(cql, f"{ks}.tbl", pk=0, num_rows=2, value_size_bytes=600 * 1024)

    # Before flushing (guardrails not yet active), create divergence:
    # prevent node1 from receiving one extra write.
    await manager.api.enable_injection(
        node1.ip_addr, "database_apply", one_shot=False,
        parameters={"ks_name": ks, "cf_name": "tbl", "what": "throw"})

    insert_one = cql.prepare(f"INSERT INTO {ks}.tbl (pk, ck, v) VALUES (?, ?, ?)")
    insert_one.consistency_level = ConsistencyLevel.ONE
    await cql.run_async(insert_one, [0, 100, b"\x01"])

    await manager.api.disable_injection(node1.ip_addr, "database_apply")

    # Now flush both nodes — this makes the guardrail aware of the
    # oversized partition and activates rejection on subsequent writes.
    for node in nodes:
        await manager.api.keyspace_flush(node.ip_addr, ks, "tbl")

    # Verify that normal writes to this partition are now rejected.
    insert = cql.prepare(f"INSERT INTO {ks}.tbl (pk, ck, v) VALUES (?, ?, ?)")
    with pytest.raises(WriteFailure, match=REJECT_MSG_RE):
        await cql.run_async(insert, [0, 99, b"\x00"])

    # CL=ALL read triggers a digest mismatch and read repair.
    # The read-repair write to node1 must succeed despite the partition
    # exceeding the guardrail — otherwise this would raise WriteFailure.
    rows = cql.execute(SimpleStatement(
        f"SELECT ck FROM {ks}.tbl WHERE pk = 0",
        consistency_level=ConsistencyLevel.ALL))
    cks = {row.ck for row in rows}
    assert 100 in cks, "Read-repair row (ck=100) missing from CL=ALL result"
