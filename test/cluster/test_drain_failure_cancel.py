#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import logging
import time

import pytest

from test.cluster.util import new_test_keyspace
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_decommission_blocked_by_a_rack_list_fails_promptly(manager: ScyllaClusterManager):
    """
    Decommissioning the last node of a rack that a rack-list keyspace still names cannot
    drain that node: no other node in the rack can take its replicas. The balancer reports
    a drain failure and the coordinator cancels the leave request with that reason.

    Depending on which tablet the balancer examines first the reason is either "Unable to
    find new replica for tablet ... when draining" or "No candidate nodes in dc1/r3 to
    drain"; both carry the same advice, which is what the test matches on.

    The cancellation used to be dropped: it was written into the update collector without
    bumping its change counter, so a plan consisting of drain failures alone looked like
    "nothing to commit" and the request stayed pending until some unrelated migration
    happened to flush the collector, minutes later or never. The request has to fail
    promptly, with the drain failure as the reason.

    rf_rack_valid_keyspaces is off so that the up-front RF-rack validity check does not
    reject the decommission before the drain is even attempted; that check is what the
    harness default would exercise instead, and production runs with it off.
    """
    cfg = {"rf_rack_valid_keyspaces": False}
    servers = await manager.servers_add(4, config=cfg, property_file=[
        {"dc": "dc1", "rack": "r1"},
        {"dc": "dc1", "rack": "r1"},
        {"dc": "dc1", "rack": "r2"},
        {"dc": "dc1", "rack": "r3"},
    ])
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy',"
                                          " 'dc1': ['r1', 'r2', 'r3']} AND tablets = {'initial': 4}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY)")

        # No other topology work may be in flight: a concurrent split or auto-RF
        # update in the same balancing pass would flush the collector and hide a
        # dropped cancellation, so the test would pass on the unfixed code.
        await manager.api.quiesce_topology(servers[0].ip_addr)

        last_in_r3 = next(s for s in servers if s.rack == "r3")
        logger.info(f"Decommissioning {last_in_r3}, the only node in r3, while {ks} still lists r3")
        start = time.time()
        await manager.decommission_node(last_in_r3.server_id,
                                        expected_error="Consider adding new nodes or reducing replication factor",
                                        timeout=300)
        elapsed = time.time() - start
        logger.info(f"Decommission was rejected after {elapsed:.1f}s")
        # The drain failure is known on the coordinator's first balancing pass. Anything
        # close to the timeout means the cancellation was lost again.
        assert elapsed < 120, f"the drain failure took {elapsed:.0f}s to cancel the decommission"

        # The node is still a normal member and its keyspace still lists r3.
        running = {s.server_id for s in await manager.running_servers()}
        assert last_in_r3.server_id in running
