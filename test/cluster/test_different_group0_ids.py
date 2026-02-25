#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from test.pylib.manager_client import ManagerClient

import pytest


@pytest.mark.asyncio
async def test_different_group0_ids(manager: ManagerClient):
    """
    The test starts two single-node clusters (with different group0_ids). Node B (the
    node from the second cluster) is restarted with seeds containing node A (the node
    from the first cluster), and thus it tries to gossip node A. The test checks that
    node A rejects gossip_digest_syn.

    Note: this test relies on the fact that the only node in a single-node cluster
    always gossips with its seeds. This can be considered a bug, although a mild one.
    If we ever fix it, this test can be rewritten by starting a two-node cluster and
    recreating group0 on one of the nodes via the recovery procedure.
    """
    scylla_a = await manager.server_add()
    scylla_b = await manager.server_add(start=False)
    await manager.server_start(scylla_b.server_id, seeds=[scylla_b.ip_addr])

    id_b = await manager.get_host_id(scylla_b.server_id)

    await manager.server_stop(scylla_b.server_id)
    await manager.server_start(scylla_b.server_id, seeds=[scylla_a.ip_addr])

    # Since scylla_a and scylla_b have different group0 IDs and didn't join each other,
    # they are separate clusters. We need to set audit keyspace RF=0 on scylla_b (the node
    # being decommissioned) to prevent its audit replicas from interfering with the expected
    # "zero replica after the removal" error from the repair service.
    cql_b = await manager.get_cql_exclusive(scylla_b)
    result_b = await cql_b.run_async("SELECT * FROM system_schema.keyspaces WHERE keyspace_name = 'audit'")
    if result_b:
        await cql_b.run_async("DROP KEYSPACE audit")

    log_file_a = await manager.server_open_log(scylla_a.server_id)
    await log_file_a.wait_for(f'Group0Id mismatch from {id_b}', timeout=30)
