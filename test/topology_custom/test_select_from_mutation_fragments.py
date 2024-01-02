#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file configures pytest for all tests in this directory, and also

import pytest

from cassandra.protocol import InvalidRequest  # type: ignore
from cassandra.query import SimpleStatement  # type: ignore

from test.pylib.manager_client import ManagerClient


@pytest.mark.asyncio
async def test_sticky_coordinator_enforced(manager: ManagerClient) -> None:
    await manager.servers_add(2, cmdline=['--logger-log-level', 'paging=trace'])

    cql = manager.get_cql()

    await cql.run_async("create keyspace ks"
                        " with replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}")
    await cql.run_async("create table ks.tbl (pk int, ck int, v int, primary key (pk, ck))")

    num_rows = 43
    expected_num_rows = num_rows + 2  # rows + partition-start + partitione-end
    for ck in range(0, num_rows):
        await cql.run_async(f"INSERT INTO ks.tbl (pk, ck, v) VALUES (0, {ck}, 100)")

    unpaged_res = await cql.run_async("SELECT * FROM MUTATION_FRAGMENTS(ks.tbl) WHERE pk = 0")
    assert len(unpaged_res) == expected_num_rows

    read_stmt = SimpleStatement("SELECT * FROM MUTATION_FRAGMENTS(ks.tbl) WHERE pk = 0", fetch_size=10)

    # The default round-robin load-balancing policy will jump between the nodes.
    # This should trigger an exception.
    with pytest.raises(
            InvalidRequest,
            match="Moving between coordinators is not allowed in SELECT FROM MUTATION_FRAGMENTS\\(\\) statements.*"):
        await cql.run_async(read_stmt, all_pages=True)
