#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import time

import pytest

from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for, wait_for_view


@pytest.mark.parametrize("flush", [False, True], ids=["without_flush", "with_flush"])
@pytest.mark.parametrize("tablets", [False, True], ids=["vnodes", "tablets"])
async def test_no_base_column_in_view_pk_complex_timestamp(manager: ScyllaClusterManager, flush: bool, tablets: bool) -> None:
    """Check view-row liveness when no non-key base column is in the view key.

    A selected or unselected live base cell must keep the view row alive, while
    deleting the last live base cell must remove it. This is a regression test
    for CASSANDRA-11500.
    """
    # This test exercises MV timestamp handling rather than topology changes, so
    # one node and, when enabled, one tablet are sufficient.
    server = await manager.server_add()
    cql = manager.get_cql()
    tablets_options = "{'initial': 1}" if tablets else "{'enabled': false}"
    keyspace = "ks"
    table = f"{keyspace}.base"
    view = f"{keyspace}.mv"

    await cql.run_async(
        f"CREATE KEYSPACE {keyspace} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} "
        f"AND tablets = {tablets_options}")
    await cql.run_async(f"CREATE TABLE {table} (k int, c int, a int, b int, e int, f int, PRIMARY KEY (k, c))")
    await cql.run_async(f"CREATE MATERIALIZED VIEW {view} AS SELECT k, c, a, b FROM {table} "
        "WHERE k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (c, k)")
    await wait_for_view(cql, "mv", 1)

    base_query = f"SELECT k, c, a, b, e, f FROM {table}"
    view_query = f"SELECT k, c, a, b FROM {view}"
    BASE_ROW_WITH_ONLY_A = (1, 1, 1, None, None, None)
    BASE_ROW_WITH_ONLY_B = (1, 1, None, 1, None, None)
    BASE_ROW_WITH_ONLY_E = (1, 1, None, None, 1, None)
    BASE_ROW_WITH_ONLY_F = (1, 1, None, None, None, 1)
    BASE_ROW_WITH_A_AND_F = (1, 1, 1, None, None, 1)
    BASE_ROW_WITH_ONLY_ROW_LIVENESS = (1, 1, None, None, None, None)
    VIEW_ROW_WITH_ONLY_A = (1, 1, 1, None)
    VIEW_ROW_WITH_ONLY_B = (1, 1, None, 1)
    VIEW_ROW_WITH_NULL_A_AND_B = (1, 1, None, None)
    EMPTY: list[tuple] = []

    async def assert_state(expected_base: list[tuple], expected_view: list[tuple]) -> None:
        async def rows_match() -> bool:
            base_rows = [tuple(row) for row in await cql.run_async(base_query)]
            view_rows = [tuple(row) for row in await cql.run_async(view_query)]
            assert base_rows == expected_base, f"unexpected base rows: {base_rows}"
            assert view_rows == expected_view, f"unexpected view rows: {view_rows}"
            return True

        await wait_for(rows_match, time.time() + 60, label="base_and_view_state")

    async def update_and_assert(query: str, expected_base: list[tuple], expected_view: list[tuple]) -> None:
        await cql.run_async(query)
        if flush:
            await manager.api.keyspace_flush(server.ip_addr, keyspace)
        await assert_state(expected_base, expected_view)

    # An unselected column keeps both the base and view rows alive.
    await update_and_assert(
        f"UPDATE {table} USING TIMESTAMP 1 SET e = 1 WHERE k = 1 AND c = 1",
        [BASE_ROW_WITH_ONLY_E], [VIEW_ROW_WITH_NULL_A_AND_B])

    # Replace the live unselected cell with a selected one.
    await update_and_assert(
        f"UPDATE {table} USING TIMESTAMP 2 SET e = null, b = 1 WHERE k = 1 AND c = 1",
        [BASE_ROW_WITH_ONLY_B], [VIEW_ROW_WITH_ONLY_B])

    # Removing the last live cell removes both rows.
    await update_and_assert(f"UPDATE {table} USING TIMESTAMP 2 SET e = null, b = null WHERE k = 1 AND c = 1",
        EMPTY, EMPTY)

    # Another unselected cell recreates the view row.
    await update_and_assert(f"UPDATE {table} USING TIMESTAMP 3 SET f = 1 WHERE k = 1 AND c = 1",
        [BASE_ROW_WITH_ONLY_F], [VIEW_ROW_WITH_NULL_A_AND_B])

    # Add row liveness at the same timestamp as the cell.
    await update_and_assert(f"INSERT INTO {table} (k, c) VALUES (1, 1) USING TIMESTAMP 3",
        [BASE_ROW_WITH_ONLY_F], [VIEW_ROW_WITH_NULL_A_AND_B])

    # Row liveness keeps the row alive after the cell is removed.
    await update_and_assert(
        f"UPDATE {table} USING TIMESTAMP 3 SET f = null WHERE k = 1 AND c = 1",
        [BASE_ROW_WITH_ONLY_ROW_LIVENESS], [VIEW_ROW_WITH_NULL_A_AND_B])

    await update_and_assert(
        f"UPDATE {table} USING TIMESTAMP 3 SET a = 1 WHERE k = 1 AND c = 1",
        [BASE_ROW_WITH_ONLY_A], [VIEW_ROW_WITH_ONLY_A])

    await update_and_assert(
        f"UPDATE {table} USING TIMESTAMP 4 SET f = 1 WHERE k = 1 AND c = 1",
        [BASE_ROW_WITH_A_AND_F], [VIEW_ROW_WITH_ONLY_A])

    # The newer unselected cell survives this row deletion.
    await update_and_assert(
        f"DELETE FROM {table} USING TIMESTAMP 3 WHERE k = 1 AND c = 1",
        [BASE_ROW_WITH_ONLY_F], [VIEW_ROW_WITH_NULL_A_AND_B])

    await update_and_assert(
        f"UPDATE {table} USING TIMESTAMP 4 SET f = null WHERE k = 1 AND c = 1",
        EMPTY, EMPTY)

    await update_and_assert(
        f"UPDATE {table} USING TIMESTAMP 7 SET b = 1 WHERE k = 1 AND c = 1",
        [BASE_ROW_WITH_ONLY_B], [VIEW_ROW_WITH_ONLY_B])

    await update_and_assert(
        f"UPDATE {table} USING TIMESTAMP 7 SET b = null WHERE k = 1 AND c = 1",
        EMPTY, EMPTY)

    # Timestamps of different selected columns do not shadow one another.
    await update_and_assert(
        f"UPDATE {table} USING TIMESTAMP 5 SET a = 1 WHERE k = 1 AND c = 1",
        [BASE_ROW_WITH_ONLY_A], [VIEW_ROW_WITH_ONLY_A])

    await update_and_assert(
        f"UPDATE {table} USING TTL 10 SET a = 1 WHERE k = 1 AND c = 1",
        [BASE_ROW_WITH_ONLY_A], [VIEW_ROW_WITH_ONLY_A])
    
    await assert_state(EMPTY, EMPTY)

    # An unselected expiring cell can recreate the view row too.
    await update_and_assert(
        f"UPDATE {table} USING TTL 10 SET f = 1 WHERE k = 1 AND c = 1",
        [BASE_ROW_WITH_ONLY_F], [VIEW_ROW_WITH_NULL_A_AND_B])
    
    await assert_state(EMPTY, EMPTY)
    await cql.run_async(f"DROP KEYSPACE {keyspace}")
