# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
import asyncio
import pytest
from itertools import zip_longest

from cassandra.query import SimpleStatement, ConsistencyLevel
from test.pylib.manager_client import ManagerClient
from test.topology.conftest import skip_mode


def verify_data(response, expected_data):
    try:
        for row, (pk, ck1, ck2) in zip_longest(response, expected_data):
            assert row.pk == pk and row.ck1 == ck1 and row.ck2 == ck2
    except (AttributeError, TypeError):
        pytest.fail("Length of response and expected data mismatch")


@pytest.mark.asyncio
@skip_mode("release", "error injections are not supported in release mode")
async def test_reversed_queries_during_upgrade(manager: ManagerClient) -> None:
    """
    Use `suppress_features` error injection to simulate cluster upgrade process
    in order to test both native and legacy reversed formats.
    """
    cmdline = ["--hinted-handoff-enabled", "0"]
    node1, _ = await manager.servers_add(2, cmdline)

    cql = manager.get_cql()

    await cql.run_async("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}")
    await cql.run_async("CREATE TABLE ks.test (pk int, ck1 int, ck2 int, PRIMARY KEY (pk, ck1, ck2));")

    await asyncio.gather(*[cql.run_async(f"INSERT INTO ks.test (pk, ck1, ck2) VALUES ({k % 10}, {k % 3}, {k});") for k in range(100)])

    native_reverse_format_config = []
    legacy_reverse_format_config = [
        {"name": "suppress_features", "value": ["NATIVE_REVERSE_QUERIES"]}
    ]

    queries = [
        (SimpleStatement("SELECT * FROM ks.test WHERE pk = 6 ORDER BY ck1 DESC, ck2 DESC BYPASS CACHE;", consistency_level=ConsistencyLevel.ALL),
         ((6, 2, 86), (6, 2, 56), (6, 2, 26), (6, 1, 76), (6, 1, 46), (6, 1, 16), (6, 0, 96), (6, 0, 66), (6, 0, 36), (6, 0, 6))),
        (SimpleStatement("SELECT * FROM ks.test WHERE pk = 6 AND ck1 < 2 ORDER BY ck1 DESC, ck2 DESC BYPASS CACHE;", consistency_level=ConsistencyLevel.ALL),
         ((6, 1, 76), (6, 1, 46), (6, 1, 16), (6, 0, 96), (6, 0, 66), (6, 0, 36), (6, 0, 6))),
        (SimpleStatement("SELECT * FROM ks.test WHERE pk = 6 AND ck1 = 0 AND ck2 > 10 AND ck2 < 80 ORDER BY ck1 DESC, ck2 DESC BYPASS CACHE;", consistency_level=ConsistencyLevel.ALL),
         ((6, 0, 66), (6, 0, 36))),
        (SimpleStatement("SELECT * FROM ks.test WHERE pk = 6 AND (ck1, ck2) >= (1, 46) ORDER BY ck1 DESC, ck2 DESC BYPASS CACHE;", consistency_level=ConsistencyLevel.ALL),
         ((6, 2, 86), (6, 2, 56), (6, 2, 26), (6, 1, 76), (6, 1, 46))),
        (SimpleStatement("SELECT * FROM ks.test WHERE pk IN (5, 6) AND (ck1, ck2) >= (1, 55) ORDER BY ck1 DESC, ck2 DESC BYPASS CACHE;", consistency_level=ConsistencyLevel.ALL, fetch_size=0),
         ((5, 2, 95), (6, 2, 86), (5, 2, 65), (6, 2, 56), (5, 2, 35), (6, 2, 26), (5, 2, 5), (5, 1, 85), (6, 1, 76), (5, 1, 55))),
    ]

    for config in [legacy_reverse_format_config, native_reverse_format_config]:
        await manager.server_stop_gracefully(node1.server_id)
        await manager.server_update_config(
            node1.server_id, "error_injections_at_startup", config
        )
        await manager.server_start(node1.server_id)

        for query, expected_data in queries:
            await manager.server_stop_gracefully(node1.server_id)
            await manager.server_wipe_sstables(node1.server_id, "ks", "test")
            await manager.server_start(node1.server_id)

            verify_data(cql.execute(query), expected_data)
