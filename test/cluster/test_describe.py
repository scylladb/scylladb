#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import asyncio
import pytest
from test.cluster.util import new_test_keyspace, new_test_table
from test.pylib.manager_client import ManagerClient

# The following test verifies that Scylla avoids making an oversized allocation
# when generating a large create statement when performing a DESCRIBE statement.
# The threshold for generating a warning about an oversized allocation is set
# to 128 * 2^10 bytes.
#
# Reproducer for issue scylladb/scylladb#24018.
@pytest.mark.asyncio
async def test_large_create_statement(manager: ManagerClient):
    cmdline = ["--logger-log-level", "describe=trace"]
    srv = await manager.server_add(cmdline=cmdline)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        async with new_test_table(manager, ks, "p int PRIMARY KEY") as table:
            # CQL will not accept identifiers longers than ~2^16.
            col_name_len = 60_000
            # An oversized allocation warning is issued for allocations bigger than 128 * 2^10.
            target_size_threshold = 128 * (2 ** 10)

            async def add_and_drop(col_name: str) -> None:
                await cql.run_async(f"ALTER TABLE {table} ADD {col_name} int")
                await cql.run_async(f"ALTER TABLE {table} DROP {col_name}")

            # Let's get ourselves a little bit more room with the size just
            # to make sure an oversized allocation will be triggered.
            col_count = 2 * (target_size_threshold // col_name_len) + 1
            col_name_prefix = "a" * col_name_len

            await asyncio.gather(*[add_and_drop(f"{col_name_prefix}{idx}") for idx in range(col_count)])

            log = await manager.server_open_log(srv.server_id)
            marker = await log.mark()

            await cql.run_async("DESCRIBE SCHEMA WITH INTERNALS")

            matches = await log.grep("oversized allocation", from_mark=marker)
            assert len(matches) == 0
