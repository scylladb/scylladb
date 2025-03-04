#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
import logging
import pytest
import asyncio
import time

from cassandra import ConsistencyLevel  # type: ignore
from cassandra.query import SimpleStatement  # type: ignore
from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for_cql_and_get_hosts
from test.cluster.util import new_test_keyspace


logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_read_repair_with_conflicting_hash_keys(request: pytest.FixtureRequest, manager: ManagerClient) -> None:
    """
    Test that conflicting hash keys are handled correctly during read repair.
    Issue https://github.com/scylladb/scylladb/issues/19101

    1. Create a new cluster with 3 nodes.
    2. Create a keyspace and a table with replication factor = 3.
    3. Stop one of the nodes.
    4. Add 2 rows that have primary keys causing a hash collision.
    5. Start the offline node.
    6. Run a SELECT query with ALL consistency level, forcing reading from all 3 nodes.
       The node that's been offline will not have a value, causing a read repair.
       Since difference calculation logic is using a token for it's hashmap key and the
       token value is the same for both keys, this causes an incorrect diff calculation
       and propagation to the node that was offline.
    7. Run the same SELECT query with ALL consistency level, forcing reading from all 3 nodes.
       now there is also a conflict, since the node that was reset got an incorrect value as a
       result of and prev step read repair. This incorrect value is newer than others, thus it
       will be the result of reconciliation in case the diff calculation algorithm is using a
       token as a key.

    """
    logger.info("Creating a new cluster")
    srvs = await manager.servers_add(3)
    cql, _ = await manager.get_ready_cql(srvs)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3};") as ks:
        table = f"{ks}.t"
        await cql.run_async(f"CREATE TABLE {table} (pk bigint PRIMARY KEY, c int);")

        # Stop one of the nodes.
        await manager.server_stop_gracefully(srvs[0].server_id)

        # Add rows with partition kays that cause murmur3 hash collision, token value [6874760189787677834].
        pk1 = -4818441857111425024
        pk2 = -8686612841249112064
        await cql.run_async(SimpleStatement(f"INSERT INTO {table} (pk, c) VALUES ({pk1}, 111)", consistency_level=ConsistencyLevel.ONE))
        await cql.run_async(SimpleStatement(f"INSERT INTO {table} (pk, c) VALUES ({pk2}, 222)", consistency_level=ConsistencyLevel.ONE))

        # Start the offline node.
        await manager.server_start(srvs[0].server_id, wait_others=2)

        # Run a SELECT query with ALL consistency level, forcing reading from all 3 nodes.
        res = await cql.run_async(SimpleStatement(f"SELECT * FROM {table}", consistency_level=ConsistencyLevel.ALL))

        # Validate the results (should be OK).
        assert len(res) == 2
        for row in res:
            if (row.pk == pk1):
                assert row.c == 111
            elif (row.pk == pk2):
                assert row.c == 222

        res = await cql.run_async(SimpleStatement(f"SELECT * FROM {table}", consistency_level=ConsistencyLevel.ALL))

        # Validate the results (will be wrong in case the diff calculation hash map uses tokens as keys).
        assert len(res) == 2
        for row in res:
            if (row.pk == pk1):
                assert row.c == 111
            elif (row.pk == pk2):
                assert row.c == 222
