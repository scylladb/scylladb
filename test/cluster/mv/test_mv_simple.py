#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import asyncio
import logging
import pytest

from cassandra import ConsistencyLevel
from cassandra.cluster import Session as CassandraSession
from cassandra.protocol import InvalidRequest

from test.pylib.manager_client import ManagerClient
from test.cluster.test_hints import await_sync_point, create_sync_point

logger = logging.getLogger(__name__)

@pytest.mark.asyncio
@pytest.mark.parametrize("schema_kind", ["view", "index"])
# Views no longer depend on the experimental feature `views-with-tablets`,
# but let's keep these test cases to make sure it's really not needed anymore.
@pytest.mark.parametrize("views_with_tablets", [False, True])
@pytest.mark.parametrize("rf_rack_valid_keyspaces", [False, True])
async def test_mv_and_index_restrictions_in_tablet_keyspaces(manager: ManagerClient, schema_kind: str,
                                                             views_with_tablets: bool, rf_rack_valid_keyspaces: bool):
    """
    Verify that creating a materialized view or a secondary index in a tablet-based keyspace
    is only possible when both the configuration option `rf_rack_valid_keyspaces` is enabled.
    """

    async def create_mv_or_index(cql: CassandraSession):
        if schema_kind == "view":
            await cql.run_async("CREATE MATERIALIZED VIEW ks.mv "
                                "AS SELECT * FROM ks.t "
                                "WHERE p IS NOT NULL AND v IS NOT NULL "
                                "PRIMARY KEY (v, p)")
        elif schema_kind == "index":
            await cql.run_async("CREATE INDEX myindex ON ks.t(v)")
        else:
            assert False, "Unknown schema kind"

    async def try_pass(cql: CassandraSession):
        try:
            await cql.run_async(f"CREATE KEYSPACE ks WITH replication = "
                                 "{'class': 'NetworkTopologyStrategy', 'replication_factor': 1} "
                                 "AND tablets = {'enabled': true}")
            await cql.run_async(f"CREATE TABLE ks.t (p int PRIMARY KEY, v int)")
            await create_mv_or_index(cql)
        finally:
            await cql.run_async(f"DROP KEYSPACE IF EXISTS ks")

    async def try_fail(cql: CassandraSession):
        err = "Materialized views and secondary indexes are not supported on base tables with tablets. " \
              "To be able to use them, enable the configuration option `rf_rack_valid_keyspaces` and " \
              "make sure that the cluster feature `VIEWS_WITH_TABLETS` is enabled."
        with pytest.raises(InvalidRequest, match=err):
            await try_pass(cql)

    feature = ["views-with-tablets"] if views_with_tablets else []
    config = {"experimental_features": feature, "rf_rack_valid_keyspaces": rf_rack_valid_keyspaces}

    srv = await manager.server_add(config=config)

    # Necessary because we're restarting the node multiple times.
    cql, _ = await manager.get_ready_cql([srv])
    logger.debug("Obtained CassandraSession object")

    # We just want to validate the statements. We don't need to wait.
    assert hasattr(cql.cluster, "max_schema_agreement_wait")
    cql.cluster.max_schema_agreement_wait = 0
    logger.debug("Set max_schema_agreement_wait to 0")

    if rf_rack_valid_keyspaces:
        await try_pass(cql)
        logger.debug("try_pass finished successfully")
    else:
        await try_fail(cql)
        logger.debug("try_fail finished successfully")

@pytest.mark.asyncio
@pytest.mark.parametrize("view_type", ["view", "index"])
async def test_view_startup(manager: ManagerClient, view_type: str):
    """
    Verify that starting a node with materialized views in a tablet-based
    keyspace when the configuration option `rf_rack_valid_keyspaces` is disabled
    leads to a warning.
    """

    srv = await manager.server_add(config={"rf_rack_valid_keyspaces": True})
    cql = manager.get_cql()

    await cql.run_async("CREATE KEYSPACE ks WITH replication = "
                        "{'class': 'NetworkTopologyStrategy', 'replication_factor': 1} "
                        "AND tablets = {'enabled': true}")
    await cql.run_async("CREATE TABLE ks.t (p int PRIMARY KEY, v int)")

    if view_type == "view":
        await cql.run_async("CREATE MATERIALIZED VIEW ks.mv "
                            "AS SELECT * FROM ks.t "
                            "WHERE p IS NOT NULL AND v IS NOT NULL "
                            "PRIMARY KEY (v, p)")
    elif view_type == "index":
        await cql.run_async("CREATE INDEX i ON ks.t(v)")
    else:
        logger.error(f"Unexpected view type: {view_type}")
        assert False

    await manager.server_stop(srv.server_id)
    await manager.server_update_config(srv.server_id, "rf_rack_valid_keyspaces", False)

    log = await manager.server_open_log(srv.server_id)
    mark = await log.mark()

    start_task = asyncio.create_task(manager.server_start(srv.server_id))
    err = "Some of the existing keyspaces violate the requirements for using materialized " \
          "views or secondary indexes. Those features require enabling the configuration " \
          "option `rf_rack_valid_keyspaces` and the cluster feature `VIEWS_WITH_TABLETS`. " \
          "The keyspaces that violate that condition: ks"
    await log.wait_for(err, from_mark=mark)
    await start_task

@pytest.mark.asyncio
async def test_write_to_hinted_handoff_for_views(manager: ManagerClient):
    """
    Verify that view updates work correctly: they're stored and replayed.

    1. Create a 2-node cluster and a keyspace with RF=1. Create a table
       and a materialized view.
    2. Stop node 2.
    3. Insert data. Some of the mutations may fail if the partition key
       belongs to node 2; ignore them. About half of the successful mutations
       should produce view hints.
    4. Revive node 2.
    5. Wait for the view hints to be sent to node 2.
    6. Query data from the view. We should see all of the successful mutations
       there.
    """

    # To speed up the test.
    config = {"error_injections_at_startup": ["decrease_hints_flush_period"]}
    # We want to test view hints specifically, so let's disable regular hints.
    config = config | {"hinted_handoff_enabled": False}

    s1, s2 = await manager.servers_add(2, config=config, property_file=[
        {"dc": "dc1", "rack": "r1"},
        {"dc": "dc1", "rack": "r1"}
    ])

    cql, _ = await manager.get_ready_cql([s1])

    await cql.run_async("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}")
    await cql.run_async("CREATE TABLE ks.t (pk int, ck int, v int, PRIMARY KEY (pk, ck))")
    await cql.run_async("CREATE MATERIALIZED VIEW ks.mv AS SELECT * FROM ks.t "
                        "WHERE pk IS NOT NULL AND ck IS NOT NULL "
                        "AND v IS NOT NULL PRIMARY KEY ((ck, pk), v)")

    await manager.server_stop_gracefully(s2.server_id)

    stmt = cql.prepare("INSERT INTO ks.t (pk, ck, v) VALUES (?, ?, ?)")
    stmt.consistency_level = ConsistencyLevel.ONE

    # If the replica for the base table turns out to be the dead node, ignore it.
    # Note that the replicas for the base table and the replicas for the view
    # may be different.
    async def insert_data(i):
        try:
            await cql.run_async(stmt, (i, i, i))
            return i
        except:
            return None

    indices = await asyncio.gather(*[insert_data(i) for i in range(1000)])
    indices = [index for index in indices if index is not None]

    # Hints are written asynchronously, so let's give the node a bit of time.
    await asyncio.sleep(1)

    sync_point1 = create_sync_point(s1)

    await manager.server_start(s2.server_id, wait_others=1)

    await_sync_point(s1, sync_point1, 120)

    cql, _ = await manager.get_ready_cql([s1, s2])

    rows = await cql.run_async("SELECT * FROM ks.mv")
    # Normally, if a mutation fails, it doesn't mean that it wasn't applied;
    # for instance, the consistency level might've just not been satisfied.
    # Due to that, the number of rows in the base table could, under normal
    # circumstances, be greater than `len(indices)`. That's impossible here,
    # though, because we use RF=1.
    assert len(rows) == len(indices)
