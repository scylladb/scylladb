#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import logging
import pytest

from cassandra.cluster import Session as CassandraSession
from cassandra.protocol import InvalidRequest

from test.pylib.manager_client import ManagerClient

logger = logging.getLogger(__name__)

@pytest.mark.asyncio
@pytest.mark.parametrize("schema_kind", ["view", "index"])
async def test_mv_and_index_restrictions_in_tablet_keyspaces(manager: ManagerClient, schema_kind: str):
    """
    Verify that creating a materialized view or a secondary index in a tablet-based keyspace
    is only possible when both the experimental flag `views-with-tablets` and the configuration
    option `rf_rack_valid_keyspaces` are enabled.
    """

    srv = await manager.server_add(start=False)

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
              "To be able to use them, enable the experimental feature `views-with-tablets` and the configuration " \
              "option `rf_rack_valid_keyspaces`."
        with pytest.raises(InvalidRequest, match=err):
            await try_pass(cql)

    async def run_test(views_with_tablets: bool, rf_rack_valid_keyspaces: bool):
        logger.info(f"Running test case: views-with-tablets={views_with_tablets}, "
                    f"rf_rack_valid_keyspaces={rf_rack_valid_keyspaces}")

        feature = ["views-with-tablets"] if views_with_tablets else []
        await manager.server_update_config(srv.server_id, "experimental_features", feature)
        await manager.server_update_config(srv.server_id, "rf_rack_valid_keyspaces", rf_rack_valid_keyspaces)

        logger.debug("Updated configuration of node")

        await manager.server_start(srv.server_id)
        logger.debug("Node has started")

        # Necessary because we're restarting the node multiple times.
        cql, _ = await manager.get_ready_cql([srv])
        logger.debug("Obtained CassandraSession object")

        # We just want to validate the statements. We don't need to wait.
        assert hasattr(cql.cluster, "max_schema_agreement_wait")
        cql.cluster.max_schema_agreement_wait = 0
        logger.debug("Set max_schema_agreement_wait to 0")

        if views_with_tablets and rf_rack_valid_keyspaces:
            await try_pass(cql)
            logger.debug("try_pass finished successfully")
        else:
            await try_fail(cql)
            logger.debug("try_fail finished successfully")

        await manager.server_stop_gracefully(srv.server_id)
        logger.debug("Node stopped successfully")

        logger.info("Test case finished successfully")

    # The order is important here. Trying to start a node without
    # an experimental feature will fail if it was once enabled.
    await run_test(False, False)
    await run_test(False, True)
    await run_test(True, False)
    await run_test(True, True)
