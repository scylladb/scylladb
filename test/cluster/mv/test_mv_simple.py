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
@pytest.mark.parametrize("views_with_tablets", [False, True])
@pytest.mark.parametrize("rf_rack_valid_keyspaces", [False, True])
async def test_mv_and_index_restrictions_in_tablet_keyspaces(manager: ManagerClient, schema_kind: str,
                                                             views_with_tablets: bool, rf_rack_valid_keyspaces: bool):
    """
    Verify that creating a materialized view or a secondary index in a tablet-based keyspace
    is only possible when both the experimental flag `views-with-tablets` and the configuration
    option `rf_rack_valid_keyspaces` are enabled.
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
              "To be able to use them, enable the experimental feature `views-with-tablets` and the configuration " \
              "option `rf_rack_valid_keyspaces`."
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

    if views_with_tablets and rf_rack_valid_keyspaces:
        await try_pass(cql)
        logger.debug("try_pass finished successfully")
    else:
        await try_fail(cql)
        logger.debug("try_fail finished successfully")
