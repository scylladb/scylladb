#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import logging
import pytest

from test.pylib.manager_client import ManagerClient

logger = logging.getLogger(__name__)


# Regression test for SCYLLADB-81.
# 
# Unlike regular secondary indexes, vector indexes do not use materialized views
# in their implementation, therefore they do not need the keyspace
# to be rf-rack-valid in order to function properly.
# 
# In this test, we check that creating a vector index without
# rf_rack_valid_keyspaces being set is possible.
@pytest.mark.asyncio
async def test_vector_store_can_be_created_without_rf_rack_valid(manager: ManagerClient):
    # Explicitly disable the rf_rack_valid_keyspaces option.
    config = {"rf_rack_valid_keyspaces": False}
    srv = await manager.server_add(config=config)
    cql, _ = await manager.get_ready_cql([srv])

    # Explicitly create a keyspace with tablets.
    await cql.run_async("CREATE KEYSPACE ks WITH replication = "
                        "{'class': 'NetworkTopologyStrategy', 'replication_factor': 1} "
                        "AND tablets = {'enabled': true}")
    
    await cql.run_async("CREATE TABLE ks.t (pk int PRIMARY KEY, v vector<float, 3>)")

    # Creating a vector store index should succeed.
    await cql.run_async("CREATE CUSTOM INDEX ON ks.t(v) USING 'vector_index'")
    
