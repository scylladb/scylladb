#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""The TUPLE_CONSTRUCTOR feature changes how the database spells a one-element
tuple in the CQL text it stores for itself - a view's WHERE clause, an
aggregate's INITCOND - from "(x)" to tuple(x).  Text stored before the feature
is enabled has to be rewritten in the same step that enables it, or a node
would read a tuple as a plain parenthesized value.
"""
import time

import pytest

from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for_feature

ERROR_INJECTIONS_AT_STARTUP_CONFIG_KEY = "error_injections_at_startup"


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_stored_one_element_tuples_are_respelled_when_the_feature_is_enabled(manager: ScyllaClusterManager) -> None:
    # A node that does not yet support the feature stands in for a cluster that
    # predates it; restarting it with support stands in for the upgrade.
    srv = await manager.server_add(config={
        ERROR_INJECTIONS_AT_STARTUP_CONFIG_KEY: [{'name': 'suppress_features', 'value': 'TUPLE_CONSTRUCTOR'}],
        'enable_user_defined_functions': True,
        'experimental_features': ['udf'],
    })
    cql, hosts = await manager.get_ready_cql([srv])

    await cql.run_async("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}")
    await cql.run_async("CREATE TABLE ks.t (p int, c int, v int, PRIMARY KEY (p, c))")
    await cql.run_async("CREATE MATERIALIZED VIEW ks.mv AS SELECT * FROM ks.t WHERE p IS NOT NULL AND (c) > (1) PRIMARY KEY (c, p)")
    await cql.run_async("CREATE FUNCTION ks.sfunc(acc tuple<int>, v int) CALLED ON NULL INPUT RETURNS tuple<int> LANGUAGE lua AS 'return acc'")
    await cql.run_async("CREATE AGGREGATE ks.agg(int) SFUNC sfunc STYPE tuple<int> INITCOND (7)")

    async def stored_texts():
        where = (await cql.run_async("SELECT where_clause FROM system_schema.views WHERE keyspace_name = 'ks' AND view_name = 'mv'"))[0].where_clause
        initcond = (await cql.run_async("SELECT initcond FROM system_schema.aggregates WHERE keyspace_name = 'ks' AND aggregate_name = 'agg'"))[0].initcond
        return where, initcond

    # Written the only way a node without the feature can read.
    assert await stored_texts() == ("p IS NOT null AND (c) > (1)", "(7)")

    # Upgrade: the node comes back supporting the feature, and the topology
    # coordinator enables it.
    await manager.server_update_config(srv.server_id, ERROR_INJECTIONS_AT_STARTUP_CONFIG_KEY, [])
    await manager.server_restart(srv.server_id)
    cql, hosts = await manager.get_ready_cql([srv])
    await wait_for_feature("TUPLE_CONSTRUCTOR", cql, hosts[0], time.time() + 60)

    # Enabling the feature rewrote the stored text in the same command.
    assert await stored_texts() == ("p IS NOT null AND tuple(c) > tuple(1)", "tuple(7)")

    # Both are still read back correctly: the view keeps filtering, and the
    # aggregate still has its initial state.
    await cql.run_async("INSERT INTO ks.t (p, c, v) VALUES (1, 2, 3)")
    await cql.run_async("INSERT INTO ks.t (p, c, v) VALUES (1, 0, 3)")
    assert sorted(r.c for r in await cql.run_async("SELECT c FROM ks.mv")) == [2]
    assert (await cql.run_async("SELECT ks.agg(v) FROM ks.t"))[0][0] == (7,)
