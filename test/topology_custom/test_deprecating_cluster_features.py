#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
import asyncio
import time

from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for_cql_and_get_hosts
import pytest


TEST_FEATURE_ENABLE_ERROR_INJECTION = "features_enable_test_feature"
TEST_FEATURE_ENABLE_AS_DEPRECATED_ERROR_INJECTION = "features_enable_test_feature_as_deprecated"
SUPPRESS_FEATURES = "suppress_features"
ERROR_INJECTIONS_AT_STARTUP_CONFIG_KEY = "error_injections_at_startup"


@pytest.mark.asyncio
async def test_feature_deprecation_works(manager: ManagerClient) -> None:
    """Simulate a very old node which, long ago, has enabled some features,
       persisted them in system.scylla_local, and now some of them became
       deprecated.
    """

    # Start the node, enable the feature
    srv = await manager.server_add(config={
        ERROR_INJECTIONS_AT_STARTUP_CONFIG_KEY: [TEST_FEATURE_ENABLE_ERROR_INJECTION]
    })
    await manager.server_start(srv.server_id)

    # Restart the node, but this time the feature is deprecated
    await manager.server_stop_gracefully(srv.server_id)
    await manager.server_update_config(srv.server_id, ERROR_INJECTIONS_AT_STARTUP_CONFIG_KEY,
                                       [TEST_FEATURE_ENABLE_AS_DEPRECATED_ERROR_INJECTION])
    await manager.server_start(srv.server_id)

    # The node should restart successfully

async def check_features_status(cql, features, enabled):
    scylla_local_features = await cql.run_async("select value from system.scylla_local where key='enabled_features'")
    topology_features = await cql.run_async("select enabled_features, supported_features from system.topology")

    check_feature = lambda v, f: (f in v) == enabled

    for feature in features:
        assert check_feature(scylla_local_features[0].value, feature)
        assert check_feature(topology_features[0].enabled_features, feature)
        assert check_feature(topology_features[0].supported_features, feature)


@pytest.mark.asyncio
async def test_features_suppress_works(manager: ManagerClient, mode) -> None:
    """ `suppress_features` error injection allows to revoke support for
        specified cluster features. It can be used to simulate upgrade process.
    """
    if mode == "release":
        return

    features_to_suppress = ["PARALLELIZED_AGGREGATION", "UDA_NATIVE_PARALLELIZED_AGGREGATION"]

    srv = await manager.server_add(config={
        ERROR_INJECTIONS_AT_STARTUP_CONFIG_KEY: [
            {
                'name': SUPPRESS_FEATURES,
                'value': ";".join(features_to_suppress)
            }
        ]
    })
    await manager.server_start(srv.server_id)
    await check_features_status(manager.get_cql(), features_to_suppress, False)

    await manager.server_stop_gracefully(srv.server_id)
    await manager.server_update_config(srv.server_id, ERROR_INJECTIONS_AT_STARTUP_CONFIG_KEY, [])
    await manager.server_start(srv.server_id)
    await wait_for_cql_and_get_hosts(manager.get_cql(), [srv], time.time() + 60)
    await check_features_status(manager.get_cql(), features_to_suppress, True)
