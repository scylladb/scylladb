#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
import asyncio
import time

from test.pylib.manager_client import ManagerClient
import pytest


TEST_FEATURE_ENABLE_ERROR_INJECTION = "features_enable_test_feature"
TEST_FEATURE_ENABLE_AS_DEPRECATED_ERROR_INJECTION = "features_enable_test_feature_as_deprecated"
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
