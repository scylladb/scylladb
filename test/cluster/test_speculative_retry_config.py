#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import pytest

from test.pylib.scylla_cluster_manager import ScyllaClusterManager


@pytest.mark.parametrize('cfg_source', ['yaml', 'cmdline'])
async def test_invalid_speculative_retry_config(manager: ScyllaClusterManager, cfg_source: str):
    """
    Check that a node refuses to start when speculative_retry_user_table_default
    is set to a value that does not parse as a speculative_retry option.
    """
    expected_error = 'Invalid speculative_retry_user_table_default'
    if cfg_source == 'yaml':
        await manager.server_add(config={'speculative_retry_user_table_default': 'dog'}, expected_error=expected_error)
    else:
        await manager.server_add(cmdline=['--speculative-retry-user-table-default', 'dog'], expected_error=expected_error)
