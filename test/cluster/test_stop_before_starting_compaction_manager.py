#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from test.cluster.conftest import skip_mode
from test.pylib.manager_client import ManagerClient


@skip_mode('release', 'error injections are not supported in release mode')
async def test_stop_before_starting_compaction_manager(manager: ManagerClient) -> None:
    """Test that Scylla doesn't crash when stopped during boot after constructing compaction manager (and thus
    registering its task_manager module), but before enabling it (calling compaction_manager::enable()).

    Reproducer for SCYLLADB-2106.
    """
    await manager.server_add(
            config={"error_injections_at_startup": ["stop_before_starting_compaction_manager"]},
            expected_error="injected failure before starting compaction_manager")
