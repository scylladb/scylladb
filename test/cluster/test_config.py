# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

import logging
import os
import pytest
import signal
from cassandra import WriteFailure  # type: ignore
import requests
import sys
import threading
import time

from test import DEBUG_MODES
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.rest_client import read_barrier
from test.pylib.util import wait_for

async def wait_for_config(manager, server, config_name, value):
    async def config_value_equal():
        await read_barrier(manager.api, server.ip_addr)
        resp = await manager.api.get_config(server.ip_addr, config_name)
        logging.info(f"Obtained config via REST api - config_name={config_name} response={resp}  expected value={value}")
        if resp == value:
            return True
        return None
    await wait_for(config_value_equal, deadline=time.time() + 60)

async def test_non_liveupdatable_config(manager):

    server = await manager.server_add()
    not_liveupdatable_param = "rpc_keepalive"
    liveupdatable_param = "compaction_enforce_min_threshold"

    logging.info("Verify initial (default) config values")
    await wait_for_config(manager, server, not_liveupdatable_param, True)
    await wait_for_config(manager, server, liveupdatable_param, False)

    logging.info("Change config values - verify only liveupdatable config is changed")
    await manager.server_update_config(server.server_id, not_liveupdatable_param, False)
    await manager.server_update_config(server.server_id, liveupdatable_param, True)
    await wait_for_config(manager, server, liveupdatable_param, True)
    await wait_for_config(manager, server, not_liveupdatable_param, True)


# A SIGHUP while the configuration file is missing logs an error and does
# not stop the node; once the file is back, reloading it works again.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_config_reload_without_config_file(manager: ScyllaClusterManager):
    server = await manager.server_add(config={"tablets_mode_for_new_keyspaces": "disabled"})
    manager.ignore_log_patterns += ["Could not read configuration file", "failed to re-read configuration file"]
    conf = os.path.join(await manager.server_get_workdir(server.server_id), "conf", "scylla.yaml")
    log = await manager.server_open_log(server.server_id)
    mark = await log.mark()
    os.rename(conf, conf + ".backup")
    try:
        os.kill(server.pid, signal.SIGHUP)
        await log.wait_for("Could not read configuration file", from_mark=mark, timeout=60)
        await log.wait_for("failed to re-read configuration file: std::invalid_argument", from_mark=mark, timeout=60)
    finally:
        os.rename(conf + ".backup", conf)
    for value in [True, False]:
        await manager.server_update_config(server.server_id, "compaction_enforce_min_threshold", value)
        await wait_for_config(manager, server, "compaction_enforce_min_threshold", value)


# live_updatable_config_params_changeable_via_cql decides whether live
# updatable parameters can be changed by updating system.config; through the
# configuration file and the REST API they can be changed either way.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("changeable_via_cql", [True, False])
async def test_live_updatable_config_params_changeable_via_cql(manager: ScyllaClusterManager, changeable_via_cql: bool):
    server = await manager.server_add(config={"live_updatable_config_params_changeable_via_cql": changeable_via_cql, "tablets_mode_for_new_keyspaces": "disabled"})
    cql = manager.get_cql()
    await manager.server_update_config(server.server_id, "compaction_enforce_min_threshold", True)
    await wait_for_config(manager, server, "compaction_enforce_min_threshold", True)
    await manager.api.client.post("/task_manager/ttl", host=server.ip_addr, params={"ttl": "7"})
    assert await manager.api.client.get_json("/task_manager/ttl", host=server.ip_addr) == 7

    update = "UPDATE system.config SET value = 'false' WHERE name = 'compaction_enforce_min_threshold'"
    if changeable_via_cql:
        await cql.run_async(update)
        await wait_for_config(manager, server, "compaction_enforce_min_threshold", False)
    else:
        with pytest.raises(WriteFailure, match="option is not live-updateable"):
            await cql.run_async(update)
        await wait_for_config(manager, server, "compaction_enforce_min_threshold", True)
        with pytest.raises(WriteFailure, match="option is not live-updateable"):
            await cql.run_async("UPDATE system.config SET value = '100' WHERE name = 'compaction_large_partition_warning_threshold_mb'")
