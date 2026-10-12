#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import asyncio
import logging
import os
import shutil
from typing import Callable

import pytest

from test.cluster.util import new_test_keyspace, reconnect_driver
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)


@pytest.mark.parametrize("first_boot_on_volume", [True, False])
async def test_commitlog_enospc(manager: ScyllaClusterManager, volumes_factory: Callable, first_boot_on_volume: bool) -> None:
    """Fill a commitlog directory that lives on a small (20MB) volume until
    writes fail with ENOSPC. The node must stay up (it stops serving requests)
    and report the error;
    after it is killed and its commitlog is moved to a directory with enough
    space, it must replay that commitlog and accept writes again.
    The node either boots for the first time on the small volume, or is
    moved there after a drain, with its earlier commitlog already flushed.
    See https://github.com/scylladb/scylladb/pull/6368"""
    manager.ignore_log_patterns += [
        r"commitlog - Failed to (flush|persist) commits to disk.*No space left on device",
        r"storage_proxy - .*Could not write mutation .* to commitlog.*No space left on device",
        r"No space left on device",
    ]
    with volumes_factory(["20M"]) as [volume]:
        config = {'commitlog_segment_size_in_mb': 1}
        volume_config = {'commitlog_directory': str(volume.mount)}
        if first_boot_on_volume:
            server = await manager.server_add(config=config | volume_config)
        else:
            server = await manager.server_add(config=config)
            await manager.api.drain(server.ip_addr)
            await manager.server_stop_gracefully(server.server_id)
            await manager.server_update_config(server.server_id, config_options=volume_config)
            await manager.server_start(server.server_id)
            await reconnect_driver(manager)
        cql, _ = await manager.get_ready_cql([server])
        log = await manager.server_open_log(server.server_id)

        async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
            await cql.run_async(f"CREATE TABLE {ks}.cf (pk int PRIMARY KEY, v text)")
            insert = cql.prepare(f"INSERT INTO {ks}.cf (pk, v) VALUES (?, ?)")
            value = 'x' * 1024
            batch = 32
            attempted = 0
            # 1KB rows: 100k of them are 5 times the volume size.
            with pytest.raises(Exception) as failure:
                while attempted < 100000:
                    await asyncio.gather(*(cql.run_async(insert, [pk, value]) for pk in range(attempted, attempted + batch)))
                    attempted += batch
            attempted += batch
            logger.info(f"Writes failed after {attempted} rows: {failure.value}")
            await log.wait_for("No space left on device", timeout=60)
            assert await manager.server_is_alive(server.server_id)

            # Move the commitlog to a directory with enough space.
            await manager.server_stop(server.server_id, convict=False)
            workdir = await manager.server_get_workdir(server.server_id)
            new_commitlog_dir = os.path.join(workdir, "commitlog-recovered")
            shutil.copytree(volume.mount, new_commitlog_dir, ignore=shutil.ignore_patterns("lost+found"))
            await manager.server_update_config(server.server_id, config_options={'commitlog_directory': new_commitlog_dir})
            await manager.server_start(server.server_id)
            await reconnect_driver(manager)
            cql, _ = await manager.get_ready_cql([server])

            count = (await cql.run_async(f"SELECT count(*) FROM {ks}.cf"))[0].count
            logger.info(f"{count} of {attempted} rows survived")
            assert 0 < count < attempted

            for start in range(0, attempted, batch):
                await asyncio.gather(*(cql.run_async(insert, [pk, value]) for pk in range(start, min(start + batch, attempted))))
            assert (await cql.run_async(f"SELECT count(*) FROM {ks}.cf"))[0].count == attempted
