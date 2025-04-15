#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from test.pylib.manager_client import ManagerClient
from cassandra.connection import ConnectionShutdown
from test.cluster.util import new_test_keyspace

import asyncio
import logging
import pytest

logger = logging.getLogger(__name__)
pytestmark = pytest.mark.prepare_3_nodes_cluster


@pytest.mark.asyncio
async def test_upgrade_to_ssl(manager: ManagerClient) -> None:
    """Tests rolling upgrade/downgrade from non-SSL to SSL and back
    """

    mode2ports = {
        "none": [7000],
        "transitional": [7000, 7001],
        "all": [7001],
    }

    cf = 'cf'

    servers = await manager.running_servers()
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.{cf} (pk int PRIMARY KEY) WITH tombstone_gc = {{'mode': 'immediate'}}")

        async def update_config_and_restart(mode):
            for srv in servers:
                # get the log and current pos
                log = await manager.server_open_log(srv.server_id)
                mark = await log.mark()
                # stop one server
                await manager.server_stop_gracefully(srv.server_id)
                # change internode encryption
                seo = (await manager.server_get_config(srv.server_id))['server_encryption_options']
                seo['internode_encryption'] = mode
                await manager.server_update_config(srv.server_id, "server_encryption_options", seo)
                # restart
                await manager.server_start(srv.server_id)
                # now check we get the expected messaging server listening lines in log
                expected_ports = mode2ports[mode]
                pattern = "|".join(["port " + str(port) for port in expected_ports])
                res = await log.grep(pattern, from_mark=mark)
                assert len(res) == len(expected_ports), \
                    f"The listened ports are not same as expected! " \
                    f"Expected ports: {expected_ports}\nReal listened ports: {res}"

        async def reconnect():
            manager.driver_close()
            await manager.driver_connect()
            return manager.get_cql()

        async def run_retry_async(stmt : str):
            lcql = cql
            while True:
                try:
                    await lcql.run_async(stmt)
                    return
                except ConnectionShutdown:
                    lcql = await reconnect();

        # iterate from none to all and back
        for mode in ["none", "transitional", "all", "transitional", "none"]:
            # run a bunch of inserts in background. TODO: have something akin to cassandra-stress
            # we can run in separate thread/process to really guarantee parallelism.
            go_on = True

            async def write_in_background():
                count = 0;
                while go_on:
                    await run_retry_async(f"INSERT INTO {ks}.{cf} (pk) VALUES ({count});")
                    count = count + 1
                return count

            #        f = asyncio.gather(
            #            *[run_retry_async(f"INSERT INTO {ks}.{cf} (pk) VALUES ({k});") for k in range(count)]
            #            )
            f = write_in_background()

            # do a rolling restart, updating the internode_encryption mode
            await update_config_and_restart(mode)
            go_on = False
            # wait for the writes to finish
            count = await f
            cql = await reconnect()
            # check writes completed even though we are so very rolling
            await cql.run_async(f"SELECT COUNT(*) FROM {ks}.{cf}")
            assert count == (await cql.run_async(f"SELECT COUNT(*) FROM {ks}.{cf}"))[0].count
            # and drop data
            await cql.run_async(f"TRUNCATE {ks}.{cf}")

        await cql.run_async(f"DROP TABLE {ks}.{cf};")

