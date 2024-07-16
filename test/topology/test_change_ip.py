#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""
Test clusters can restart fine after an IP address change.
"""

import logging
import time

import pytest
import uuid
from cassandra.cluster import NoHostAvailable  # type: ignore # pylint: disable=no-name-in-module
from cassandra.pool import Host  # type: ignore # pylint: disable=no-name-in-module
from test.pylib.internal_types import ServerInfo
from test.pylib.random_tables import Column, IntType, TextType
from test.pylib.util import wait_for_cql_and_get_hosts, wait_for
from test.topology.util import reconnect_driver

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_change_two(manager, random_tables, mode):
    """Stop two nodes, change their IPs and start, check the cluster is
    functional"""
    servers = await manager.running_servers()
    host_ids = {s.server_id: await manager.get_host_id(s.server_id) for s in servers}
    table = await random_tables.add_table(name='t1', pks=1, columns=[
        Column("pk", IntType),
        Column('int_c', IntType)
    ])

    logger.info(f"Servers {servers}, gracefully stopping servers {servers[1]} and {servers[2]} to change ips")
    await manager.server_stop_gracefully(servers[1].server_id)
    await manager.server_stop_gracefully(servers[2].server_id)
    s1_new_ip = await manager.server_change_ip(servers[1].server_id)
    s2_new_ip = await manager.server_change_ip(servers[2].server_id)
    logger.info(f"s1 new ip {s1_new_ip}, s2 new ip {s2_new_ip}")

    async def wait_proper_ips(alive_servers: list[ServerInfo]):
        hosts = {h.address: h for h in await wait_for_cql_and_get_hosts(manager.get_cql(), alive_servers, time.time() + 60)}
        expected_alive_ips = {s.rpc_address for s in alive_servers}
        expected_down_ips = {s.rpc_address for s in servers if s not in alive_servers}
        expected_host_id_map = {host_ids[s.server_id]: s.rpc_address for s in servers}
        logger.info(f"wait for proper ips, servers {servers}, alive servers {alive_servers}, expected host id map {expected_host_id_map}")
        for server in alive_servers:
            ip = server.rpc_address
            host = hosts[ip]
            host_id = host_ids[server.server_id]
            expected_peers = [(uuid.UUID(host_ids[s.server_id]), s.rpc_address) for s in servers if s.server_id != server.server_id]
            expected_peers.sort()
            logger.info(f"waiting for {ip}/{host_id} to see the proper ips, expected peers {expected_peers}")

            async def see_proper_ips():
                local_rows = await manager.get_cql().run_async("select host_id, listen_address, rpc_address, broadcast_address from system.local", host=host)
                if len(local_rows) != 1:
                    logger.info(f"len(system.local) == {len(local_rows)} != 1")
                    return None
                local_row = local_rows[0]
                local_vals = (local_row.host_id, local_row.listen_address, local_row.rpc_address, local_row.broadcast_address)
                local_expected_vals = (uuid.UUID(host_id), ip, ip, ip)
                if local_vals != local_expected_vals:
                    logger.info(f"local vals {local_vals} doesn't match expected vals {local_expected_vals}")
                    return None

                current_peers = [(r.host_id, r.peer) for r in await manager.get_cql().run_async("select peer, host_id from system.peers", host=host)]
                current_peers.sort()
                if current_peers != expected_peers:
                    logger.info(f"host {host} current peers {current_peers} doesn't match the expected_peers {expected_peers}, continue waiting")
                    return None

                alive_eps = set(await manager.api.get_alive_endpoints(ip))
                if alive_eps != expected_alive_ips:
                    logger.info(f"host {host} alive endpoints {alive_eps} doesn't match the expected alive ips {expected_alive_ips}, continue waiting")
                    return None

                down_eps = set(await manager.api.get_down_endpoints(ip))
                if down_eps != expected_down_ips:
                    logger.info(f"host {host} down endpoints {down_eps} doesn't match the expected down ips {expected_down_ips}, continue waiting")
                    return None

                actual_host_id_map = {x['value']: x['key'] for x in await manager.api.get_host_id_map(host.address)}
                if expected_host_id_map != actual_host_id_map:
                    logger.info(f"host {host} id map {actual_host_id_map} doesn't match the expected host id map {expected_host_id_map}, continue waiting")
                    return None

                return True

            # FIXME: This is a workaround for the scylladb/python-driver#295 issue.
            #        We ignore the exception and keep retrying the operation.
            #        Can be removed once the issue is fixed.
            async def safe_see_proper_ips():
                try:
                    return await see_proper_ips()
                except NoHostAvailable as e:
                    logger.info(f"see_proper_ips failed: {e}")
                    return None

            await wait_for(safe_see_proper_ips, time.time() + 60)

    # We're checking the crash scenario here - the servers[0] crashes just after
    # saving s1_new_ip but before removing s1_old_ip. After its restart we should
    # see s1_new_ip.
    if mode != 'release':
        await manager.api.enable_injection(servers[0].ip_addr, 'crash-before-prev-ip-removed', one_shot=True)
        # There is a code in raft_ip_address_updater::on_endpoint_change which
        # calls gossiper.force_remove_endpoint for an endpoint if it sees
        # that the current generation of host_id -> ip mapping in raft_address_map
        # is greater than the generation of the endpoint.
        # We need to inject a delay ip-change-raft-sync-delay
        # before old IP is removed. Otherwise, servers[0] removes the old
        # IP-s before they are send back to servers[1] and servers[2],
        # and the mentioned above code is not exercised by this test.
        await manager.api.enable_injection(servers[0].ip_addr, 'ip-change-raft-sync-delay', one_shot=False)
    await manager.server_start(servers[1].server_id)
    servers[1] = ServerInfo(servers[1].server_id, s1_new_ip, s1_new_ip)
    if mode != 'release':
        s0_logs = await manager.server_open_log(servers[0].server_id)
        await s0_logs.wait_for('crash-before-prev-ip-removed hit, killing the node')
        await manager.server_stop(servers[0].server_id)
        await manager.server_start(servers[0].server_id)
        await manager.api.enable_injection(servers[0].ip_addr, 'ip-change-raft-sync-delay', one_shot=False)
    await reconnect_driver(manager)
    await wait_proper_ips([servers[0], servers[1]])

    await manager.server_start(servers[2].server_id)
    servers[2] = ServerInfo(servers[2].server_id, s2_new_ip, s2_new_ip)
    await reconnect_driver(manager)
    await wait_proper_ips([servers[0], servers[1], servers[2]])

    await table.add_column(column=Column("str_c", TextType))
    await random_tables.verify_schema()

    host0 = (await wait_for_cql_and_get_hosts(manager.get_cql(), [servers[0]], time.time() + 60))[0]
    await manager.get_cql().run_async(f"USE {random_tables.keyspace}")
    await manager.get_cql().run_async("insert into t1(pk, int_c, str_c) values (1, 2, 'test-val')", host=host0)
    rows = list(await manager.get_cql().run_async("select * from t1", host=host0))
    assert len(rows) == 1
    row = rows[0]
    assert row.pk == 1
    assert row.int_c == 2
    assert row.str_c == 'test-val'
