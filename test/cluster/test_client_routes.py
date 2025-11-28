# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
import asyncio
from datetime import datetime, timedelta
import pytest
import logging
import time
import uuid

from test.cluster.conftest import skip_mode
from test.pylib.manager_client import ManagerClient
from test.pylib.random_tables import RandomTables, Column, IntType
from test.pylib.rest_client import inject_error_one_shot, HTTPError
from test.pylib.util import wait_for_first_completed, wait_for

from cassandra.cluster import ControlConnection
from cassandra.protocol import EventMessage
import cassandra.protocol as cass_proto

logger = logging.getLogger(__name__)
CLIENT_ROUTES_CHANGE_EVENT_NAME = "CLIENT_ROUTES_CHANGE"

async def wait_for_expected_client_routes_size(cql, expected_routes_size):
    async def expected_client_routes_size(cql, expected_size):
        res = list(cql.execute("SELECT * FROM system.client_routes"))
        logger.info(f"Got client routes, expected_size={expected_size}, res={res}")
        if len(res) == expected_size:
            return res
        return None
    await wait_for(lambda: expected_client_routes_size(cql, expected_routes_size), time.time() + 10)

def generate_connection_id(i):
    return f"connection_id_{i}"

def generate_host_id(i):
    return str(uuid.UUID(int=(i + 100)))

def generate_client_routes_entry(i):
    return {
        "connection_id": generate_connection_id(i),
        "host_id": generate_host_id(i),
        "address": "addr1.test",
        "port": 8001,
        "tls_port": 8002,
        "alternator_port": 8003,
        "alternator_https_port": 8004
    }

@pytest.mark.asyncio
async def test_client_routes(request, manager: ManagerClient):
    num_servers = 3
    cql = None
    # Run three nodes one by one
    for i in range(num_servers):
        # SMP=2 to verify that requests work properily even when other shard than 0 gets them
        servers = await manager.servers_add(1, cmdline=['--smp=2'])
        cql, hosts = await manager.get_ready_cql(await manager.running_servers())
        await manager.api.client.post("/v2/client-routes", host=servers[0].ip_addr, json=[generate_client_routes_entry(i)])
        await wait_for_expected_client_routes_size(cql, i+1)


    # Remove one node
    running_servers = await manager.running_servers()
    server_to_stop = running_servers[0]
    running_server = running_servers[1]
    await manager.server_stop(server_to_stop.server_id)
    await manager.remove_node(running_server.server_id, server_to_stop.server_id)
    await wait_for_expected_client_routes_size(cql, num_servers)

    # Verify everything works
    await manager.api.client.post("/v2/client-routes", host=running_server.ip_addr, json=[generate_client_routes_entry(num_servers + 1)])
    await wait_for_expected_client_routes_size(cql, num_servers + 1)
    await manager.api.client.delete("/v2/client-routes", host=running_server.ip_addr, json=[generate_client_routes_entry(0)])
    await wait_for_expected_client_routes_size(cql, num_servers)

# This test verifies updating system to version with CLIENT_ROUTES feature in the following steps:
# 1. Create 2 nodes with disabled CLIENT_ROUTES feature
# 2. Verify `/v2/client-routes` rejects requests
# 3. Enable `CLIENT_ROUTES` server after server
# 4. Verify `/v2/client-routes` works
@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_client_routes_upgrade(request, manager: ManagerClient):
    num_servers = 2
    config = [
        {"name": "suppress_features", "value": "CLIENT_ROUTES"}
    ]
    servers = await manager.servers_add(num_servers, config={'error_injections_at_startup': config})
    cql, hosts = await manager.get_ready_cql(servers)
    # Empty `system.client_routes` is there even if the feature is disabled`
    wait_for_expected_client_routes_size(cql, 0)

    with pytest.raises(HTTPError) as exc:
        await manager.api.client.post("/v2/client-routes", host=servers[0].ip_addr, json=[generate_client_routes_entry(0)])
    with pytest.raises(HTTPError) as exc:
        await manager.api.client.delete("/v2/client-routes", host=servers[0].ip_addr, json=[generate_client_routes_entry(0)])
    with pytest.raises(HTTPError) as exc:
        await manager.api.client.get("/v2/client-routes", host=servers[0].ip_addr)

    for server in servers:
        await manager.server_update_config(server.server_id, "error_injections_at_startup", [])
        await manager.server_restart(server.server_id)

    await manager.api.client.post("/v2/client-routes", host=servers[0].ip_addr, json=[generate_client_routes_entry(0)])
    await manager.api.client.delete("/v2/client-routes", host=servers[0].ip_addr, json=[generate_client_routes_entry(0)])
    await manager.api.client.get("/v2/client-routes", host=servers[0].ip_addr)

@pytest.mark.asyncio
async def test_client_routes_lost_quorum(request, manager: ManagerClient):
    num_servers = 3
    timeout = 5
    servers = await manager.servers_add(num_servers)
    cql, hosts = await manager.get_ready_cql(servers)

    await wait_for_expected_client_routes_size(cql, 0)
    await manager.api.client.post("/v2/client-routes", host=servers[0].ip_addr, json=[generate_client_routes_entry(0)], timeout=timeout)
    await wait_for_expected_client_routes_size(cql, 1)

    for server in servers[1:]:
        await manager.server_stop(server.server_id)

    async def fail_post():
        with pytest.raises(TimeoutError) as exc:
            await manager.api.client.post("/v2/client-routes", host=servers[0].ip_addr, json=[generate_client_routes_entry(0)], timeout=timeout)

    async def fail_delete():
        with pytest.raises(TimeoutError) as exc:
            await manager.api.client.delete("/v2/client-routes", host=servers[0].ip_addr, json=[generate_client_routes_entry(0)], timeout=timeout)

    await asyncio.gather(fail_post(), fail_delete())
    await wait_for_expected_client_routes_size(cql, 1)

# The monkeypatching is a temporary solution. It will be removed very soon after a new Python Driver is released
def monkeypatch_driver_to_handle_client_routes_event(monkeypatch, received_events):
    _original_try_connect = ControlConnection._try_connect
    _original_known_event_types = cass_proto.known_event_types

    def _handle_event(arg1):
        logger.info(f"monkeypatch_driver handle_event called with arg1={arg1}")
        received_events.append(arg1)

    def _recv_client_routes_change(f, arg2):
        logger.info(f"monkeypatch_driver recv_client_routes_change, f={f} arg2={arg2}")
        change_type = cass_proto.read_string(f)
        connection_ids = [cass_proto.read_string(f) for _ in range(cass_proto.read_short(f))]
        host_ids = [cass_proto.read_string(f) for _ in range(cass_proto.read_short(f))]
        return {
            "change_type": change_type,
            "connection_ids": connection_ids,
            "host_ids": host_ids
        }

    def _try_connect_register_client_routes_change_event(self, *args, **kwargs):
        connection = _original_try_connect(self, *args, **kwargs)
        connection.register_watchers({CLIENT_ROUTES_CHANGE_EVENT_NAME: _handle_event})
        return connection   

    monkeypatch.setattr(cass_proto, "known_event_types", _original_known_event_types.union([CLIENT_ROUTES_CHANGE_EVENT_NAME]), raising=True)
    monkeypatch.setattr(EventMessage, "recv_client_routes_change", _recv_client_routes_change, raising=False)
    monkeypatch.setattr(ControlConnection, "_try_connect", _try_connect_register_client_routes_change_event, raising=True)

# This test verifies client routes change event in the following steps:
# 1. Add one new entry to client_routes.
# 2. Verify driver received one new event.
# 3. Add two new entries to client_routes using one POST request.
# 4. Verify driver received one new event with two updates.
@pytest.mark.asyncio
async def test_events(request, manager: ManagerClient, monkeypatch):
    received_events = []
    monkeypatch_driver_to_handle_client_routes_event(monkeypatch, received_events)

    servers = await manager.servers_add(2)
    cql, hosts = await manager.get_ready_cql(servers)

    await manager.api.client.post("/v2/client-routes", host=servers[0].ip_addr, json=[generate_client_routes_entry(0)])

    async def wait_for_expected_event_num(expected_num):
        async def expected_event_num(num):
            if len(received_events) == num:
                return num
            return None
        await wait_for(lambda: expected_event_num(expected_num), time.time() + 10)

    await wait_for_expected_event_num(1)
    assert received_events[0]["change_type"] == "UPDATE_NODES"
    assert received_events[0]["connection_ids"] == [generate_connection_id(0)]
    assert received_events[0]["host_ids"] == [generate_host_id(0)]

    await manager.api.client.post("/v2/client-routes", host=servers[0].ip_addr, json=[
        generate_client_routes_entry(1),
        generate_client_routes_entry(2),
    ])
    await wait_for_expected_event_num(2)
    assert received_events[1]["change_type"] == "UPDATE_NODES"
    assert received_events[1]["connection_ids"] == [generate_connection_id(1), generate_connection_id(2)]
    assert received_events[1]["host_ids"] == [generate_host_id(1), generate_host_id(2)]
