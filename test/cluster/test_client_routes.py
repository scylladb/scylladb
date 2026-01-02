# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
import asyncio
import pytest
import logging
import time
import uuid

from test.cluster.conftest import skip_mode
from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import HTTPError
from test.pylib.util import wait_for
from test.cluster.util import trigger_snapshot

from cassandra.protocol import EventMessage
import cassandra.protocol

logger = logging.getLogger(__name__)
CLIENT_ROUTES_CHANGE_EVENT_NAME = "CLIENT_ROUTES_CHANGE"

async def wait_for_expected_client_routes_size(cql, expected_routes_size):
    async def expected_client_routes_size(cql, expected_size):
        client_routes = await cql.run_async("SELECT * FROM system.client_routes")
        logger.info(f"Got client routes, expected_size={expected_size}, res={client_routes}")
        if len(client_routes) == expected_size:
            return client_routes
        return None
    await wait_for(lambda: expected_client_routes_size(cql, expected_routes_size), time.time() + 10)

def generate_connection_id(i):
    # Make the string longer than 30 characters to make sure that in C++ the string has a heap allocation
    return f"connection_id_{i}_" + "abc" * 10

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
        # SMP=2 to verify that requests work properly even when a shard other than 0 receives them
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

@pytest.mark.asyncio
async def test_client_routes_node_restart(request, manager: ManagerClient):
    """
    This test verifies that a node receives updates if client routes were updated
    when the node was down.
    """
    servers = await manager.servers_add(3)
    cql, hosts = await manager.get_ready_cql(servers)
    server_to_restart = servers[2]

    await manager.server_stop(server_to_restart.server_id)
    await manager.api.client.post("/v2/client-routes", host=servers[0].ip_addr, json=[generate_client_routes_entry(1)])
    await wait_for_expected_client_routes_size(cql, 1)

    await manager.server_start(server_to_restart.server_id)
    cql = await manager.get_cql_exclusive(server_to_restart)
    await wait_for_expected_client_routes_size(cql, 1)

@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_client_routes_upgrade(request, manager: ManagerClient):
    """
    This test verifies updating the system to a version with the CLIENT_ROUTES feature in the following steps:
      1. Create 2 nodes with the CLIENT_ROUTES feature disabled.
      2. Verify `/v2/client-routes` rejects requests.
      3. Enable the `CLIENT_ROUTES` feature after restart.
      4. Verify `/v2/client-routes` works.
    """
    num_servers = 2
    config = [
        {"name": "suppress_features", "value": "CLIENT_ROUTES"}
    ]
    servers = await manager.servers_add(num_servers, config={'error_injections_at_startup': config})
    cql, hosts = await manager.get_ready_cql(servers)
    # Empty `system.client_routes` is there even if the feature is disabled.
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

    async def client_routes_ready():
        try:
            await manager.api.client.post("/v2/client-routes", host=servers[0].ip_addr, json=[generate_client_routes_entry(0)])
            await manager.api.client.delete("/v2/client-routes", host=servers[0].ip_addr, json=[generate_client_routes_entry(0)])
            await manager.api.client.get("/v2/client-routes", host=servers[0].ip_addr)
            return True
        except HTTPError as exc:
            # Allow cluster to be not ready
            if "requires all nodes to support the CLIENT_ROUTES cluster feature" not in exc.message:
                raise exc
        return None

    wait_for(client_routes_ready, time.time() + 10)


@pytest.mark.asyncio
async def test_client_routes_lost_quorum(request, manager: ManagerClient):
    """
    This test verifies that `/v2/client-routes` fails with a timeout if the Raft quorum cannot be reached.
    """
    num_servers = 3
    timeout = 10
    config = {'group0_raft_op_timeout_in_ms': timeout * 1000}
    servers = await manager.servers_add(num_servers, config=config)
    cql, hosts = await manager.get_ready_cql(servers)

    await wait_for_expected_client_routes_size(cql, 0)
    await manager.api.client.post("/v2/client-routes", host=servers[0].ip_addr, json=[generate_client_routes_entry(0)], timeout=timeout + 60)
    await wait_for_expected_client_routes_size(cql, 1)

    for server in servers[1:]:
        await manager.server_stop(server.server_id)

    async def fail_req(f):
        with pytest.raises(HTTPError) as exc:
            await f("/v2/client-routes", host=servers[0].ip_addr, json=[generate_client_routes_entry(0)], timeout=timeout + 60)
        assert "raft operation [read_barrier] timed out, there is no raft quorum" in exc.value.message

    await asyncio.gather(fail_req(manager.api.client.post), fail_req(manager.api.client.delete))
    await wait_for_expected_client_routes_size(cql, 1)

def setup_events_test(cql, received_events, monkeypatch):
    # scylla-driver >= 3.29.6  supports CLIENT_ROUTES_CHANGE events.
    # For older python driver, monkeypatching is necessary
    if CLIENT_ROUTES_CHANGE_EVENT_NAME not in cassandra.protocol.known_event_types:
        def _recv_client_routes_change(f, arg):
            logger.info(f"monkeypatch_driver recv_client_routes_change, f={f} arg={arg}")
            change_type = cassandra.protocol.read_string(f)
            connection_ids = [cassandra.protocol.read_string(f) for _ in range(cassandra.protocol.read_short(f))]
            host_ids = [cassandra.protocol.read_string(f) for _ in range(cassandra.protocol.read_short(f))]
            return {
                "change_type": change_type,
                "connection_ids": connection_ids,
                "host_ids": host_ids
            }
        monkeypatch.setattr(cassandra.protocol, "known_event_types", cassandra.protocol.known_event_types.union([CLIENT_ROUTES_CHANGE_EVENT_NAME]), raising=True)
        monkeypatch.setattr(EventMessage, "recv_client_routes_change", _recv_client_routes_change, raising=False)

    def on_event(event):
        logger.info(f"Received an event: {event}")
        if len(received_events) > 0 and received_events[-1] == event:
            logger.info(f"The received event is a duplicate: {event}")
        else:
            received_events.append(event)

    cql.cluster.control_connection._connection.register_watchers({CLIENT_ROUTES_CHANGE_EVENT_NAME: on_event})

async def wait_for_expected_event_num(expected_num, received_events):
    async def expected_event_num(num):
        logger.info(f"Checking if number of events is equal expected_num={expected_num}, events={received_events}")
        if len(received_events) == num:
            return num
        return None
    await wait_for(lambda: expected_event_num(expected_num), time.time() + 10)

@pytest.mark.asyncio
async def test_events(request, manager: ManagerClient, monkeypatch):
    """
    This test verifies client routes change events in the following steps:
      1. Add one new entry to client_routes.
      2. Verify that the driver received one new event.
      3. Add two new entries to client_routes using one POST request.
      4. Verify that the driver received one new event with two updates.
      5. Delete an entry, and verify that the driver received the event.
    """

    servers = await manager.servers_add(2, cmdline=['--smp=2'])
    cql, hosts = await manager.get_ready_cql(servers)

    received_events = []
    setup_events_test(cql, received_events, monkeypatch)

    await manager.api.client.post("/v2/client-routes", host=servers[0].ip_addr, json=[generate_client_routes_entry(0)])

    await wait_for_expected_event_num(1, received_events)
    assert received_events[0]["change_type"] == "UPDATE_NODES"
    assert received_events[0]["connection_ids"] == [generate_connection_id(0)]
    assert received_events[0]["host_ids"] == [generate_host_id(0)]

    await manager.api.client.post("/v2/client-routes", host=servers[0].ip_addr, json=[
        generate_client_routes_entry(1),
        generate_client_routes_entry(2),
    ])
    await wait_for_expected_event_num(2, received_events)
    assert received_events[1]["change_type"] == "UPDATE_NODES"
    assert received_events[1]["connection_ids"] == [generate_connection_id(1), generate_connection_id(2)]
    assert received_events[1]["host_ids"] == [generate_host_id(1), generate_host_id(2)]

    await manager.api.client.delete("/v2/client-routes", host=servers[0].ip_addr, json=[generate_client_routes_entry(0)])
    await wait_for_expected_event_num(3, received_events)
    assert received_events[2]["change_type"] == "UPDATE_NODES"
    assert received_events[2]["connection_ids"] == [generate_connection_id(0)]
    assert received_events[2]["host_ids"] == [generate_host_id(0)]

@pytest.mark.asyncio
@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
async def test_client_routes_snapshot_transfer(request, manager: ManagerClient, monkeypatch):
    """
    This test verifies that client routes change events are sent when client_routes
    data is propagated via snapshot transfer:
      1. Create a 3-node cluster.
      2. Enable `block_group0_transfer_snapshot` error injection on one node, and stop it.
      3. Change client routes with a POST request on other nodes, and trigger a snapshot.
      4. Start the stopped node, and send a message to stop waiting on `block_group0_transfer_snapshot`.
      5. Verify that an event was sent.
    """
    servers = await manager.servers_add(3, cmdline=['--smp=2'])
    cql, hosts = await manager.get_ready_cql(servers)
    server_to_restart = servers[2]
    error_to_inject = "block_group0_transfer_snapshot"

    await manager.server_update_config(server_to_restart.server_id, "error_injections_at_startup", [error_to_inject])
    await manager.server_stop(server_to_restart.server_id)

    await manager.api.client.post("/v2/client-routes", host=servers[0].ip_addr, json=[generate_client_routes_entry(1)])
    await wait_for_expected_client_routes_size(cql, 1)
    await trigger_snapshot(manager, servers[0])

    await manager.server_start(server_to_restart.server_id)
    log = await manager.server_open_log(server_to_restart.server_id)
    await log.wait_for("block_group0_transfer_snapshot: waiting for message")
    cql = await manager.get_cql_exclusive(server_to_restart)
    await wait_for_expected_client_routes_size(cql, 0)

    received_events = []
    setup_events_test(cql, received_events, monkeypatch)

    await manager.api.message_injection(server_to_restart.ip_addr, error_to_inject)
    await wait_for_expected_client_routes_size(cql, 1)
    await wait_for_expected_event_num(1, received_events)
    assert received_events[0]["change_type"] == "UPDATE_NODES"
    assert received_events[0]["connection_ids"] == [generate_connection_id(1)]
    assert received_events[0]["host_ids"] == [generate_host_id(1)]
    await log.wait_for("transfer snapshot: raft snapshot includes client_routes mutation")

@pytest.mark.asyncio
async def test_huge_event(request, manager: ManagerClient, monkeypatch):
    """
    This test verifies that an event can be sent to the driver even when it contains many host_ids and connection_ids.
    """
    servers = await manager.servers_add(2, cmdline=['--smp=2'])
    cql, hosts = await manager.get_ready_cql(servers)

    received_events = []
    setup_events_test(cql, received_events, monkeypatch)

    await manager.api.client.post("/v2/client-routes", host=servers[0].ip_addr, json=[generate_client_routes_entry(i) for i in range(1000)])

    await wait_for_expected_event_num(1, received_events)
    assert set(received_events[0]["connection_ids"]) == set([generate_connection_id(i) for i in range(1000)])
    assert set(received_events[0]["host_ids"]) == set([generate_host_id(i) for i in range(1000)])
