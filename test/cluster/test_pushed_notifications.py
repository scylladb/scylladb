#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
# Tests for the TOPOLOGY_CHANGE and STATUS_CHANGE events that a node pushes
# to the CQL clients which registered for them.
import asyncio
import re
import logging
import time

from cassandra.policies import WhiteListRoundRobinPolicy

from test import DEBUG_MODES
from test.cluster.dtest_helpers import create_cf, create_ks, insert_c1c2
from test.pylib.internal_types import IPAddress
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for
import pytest

logger = logging.getLogger(__name__)


def register_for_events(manager: ScyllaClusterManager, ip: IPAddress) -> list[dict]:
    """
    Connect a new driver to the given node only, register for its
    TOPOLOGY_CHANGE and STATUS_CHANGE events, and return the list
    the events will be appended to as they arrive.
    """
    cluster = manager.con_gen([ip], load_balancing_policy=WhiteListRoundRobinPolicy([ip]))
    cluster.connect()
    events = []
    def on_event(event):
        logger.info(f"Received event: {event}")
        events.append(event)
    cluster.control_connection._connection.register_watchers(
        {"TOPOLOGY_CHANGE": on_event, "STATUS_CHANGE": on_event})
    return events


async def wait_for_events(events: list[dict], count: int) -> None:
    async def have_events():
        return True if len(events) >= count else None
    await wait_for(have_events, time.time() + 60)


@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_restart_node_status_events(manager: ScyllaClusterManager):
    """
    A node pushes NEW_NODE and UP events for a node joining the cluster, and
    exactly one DOWN and then one UP event when that node restarts (and no
    NEW_NODE: it is not new). See CASSANDRA-7816, CASSANDRA-11038.
    """
    server1 = await manager.server_add(config={"tablets_mode_for_new_keyspaces": "disabled"})
    events = register_for_events(manager, server1.ip_addr)
    server2 = await manager.server_add(config={"tablets_mode_for_new_keyspaces": "disabled"})
    address = server2.rpc_address
    await wait_for_events(events, 2)
    # Restart twice: a duplicate or extra event of the previous step would
    # show up before the events of the next one.
    for i in range(2):
        await manager.server_stop_gracefully(server2.server_id)
        await wait_for_events(events, 2 * i + 3)
        await manager.server_start(server2.server_id)
        await wait_for_events(events, 2 * i + 4)
    assert [(e["change_type"], e["address"][0]) for e in events] == [("NEW_NODE", address), ("UP", address)] + [("DOWN", address), ("UP", address)] * 2


# Ported from scylla-dtest pushed_notifications_test.py::TestPushedNotifications::test_new_node_event_delay.
# Two nodes (datacenter1, rack1 and rack2, vnodes): with a client registered
# for node1's TOPOLOGY_CHANGE and STATUS_CHANGE events, node2 is started;
# as soon as a notification arrives (30 s at most), a session to node2
# creates ks (RF 2) and cf and writes 100 rows - the NEW_NODE event is held
# until node2's CQL server is ready. All notifications are about node2, and
# one is NEW_NODE.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_new_node_event_after_cql_is_ready(manager: ScyllaClusterManager):
    config = {"tablets_mode_for_new_keyspaces": "disabled", "num_tokens": 256}
    server1 = await manager.server_add(config=config, property_file={"dc": "datacenter1", "rack": "rack1"})
    server2 = await manager.server_add(start=False, config=config, property_file={"dc": "datacenter1", "rack": "rack2"})
    events = register_for_events(manager, server1.ip_addr)
    start = asyncio.create_task(manager.server_start(server2.server_id))
    deadline = time.time() + 30
    while not events and time.time() < deadline:
        await asyncio.sleep(0.1)
    notifications = list(events)

    cluster = manager.con_gen([server2.rpc_address])
    try:
        session = cluster.connect()
        create_ks(session, "ks", 2)
        session.cluster.control_connection.wait_for_schema_agreement(wait_time=120)
        session.execute("USE ks")
        create_cf(session, "cf", {"c1": "text", "c2": "text"})
        insert_c1c2(session, keys=range(100))
    finally:
        cluster.shutdown()
    await start

    received_new_node_event = False
    for notification in notifications:
        assert server2.rpc_address == notification["address"][0]
        if notification["change_type"] == "NEW_NODE":
            received_new_node_event = True
    assert received_new_node_event, "NEW_NODE event isn't received"


@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_sleep_and_restart_node(manager: ScyllaClusterManager):
    """
    Once the first node has sent the notifications of the second node's
    startup, a client registered on the first node gets exactly a DOWN and
    then an UP event for the second node when it restarts. Ported from
    scylla-dtest pushed_notifications_test.py, with its nodes' vnodes (256
    per node). As in the dtest, it waits for the first node to log both
    notifications (it holds the NEW_NODE event until the second node is
    CQL-ready, then sends it with UP), so no late event reaches the client.
    """
    config = {'tablets_mode_for_new_keyspaces': 'disabled', 'num_tokens': 256}
    server1, server2 = await manager.servers_add(2, config=config, cmdline=["--logger-log-level", "storage_service=debug"])
    log1 = await manager.server_open_log(server1.server_id)
    ip2 = re.escape(str(server2.ip_addr))
    await log1.wait_for(f"Notify node {ip2}/.* has joined the cluster", f"Notify node {ip2}/.* has been up", timeout=300)
    events = register_for_events(manager, server1.ip_addr)
    await manager.server_stop_gracefully(server2.server_id)
    await manager.server_start(server2.server_id)
    await wait_for_events(events, 2)
    await asyncio.sleep(1)
    assert [(e["change_type"], e["address"][0]) for e in events] == [("DOWN", server2.rpc_address), ("UP", server2.rpc_address)]
