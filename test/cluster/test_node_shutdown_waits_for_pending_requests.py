# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

import asyncio
import struct
import time
from pathlib import Path

import pytest
import aiohttp
from cassandra import ConsistencyLevel
from cassandra.cluster import Session
from cassandra.policies import FallthroughRetryPolicy, WhiteListRoundRobinPolicy
from cassandra.protocol import ReadFailure, ReadTimeout, Unavailable
from cassandra.query import SimpleStatement

from test.cluster.util import ensure_raft_group_leader_on, get_coordinator_host, new_test_keyspace, reconnect_driver, trigger_stepdown
from test.pylib.internal_types import IPAddress
from test.pylib.rest_client import inject_error_one_shot, read_barrier
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.scylla_server import ScyllaVersionDescription, get_current_version_description
from test.pylib.util import get_enabled_features, get_supported_features, wait_for, wait_for_feature


def _connect_via_control_connection(manager: ScyllaClusterManager, ip_addr: IPAddress) -> Session:
    cluster = manager.con_gen([ip_addr], load_balancing_policy=WhiteListRoundRobinPolicy([ip_addr]),
                              queries_only_via_control_connection=True)
    manager.exclusive_clusters.append(cluster)
    return cluster.connect()


@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
async def test_drain_notifies_clients_before_pending_reply(manager: ScyllaClusterManager) -> None:
    """Regression for #15899: notify drivers and close admission without losing an admitted reply."""
    servers = await manager.servers_add(3, config={"alternator_port": 8000, "alternator_enforce_authorization": False,
                                                  "alternator_write_isolation": "only_rmw_uses_lwt"}, auto_rack_dc="dc1")
    ready_cql, hosts = await manager.get_ready_cql(servers)
    await wait_for_feature("GRACEFUL_READ_SHUTDOWN", ready_cql, hosts[0], time.time() + 30)
    coordinator, replica = servers[:2]
    cql = await manager.get_cql_exclusive(coordinator)
    events = []
    cql.cluster.control_connection._connection.register_watchers({"STATUS_CHANGE": events.append})
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} "
                                 "AND tablets = {'enabled': false}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test_table (pk int PRIMARY KEY) WITH speculative_retry = 'NONE'")
        await cql.run_async(f"INSERT INTO {ks}.test_table (pk) VALUES (42)")
        reply = await inject_error_one_shot(manager.api, replica.ip_addr, "storage_proxy::handle_read",
                                            parameters={"cf_name": "test_table"})
        writer = None
        read = None
        drain = None
        try:
            # Keep an established client which does not subscribe to status events.
            reader, writer = await asyncio.wait_for(asyncio.open_connection(replica.ip_addr, 9042), timeout=5)
            startup = b'\x00\x01\x00\x0bCQL_VERSION\x00\x053.0.0'
            writer.write(struct.pack('!BBhBI', 4, 0, 0, 1, len(startup)) + startup)
            await asyncio.wait_for(writer.drain(), timeout=5)
            header = await asyncio.wait_for(reader.readexactly(9), timeout=5)
            _, _, _, opcode, length = struct.unpack('!BBhBI', header)
            await asyncio.wait_for(reader.readexactly(length), timeout=5)
            assert opcode == 2, "expected a READY response to STARTUP"
            read = cql.run_async(SimpleStatement(f"SELECT pk FROM {ks}.test_table WHERE pk = 42 USING TIMEOUT 30s",
                                                 consistency_level=ConsistencyLevel.ALL, retry_policy=FallthroughRetryPolicy()), timeout=40)
            await manager.api.wait_for_injection_enter(replica.ip_addr, "storage_proxy::handle_read")
            drain = asyncio.create_task(manager.api.drain(replica.ip_addr))

            async def notified():
                return any(event["change_type"] == "DOWN" and event["address"][0] == replica.ip_addr for event in events) or None

            await wait_for(notified, time.time() + 10, period=0.1)
            assert await asyncio.wait_for(reader.read(1), timeout=5) == b'', "an existing client still has request admission"
            # Even clients which ignore the DOWN event must not open new sessions.
            with pytest.raises(ConnectionRefusedError):
                _, probe_writer = await asyncio.wait_for(asyncio.open_connection(replica.ip_addr, 9042), timeout=5)
                probe_writer.close()
                await asyncio.wait_for(probe_writer.wait_closed(), timeout=5)
            async with aiohttp.ClientSession() as http:
                url = f"http://{replica.ip_addr}:8000/"
                async with http.post(url, json={}, headers={"X-Amz-Target": "DynamoDB_20120810.ListTables"}) as response:
                    assert response.status == 500
                    assert "draining" in await response.text()
                async with http.get(url) as response:
                    assert response.status == 503
                async with http.get(f"http://{coordinator.ip_addr}:8000/localnodes") as response:
                    assert replica.ip_addr not in await response.json()
            assert not read.done(), "client notification interrupted the outstanding internode read"
            assert not drain.done()
        finally:
            try:
                if writer is not None:
                    writer.close()
                    await asyncio.wait_for(writer.wait_closed(), timeout=5)
            finally:
                await reply.message()
                await manager.api.disable_injection(replica.ip_addr, "storage_proxy::handle_read")
                results = await asyncio.wait_for(asyncio.gather(*([read] if read is not None else []),
                                                               *([drain] if drain else []), return_exceptions=True), timeout=60)
        assert not isinstance(results[0], BaseException), results[0]
        assert list(results[0]) == [(42,)]
        assert drain is not None and results[1] is None
        await manager.server_stop_gracefully(replica.server_id)
        await manager.server_start(replica.server_id)
        await manager.get_ready_cql(servers)


@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
async def test_drain_waits_for_forwarded_write_reply(manager: ScyllaClusterManager) -> None:
    """Regression for #15899: transfer leadership, exclude new queries, and retain an admitted write reply."""
    config = {"experimental_features": ["strongly-consistent-tables"]}
    replicas = await manager.servers_add(3, config=config, auto_rack_dc="dc1")
    coordinator = await manager.server_add(config=config | {"join_ring": False},
                                           cmdline=["--logger-log-level=storage_service=debug"],
                                           property_file={"dc": "dc1", "rack": "r0"})
    ready_cql, hosts = await manager.get_ready_cql(replicas)
    await wait_for_feature("GRACEFUL_READ_SHUTDOWN", ready_cql, hosts[0], time.time() + 30)
    cql = _connect_via_control_connection(manager, coordinator.ip_addr)
    replica = replicas[0]
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} "
                                 "AND tablets = {'initial': 1} AND consistency = 'global'") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test_table (pk int PRIMARY KEY, v int)")
        table_id = await manager.get_table_id(ks, "test_table")
        rows = await cql.run_async(f"SELECT raft_group_id FROM system.tablets WHERE table_id = {table_id}")
        group_id = str(rows[0].raft_group_id)
        await ensure_raft_group_leader_on(manager, replica, group_id)
        # Populate the forwarding coordinator's leader cache before drain.
        await cql.run_async(f"INSERT INTO {ks}.test_table (pk, v) VALUES (42, 0)")
        reply = await inject_error_one_shot(manager.api, replica.ip_addr, "forward_cql_pause_before_reply")
        write = cql.run_async(SimpleStatement(f"INSERT INTO {ks}.test_table (pk, v) VALUES (42, 1) USING TIMEOUT 30s",
                                              retry_policy=FallthroughRetryPolicy()), timeout=40)
        drain = None
        try:
            await manager.api.wait_for_injection_enter(replica.ip_addr, "forward_cql_pause_before_reply")
            log = await manager.server_open_log(replica.server_id)
            mark = await log.mark()
            drain = asyncio.create_task(manager.api.drain(replica.ip_addr))
            await log.wait_for("Preparing shutdown: waiting for coordinators", from_mark=mark)
            assert await manager.api.get_raft_leader(replica.ip_addr, group_id) != await manager.get_host_id(replica.server_id)
            # A cached leader and a new write must both avoid the draining node.
            await cql.run_async(f"INSERT INTO {ks}.test_table (pk, v) VALUES (43, 2)")
            assert list(await cql.run_async(f"SELECT v FROM {ks}.test_table WHERE pk = 43")) == [(2,)]
            with pytest.raises(TimeoutError):
                await log.wait_for("Preparing shutdown: coordinator request wait finished", from_mark=mark, timeout=1)
            assert not drain.done() and not write.done()
        finally:
            await reply.message()
            await manager.api.disable_injection(replica.ip_addr, "forward_cql_pause_before_reply")
            results = await asyncio.wait_for(asyncio.gather(write, *([drain] if drain else []), return_exceptions=True), timeout=60)
        assert not isinstance(results[0], BaseException), results[0]
        assert drain is not None and results[1] is None
        await manager.server_stop_gracefully(replica.server_id)
        await manager.server_start(replica.server_id)
        await manager.get_ready_cql(replicas)
        assert list(await cql.run_async(f"SELECT v FROM {ks}.test_table WHERE pk = 42")) == [(1,)]


@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
@pytest.mark.parametrize("is_write", [False, True])
@pytest.mark.parametrize("before_marker", [False, True], ids=["after_marker", "before_marker"])
async def test_drain_reselects_late_forwarded_query(manager: ScyllaClusterManager, is_write: bool, before_marker: bool) -> None:
    """Regression for #15899: enforce admission even for a peer's earlier, delayed forwarding decision."""
    config = {"experimental_features": ["strongly-consistent-tables"]}
    replicas = [await manager.server_add(config=config, cmdline=["--logger-log-level=cql_server=debug"],
                                        property_file={"dc": "dc1", "rack": f"r{i}"}) for i in range(3)]
    coordinator = await manager.server_add(config=config | {"join_ring": False},
                                           cmdline=["--logger-log-level=storage_service=debug"],
                                           property_file={"dc": "dc1", "rack": "r0"})
    ready_cql, hosts = await manager.get_ready_cql(replicas)
    await wait_for_feature("GRACEFUL_READ_SHUTDOWN", ready_cql, hosts[0], time.time() + 30)
    cql = _connect_via_control_connection(manager, coordinator.ip_addr)
    replica = replicas[0]
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} "
                                 "AND tablets = {'initial': 1} AND consistency = 'global'") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test_table (pk int PRIMARY KEY, v int)")
        table_id = await manager.get_table_id(ks, "test_table")
        rows = await cql.run_async(f"SELECT raft_group_id FROM system.tablets WHERE table_id = {table_id}")
        await ensure_raft_group_leader_on(manager, replica, str(rows[0].raft_group_id))
        await cql.run_async(f"INSERT INTO {ks}.test_table (pk, v) VALUES (42, 0)")
        admission = await inject_error_one_shot(manager.api, replica.ip_addr, "wait_before_handling_forwarded_request")
        statement = (f"INSERT INTO {ks}.test_table (pk, v) VALUES (42, 1) USING TIMEOUT 30s" if is_write
                     else f"SELECT v FROM {ks}.test_table WHERE pk = 42 USING TIMEOUT 30s")
        request = cql.run_async(SimpleStatement(statement, consistency_level=ConsistencyLevel.QUORUM,
                                                retry_policy=FallthroughRetryPolicy()), timeout=40)
        drain = None
        marker = None
        try:
            await manager.api.wait_for_injection_enter(replica.ip_addr, "wait_before_handling_forwarded_request")
            if before_marker:
                # Admission can stop while the local gossip lock prevents the
                # marker from reaching the origin's cached-leader routing.
                marker = await inject_error_one_shot(manager.api, replica.ip_addr, "gossiper_hold_local_endpoint")
                await manager.api.wait_for_injection_enter(replica.ip_addr, "gossiper_hold_local_endpoint")
            log = await manager.server_open_log(coordinator.server_id)
            mark = await log.mark()
            target_log = await manager.server_open_log(replica.server_id)
            target_mark = await target_log.mark()
            drain = asyncio.create_task(manager.api.drain(replica.ip_addr))
            host_id = await manager.get_host_id(replica.server_id)
            if before_marker:
                await target_log.wait_for("Shutting down RX side", from_mark=target_mark, timeout=10)
            else:
                await log.wait_for(f"Waiting for coordinated requests before {host_id} shuts down", from_mark=mark)
            await admission.message()
            result = await asyncio.wait_for(asyncio.shield(request), timeout=5)
            if not is_write:
                assert list(result) == [(0,)]
            await target_log.wait_for("Rerouting a late forwarded query after admission stopped", timeout=5)
        finally:
            await admission.message()
            await manager.api.disable_injection(replica.ip_addr, "wait_before_handling_forwarded_request")
            if marker is not None:
                await marker.message()
                await manager.api.disable_injection(replica.ip_addr, "gossiper_hold_local_endpoint")
            results = await asyncio.wait_for(asyncio.gather(request, *([drain] if drain else []), return_exceptions=True), timeout=60)
        assert not isinstance(results[0], BaseException), results[0]
        assert drain is not None and results[1] is None
        await manager.server_stop_gracefully(replica.server_id)
        await manager.server_start(replica.server_id)
        await manager.get_ready_cql(replicas)
        assert list(await cql.run_async(f"SELECT v FROM {ks}.test_table WHERE pk = 42")) == [(int(is_write),)]


@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
@pytest.mark.parametrize("tablets", [False, True])
@pytest.mark.parametrize("single_partition", [False, True])
@pytest.mark.parametrize("shutdown", [False, True])
async def test_node_shutdown_waits_for_pending_requests(
        manager: ScyllaClusterManager, tablets: bool, single_partition: bool, shutdown: bool) -> None:
    """Regressions for #15899 and #16382: drain must deliver pending read replies.

    Once the draining replica is excluded from new reads, its connections must
    remain open until coordinators have received the replies to existing reads.
    """
    servers = await manager.servers_add(3, auto_rack_dc="dc1")
    ready_cql, hosts = await manager.get_ready_cql(servers)
    await wait_for_feature("GRACEFUL_READ_SHUTDOWN", ready_cql, hosts[0], time.time() + 30)
    coordinator, replica = servers[:2]
    cql = await manager.get_cql_exclusive(coordinator)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} "
                                 f"AND tablets = {{'enabled': {str(tablets).lower()}}}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test_table (pk int PRIMARY KEY) WITH speculative_retry = 'NONE'")
        await cql.run_async(SimpleStatement(f"INSERT INTO {ks}.test_table (pk) VALUES (42)",
                                           consistency_level=ConsistencyLevel.ALL))
        # A separate table lets availability probes run while the original read is paused.
        await cql.run_async(f"CREATE TABLE {ks}.probe (pk int PRIMARY KEY) WITH speculative_retry = 'NONE'")
        injections = [await inject_error_one_shot(manager.api, server.ip_addr, "storage_proxy::handle_read",
                                                  parameters={"cf_name": "test_table"}) for server in servers[1:]]
        where = " WHERE pk = 42" if single_partition else ""
        # A full scan may select new replicas for later batches. QUORUM remains
        # available after drain starts, whereas a new ALL batch cannot proceed.
        cl = ConsistencyLevel.ALL if single_partition else ConsistencyLevel.QUORUM
        pending_read = cql.run_async(SimpleStatement(f"SELECT pk FROM {ks}.test_table{where} USING TIMEOUT 10s",
                                                     consistency_level=cl,
                                                     retry_policy=FallthroughRetryPolicy()), timeout=30)
        waiters = [asyncio.create_task(manager.api.wait_for_injection_enter(server.ip_addr, "storage_proxy::handle_read"))
                   for server in servers[1:]]
        drain = None
        try:
            done, _ = await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED, timeout=30)
            assert done, "read did not reach a remote replica"
            selected = next(iter(done))
            await selected
            replica = servers[1 + waiters.index(selected)]
            operation = manager.server_stop_gracefully(replica.server_id) if shutdown else manager.api.drain(replica.ip_addr)
            drain = asyncio.create_task(operation)

            async def no_longer_selected():
                try:
                    await cql.run_async(SimpleStatement(f"SELECT * FROM {ks}.probe WHERE pk = 42 USING TIMEOUT 1s",
                                                       consistency_level=ConsistencyLevel.ALL,
                                                       retry_policy=FallthroughRetryPolicy()), timeout=2)
                except Unavailable:
                    return True
                return None

            await wait_for(no_longer_selected, time.time() + 30, period=0.1)
            # New reads can still meet QUORUM without contacting the draining replica.
            # Release the other remote replica's injection before probing it.
            other = 1 - (servers.index(replica) - 1)
            await injections[other].message()
            await manager.api.disable_injection(servers[1 + other].ip_addr, "storage_proxy::handle_read")
            assert list(await cql.run_async(SimpleStatement(f"SELECT pk FROM {ks}.test_table{where} USING TIMEOUT 5s",
                                                           consistency_level=ConsistencyLevel.QUORUM,
                                                           retry_policy=FallthroughRetryPolicy()), timeout=10)) == [(42,)]
            assert not drain.done(), "drain closed transport before the pending read completed"
        finally:
            for waiter in waiters:
                waiter.cancel()
            await asyncio.gather(*waiters, return_exceptions=True)
            await asyncio.gather(*(injection.message() for injection in injections), return_exceptions=True)
            await asyncio.gather(*(manager.api.disable_injection(server.ip_addr, "storage_proxy::handle_read")
                                   for server in servers[1:]), return_exceptions=True)
            # Join the REST task on assertion and timeout paths too, before manager teardown.
            pending = [pending_read] + ([drain] if drain is not None else [])
            results = await asyncio.wait_for(asyncio.gather(*pending, return_exceptions=True), timeout=60)
        assert not isinstance(results[0], BaseException), results[0]
        assert list(results[0]) == [(42,)]
        assert drain is not None and results[1] is None
        # Restore the replica so the keyspace can be dropped on all nodes.
        if not shutdown:
            await manager.server_stop_gracefully(replica.server_id)
        await manager.server_start(replica.server_id)
        await manager.get_ready_cql(servers)

        async def selected_after_restart():
            rows = await cql.run_async(SimpleStatement(f"SELECT pk FROM {ks}.test_table{where}",
                                                      consistency_level=ConsistencyLevel.ALL,
                                                      retry_policy=FallthroughRetryPolicy()))
            assert list(rows) == [(42,)]
            return True

        await wait_for(selected_after_restart, time.time() + 30)


@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
@pytest.mark.parametrize("linearizable", [False, True])
@pytest.mark.parametrize("leader", [False, True])
async def test_strong_read_shutdown_waits_for_forwarded_reply(
        manager: ScyllaClusterManager, linearizable: bool, leader: bool) -> None:
    """Regression for #15899: wait for the forwarded reply, after remote execution finishes."""
    config = {"experimental_features": ["strongly-consistent-tables"]}
    # The non-replica coordinator shares a rack with the target, making the first
    # forwarding hop deterministic even when the target is a follower.
    replicas = [await manager.server_add(config=config, cmdline=["--smp=2"],
                                        property_file={"dc": "dc1", "rack": f"r{i}"}) for i in range(3)]
    coordinator = await manager.server_add(config=config | {"join_ring": False},
                                           cmdline=["--smp=2", "--logger-log-level=storage_service=debug"],
                                           property_file={"dc": "dc1", "rack": "r0"})
    ready_cql, hosts = await manager.get_ready_cql(replicas)
    await wait_for_feature("GRACEFUL_READ_SHUTDOWN", ready_cql, hosts[0], time.time() + 30)
    cql = _connect_via_control_connection(manager, coordinator.ip_addr)
    replica = replicas[0]
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} "
                                 "AND tablets = {'initial': 1} AND consistency = 'global'") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test_table (pk int PRIMARY KEY, v int)")
        # Keep the forwarding coordinator's leader cache empty, so a linearizable
        # read also exercises the closest-replica hop when that replica is a follower.
        await ready_cql.run_async(f"INSERT INTO {ks}.test_table (pk, v) VALUES (42, 42)", host=hosts[0])
        table_id = await manager.get_table_id(ks, "test_table")
        rows = await ready_cql.run_async(f"SELECT raft_group_id FROM system.tablets WHERE table_id = {table_id}")
        await ensure_raft_group_leader_on(manager, replicas[0 if leader else 1], str(rows[0].raft_group_id))
        query = f"SELECT v FROM {ks}.test_table WHERE pk = 42 USING TIMEOUT 20s"
        # Cover both QUERY and EXECUTE admission before forwarding a read.
        read = await asyncio.to_thread(cql.prepare, query) if linearizable else SimpleStatement(query)
        read.consistency_level = ConsistencyLevel.QUORUM if linearizable else ConsistencyLevel.ONE
        read.retry_policy = FallthroughRetryPolicy()
        injection = await inject_error_one_shot(manager.api, replica.ip_addr, "forward_cql_pause_before_reply")
        pending_read = cql.run_async(read, timeout=30)
        drain = None
        try:
            await manager.api.wait_for_injection_enter(replica.ip_addr, "forward_cql_pause_before_reply")
            log = await manager.server_open_log(coordinator.server_id)
            mark = await log.mark()
            host_id = await manager.get_host_id(replica.server_id)
            drain = asyncio.create_task(manager.api.drain(replica.ip_addr))
            await log.wait_for(f"Waiting for coordinated requests before {host_id} shuts down", from_mark=mark)
            # New reads must use another replica, while the old reply is held.
            assert list(await cql.run_async(read, timeout=30)) == [(42,)]
            assert not drain.done(), "drain closed messaging before the forwarding coordinator received its reply"
        finally:
            await asyncio.gather(injection.message(), return_exceptions=True)
            await asyncio.gather(manager.api.disable_injection(replica.ip_addr, "forward_cql_pause_before_reply"),
                                 return_exceptions=True)
            results = await asyncio.wait_for(asyncio.gather(pending_read, *([drain] if drain else []),
                                                          return_exceptions=True), timeout=60)
        assert not isinstance(results[0], BaseException), results[0]
        assert list(results[0]) == [(42,)]
        assert drain is not None and results[1] is None
        await manager.server_stop_gracefully(replica.server_id)
        await manager.server_start(replica.server_id)
        await manager.get_ready_cql(replicas)
        assert list(await cql.run_async(read, timeout=30)) == [(42,)]


@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
async def test_strong_read_reselects_after_draining_redirect(manager: ScyllaClusterManager) -> None:
    """Regression for #15899: a delayed redirect must exclude both draining forwarding hops."""
    config = {"experimental_features": ["strongly-consistent-tables"]}
    replicas = [await manager.server_add(config=config, property_file={"dc": "dc1", "rack": f"r{i}"})
                for i in range(3)]
    coordinator = await manager.server_add(config=config | {"join_ring": False},
                                           cmdline=["--logger-log-level=storage_service=debug"],
                                           property_file={"dc": "dc1", "rack": "r0"})
    ready_cql, hosts = await manager.get_ready_cql(replicas)
    await wait_for_feature("GRACEFUL_READ_SHUTDOWN", ready_cql, hosts[0], time.time() + 30)
    cql = _connect_via_control_connection(manager, coordinator.ip_addr)
    first_hop, old_leader, _ = replicas
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} "
                                 "AND tablets = {'initial': 1} AND consistency = 'global'") as ks:
        await ready_cql.run_async(f"CREATE TABLE {ks}.test_table (pk int PRIMARY KEY, v int)")
        await ready_cql.run_async(f"INSERT INTO {ks}.test_table (pk, v) VALUES (42, 42)", host=hosts[0])
        table_id = await manager.get_table_id(ks, "test_table")
        rows = await ready_cql.run_async(f"SELECT raft_group_id FROM system.tablets WHERE table_id = {table_id}")
        await ensure_raft_group_leader_on(manager, old_leader, str(rows[0].raft_group_id))
        redirect = await inject_error_one_shot(manager.api, first_hop.ip_addr, "forward_cql_pause_before_reply")
        forbidden = await inject_error_one_shot(manager.api, old_leader.ip_addr, "wait_before_handling_forwarded_request")
        read = cql.run_async(SimpleStatement(f"SELECT v FROM {ks}.test_table WHERE pk = 42 USING TIMEOUT 20s",
                                            consistency_level=ConsistencyLevel.QUORUM,
                                            retry_policy=FallthroughRetryPolicy()), timeout=30)
        drains = []
        try:
            await manager.api.wait_for_injection_enter(first_hop.ip_addr, "forward_cql_pause_before_reply")
            log = await manager.server_open_log(coordinator.server_id)
            mark = await log.mark()
            for server in [old_leader, first_hop]:
                host_id = await manager.get_host_id(server.server_id)
                drains.append(asyncio.create_task(manager.api.drain(server.ip_addr)))
                # This callback runs only after the marker is visible on all shards.
                await log.wait_for(f"Waiting for coordinated requests before {host_id} shuts down", from_mark=mark, timeout=10)
            await redirect.message()
            assert list(await read) == [(42,)]
            assert await manager.api.get_injection_enter_count(old_leader.ip_addr, "wait_before_handling_forwarded_request") == 0
        finally:
            await asyncio.gather(redirect.message(), forbidden.message(), return_exceptions=True)
            await asyncio.gather(manager.api.disable_injection(first_hop.ip_addr, "forward_cql_pause_before_reply"),
                                 manager.api.disable_injection(old_leader.ip_addr, "wait_before_handling_forwarded_request"),
                                 return_exceptions=True)
            await asyncio.wait_for(asyncio.gather(read, *drains, return_exceptions=True), timeout=60)
            for server in [old_leader, first_hop]:
                await manager.server_stop_gracefully(server.server_id)
                await manager.server_start(server.server_id)
            await manager.get_ready_cql(replicas)


@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
async def test_strong_read_waits_for_draining_leader_change(manager: ScyllaClusterManager) -> None:
    """Regression for #15899: a draining leader must not cause repeated Raft discovery per read."""
    servers = await manager.servers_add(3, config={"experimental_features": ["strongly-consistent-tables"]}, auto_rack_dc="dc1")
    leader, follower, _ = servers
    ready_cql, hosts = await manager.get_ready_cql(servers)
    await wait_for_feature("GRACEFUL_READ_SHUTDOWN", ready_cql, hosts[0], time.time() + 30)
    cql = await manager.get_cql_exclusive(follower)
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} "
                                 "AND tablets = {'initial': 1} AND consistency = 'global'") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test_table (pk int PRIMARY KEY, v int)")
        await cql.run_async(f"INSERT INTO {ks}.test_table (pk, v) VALUES (42, 42)")
        table_id = await manager.get_table_id(ks, "test_table")
        rows = await cql.run_async(f"SELECT raft_group_id FROM system.tablets WHERE table_id = {table_id}")
        await ensure_raft_group_leader_on(manager, leader, str(rows[0].raft_group_id))
        reply = await inject_error_one_shot(manager.api, leader.ip_addr, "forward_cql_pause_before_reply")
        await manager.api.enable_injection(leader.ip_addr, "strong_consistency_fail_leadership_transfer", one_shot=False)
        query = f"SELECT v FROM {ks}.test_table WHERE pk = 42"
        pending = cql.run_async(SimpleStatement(f"{query} USING TIMEOUT 20s", consistency_level=ConsistencyLevel.QUORUM,
                                                retry_policy=FallthroughRetryPolicy()), timeout=30)
        drain = None
        resuming_read = None
        try:
            await manager.api.wait_for_injection_enter(leader.ip_addr, "forward_cql_pause_before_reply")
            log = await manager.server_open_log(follower.server_id)
            await manager.api.set_logger_level(follower.ip_addr, "storage_service", "debug")
            mark = await log.mark()
            drain = asyncio.create_task(manager.api.drain(leader.ip_addr))
            host_id = await manager.get_host_id(leader.server_id)
            await log.wait_for(f"Waiting for coordinated requests before {host_id} shuts down", from_mark=mark, timeout=10)

            async def stale_leader_retries():
                metrics = await manager.metrics.query(follower.ip_addr)
                return metrics.get("scylla_strong_consistency_coordinator_read_retries", labels={"reason": "stale_leader"}) or 0

            await manager.api.set_logger_level(follower.ip_addr, "raft", "trace")
            discovery_mark = await log.mark()
            before = await stale_leader_retries()
            # Each read must wait under its own deadline without rediscovering
            # the same live leader. The held reply keeps the draining node alive.
            async def timed_read():
                with pytest.raises(ReadTimeout):
                    await cql.run_async(SimpleStatement(f"{query} USING TIMEOUT 1s", consistency_level=ConsistencyLevel.QUORUM,
                                                       retry_policy=FallthroughRetryPolicy()), timeout=5)

            await asyncio.gather(*(timed_read() for _ in range(4)))
            # A deadline may expire before a concurrent request reaches routing.
            # Each read that reaches the leader check must wait at most once.
            assert 1 <= await stale_leader_retries() - before <= 4
            assert not await log.grep(f"sc-{rows[0].raft_group_id}.*searching for a leader", from_mark=discovery_mark)
            assert not drain.done()

            # The queried replica stays a follower throughout the transfer.
            # Raft's role-change notification alone does not cover this case.
            await manager.api.set_logger_level(follower.ip_addr, "sc_coordinator", "debug")
            mark = await log.mark()
            resuming_read = cql.run_async(SimpleStatement(f"{query} USING TIMEOUT 5s", consistency_level=ConsistencyLevel.QUORUM,
                                                         retry_policy=FallthroughRetryPolicy()), timeout=10)
            await log.wait_for("reported leader .*waiting for a new leader", from_mark=mark, timeout=5)
            successor = await manager.get_host_id(servers[2].server_id)
            await trigger_stepdown(manager, leader, str(rows[0].raft_group_id), target_host_id=successor)
            assert list(await resuming_read) == [(42,)]
        finally:
            await reply.message()
            await manager.api.disable_injection(leader.ip_addr, "forward_cql_pause_before_reply")
            await manager.api.disable_injection(leader.ip_addr, "strong_consistency_fail_leadership_transfer")
            await asyncio.wait_for(asyncio.gather(pending, *([drain] if drain else []),
                                                *([resuming_read] if resuming_read is not None else []), return_exceptions=True), timeout=60)
            await manager.api.set_logger_level(follower.ip_addr, "storage_service", "info")
            await manager.api.set_logger_level(follower.ip_addr, "raft", "info")
            await manager.api.set_logger_level(follower.ip_addr, "sc_coordinator", "info")
            await manager.server_stop_gracefully(leader.server_id)
            await manager.server_start(leader.server_id)
            await manager.get_ready_cql(servers)


@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
async def test_strong_read_bounds_unsynchronized_leader_redirects(manager: ScyllaClusterManager) -> None:
    """Regression for #15899: origin-only leader exclusion must not cause a forwarding loop."""
    config = {"experimental_features": ["strongly-consistent-tables"]}
    replicas = [await manager.server_add(config=config, property_file={"dc": "dc1", "rack": f"r{i}"})
                for i in range(3)]
    coordinator = await manager.server_add(config=config | {"join_ring": False}, property_file={"dc": "dc1", "rack": "r0"})
    leader = replicas[1]
    ready_cql, hosts = await manager.get_ready_cql(replicas)
    await wait_for_feature("GRACEFUL_READ_SHUTDOWN", ready_cql, hosts[0], time.time() + 30)
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} "
                                 "AND tablets = {'initial': 1} AND consistency = 'global'") as ks:
        await ready_cql.run_async(f"CREATE TABLE {ks}.test_table (pk int PRIMARY KEY, v int)")
        await ready_cql.run_async(f"INSERT INTO {ks}.test_table (pk, v) VALUES (42, 42)")
        table_id = await manager.get_table_id(ks, "test_table")
        rows = await ready_cql.run_async(f"SELECT raft_group_id FROM system.tablets WHERE table_id = {table_id}")
        await ensure_raft_group_leader_on(manager, leader, str(rows[0].raft_group_id))
        await manager.server_stop_gracefully(coordinator.server_id)
        host_id = await manager.get_host_id(leader.server_id)
        await manager.server_update_config(coordinator.server_id, "error_injections_at_startup",
                                           [{"name": "gossip_get_states_connection_closed", "one_shot": False, "value": host_id}])
        injections = ["gossip_pause_echo_reply", "gossip_fail_up_notification"]
        for name in injections:
            await manager.api.enable_injection(leader.ip_addr, name, one_shot=False)
        try:
            await manager.server_start(coordinator.server_id)
            cql = _connect_via_control_connection(manager, coordinator.ip_addr)
            await manager.api.message_injection(leader.ip_addr, "gossip_pause_echo_reply")
            await manager.api.disable_injection(leader.ip_addr, "gossip_pause_echo_reply")

            async def leader_alive():
                return str(leader.ip_addr) in await manager.api.get_alive_endpoints(coordinator.ip_addr) or None

            await wait_for(leader_alive, time.time() + 30)
            before = (await manager.metrics.query(coordinator.ip_addr)).get("scylla_transport_requests_forwarded_redirected") or 0
            statement = SimpleStatement(f"SELECT v FROM {ks}.test_table WHERE pk = 42 USING TIMEOUT 1s",
                                        consistency_level=ConsistencyLevel.QUORUM, retry_policy=FallthroughRetryPolicy())
            with pytest.raises(Unavailable):
                await cql.run_async(statement, timeout=5)
            after = (await manager.metrics.query(coordinator.ip_addr)).get("scylla_transport_requests_forwarded_redirected") or 0
            assert after - before <= 3
        finally:
            await manager.api.message_injection(leader.ip_addr, "gossip_pause_echo_reply")
            for name in injections:
                await manager.api.disable_injection(leader.ip_addr, name)
            await manager.api.disable_injection(coordinator.ip_addr, "gossip_get_states_connection_closed")

        async def synchronized():
            try:
                assert list(await cql.run_async(statement, timeout=5)) == [(42,)]
                return True
            except Unavailable:
                return None

        await wait_for(synchronized, time.time() + 30)


@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
async def test_read_shutdown_rolling_upgrade(
        manager: ScyllaClusterManager, scylla_2026_1: ScyllaVersionDescription, scylla_binary: Path) -> None:
    """The fix for #15899 must keep the legacy shutdown path until the last node is upgraded."""
    feature = "GRACEFUL_READ_SHUTDOWN"
    servers = await manager.servers_add(2, version=scylla_2026_1, auto_rack_dc="dc1")
    coordinator, replica = servers
    current_version = get_current_version_description(str(scylla_binary))
    cql, hosts = await manager.get_ready_cql(servers)
    for host in hosts:
        assert feature not in await get_supported_features(cql, host)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2} "
                                 "AND tablets = {'enabled': false}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test_table (pk int PRIMARY KEY) WITH speculative_retry = 'NONE'")
        await cql.run_async(f"CREATE TABLE {ks}.probe (pk int PRIMARY KEY)")
        await cql.run_async(SimpleStatement(f"INSERT INTO {ks}.test_table (pk) VALUES (42)",
                                           consistency_level=ConsistencyLevel.ALL))
        read = SimpleStatement(f"SELECT pk FROM {ks}.test_table WHERE pk = 42 USING TIMEOUT 10s",
                               consistency_level=ConsistencyLevel.ALL, retry_policy=FallthroughRetryPolicy())
        for host in hosts:
            assert list(await cql.run_async(read, host=host)) == [(42,)]

        await manager.server_change_version(replica.server_id, str(scylla_binary), current_version)
        await reconnect_driver(manager)
        cql, hosts = await manager.get_ready_cql(servers)
        assert feature not in await get_supported_features(cql, hosts[0])
        assert feature in await get_supported_features(cql, hosts[1])
        for host in hosts:
            assert feature not in await get_enabled_features(cql, host)
        # Exercise internode reads in both directions while the binaries differ.
        for host in hosts:
            assert list(await cql.run_async(read, host=host)) == [(42,)]

        log = await manager.server_open_log(replica.server_id)
        mark = await log.mark()
        await manager.server_stop_gracefully(replica.server_id)
        # An upgraded replica must not send the new RPC to the old coordinator.
        assert not await log.grep("Preparing shutdown:", from_mark=mark)
        await manager.server_start(replica.server_id)
        cql, hosts = await manager.get_ready_cql(servers)
        for host in hosts:
            assert feature not in await get_enabled_features(cql, host)
        assert list(await cql.run_async(read, host=hosts[0])) == [(42,)]

        # The old node's shadow request omits the new optional endpoint filter.
        await manager.server_restart(coordinator.server_id)
        cql, hosts = await manager.get_ready_cql(servers)
        for host in hosts:
            assert feature not in await get_enabled_features(cql, host)
            assert list(await cql.run_async(read, host=host)) == [(42,)]

        await manager.server_change_version(coordinator.server_id, str(scylla_binary), current_version)
        await reconnect_driver(manager)
        cql, hosts = await manager.get_ready_cql(servers)
        await asyncio.gather(*(wait_for_feature(feature, cql, host, time.time() + 60) for host in hosts))
        injection = await inject_error_one_shot(manager.api, replica.ip_addr, "storage_proxy::handle_read",
                                               parameters={"cf_name": "test_table"})
        pending_read = cql.run_async(read, host=hosts[0], timeout=30)
        stop = None
        try:
            await manager.api.wait_for_injection_enter(replica.ip_addr, "storage_proxy::handle_read")
            stop = asyncio.create_task(manager.server_stop_gracefully(replica.server_id))

            async def no_longer_selected():
                try:
                    await cql.run_async(SimpleStatement(f"SELECT * FROM {ks}.probe WHERE pk = 42 USING TIMEOUT 1s",
                                                       consistency_level=ConsistencyLevel.ALL,
                                                       retry_policy=FallthroughRetryPolicy()), host=hosts[0], timeout=2)
                except Unavailable:
                    return True
                return None

            await wait_for(no_longer_selected, time.time() + 30, period=0.1)
            assert not stop.done(), "upgraded replica shut down before the coordinator received its pending read"
        finally:
            await asyncio.gather(injection.message(), return_exceptions=True)
            await asyncio.gather(manager.api.disable_injection(replica.ip_addr, "storage_proxy::handle_read"),
                                 return_exceptions=True)
            pending = [pending_read] + ([stop] if stop is not None else [])
            results = await asyncio.wait_for(asyncio.gather(*pending, return_exceptions=True), timeout=60)
        assert not isinstance(results[0], BaseException), results[0]
        assert list(results[0]) == [(42,)]
        assert stop is not None and results[1] is None
        await manager.server_start(replica.server_id)
        cql, hosts = await manager.get_ready_cql(servers)
        assert list(await cql.run_async(read, host=hosts[0])) == [(42,)]


@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
@pytest.mark.parametrize("zero_token", [False, True])
async def test_read_shutdown_feature_gate(manager: ScyllaClusterManager, zero_token: bool) -> None:
    """Offline and zero-token members count for #15899's feature gate; enabled features block rollback."""
    feature = "GRACEFUL_READ_SHUTDOWN"
    unsupported = {"error_injections_at_startup": [{"name": "suppress_features", "value": feature}]}
    first = await manager.server_add(config=unsupported, property_file={"dc": "dc1", "rack": "r0"})
    second = await manager.server_add(property_file={"dc": "dc1", "rack": "r1"})
    third = await manager.server_add(config=unsupported | {"join_ring": False} if zero_token else {},
                                     property_file={"dc": "dc1", "rack": "r2"})
    servers = [first, second, third]
    if zero_token:
        await manager.server_update_config(first.server_id, "error_injections_at_startup", [])
        await manager.server_restart(first.server_id)
    old = third if zero_token else first
    await manager.server_stop_gracefully(old.server_id)
    live = [server for server in servers if server != old]
    cql, hosts = await manager.get_ready_cql(live)
    await read_barrier(manager.api, live[0].ip_addr)
    for host in hosts:
        assert feature in await get_supported_features(cql, host)
        assert feature not in await get_enabled_features(cql, host)
    await manager.server_update_config(old.server_id, "error_injections_at_startup", [])
    await manager.server_start(old.server_id)
    cql, hosts = await manager.get_ready_cql(servers[:2] if zero_token else servers)
    await asyncio.gather(*(wait_for_feature(feature, cql, host, time.time() + 60) for host in hosts))
    await manager.server_stop_gracefully(old.server_id)
    await manager.server_update_config(old.server_id, "error_injections_at_startup", unsupported["error_injections_at_startup"])
    await manager.server_start(old.server_id,
                               expected_error="Feature 'GRACEFUL_READ_SHUTDOWN' was previously enabled in the cluster")


@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
async def test_read_shutdown_waits_for_background_replies(manager: ScyllaClusterManager) -> None:
    """Regression for #15899: a successful foreground read can still have outstanding replica replies."""
    servers = await manager.servers_add(3, auto_rack_dc="dc1")
    ready_cql, hosts = await manager.get_ready_cql(servers)
    await wait_for_feature("GRACEFUL_READ_SHUTDOWN", ready_cql, hosts[0], time.time() + 30)
    coordinator, _, replica = servers
    cql = await manager.get_cql_exclusive(coordinator)
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} "
                                 "AND tablets = {'enabled': false}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test_table (pk int PRIMARY KEY) WITH speculative_retry = 'ALWAYS'")
        await cql.run_async(SimpleStatement(f"INSERT INTO {ks}.test_table (pk) VALUES (42)",
                                           consistency_level=ConsistencyLevel.ALL))
        injection = await inject_error_one_shot(manager.api, replica.ip_addr, "storage_proxy::handle_read",
                                               parameters={"cf_name": "test_table"})
        read = cql.run_async(SimpleStatement(f"SELECT pk FROM {ks}.test_table WHERE pk = 42 USING TIMEOUT 20s",
                                            consistency_level=ConsistencyLevel.QUORUM,
                                            retry_policy=FallthroughRetryPolicy()), timeout=30)
        drain = None
        try:
            await manager.api.wait_for_injection_enter(replica.ip_addr, "storage_proxy::handle_read")
            assert list(await read) == [(42,)]
            log = await manager.server_open_log(replica.server_id)
            mark = await log.mark()
            await manager.api.enable_injection(coordinator.ip_addr, "storage_proxy_drain_reads_pending", one_shot=False)
            drain = asyncio.create_task(manager.api.drain(replica.ip_addr))
            await log.wait_for("Preparing shutdown: waiting for coordinators", from_mark=mark)
            # Observe the coordinator's unresolved read barrier after delivery
            # of the foreground result, while the replica reply is still held.
            await manager.api.wait_for_injection_enter(coordinator.ip_addr, "storage_proxy_drain_reads_pending",
                                                       deadline=time.time() + 10)
            assert not drain.done(), "drain ignored a reply still needed by background read work"
        finally:
            await manager.api.disable_injection(coordinator.ip_addr, "storage_proxy_drain_reads_pending")
            await injection.message()
            await asyncio.wait_for(asyncio.gather(read, *([drain] if drain else [])), timeout=60)
        await manager.server_stop_gracefully(replica.server_id)
        await manager.server_start(replica.server_id)
        await manager.get_ready_cql(servers)


@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
async def test_range_read_shutdown_retains_selected_replica_reply(manager: ScyllaClusterManager) -> None:
    """Regression for #15899: selection before drain must retain later RPCs after the foreground fails."""
    servers = await manager.servers_add(3, auto_rack_dc="dc1")
    ready_cql, hosts = await manager.get_ready_cql(servers)
    await wait_for_feature("GRACEFUL_READ_SHUTDOWN", ready_cql, hosts[0], time.time() + 30)
    coordinator, failing_replica, draining_replica = servers
    cql = await manager.get_cql_exclusive(coordinator)
    # One tablet keeps the selected range from being merged or followed by a
    # later batch that legitimately excludes the draining replica at ALL.
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} "
                                 "AND tablets = {'initial': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test_table (pk int PRIMARY KEY)")
        await cql.run_async(SimpleStatement(f"INSERT INTO {ks}.test_table (pk) VALUES (42)",
                                           consistency_level=ConsistencyLevel.ALL))
        selection_name = "query_partition_key_range_concurrent_after_selection"
        selection = await inject_error_one_shot(manager.api, coordinator.ip_addr, selection_name,
                                               parameters={"cf_name": "test_table"})
        reply = await inject_error_one_shot(manager.api, draining_replica.ip_addr, "storage_proxy::handle_read",
                                           parameters={"cf_name": "test_table"})
        await manager.api.enable_injection(failing_replica.ip_addr, "storage_proxy::handle_read", one_shot=True,
                                           parameters={"cf_name": "test_table", "what": "throw"})
        await manager.api.enable_injection(coordinator.ip_addr, "storage_proxy_drain_reads_pending", one_shot=False)
        read = cql.run_async(SimpleStatement(f"SELECT pk FROM {ks}.test_table USING TIMEOUT 20s",
                                            consistency_level=ConsistencyLevel.ALL,
                                            retry_policy=FallthroughRetryPolicy()), timeout=30)
        drain = None
        try:
            await manager.api.wait_for_injection_enter(coordinator.ip_addr, selection_name)
            log = await manager.server_open_log(draining_replica.server_id)
            mark = await log.mark()
            drain = asyncio.create_task(manager.api.drain(draining_replica.ip_addr))
            await manager.api.wait_for_injection_enter(coordinator.ip_addr, "storage_proxy_drain_reads_pending",
                                                       deadline=time.time() + 10)
            # The barrier has advanced after selection but before executor
            # construction. The executor must inherit that earlier operation.
            await selection.message()
            await manager.api.wait_for_injection_enter(draining_replica.ip_addr, "storage_proxy::handle_read")
            with pytest.raises(ReadFailure):
                await read
            # The CQL request's holder is gone. The selected replica's pending
            # RPC must still keep the coordinator's acknowledgement outstanding.
            # Check preparation itself: later teardown can hide an early reply
            # even when the full drain task has not completed yet.
            with pytest.raises(TimeoutError):
                await log.wait_for("Preparing shutdown: coordinator request wait finished", from_mark=mark, timeout=1)
            assert not drain.done(), "drain lost a selected replica's reply after the foreground failed"
        finally:
            await asyncio.gather(selection.message(), reply.message(), return_exceptions=True)
            await asyncio.gather(manager.api.disable_injection(coordinator.ip_addr, selection_name),
                                 manager.api.disable_injection(coordinator.ip_addr, "storage_proxy_drain_reads_pending"),
                                 *(manager.api.disable_injection(server.ip_addr, "storage_proxy::handle_read")
                                   for server in servers[1:]), return_exceptions=True)
            results = await asyncio.wait_for(asyncio.gather(read, *([drain] if drain else []), return_exceptions=True), timeout=60)
        assert isinstance(results[0], ReadFailure), results[0]
        assert drain is not None and results[1] is None
        await manager.server_stop_gracefully(draining_replica.server_id)
        await manager.server_start(draining_replica.server_id)
        await manager.get_ready_cql(servers)


@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
@pytest.mark.parametrize("restart,shutdown", [(False, False), (True, False), (True, True)])
async def test_new_coordinator_learns_read_shutdown(manager: ScyllaClusterManager, restart: bool, shutdown: bool) -> None:
    """A coordinator joining or restarting during #15899's wait must learn the drain marker."""
    servers = await manager.servers_add(2, auto_rack_dc="dc1")
    newcomer = await manager.server_add(config={"join_ring": False}, property_file={"dc": "dc1", "rack": "r2"}) if restart else None
    ready_cql, hosts = await manager.get_ready_cql(servers)
    await wait_for_feature("GRACEFUL_READ_SHUTDOWN", ready_cql, hosts[0], time.time() + 30)
    coordinator, replica = servers[:2]
    cql = await manager.get_cql_exclusive(coordinator)
    if restart:
        await manager.server_stop_gracefully(newcomer.server_id)
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2} "
                                 "AND tablets = {'enabled': false}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test_table (pk int PRIMARY KEY) WITH speculative_retry = 'NONE'")
        # Keep a single existing read pending. A scan could start later batches
        # after the marker and correctly fail ALL when the replica is excluded.
        injection = await inject_error_one_shot(manager.api, replica.ip_addr, "storage_proxy::handle_read",
                                               parameters={"cf_name": "test_table"})
        pending_read = cql.run_async(SimpleStatement(f"SELECT pk FROM {ks}.test_table WHERE pk = 42 USING TIMEOUT 30s",
                                                     consistency_level=ConsistencyLevel.ALL,
                                                     retry_policy=FallthroughRetryPolicy()), timeout=40)
        drain = None
        try:
            await manager.api.wait_for_injection_enter(replica.ip_addr, "storage_proxy::handle_read")
            drain = asyncio.create_task(manager.server_stop_gracefully(replica.server_id) if shutdown
                                        else manager.api.drain(replica.ip_addr))
            log = await manager.server_open_log(replica.server_id)
            await log.wait_for("Preparing shutdown: waiting for coordinators")
            if restart:
                assert newcomer is not None
                await manager.server_start(newcomer.server_id)
            else:
                newcomer = await manager.server_add(config={"join_ring": False}, property_file={"dc": "dc1", "rack": "r2"})
            # This coordinator was offline when the test schema was created.
            await read_barrier(manager.api, newcomer.ip_addr)
            new_cql = _connect_via_control_connection(manager, newcomer.ip_addr)
            with pytest.raises(Unavailable):
                await new_cql.run_async(SimpleStatement(f"SELECT pk FROM {ks}.test_table USING TIMEOUT 2s",
                                                       consistency_level=ConsistencyLevel.ALL,
                                                       retry_policy=FallthroughRetryPolicy()), timeout=5)
            assert not drain.done(), "drain ended while an existing coordinator still needed a reply"
        finally:
            await injection.message()
            results = await asyncio.wait_for(asyncio.gather(pending_read, *([drain] if drain else []),
                                                          return_exceptions=True), timeout=60)
        assert not isinstance(results[0], BaseException), results[0]
        if not shutdown:
            await manager.server_stop_gracefully(replica.server_id)
        # Readiness changes can close an RPC connection during restart. The
        # startup exchange must reconnect before admitting queries.
        await manager.server_update_config(replica.server_id, "error_injections_at_startup",
                                           [{"name": "gossip_get_states_connection_closed", "one_shot": True}])
        await manager.server_start(replica.server_id)
        await manager.get_ready_cql(servers)


@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
@pytest.mark.parametrize("rebuilding_peer", [False, True])
async def test_new_coordinator_requires_up_from_responsive_peer(manager: ScyllaClusterManager, rebuilding_peer: bool) -> None:
    """Regression for #15899: responsive normal and rebuilding peers must confirm UP or supply a drain marker."""
    servers = await manager.servers_add(3 if rebuilding_peer else 2, auto_rack_dc="dc1")
    cql, hosts = await manager.get_ready_cql(servers)
    await wait_for_feature("GRACEFUL_READ_SHUTDOWN", cql, hosts[0], time.time() + 30)
    peer, coordinator = servers[:2]
    rebuild = None
    transition = None
    start = None
    echo_paused = False
    up_notification_failed = False
    try:
        if rebuilding_peer:
            topology_coordinator = await get_coordinator_host(manager)
            coordinator = next(server for server in servers if server != topology_coordinator)
            peer = next(server for server in servers if server not in [coordinator, topology_coordinator])
            transition = await inject_error_one_shot(manager.api, topology_coordinator.ip_addr,
                                                    "topology_coordinator_pause_after_node_transition")
            rebuild = asyncio.create_task(manager.api.rebuild_node(peer.ip_addr, timeout=120))
            await manager.api.wait_for_injection_enter(topology_coordinator.ip_addr, "topology_coordinator_pause_after_node_transition")
            # Persist the rebuilding state before stopping the coordinator, so its
            # startup peer snapshot cannot still contain the earlier NORMAL state.
            await read_barrier(manager.api, coordinator.ip_addr)
            transition_cql = await manager.get_cql_exclusive(coordinator)
            peer_id = await manager.get_host_id(peer.server_id)
            rows = await transition_cql.run_async(
                f"SELECT node_state FROM system.topology WHERE key = 'topology' AND host_id = {peer_id}")
            assert rows[0].node_state == "rebuilding"
        await manager.server_stop_gracefully(coordinator.server_id)
        # Keep the peer locally DOWN on the restarting coordinator, while the
        # endpoint-state RPC can still return its unmarked state. A SIGTERM before
        # marker publication can fail the UP acknowledgement in this same window.
        await manager.api.enable_injection(peer.ip_addr, "gossip_pause_echo_reply", one_shot=False)
        echo_paused = True
        await manager.api.enable_injection(peer.ip_addr, "gossip_fail_up_notification", one_shot=False)
        up_notification_failed = True
        log = await manager.server_open_log(coordinator.server_id)
        mark = await log.mark()
        start = asyncio.create_task(manager.server_start(coordinator.server_id, expected_error="Injected UP notification failure"))
        await log.wait_for("Failed to notify node .* that I am UP", from_mark=mark, timeout=10)
        assert await manager.api.get_injection_enter_count(peer.ip_addr, "gossip_pause_echo_reply") > 0
    finally:
        try:
            if echo_paused:
                await manager.api.message_injection(peer.ip_addr, "gossip_pause_echo_reply")
                await manager.api.disable_injection(peer.ip_addr, "gossip_pause_echo_reply")
            if up_notification_failed:
                await manager.api.disable_injection(peer.ip_addr, "gossip_fail_up_notification")
        finally:
            try:
                if transition is not None:
                    await transition.message()
            finally:
                await asyncio.wait_for(asyncio.gather(*([start] if start is not None else []),
                                                      *([rebuild] if rebuild is not None else []),
                                                      return_exceptions=True), timeout=120)
    await start
    await manager.server_start(coordinator.server_id)
    await manager.get_ready_cql(servers)


@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
@pytest.mark.parametrize("hold_endpoint,rebuild_peer", [(False, False), (True, False), (False, True)])
async def test_new_coordinator_quarantines_unsynchronized_peer(
        manager: ScyllaClusterManager, hold_endpoint: bool, rebuild_peer: bool) -> None:
    """Regression for #15899: ordinary echo recovery must not bypass a failed startup exchange."""
    servers = await manager.servers_add(2, auto_rack_dc="dc1")
    cql, hosts = await manager.get_ready_cql(servers)
    await wait_for_feature("GRACEFUL_READ_SHUTDOWN", cql, hosts[0], time.time() + 30)
    peer, coordinator = servers
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2} "
                                 "AND tablets = {'enabled': false}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test_table (pk int PRIMARY KEY)")
        await manager.server_stop_gracefully(coordinator.server_id)
        await manager.server_update_config(coordinator.server_id, "error_injections_at_startup",
                                           [{"name": "gossip_get_states_connection_closed", "one_shot": False}])
        injections = ["gossip_pause_echo_reply", "gossip_fail_up_notification"]
        for name in injections:
            await manager.api.enable_injection(peer.ip_addr, name, one_shot=False)
        rebuild = None
        transition_coordinator = None
        transition_pause = "topology_coordinator_pause_after_node_transition"
        try:
            await manager.server_start(coordinator.server_id)
            new_cql = await manager.get_cql_exclusive(coordinator)
            if hold_endpoint:
                await manager.api.enable_injection(coordinator.ip_addr, "gossiper_hold_mark_alive_endpoint", one_shot=True)
            # The startup RPCs both failed while the peer was DOWN. Permit its
            # ordinary echo to succeed without permitting either synchronization RPC.
            await manager.api.message_injection(peer.ip_addr, "gossip_pause_echo_reply")
            await manager.api.disable_injection(peer.ip_addr, "gossip_pause_echo_reply")

            async def peer_alive():
                return str(peer.ip_addr) in await manager.api.get_alive_endpoints(coordinator.ip_addr) or None

            await wait_for(peer_alive, time.time() + 30)
            statement = SimpleStatement(f"SELECT * FROM {ks}.test_table WHERE pk = 42 USING TIMEOUT 2s",
                                        consistency_level=ConsistencyLevel.ALL, retry_policy=FallthroughRetryPolicy())

            async def synchronized():
                try:
                    await new_cql.run_async(statement, timeout=5)
                    return True
                except Unavailable:
                    return None

            with pytest.raises(Unavailable):
                await new_cql.run_async(statement, timeout=5)
            if hold_endpoint:
                # Hold the peer's actual endpoint lock while the retry applies
                # its snapshot. The exchange deadline must cancel this lock wait.
                await manager.api.wait_for_injection_enter(coordinator.ip_addr, "gossiper_hold_mark_alive_endpoint")
                log = await manager.server_open_log(coordinator.server_id)
                await manager.api.set_logger_level(coordinator.ip_addr, "gossip", "debug")
                mark = await log.mark()
                await manager.api.disable_injection(peer.ip_addr, "gossip_fail_up_notification")
                await manager.api.disable_injection(coordinator.ip_addr, "gossip_get_states_connection_closed")
                waiting_mark, _ = await log.wait_for("do_apply_state_locally.*lock_endpoint.*waiting", from_mark=mark, timeout=15)
                await log.wait_for("Read-shutdown synchronization with .* failed: .*semaphore_aborted", from_mark=waiting_mark, timeout=15)
            if rebuild_peer:
                transition_coordinator = await get_coordinator_host(manager)
                await manager.api.enable_injection(transition_coordinator.ip_addr, transition_pause, one_shot=True)
                rebuild = asyncio.create_task(manager.api.rebuild_node(peer.ip_addr, timeout=120))
                await manager.api.wait_for_injection_enter(transition_coordinator.ip_addr, transition_pause)
                await read_barrier(manager.api, coordinator.ip_addr)
                peer_id = await manager.get_host_id(peer.server_id)
                state = await new_cql.run_async(
                    f"SELECT node_state FROM system.topology WHERE key = 'topology' AND host_id = {peer_id}")
                assert state[0].node_state == "rebuilding"
                log = await manager.server_open_log(coordinator.server_id)
                await manager.api.set_logger_level(coordinator.ip_addr, "gossip", "debug")
                mark = await log.mark()
                # Rebuilding peers still serve reads, but are in transition_nodes.
                # Observe a failed retry there before allowing the next one to succeed.
                await log.wait_for("Read-shutdown synchronization with .* failed:", from_mark=mark, timeout=20)
                await manager.api.disable_injection(peer.ip_addr, "gossip_fail_up_notification")
                await manager.api.disable_injection(coordinator.ip_addr, "gossip_get_states_connection_closed")
                await wait_for(synchronized, time.time() + 30)
        finally:
            await manager.api.message_injection(peer.ip_addr, "gossip_pause_echo_reply")
            for name in injections:
                await manager.api.disable_injection(peer.ip_addr, name)
            await manager.api.disable_injection(coordinator.ip_addr, "gossip_get_states_connection_closed")
            await manager.api.disable_injection(coordinator.ip_addr, "gossiper_hold_mark_alive_endpoint")
            if rebuild is not None:
                await manager.api.message_injection(transition_coordinator.ip_addr, transition_pause)
                await manager.api.disable_injection(transition_coordinator.ip_addr, transition_pause)
                await rebuild
        # A successful background exchange must restore read availability.
        await wait_for(synchronized, time.time() + 30)


@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
async def test_rebuilding_replica_shutdown_waits_for_read_reply(manager: ScyllaClusterManager) -> None:
    """Regression for #15899: rebuilding replicas still own data and must prepare before SIGTERM."""
    servers = await manager.servers_add(3, auto_rack_dc="dc1")
    ready_cql, hosts = await manager.get_ready_cql(servers)
    await wait_for_feature("GRACEFUL_READ_SHUTDOWN", ready_cql, hosts[0], time.time() + 30)
    coordinator = await get_coordinator_host(manager)
    replica = next(server for server in servers if server != coordinator)
    cql = await manager.get_cql_exclusive(coordinator)
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} "
                                 "AND tablets = {'enabled': false}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test_table (pk int PRIMARY KEY) WITH speculative_retry = 'NONE'")
        await cql.run_async(SimpleStatement(f"INSERT INTO {ks}.test_table (pk) VALUES (42)",
                                           consistency_level=ConsistencyLevel.ALL))
        transition = await inject_error_one_shot(manager.api, coordinator.ip_addr,
                                                "topology_coordinator_pause_after_node_transition")
        rebuild = asyncio.create_task(manager.api.rebuild_node(replica.ip_addr, timeout=120))
        reply = None
        read = None
        stop = None
        try:
            await manager.api.wait_for_injection_enter(coordinator.ip_addr, "topology_coordinator_pause_after_node_transition")
            await read_barrier(manager.api, replica.ip_addr)
            host_id = await manager.get_host_id(replica.server_id)
            rows = await cql.run_async(f"SELECT node_state FROM system.topology WHERE key = 'topology' AND host_id = {host_id}")
            assert rows[0].node_state == "rebuilding"
            reply = await inject_error_one_shot(manager.api, replica.ip_addr, "storage_proxy::handle_read",
                                               parameters={"cf_name": "test_table"})
            read = cql.run_async(SimpleStatement(f"SELECT pk FROM {ks}.test_table WHERE pk = 42 USING TIMEOUT 20s",
                                                consistency_level=ConsistencyLevel.ALL,
                                                retry_policy=FallthroughRetryPolicy()), timeout=30)
            await manager.api.wait_for_injection_enter(replica.ip_addr, "storage_proxy::handle_read")
            await manager.api.enable_injection(coordinator.ip_addr, "storage_proxy_drain_reads_pending", one_shot=False)
            stop = asyncio.create_task(manager.server_stop_gracefully(replica.server_id))
            await manager.api.wait_for_injection_enter(coordinator.ip_addr, "storage_proxy_drain_reads_pending",
                                                       deadline=time.time() + 10)
            assert not stop.done(), "SIGTERM skipped read preparation while the replica was rebuilding"
        finally:
            # Let rebuild leave its temporary topology state before finishing shutdown.
            await asyncio.gather(transition.message(), return_exceptions=True)
            if reply is not None:
                await asyncio.gather(reply.message(), return_exceptions=True)
            await asyncio.gather(manager.api.disable_injection(coordinator.ip_addr, "storage_proxy_drain_reads_pending"),
                                 manager.api.disable_injection(coordinator.ip_addr, "topology_coordinator_pause_after_node_transition"),
                                 return_exceptions=True)
            results = await asyncio.wait_for(asyncio.gather(*([read] if read is not None else []),
                                                          *([stop] if stop is not None else []), rebuild,
                                                          return_exceptions=True), timeout=120)
        assert read is not None and not isinstance(results[0], BaseException), results[0]
        assert list(results[0]) == [(42,)]
        assert stop is not None and results[1] is None
        await manager.server_start(replica.server_id)
        await manager.get_ready_cql(servers)

@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
async def test_read_shutdown_deadline_covers_endpoint_lock(manager: ScyllaClusterManager) -> None:
    """The prepare timeout for #15899 must also bound waiting for the local gossip lock."""
    server = await manager.server_add(config={"request_timeout_on_shutdown_in_seconds": 1})
    cql, hosts = await manager.get_ready_cql([server])
    await wait_for_feature("GRACEFUL_READ_SHUTDOWN", cql, hosts[0], time.time() + 30)
    injection = await inject_error_one_shot(manager.api, server.ip_addr, "gossiper_hold_local_endpoint")
    drain = None
    try:
        await manager.api.wait_for_injection_enter(server.ip_addr, "gossiper_hold_local_endpoint")
        log = await manager.server_open_log(server.server_id)
        mark = await log.mark()
        drain = asyncio.create_task(manager.api.drain(server.ip_addr))
        # Leave the lock held until preparation reports the expired deadline.
        await log.wait_for("Failed to prepare coordinator requests for shutdown", from_mark=mark, timeout=10)
    finally:
        await injection.message()
        if drain:
            await asyncio.wait_for(drain, timeout=30)


@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
async def test_isolation_interrupts_read_drain(manager: ScyllaClusterManager) -> None:
    """An I/O-error isolation during #15899's read wait must still close transport."""
    servers = await manager.servers_add(3, auto_rack_dc="dc1")
    ready_cql, hosts = await manager.get_ready_cql(servers)
    await wait_for_feature("GRACEFUL_READ_SHUTDOWN", ready_cql, hosts[0], time.time() + 30)
    coordinator, replica = servers[:2]
    cql = await manager.get_cql_exclusive(coordinator)
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test_table (pk int PRIMARY KEY) WITH speculative_retry = 'NONE'")
        injection = await inject_error_one_shot(manager.api, replica.ip_addr, "storage_proxy::handle_read",
                                              parameters={"cf_name": "test_table"})
        isolation = await inject_error_one_shot(manager.api, replica.ip_addr, "storage_service_isolate_during_read_drain")
        pending_read = cql.run_async(SimpleStatement(f"SELECT pk FROM {ks}.test_table WHERE pk = 42 USING TIMEOUT 10s",
                                                     consistency_level=ConsistencyLevel.ALL,
                                                     retry_policy=FallthroughRetryPolicy()), timeout=30)
        drain = None
        try:
            await manager.api.wait_for_injection_enter(replica.ip_addr, "storage_proxy::handle_read")
            log = await manager.server_open_log(replica.server_id)
            drain = asyncio.create_task(manager.api.drain(replica.ip_addr))
            await manager.api.wait_for_injection_enter(replica.ip_addr, "storage_service_isolate_during_read_drain")
            await isolation.message()
            # Isolation closes transport even though the read has not been released.
            await log.wait_for("Stop transport: shutdown messaging_service done", timeout=30)
            await asyncio.wait_for(asyncio.shield(drain), timeout=30)
        finally:
            await asyncio.gather(injection.message(), isolation.message(), return_exceptions=True)
            await asyncio.gather(manager.api.disable_injection(replica.ip_addr, "storage_proxy::handle_read"),
                                 manager.api.disable_injection(replica.ip_addr, "storage_service_isolate_during_read_drain"),
                                 return_exceptions=True)
            pending = [pending_read] + ([drain] if drain is not None else [])
            results = await asyncio.wait_for(asyncio.gather(*pending, return_exceptions=True), timeout=60)
        assert drain is not None and results[1] is None
