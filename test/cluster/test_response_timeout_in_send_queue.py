# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""
Test that the response timeout accounting extends through the send queue.

Responses that have already exceeded their request timeout while waiting
in the send queue should be replaced with a typed timeout error
(ReadTimeout/WriteTimeout) rather than sent to the client as a successful
result. This prevents a cascade effect where expired responses block fresh
ones, causing unnecessary additional timeouts.
"""

import asyncio
import pytest

from cassandra import WriteType  # type: ignore
from cassandra.cluster import OperationTimedOut  # type: ignore
from cassandra.protocol import ReadTimeout, WriteTimeout  # type: ignore

from test import MODES_TIMEOUT_FACTOR
from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import inject_error
from test.pylib.tablets import get_tablet_replicas
from test.pylib.util import gather_safely

from .util import new_test_keyspace, new_test_table

REQUESTS_DROPPED_METRIC = "scylla_transport_requests_dropped_due_to_timeout"
REQUESTS_SENT_AFTER_TIMEOUT_METRIC = "scylla_transport_requests_sent_after_timeout"


@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
async def test_read_response_replaced_with_read_timeout_in_send_queue(manager: ManagerClient):
    """A SELECT whose response expires in the send queue must produce ReadTimeout."""
    servers = await manager.servers_add(1)
    cql, hosts = await manager.get_ready_cql(servers)
    host_ip = servers[0].ip_addr

    async with new_test_keyspace(manager, "WITH REPLICATION = { 'replication_factor' : '1' }", hosts[0]) as ks:
        async with new_test_table(manager, ks, "p int PRIMARY KEY, v int", "", hosts[0]) as tbl:
            await cql.run_async(f"INSERT INTO {tbl} (p, v) VALUES (1, 1)")

            metrics_before = await manager.metrics.query(host_ip)
            dropped_before = metrics_before.get(REQUESTS_DROPPED_METRIC) or 0

            # Force-expire the response at the send-queue exit - the server must
            # replace the RESULT with ReadTimeout.
            async with inject_error(manager.api, host_ip, "transport_expire_response_deadline"):
                with pytest.raises(ReadTimeout, match="Request timeout exceeded"):
                    await asyncio.wait_for(
                        cql.run_async(f"SELECT * FROM {tbl} WHERE p = 1"), timeout=30.0)

            metrics_after = await manager.metrics.query(host_ip)
            dropped_after = metrics_after.get(REQUESTS_DROPPED_METRIC) or 0
            assert dropped_after > dropped_before, (
                f"Expected requests_dropped_due_to_timeout metric to increase, "
                f"but it went from {dropped_before} to {dropped_after}."
            )


@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
async def test_write_response_replaced_with_write_timeout_in_send_queue(manager: ManagerClient):
    """An INSERT whose response expires in the send queue must produce WriteTimeout."""
    servers = await manager.servers_add(1)
    cql, hosts = await manager.get_ready_cql(servers)
    host_ip = servers[0].ip_addr

    async with new_test_keyspace(manager, "WITH REPLICATION = { 'replication_factor' : '1' }", hosts[0]) as ks:
        async with new_test_table(manager, ks, "p int PRIMARY KEY, v int", "", hosts[0]) as tbl:
            metrics_before = await manager.metrics.query(host_ip)
            dropped_before = metrics_before.get(REQUESTS_DROPPED_METRIC) or 0

            # Force-expire the response at the send-queue exit - the server must
            # replace the RESULT with WriteTimeout.
            async with inject_error(manager.api, host_ip, "transport_expire_response_deadline"):
                with pytest.raises(WriteTimeout, match="Request timeout exceeded"):
                    await asyncio.wait_for(
                        cql.run_async(f"INSERT INTO {tbl} (p, v) VALUES (2, 2)"), timeout=30.0)

            metrics_after = await manager.metrics.query(host_ip)
            dropped_after = metrics_after.get(REQUESTS_DROPPED_METRIC) or 0
            assert dropped_after > dropped_before, (
                f"Expected requests_dropped_due_to_timeout metric to increase, "
                f"but it went from {dropped_before} to {dropped_after}."
            )


@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
async def test_using_timeout_drives_send_queue_deadline(manager: ManagerClient, build_mode: str):
    """USING TIMEOUT should drive the send-queue deadline, not the default server timeout."""
    factor = MODES_TIMEOUT_FACTOR.get(build_mode, 1)
    timeout_ms = 500 * factor
    # Use a large default timeout - USING TIMEOUT should override it.
    config = {
        "read_request_timeout_in_ms": 60000,
        "write_request_timeout_in_ms": 60000,
        "range_read_request_timeout_in_ms": 60000,
    }

    servers = await manager.servers_add(1, config=config)
    cql, hosts = await manager.get_ready_cql(servers)
    host_ip = servers[0].ip_addr

    async with new_test_keyspace(manager, "WITH REPLICATION = { 'replication_factor' : '1' }", hosts[0]) as ks:
        async with new_test_table(manager, ks, "p int PRIMARY KEY, v int", "", hosts[0]) as tbl:
            await cql.run_async(f"INSERT INTO {tbl} (p, v) VALUES (1, 1)")

            log = await manager.server_open_log(servers[0].server_id)

            # The SELECT itself must complete within USING TIMEOUT; on an
            # overloaded machine it may genuinely time out instead - retry.
            for _ in range(10):
                metrics_before = await manager.metrics.query(host_ip)
                dropped_before = metrics_before.get(REQUESTS_DROPPED_METRIC) or 0

                mark = await log.mark()

                async with inject_error(manager.api, host_ip, "transport_write_response_delay") as handler:
                    # USING TIMEOUT overrides the 60s default.
                    query_task = cql.run_async(f"SELECT * FROM {tbl} WHERE p = 1 USING TIMEOUT {timeout_ms}ms")

                    await log.wait_for("transport_write_response_delay: waiting", from_mark=mark)

                    # Sleep past USING TIMEOUT but well below the configured default.
                    await asyncio.sleep(1.5 * factor)

                    await handler.message()

                    # A ReadTimeout from the transport layer proves that USING
                    # TIMEOUT drove the send-queue deadline.
                    try:
                        await asyncio.wait_for(query_task, timeout=10.0)
                        pytest.fail(f"Expected the response to be dropped after "
                                    f"USING TIMEOUT {timeout_ms}ms, but it was delivered.")
                    except ReadTimeout as e:
                        if "waiting in send queue" not in str(e):
                            # A genuine coordinator timeout, not our replacement:
                            # the SELECT outran its budget - retry.
                            continue

                metrics_after = await manager.metrics.query(host_ip)
                dropped_after = metrics_after.get(REQUESTS_DROPPED_METRIC) or 0
                assert dropped_after > dropped_before, (
                    f"Expected requests_dropped_due_to_timeout metric to increase "
                    f"when using USING TIMEOUT {timeout_ms}ms, but it went from "
                    f"{dropped_before} to {dropped_after}."
                )
                break
            else:
                pytest.fail(f"SELECT never completed within USING TIMEOUT "
                            f"{timeout_ms}ms in 10 attempts.")


@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
async def test_fresh_response_not_dropped_in_send_queue(manager: ManagerClient):
    """A response that has not exceeded its deadline must be delivered, not dropped.

    With a large timeout the response is well within its deadline when it leaves
    the send queue, so it must arrive intact and the dropped metric must not move.
    """
    config = {
        "read_request_timeout_in_ms": 60000,
        "write_request_timeout_in_ms": 60000,
        "range_read_request_timeout_in_ms": 60000,
    }

    servers = await manager.servers_add(1, config=config)
    cql, hosts = await manager.get_ready_cql(servers)
    host_ip = servers[0].ip_addr

    async with new_test_keyspace(manager, "WITH REPLICATION = { 'replication_factor' : '1' }", hosts[0]) as ks:
        async with new_test_table(manager, ks, "p int PRIMARY KEY, v int", "", hosts[0]) as tbl:
            await cql.run_async(f"INSERT INTO {tbl} (p, v) VALUES (1, 1)")

            metrics_before = await manager.metrics.query(host_ip)
            dropped_before = metrics_before.get(REQUESTS_DROPPED_METRIC) or 0

            log = await manager.server_open_log(servers[0].server_id)
            mark = await log.mark()

            async with inject_error(manager.api, host_ip, "transport_write_response_delay") as handler:
                query_task = cql.run_async(f"SELECT * FROM {tbl} WHERE p = 1")

                await log.wait_for("transport_write_response_delay: waiting", from_mark=mark)

                # Release immediately - the brief hold is far below the 60s
                # deadline, so the response must not be dropped.
                await handler.message()

                result = await asyncio.wait_for(query_task, timeout=10.0)
                assert len(result) == 1 and result[0].v == 1, (
                    f"Expected [(p=1, v=1)] but got {result}")

            metrics_after = await manager.metrics.query(host_ip)
            dropped_after = metrics_after.get(REQUESTS_DROPPED_METRIC) or 0
            assert dropped_after == dropped_before, (
                f"Expected requests_dropped_due_to_timeout metric to stay flat "
                f"for a fresh response, but it went from {dropped_before} to {dropped_after}."
            )


@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
async def test_response_sent_after_timeout_is_counted(manager: ManagerClient, build_mode: str):
    """Responses that expire during write/flush are sent but counted via requests_sent_after_timeout.

    When a response's deadline expires during write_message()/flush(), we cannot
    replace it (a partial CQL frame may already be on the wire). Instead, the
    response is sent and counted via the requests_sent_after_timeout metric.
    """
    factor = MODES_TIMEOUT_FACTOR.get(build_mode, 1)
    config = {
        "read_request_timeout_in_ms": 500 * factor,
        "write_request_timeout_in_ms": 500 * factor,
        "range_read_request_timeout_in_ms": 500 * factor,
    }

    servers = await manager.servers_add(1, config=config)
    cql, hosts = await manager.get_ready_cql(servers)
    host_ip = servers[0].ip_addr

    async with new_test_keyspace(manager, "WITH REPLICATION = { 'replication_factor' : '1' }", hosts[0]) as ks:
        async with new_test_table(manager, ks, "p int PRIMARY KEY, v int", "", hosts[0]) as tbl:
            await cql.run_async(f"INSERT INTO {tbl} (p, v) VALUES (1, 1)")

            log = await manager.server_open_log(servers[0].server_id)

            # The SELECT must build its response within the configured timeout;
            # on an overloaded machine it may genuinely time out instead - retry.
            for _ in range(10):
                metrics_before = await manager.metrics.query(host_ip)
                sent_after_before = metrics_before.get(REQUESTS_SENT_AFTER_TIMEOUT_METRIC) or 0

                mark = await log.mark()

                # transport_pre_flush_delay fires after the frame header and body are
                # written but before flush(), so the response can't be replaced (partial
                # frame); the deadline expires mid-write and the response is sent but counted.
                async with inject_error(manager.api, host_ip, "transport_pre_flush_delay") as handler:
                    query_task = cql.run_async(f"SELECT * FROM {tbl} WHERE p = 1")

                    await log.wait_for("transport_pre_flush_delay: waiting", from_mark=mark)

                    # Sleep past the configured timeout while write is "in progress"
                    await asyncio.sleep(1.5 * factor)

                    await handler.message()

                    # The response was SENT, not replaced: the deadline expired during
                    # write/flush, which the server counts but can't abort. A ReadTimeout
                    # here would mean it was replaced, which must not happen mid-write.
                    # The driver's request_timeout (200s, see conftest) is far larger, so
                    # the result usually still arrives; the metric is what we verify.
                    try:
                        result = await asyncio.wait_for(query_task, timeout=10.0)
                        assert len(result) == 1 and result[0].v == 1, (
                            f"Expected [(p=1, v=1)] but got {result}")
                    except OperationTimedOut:
                        # Driver gave up waiting; the server still sent it, metric still valid.
                        pass
                    except ReadTimeout:
                        # The SELECT outran its budget before the response was built
                        # (a genuine coordinator timeout) - retry.
                        continue

                metrics_after = await manager.metrics.query(host_ip)
                sent_after_after = metrics_after.get(REQUESTS_SENT_AFTER_TIMEOUT_METRIC) or 0
                assert sent_after_after > sent_after_before, (
                    f"Expected requests_sent_after_timeout metric to increase, "
                    f"but it went from {sent_after_before} to {sent_after_after}."
                )
                break
            else:
                pytest.fail("SELECT never completed within its request timeout in 10 attempts.")


@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
async def test_truncate_and_drop_responses_not_dropped_in_send_queue(manager: ManagerClient):
    """TRUNCATE and DROP responses must never be replaced with a timeout error.

    They opt out of send-queue dropping (has_send_queue_deadline=false). With
    transport_expire_response_deadline treating every deadline-carrying response
    as already expired, a SELECT is dropped (control), while TRUNCATE and DROP
    must still be delivered, since the operation was already performed.
    """
    servers = await manager.servers_add(1)
    cql, hosts = await manager.get_ready_cql(servers)
    host_ip = servers[0].ip_addr

    async with new_test_keyspace(manager, "WITH REPLICATION = { 'replication_factor' : '1' }", hosts[0]) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (p int PRIMARY KEY, v int)")
        await cql.run_async(f"INSERT INTO {ks}.t (p, v) VALUES (1, 1)")

        metrics_before = await manager.metrics.query(host_ip)
        dropped_before = metrics_before.get(REQUESTS_DROPPED_METRIC) or 0

        # The injection also drops the driver's schema-agreement probes, which
        # would stall the DROP below for max_schema_agreement_wait; bypass it.
        schema_agreement_wait = cql.cluster.max_schema_agreement_wait
        cql.cluster.max_schema_agreement_wait = 0
        try:
            async with inject_error(manager.api, host_ip, "transport_expire_response_deadline"):
                # Control: a read response carries a deadline, so it must be
                # dropped - proving the injection covers the statements below.
                with pytest.raises(ReadTimeout, match="Request timeout exceeded"):
                    await asyncio.wait_for(
                        cql.run_async(f"SELECT * FROM {ks}.t WHERE p = 1"), timeout=30.0)

                # No send-queue deadline -> delivered despite the forced expiry.
                await asyncio.wait_for(cql.run_async(f"TRUNCATE {ks}.t"), timeout=30.0)
                await asyncio.wait_for(cql.run_async(f"DROP TABLE {ks}.t"), timeout=30.0)
        finally:
            cql.cluster.max_schema_agreement_wait = schema_agreement_wait

        metrics_after = await manager.metrics.query(host_ip)
        dropped_after = metrics_after.get(REQUESTS_DROPPED_METRIC) or 0
        assert dropped_after > dropped_before, (
            f"Expected the control SELECT to be dropped, but "
            f"requests_dropped_due_to_timeout stayed at {dropped_before}."
        )


@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
async def test_shard_bounced_response_replaced_in_send_queue(manager: ManagerClient):
    """A response built on a bounced-to shard must still be dropped if it expires.

    An LWT can run on a different shard (a shard bounce) with a fresh permit, so
    the request start time must cross the shard boundary for the response to carry
    a deadline. Force a bounce and a forced expiry: if the bounce lost the deadline,
    the response would be delivered instead of the expected dropped+counted timeout.
    """
    servers = await manager.servers_add(1, cmdline=['--smp=2'])
    cql, hosts = await manager.get_ready_cql(servers)
    host_ip = servers[0].ip_addr

    async with new_test_keyspace(manager, "WITH REPLICATION = { 'replication_factor' : '1' }", hosts[0]) as ks:
        async with new_test_table(manager, ks, "p int PRIMARY KEY, v int", "", hosts[0]) as tbl:
            await cql.run_async(f"INSERT INTO {tbl} (p, v) VALUES (1, 1)")

            metrics_before = await manager.metrics.query(host_ip)
            dropped_before = metrics_before.get(REQUESTS_DROPPED_METRIC) or 0

            # Force the update to bounce to another shard (reached via invoke_on),
            # the path that must carry the request start time across the boundary.
            await manager.api.enable_injection(host_ip, "forced_bounce_to_shard_counter",
                                               one_shot=False, parameters={'value': '2'})

            async with inject_error(manager.api, host_ip, "transport_expire_response_deadline"):
                # Conditional update -> LWT/CAS, runs on the cas_shard via a bounce.
                with pytest.raises(WriteTimeout, match="Request timeout exceeded"):
                    await asyncio.wait_for(
                        cql.run_async(f"UPDATE {tbl} SET v = 2 WHERE p = 1 IF v = 1"), timeout=30.0)

            metrics_after = await manager.metrics.query(host_ip)
            dropped_after = metrics_after.get(REQUESTS_DROPPED_METRIC) or 0
            assert dropped_after > dropped_before, (
                f"Expected requests_dropped_due_to_timeout to increase for a "
                f"shard-bounced response, but it went from {dropped_before} to {dropped_after}. "
                f"This means the bounced response carried no send-queue deadline.")


@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
async def test_cas_response_replaced_with_cas_write_timeout_in_send_queue(manager: ManagerClient):
    """A dropped LWT (CAS) response must report write_type=CAS, not SIMPLE.

    So a driver handles it like any other CAS timeout, matching the write_type
    the storage proxy's CAS path reports.
    """
    # Single shard so the CAS runs locally (no bounce), isolating the write_type.
    servers = await manager.servers_add(1, cmdline=['--smp=1'])
    cql, hosts = await manager.get_ready_cql(servers)
    host_ip = servers[0].ip_addr

    async with new_test_keyspace(manager, "WITH REPLICATION = { 'replication_factor' : '1' }", hosts[0]) as ks:
        async with new_test_table(manager, ks, "p int PRIMARY KEY, v int", "", hosts[0]) as tbl:
            async with inject_error(manager.api, host_ip, "transport_expire_response_deadline"):
                # Conditional INSERT -> LWT/CAS.
                with pytest.raises(WriteTimeout, match="Request timeout exceeded") as excinfo:
                    await asyncio.wait_for(
                        cql.run_async(f"INSERT INTO {tbl} (p, v) VALUES (1, 1) IF NOT EXISTS"), timeout=30.0)
                # Driver may expose write_type as code or name; accept either.
                assert excinfo.value.write_type in (WriteType.CAS, "CAS"), (
                    f"Expected write_type CAS for a dropped LWT response, "
                    f"got {excinfo.value.write_type!r}.")


@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
async def test_forwarded_response_replaced_in_send_queue(manager: ManagerClient):
    """A forwarded response that expires in the send queue must be replaced.

    An SC read issued on a non-leader node is forwarded to the leader and written
    back from the receiving node, which must apply the request-anchored deadline
    and bounce timeout context. An expired forwarded read must be a ReadTimeout
    (is_write=false carried on the bounce), not a WriteTimeout.
    """
    config = {
        'experimental_features': ['strongly-consistent-tables'],
    }

    servers = await manager.servers_add(2, config=config, auto_rack_dc='my_dc')
    cql, hosts = await manager.get_ready_cql(servers)
    host_ids = await gather_safely(*[manager.get_host_id(s.server_id) for s in servers])

    async with new_test_keyspace(manager,
            "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} "
            "AND tablets = {'initial': 1} AND consistency = 'global'") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int)")
        await cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES (1, 10)")

        # The single tablet has one replica, which is the Raft leader. An SC read
        # issued on the other (non-replica) node is forwarded to that leader.
        tablet_replicas = await get_tablet_replicas(manager, servers[0], ks, "test", 0)
        replica_host_ids = {str(r[0]) for r in tablet_replicas}
        non_replica_idx = next(i for i, hid in enumerate(host_ids)
                               if str(hid) not in replica_host_ids)
        non_replica_server = servers[non_replica_idx]
        non_replica_host = hosts[non_replica_idx]
        host_ip = non_replica_server.ip_addr

        metrics_before = await manager.metrics.query(host_ip)
        dropped_before = metrics_before.get(REQUESTS_DROPPED_METRIC) or 0

        async with inject_error(manager.api, host_ip, "transport_expire_response_deadline"):
            # Issued on the non-replica node -> forwarded to the leader.
            with pytest.raises(ReadTimeout, match="Request timeout exceeded"):
                await asyncio.wait_for(
                    cql.run_async(f"SELECT * FROM {ks}.test WHERE pk = 1", host=non_replica_host),
                    timeout=30.0)

        metrics_after = await manager.metrics.query(host_ip)
        dropped_after = metrics_after.get(REQUESTS_DROPPED_METRIC) or 0
        assert dropped_after > dropped_before, (
            f"Expected requests_dropped_due_to_timeout to increase on the "
            f"forwarding node, but it went from {dropped_before} to {dropped_after}.")
