#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import asyncio
import logging
import time

import pytest
from cassandra import ConsistencyLevel  # type: ignore
from cassandra.policies import FallthroughRetryPolicy  # type: ignore

from test.cluster.util import new_test_keyspace
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import scale_timeout_by_mode, wait_for

logger = logging.getLogger(__name__)


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_reads_during_drain(manager: ScyllaClusterManager, build_mode: str) -> None:
    """
    Reads coordinated by other nodes and already sent to a node must succeed
    when that node drains.

    Expected drain sequence, as the other nodes see it:

     1. The node stops its CQL server. The other nodes learn that through
        gossip. They still see the node as alive, keep sending it replica
        reads, and must not drop their connections to it.
     2. The node announces its shutdown through gossip. The other nodes mark
        it down and stop sending it reads.
     3. For shutdown_announce_in_ms after the announcement, the node keeps
        serving the reads it received, and the other nodes must keep their
        connections to it, so that the replies arrive.
     4. The node shuts down its messaging service, which closes the
        connections.

    With speculative_retry = 'NONE', a QUORUM read has no spare replica, so a
    single failed replica fails the whole read.

    The test sends a batch of reads and holds on the node those which reach
    it. Then it drains the node, pausing the drain after each step of
    stop_transport(). At each pause it waits until the other nodes see the
    node's CQL server as the node itself does, so that they learn that it
    stopped before the node announces its shutdown. It releases the reads in
    step 3, a quarter of shutdown_announce_in_ms after both other nodes marked
    the node down.
    """
    # Test clusters disable the announcement window.
    shutdown_announce_ms = scale_timeout_by_mode(build_mode, 5000)
    servers = await manager.servers_add(3, auto_rack_dc="dc1", config={'shutdown_announce_in_ms': shutdown_announce_ms})
    cql, hosts = await manager.get_ready_cql(servers)
    target, others = servers[2], servers[:2]

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}") as ks:
        # The table stays empty: a read of a missing key goes through the replicas all the same.
        await cql.run_async(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY, v int) WITH speculative_retry = 'NONE'")

        read = cql.prepare(f"SELECT v FROM {ks}.t WHERE pk = ?")
        read.consistency_level = ConsistencyLevel.QUORUM
        # Report every failure; the driver must not retry it elsewhere.
        read.retry_policy = FallthroughRetryPolicy()

        async def read_all() -> list[Exception]:
            """Reads 100 keys through each of the other nodes. Returns the failures."""
            results = await asyncio.gather(*(cql.run_async(read, (k,), host=h) for k in range(100) for h in hosts[:2]),
                                           return_exceptions=True)
            return [r for r in results if isinstance(r, Exception)]

        # Predicates for wait_for(), which polls until they return something else than None.

        async def others_see_down():
            """Both other nodes see the draining node as down."""
            return all([target.ip_addr in await manager.api.get_down_endpoints(s.ip_addr) for s in others]) or None

        async def others_see_cql_as_target_does():
            """Both other nodes see the CQL server of the draining node as ready or not, the same as the node itself."""
            ready = await manager.api.sees_cql_ready(target.ip_addr, target.ip_addr)
            return all([await manager.api.sees_cql_ready(s.ip_addr, target.ip_addr) == ready for s in others]) or None

        wait_timeout = scale_timeout_by_mode(build_mode, 60)

        async def release_reads() -> None:
            await wait_for(others_see_down, time.time() + wait_timeout, period=0.01, max_period=0.5)
            # A reply sent this long after the other nodes marked the node
            # down arrives only because they kept their connections. It would
            # have been lost if they had dropped them. It is sent well within
            # shutdown_announce_in_ms.
            await asyncio.sleep(shutdown_announce_ms / 4 / 1000)
            await manager.api.disable_injection(target.ip_addr, "storage_proxy_replica_read_pause")

        held_reads = None
        releaser = None
        try:
            await manager.api.enable_injection(target.ip_addr, "storage_proxy_replica_read_pause", one_shot=False)
            held_reads = asyncio.create_task(read_all())
            await manager.api.wait_for_injection_enter(target.ip_addr, "storage_proxy_replica_read_pause")
            releaser = asyncio.create_task(release_reads())

            await manager.api.enable_injection(target.ip_addr, "stop_transport_pause", one_shot=False)
            logger.info(f"Draining {target}")
            drain_task = asyncio.create_task(manager.api.drain(target.ip_addr))
            pauses = 0
            while True:
                # The drain blocks at its next pause, or has finished.
                # stop_transport() runs on shard 0 only, so the enter count of
                # its injection is the number of pauses reached.
                async def paused_or_done():
                    paused = await manager.api.get_injection_enter_count(target.ip_addr, "stop_transport_pause") > pauses
                    return (drain_task.done() or paused) or None
                await wait_for(paused_or_done, time.time() + wait_timeout, period=0.01, max_period=0.5)
                if drain_task.done():
                    break
                pauses += 1
                # The pauses let the other nodes learn that the CQL server
                # stopped while they still see the node as alive. They learn
                # it through gossip. Without pausing, the drain would have
                # announced the shutdown before that, making the test
                # unreliable in detecting peers wrongly killing connections
                # with reads still in flight. At the other pauses they
                # already agree with the node, and the wait returns at once.
                await wait_for(others_see_cql_as_target_does, time.time() + wait_timeout, period=0.01, max_period=0.5)
                await manager.api.message_injection(target.ip_addr, "stop_transport_pause")
            await drain_task
            await releaser
            failures = await held_reads
        finally:
            # A failed run can leave the releaser running, and the drain and the reads paused.
            if releaser:
                releaser.cancel()
            await manager.api.disable_injection(target.ip_addr, "storage_proxy_replica_read_pause")
            await manager.api.disable_injection(target.ip_addr, "stop_transport_pause")
            await asyncio.gather(*filter(None, [releaser, held_reads]), return_exceptions=True)

        await manager.server_stop_gracefully(target.server_id)

        assert not failures, f"{len(failures)} reads held on the draining node failed, first: {failures[0]!r}"
