#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
# Regression test for CUSTOMER-573 / SCYLLADB-3348: a failed one-shot
# CLIENT_ID handshake (e.g. bad_alloc under memory pressure) must not leave
# a usable-but-unidentified RPC connection behind. The sender must drop the
# connection and recover with a fresh one; no verb may be sent on a
# connection whose CLIENT_ID never reached the wire (the peer would abort,
# or crash on unpatched versions, when handlers retrieve the missing
# host_id auxiliary data).

import logging
import time

import pytest
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from test.pylib.manager_client import ManagerClient
from test.pylib.util import unique_name, wait_for_cql_and_get_hosts

logger = logging.getLogger(__name__)


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
@pytest.mark.asyncio
async def test_client_id_send_failure(manager: ManagerClient):
    """A CLIENT_ID send failure on a newly created RPC connection must:
    1. produce the "dropping the connection" warning on the sender,
    2. not be followed by any verb sent on the unidentified connection,
    3. recover automatically: the poisoned connection is evicted and the
       next send creates a fresh one which re-sends CLIENT_ID.

    The scenario mirrors the field incident: node 0's connections to node 1
    are torn down (here: by restarting node 1; in the field: by bad_alloc),
    and the CLIENT_ID send on a re-created connection fails locally (here:
    via the fail_outgoing_client_id error injection; in the field: bad_alloc
    during marshalling). Without the fix the connection stayed cached and
    node 0's next verb on it crashed node 1 with an assertion failure in
    rpc::client_info::retrieve_auxiliary.
    """

    logger.info("Starting a 2-node cluster")
    cfg = {"rf_rack_valid_keyspaces": False}
    servers = await manager.servers_add(1, config=cfg)
    servers += await manager.servers_add(1, seeds=[servers[0].ip_addr], config=cfg)

    cql = manager.get_cql()
    host0 = (await wait_for_cql_and_get_hosts(cql, [servers[0]], time.time() + 60))[0]

    ks = unique_name()
    await cql.run_async(f"CREATE KEYSPACE {ks} WITH replication = "
                        "{'class': 'NetworkTopologyStrategy', 'replication_factor': 2}")
    await cql.run_async(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY, v int)")

    write = SimpleStatement(f"INSERT INTO {ks}.t (pk, v) VALUES (1, 1)",
                            consistency_level=ConsistencyLevel.ALL)
    # Baseline: a CL=ALL write coordinated by node 0 works.
    await cql.run_async(write, host=host0)

    log0 = await manager.server_open_log(servers[0].server_id)
    mark = await log0.mark()

    # Tear down node 0's connections to node 1 and make the CLIENT_ID send
    # on the first re-created connection fail. The injection is one-shot:
    # exactly one new connection gets a poisoned handshake, later ones
    # succeed, so the cluster must converge back on its own.
    logger.info("Stopping node 1")
    await manager.server_stop_gracefully(servers[1].server_id)

    logger.info("Enabling one-shot fail_outgoing_client_id injection on node 0")
    await manager.api.enable_injection(servers[0].ip_addr, "fail_outgoing_client_id",
                                       one_shot=True)

    logger.info("Starting node 1")
    await manager.server_start(servers[1].server_id)

    # The failed handshake must be detected and the connection dropped.
    logger.info("Waiting for node 0 to report the dropped connection")
    await log0.wait_for("Failed to send CLIENT_ID .* dropping the connection",
                        from_mark=mark, timeout=60)

    # Recovery: node 0 must re-establish connectivity to node 1 on a fresh
    # connection. Without the eviction, the poisoned connection would stay
    # cached and this would never converge.
    logger.info("Waiting for node 0 to see node 1 as alive")
    await manager.server_sees_other_server(servers[0].ip_addr, servers[1].ip_addr,
                                           interval=60)

    # one_shot injections are armed per shard; disarm the remaining shards so
    # the verification write below cannot consume a leftover shot when it
    # creates its own (statement-class, per-shard) connections.
    await manager.api.disable_injection(servers[0].ip_addr, "fail_outgoing_client_id")

    # And internode writes work again: CL=ALL requires node 1's replica to
    # receive the mutation from coordinator node 0 and acknowledge it over
    # node 1's own (also freshly re-created) connection.
    logger.info("Verifying a CL=ALL write coordinated by node 0 succeeds")
    await cql.run_async(write, host=host0)

    # The peer must not have seen a verb on an unidentified connection.
    # On a patched receiver that would be reported before aborting the
    # connection; on an unpatched one it would crash the node (which the
    # test harness reports on its own).
    log1 = await manager.server_open_log(servers[1].server_id)
    matches = await log1.grep("missing CLIENT_ID aux data")
    assert not matches, f"peer received a verb on an unidentified connection: {matches}"
