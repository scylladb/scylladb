#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import asyncio
import logging

import pytest

from test.cluster.util import get_current_group0_config, quiesce_and_disable_tablet_balancing
from test.pylib.rest_client import read_barrier
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

INJECTION = "block_raft_io_fiber_commit_past_start"


@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
@pytest.mark.parametrize("with_non_voter", [False, True], ids=["sole_member", "with_non_voter"])
async def test_sole_voter_loads_table_from_uncommitted_log_tail(manager: ScyllaClusterManager, with_non_voter: bool):
    """Regression test for https://scylladb.atlassian.net/browse/SCYLLADB-5280.

    A restarting node replays the group0 log before it loads the non-system
    keyspaces, so that they are loaded from the current schema. The replay
    covers the entries up to the persisted commit index and, on the sole
    voter of the group, its whole log: the voter commits the rest of it by
    itself as soon as it becomes leader, and before start() treated it as
    committed, a CREATE TABLE past the persisted commit index was applied
    behind the back of the startup sequence, after the keyspaces were
    loaded.

    The test kills the sole voter with a CREATE TABLE in the log past the
    persisted commit index and restarts it holding every commit it makes on
    its own as leader until the keyspaces are loaded, so that the table is
    loaded with them only if the early replay applied it; it checks that it
    was, and that the table is queryable. The voter is alone in the group,
    or has a non-voter beside it: a two-node cluster keeps a single voter,
    and that variant tells a sole voter from a sole member.
    """
    servers = [await manager.server_add()]
    if with_non_voter:
        servers.append(await manager.server_add())
    voters = [host_id for host_id, can_vote in await get_current_group0_config(manager, servers[0]) if can_vote]
    assert len(voters) == 1, f"expected a sole voter, got {voters}"
    [srv] = [s for s in servers if str(await manager.get_host_id(s.server_id)) == voters[0]]

    cql, [host] = await manager.get_ready_cql([srv])
    await cql.run_async("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}", host=host)
    # The balancer moves the system keyspaces' tablets onto the non-voter after it joins; the
    # injection must hold the CREATE TABLE, not one of those commits.
    await quiesce_and_disable_tablet_balancing(manager, srv.ip_addr)

    logger.info("Killing the voter with the CREATE TABLE entry in the log, past the persisted commit index")
    await manager.api.enable_injection(srv.ip_addr, INJECTION, one_shot=False)
    create = cql.run_async("CREATE TABLE ks.t (pk int PRIMARY KEY)", host=host)
    await manager.api.wait_for_injection_enter(srv.ip_addr, INJECTION)
    await manager.server_stop(srv.server_id, convict=False)
    with pytest.raises(Exception):
        await create

    logger.info("Restarting the voter, holding its own commits until the non-system keyspaces are loaded")
    await manager.server_update_config(srv.server_id, "error_injections_at_startup", [INJECTION])
    log = await manager.server_open_log(srv.server_id)
    mark = await log.mark()
    start = asyncio.create_task(manager.server_start(srv.server_id))
    # The first startup checkpoint after init_non_system_keyspaces() in main.cc.
    await log.wait_for("init - recovering logstor", from_mark=mark)
    await manager.api.disable_injection(srv.ip_addr, INJECTION)
    await start

    # The early replay covered the log tail: the table was then loaded from
    # the schema tables together with the keyspaces, not created by a later
    # apply.
    assert await log.grep("Keyspace ks: Reading CF t ", from_mark=mark), \
        "the CREATE TABLE entry was applied after the keyspaces were loaded"

    # The non-voter, if any, learns of the commit once the voter leads again.
    # The read barrier waits until each node applied the entry, i.e. created
    # the table in memory, not only wrote its schema rows.
    cql, _ = await manager.get_ready_cql(servers)
    await asyncio.gather(*(read_barrier(manager.api, s.ip_addr) for s in servers))
    await cql.run_async("SELECT * FROM ks.t")
