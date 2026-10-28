#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Paxos state must survive the tablet operations that move or reshape its tablet.

Idea: with tablets the paxos state table is co-located with its base table, so
its tablets are migrated, split and merged together with the base table's,
memtables included.  A replica that is the only one, within some quorum, to
remember an accepted proposal must still remember it once its tablet was
cloned to the other shard, split in two or merged with its neighbour.  The
interleaving is the one of test_chosen_value_survives_replica_crash, with the
tablet operation in place of the crash, and the checks are the same: through
the quorum {n1, n3} a SERIAL read must return the chosen value and a CAS
conditioned on the initial value must not apply.

Setup: three nodes with two shards each, RF=3, tablet balancing off except
while a resize runs; one tablet, or two for the merge.
"""

import logging

import pytest
from cassandra import ConsistencyLevel

from test.cluster.lwt.lwt_common import get_token_for_pk, wait_for_tablet_count
from test.cluster.lwt.test_lwt_paxos_state_durability import cas, create_table, first_round, second_round_problems
from test.cluster.util import new_test_keyspace
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.tablets import get_tablet_count, get_tablet_replicas

logger = logging.getLogger(__name__)

CONFIG = {"rf_rack_valid_keyspaces": False}
CMDLINE = ["--smp", "2", "--logger-log-level", "paxos=trace"]


def keyspace_opts(tablets: int) -> str:
    return ("WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} "
            f"AND tablets = {{'initial': {tablets}}}")


async def move_to_other_shard(manager: ScyllaClusterManager, cql, node, ks: str, scale_timeout) -> None:
    """Migrate the key's tablet replica on the node to its other shard."""
    token = await get_token_for_pk(cql, ks, "t", 1)
    host = await manager.get_host_id(node.server_id)
    [shard] = [s for h, s in await get_tablet_replicas(manager, node, ks, "t", token) if h == host]
    await manager.api.move_tablet(node.ip_addr, ks, "t", host, shard, host, 1 - shard, token,
                                  timeout=scale_timeout(120))


async def resize(manager: ScyllaClusterManager, cql, servers, ks: str, target: int, scale_timeout) -> None:
    """Split or merge the table's tablets to the target count through the balancer.

    A split follows a raised min_tablet_count.  A merge is forced by error
    injection: without tablet size statistics, which a fresh table has none
    of, the balancer never lowers a tablet count on its own.
    """
    node = servers[0]
    split = target > await get_tablet_count(manager, node, ks, "t")
    await manager.enable_tablet_balancing()
    try:
        if split:
            await cql.run_async(f"ALTER TABLE {ks}.t WITH tablets = {{'min_tablet_count': {target}}}")
        else:
            for s in servers:
                await manager.api.enable_injection(s.ip_addr, "tablet_force_tablet_count_decrease", one_shot=False)
        done = (lambda c: c >= target) if split else (lambda c: c <= target)
        await wait_for_tablet_count(manager, node, ks, "t", done, target, scale_timeout=scale_timeout, timeout_s=120)
    finally:
        await manager.disable_tablet_balancing()
        if not split:
            for s in servers:
                await manager.api.disable_injection(s.ip_addr, "tablet_force_tablet_count_decrease")


@pytest.mark.parametrize("operation", ["intranode", "split", "merge"])
@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
async def test_chosen_value_survives_tablet_operation(manager: ScyllaClusterManager, scale_timeout, operation: str):
    """The accepted proposal only n1 remembers survives the operation on n1's tablet.

    The first CAS is accepted by n1 and n2 and learned by n2 only (see
    first_round); the proposal sits in n1's paxos state memtable.  Then the
    key's tablet is moved to n1's other shard, or the table is split in two
    tablets, or its two tablets are merged into one.  The second round runs
    through the quorum {n1, n3} (see second_round_problems).
    """
    servers = await manager.servers_add(3, config=CONFIG, cmdline=CMDLINE)
    n1, n2, n3 = servers
    cql, hosts = await manager.get_ready_cql(servers)
    await manager.disable_tablet_balancing()

    async with new_test_keyspace(manager, keyspace_opts(2 if operation == "merge" else 1)) as ks:
        await create_table(manager, cql, servers, ks)
        await first_round(manager, cql, servers, hosts[1], cas(ks, 1, cl=ConsistencyLevel.ONE))

        logger.info("Tablet operation: %s", operation)
        if operation == "intranode":
            await move_to_other_shard(manager, cql, n1, ks, scale_timeout)
        else:
            await resize(manager, cql, servers, ks, 2 if operation == "split" else 1, scale_timeout)

        problems = await second_round_problems(manager, cql, ks, servers, hosts)
        assert not problems, f"the chosen value was lost in the {operation}:\n" + "\n".join(problems)
