#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""
Measures how well the dedicated_rack leader preference holds in a real cluster.

The election timeout only biases leadership towards the dedicated rack, so the
guarantee is statistical rather than absolute: ticks fire independently on each node
and a slower node can undercut a faster one. These tests disrupt leadership over and
over and report, per round, in how many groups the dedicated rack won the election and
how many terms were burnt getting there (extra terms mean split votes or retries).
"""

import asyncio
import logging
import time
import uuid

import pytest

from test.cluster.util import new_test_keyspace
from test.pylib.manager_client import ManagerClient
from test.pylib.util import Host, gather_safely, wait_for

logger = logging.getLogger(__name__)

CONFIG = {'experimental_features': ['strongly-consistent-tables']}
CMDLINE = ['--logger-log-level', 'sc_groups_manager=debug']

DEDICATED_RACK = 'rack1'
RACKS = [DEDICATED_RACK, 'rack2', 'rack3']
TABLETS = 8
# Rounds of disruption. Every round elects a leader for every group, so this is also
# the number of samples per group.
ROUNDS = 3
# How often a leader reconsiders whether a preferred replica should be leading instead,
# in seconds: 5 * ELECTION_TIMEOUT ticks of 100ms.
PLACEMENT_INTERVAL = 5


async def group_ids(manager: ManagerClient, ks: str, table: str) -> list[str]:
    """Raft group ids of all the tablets of a table, one per tablet."""
    table_id = await manager.get_table_id(ks, table)
    rows = await manager.get_cql().run_async(
        f"SELECT raft_group_id FROM system.tablets WHERE table_id = {table_id}")
    return [str(row.raft_group_id) for row in rows]


async def leader_of(manager: ManagerClient, ip: str, group_id: str) -> str | None:
    """The group's leader as seen by the node at `ip`, or None during an election."""
    leader = await manager.api.get_raft_leader(ip, group_id)
    return None if uuid.UUID(leader).int == 0 else leader


async def leaders(manager: ManagerClient, ip: str, groups: list[str]) -> dict[str, str | None]:
    found = await gather_safely(*[leader_of(manager, ip, group) for group in groups])
    return dict(zip(groups, found))


async def terms(manager: ManagerClient, host: Host) -> dict[str, int]:
    """vote_term of every group as persisted by one node."""
    # DISTINCT requires the whole partition key, which is (shard, group_id).
    rows = await manager.get_cql().run_async(
        "SELECT DISTINCT shard, group_id, vote_term FROM system.raft_groups", host=host)
    return {str(row.group_id): row.vote_term or 0 for row in rows}


def report(round_name: str, groups: list[str], found: dict[str, str | None],
           dedicated_host_id: str, term_deltas: dict[str, int]) -> int:
    """Logs the outcome of one round and returns the number of groups led from the
    dedicated rack."""
    won = sum(1 for group in groups if found[group] == dedicated_host_id)
    undecided = sum(1 for group in groups if found[group] is None)
    burnt = [delta for delta in term_deltas.values() if delta > 1]
    logger.info(f"{round_name}: dedicated rack leads {won}/{len(groups)} groups "
                f"({100 * won // len(groups)}%), {undecided} undecided; "
                f"groups needing more than one term: {len(burnt)}/{len(term_deltas)}, "
                f"max terms burnt: {max(term_deltas.values(), default=0)}")
    for group in groups:
        logger.debug(f"{round_name}: group {group} leader {found[group]} "
                     f"terms burnt {term_deltas.get(group)}")
    return won


async def wait_for_all_led_by(manager: ManagerClient, ip: str, groups: list[str],
                              host_id: str, deadline: float) -> None:
    async def all_led_by():
        found = await leaders(manager, ip, groups)
        missing = [group for group in groups if found[group] != host_id]
        return True if not missing else None
    await wait_for(all_led_by, deadline, label=f"all groups led by {host_id}")


@pytest.mark.asyncio
async def test_dedicated_rack_wins_elections(manager: ManagerClient):
    """One node per rack, dedicated_rack = rack1. Restart the leading node repeatedly
    and check that leadership goes back to the dedicated rack every time."""
    servers = []
    for rack in RACKS:
        servers.append(await manager.server_add(config=CONFIG, cmdline=CMDLINE,
                                                property_file={'dc': 'dc1', 'rack': rack}))
    dedicated, other = servers[0], servers[1]
    cql, hosts = await manager.get_ready_cql(servers)
    host_ids = await gather_safely(*[manager.get_host_id(s.server_id) for s in servers])
    dedicated_host_id = host_ids[0]
    # Ask a node which is never restarted, so that a stale view doesn't skew the counts.
    observer = servers[2]

    async with new_test_keyspace(manager,
            "WITH replication = {'class': 'NetworkTopologyStrategy', "
            f"'dc1': ['{RACKS[0]}', '{RACKS[1]}', '{RACKS[2]}']}} "
            f"AND tablets = {{'initial': {TABLETS}}} "
            "AND consistency = {'type': 'global', "
            f"'dedicated_rack': {{'dc1': '{DEDICATED_RACK}'}}}}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY, v int)")
        groups = await group_ids(manager, ks, 't')
        assert len(groups) == TABLETS

        # A fresh group bootstraps its leader on the preferred replica, so the rack
        # leads from the start - without any election having happened yet.
        await wait_for_all_led_by(manager, observer.ip_addr, groups, dedicated_host_id,
                                  time.time() + 120)
        before = await terms(manager, hosts[2])
        found = await leaders(manager, observer.ip_addr, groups)
        deltas = {group: 0 for group in groups}
        assert report("initial", groups, found, dedicated_host_id, deltas) == len(groups)

        for round_no in range(ROUNDS):
            # Disrupt: take the dedicated replica away, so every group has to elect a
            # leader outside the preferred rack, then bring it back.
            await manager.server_stop_gracefully(dedicated.server_id)

            # The surviving replicas have to elect a leader among themselves before the
            # dedicated one comes back, or the round proves nothing: the assertion below
            # would hold without the leadership ever having left the rack.
            async def led_from_elsewhere():
                found = await leaders(manager, observer.ip_addr, groups)
                return True if all(found[group] not in (None, dedicated_host_id)
                                   for group in groups) else None
            await wait_for(led_from_elsewhere, time.time() + 120,
                           label="every group led outside the dedicated rack")
            logger.info(f"round {round_no}: the dedicated rack is down and leads nothing")

            await manager.server_start(dedicated.server_id)
            await manager.servers_see_each_other(servers)

            # Once the dedicated replica is back it should take the leadership over: it
            # holds the first election timeout slot, and a group led from another rack
            # steps down for it.
            await wait_for_all_led_by(manager, observer.ip_addr, groups, dedicated_host_id,
                                      time.time() + 120)

            after = await terms(manager, hosts[2])
            deltas = {group: after.get(group, 0) - before.get(group, 0) for group in groups}
            before = after
            found = await leaders(manager, observer.ip_addr, groups)
            won = report(f"round {round_no}", groups, found, dedicated_host_id, deltas)
            assert won == len(groups)


@pytest.mark.asyncio
async def test_dedicated_rack_keeps_leadership(manager: ManagerClient):
    """Restarting a node outside the dedicated rack must not move leadership out of
    it."""
    servers = []
    for rack in RACKS:
        servers.append(await manager.server_add(config=CONFIG, cmdline=CMDLINE,
                                                property_file={'dc': 'dc1', 'rack': rack}))
    cql, hosts = await manager.get_ready_cql(servers)
    host_ids = await gather_safely(*[manager.get_host_id(s.server_id) for s in servers])
    dedicated_host_id = host_ids[0]
    observer = servers[0]

    async with new_test_keyspace(manager,
            "WITH replication = {'class': 'NetworkTopologyStrategy', "
            f"'dc1': ['{RACKS[0]}', '{RACKS[1]}', '{RACKS[2]}']}} "
            f"AND tablets = {{'initial': {TABLETS}}} "
            "AND consistency = {'type': 'global', "
            f"'dedicated_rack': {{'dc1': '{DEDICATED_RACK}'}}}}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY, v int)")
        groups = await group_ids(manager, ks, 't')

        await wait_for_all_led_by(manager, observer.ip_addr, groups, dedicated_host_id,
                                  time.time() + 120)

        await manager.server_restart(servers[1].server_id)
        await manager.servers_see_each_other(servers)

        # Sample across more than one placement check: a single look right after the
        # restart would also pass if the leadership were about to move away.
        deadline = time.time() + 2 * PLACEMENT_INTERVAL + 1
        while time.time() < deadline:
            found = await leaders(manager, observer.ip_addr, groups)
            assert all(found[group] == dedicated_host_id for group in groups), \
                f"the leadership left the dedicated rack: {found}"
        report("after restarting a non-dedicated replica", groups, found, dedicated_host_id,
               {group: 0 for group in groups})


@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_dedicated_rack_regains_leadership(manager: ManagerClient):
    """A group which elected another replica while the dedicated-rack one was up and unable
    to win must hand the leadership back to it once it can."""
    # The dedicated replica cannot become a leader of anything while the injection is on,
    # so it is added last - the first node has to lead group0 to form the cluster.
    servers = [await manager.server_add(config=CONFIG, cmdline=CMDLINE,
                                        property_file={'dc': 'dc1', 'rack': rack})
               for rack in RACKS[1:]]
    dedicated = await manager.server_add(
            config=CONFIG | {'error_injections_at_startup': [{'name': 'avoid_being_raft_leader'}]},
            cmdline=CMDLINE, property_file={'dc': 'dc1', 'rack': DEDICATED_RACK})
    servers.append(dedicated)
    cql, hosts = await manager.get_ready_cql(servers)
    host_ids = await gather_safely(*[manager.get_host_id(s.server_id) for s in servers])
    dedicated_host_id = host_ids[-1]
    observer = servers[0]

    async with new_test_keyspace(manager,
            "WITH replication = {'class': 'NetworkTopologyStrategy', "
            f"'dc1': ['{RACKS[0]}', '{RACKS[1]}', '{RACKS[2]}']}} "
            f"AND tablets = {{'initial': {TABLETS}}} "
            "AND consistency = {'type': 'global', "
            f"'dedicated_rack': {{'dc1': '{DEDICATED_RACK}'}}}}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY, v int)")
        groups = await group_ids(manager, ks, 't')

        # Keep writing. A leader which replicates tells the followers who it is, so the
        # preferred replica knows there is one and stays a follower even once it is allowed
        # to campaign again - an idle group would instead get its leadership back for free,
        # by that replica timing out first.
        writing = True

        async def writer():
            insert = cql.prepare(f"INSERT INTO {ks}.t (pk, v) VALUES (?, ?)")
            pk = 0
            while writing:
                try:
                    # Awaiting the write is the pacing: the next one starts once this one
                    # is answered.
                    await cql.run_async(insert, [pk, pk])
                except Exception as e:
                    logger.debug(f"write of {pk} failed, most likely a leader change: {e}")
                pk += 1

        writer_task = asyncio.create_task(writer())
        try:
            # The preferred replica holds the first slot but refuses to campaign, so every
            # group ends up led from another rack while it is up and healthy.
            async def none_led_by_dedicated():
                found = await leaders(manager, observer.ip_addr, groups)
                return True if all(found[group] != dedicated_host_id for group in groups) else None
            await wait_for(none_led_by_dedicated, time.time() + 120,
                           label="no group led by the dedicated rack")

            # The leadership must sit still while the preferred replica cannot take it over.
            # Handing it to whoever is up to date instead would move it to another replica,
            # which would hand it on in turn, and the group would keep churning.
            settled = await leaders(manager, observer.ip_addr, groups)
            # More than one placement check has to pass, so that the leaders had the chance
            # to hand the leadership over and declined it.
            deadline = time.time() + 2 * PLACEMENT_INTERVAL + 1
            while time.time() < deadline:
                again = await leaders(manager, observer.ip_addr, groups)
                moved = {group: (settled[group], again[group]) for group in groups
                         if settled[group] != again[group]}
                assert not moved, \
                    f"the leadership moved while the preferred replica could not take over: {moved}"

            # Let it win again. Nothing announces that - it knows the leaders and stays a
            # follower, and their own state doesn't change - so the leadership only comes
            # back because they reconsider it themselves.
            await manager.api.disable_injection(dedicated.ip_addr, 'avoid_being_raft_leader')
            await wait_for_all_led_by(manager, observer.ip_addr, groups, dedicated_host_id,
                                      time.time() + 120)
        finally:
            # Whichever way we leave, the keyspace is about to be dropped from under the
            # writer.
            writing = False
            await writer_task
