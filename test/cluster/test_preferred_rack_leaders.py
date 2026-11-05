#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""
Checks that the leadership of the groups returns to the preferred rack whenever it
leaves it, and follows the preferred rack when it is altered. Also checks how ALTER
KEYSPACE handles preferred_rack together with replication changes.
"""

import asyncio
import logging
import time

import pytest
from cassandra.cluster import Session
from cassandra.protocol import ConfigurationException, InvalidRequest

from test.cluster.util import get_topology_coordinator, new_test_keyspace
from test.pylib.rest_client import inject_error_one_shot
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import Host, gather_safely, wait_for

logger = logging.getLogger(__name__)

CONFIG = {'experimental_features': ['strongly-consistent-tables']}
CMDLINE = ['--logger-log-level', 'sc_groups_manager=debug']

PREFERRED_RACK = 'rack1'
RACKS = [PREFERRED_RACK, 'rack2', 'rack3']
TABLETS = 8
# Restarts of the preferred replica.
ROUNDS = 3
# How long a leader waits between two attempts to hand the leadership over, in seconds:
# Raft's transfer timeout plus the wait which follows it, at the default tick.
HANDOVER_INTERVAL = 6
# Terms a round may cost a group: one transfer away from the preferred replica when it
# stops, one back when it returns, and slack for a retried handover.
MAX_TERMS_PER_ROUND = 4


async def group_ids(manager: ScyllaClusterManager, ks: str, table: str) -> list[str]:
    """Raft group ids of all the tablets of a table, one per tablet."""
    table_id = await manager.get_table_id(ks, table)
    rows = await manager.get_cql().run_async(
        f"SELECT raft_group_id FROM system.tablets WHERE table_id = {table_id}")
    return [str(row.raft_group_id) for row in rows]


async def leader_of(manager: ScyllaClusterManager, ip: str, group_id: str) -> str | None:
    """The group's leader as seen by the node at `ip`, or None during an election."""
    return await manager.api.get_raft_leader(ip, group_id)


async def leaders(manager: ScyllaClusterManager, ip: str, groups: list[str]) -> dict[str, str | None]:
    found = await gather_safely(*[leader_of(manager, ip, group) for group in groups])
    return dict(zip(groups, found))


async def terms(manager: ScyllaClusterManager, host: Host) -> dict[str, int]:
    """vote_term of every group as persisted by one node."""
    rows = await manager.get_cql().run_async(
        "SELECT shard, group_id, vote_term FROM system.raft_groups", host=host)
    return {str(row.group_id): row.vote_term or 0 for row in rows}


async def wait_for_all_led_by(manager: ScyllaClusterManager, ip: str, groups: list[str],
                              host_id: str, deadline: float) -> None:
    async def all_led_by():
        found = await leaders(manager, ip, groups)
        missing = [group for group in groups if found[group] != host_id]
        return True if not missing else None
    await wait_for(all_led_by, deadline, label=f"all groups led by {host_id}")


@pytest.mark.asyncio
async def test_preferred_rack_leadership_returns_after_restart(manager: ScyllaClusterManager):
    """One node per rack, preferred_rack = rack1. Restart the leading node repeatedly
    and check that leadership goes back to the preferred rack every time."""
    servers = []
    for rack in RACKS:
        servers.append(await manager.server_add(config=CONFIG, cmdline=CMDLINE,
                                                property_file={'dc': 'dc1', 'rack': rack}))
    preferred = servers[0]
    cql, hosts = await manager.get_ready_cql(servers)
    host_ids = await gather_safely(*[manager.get_host_id(s.server_id) for s in servers])
    preferred_host_id = host_ids[0]
    # Ask a node which is never restarted, so that a stale view doesn't skew the counts.
    observer = servers[2]

    async with new_test_keyspace(manager,
            "WITH replication = {'class': 'NetworkTopologyStrategy', "
            f"'dc1': ['{RACKS[0]}', '{RACKS[1]}', '{RACKS[2]}']}} "
            f"AND tablets = {{'initial': {TABLETS}}} "
            "AND consistency = 'global' "
            f"AND preferred_rack = {{'dc1': '{PREFERRED_RACK}'}}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY, v int)")
        groups = await group_ids(manager, ks, 't')
        assert len(groups) == TABLETS

        # A fresh group bootstraps its leader on the preferred replica, so the rack
        # leads from the start - without any election having happened yet.
        await wait_for_all_led_by(manager, observer.ip_addr, groups, preferred_host_id,
                                  time.time() + 120)
        before = await terms(manager, hosts[2])

        for round_no in range(ROUNDS):
            await manager.server_stop_gracefully(preferred.server_id)

            # Otherwise the round proves nothing.
            async def led_from_elsewhere():
                found = await leaders(manager, observer.ip_addr, groups)
                return True if all(found[group] not in (None, preferred_host_id)
                                   for group in groups) else None
            await wait_for(led_from_elsewhere, time.time() + 120,
                           label="every group led outside the preferred rack")
            logger.info(f"round {round_no}: the preferred rack is down and leads nothing")

            await manager.server_start(preferred.server_id)
            await manager.servers_see_each_other(servers)

            # Once the preferred replica is back, the groups hand the leadership to it.
            await wait_for_all_led_by(manager, observer.ip_addr, groups, preferred_host_id,
                                      time.time() + 120)

            after = await terms(manager, hosts[2])
            deltas = {group: after.get(group, 0) - before.get(group, 0) for group in groups}
            before = after
            logger.info(f"round {round_no}: terms per group: {deltas}")
            wasteful = {group: delta for group, delta in deltas.items() if delta > MAX_TERMS_PER_ROUND}
            assert not wasteful, f"round {round_no}: groups which burnt too many terms: {wasteful}"


@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_preferred_rack_regains_leadership(manager: ScyllaClusterManager):
    """A group which elected another replica while the preferred-rack one was up and unable
    to win must hand the leadership back to it once it can."""
    # The preferred replica cannot become a leader of anything while the injection is on,
    # so it is added last - the first node has to lead group0 to form the cluster.
    servers = [await manager.server_add(config=CONFIG, cmdline=CMDLINE,
                                        property_file={'dc': 'dc1', 'rack': rack})
               for rack in RACKS[1:]]
    preferred = await manager.server_add(
            config=CONFIG | {'error_injections_at_startup': [{'name': 'avoid_being_raft_leader'}]},
            cmdline=CMDLINE, property_file={'dc': 'dc1', 'rack': PREFERRED_RACK})
    servers.append(preferred)
    cql, _ = await manager.get_ready_cql(servers)
    host_ids = await gather_safely(*[manager.get_host_id(s.server_id) for s in servers])
    preferred_host_id = host_ids[-1]
    observer = servers[0]

    async with new_test_keyspace(manager,
            "WITH replication = {'class': 'NetworkTopologyStrategy', "
            f"'dc1': ['{RACKS[0]}', '{RACKS[1]}', '{RACKS[2]}']}} "
            f"AND tablets = {{'initial': {TABLETS}}} "
            "AND consistency = 'global' "
            f"AND preferred_rack = {{'dc1': '{PREFERRED_RACK}'}}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY, v int)")
        groups = await group_ids(manager, ks, 't')

        # Keep writing: the injection makes the preferred replica forget the leader on
        # every handover attempt, and only an append tells it again.
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
            async def none_led_by_preferred():
                found = await leaders(manager, observer.ip_addr, groups)
                return True if all(found[group] != preferred_host_id for group in groups) else None
            await wait_for(none_led_by_preferred, time.time() + 120,
                           label="no group led by the preferred rack")

            # The leadership must sit still while the preferred replica cannot take it over.
            # Handing it to whoever is up to date instead would move it to another replica,
            # which would hand it on in turn, and the group would keep churning.
            settled = await leaders(manager, observer.ip_addr, groups)
            # More than one handover attempt has to pass, so that the leaders had the chance
            # to hand the leadership over and failed.
            deadline = time.time() + 2 * HANDOVER_INTERVAL + 1
            while time.time() < deadline:
                again = await leaders(manager, observer.ip_addr, groups)
                moved = {group: (settled[group], again[group]) for group in groups
                         if settled[group] != again[group]}
                assert not moved, \
                    f"the leadership moved while the preferred replica could not take over: {moved}"
                await asyncio.sleep(0.5)

            # Let it win again. Nothing announces that - it knows the leaders and stays a
            # follower, and their own state doesn't change - so the leadership only comes
            # back because the leaders retry the handover.
            await manager.api.disable_injection(preferred.ip_addr, 'avoid_being_raft_leader')
            await wait_for_all_led_by(manager, observer.ip_addr, groups, preferred_host_id,
                                      time.time() + 120)
        finally:
            # Whichever way we leave, the keyspace is about to be dropped from under the
            # writer.
            writing = False
            await writer_task


@pytest.mark.asyncio
async def test_preferred_rack_alter_moves_leadership(manager: ScyllaClusterManager):
    """One node per rack. Altering preferred_rack moves the leadership of every group
    to the new rack."""
    servers = [await manager.server_add(config=CONFIG, cmdline=CMDLINE,
                                        property_file={'dc': 'dc1', 'rack': rack})
               for rack in RACKS]
    cql, _ = await manager.get_ready_cql(servers)
    host_ids = await gather_safely(*[manager.get_host_id(s.server_id) for s in servers])
    observer = servers[2]

    async with new_test_keyspace(manager,
            "WITH replication = {'class': 'NetworkTopologyStrategy', "
            f"'dc1': ['{RACKS[0]}', '{RACKS[1]}', '{RACKS[2]}']}} "
            f"AND tablets = {{'initial': {TABLETS}}} "
            "AND consistency = 'global' "
            f"AND preferred_rack = {{'dc1': '{RACKS[0]}'}}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY, v int)")
        groups = await group_ids(manager, ks, 't')
        await wait_for_all_led_by(manager, observer.ip_addr, groups, host_ids[0],
                                  time.time() + 120)

        await cql.run_async(f"ALTER KEYSPACE {ks} WITH preferred_rack = {{'dc1': '{RACKS[1]}'}}")
        await wait_for_all_led_by(manager, observer.ip_addr, groups, host_ids[1],
                                  time.time() + 120)


def replication(racks: dict[str, list[str]]) -> str:
    """The replication clause of a keyspace with the given rack list per datacenter."""
    dcs = ", ".join(f"'{dc}': {dc_racks}" for dc, dc_racks in racks.items())
    return f"replication = {{'class': 'NetworkTopologyStrategy', {dcs}}}"


async def preferred_rack_and_durable_writes(cql: Session, ks: str) -> tuple[dict[str, str], bool]:
    """The preferred_rack and durable_writes options of a keyspace, as stored in the schema."""
    racks = await cql.run_async(
        f"SELECT preferred_rack FROM system_schema.scylla_keyspaces WHERE keyspace_name = '{ks}'")
    durable = await cql.run_async(
        f"SELECT durable_writes FROM system_schema.keyspaces WHERE keyspace_name = '{ks}'")
    return dict(racks[0].preferred_rack or {}), durable[0].durable_writes


@pytest.mark.asyncio
async def test_preferred_rack_alter_with_replication(manager: ScyllaClusterManager):
    """A replication change keeps preferred_rack, which must stay in the new rack list.
    Altered together with the replication, preferred_rack is checked against the new rack
    list, and an empty map unsets it. Like any ALTER, these reset durable_writes unless
    they restate it."""
    servers = [await manager.server_add(config=CONFIG, cmdline=CMDLINE,
                                        property_file={'dc': 'dc1', 'rack': rack})
               for rack in RACKS]
    cql, _ = await manager.get_ready_cql(servers)
    await manager.disable_tablet_balancing()

    async with new_test_keyspace(manager,
            f"WITH {replication({'dc1': RACKS[:2]})} "
            f"AND tablets = {{'initial': {TABLETS}}} "
            "AND consistency = 'global' AND durable_writes = false "
            f"AND preferred_rack = {{'dc1': '{RACKS[0]}'}}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY, v int)")

        await cql.run_async(f"ALTER KEYSPACE {ks} WITH {replication({'dc1': RACKS})}")
        assert await preferred_rack_and_durable_writes(cql, ks) == ({'dc1': RACKS[0]}, True)

        with pytest.raises(ConfigurationException, match=f"preferred_rack '{RACKS[0]}' is not in the replication rack list"):
            await cql.run_async(f"ALTER KEYSPACE {ks} WITH {replication({'dc1': RACKS[1:]})}")
        with pytest.raises(ConfigurationException, match=f"preferred_rack '{RACKS[0]}' is not in the replication rack list"):
            await cql.run_async(f"ALTER KEYSPACE {ks} WITH {replication({'dc1': RACKS[1:]})} "
                                f"AND preferred_rack = {{'dc1': '{RACKS[0]}'}}")

        # Drop the preferred rack and move the preference in one statement.
        await cql.run_async(f"ALTER KEYSPACE {ks} WITH {replication({'dc1': RACKS[1:]})} "
                            f"AND preferred_rack = {{'dc1': '{RACKS[2]}'}} AND durable_writes = false")
        assert await preferred_rack_and_durable_writes(cql, ks) == ({'dc1': RACKS[2]}, False)

        # Prefer a rack which only the new rack list has.
        await cql.run_async(f"ALTER KEYSPACE {ks} WITH {replication({'dc1': RACKS})} "
                            f"AND preferred_rack = {{'dc1': '{RACKS[0]}'}}")
        assert await preferred_rack_and_durable_writes(cql, ks) == ({'dc1': RACKS[0]}, True)

        # Unset it in the same statement as dropping it from the rack list.
        await cql.run_async(f"ALTER KEYSPACE {ks} WITH {replication({'dc1': RACKS[1:]})} "
                            "AND preferred_rack = {}")
        assert await preferred_rack_and_durable_writes(cql, ks) == ({}, True)


@pytest.mark.asyncio
async def test_preferred_rack_alter_with_multi_rf_change(manager: ScyllaClusterManager):
    """Adding a datacenter goes through the multi-step RF change, in which the keyspace
    keeps the old replication until the end. A preferred rack in the new datacenter takes
    effect only when the change completes."""
    servers = [await manager.server_add(config=CONFIG, cmdline=CMDLINE,
                                        property_file={'dc': dc, 'rack': RACKS[0]})
               for dc in ['dc1', 'dc2']]
    cql, _ = await manager.get_ready_cql(servers)
    await manager.disable_tablet_balancing()
    host_ids = await gather_safely(*[manager.get_host_id(s.server_id) for s in servers])

    async with new_test_keyspace(manager,
            f"WITH {replication({'dc1': [RACKS[0]]})} "
            f"AND tablets = {{'initial': {TABLETS}}} AND consistency = 'global' "
            f"AND preferred_rack = {{'dc1': '{RACKS[0]}'}}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY, v int)")

        coordinator = servers[host_ids.index(str(await get_topology_coordinator(manager)))]
        log = await manager.server_open_log(coordinator.server_id)
        mark = await log.mark()

        await cql.run_async(f"ALTER KEYSPACE {ks} WITH {replication({'dc1': [RACKS[0]], 'dc2': [RACKS[0]]})} "
                            f"AND preferred_rack = {{'dc2': '{RACKS[0]}'}}")
        assert await log.grep("will use multi-rf change procedure", from_mark=mark)
        assert (await preferred_rack_and_durable_writes(cql, ks))[0] == {'dc2': RACKS[0]}


@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_preferred_rack_alter_rejected_during_rf_change(manager: ScyllaClusterManager):
    """preferred_rack can't be altered while an RF change of the keyspace is pending."""
    servers = [await manager.server_add(config=CONFIG, cmdline=CMDLINE,
                                        property_file={'dc': 'dc1', 'rack': rack})
               for rack in RACKS]
    cql, _ = await manager.get_ready_cql(servers)
    await manager.disable_tablet_balancing()
    host_ids = await gather_safely(*[manager.get_host_id(s.server_id) for s in servers])

    async with new_test_keyspace(manager,
            f"WITH {replication({'dc1': RACKS[:2]})} "
            f"AND tablets = {{'initial': {TABLETS}}} AND consistency = 'global' "
            f"AND preferred_rack = {{'dc1': '{RACKS[0]}'}}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY, v int)")

        coordinator = servers[host_ids.index(str(await get_topology_coordinator(manager)))]
        injection = await inject_error_one_shot(manager.api, coordinator.ip_addr,
                                                'pause-before-handling-keyspace-rf-change')
        rf_change = cql.run_async(f"ALTER KEYSPACE {ks} WITH {replication({'dc1': RACKS})}")
        try:
            await manager.api.wait_for_injection_enter(coordinator.ip_addr, 'pause-before-handling-keyspace-rf-change')
            with pytest.raises(InvalidRequest, match="retry altering preferred_rack later"):
                await cql.run_async(f"ALTER KEYSPACE {ks} WITH preferred_rack = {{'dc1': '{RACKS[1]}'}}")
        finally:
            await injection.message()
            await rf_change
        assert (await preferred_rack_and_durable_writes(cql, ks))[0] == {'dc1': RACKS[0]}


@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_preferred_leaders_ping_pong_is_stopped(manager: ScyllaClusterManager):
    """Two replicas which prefer each other hand the leadership back and forth. After a
    few rounds one of them withdraws its preferred leaders and keeps the leadership until
    the hold ends, after which the bounce resumes and is stopped again."""
    servers = [await manager.server_add(config=CONFIG, cmdline=CMDLINE,
                                        property_file={'dc': 'dc1', 'rack': rack})
               for rack in RACKS[:2]]
    cql, _ = await manager.get_ready_cql(servers)
    host_ids = await gather_safely(*[manager.get_host_id(s.server_id) for s in servers])
    hold_s = 20
    for server, other in zip(servers, reversed(host_ids)):
        await manager.api.enable_injection(server.ip_addr, 'sc_preferred_leader_override', one_shot=False,
                                           parameters={'value': str(other)})
        await manager.api.enable_injection(server.ip_addr, 'sc_priority_members_hold_ms', one_shot=False,
                                           parameters={'value': str(hold_s * 1000)})
    logs = [await manager.server_open_log(s.server_id) for s in servers]
    marks = [await log.mark() for log in logs]

    async def holds() -> int:
        found = [await log.grep("ignoring the preferred leaders", from_mark=mark) for log, mark in zip(logs, marks)]
        return sum(len(matches) for matches in found)

    async def holds_at_least(count: int) -> bool | None:
        return True if await holds() >= count else None

    async with new_test_keyspace(manager,
            f"WITH {replication({'dc1': RACKS[:2]})} "
            "AND tablets = {'initial': 1} AND consistency = 'global'") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY, v int)")
        [group] = await group_ids(manager, ks, 't')

        await wait_for(lambda: holds_at_least(1), time.time() + 180, period=1, label="first hold")
        # The holding replica leads, and the other one, which prefers it, leaves it be.
        async def agreed_leader() -> str | None:
            seen = {await leader_of(manager, s.ip_addr, group) for s in servers}
            return seen.pop() if len(seen) == 1 and None not in seen else None
        leader = await wait_for(agreed_leader, time.time() + 60, period=1, label="agreed leader")
        for _ in range(hold_s // 2):
            await asyncio.sleep(1)
            assert await leader_of(manager, servers[0].ip_addr, group) == leader

        # Only the restored list can start the bounce, and with it the next hold.
        marks[:] = [await log.mark() for log in logs]
        await wait_for(lambda: holds_at_least(1), time.time() + 180, period=1, label="second hold")
