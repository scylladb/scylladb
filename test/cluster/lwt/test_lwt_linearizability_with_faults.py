#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Exploratory linearizability test for LWT under node crashes and tablet migrations.

Idea: each partition key of one table is a register holding an int that is
only ever changed through lightweight transactions.  A writer sets a register
with a compare-and-set against the value it last saw there: ``INSERT ... IF
NOT EXISTS`` while it believes the register is empty, ``UPDATE ... IF c = ?``
otherwise, always to a globally unique value; now and then it deletes the row
instead, ``DELETE ... IF c = ?``, which empties the register and leaves a
tombstone for the next rounds to decide on.  Readers do SERIAL reads; one
reader does QUORUM reads: a quorum read returns only once its read repair is
done, so quorum reads are monotonic and, with every write learned by a quorum
before the client is answered, must be linearizable too.  Should that reader
alone ever flag a violation, question that claim before the LWT code.  Every
attempt goes into a history that the Porcupine checker verifies per key
against a register with a ``cas`` operation: an applied CAS replaced exactly
the value it expected, a CAS reported as not applied saw the value the
register held at that moment, and a CAS whose outcome the client never learned
may have applied at any later point while the register still held its
expected value.  The final value of every register is read once more at the
end and checked the same way.

Meanwhile a nemesis task takes the cluster apart.  Each of its rounds runs
every action once, in random order:

* ``crash``: SIGKILL a node, keep it down for a few seconds, start it again.
  Sometimes the node is first parked by error injection in the middle of a
  paxos accept (before the proposal is saved) or a learn (after the decision
  was applied but before it was recorded), so the kill lands right there; or
  it first fails every learn for a while, so that the kill takes away
  promises and proposals it acknowledged without ever syncing them.
* ``restart``: stop a node gracefully, with paxos rounds in flight on it,
  and start it again.
* ``paxos_rpc_fault``: for a moment one replica fails one kind of paxos step
  after doing its part -- the promise or the proposal is saved but the
  coordinator gets an error back, or a decision is not learned -- so a later
  round has to repair it.
* ``disconnect``: drop the RPC connections between two nodes, failing
  whatever paxos messages were in flight on them.
* ``repair``: repair the table; with tablets the paxos state table is
  co-located with it, so promises and proposals travel between replicas too.
* ``migrate``: move one tablet to another node, or to the other shard.
* ``migrate_and_crash``: start a migration, park it while it streams, crash a
  node meanwhile (the node receiving the tablet, one of the tablet's replicas
  or a bystander), bring it back and let the migration finish.
* ``resize``: double the table's tablet count, or halve it back: every tablet
  of the table and of its paxos state table is split or merged under load.

The commitlog sync period is raised so that nothing but an explicit sync makes
a write durable before a crash: a paxos promise or proposal acknowledged
without a sync is lost with the kill, which is how the paxos state of tables
with tablets used to be lost.

What is accepted: inside a crash or restart window the errors a lost node
causes (see tolerate_reboot); at any time a CAS or a read may time out,
because writers contending for one key cut each other's paxos rounds off.
Anything else -- an Unavailable or a server error while the cluster is whole,
e.g. during a migration -- fails the test.  Linearizability is checked
regardless of whether the tasks failed, so a violation is reported either
way.  A run that did not crash a node, or with tablets did not migrate one,
proves nothing and fails as well.

Setup: 4 nodes with 2 shards each, one DC, RF=3; with tablets, 4 tablets so
that migrations hit a key that is being written.  Tablet balancing is off
except while a resize runs, so otherwise only the nemesis moves tablets.  The
vnodes flavour has no tablet actions.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import Counter
from dataclasses import dataclass, field
from typing import Callable, Optional

import pytest
from cassandra import (
    ConsistencyLevel,
    OperationTimedOut,
    ReadFailure,
    ReadTimeout,
    WriteFailure,
    WriteTimeout,
)
from cassandra.policies import FallthroughRetryPolicy
from cassandra.query import PreparedStatement

from test.cluster.strong_consistency.outcomes import (
    FailureContext,
    Outcome,
    first_match,
    only_during,
    tolerate_reboot,
)
from test.cluster.strong_consistency.register_workload import (
    RegisterWorkload,
    check_linearizable,
    reader_task,
    run_workload,
)
from test.cluster.lwt.lwt_common import wait_for_tablet_count
from test.cluster.util import new_test_keyspace
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.tablets import get_all_tablet_replicas, get_tablet_count
from test.pylib.util import wait_for_cql_and_get_hosts

logger = logging.getLogger(__name__)

NUM_NODES = 4
SMP = 2
RF = 3
NUM_TABLETS = 4

NUM_KEYS = 24
NUM_WRITERS = 4
# The last reader uses QUORUM, the others SERIAL.
NUM_READERS = 4
DURATION_S = 180
MIN_WRITES = 200
MIN_READS = 200
# How often a writer deletes a row it believes to exist instead of updating it.
DELETE_P = 0.2

# How long a crashed node stays down.
DOWNTIME_S = (0.0, 5.0)
# Pause between two nemesis actions.
NEMESIS_PAUSE_S = (1.0, 3.0)

CRASH = "crash"

# Where a crash may land: right away; once a replica is parked by an injection
# in the middle of a paxos step (the first two are wait points); or after the
# replica spent a while failing every learn, so that it holds promises and
# proposals it acknowledged but, with no learn to force a commitlog sync,
# never made durable.
NO_LEARN = "paxos_error_before_learn"
KILL_POINTS = [None, "paxos_accept_proposal_wait", "paxos_state_learn_after_mutate", NO_LEARN]
# How long a replica fails its learns before the kill.
NO_LEARN_S = (0.5, 2.0)

# Errors a replica raises after doing its part of a paxos step.
PAXOS_RPC_FAULTS = [
    "paxos_error_after_save_promise",
    "paxos_error_after_save_proposal",
    "paxos_error_before_learn",
]

# Only forced syncs (a learn does one) make anything durable before a crash.
SERVER_CONFIG = {
    "commitlog_sync_period_in_ms": 3600000,
    "rf_rack_valid_keyspaces": False,
}
SERVER_CMDLINE = ["--smp", str(SMP), "--logger-log-level", "paxos=debug"]


def tolerate_lwt_timeouts(exc: BaseException, ctx: FailureContext) -> Optional[Outcome]:
    """Accept what LWT may do on a whole cluster: time out.

    Writers contending for one key preempt each other's paxos rounds until the
    contention timeout runs out; with the nemesis dropping replies, a round can
    also end in "write timeout due to uncertainty" or a WriteFailure.  Any of
    these leaves a CAS indeterminate.  A timed-out read changes no state.
    """
    if ctx.is_write and isinstance(exc, (WriteTimeout, WriteFailure, OperationTimedOut)):
        return Outcome.UNKNOWN
    if not ctx.is_write and isinstance(exc, (ReadTimeout, ReadFailure, OperationTimedOut)):
        return Outcome.FAIL
    return None


@dataclass
class CasWorkload(RegisterWorkload):
    """The register workload with LWT statements.

    ``write_stmt`` is the conditional update, ``insert_stmt`` the conditional
    insert for a register the writer believes empty, ``delete_stmt`` the
    conditional delete that empties one, ``read_stmt`` a SERIAL read and
    ``quorum_read_stmt`` a QUORUM one.
    """

    insert_stmt: Optional[PreparedStatement] = field(default=None, repr=False)
    delete_stmt: Optional[PreparedStatement] = field(default=None, repr=False)
    quorum_read_stmt: Optional[PreparedStatement] = field(default=None, repr=False)

    def prepare(self, cql) -> None:
        self.write_stmt = cql.prepare(f"UPDATE {self.fqtn} SET c = ? WHERE pk = ? IF c = ?")
        self.insert_stmt = cql.prepare(f"INSERT INTO {self.fqtn} (pk, c) VALUES (?, ?) IF NOT EXISTS")
        self.delete_stmt = cql.prepare(f"DELETE FROM {self.fqtn} WHERE pk = ? IF c = ?")
        self.read_stmt = cql.prepare(f"SELECT c FROM {self.fqtn} WHERE pk = ?")
        self.read_stmt.consistency_level = ConsistencyLevel.SERIAL
        self.quorum_read_stmt = cql.prepare(f"SELECT c FROM {self.fqtn} WHERE pk = ?")
        self.quorum_read_stmt.consistency_level = ConsistencyLevel.QUORUM
        # One attempt per recorded operation, see RegisterWorkload.prepare().
        for stmt in (self.write_stmt, self.insert_stmt, self.delete_stmt, self.read_stmt, self.quorum_read_stmt):
            stmt.retry_policy = FallthroughRetryPolicy()

    def read_statement(self, reader_id: int) -> PreparedStatement:
        if reader_id == self.num_readers - 1:
            return self.quorum_read_stmt
        return self.read_stmt

    @property
    def final_read_client_id(self) -> int:
        return self.reset_client_id + 1


async def cas_writer_task(workload: CasWorkload, cql, writer_id: int) -> None:
    rng = workload.rng_for(f"writer/{writer_id}")
    # What this writer last saw in each register; 0 is an empty one.  Another
    # writer's CAS makes it stale, and the next CAS then comes back not
    # applied with the current value.
    seen: dict[int, int] = {}
    logger.info("Writer %d started", writer_id)

    while not workload.stop_event.is_set():
        pk = rng.randint(0, workload.num_keys - 1)
        expected = seen.get(pk, 0)
        # A delete is a CAS to the empty register, 0.
        delete = expected != 0 and rng.random() < DELETE_P
        value = 0 if delete else workload.next_write_value()

        op_id, t_call_ns = workload.history.record_call(
            writer_id, "cas", pk, value, expected=expected)
        try:
            if delete:
                bound = workload.delete_stmt.bind([pk, expected])
            elif expected == 0:
                bound = workload.insert_stmt.bind([pk, value])
            else:
                bound = workload.write_stmt.bind([value, pk, expected])
            row = (await cql.run_async(bound))[0]
            if row.applied:
                workload.history.record_return(
                    op_id, writer_id, "cas", pk, value, Outcome.OK)
                seen[pk] = value
            else:
                # The row comes back with the current values of the condition
                # columns, or without them when it does not exist: an empty
                # register.
                current = getattr(row, "c", None) or 0
                workload.history.record_return(
                    op_id, writer_id, "cas", pk, current, Outcome.FAIL)
                seen[pk] = current
        except Exception as exc:
            workload.record_failure(
                exc, op_id=op_id, client_id=writer_id, op="cas", key=pk,
                value=value, t_call_ns=t_call_ns)

        await asyncio.sleep(rng.uniform(*workload.dml_pause_s))

    logger.info("Writer %d finished: %s", writer_id, workload.stats(writer_id))


async def read_final_values(workload: CasWorkload, cql) -> None:
    """Read every register once more, into the history, after the clients stopped."""
    client_id = workload.final_read_client_id
    for pk in range(workload.num_keys):
        op_id, t_call_ns = workload.history.record_call(client_id, "read", pk)
        try:
            rows = await cql.run_async(workload.read_stmt.bind([pk]))
            value = 0 if not rows or rows[0].c is None else rows[0].c
            workload.history.record_return(op_id, client_id, "read", pk, value, Outcome.OK)
        except Exception as exc:
            workload.record_failure(
                exc, op_id=op_id, client_id=client_id, op="read", key=pk,
                value=0, t_call_ns=t_call_ns)


@dataclass
class Nemesis:
    manager: ScyllaClusterManager
    servers: list
    workload: CasWorkload
    cql: object
    scale_timeout: Callable[[float], float]
    tablets: bool
    host_ids: dict = field(default_factory=dict)  # server_id -> host id
    actions: Counter = field(default_factory=Counter)

    def __post_init__(self):
        self.rng = self.workload.rng_for("nemesis")

    def server_of(self, host_id):
        return next(s for s in self.servers if self.host_ids[s.server_id] == host_id)

    async def injection_fired(self, node, injection: str, timeout: float) -> bool:
        """Poll until the injection was entered at least once, or give up."""
        deadline = time.time() + self.scale_timeout(timeout)
        while time.time() < deadline:
            if await self.manager.api.get_injection_enter_count(node.ip_addr, injection) > 0:
                return True
            await asyncio.sleep(0.1)
        return False

    async def run(self) -> None:
        choices = [self.crash, self.restart, self.paxos_rpc_fault, self.disconnect, self.repair]
        if self.tablets:
            choices += [self.migrate, self.migrate_and_crash, self.resize]
        while not self.workload.stop_event.is_set():
            # Every action once per round, so that a run covers them all.
            for action in self.rng.sample(choices, len(choices)):
                if self.workload.stop_event.is_set():
                    break
                logger.info("Nemesis: %s", action.__name__)
                await action()
                self.actions[action.__name__] += 1
                await asyncio.sleep(self.rng.uniform(*NEMESIS_PAUSE_S))
        logger.info("Nemesis finished: %s", dict(self.actions))

    async def restart(self) -> None:
        """Stop a node gracefully, keep it down for a while and start it again."""
        victim = self.rng.choice(self.servers)
        async with self.workload.disruption_window(CRASH):
            logger.info("Nemesis: stopping %s gracefully", victim.ip_addr)
            await self.manager.server_stop_gracefully(victim.server_id)
            await asyncio.sleep(self.rng.uniform(*DOWNTIME_S))
            await self.manager.server_start(victim.server_id)
            await wait_for_cql_and_get_hosts(self.cql, [victim], time.time() + self.scale_timeout(120))
            logger.info("Nemesis: %s is back", victim.ip_addr)

    async def repair(self) -> None:
        """Repair the table from a random node; with tablets its paxos state table goes with it."""
        node = self.rng.choice(self.servers)
        logger.info("Nemesis: repairing %s on %s", self.workload.fqtn, node.ip_addr)
        await self.manager.api.repair(node.ip_addr, self.workload.ks, self.workload.table_name)

    async def resize(self) -> None:
        """Double the tablet count, or halve it back, through the balancer."""
        api_node = self.rng.choice(self.servers)
        current = await get_tablet_count(self.manager, api_node, self.workload.ks, self.workload.table_name)
        target = NUM_TABLETS if current > NUM_TABLETS else 2 * NUM_TABLETS
        split = target > current
        logger.info("Nemesis: resizing %s from %d to %d tablets", self.workload.fqtn, current, target)
        await self.manager.enable_tablet_balancing()
        try:
            await self.cql.run_async(f"ALTER TABLE {self.workload.fqtn} WITH tablets = {{'min_tablet_count': {target}}}")
            await wait_for_tablet_count(
                self.manager, api_node, self.workload.ks, self.workload.table_name,
                (lambda c: c >= target) if split else (lambda c: c <= target), target,
                scale_timeout=self.scale_timeout, timeout_s=180)
        except TimeoutError as e:
            # A merge waits for tablet size statistics the balancer may not
            # have yet; that is not what this test is about.
            logger.info("Nemesis: resize did not converge, moving on: %s", e)
            self.actions["resize/timeout"] += 1
            return
        finally:
            await self.manager.disable_tablet_balancing()
        self.actions["resize/split" if split else "resize/merge"] += 1

    async def crash(self, victim=None) -> None:
        """Kill a node, keep it down for a while, start it and wait for the driver to have it back."""
        if victim is None:
            victim = self.rng.choice(self.servers)
        park = self.rng.choice(KILL_POINTS)
        async with self.workload.disruption_window(CRASH):
            if park == NO_LEARN:
                await self.manager.api.enable_injection(victim.ip_addr, park, one_shot=False)
                await asyncio.sleep(self.rng.uniform(*NO_LEARN_S))
            elif park:
                # Every paxos step of that kind on the node parks from now on;
                # the kill comes once at least one did.
                await self.manager.api.enable_injection(victim.ip_addr, park, one_shot=False)
                if not await self.injection_fired(victim, park, timeout=10):
                    logger.info("No paxos step parked at %s on %s, killing anyway", park, victim.ip_addr)
            logger.info("Nemesis: killing %s (parked at %s)", victim.ip_addr, park)
            await self.manager.server_stop(victim.server_id, convict=False)
            self.actions[f"kill@{park}"] += 1
            await asyncio.sleep(self.rng.uniform(*DOWNTIME_S))
            logger.info("Nemesis: starting %s", victim.ip_addr)
            await self.manager.server_start(victim.server_id)
            await wait_for_cql_and_get_hosts(self.cql, [victim], time.time() + self.scale_timeout(120))
            logger.info("Nemesis: %s is back", victim.ip_addr)

    async def paxos_rpc_fault(self) -> None:
        """Make one replica fail one kind of paxos step, after having done its part of it.

        Not one-shot: a one-shot injection takes its enter count with it when
        it fires, so there would be no telling whether it did.  It stays on
        until it fired at least once, which is a matter of milliseconds.
        """
        node = self.rng.choice(self.servers)
        fault = self.rng.choice(PAXOS_RPC_FAULTS)
        await self.manager.api.enable_injection(node.ip_addr, fault, one_shot=False)
        try:
            if await self.injection_fired(node, fault, timeout=10):
                logger.info("Nemesis: %s fired on %s", fault, node.ip_addr)
                self.actions[fault] += 1
            else:
                logger.info("Nemesis: %s did not fire on %s", fault, node.ip_addr)
        finally:
            await self.manager.api.disable_injection(node.ip_addr, fault)

    async def disconnect(self) -> None:
        """Drop the RPC connections between two nodes; they reconnect on their own."""
        a, b = self.rng.sample(self.servers, 2)
        logger.info("Nemesis: disconnecting %s from %s", a.ip_addr, b.ip_addr)
        await self.manager.api.inject_disconnect(a.ip_addr, b.ip_addr)

    async def pick_migration(self, api_node):
        """A random tablet, one of its replicas and where to move it: another node, or the other shard."""
        tablets = await get_all_tablet_replicas(self.manager, api_node, self.workload.ks, self.workload.table_name)
        tablet = self.rng.choice(tablets)
        src_host, src_shard = self.rng.choice(tablet.replicas)
        intranode = self.rng.random() < 0.3
        if intranode:
            dst_host, dst_shard = src_host, (src_shard + 1) % SMP
        else:
            replica_hosts = {h for h, _ in tablet.replicas}
            dst_host = self.rng.choice([h for h in self.host_ids.values() if h not in replica_hosts])
            dst_shard = self.rng.randrange(SMP)
        return tablet, (src_host, src_shard), (dst_host, dst_shard), intranode

    async def migrate(self) -> None:
        api_node = self.rng.choice(self.servers)
        tablet, src, dst, intranode = await self.pick_migration(api_node)
        logger.info("Nemesis: migrating tablet %s from %s to %s", tablet.last_token, src, dst)
        await self.manager.api.move_tablet(
            api_node.ip_addr, self.workload.ks, self.workload.table_name,
            *src, *dst, tablet.last_token, timeout=self.scale_timeout(120))
        self.actions["migrate/intranode" if intranode else "migrate/internode"] += 1

    async def migrate_and_crash(self) -> None:
        """Park a migration while it streams, crash a node, then let it finish.

        The migration is driven through a node that is not the victim, so
        that the API call outlives the crash.
        """
        victim = self.rng.choice(self.servers)
        api_node = self.rng.choice([s for s in self.servers if s is not victim])
        tablet, src, dst, intranode = await self.pick_migration(api_node)
        dst_server = self.server_of(dst[0])
        park = "intranode_migration_streaming_wait" if intranode else "migration_streaming_wait"
        logger.info("Nemesis: migrating tablet %s from %s to %s, parked at %s on %s, then crashing %s",
                    tablet.last_token, src, dst, park, dst_server.ip_addr, victim.ip_addr)
        await self.manager.api.enable_injection(dst_server.ip_addr, park, one_shot=True)
        migration = asyncio.create_task(self.manager.api.move_tablet(
            api_node.ip_addr, self.workload.ks, self.workload.table_name,
            *src, *dst, tablet.last_token, timeout=self.scale_timeout(300)))
        try:
            await self.manager.api.wait_for_injection_enter(
                dst_server.ip_addr, park, deadline=time.time() + self.scale_timeout(60))
            await self.crash(victim)
            if victim is not dst_server:
                # A crashed destination comes back without the injection and
                # streams again on its own.
                await self.manager.api.message_injection(dst_server.ip_addr, park)
            await migration
        finally:
            if not migration.done():
                migration.cancel()
        role = "destination" if victim is dst_server else (
            "replica" if self.host_ids[victim.server_id] in {h for h, _ in tablet.replicas} else "bystander")
        self.actions[f"migrate_and_crash/{role}"] += 1


@pytest.mark.parametrize("tablets", [True, False], ids=["tablets", "vnodes"])
@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
@pytest.mark.skip_mode(mode="debug", reason="debug mode is too slow for this test")
async def test_lwt_linearizability_with_faults(manager: ScyllaClusterManager, scale_timeout, tmp_path, tablets: bool):
    servers = await manager.servers_add(NUM_NODES, config=SERVER_CONFIG, cmdline=SERVER_CMDLINE)
    cql = manager.get_cql()
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + scale_timeout(60))
    if tablets:
        await manager.disable_tablet_balancing()

    tablets_opts = f"{{'initial': {NUM_TABLETS}}}" if tablets else "{'enabled': false}"
    ks_opts = (f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {RF}}} "
               f"AND tablets = {tablets_opts}")
    async with new_test_keyspace(manager, ks_opts) as ks:
        workload = CasWorkload(
            ks=ks, table_name="lwt", name="lwt", num_keys=NUM_KEYS,
            num_writers=NUM_WRITERS, num_readers=NUM_READERS,
            exception_policy=first_match(only_during(CRASH, tolerate_reboot), tolerate_lwt_timeouts),
        )
        await workload.create_table(cql)
        # The first LWT on a table with tablets creates its paxos state table;
        # keep that out of the measured run (and of the key range).
        await cql.run_async(f"INSERT INTO {workload.fqtn} (pk, c) VALUES (-1, 0) IF NOT EXISTS")

        host_ids = {s.server_id: await manager.get_host_id(s.server_id) for s in servers}
        nemesis = Nemesis(manager, servers, workload, cql, scale_timeout, tablets, host_ids)

        errors = await run_workload(
            workload, cql, duration_s=DURATION_S,
            disruptors=[("nemesis", nemesis.run)],
            writer=cas_writer_task, reader=reader_task,
        )
        logger.info("Run finished: %s; nemesis actions: %s", workload.stats_line(), dict(nemesis.actions))

        await read_final_values(workload, cql)
        await check_linearizable(workload, output_dir=tmp_path / "porcupine")

        assert not errors, f"Task(s) failed (seed={workload.seed}): {errors}"
        workload.assert_progress(min_writes=MIN_WRITES, min_reads=MIN_READS)
        required = ["crash", "migrate"] if tablets else ["crash"]
        never_ran = [a for a in required if not nemesis.actions[a]]
        assert not never_ran, f"the nemesis never got to {never_ran} (seed={workload.seed}): {dict(nemesis.actions)}"
