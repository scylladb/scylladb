#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""How a failed read/write is reported to the linearizability checker.

Every operation ends up in the history as ``ok`` (it happened), ``fail`` (it
definitely did not — the checker drops it) or ``info`` (indeterminate: the
checker must consider both).  Getting this wrong is not a flaky test, it is a
*wrong* test — calling an indeterminate write ``fail`` hides real violations,
and calling a definite failure ``info`` makes the checker accept histories it
should reject.

Which exception means which depends on what the test does to the cluster, so
the workload asks an :data:`ExceptionPolicy`.  A policy returns ``None`` for
anything it does not recognise, and the workload then fails the test: an
unexpected exception is a finding, not noise.

Tests take the default, compose the built-ins, or write their own::

    RegisterWorkload(ks=ks)                                  # DDL only
    RegisterWorkload(ks=ks, exception_policy=first_match(    # also kills nodes
        tolerate_reset_windows, tolerate_timeouts))
    RegisterWorkload(ks=ks, exception_policy=strict)         # nothing may fail
    RegisterWorkload(ks=ks, exception_policy=first_match(    # also restarts nodes
        tolerate_reset_windows,
        only_during("restart", tolerate_reboot, grace_s=5)))

What an error says about the operation does not depend on when it happened, and
:func:`tolerate_reboot` does not ask.  Whether the cluster may raise it at that
moment does: the same NoHostAvailable is expected while a node is down and a
finding while the cluster is whole.  :func:`only_during` makes that second
claim, for the windows the test opened with
:meth:`~test.cluster.strong_consistency.workload.RegisterWorkload.disruption_window`.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Callable, Optional

from cassandra import (
    OperationTimedOut,
    ReadFailure,
    ReadTimeout,
    Unavailable,
    WriteFailure,
    WriteTimeout,
)
from cassandra.cluster import NoHostAvailable
from cassandra.connection import ConnectionException
from cassandra.policies import FallthroughRetryPolicy
from cassandra.protocol import InvalidRequest, ServerError

if TYPE_CHECKING:
    from test.cluster.strong_consistency.workload import RegisterWorkload


class Outcome(StrEnum):
    """Status recorded for an operation in the Porcupine history.

    These strings are the checker's vocabulary, not ours: ``main.go`` in
    scylladb/porcupine_validator maps exactly "ok", "fail" and "unknown", and
    maps *anything else* to a status its register model has no branch for —
    which it then reports as a linearizability violation.  So a wrong spelling
    here does not produce an error, it produces a fabricated bug in ScyllaDB.

    (Jepsen calls the indeterminate outcome ``:info``; the checker calls it
    ``unknown``.  Same thing, different word.)
    """

    OK = "ok"
    FAIL = "fail"
    UNKNOWN = "unknown"

    #: ``UNKNOWN`` is what a policy *returns*; it is never written into the
    #: history as a status.  See :meth:`RegisterWorkload.record_failure` for how
    #: the workload expresses it instead, and why it must not be a status.


@dataclass(frozen=True)
class FailureContext:
    """Everything a policy may need to classify one failed operation."""

    workload: "RegisterWorkload"
    op: str  # "read" or "write"
    client_id: int
    key: int
    t_call_ns: int
    t_return_ns: int

    @property
    def is_write(self) -> bool:
        return self.op == "write"


# Returns the status to record, or None if the exception is not expected —
# in which case the workload fails the test.
ExceptionPolicy = Callable[[BaseException, FailureContext], Optional[Outcome]]


# TODO: these substring checks have not been validated against every message
# the server can produce for an absent table.  They are what the stress runs
# have actually seen; widen them when a run fails on one that is missing.
def is_table_absence_invalid_request(exc: InvalidRequest) -> bool:
    return _is_table_absence_message(str(exc).lower())


def _is_table_absence_message(msg: str) -> bool:
    return (
        "unconfigured table" in msg
        or "does not exist" in msg
        or "unknown table" in msg
        or "undefined table" in msg
        or ("keyspace" in msg and "does not exist" in msg)
    )


# TODO(SCYLLADB-4655): drop this special case once the server reports an
# unknown table id as a regular InvalidRequest.  Sending a prepared statement
# tagged with the dropped table's UUID is a legitimate thing for a client to do
# after a DROP+RECREATE, but instead of a "table does not exist" error the
# server produces one that the driver surfaces as NoHostAvailable, so a policy
# cannot tell it apart from a real loss of connectivity by exception type
# alone.  Until that is fixed, recognize it by message and tolerate it inside a
# reset window.
def is_stale_table_uuid(exc: NoHostAvailable) -> bool:
    """Detect NoHostAvailable caused by a prepared statement referencing a
    dropped table's UUID.  After DROP+RECREATE the new table gets a new UUID,
    but the driver may still send requests tagged with the old one."""
    for inner in exc.errors.values():
        msg = str(inner).lower()
        if "can't find a column family" in msg:
            return True
    return False


def is_table_absence_error(exc: BaseException) -> bool:
    """Return True if the exception indicates the table is absent (dropped/recreated).

    Two kinds of them are a SERVER_ERROR.  "Can't find a column family" is
    one.  The other is a forwarded request whose statement the target has to
    prepare again: the target's "unconfigured table" or "keyspace does not
    exist" comes back through the prepare RPC as a remote error, and the
    forwarding node returns it as a SERVER_ERROR whose message is the
    exception's type and its message.  The driver's default retry policy
    re-sends both to the next host; with FallthroughRetryPolicy they arrive
    as they are.
    """
    if isinstance(exc, InvalidRequest):
        return is_table_absence_invalid_request(exc)
    if isinstance(exc, NoHostAvailable):
        return is_stale_table_uuid(exc)
    if isinstance(exc, ServerError):
        msg = str(exc).lower()
        return "can't find a column family" in msg or (
            _FORWARDED_INVALID_REQUEST.search(msg) is not None
            and _is_table_absence_message(msg))
    return False


# The type of a remote invalid_request_exception, or of one of its subclasses
# (keyspace_not_defined_exception), as the forwarding node prints it.
_FORWARDED_INVALID_REQUEST = re.compile(
    r"exceptions::(invalid_request|keyspace_not_defined)_exception \(")


# The coordinator rejected the request outright, without forwarding it to any
# replica, so nothing can have been applied -- whatever the operation was.
NEVER_APPLIED = (Unavailable,)

# The coordinator gave up without knowing the outcome, so neither does the
# client: the write may still be applied after the client stopped waiting.
# Calling any of these 'fail' would tell the checker to drop the operation, and
# a later read of that value then looks like a register returning something
# nobody ever wrote -- a fabricated violation.
WRITE_MAY_HAVE_APPLIED = (
    WriteTimeout,
    WriteFailure,
    OperationTimedOut,
)

# Not a claim about what the server did: a read changes no state, so dropping a
# failed one from the history is sound whether or not it executed -- and
# cheaper than an extra branch for the checker to explore.
READ_SAFE_TO_DROP = (ReadTimeout, ReadFailure, OperationTimedOut)


def strict(exc: BaseException, ctx: FailureContext) -> Optional[Outcome]:
    """Tolerate nothing: any exception fails the test."""
    return None


def tolerate_reset_windows(
    exc: BaseException, ctx: FailureContext,
) -> Optional[Outcome]:
    """Accept "the table is not there" while a reset is in flight.

    A DROP+CREATE makes the table briefly absent, and readers/writers racing
    with it get InvalidRequest (or NoHostAvailable for a prepared statement
    holding the old table UUID).  The operation definitely did not happen —
    there was no table to apply it to — so it is a clean ``fail``.

    The same error *outside* a reset window is not tolerated: that is the bug
    this kind of test is looking for.
    """
    if is_table_absence_error(exc) and ctx.workload.overlaps_reset(
        ctx.t_call_ns, ctx.t_return_ns
    ):
        return Outcome.FAIL
    return None


def tolerate_timeouts(
    exc: BaseException, ctx: FailureContext,
) -> Optional[Outcome]:
    """Accept the failures a briefly unavailable cluster causes.

    Use this for disruptors that can make the cluster stop answering — node
    kills, restarts, network partitions, tablet migrations.

    The classification follows what the driver's own exceptions promise:

    ==========================  =========  ===============================
    exception                   status     why
    ==========================  =========  ===============================
    Unavailable                 fail       coordinator refused without
                                           forwarding to any replica
    WriteTimeout                unknown    replicas did not answer in time
    WriteFailure                unknown    some replica may have applied it
    OperationTimedOut (write)   unknown    no response at all
    ReadTimeout / ReadFailure   fail       a read changes no state
    OperationTimedOut (read)    fail       same
    ==========================  =========  ===============================

    Every ``unknown`` write costs the checker a branch (its register model is
    nondeterministic for them) against a 10s-per-key budget, so keep their
    number in check — :meth:`RegisterWorkload.record_failure` has the measured
    limits, and ``Stats.pending`` says how close a run came to them.
    """
    if isinstance(exc, NEVER_APPLIED):
        return Outcome.FAIL
    if ctx.is_write:
        if isinstance(exc, WRITE_MAY_HAVE_APPLIED):
            return Outcome.UNKNOWN
    elif isinstance(exc, READ_SAFE_TO_DROP):
        return Outcome.FAIL
    return None


def first_match(*policies: ExceptionPolicy) -> ExceptionPolicy:
    """Combine policies: the first one that recognises the exception wins."""

    def policy(exc: BaseException, ctx: FailureContext) -> Optional[Outcome]:
        for candidate in policies:
            outcome = candidate(exc, ctx)
            if outcome is not None:
                return outcome
        return None

    return policy


def only_during(name: str, policy: ExceptionPolicy, *,
                grace_s: float = 0) -> ExceptionPolicy:
    """Ask ``policy`` only about operations that overlapped a window ``name``.

    Anything else is not recognised, and the workload fails the test: the error
    the disruption excuses is a finding outside it.

    ``grace_s`` pushes the end of every window that much later.  A window
    closes when the test's own step returns, and the cluster can take longer to
    look whole to a client than that: the driver reconnects its pool to the
    restarted node on its own schedule, and until then requests routed to it
    find no connection.  Keep it as short as a run allows -- every second of it
    is a second in which those errors stop being findings.
    """
    grace_ns = int(grace_s * 1e9)

    def guarded(exc: BaseException, ctx: FailureContext) -> Optional[Outcome]:
        if not ctx.workload.overlaps_window(
                name, ctx.t_call_ns, ctx.t_return_ns, grace_ns):
            return None
        return policy(exc, ctx)

    return guarded


# What the coordinator says when raft lost track of an entry it had appended
# (coordinator.cc, raft::commit_status_unknown).  A SERVER_ERROR for now.
OUTCOME_UNKNOWN = "outcome of this statement is unknown"


def tolerate_reboot(
    exc: BaseException, ctx: FailureContext,
) -> Optional[Outcome]:
    """Accept the failures a stopped, killed or restarting node causes.

    Whenever they happen: what each of them says about the operation holds
    while the node is down and after it is back alike.  Whether the cluster
    may raise them at that moment is a claim of the test, not of the outcome:
    wrap it in :func:`only_during` to make it.

    Only correct with the driver's retries off (FallthroughRetryPolicy, the
    workload's default retry_policy), and asserted:
    each recorded operation is then one attempt at one node, and its error says
    what became of it.  With retries on, NoHostAvailable can follow an attempt
    that was applied, and calling it ``fail`` fabricates a violation.

    ==============================  =========  ============================
    exception                       status     why
    ==============================  =========  ============================
    NoHostAvailable                 fail       no live host to send it to:
                                               it never left the client
    ServerError "unknown verb"      fail       forwarded to a node whose CQL
                                               server, which registers the
                                               forwarding verbs, is not up
                                               yet (SCYLLADB-4722); refused
                                               before anything ran
    ConnectionException (write)     unknown    the connection died with the
                                               request in flight
    ServerError "outcome unknown"   unknown    raft appended the entry and
      (write)                                  lost its term before it knew
                                               whether it was committed
    either of these (read)          fail       a read changes no state
    anything else                   as tolerate_timeouts
    ==============================  =========  ============================
    """
    assert isinstance(ctx.workload.retry_policy, FallthroughRetryPolicy), (
        f"tolerate_reboot needs one attempt per operation, the workload has "
        f"retry_policy={ctx.workload.retry_policy!r}")
    if isinstance(exc, NoHostAvailable) or (
            isinstance(exc, ServerError) and "unknown verb" in str(exc)):
        return Outcome.FAIL
    if isinstance(exc, ConnectionException) or (
            isinstance(exc, ServerError) and OUTCOME_UNKNOWN in str(exc)):
        return Outcome.UNKNOWN if ctx.is_write else Outcome.FAIL
    return tolerate_timeouts(exc, ctx)
