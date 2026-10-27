#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""How a failed read/write is reported to the linearizability checker.

A policy classifies a failed operation as ``FAIL`` (it definitely did not
happen: recorded as a ``fail`` return, which the checker drops) or ``UNKNOWN``
(indeterminate: recorded as a call with no return, which the checker keeps
open to the end of the history and so must consider both ways).  Getting this
wrong is not a flaky test, it is a *wrong* test — calling an indeterminate
write ``FAIL`` hides real violations, and calling a definite failure
``UNKNOWN`` makes the checker accept histories it should reject.

Which exception means which depends on what the test does to the cluster, so
the workload asks an :data:`ExceptionPolicy`.  A policy returns ``None`` for
anything it does not recognise, and the workload then fails the test: an
unexpected exception is a finding, not noise.

Tests take the default, compose the built-ins, or write their own::

    RegisterWorkload(ks=ks)                                  # DDL only
    RegisterWorkload(ks=ks, exception_policy=first_match(    # also kills nodes
        tolerate_resets, tolerate_unavailability))
    RegisterWorkload(ks=ks, exception_policy=strict)         # nothing may fail
    RegisterWorkload(ks=ks, exception_policy=first_match(    # also restarts nodes
        tolerate_resets, only_during("restart", tolerate_reboot)))

What an error says about the operation does not depend on when it happened, and
:func:`tolerate_reboot` does not ask.  Whether the cluster may raise it at that
moment does: the same NoHostAvailable is expected while a node is down and a
finding while the cluster is whole.  :func:`only_during` makes that second
claim, for the windows the test opened with
:meth:`~test.cluster.strong_consistency.register_workload.RegisterWorkload.disruption_window`.
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
from cassandra.protocol import InvalidRequest, ServerError

if TYPE_CHECKING:
    from test.cluster.strong_consistency.register_workload import RegisterWorkload


class Outcome(StrEnum):
    """Status recorded for an operation in the Porcupine history.

    These strings are the checker's vocabulary, not ours: scylladb/porcupine_validator
    accepts exactly "ok", "fail" and "unknown".

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
    op: str  # "read", "write" or "cas"
    client_id: int
    key: int
    t_call_ns: int
    t_return_ns: int

    @property
    def is_write(self) -> bool:
        return self.op in ("write", "cas")


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


def is_table_absence_error(exc: BaseException) -> bool:
    """Return True if the exception indicates the table is absent (dropped/recreated).

    Two kinds of them are a SERVER_ERROR.  "Can't find a column family" is
    one: a prepared statement still tagged with the dropped table's UUID,
    which is a legitimate thing for a client to send after a DROP+RECREATE
    (TODO(SCYLLADB-4655): drop it once the server reports an unknown table id
    as a regular InvalidRequest).  The other is a forwarded request whose
    statement the target has to prepare again: the target's "unconfigured
    table" or "keyspace does not exist" comes back through the prepare RPC as
    a remote error, and the forwarding node returns it as a SERVER_ERROR whose
    message is the exception's type and its message.
    """
    if isinstance(exc, InvalidRequest):
        return is_table_absence_invalid_request(exc)
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


# The node that executes the request found too few live replicas and refused it
# before doing anything, so nothing can have been applied -- whatever the
# operation was.  That node is not necessarily the one the client sent it to:
# the request may have been forwarded or redirected on the way, and the error
# passed back unchanged.
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


def tolerate_resets(
    exc: BaseException, ctx: FailureContext,
) -> Optional[Outcome]:
    """Accept "the table is not there" while a reset is in flight.

    A DROP+CREATE makes the table briefly absent, and readers/writers racing
    with it get one of the errors :func:`is_table_absence_error` recognises.
    The operation definitely did not happen —
    there was no table to apply it to — so it is a clean ``fail``.

    The same error *outside* a reset window is not tolerated: that is the bug
    this kind of test is looking for.
    """
    if is_table_absence_error(exc) and ctx.workload.overlaps_reset(
        ctx.t_call_ns, ctx.t_return_ns
    ):
        return Outcome.FAIL
    return None


def tolerate_unavailability(
    exc: BaseException, ctx: FailureContext,
) -> Optional[Outcome]:
    """Accept the failures a briefly unavailable cluster causes.

    Use this for disruptors that can take replicas away — node kills,
    restarts, network partitions.

    The classification follows what the driver's own exceptions promise:

    ==========================  =========  ===============================
    exception                   status     why
    ==========================  =========  ===============================
    Unavailable                 fail       too few live replicas, refused
                                           before anything ran
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


def only_during(name: str, policy: ExceptionPolicy) -> ExceptionPolicy:
    """Ask ``policy`` only about operations that overlapped a window ``name``.

    Anything else is not recognised, and the workload fails the test: the error
    the disruption excuses is a finding outside it.  So the window must stay
    open until the cluster looks whole to the *driver*, not just until the
    node is up: the driver reconnects its pool to a restarted node on its own
    schedule, so wait for it (``wait_for_cql_and_get_hosts``) inside the window.
    """

    def guarded(exc: BaseException, ctx: FailureContext) -> Optional[Outcome]:
        if not ctx.workload.overlaps_window(name, ctx.t_call_ns, ctx.t_return_ns):
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

    Relies on the driver's retries being off, which the workload guarantees
    (see :meth:`RegisterWorkload.prepare`): each recorded operation is one
    attempt at one node, and its error says what became of it.

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
    anything else                   as tolerate_unavailability
    ==============================  =========  ============================
    """
    if isinstance(exc, NoHostAvailable) or (
            isinstance(exc, ServerError) and "unknown verb" in str(exc)):
        return Outcome.FAIL
    if isinstance(exc, ConnectionException) or (
            isinstance(exc, ServerError) and OUTCOME_UNKNOWN in str(exc)):
        return Outcome.UNKNOWN if ctx.is_write else Outcome.FAIL
    return tolerate_unavailability(exc, ctx)
