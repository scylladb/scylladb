#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Pins down what a lost node's errors mean, and what makes them mean it.

The outcome of such an error does not depend on when it happened, so
``tolerate_reboot`` is tested without a clock.  It is only sound with one
attempt per recorded operation, which ``retry_policy`` has to keep true across
every prepare(), and which ``tolerate_reboot`` refuses to go without.

These tests need neither a cluster nor the checker.
"""

from __future__ import annotations

import pytest
from cassandra import WriteTimeout
from cassandra.cluster import NoHostAvailable
from cassandra.connection import ConnectionShutdown
from cassandra.policies import FallthroughRetryPolicy
from cassandra.protocol import ServerError

from test.cluster.strong_consistency.outcomes import (
    OUTCOME_UNKNOWN,
    FailureContext,
    Outcome,
    tolerate_reboot,
)
from test.cluster.strong_consistency.workload import RegisterWorkload


def _workload() -> RegisterWorkload:
    return RegisterWorkload(ks="unused")


def _ctx(workload: RegisterWorkload, t_call_ns: int = 0, t_return_ns: int = 0, op: str = "write") -> FailureContext:
    return FailureContext(workload=workload, op=op, client_id=0, key=0,
                          t_call_ns=t_call_ns, t_return_ns=t_return_ns)


def _server_error(message: str) -> ServerError:
    """A SERVER_ERROR as the driver raises it: a protocol message, not a plain exception."""
    return ServerError(ServerError.error_code, message, {})


def test_what_a_reboot_error_means():
    workload = _workload()
    write, read = _ctx(workload, op="write"), _ctx(workload, op="read")

    assert tolerate_reboot(NoHostAvailable("no host", {}), write) is Outcome.FAIL
    assert tolerate_reboot(_server_error("unknown verb"), write) is Outcome.FAIL
    assert tolerate_reboot(ConnectionShutdown("connection died"), write) is Outcome.UNKNOWN
    assert tolerate_reboot(ConnectionShutdown("connection died"), read) is Outcome.FAIL
    assert tolerate_reboot(_server_error(OUTCOME_UNKNOWN), write) is Outcome.UNKNOWN
    assert tolerate_reboot(_server_error(OUTCOME_UNKNOWN), read) is Outcome.FAIL
    assert tolerate_reboot(WriteTimeout("timed out", write_type=0), write) is Outcome.UNKNOWN  # 0: SIMPLE
    assert tolerate_reboot(_server_error("some other server error"), write) is None
    assert tolerate_reboot(ValueError("not a driver error"), write) is None


def test_a_reboot_error_is_not_read_with_the_driver_retrying():
    """With the driver's retries on, NoHostAvailable can follow an attempt that was
    applied; classifying it as "never sent" would fabricate a violation."""
    with pytest.raises(AssertionError, match="one attempt per operation"):
        tolerate_reboot(NoHostAvailable("no host", {}), _ctx(RegisterWorkload(ks="unused", retry_policy=None)))


class _Statement:
    retry_policy = None


class _Session:
    def prepare(self, query: str) -> _Statement:
        return _Statement()


def test_the_retry_policy_survives_a_prepare_again():
    """prepare() runs again inside every reset and makes new statements: a
    policy the test set on the old ones would be gone."""
    policy = FallthroughRetryPolicy()
    workload = RegisterWorkload(ks="unused", retry_policy=policy)

    for _ in range(2):
        workload.prepare(_Session())
        assert workload.write_stmt.retry_policy is policy
        assert workload.read_stmt.retry_policy is policy

    workload = RegisterWorkload(ks="unused")
    workload.prepare(_Session())
    assert isinstance(workload.write_stmt.retry_policy, FallthroughRetryPolicy)  # the default

    workload = RegisterWorkload(ks="unused", retry_policy=None)
    workload.prepare(_Session())
    assert workload.write_stmt.retry_policy is None     # the driver's own
