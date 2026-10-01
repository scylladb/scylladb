#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Pins down when a disruption's errors are excused.

What an error says about an operation does not depend on when it happened;
whether it is expected does, and ``only_during`` is what decides it, by the
windows a test opened.  Getting the edge wrong one way fails a test on a
request that was in flight when a node died; the other way it accepts an error
the cluster raised while it was whole -- the bug such a test is looking for.

These tests need neither a cluster nor the checker.
"""

from __future__ import annotations

import pytest
from cassandra.connection import ConnectionShutdown
from cassandra.policies import FallthroughRetryPolicy
from cassandra.protocol import ServerError

from test.cluster.strong_consistency import workload as workload_module
from test.cluster.strong_consistency.outcomes import (
    FailureContext,
    Outcome,
    is_table_absence_error,
    only_during,
    tolerate_reboot,
    tolerate_reset_windows,
)
from test.cluster.strong_consistency.workload import RESET, DisruptionWindow, RegisterWorkload


class Clock:
    def __init__(self) -> None:
        self.now = 0

    def __call__(self) -> int:
        return self.now


@pytest.fixture
def clock(monkeypatch) -> Clock:
    clock = Clock()
    monkeypatch.setattr(workload_module.time, "monotonic_ns", clock)
    return clock


def _workload() -> RegisterWorkload:
    return RegisterWorkload(ks="unused", retry_policy=FallthroughRetryPolicy())


def _ctx(workload: RegisterWorkload, t_call_ns: int, t_return_ns: int, op: str = "write") -> FailureContext:
    return FailureContext(workload=workload, op=op, client_id=0, key=0,
                          t_call_ns=t_call_ns, t_return_ns=t_return_ns)


async def _window(workload: RegisterWorkload, clock: Clock, name: str, start: int, end: int) -> DisruptionWindow:
    clock.now = start
    async with workload.disruption_window(name) as window:
        clock.now = end
    return window


EXC = ConnectionShutdown("connection died")


async def test_a_window_excuses_what_overlaps_it(clock):
    workload = _workload()
    await _window(workload, clock, "restart", 10, 20)
    policy = only_during("restart", tolerate_reboot)

    assert policy(EXC, _ctx(workload, 5, 10)) is Outcome.UNKNOWN    # returned as it opened
    assert policy(EXC, _ctx(workload, 12, 18)) is Outcome.UNKNOWN   # inside
    assert policy(EXC, _ctx(workload, 19, 40)) is Outcome.UNKNOWN   # sent in it, failed after it closed
    assert policy(EXC, _ctx(workload, 1, 9)) is None                # before
    assert policy(EXC, _ctx(workload, 21, 30)) is None              # after


async def test_grace_pushes_only_the_end(clock):
    workload = _workload()
    await _window(workload, clock, "restart", 1_000_000_000, 2_000_000_000)
    policy = only_during("restart", tolerate_reboot, grace_s=0.5)

    assert policy(EXC, _ctx(workload, 2_400_000_000, 2_500_000_000)) is Outcome.UNKNOWN
    assert policy(EXC, _ctx(workload, 2_500_000_001, 2_600_000_000)) is None
    assert policy(EXC, _ctx(workload, 900_000_000, 999_999_999)) is None   # the start does not move


async def test_an_open_window_covers_everything_that_returns_after_it_opened(clock):
    workload = _workload()
    policy = only_during("restart", tolerate_reboot)

    clock.now = 10
    async with workload.disruption_window("restart") as window:
        assert window.end_ns is None
        assert policy(EXC, _ctx(workload, 1, 9)) is None
        assert policy(EXC, _ctx(workload, 5, 1000)) is Outcome.UNKNOWN
        clock.now = 20
    assert window.span_ns == (10, 20)


async def test_windows_nest_and_each_closes_its_own(clock):
    """A kill inside another disruption: the inner window must not end the
    outer one, and a window of another name excuses nothing for this one."""
    workload = _workload()
    policy = only_during("restart", tolerate_reboot)

    clock.now = 10
    async with workload.disruption_window("down") as outer:
        clock.now = 20
        async with workload.disruption_window("restart") as inner:
            clock.now = 30
        clock.now = 40
    clock.now = 50

    assert outer.span_ns == (10, 40)
    assert inner.span_ns == (20, 30)
    assert policy(EXC, _ctx(workload, 25, 26)) is Outcome.UNKNOWN
    assert policy(EXC, _ctx(workload, 35, 36)) is None      # only the "down" window
    assert workload.overlaps_window("down", 35, 36)


async def test_a_window_closes_when_its_body_raises(clock):
    workload = _workload()
    clock.now = 10
    with pytest.raises(RuntimeError):
        async with workload.disruption_window("restart"):
            clock.now = 20
            raise RuntimeError("the disruptor failed")
    assert [w.span_ns for w in workload.windows("restart")] == [(10, 20)]


def test_the_span_of_an_open_window_is_refused():
    """An open window has no end yet: counting it would silently mean "up to
    now", which is not what a test asking about the whole outage wants."""
    with pytest.raises(AssertionError, match="still open"):
        DisruptionWindow("restart", 10).span_ns


async def test_a_reset_is_a_window_too(clock):
    workload = RegisterWorkload(ks="unused", num_keys=2)
    clock.now = 10
    async with workload.reset_window():
        clock.now = 20

    assert [w.span_ns for w in workload.windows(RESET)] == [(10, 20)]
    assert workload.overlaps_reset(15, 16) and not workload.overlaps_reset(21, 22)
    assert workload.reset_count == 1


def test_a_stale_table_id_arrives_bare_without_retries():
    """With FallthroughRetryPolicy the server's "can't find a column family"
    is not wrapped in NoHostAvailable; inside a reset it is still a clean fail."""
    exc = ServerError(ServerError.error_code, "Can't find a column family with UUID 1234", {})
    assert is_table_absence_error(exc)
    assert not is_table_absence_error(ServerError(ServerError.error_code, "something else", {}))


async def test_a_bare_stale_table_id_is_excused_only_in_a_reset(clock):
    workload = _workload()
    clock.now = 10
    async with workload.reset_window():
        clock.now = 20
    exc = ServerError(ServerError.error_code, "Can't find a column family with UUID 1234", {})

    assert tolerate_reset_windows(exc, _ctx(workload, 12, 18)) is Outcome.FAIL
    assert tolerate_reset_windows(exc, _ctx(workload, 30, 40)) is None
