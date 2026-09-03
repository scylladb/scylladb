#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""
Reproducer for SCYLLADB-3432: every failed direct failure detector ping
produced an unthrottled "unexpected exception when pinging" warning.

Pings towards a dead or unreachable node keep timing out, one every few hundred
milliseconds. Each timeout used to surface through the catch-all in the ping
fiber and be logged at WARN with no rate limiting, which floods the journal for
the whole duration of an outage (hundreds of lines per second were observed on a
large cluster) and buries the useful transition messages. A timed-out ping to a
dead node is expected, so it now belongs at DEBUG; the node-down transition
itself is still reported at INFO.
"""

import logging

import pytest

from test.pylib.manager_client import ManagerClient

logger = logging.getLogger(__name__)

# The unthrottled catch-all warning, which a ping timeout must no longer reach.
PING_SPAM = "unexpected exception when pinging"

# How many failed pings to observe before concluding that they keep failing.
# A ping to the paused node fails only once its timeout expires, so one fails
# about every 600 ms (direct_failure_detector_ping_timeout_in_ms).
FAILED_PINGS_TO_OBSERVE = 5

# Makes the expected DEBUG report of a timed-out ping observable, so the test can
# tell "the pings failed and were logged quietly" from "no ping failed at all".
DEBUG_PING_LOGGING = ["--logger-log-level", "raft_group_registry=debug"]


@pytest.mark.asyncio
async def test_failed_fd_pings_do_not_spam_warnings(manager: ManagerClient) -> None:
    """Verify that a failed direct FD ping is not logged at WARN level.

    Reproduces SCYLLADB-3432: pings towards an unresponsive node keep timing out,
    and each timeout used to produce an unthrottled WARN through the catch-all in
    the ping fiber. Requires that the node-down transition is still reported, that
    the pings really did keep timing out, and that none of them produced a warning.
    """
    servers = await manager.servers_add(3, cmdline=DEBUG_PING_LOGGING)
    observer, victim = servers[0], servers[2]
    victim_host_id = await manager.get_host_id(victim.server_id)
    observer_log = await manager.server_open_log(server_id=observer.server_id)
    mark = await observer_log.mark()

    # SIGSTOP the victim: its kernel still accepts TCP, but the process never
    # answers, so pings from the observer time out instead of being refused,
    # which is the same failure mode as a dead host in the field.
    await manager.server_pause(victim.server_id)
    try:
        # The down transition is the intended signal and must still be reported.
        down_mark, _ = await observer_log.wait_for(
            "marking Raft server .* as dead for raft groups",
            from_mark=mark,
            timeout=120,
        )

        # Wait for the pings to keep failing rather than sleeping for a fixed
        # time. This is also what stops the test from passing vacuously: if the
        # dead node were no longer pinged at all, no warning could be logged and
        # the check below would succeed while verifying nothing.
        ping_timed_out = rf"ping\(id = {victim_host_id}\): timed out"
        ping_mark, observed = down_mark, 0
        try:
            for _ in range(FAILED_PINGS_TO_OBSERVE):
                ping_mark, _ = await observer_log.wait_for(
                    ping_timed_out, from_mark=ping_mark, timeout=30)
                observed += 1
        except TimeoutError:
            # Fall through: if the warnings are back, that is the regression this
            # test is about, and it explains the failure better than the count.
            pass

        spam = await observer_log.grep(PING_SPAM, from_mark=mark)
        assert not spam, (
            f"{len(spam)} unthrottled per-ping warnings were logged for a node "
            f"that is simply down (SCYLLADB-3432); expected none")

        assert observed == FAILED_PINGS_TO_OBSERVE, (
            f"only {observed} of {FAILED_PINGS_TO_OBSERVE} expected ping timeouts "
            f"were reported for the paused node; expected the pings to keep failing")
    finally:
        await manager.server_unpause(victim.server_id)
