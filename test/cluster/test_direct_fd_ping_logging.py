#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""
Reproducer for SCYLLADB-3432: every failed direct failure detector ping
produces an unthrottled "unexpected exception when pinging" warning.

Pings towards a dead or unreachable node time out roughly once per second
per node, and each timeout used to be logged at WARN level through the
catch-all in the ping fiber. During a real outage this floods the journal
(hundreds of lines per second on a large cluster) for the whole duration of
the outage. A timed-out ping to a dead node is expected behavior and belongs
at DEBUG level; the node-down transition itself is reported separately
("marking Raft server ... as dead for raft groups", "InetAddress ... is now
DOWN").

The test pauses one node (SIGSTOP), so that pings to it time out instead of
being rejected, waits until the failure detector reports the node dead, lets
failed pings accumulate, and requires that no per-ping warnings were logged.
"""

import asyncio
import logging

import pytest

from test.pylib.manager_client import ManagerClient

logger = logging.getLogger(__name__)

PING_SPAM = "unexpected exception when pinging"


@pytest.mark.xfail(reason="SCYLLADB-3432: every failed direct FD ping is logged at WARN level "
                          "with no rate limiting")
@pytest.mark.skip_mode(mode='release', reason='the test relies on debug-level absence checks '
                                              'that are only meaningful with default log levels')
@pytest.mark.asyncio
async def test_failed_fd_pings_do_not_spam_warnings(manager: ManagerClient) -> None:
    servers = await manager.servers_add(3)
    observer, victim = servers[0], servers[2]
    observer_log = await manager.server_open_log(server_id=observer.server_id)
    mark = await observer_log.mark()

    # SIGSTOP the victim: its kernel still accepts TCP, but the process never
    # answers, so direct FD pings from the observer fail with a timeout —
    # the same failure mode as a dead host in the field.
    await manager.server_pause(victim.server_id)
    try:
        # The down transition is the intended signal and must be reported.
        await observer_log.wait_for(
            "marking Raft server .* as dead for raft groups",
            from_mark=mark,
            timeout=120,
        )

        # Let failed pings accumulate: ~1.3 timed out pings per second on
        # the observer for the paused node.
        await asyncio.sleep(20)

        spam = await observer_log.grep(PING_SPAM, from_mark=mark)
        assert not spam, (
            f"{len(spam)} unthrottled per-ping warnings were logged for a node "
            f"that is simply down (SCYLLADB-3432); expected none")
    finally:
        await manager.server_unpause(victim.server_id)
