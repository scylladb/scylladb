#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""Tests for test.cluster.util.wait_new_coordinator_elected.

The helper decides, from 'Starting new topology coordinator' entries in
system.group0_history, whether the topology coordinator has moved to another
node.  A node which loses and immediately regains raft leadership re-elects
itself, so the two newest entries of the history may legitimately hold the same
host id.  A predicate comparing those two entries with each other is then
permanently false and the wait can only time out (SCYLLADB-3852).

The history below is the one observed in scylla-ci build 32176, newest first.
"""

import time

import pytest

from test.cluster.util import wait_new_coordinator_elected


# Host ids of build 32176, newest election first.  2f5d7290 lost leadership to
# a timed-out barrier_and_drain and regained it 6ms later, so it appears twice
# in a row at the head of the history.
REELECTED = "2f5d7290-0000-4000-8000-000000000000"
PREVIOUS = "89d5427e-0000-4000-8000-000000000000"
OLDER = ["8b8f235b-0000-4000-8000-000000000000",
         "2f5d7290-0000-4000-8000-000000000000",
         "ef8e5e01-0000-4000-8000-000000000000"]

DUPLICATED_HEAD = [REELECTED, REELECTED, PREVIOUS] + OLDER


class StubCql:
    """Returns a fixed group0_history, newest first."""

    def __init__(self, coordinator_ids: list[str]) -> None:
        self._rows = [
            _Row(f"Starting new topology coordinator {host_id}")
            for host_id in coordinator_ids
        ]

    async def run_async(self, stm):
        return self._rows


class _Row:
    def __init__(self, description: str) -> None:
        self.description = description


class StubManager:
    def __init__(self, coordinator_ids: list[str]) -> None:
        self._cql = StubCql(coordinator_ids)

    def get_cql(self) -> StubCql:
        return self._cql


async def test_same_node_reelected_is_not_mistaken_for_no_handover() -> None:
    """The coordinator moved off PREVIOUS, then re-elected itself.

    The handover the caller asked about did happen, so the wait has to return.
    Comparing the two newest entries with each other instead would compare
    REELECTED with REELECTED and never finish.
    """
    manager = StubManager(DUPLICATED_HEAD)

    await wait_new_coordinator_elected(manager=manager,
                                       previous_coordinator_id=PREVIOUS,
                                       deadline=time.time() + 5)


async def test_unchanged_coordinator_times_out() -> None:
    """No handover happened, so the wait must not report one."""
    manager = StubManager([REELECTED, PREVIOUS] + OLDER)

    with pytest.raises(AssertionError, match="timed out"):
        await wait_new_coordinator_elected(manager=manager,
                                           previous_coordinator_id=REELECTED,
                                           deadline=time.time() + 1)
