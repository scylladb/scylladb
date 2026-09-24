#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Pins down how a run's statistics are read back out of its history.

The numbers a stress test asserts on -- "at least 100 writes succeeded" -- are
not counted as the clients work.  They are derived afterwards from the recorded
history, so that they describe the file the checker was given rather than a
tally kept beside it.  That makes the derivation itself worth testing: a bug in
it does not crash anything, it makes ``assert_progress`` describe a run that
did not happen, and no stress run on a cluster would show it.

These tests need neither a cluster nor the checker: they drive
:class:`HistoryRecorder` the way the client tasks drive it and read the result
back.
"""

from __future__ import annotations

import pytest

from test.cluster.strong_consistency.outcomes import Outcome
from test.cluster.strong_consistency.workload import (
    HistoryRecorder,
    RegisterWorkload,
    Stats,
)

WRITER, READER, RESET = 0, 1, 9


def _op(history: HistoryRecorder, client_id: int, op: str, key: int,
        value: int = 0, status: str | None = None) -> int:
    """One operation; ``status=None`` leaves it without a return."""
    op_id, _ = history.record_call(client_id, op, key, value)
    if status is not None:
        history.record_return(op_id, client_id, op, key, value, status)
    return op_id


def test_every_outcome_lands_in_its_own_bucket():
    """All seven counts, one operation each, read back separately."""
    h = HistoryRecorder()
    _op(h, WRITER, "write", 1, 5, Outcome.OK)
    _op(h, WRITER, "write", 1, 6, Outcome.FAIL)
    _op(h, WRITER, "write", 1, 7)                    # no return: indeterminate
    _op(h, READER, "read", 1, 5, Outcome.OK)
    _op(h, READER, "read", 2, 0, Outcome.OK)         # value 0: nothing written
    _op(h, READER, "read", 2, 0, Outcome.FAIL)
    _op(h, READER, "read", 3)                        # no return: indeterminate

    stats = h.stats(reset_client_id=RESET)

    assert stats == Stats(
        write_ok=1, write_fail=1, write_indeterminate=1,
        read_ok=1, read_empty=1, read_fail=1, read_indeterminate=1,
    )
    assert stats.reads == 2       # a read of the default value still answered
    assert stats.pending == 2     # one write and one read left open


def test_a_read_of_zero_is_a_read_of_nothing():
    """0 in the history means "never written", and that is what makes the
    derivation possible: the value alone says whether a read found anything.

    It holds only because :meth:`RegisterWorkload.next_write_value` hands out
    values from 1 upwards.  A workload that ever wrote a 0 would silently move
    those reads into ``read_empty``.
    """
    h = HistoryRecorder()
    _op(h, READER, "read", 1, 0, Outcome.OK)
    _op(h, READER, "read", 1, 1, Outcome.OK)

    assert h.stats(reset_client_id=RESET) == Stats(read_ok=1, read_empty=1)


def test_a_reset_is_not_work_a_client_did():
    """The synthetic write(0) of a DROP+RECREATE must not count as progress.

    One reset of a 100-key table puts 100 acknowledged writes into the history.
    Counting them would let ``assert_progress(min_writes=100)`` pass a run in
    which every real write failed.
    """
    workload = RegisterWorkload(ks="unused", num_keys=100)
    workload.history.record_reset(
        workload.reset_client_id, range(workload.num_keys), 1, 2)

    assert workload.stats() == Stats()
    with pytest.raises(AssertionError, match="Too few successful writes"):
        workload.assert_progress(min_writes=1)


def test_stats_narrow_to_one_client():
    """Each task logs its own line, from the same history as the summary."""
    h = HistoryRecorder()
    _op(h, WRITER, "write", 1, 5, Outcome.OK)
    _op(h, READER, "read", 1, 5, Outcome.OK)

    assert h.stats(reset_client_id=RESET, client_id=WRITER) == Stats(write_ok=1)
    assert h.stats(reset_client_id=RESET, client_id=READER) == Stats(read_ok=1)
    assert str(h.stats(reset_client_id=RESET, client_id=WRITER)) == (
        "writes ok=1 fail=0 indeterminate=0")
