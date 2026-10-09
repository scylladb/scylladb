#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Pins down what the Porcupine checker understands.

These tests need no cluster: they feed histories to the checker and assert what
comes back.  All but the last write those histories by hand; the last one has
:class:`HistoryRecorder` produce them, because hand-written events prove
nothing about what a real run sends.

They exist because the checker answers *every* malformed history the same way —
"linearizability violation on key N".  A status string it does not recognise is
mapped to a status its register model has no branch for, and the result is a
fabricated bug in ScyllaDB, complete with a visualization.  That is what
happened with ``"info"``: Jepsen's name for the indeterminate outcome, where
the checker expects ``"unknown"``.

So: every value :class:`Outcome` can produce must be one the checker acts on,
and the whole path history → checker → assertion must still reject a history
that really is broken.  Nothing else in the SC tests proves either of those — a
green run would look identical if the history never reached the checker at all.
"""

from __future__ import annotations

import json

import pytest

from test.cluster.tools.porcupine import run_porcupine_checker
from test.cluster.strong_consistency.outcomes import Outcome
from test.cluster.strong_consistency.workload import RegisterWorkload, check_linearizable

def _history(*events: dict) -> str:
    return "".join(json.dumps(e, separators=(",", ":")) + "\n" for e in events)


def _call(op_id: int, op: str, key: int, value: int, time_ns: int) -> dict:
    return {"id": op_id, "client_id": 0, "kind": "call", "op": op,
            "key": key, "value": value, "time_ns": time_ns}


def _ret(op_id: int, op: str, key: int, value: int, time_ns: int,
         status: str) -> dict:
    return {"id": op_id, "client_id": 0, "kind": "return", "op": op,
            "key": key, "value": value, "time_ns": time_ns, "status": status}


@pytest.mark.parametrize("status", [o.value for o in Outcome])
async def test_every_outcome_is_understood(status):
    """A single write must not be a violation, whatever status we record.

    One write and nothing else is linearizable under any outcome: it either
    happened, did not happen, or may have happened.  If the checker calls this
    a violation, it did not recognise the status string — the vocabularies have
    drifted apart and every test using that status now invents bugs.
    """
    result = await run_porcupine_checker(_history(
        _call(0, "write", 1, 5, 1),
        _ret(0, "write", 1, 5, 2, status),
    ))

    assert result["valid"], (
        f"The checker does not act on status {status!r} (it is not one of "
        f"'ok', 'fail', 'unknown'), so it treated a trivially linearizable "
        f"history as a violation: {result}"
    )


async def test_indeterminate_write_may_have_applied():
    """An 'unknown' write must let a later read see *either* outcome.

    This is the whole point of recording timeouts as indeterminate, so check
    the checker really does carry both possibilities forward rather than just
    ignoring the operation.
    """
    for observed in (0, 5):
        result = await run_porcupine_checker(_history(
            _call(0, "write", 1, 5, 1),
            _ret(0, "write", 1, 5, 2, Outcome.UNKNOWN),
            _call(1, "read", 1, 0, 3),
            _ret(1, "read", 1, observed, 4, Outcome.OK),
        ))
        assert result["valid"], (
            f"read()->{observed} after an indeterminate write(5) must be "
            f"legal, got {result}"
        )


async def test_pending_write_may_land_after_a_later_write():
    """An indeterminate write must stay open to the end of the history.

    W(5) times out, W(7) is then acknowledged, and a read returns 5.  That is
    legal: the client never learned the fate of W(5), the server never learned
    the client gave up, and W(5) may reach the log after W(7).

    Recording a return for W(5) — even with status 'unknown' — bounds its
    window at the instant the client gave up, forces it to linearize before
    W(7), and turns this legal history into a reported violation.  So the
    harness leaves such operations unpaired instead; this pins both halves.
    """
    ops = (
        _call(1, "write", 1, 7, 3),
        _ret(1, "write", 1, 7, 4, Outcome.OK),
        _call(2, "read", 1, 0, 5),
        _ret(2, "read", 1, 5, 6, Outcome.OK),
    )

    pending = await run_porcupine_checker(_history(
        _call(0, "write", 1, 5, 1),          # no return: still in flight
        *ops,
    ))
    assert pending["valid"], (
        f"a pending write must be allowed to land after a later acknowledged "
        f"write, got {pending}"
    )

    bounded = await run_porcupine_checker(_history(
        _call(0, "write", 1, 5, 1),
        _ret(0, "write", 1, 5, 2, Outcome.UNKNOWN),   # what we must NOT do
        *ops,
    ))
    assert not bounded["valid"], (
        "a bounded 'unknown' return no longer constrains the write, so this "
        "test can no longer tell the two encodings apart — check whether the "
        f"checker changed: {bounded}"
    )


async def test_failed_write_did_not_apply():
    """A 'fail' write must be dropped: reading its value afterwards is illegal.

    This is the other half of the contract, and the reason a timeout must never
    be recorded as 'fail': doing so would make a perfectly good database look
    like it returned a value nobody wrote.
    """
    result = await run_porcupine_checker(_history(
        _call(0, "write", 1, 5, 1),
        _ret(0, "write", 1, 5, 2, Outcome.FAIL),
        _call(1, "read", 1, 0, 3),
        _ret(1, "read", 1, 5, 4, Outcome.OK),
    ))

    assert not result["valid"], (
        f"reading 5 after write(5) failed should be a violation, got {result}"
    )


async def test_violation_is_detected(tmp_path):
    """The pipeline must still reject a history that really is broken.

    Guards the opposite failure: a green suite proves nothing if the history
    never reaches the checker, or reaches it empty.
    """
    result = await run_porcupine_checker(
        _history(
            _call(0, "write", 1, 5, 1),
            _ret(0, "write", 1, 5, 2, Outcome.OK),
            _call(1, "read", 1, 0, 3),
            _ret(1, "read", 1, 7, 4, Outcome.OK),  # nobody ever wrote 7
        ),
        output_dir=tmp_path / "porcupine",
    )

    assert not result["valid"], f"an impossible read must be rejected: {result}"
    assert "violation" in str(result.get("error", "")), result


async def test_a_recorded_history_reaches_the_checker(tmp_path):
    """What the workload records must be what the checker reads.

    Every test above writes its events by hand, so all of them would still
    pass if :class:`HistoryRecorder` spelled a field differently and every
    stress run in the suite fed the checker something it could not parse.  This
    one goes the other way round: it drives the recorder the way the writer,
    reader and schema-changer tasks drive it, then ends with the same
    ``check_linearizable()`` a stress test ends with — once on a history that
    is sound, once on the same history plus a read no register could have
    answered.  A green suite means nothing without the second half: a history
    that never arrived would look exactly as healthy.

    No cluster is involved; only the recorder and the checker are.
    """
    workload = RegisterWorkload(ks="unused", num_keys=2)

    def write(client_id: int, key: int, value: int) -> None:
        op_id, _ = workload.history.record_call(client_id, "write", key, value)
        workload.history.record_return(
            op_id, client_id, "write", key, value, Outcome.OK)

    def read(client_id: int, key: int, value: int) -> None:
        op_id, _ = workload.history.record_call(client_id, "read", key)
        workload.history.record_return(
            op_id, client_id, "read", key, value, Outcome.OK)

    for key in range(workload.num_keys):
        value = workload.next_write_value()
        write(0, key, value)
        read(1, key, value)

    # A DROP+RECREATE puts every register back to 0.  A test says so through
    # reset_window(), whose synthetic write(0) per key lands in the same
    # history as everything else — so it belongs in this check too.
    async with workload.reset_window():
        pass

    for key in range(workload.num_keys):
        read(1, key, 0)

    await check_linearizable(workload, output_dir=tmp_path / "sound")

    # Writers hand out values from 1 upwards and a reset writes 0, so -1 is a
    # value nothing here ever wrote, whatever order the rest linearizes in.
    read(1, 0, -1)

    with pytest.raises(AssertionError, match="Linearizability violation"):
        await check_linearizable(workload, output_dir=tmp_path / "broken")
