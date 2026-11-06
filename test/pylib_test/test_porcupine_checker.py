#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Pin the anomalies the Porcupine checker must reject, each against a valid twin.

Every SC linearizability test trusts the checker's verdict, so these tests feed it
hand-written histories, no cluster involved.  Each invalid history has a twin that
differs only in the bad operation and must be accepted: a checker, or a container
runtime, that rejected everything would otherwise pass them all.
"""

import json

import pytest

from test.cluster.tools.porcupine import run_porcupine_checker


# (op, key, value, call time, return time); no return time means still in flight.
type Op = tuple[str, int, int, int, int | None]


def write(key: int, value: int, start: int, end: int | None = None) -> Op:
    """A write of `value` acknowledged at `end`, or never answered if `end` is None."""

    return ("write", key, value, start, end)


def read(key: int, value: int, start: int, end: int) -> Op:
    """A read that returned `value`."""

    return ("read", key, value, start, end)


def history_jsonl(ops: list[Op]) -> str:
    """Encode `ops` as the SC stress tests record them, one client per operation."""

    events = []
    for op_id, (op, key, value, start, end) in enumerate(ops):
        events.append({"id": op_id, "client_id": op_id, "kind": "call", "op": op, "key": key,
                       "value": value if op == "write" else 0, "time_ns": start})
        if end is not None:
            events.append({"id": op_id, "client_id": op_id, "kind": "return", "op": op, "key": key,
                           "value": value, "time_ns": end, "status": "ok"})

    # The checker orders events with an unstable sort by time, so equal times would make the verdict arbitrary.
    times = [event["time_ns"] for event in events]
    assert len(set(times)) == len(times), f"event times must be distinct: {sorted(times)}"
    return "".join(json.dumps(event) + "\n" for event in events)


async def check(ops: list[Op]) -> dict:
    """The checker's verdict on `ops`, without the visualization path."""

    result = await run_porcupine_checker(history_jsonl(ops))
    return {field: result.get(field) for field in ("valid", "keys_checked", "total_ops", "error")}


def verdict(ops: list[Op], violation_on_key: int | None = None) -> dict:
    """The verdict expected for `ops`, with the counts taken from `ops` rather than from the checker."""

    return {
        "valid": violation_on_key is None,
        "keys_checked": len({key for _, key, *_ in ops}),
        "total_ops": len(ops),
        "error": None if violation_on_key is None else f"linearizability violation detected on key {violation_on_key}",
    }


def anomaly(ops: list[Op], bad: int, fixed: Op) -> tuple[list[Op], list[Op], int]:
    """`ops`, its twin with `ops[bad]` replaced by `fixed`, and the key of the bad operation."""

    return ops, ops[:bad] + [fixed] + ops[bad + 1:], ops[bad][1]


ANOMALIES = {
    # W1 and W2 both completed before R started, in that order, so R must see 2.
    "stale_read": anomaly([write(0, 1, 1, 2), write(0, 2, 3, 4), read(0, 1, 5, 6)], 2, read(0, 2, 5, 6)),

    # The only write completed before R started, so R must see 5, not the initial 0.
    "lost_write": anomaly([write(0, 5, 1, 2), read(0, 0, 3, 4)], 1, read(0, 5, 3, 4)),

    # Nobody wrote 7.
    "never_written_value": anomaly([write(0, 5, 1, 2), read(0, 7, 3, 4)], 1, read(0, 5, 3, 4)),

    # W5 may take effect at any point after its call, but R1 seeing 5 places it after W7, so R2 must see 5 too.
    "pending_write_does_not_legalize_everything": anomaly(
        [write(0, 5, 1), write(0, 7, 2, 3), read(0, 5, 4, 5), read(0, 7, 6, 7)], 3, read(0, 5, 6, 7)),

    # Keys are checked independently: nine sound keys don't hide a lost write on key 7.
    "one_bad_key_among_many": anomaly(
        [op for key in range(10)
         for op in (write(key, key + 1, 10 * key + 1, 10 * key + 2), read(key, 0 if key == 7 else key + 1, 10 * key + 3, 10 * key + 4))],
        15, read(7, 8, 73, 74)),
}


@pytest.mark.parametrize("bad, twin, bad_key", list(ANOMALIES.values()), ids=list(ANOMALIES))
async def test_checker_rejects_anomaly(bad: list[Op], twin: list[Op], bad_key: int) -> None:
    """The anomaly is reported as a violation on its key, while the same history without it is accepted."""

    assert await check(twin) == verdict(twin), "the checker rejects a legal history, so none of its verdicts can be trusted"
    assert await check(bad) == verdict(bad, violation_on_key=bad_key)


@pytest.mark.parametrize("observed", [1, 2])
async def test_read_concurrent_with_write_may_see_either_value(observed: int) -> None:
    """A read overlapping W2 may take effect before or after it."""

    ops = [write(0, 1, 1, 2), write(0, 2, 3, 6), read(0, observed, 4, 5)]
    assert await check(ops) == verdict(ops)
