#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Linearizability of a strongly consistent table on a healthy cluster.

This is the baseline for the disrupted runs: the register workload with no
disruptor.  Its recorded history then gets one injected read at a time, and the
checker must reject the impossible ones and accept their legal twin, so a green
baseline cannot come from a history that never reached the checker.
"""

import copy
import dataclasses
import logging
from pathlib import Path

import pytest
from cassandra import ConsistencyLevel
from cassandra.policies import FallthroughRetryPolicy

from test.cluster.strong_consistency.config import boot_sc_cluster, sc_keyspace_opts
from test.cluster.strong_consistency.outcomes import Outcome, tolerate_timeouts
from test.cluster.strong_consistency.workload import RegisterWorkload, check_linearizable, run_workload
from test.cluster.util import new_test_keyspace
from test.pylib.scylla_cluster_manager import ScyllaClusterManager


logger = logging.getLogger(__name__)

DURATION_S = 30
MIN_WRITES = 100
MIN_READS = 100


def with_read(workload: RegisterWorkload, key: int, value: int) -> RegisterWorkload:
    """A copy of `workload` whose history ends with a read of `key` that returned `value`."""

    copied = dataclasses.replace(workload, history=copy.deepcopy(workload.history))
    client_id = workload.reset_client_id + 1
    op_id, _ = copied.history.record_call(client_id, "read", key)
    copied.history.record_return(op_id, client_id, "read", key, value, Outcome.OK)
    return copied


@pytest.mark.tier2
async def test_sc_linearizability_baseline(manager: ScyllaClusterManager, tmp_path: Path) -> None:
    """Concurrent readers and writers on a healthy cluster leave a linearizable history."""

    _, cql = await boot_sc_cluster(manager)

    async with new_test_keyspace(manager, sc_keyspace_opts(replication_factor=3, initial_tablets=10)) as ks:
        # One attempt per recorded operation, so a timed-out read is never re-sent as a relaxed one (SCYLLADB-4756).
        workload = RegisterWorkload(ks=ks, retry_policy=FallthroughRetryPolicy(), exception_policy=tolerate_timeouts)
        await cql.run_async(f"CREATE TABLE {workload.fqtn} (pk int PRIMARY KEY, c int)")
        workload.prepare(cql)
        for stmt in (workload.write_stmt, workload.read_stmt):
            stmt.consistency_level = ConsistencyLevel.QUORUM

        errors = await run_workload(workload, cql, DURATION_S)
        logger.info("Stats: %s", workload.stats_line())
        assert not errors, f"Task(s) failed (seed={workload.seed}): {errors}"
        workload.assert_progress(min_writes=MIN_WRITES, min_reads=MIN_READS)

        result = await check_linearizable(workload, output_dir=tmp_path / "baseline")
        assert result["total_ops"] == workload.history.op_count, f"the checker did not see the whole history: {result}"

        # One more acknowledged write on key 0, with the load stopped, for the injected reads to contradict.
        client_id = workload.reset_client_id + 1
        value = workload.next_write_value()
        op_id, _ = workload.history.record_call(client_id, "write", 0, value)
        await cql.run_async(workload.write_stmt.bind([value, 0]))
        workload.history.record_return(op_id, client_id, "write", 0, value, Outcome.OK)

        # Nothing was written to key 0 after that write returned, and a write still in flight from the run may land before or after the read.
        await check_linearizable(with_read(workload, 0, value), output_dir=tmp_path / f"read_{value}")

        # 0 is never written without a reset and the writers write values from 1 up, so neither read can follow the acknowledged write.
        for impossible in (0, -1):
            with pytest.raises(AssertionError, match=r"violation detected on key 0\n"):
                await check_linearizable(with_read(workload, 0, impossible), output_dir=tmp_path / f"read_{impossible}")
