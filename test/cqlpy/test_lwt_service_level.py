# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Test that LWT (Paxos) reads are admitted under the caller's service
# level, not always under the default one. Each service level has its own
# reader_concurrency_semaphore, exposed as the metric
# scylla_database_total_reads{class="sl:<name>"}. We attach a service level
# to a role, run LWTs as that role, and check the reads land on that
# semaphore - not on sl:default.
#
# Reference: SCYLLADB-2379.
#############################################################################

import re
import time

import requests

from .util import new_user, new_session, new_test_table
from .test_service_levels import new_service_level

from cassandra.concurrent import execute_concurrent_with_args


_METRIC_RE = re.compile(r'^scylla_database_total_reads\{([^}]*)\}\s+([0-9.eE+-]+)$')
_LABEL_RE = re.compile(r'(\w+)="([^"]*)"')

def total_reads_by_class(host, sl_classes):
    # Scrape the metrics endpoint once and sum scylla_database_total_reads,
    # across all shards, for each requested semaphore class label (e.g.
    # "sl:default" or "sl:sl_<name>"). Returns a dict class -> total reads.
    resp = requests.get(f"http://{host}:9180/metrics", timeout=30)
    resp.raise_for_status()
    totals = {c: 0.0 for c in sl_classes}
    for line in resp.text.splitlines():
        m = _METRIC_RE.match(line)
        if not m:
            continue
        cls = dict(_LABEL_RE.findall(m.group(1))).get("class")
        if cls in totals:
            totals[cls] += float(m.group(2))
    return totals


def test_lwt_reads_use_attached_service_level(scylla_only, cql, host, test_keyspace_vnodes):
    # A superuser role is used so it has permission to access the table; service
    # level attachment is independent of superuser status.
    with new_user(cql, with_superuser_privileges=True) as user, \
         new_service_level(cql, shares=200, role=user) as sl, \
         new_test_table(cql, test_keyspace_vnodes,
                        "p int, c int, v int, PRIMARY KEY (p, c)") as table:
        sl_class = f"sl:{sl}"
        # A connection's service level is resolved once, at login, from a
        # cache populated asynchronously after ATTACH SERVICE LEVEL. So we
        # keep opening fresh sessions until one lands on the SL's semaphore.
        # Each attempt runs the real LWT batch: if the SL semaphore accounted
        # for the reads, the session is on the right group and we assert; if
        # not (still on sl:default), we reconnect and retry.
        insert = f"INSERT INTO {table} (p, c, v) VALUES (?, 0, 0) IF NOT EXISTS"
        # Run a handful of LWTs per attempt - enough that the reads on the
        # SL's semaphore clearly dominate any unrelated background reads on
        # sl:default during the measurement window, so the "no leak to
        # sl:default" check below is not racy. This is a correctness test,
        # not a load test, so the count is kept small.
        N = 20
        deadline = time.time() + 60
        partition = 0

        while True:
            with new_session(cql, user) as session:
                stmt = session.prepare(insert)
                params = [(p,) for p in range(partition, partition + N)]
                partition += N

                classes = [sl_class, "sl:default"]
                before = total_reads_by_class(host, classes)
                execute_concurrent_with_args(session, stmt, params)
                after = total_reads_by_class(host, classes)
                sl_delta = after[sl_class] - before[sl_class]
                default_delta = after["sl:default"] - before["sl:default"]

            if sl_delta >= N:
                # The session is on the SL's semaphore. Every LWT performs at
                # least one internal read (the Paxos read-round optimization
                # read), so the SL semaphore accounted for >= N reads, and
                # those reads must not have leaked to sl:default.
                assert default_delta < sl_delta, (
                    f"LWT reads leaked to sl:default: sl:default delta "
                    f"{default_delta} vs {sl_class} delta {sl_delta}")
                return

            assert time.time() < deadline, (
                f"service level was never applied to a new session "
                f"(semaphore {sl_class} saw {sl_delta} reads)")
            time.sleep(0.5)
