#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
from datetime import datetime

from attr import define


@define
class HostInfo:
    host_id: str
    cpu_model: str
    cpu_cores: int
    ram_bytes: int


@define
class CgroupMetric:
    memory: int
    test_id: int
    host_id: str
    timestamp: datetime


@define
class Metric:
    test_id: int
    host_id: str
    memory_peak: int = None
    success: bool = None
    status: str = None
    system_sec: float = None
    time_end: datetime = None
    time_start: datetime = None
    time_taken: float = None
    usage_sec: float = None
    user_sec: float = None
    worker_id: str = None


@define
class SystemResourceMetric:
    host_id: str
    cpu: float
    memory_free: int
    memory_available: int
    memory_used: int
    memory_active: int
    memory_inactive: int
    memory_buffers: int
    timestamp: datetime


@define
class Test:
    host_id: str
    architecture: str
    path: str
    file: str
    mode: str
    run_id: int
    test_name: str


@define
class ClusterMetric:
    """Per-test cluster capacity: what the test's cluster actually ran.

    `nodeid` is here alongside the `tests` row it points at because it is the
    key the backfill script needs -- it names the class and the parameters of
    the source function to mark, which `tests.test_name` does not carry.
    """
    test_id: int
    host_id: str
    nodeid: str
    max_running_shards: int
    # How the test ended, the same value test_metrics gets.  Here as well
    # because tests.id is shared by every run of a test with the same name,
    # mode and run id, so a join on it cannot tell one run's rows from
    # another's -- and only the run that finished measured the whole test.
    status: str
    # The claim in force while this was measured, or None if none was.  Without
    # it a row is not interpretable: a claim caps the peak (see
    # RunningShards.reserve), so `max_running_shards` from an enforced run is
    # the smaller of what the test used and what it was allowed.  Only a row
    # with no claim reports what the test would use unrestricted.
    claim: int | None
