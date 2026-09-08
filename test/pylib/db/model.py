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
class SchedulerRun:
    """Which scheduler ran a session, and the run configuration it produced.

    ``config`` is that configuration as JSON — concurrency, distribution mode
    and the whole test.py command line — so a past run can be read back
    without the schema having to grow a column per option.
    """
    host_id: str
    name: str
    version: str
    plugin: str | None
    config: str
    timestamp: datetime


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
