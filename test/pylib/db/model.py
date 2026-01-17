#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
from datetime import datetime

from attr import define


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
