#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
from datetime import datetime

from attr import define


@define
class CgroupMetric:
    memory: int
    test_id: int
    timestamp: datetime


@define
class Metric:
    test_id: int
    memory_peak: int = None
    success: bool = None
    system_sec: float = None
    time_end: float = None
    time_start: float = None
    time_taken: float = None
    usage_sec: float = None
    user_sec: float = None

@define
class SystemResourceMetric:
    cpu: float
    memory: float
    timestamp: datetime

@define
class Test:
    architecture: str
    directory: str
    mode: str
    run_id: int
    test_name: str
