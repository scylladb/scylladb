#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from __future__ import annotations

from dataclasses import dataclass
import math
import os
import pathlib
import re
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from _pytest.nodes import Node


_MEMORY_UNITS = {
    "": 1,
    "b": 1,
    "k": 1024,
    "kb": 1024,
    "ki": 1024,
    "kib": 1024,
    "m": 1024 ** 2,
    "mb": 1024 ** 2,
    "mi": 1024 ** 2,
    "mib": 1024 ** 2,
    "g": 1024 ** 3,
    "gb": 1024 ** 3,
    "gi": 1024 ** 3,
    "gib": 1024 ** 3,
    "t": 1024 ** 4,
    "tb": 1024 ** 4,
    "ti": 1024 ** 4,
    "tib": 1024 ** 4,
}

MIN_SYSTEM_MEMORY_RESERVE_BYTES = int(5e9)
MAX_SYSTEM_MEMORY_RESERVE_BYTES = int(8e9)
SYSTEM_MEMORY_RESERVE_FRACTION = 16
TEST_MEMORY_FRACTION = 8.0
MAX_TEST_MEMORY_BYTES = int(5e9)
NON_DEBUG_MAX_TEST_MEMORY_BYTES = int(4e9)
DEBUG_TEST_MEMORY_MULTIPLIER = 1.5
DEBUG_CPUS_PER_TEST_JOB = 1.5
NON_DEBUG_CPUS_PER_TEST_JOB = 1.0


def parse_scylla_memory(value: str | int | float) -> int:
    """Parse Scylla memory values such as 512M or 2G into bytes."""
    if isinstance(value, int):
        if value < 0:
            raise ValueError(f"memory must be non-negative, got {value}")
        return value
    if isinstance(value, float):
        if value < 0:
            raise ValueError(f"memory must be non-negative, got {value}")
        return int(value)

    match = re.fullmatch(r"\s*(\d+(?:\.\d+)?)([kmgt]?i?b?)?\s*", value, flags=re.IGNORECASE)
    if match is None:
        raise ValueError(f"invalid Scylla memory value: {value!r}")
    number, unit = match.groups()
    multiplier = _MEMORY_UNITS[unit.lower() if unit else ""]
    return int(float(number) * multiplier)


@dataclass(frozen=True)
class ScyllaResourceAmount:
    cores: float = 0
    memory_bytes: int = 0

    def __add__(self, other: 'ScyllaResourceAmount') -> 'ScyllaResourceAmount':
        return ScyllaResourceAmount(
            cores=self.cores + other.cores,
            memory_bytes=self.memory_bytes + other.memory_bytes,
        )

    def __mul__(self, multiplier: int | float) -> 'ScyllaResourceAmount':
        return ScyllaResourceAmount(
            cores=self.cores * multiplier,
            memory_bytes=int(self.memory_bytes * multiplier),
        )

    def fits_in(self, total: 'ScyllaResourceAmount') -> bool:
        return self.cores <= total.cores + 1e-9 and self.memory_bytes <= total.memory_bytes

    def max(self, other: 'ScyllaResourceAmount') -> 'ScyllaResourceAmount':
        return ScyllaResourceAmount(
            cores=max(self.cores, other.cores),
            memory_bytes=max(self.memory_bytes, other.memory_bytes),
        )

    def to_json(self) -> dict[str, float | int]:
        return {"cores": self.cores, "memory_bytes": self.memory_bytes}

    @classmethod
    def from_json(cls, data: dict[str, object]) -> 'ScyllaResourceAmount':
        return cls(cores=float(data["cores"]), memory_bytes=int(data["memory_bytes"]))


@dataclass(frozen=True)
class ScyllaResourceLimit:
    cores: int | None = None
    memory_bytes: int | None = None
    allow_memory_override: bool = False
    enforce_usage_limits: bool = True

    def as_manager_kwargs(self) -> dict[str, object]:
        return {
            "cores": self.cores,
            "memory_bytes": self.memory_bytes,
            "allow_memory_override": self.allow_memory_override,
            "enforce_usage_limits": self.enforce_usage_limits,
        }

    def resource_amount(self, default_memory_bytes: int) -> ScyllaResourceAmount:
        assert self.cores is not None
        return ScyllaResourceAmount(
            cores=float(self.cores),
            memory_bytes=self.memory_bytes if self.memory_bytes is not None else default_memory_bytes,
        )


def _positive_int(value: object, marker_name: str, field_name: str) -> int:
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise pytest.UsageError(f"@pytest.mark.{marker_name} {field_name} must be a positive integer") from exc
    if result <= 0:
        raise pytest.UsageError(f"@pytest.mark.{marker_name} {field_name} must be a positive integer")
    return result


def _memory_bytes(value: str | int | float, marker_name: str, field_name: str) -> int:
    try:
        result = parse_scylla_memory(value)
    except ValueError as exc:
        raise pytest.UsageError(f"@pytest.mark.{marker_name} {field_name} must be a valid Scylla memory size") from exc
    if result <= 0:
        raise pytest.UsageError(f"@pytest.mark.{marker_name} {field_name} must be a positive Scylla memory size")
    return result


def _bool_flag(value: object, marker_name: str, field_name: str) -> bool:
    if isinstance(value, bool):
        return value
    raise pytest.UsageError(f"@pytest.mark.{marker_name} {field_name} must be a boolean")


def default_scylla_resource_cpus() -> int:
    try:
        return max(1, len(os.sched_getaffinity(0)))
    except AttributeError:
        return max(1, os.cpu_count() or 1)


def _read_memory_limit(path: pathlib.Path) -> int | None:
    try:
        text = path.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    if not text or text == "max":
        return None
    try:
        limit = int(text)
    except ValueError:
        return None
    if limit <= 0 or limit >= 1 << 60:
        return None
    return limit


def system_memory_bytes() -> int:
    sys_mem = int(os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES"))
    cgroup_limits = [
        _read_memory_limit(pathlib.Path("/sys/fs/cgroup/memory.max")),
        _read_memory_limit(pathlib.Path("/sys/fs/cgroup/memory/memory.limit_in_bytes")),
    ]
    limits = [limit for limit in cgroup_limits if limit is not None]
    if limits:
        return min(sys_mem, *limits)
    return sys_mem


def system_memory_reserve_bytes(total_memory_bytes: int) -> int:
    return int(min(
        max(total_memory_bytes / SYSTEM_MEMORY_RESERVE_FRACTION, MIN_SYSTEM_MEMORY_RESERVE_BYTES),
        MAX_SYSTEM_MEMORY_RESERVE_BYTES,
    ))


def default_scylla_resource_memory() -> int:
    total = system_memory_bytes()
    return max(1, total - system_memory_reserve_bytes(total))


def configured_scylla_resource_memory(config: pytest.Config) -> int:
    value = config.getoption("--scylla-resource-memory")
    if value is None:
        return default_scylla_resource_memory()
    try:
        memory = parse_scylla_memory(value)
    except ValueError as exc:
        raise pytest.UsageError(f"--scylla-resource-memory must be a valid Scylla memory size: {value!r}") from exc
    if memory <= 0:
        raise pytest.UsageError("--scylla-resource-memory must be positive")
    return memory


def auto_test_jobs(
        modes: list[str],
        nr_cpus: int,
        *,
        total_memory_bytes: int | None = None,
        test_memory_fraction: float = TEST_MEMORY_FRACTION,
        max_test_memory_bytes: int = MAX_TEST_MEMORY_BYTES,
        non_debug_max_test_memory_bytes: int = NON_DEBUG_MAX_TEST_MEMORY_BYTES,
        debug_test_memory_multiplier: float = DEBUG_TEST_MEMORY_MULTIPLIER,
        debug_cpus_per_test_job: float = DEBUG_CPUS_PER_TEST_JOB,
        non_debug_cpus_per_test_job: float = NON_DEBUG_CPUS_PER_TEST_JOB,
        multiplier: int = 1) -> int:
    total_memory_bytes = total_memory_bytes if total_memory_bytes is not None else system_memory_bytes()
    available_mem = max(0, total_memory_bytes - system_memory_reserve_bytes(total_memory_bytes))
    is_debug = "debug" in modes
    test_mem = min(
        total_memory_bytes / test_memory_fraction,
        max_test_memory_bytes if is_debug else non_debug_max_test_memory_bytes,
    )
    if is_debug:
        test_mem *= debug_test_memory_multiplier
    default_num_jobs_mem = max(1, int(available_mem // test_mem))
    cpus_per_test_job = debug_cpus_per_test_job if is_debug else non_debug_cpus_per_test_job
    default_num_jobs_cpu = max(1, math.ceil(nr_cpus / cpus_per_test_job))
    return min(default_num_jobs_mem, default_num_jobs_cpu) * multiplier


def _cmdline_option_value(cmdline: list[str], names: set[str], default: str | None = None) -> str | None:
    i = 0
    while i < len(cmdline):
        arg = cmdline[i]
        if arg in names:
            if i + 1 >= len(cmdline):
                return default
            value = cmdline[i + 1]
            i += 2
            if value.startswith('-'):
                return default
            default = value
            continue
        if arg.startswith('--') and '=' in arg:
            name, _, value = arg.partition('=')
            if name in names:
                default = value
        elif '-m' in names and arg.startswith('-m') and arg != '-m':
            default = arg[2:]
        i += 1
    return default


def scylla_resource_usage_from_cmdline(cmdline: list[str]) -> ScyllaResourceAmount:
    smp = _cmdline_option_value(cmdline, {'--smp'}, default='2')
    memory = _cmdline_option_value(cmdline, {'-m', '--memory'}, default='1G')
    assert smp is not None
    assert memory is not None
    return ScyllaResourceAmount(cores=int(smp), memory_bytes=parse_scylla_memory(memory))


def scylla_resource_limit_from_payload(payload: dict[str, object] | None) -> ScyllaResourceLimit | None:
    if payload is None:
        return None
    cores = payload.get("cores")
    memory = payload.get("memory_bytes", payload.get("memory"))
    return ScyllaResourceLimit(
        cores=int(cores) if cores is not None else None,
        memory_bytes=parse_scylla_memory(memory) if memory is not None else None,
        allow_memory_override=bool(payload.get("allow_memory_override", False)),
        enforce_usage_limits=bool(payload.get("enforce_usage_limits", True)),
    )


def scylla_resource_limit_from_markers(node: Node) -> ScyllaResourceLimit | None:
    """Return the explicit Scylla resource limit declared on a pytest node."""
    cores_marker = node.get_closest_marker("scylla_cores")
    resources_marker = node.get_closest_marker("scylla_resources")
    if cores_marker is not None and resources_marker is not None:
        raise pytest.UsageError("Use either @pytest.mark.scylla_cores or @pytest.mark.scylla_resources, not both")

    if cores_marker is not None:
        unknown_kwargs = set(cores_marker.kwargs) - {"cores", "cpu", "unbounded"}
        if unknown_kwargs:
            raise pytest.UsageError(f"Unknown @pytest.mark.scylla_cores arguments: {', '.join(sorted(unknown_kwargs))}")
        cores = cores_marker.kwargs.get("cores", cores_marker.kwargs.get("cpu"))
        unbounded = _bool_flag(cores_marker.kwargs.get("unbounded", False), "scylla_cores", "unbounded")
        if cores_marker.args:
            if len(cores_marker.args) > 1 or cores is not None:
                raise pytest.UsageError("Use @pytest.mark.scylla_cores(n) or @pytest.mark.scylla_cores(cores=n)")
            cores = cores_marker.args[0]
        if cores is None:
            raise pytest.UsageError("@pytest.mark.scylla_cores requires a core limit")
        return ScyllaResourceLimit(
            cores=_positive_int(cores, "scylla_cores", "cores"),
            memory_bytes=None,
            allow_memory_override=False,
            enforce_usage_limits=not unbounded,
        )

    if resources_marker is not None:
        unknown_kwargs = set(resources_marker.kwargs) - {"cpu", "cores", "mem", "memory", "unbounded"}
        if unknown_kwargs:
            raise pytest.UsageError(f"Unknown @pytest.mark.scylla_resources arguments: {', '.join(sorted(unknown_kwargs))}")
        cores = resources_marker.kwargs.get("cpu", resources_marker.kwargs.get("cores"))
        memory = resources_marker.kwargs.get("mem", resources_marker.kwargs.get("memory"))
        unbounded = _bool_flag(resources_marker.kwargs.get("unbounded", False), "scylla_resources", "unbounded")
        if resources_marker.args:
            if len(resources_marker.args) > 2:
                raise pytest.UsageError("Use @pytest.mark.scylla_resources(cpu, mem) or keyword arguments")
            if cores is not None:
                raise pytest.UsageError("@pytest.mark.scylla_resources got duplicate cpu/cores argument")
            cores = resources_marker.args[0]
            if len(resources_marker.args) == 2:
                if memory is not None:
                    raise pytest.UsageError("@pytest.mark.scylla_resources got duplicate mem/memory argument")
                memory = resources_marker.args[1]
        if cores is None or memory is None:
            raise pytest.UsageError("@pytest.mark.scylla_resources requires cpu and mem arguments")
        return ScyllaResourceLimit(
            cores=_positive_int(cores, "scylla_resources", "cpu"),
            memory_bytes=_memory_bytes(memory, "scylla_resources", "mem"),
            allow_memory_override=True,
            enforce_usage_limits=not unbounded,
        )

    return None
