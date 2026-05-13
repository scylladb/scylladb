#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from __future__ import annotations

from dataclasses import dataclass
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
class ScyllaResourceMarkerLimit:
    cores: int
    memory: str | int | float | None
    memory_bytes: int | None
    allow_memory_override: bool
    enforce_usage_limits: bool

    def as_manager_kwargs(self) -> dict[str, object]:
        return {
            "cores": self.cores,
            "memory": self.memory,
            "allow_memory_override": self.allow_memory_override,
            "enforce_usage_limits": self.enforce_usage_limits,
        }


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


def scylla_resource_limit_from_markers(node: Node) -> ScyllaResourceMarkerLimit | None:
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
        return ScyllaResourceMarkerLimit(
            cores=_positive_int(cores, "scylla_cores", "cores"),
            memory=None,
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
        return ScyllaResourceMarkerLimit(
            cores=_positive_int(cores, "scylla_resources", "cpu"),
            memory=memory,
            memory_bytes=_memory_bytes(memory, "scylla_resources", "mem"),
            allow_memory_override=True,
            enforce_usage_limits=not unbounded,
        )

    return None
