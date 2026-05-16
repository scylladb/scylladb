#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from __future__ import annotations

from collections import OrderedDict, deque
from collections.abc import Sequence
from dataclasses import dataclass, replace
import json
import logging
import os
import pathlib
import sqlite3
import time
from typing import TYPE_CHECKING, Any

import pytest
import universalasync

from test.pylib.scylla_cluster import SCYLLA_CMDLINE_OPTIONS, merge_cmdline_options
from test.pylib.db.writer import DEFAULT_DB_NAME, METRICS_TABLE, TESTS_TABLE
from test.pylib.scylla_cluster_profile import ScyllaClusterProfile, scylla_cluster_profile_from_node
from test.pylib.scylla_resources import (
    ScyllaResourceAmount,
    configured_scylla_resource_memory,
    default_scylla_resource_cpus,
    scylla_resource_limit_from_markers,
    scylla_resource_usage_from_cmdline,
)
from test.pylib.session_services import SessionServiceManager, normalize_session_services, session_service_requirements_for_item

if TYPE_CHECKING:
    from test.pylib.runner import TestSuiteConfig


logger = logging.getLogger(__name__)

SCYLLA_BACKED_NON_CLUSTER_SUITES = frozenset({
    "alternator",
    "broadcast_tables",
    "cql",
    "cqlpy",
    "boost",
    "nodetool",
    "ldap",
    "raft",
    "rest_api",
    "vector_search",
    "scylla_gdb",
})
SCYLLA_RESOURCE_METADATA_VERSION = 1
SCYLLA_RESOURCE_METADATA_PREFIX = "scylla_resource_metadata_"
SCYLLA_RESOURCE_TIMELINE_VERSION = 1
SCYLLA_RESOURCE_TIMELINE_FILENAME = "scylla_resource_timeline.jsonl"
DEFAULT_NON_CLUSTER_CMDLINE_OPTIONS = ["--smp", "1"]
DEFAULT_TOPOLOGY_NODE_COUNT = 3
DEBUG_RESOURCE_CPU_MULTIPLIER = 1.5
DEFAULT_SCYLLA_MEMORY_BYTES = 1024 ** 3
HISTORICAL_RESOURCE_MIN_SAMPLES = 3
HISTORICAL_CPU_HEADROOM = 1.5
HISTORICAL_MEMORY_HEADROOM = 1.25
HISTORICAL_DURATION_HEADROOM = 1.25
HISTORICAL_CPU_REDUCTION_FLOOR = 0.5
HISTORICAL_MEMORY_REDUCTION_FLOOR = 0.5
# Reserve one quarter of a host CPU for the default --smp 2 topology node.
TOPOLOGY_CPU_RESERVATION_PER_SMP = 0.125
DEFAULT_ESTIMATED_DURATION_SECONDS = 1.0
PREFETCH_MIN_ITEMS = 2
PREFETCH_MAX_ITEMS = 8
PREFETCH_TARGET_SECONDS = 5.0
LINGER_MIN_DURATION_SECONDS = 5.0
LINGER_MAX_CPU_CORES = 0.1

SchedulerResource = ScyllaResourceAmount
ResourceBucketKey = tuple[float, int]


@dataclass(frozen=True)
class ScyllaResourceMetadata:
    group_key: str
    resources: SchedulerResource
    services: frozenset[str] = frozenset()
    cluster_profile_key: str | None = None
    cluster_profile_name: str | None = None
    cluster_reuse: str | None = None
    estimated_duration_seconds: float | None = None
    observed_cpu_cores: float | None = None

    def to_json(self) -> dict[str, object]:
        return {
            "group_key": self.group_key,
            "resources": self.resources.to_json(),
            "services": sorted(self.services),
            "cluster_profile_key": self.cluster_profile_key,
            "cluster_profile_name": self.cluster_profile_name,
            "cluster_reuse": self.cluster_reuse,
            "estimated_duration_seconds": self.estimated_duration_seconds,
            "observed_cpu_cores": self.observed_cpu_cores,
        }

    @classmethod
    def from_json(cls, data: dict[str, object]) -> ScyllaResourceMetadata:
        return cls(
            group_key=str(data["group_key"]),
            resources=SchedulerResource.from_json(data["resources"]),
            services=normalize_session_services(str(service) for service in data.get("services", [])),
            cluster_profile_key=str(data["cluster_profile_key"]) if data.get("cluster_profile_key") is not None else None,
            cluster_profile_name=str(data["cluster_profile_name"]) if data.get("cluster_profile_name") is not None else None,
            cluster_reuse=str(data["cluster_reuse"]) if data.get("cluster_reuse") is not None else None,
            estimated_duration_seconds=float(data["estimated_duration_seconds"]) if data.get("estimated_duration_seconds") is not None else None,
            observed_cpu_cores=float(data["observed_cpu_cores"]) if data.get("observed_cpu_cores") is not None else None,
        )


@dataclass(frozen=True)
class HistoricalResourceUsage:
    sample_count: int
    cores: float | None = None
    memory_bytes: int | None = None
    duration_seconds: float | None = None


def scylla_resource_budget_from_config(config: pytest.Config) -> SchedulerResource:
    return SchedulerResource(
        cores=float(config.getoption("--scylla-resource-cpus") or default_scylla_resource_cpus()),
        memory_bytes=configured_scylla_resource_memory(config),
    )


def scylla_resource_budget_failure(nodeid: str, resources: SchedulerResource, total_resources: SchedulerResource) -> str | None:
    if resources.fits_in(total_resources):
        return None
    return (
        f"Scylla resource group {nodeid!r} requires {resources.cores:g} CPU(s) "
        f"and {resources.memory_bytes} bytes, exceeding scheduler budget "
        f"{total_resources.cores:g} CPU(s) and {total_resources.memory_bytes} bytes"
    )


def scylla_resource_metadata_dir(config: pytest.Config) -> pathlib.Path:
    return pathlib.Path(config.getoption("--tmpdir")).absolute() / "pytest_log"


def scylla_resource_metadata_path(config: pytest.Config, worker_id: str) -> pathlib.Path:
    return scylla_resource_metadata_dir(config) / f"{SCYLLA_RESOURCE_METADATA_PREFIX}{worker_id}.json"


def scylla_resource_timeline_path(config: pytest.Config) -> pathlib.Path | None:
    tmpdir = config.getoption("--tmpdir")
    if tmpdir is None:
        return None
    return pathlib.Path(tmpdir).absolute() / "pytest_log" / SCYLLA_RESOURCE_TIMELINE_FILENAME


def historical_resource_db_path(config: pytest.Config) -> pathlib.Path | None:
    tmpdir = config.getoption("--tmpdir")
    if tmpdir is None:
        return None
    return pathlib.Path(tmpdir).absolute() / DEFAULT_DB_NAME


def historical_resource_db_paths(config: pytest.Config) -> list[pathlib.Path]:
    tmpdir = config.getoption("--tmpdir")
    if tmpdir is None:
        return []

    tmpdir_path = pathlib.Path(tmpdir).absolute()
    db_paths = sorted(path for path in tmpdir_path.glob("sqlite_*.db") if path.is_file())
    if not db_paths:
        current_db = tmpdir_path / DEFAULT_DB_NAME
        if current_db.exists():
            db_paths.append(current_db)
    return db_paths


def _nodeid_path(nodeid: str) -> str:
    return nodeid.split("::", 1)[0]


def _module_group_key(nodeid: str, build_mode: str) -> str:
    return f"module:{build_mode}:{_nodeid_path(nodeid)}"


def _item_group_key(nodeid: str) -> str:
    return f"item:{nodeid}"


def _suite_relative_test_name(item: pytest.Item, suite_config: TestSuiteConfig | None) -> str | None:
    if suite_config is None:
        return None
    try:
        relative = pathlib.Path(item.path).relative_to(suite_config.path)
    except ValueError:
        try:
            relative = pathlib.Path(item.path).resolve().relative_to(suite_config.path.resolve())
        except ValueError:
            return None
    return relative.with_suffix("").as_posix()


def _load_historical_resource_usage(config: pytest.Config) -> dict[tuple[str, str, str], HistoricalResourceUsage]:
    cached = getattr(config, "_scylla_historical_resource_usage", None)
    if cached is not None:
        return cached

    result: dict[tuple[str, str, str], HistoricalResourceUsage] = {}
    db_paths = historical_resource_db_paths(config)
    if not db_paths:
        config._scylla_historical_resource_usage = result
        return result

    aggregated: dict[tuple[str, str, str], dict[str, float | int | None]] = {}

    for db_path in db_paths:
        connection = None
        try:
            connection = sqlite3.connect(db_path)
            cursor = connection.cursor()
            cursor.execute(
                f"""
                SELECT
                    t.directory,
                    t.mode,
                    t.test_name,
                    COUNT(*) AS sample_count,
                    MAX(CAST(m.memory_peak AS INTEGER)) AS max_memory_peak,
                    MAX(CASE WHEN m.time_taken > 0 AND m.usage_sec IS NOT NULL THEN m.usage_sec / m.time_taken END) AS max_avg_cpu,
                    SUM(CASE WHEN m.time_taken > 0 THEN m.time_taken ELSE 0 END) AS total_time_taken,
                    SUM(CASE WHEN m.time_taken > 0 THEN 1 ELSE 0 END) AS timed_samples
                FROM {TESTS_TABLE} AS t
                JOIN {METRICS_TABLE} AS m ON m.test_id = t.id
                WHERE m.success = 1
                GROUP BY t.directory, t.mode, t.test_name
                """
            )
            for directory, mode, test_name, sample_count, max_memory_peak, max_avg_cpu, total_time_taken, timed_samples in cursor.fetchall():
                key = (str(directory), str(mode), str(test_name))
                entry = aggregated.setdefault(
                    key,
                    {
                        "sample_count": 0,
                        "memory_bytes": None,
                        "cores": None,
                        "total_time_taken": 0.0,
                        "timed_samples": 0,
                    },
                )
                entry["sample_count"] = int(entry["sample_count"]) + int(sample_count)
                if max_memory_peak is not None:
                    current_memory = entry["memory_bytes"]
                    entry["memory_bytes"] = max(int(max_memory_peak), int(current_memory)) if current_memory is not None else int(max_memory_peak)
                if max_avg_cpu is not None:
                    current_cores = entry["cores"]
                    entry["cores"] = max(float(max_avg_cpu), float(current_cores)) if current_cores is not None else float(max_avg_cpu)
                entry["total_time_taken"] = float(entry["total_time_taken"]) + float(total_time_taken or 0.0)
                entry["timed_samples"] = int(entry["timed_samples"]) + int(timed_samples or 0)
        except sqlite3.Error:
            logger.debug("Failed to load historical resource usage from %s", db_path, exc_info=True)
        finally:
            try:
                if connection is not None:
                    connection.close()
            except Exception:
                pass

    for key, entry in aggregated.items():
        timed_samples = int(entry["timed_samples"])
        duration_seconds = None if timed_samples == 0 else float(entry["total_time_taken"]) / timed_samples
        result[key] = HistoricalResourceUsage(
            sample_count=int(entry["sample_count"]),
            cores=float(entry["cores"]) if entry["cores"] is not None else None,
            memory_bytes=int(entry["memory_bytes"]) if entry["memory_bytes"] is not None else None,
            duration_seconds=duration_seconds,
        )

    config._scylla_historical_resource_usage = result
    return result


def _historical_resource_usage_for_item(
        item: pytest.Item,
        suite_config: TestSuiteConfig | None,
        build_mode: str,
        config: pytest.Config | None) -> HistoricalResourceUsage | None:
    if config is None or suite_config is None:
        return None
    if not (test_name := _suite_relative_test_name(item, suite_config)):
        return None
    history = _load_historical_resource_usage(config).get((suite_config.name, build_mode, test_name))
    if history is None or history.sample_count < HISTORICAL_RESOURCE_MIN_SAMPLES:
        return None
    return history


def _apply_historical_resource_usage(default_resources: SchedulerResource, history: HistoricalResourceUsage | None) -> SchedulerResource:
    if history is None:
        return default_resources

    resources = default_resources
    if default_resources.cores > 0 and history.cores is not None and history.cores > 0:
        minimum_cores = default_resources.cores * HISTORICAL_CPU_REDUCTION_FLOOR
        adjusted_cores = max(history.cores * HISTORICAL_CPU_HEADROOM, minimum_cores)
        resources = replace(resources, cores=min(default_resources.cores, adjusted_cores))

    if default_resources.memory_bytes > 0 and history.memory_bytes is not None and history.memory_bytes > 0:
        minimum_memory = int(default_resources.memory_bytes * HISTORICAL_MEMORY_REDUCTION_FLOOR)
        adjusted_memory = max(int(history.memory_bytes * HISTORICAL_MEMORY_HEADROOM), minimum_memory)
        resources = replace(resources, memory_bytes=min(default_resources.memory_bytes, adjusted_memory))

    return resources


def _estimated_duration_seconds_from_history(history: HistoricalResourceUsage | None) -> float | None:
    if history is None or history.duration_seconds is None or history.duration_seconds <= 0:
        return None
    return history.duration_seconds * HISTORICAL_DURATION_HEADROOM


def _observed_cpu_cores_from_history(history: HistoricalResourceUsage | None) -> float | None:
    if history is None or history.cores is None or history.cores <= 0:
        return None
    return history.cores


def _is_lingering_candidate(item_metadata: ScyllaResourceMetadata) -> bool:
    return (
        item_metadata.estimated_duration_seconds is not None
        and item_metadata.estimated_duration_seconds >= LINGER_MIN_DURATION_SECONDS
        and item_metadata.observed_cpu_cores is not None
        and item_metadata.observed_cpu_cores <= LINGER_MAX_CPU_CORES
    )


def _item_priority_key(nodeid: str, item_metadata: ScyllaResourceMetadata) -> tuple[int, float, float]:
    if not _is_lingering_candidate(item_metadata):
        return (1, 0.0, 0.0)

    duration_rank = -(item_metadata.estimated_duration_seconds or DEFAULT_ESTIMATED_DURATION_SECONDS)
    cpu_rank = item_metadata.observed_cpu_cores if item_metadata.observed_cpu_cores is not None else item_metadata.resources.cores
    return (0, duration_rank, cpu_rank)


def _suite_cmdline_options(suite_config: TestSuiteConfig | None) -> list[str]:
    if suite_config is None:
        return []
    cmdline_options = suite_config.cfg.get("extra_scylla_cmdline_options", [])
    if isinstance(cmdline_options, str):
        return [cmdline_options]
    return list(cmdline_options or [])


def _resource_from_cmdline(cmdline_options: list[str]) -> SchedulerResource:
    usage = scylla_resource_usage_from_cmdline(cmdline_options)
    return SchedulerResource(cores=usage.cores, memory_bytes=usage.memory_bytes)


def _topology_resource_from_cmdline(cmdline_options: list[str]) -> SchedulerResource:
    resource = _resource_from_cmdline(cmdline_options)
    return replace(resource, cores=resource.cores * TOPOLOGY_CPU_RESERVATION_PER_SMP)


def _non_cluster_default_resource(suite_config: TestSuiteConfig | None) -> SchedulerResource:
    cmdline_options = merge_cmdline_options(DEFAULT_NON_CLUSTER_CMDLINE_OPTIONS, _suite_cmdline_options(suite_config))
    cmdline_options = merge_cmdline_options(SCYLLA_CMDLINE_OPTIONS, cmdline_options)
    return _resource_from_cmdline(cmdline_options)


def _topology_default_node_count(item: pytest.Item) -> int:
    if item.get_closest_marker("single_node") is not None:
        return 1
    return DEFAULT_TOPOLOGY_NODE_COUNT


def _topology_default_resource(suite_config: TestSuiteConfig | None, node_count: int = DEFAULT_TOPOLOGY_NODE_COUNT) -> SchedulerResource:
    cmdline_options = merge_cmdline_options(SCYLLA_CMDLINE_OPTIONS, _suite_cmdline_options(suite_config))
    node_resource = _topology_resource_from_cmdline(cmdline_options)
    return SchedulerResource(
        cores=node_resource.cores * node_count,
        memory_bytes=node_resource.memory_bytes * node_count,
    )


def _topology_profile_resource(profile: ScyllaClusterProfile, suite_config: TestSuiteConfig | None) -> SchedulerResource:
    suite_cmdline_options = _suite_cmdline_options(suite_config)
    resources = SchedulerResource()
    for node in profile.nodes:
        cmdline_options = merge_cmdline_options(SCYLLA_CMDLINE_OPTIONS, suite_cmdline_options)
        cmdline_options = merge_cmdline_options(cmdline_options, node.cmdline)
        resources += _topology_resource_from_cmdline(cmdline_options)
    return resources


def _is_topology_suite(suite_config: TestSuiteConfig | None) -> bool:
    if suite_config is None:
        return False
    return suite_config.name == "cluster" or str(suite_config.cfg.get("type", "")).casefold() == "topology"


def _is_scylla_backed_non_cluster_suite(suite_config: TestSuiteConfig | None) -> bool:
    return suite_config is not None and suite_config.name in SCYLLA_BACKED_NON_CLUSTER_SUITES


def scylla_resource_metadata_for_item(
        item: pytest.Item,
        suite_config: TestSuiteConfig | None,
        build_mode: str,
        is_debug_mode: bool,
        config: pytest.Config | None = None) -> ScyllaResourceMetadata:
    services = session_service_requirements_for_item(item=item, suite_config=suite_config)
    if item.get_closest_marker("without_scylla") is not None:
        return ScyllaResourceMetadata(group_key=_item_group_key(item.nodeid), resources=SchedulerResource(), services=services)

    default_resources = SchedulerResource()
    group_key = _item_group_key(item.nodeid)
    cluster_profile = scylla_cluster_profile_from_node(item, suite_config, build_mode) if _is_topology_suite(suite_config) else None
    if _is_topology_suite(suite_config):
        if cluster_profile is not None:
            default_resources = _topology_profile_resource(cluster_profile, suite_config)
            group_key = f"cluster:{cluster_profile.key}"
        else:
            default_resources = _topology_default_resource(suite_config, _topology_default_node_count(item))
    elif _is_scylla_backed_non_cluster_suite(suite_config):
        default_resources = _non_cluster_default_resource(suite_config)
        group_key = _module_group_key(item.nodeid, build_mode)

    history = _historical_resource_usage_for_item(item, suite_config, build_mode, config or getattr(item, "config", None))

    if resource_limit := scylla_resource_limit_from_markers(item):
        resources = resource_limit.resource_amount(default_resources.memory_bytes or DEFAULT_SCYLLA_MEMORY_BYTES)
    else:
        resources = _apply_historical_resource_usage(
            default_resources,
            history,
        )

    if resources.cores and is_debug_mode:
        resources = replace(resources, cores=resources.cores * DEBUG_RESOURCE_CPU_MULTIPLIER)

    return ScyllaResourceMetadata(
        group_key=group_key,
        resources=resources,
        services=services,
        cluster_profile_key=cluster_profile.key if cluster_profile is not None else None,
        cluster_profile_name=cluster_profile.name if cluster_profile is not None else None,
        cluster_reuse=cluster_profile.reuse if cluster_profile is not None else None,
        estimated_duration_seconds=_estimated_duration_seconds_from_history(history),
        observed_cpu_cores=_observed_cpu_cores_from_history(history),
    )


def write_scylla_resource_metadata(config: pytest.Config, items: Sequence[pytest.Item], metadata: dict[str, ScyllaResourceMetadata]) -> None:
    worker_id = os.environ.get("PYTEST_XDIST_WORKER")
    if worker_id is None:
        return
    if config.getoption("--scylla-resource-scheduler") == "off":
        return

    metadata_dir = scylla_resource_metadata_dir(config)
    metadata_dir.mkdir(parents=True, exist_ok=True)
    path = scylla_resource_metadata_path(config, worker_id)
    tmp_path = path.with_suffix(".tmp")
    payload = {
        "version": SCYLLA_RESOURCE_METADATA_VERSION,
        "items": {item.nodeid: metadata[item.nodeid].to_json() for item in items},
    }
    tmp_path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    tmp_path.replace(path)


def load_scylla_resource_metadata(config: pytest.Config) -> dict[str, ScyllaResourceMetadata]:
    result: dict[str, ScyllaResourceMetadata] = {}
    metadata_dir = scylla_resource_metadata_dir(config)
    for path in sorted(metadata_dir.glob(f"{SCYLLA_RESOURCE_METADATA_PREFIX}*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise pytest.UsageError(f"Failed to read Scylla resource metadata from {path}: {exc}") from exc
        if payload.get("version") != SCYLLA_RESOURCE_METADATA_VERSION:
            raise pytest.UsageError(f"Unsupported Scylla resource metadata version in {path}")
        for nodeid, item_data in payload.get("items", {}).items():
            result[nodeid] = ScyllaResourceMetadata.from_json(item_data)
    return result


def _is_scylla_backed_nodeid(nodeid: str) -> bool:
    path = _nodeid_path(nodeid)
    if path.startswith("test/cluster/"):
        return True
    return any(path.startswith(f"test/{suite}/") for suite in SCYLLA_BACKED_NON_CLUSTER_SUITES)


def _fallback_metadata_from_nodeid(nodeid: str) -> ScyllaResourceMetadata:
    path = _nodeid_path(nodeid)
    if path.startswith("test/cluster/"):
        return ScyllaResourceMetadata(group_key=_item_group_key(nodeid), resources=_topology_default_resource(None))
    for suite in SCYLLA_BACKED_NON_CLUSTER_SUITES:
        if path.startswith(f"test/{suite}/"):
            return ScyllaResourceMetadata(group_key=f"module:unknown:{path}", resources=SchedulerResource(cores=1, memory_bytes=DEFAULT_SCYLLA_MEMORY_BYTES))
    return ScyllaResourceMetadata(group_key=_item_group_key(nodeid), resources=SchedulerResource())


def queue_items_from_collection(collection: Sequence[str], metadata: dict[str, ScyllaResourceMetadata]) -> OrderedDict[str, ScyllaResourceMetadata]:
    return OrderedDict(
        (nodeid, metadata.get(nodeid, _fallback_metadata_from_nodeid(nodeid)))
        for nodeid in collection
    )


def plan_items_by_service_phase(workqueue: OrderedDict[str, ScyllaResourceMetadata]) -> OrderedDict[str, ScyllaResourceMetadata]:
    phase_order: list[frozenset[str]] = []
    phased: dict[frozenset[str], OrderedDict[str, ScyllaResourceMetadata]] = {}

    for nodeid, item_metadata in workqueue.items():
        services = item_metadata.services
        if services not in phased:
            phased[services] = OrderedDict()
            phase_order.append(services)
        phased[services][nodeid] = item_metadata

    # Prefer tests with no external service first so a run that mostly does not
    # need MinIO/LDAP does not pay that memory cost during the whole session.
    phase_order.sort(key=lambda services: (bool(services), tuple(sorted(services))))

    planned: OrderedDict[str, ScyllaResourceMetadata] = OrderedDict()
    for services in phase_order:
        for nodeid, item_metadata in sorted(phased[services].items(), key=lambda item: _item_priority_key(item[0], item[1])):
            planned[nodeid] = item_metadata
    return planned


def _resource_bucket_key(resources: SchedulerResource) -> ResourceBucketKey:
    return (resources.cores, resources.memory_bytes)


class _SchedulerLog:
    def __call__(self, *args: object) -> None:
        logger.debug(" ".join(map(str, args)))


class SchedulerTimeline:
    def __init__(self, config: pytest.Config, total_resources: SchedulerResource, numnodes: int) -> None:
        self.path = scylla_resource_timeline_path(config)
        self.started_at = time.time()
        self.started_monotonic = time.monotonic()
        self.disabled = self.path is None
        self.finished = False
        if self.disabled:
            return

        assert self.path is not None
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.path.write_text("", encoding="utf-8")
        except OSError as exc:
            logger.warning("Failed to initialize Scylla scheduler timeline at %s: %s", self.path, exc)
            self.disabled = True
            return

        self.record(
            "scheduler_start",
            budget=total_resources.to_json(),
            numnodes=numnodes,
        )

    def record(self, event: str, **fields: object) -> None:
        if self.disabled:
            return

        now_monotonic = time.monotonic()
        payload = {
            "version": SCYLLA_RESOURCE_TIMELINE_VERSION,
            "event": event,
            "timestamp": self.started_at + (now_monotonic - self.started_monotonic),
            "elapsed_seconds": now_monotonic - self.started_monotonic,
            **fields,
        }
        assert self.path is not None
        try:
            with self.path.open("a", encoding="utf-8") as timeline:
                timeline.write(json.dumps(payload, sort_keys=True) + "\n")
        except OSError as exc:
            logger.warning("Failed to write Scylla scheduler timeline event %s to %s: %s", event, self.path, exc)
            self.disabled = True


class ScyllaResourceScheduler:
    """xdist scheduler that admits tests only when their Scylla resource group fits."""

    def __init__(
            self,
            config: pytest.Config,
            numnodes: int,
            log: Any | None = None,
            resource_metadata: dict[str, ScyllaResourceMetadata] | None = None,
            service_manager: SessionServiceManager | None = None) -> None:
        self.numnodes = numnodes
        self.collection: list[str] | None = None
        self.workqueue: OrderedDict[str, ScyllaResourceMetadata] = OrderedDict()
        self.workqueue_by_services: dict[frozenset[str], OrderedDict[ResourceBucketKey, deque[str]]] = {}
        self.assigned_work: dict[Any, OrderedDict[str, ScyllaResourceMetadata]] = {}
        self.registered_collections: dict[Any, list[str]] = {}
        self.worker_collection_indexes: dict[Any, dict[str, int]] = {}
        self.node_reservations: dict[Any, SchedulerResource] = {}
        self.node_backlog_seconds: dict[Any, float] = {}
        self.resource_usage_in_use: SchedulerResource = SchedulerResource()
        self.config = config
        self.resource_metadata = resource_metadata
        self.service_manager = service_manager
        self.active_services: frozenset[str] | None = None
        self.total_resources = scylla_resource_budget_from_config(config)
        self.log = getattr(log, "scyllaresourcesched", _SchedulerLog()) if log is not None else _SchedulerLog()
        self.timeline = SchedulerTimeline(config=config, total_resources=self.total_resources, numnodes=numnodes)

    @property
    def nodes(self) -> list[Any]:
        return list(self.assigned_work.keys())

    @property
    def collection_is_completed(self) -> bool:
        return len(self.registered_collections) >= self.numnodes

    @property
    def tests_finished(self) -> bool:
        return self.collection_is_completed and not self.workqueue and all(not workload for workload in self.assigned_work.values())

    @property
    def has_pending(self) -> bool:
        return bool(self.workqueue) or any(workload for workload in self.assigned_work.values())

    def add_node(self, node: Any) -> None:
        assert node not in self.assigned_work
        self.assigned_work[node] = OrderedDict()
        self.worker_collection_indexes[node] = {}
        self.node_reservations[node] = SchedulerResource()
        self.node_backlog_seconds[node] = 0.0
        self._record_timeline("node_added", node=self._node_name(node))

    def add_node_collection(self, node: Any, collection: Sequence[str]) -> None:
        assert node in self.assigned_work
        if self.collection_is_completed:
            assert self.collection is not None
            if list(collection) != self.collection:
                self.log(f"New worker {self._node_name(node)} collected different tests; ignoring it")
                return
        registered_collection = list(collection)
        self.registered_collections[node] = registered_collection
        self.worker_collection_indexes[node] = {
            nodeid: index for index, nodeid in enumerate(registered_collection)
        }
        self._record_timeline("collection_registered", node=self._node_name(node), tests=len(collection))

    def mark_test_complete(self, node: Any, item_index: int, duration: float = 0) -> None:
        nodeid = self.registered_collections[node][item_index]
        workload = self.assigned_work[node]
        item_metadata = workload.pop(nodeid, None)
        if item_metadata is None:
            raise KeyError(f"Completed test {nodeid} was not assigned to {self._node_name(node)}")

        self._record_timeline(
            "test_complete",
            node=self._node_name(node),
            nodeid=nodeid,
            item_index=item_index,
            resources=item_metadata.resources.to_json(),
            services=sorted(item_metadata.services),
            duration_seconds=duration,
            estimated_start_timestamp=self._estimated_start_timestamp(duration),
        )
        self._record_timeline("item_complete", node=self._node_name(node), nodeid=nodeid)

        self._refresh_node_reservation(node)
        self._refresh_node_backlog_seconds(node)
        self._reschedule_after_completion(node)

    def mark_test_pending(self, item: str) -> None:
        assert self.collection is not None
        metadata = self._metadata_for_collection(self.collection)
        item_metadata = metadata.get(item, _fallback_metadata_from_nodeid(item))
        self._enqueue_item(item, item_metadata, front=True)
        self._record_timeline("test_requeued", nodeid=item, reason="mark_test_pending")
        self._reschedule_all()

    def remove_pending_tests_from_node(self, node: Any, indices: Sequence[int]) -> None:
        nodeids = {self.registered_collections[node][index] for index in indices}
        workload = self.assigned_work[node]
        requeued: OrderedDict[str, ScyllaResourceMetadata] = OrderedDict()
        for nodeid in nodeids:
            item_metadata = workload.pop(nodeid, None)
            if item_metadata is None:
                continue
            requeued[nodeid] = item_metadata
            self._record_timeline(
                "item_requeued",
                node=self._node_name(node),
                nodeid=nodeid,
                reason="remove_pending_tests_from_node",
            )
        self._refresh_node_reservation(node)
        self._refresh_node_backlog_seconds(node)
        for nodeid, item_metadata in reversed(list(requeued.items())):
            self._enqueue_item(nodeid, item_metadata, front=True)
        self._reschedule_all()

    def remove_node(self, node: Any) -> str | None:
        workload = self.assigned_work.pop(node)
        self.worker_collection_indexes.pop(node, None)
        self._drop_node_reservation(node)
        self.node_backlog_seconds.pop(node, None)
        if not workload:
            return None

        crashitem = None
        requeued: OrderedDict[str, ScyllaResourceMetadata] = OrderedDict(workload.items())
        for nodeid in requeued:
            if crashitem is None:
                crashitem = nodeid
            self._record_timeline(
                "item_requeued",
                node=self._node_name(node),
                nodeid=nodeid,
                reason="node_removed",
            )

        for nodeid, item_metadata in reversed(list(requeued.items())):
            self._enqueue_item(nodeid, item_metadata, front=True)
        self._record_timeline("node_removed", node=self._node_name(node), crashitem=crashitem)
        self._reschedule_all()
        return crashitem

    def schedule(self) -> None:
        assert self.collection_is_completed

        if self.collection is not None:
            self._reschedule_all()
            return

        if not self._check_nodes_have_same_collection():
            self.log("**Different tests collected, aborting run**")
            return

        self.collection = list(next(iter(self.registered_collections.values())))
        if not self.collection:
            self._record_timeline("collection_ready", tests=0, work_items=0)
            self._record_finish_if_done()
            return

        metadata = self._metadata_for_collection(self.collection)
        self._set_workqueue(plan_items_by_service_phase(queue_items_from_collection(self.collection, metadata)))
        self._record_timeline("collection_ready", tests=len(self.collection), work_items=len(self.workqueue))
        self._reschedule_all()

    def _metadata_for_collection(self, collection: Sequence[str]) -> dict[str, ScyllaResourceMetadata]:
        if self.resource_metadata is None:
            self.resource_metadata = load_scylla_resource_metadata(self.config)

        missing = [nodeid for nodeid in collection if nodeid not in self.resource_metadata and _is_scylla_backed_nodeid(nodeid)]
        if missing:
            raise pytest.UsageError(
                "Scylla resource scheduler did not receive metadata for Scylla-backed tests, "
                f"for example {missing[0]!r}"
            )
        return self.resource_metadata

    def _resource_usage_in_use(self) -> SchedulerResource:
        return self.resource_usage_in_use

    def _resource_reservation_of(self, workload: OrderedDict[str, ScyllaResourceMetadata]) -> SchedulerResource:
        reservation = SchedulerResource()
        for item_metadata in workload.values():
            reservation = reservation.max(item_metadata.resources)
        return reservation

    def _fit_budget_for_node(self, node: Any) -> SchedulerResource:
        current_reservation = self.node_reservations.get(node, SchedulerResource())
        return SchedulerResource(
            cores=self.total_resources.cores - self.resource_usage_in_use.cores + current_reservation.cores,
            memory_bytes=self.total_resources.memory_bytes - self.resource_usage_in_use.memory_bytes + current_reservation.memory_bytes,
        )

    def _item_fits_now(self, node: Any, item_metadata: ScyllaResourceMetadata, fit_budget: SchedulerResource | None = None) -> bool:
        if self._item_exceeds_budget(item_metadata):
            return self._pending_of(self.assigned_work[node]) == 0
        return item_metadata.resources.fits_in(fit_budget if fit_budget is not None else self._fit_budget_for_node(node))

    def _item_exceeds_budget(self, item_metadata: ScyllaResourceMetadata) -> bool:
        return not item_metadata.resources.fits_in(self.total_resources)

    def _estimated_duration_seconds(self, item_metadata: ScyllaResourceMetadata) -> float:
        if item_metadata.estimated_duration_seconds is not None and item_metadata.estimated_duration_seconds > 0:
            return item_metadata.estimated_duration_seconds
        return DEFAULT_ESTIMATED_DURATION_SECONDS

    def _prefetch_saturated(self, node: Any) -> bool:
        assigned_count = self._pending_of(self.assigned_work[node])
        if assigned_count < PREFETCH_MIN_ITEMS:
            return False
        if assigned_count >= PREFETCH_MAX_ITEMS:
            return True
        return self.node_backlog_seconds.get(node, 0.0) >= PREFETCH_TARGET_SECONDS

    def _set_workqueue(self, workqueue: OrderedDict[str, ScyllaResourceMetadata]) -> None:
        self.workqueue = OrderedDict()
        self.workqueue_by_services = {}
        for nodeid, item_metadata in workqueue.items():
            self._enqueue_item(nodeid, item_metadata)

    def _enqueue_item(self, nodeid: str, item_metadata: ScyllaResourceMetadata, *, front: bool = False) -> None:
        self.workqueue[nodeid] = item_metadata
        services_queue = self.workqueue_by_services.setdefault(item_metadata.services, OrderedDict())
        bucket_key = _resource_bucket_key(item_metadata.resources)
        bucket = services_queue.get(bucket_key)
        if bucket is None:
            services_queue[bucket_key] = deque()
            if front:
                services_queue.move_to_end(bucket_key, last=False)
            bucket = services_queue[bucket_key]
        if front:
            bucket.appendleft(nodeid)
        else:
            bucket.append(nodeid)

    def _pop_queued_item(self, services: frozenset[str], bucket_key: ResourceBucketKey) -> tuple[str, ScyllaResourceMetadata]:
        services_queue = self.workqueue_by_services[services]
        bucket = services_queue[bucket_key]
        nodeid = bucket.popleft()
        if not bucket:
            del services_queue[bucket_key]
            if not services_queue:
                del self.workqueue_by_services[services]
        item_metadata = self.workqueue.pop(nodeid)
        return nodeid, item_metadata

    def _ready_service_groups(self) -> list[frozenset[str]]:
        active_services = self.active_services or frozenset()
        return sorted(
            [services for services in self.workqueue_by_services if services.issubset(active_services)],
            key=lambda services: (bool(services), tuple(sorted(services))),
        )

    def _next_fitting_nodeid(self, node: Any) -> str | None:
        self._advance_service_phase_if_needed()
        oversized_nodeid: str | None = None
        fit_budget = self._fit_budget_for_node(node)
        for services in self._ready_service_groups():
            services_queue = self.workqueue_by_services[services]
            for bucket_key, bucket in list(services_queue.items()):
                resources = SchedulerResource(cores=bucket_key[0], memory_bytes=bucket_key[1])
                if not resources.fits_in(self.total_resources):
                    oversized_nodeid = oversized_nodeid or bucket[0]
                    continue
                if not resources.fits_in(fit_budget):
                    continue
                return bucket[0]
        if (
            oversized_nodeid is not None
            and self._pending_of(self.assigned_work[node]) == 0
            and self._resource_usage_in_use() == SchedulerResource()
        ):
            return oversized_nodeid
        return None

    def _assign_item(self, node: Any, nodeid: str) -> None:
        item_metadata = self.workqueue.pop(nodeid)
        self._remove_item_from_indexes(nodeid, item_metadata)
        self.assigned_work[node][nodeid] = item_metadata
        self._refresh_node_reservation(node)
        self._refresh_node_backlog_seconds(node)
        self._send_item(node, nodeid, item_metadata)

    def _send_item(self, node: Any, nodeid: str, item_metadata: ScyllaResourceMetadata) -> None:
        worker_collection_indexes = self.worker_collection_indexes[node]
        item_index = worker_collection_indexes[nodeid]
        node.send_runtest_some([item_index])
        self._record_timeline(
            "dispatch",
            node=self._node_name(node),
            item_index=item_index,
            nodeid=nodeid,
            nodeids=[nodeid],
            resources=item_metadata.resources.to_json(),
            exceeds_budget=self._item_exceeds_budget(item_metadata),
            services=sorted(item_metadata.services),
            active_services=sorted(self.active_services or ()),
            workqueue_items=len(self.workqueue),
            resource_in_use=self._resource_usage_in_use().to_json(),
        )

    def _reschedule(self, node: Any) -> bool:
        if getattr(node, "shutting_down", False):
            return False
        if self._prefetch_saturated(node):
            return False
        if not self.workqueue:
            if self._can_shutdown_nodes():
                self._shutdown_node(node)
            return False

        nodeid = self._next_fitting_nodeid(node)
        if nodeid is None:
            return False
        self._assign_item(node, nodeid)
        return True

    def _reschedule_all(self) -> None:
        self._advance_service_phase_if_needed()
        made_progress = True
        while made_progress:
            made_progress = False
            for node in self.nodes:
                while self._reschedule(node):
                    made_progress = True
            self._advance_service_phase_if_needed()
            self._send_service_phase_lookahead()

        if not self.workqueue and self._can_shutdown_nodes():
            for node in self.nodes:
                if not getattr(node, "shutting_down", False):
                    self._shutdown_node(node)

        self._record_finish_if_done()

        self.log("Scylla resource scheduler waiting items:", len(self.workqueue), "in-use:", self._resource_usage_in_use())

    def _reschedule_after_completion(self, node: Any) -> None:
        previous_services = self.active_services
        self._advance_service_phase_if_needed()
        while self._reschedule(node):
            pass
        if self.active_services != previous_services:
            for other in self.nodes:
                if other is node:
                    continue
                while self._reschedule(other):
                    pass

        if not self.workqueue and self._can_shutdown_nodes():
            for candidate in self.nodes:
                if not getattr(candidate, "shutting_down", False):
                    self._shutdown_node(candidate)

        self._record_finish_if_done()

        self.log("Scylla resource scheduler waiting items:", len(self.workqueue), "in-use:", self._resource_usage_in_use())

    def _advance_service_phase_if_needed(self) -> None:
        if self._has_service_free_remaining():
            if self.active_services != frozenset():
                if self.active_services:
                    self._ensure_session_services(frozenset())
                    self._record_timeline("service_phase", services=[])
                self.active_services = frozenset()
            return

        remaining_services = self._remaining_service_union()
        if not remaining_services:
            if self.active_services:
                self._ensure_session_services(frozenset())
                self._record_timeline("service_phase", services=[])
            self.active_services = None
            return

        if self.active_services is not None and remaining_services.issubset(self.active_services):
            return

        self.active_services = remaining_services
        self._ensure_session_services(self.active_services)
        self._record_timeline("service_phase", services=sorted(self.active_services))

    def _send_service_phase_lookahead(self) -> None:
        return

    def _has_service_free_remaining(self) -> bool:
        return any(
            item_metadata.services == frozenset()
            for workload in self.assigned_work.values()
            for item_metadata in workload.values()
        ) or frozenset() in self.workqueue_by_services

    def _remaining_service_union(self) -> frozenset[str]:
        services: set[str] = set()
        for workload in self.assigned_work.values():
            for item_metadata in workload.values():
                services.update(item_metadata.services)
        for queued_services in self.workqueue_by_services:
            services.update(queued_services)
        return normalize_session_services(services)

    def _next_workqueue_services(self) -> frozenset[str] | None:
        ready_groups = self._ready_service_groups()
        if not ready_groups:
            return None
        return ready_groups[0]

    def _next_assigned_services(self) -> frozenset[str] | None:
        for node in self.nodes:
            for item_metadata in self.assigned_work[node].values():
                if item_metadata.services != self.active_services:
                    return item_metadata.services
        return None

    def _first_workqueue_key_for_services(self, services: frozenset[str]) -> str | None:
        services_queue = self.workqueue_by_services.get(services)
        if not services_queue:
            return None
        for bucket in services_queue.values():
            if bucket:
                return bucket[0]
        return None

    def _can_shutdown_nodes(self) -> bool:
        return not self._has_pending_outside_active_services()

    def _has_pending_outside_active_services(self) -> bool:
        return any(
            not item_metadata.services.issubset(self.active_services or frozenset())
            for workload in self.assigned_work.values()
            for item_metadata in workload.values()
        )

    def _workqueue_has_services(self, services: frozenset[str]) -> bool:
        return services in self.workqueue_by_services

    def _assigned_has_services(self, services: frozenset[str]) -> bool:
        return any(
            item_metadata.services == services
            for workload in self.assigned_work.values()
            for item_metadata in workload.values()
        )

    def _ensure_session_services(self, services: frozenset[str]) -> None:
        manager = self.service_manager or getattr(self.config, "_scylla_session_service_manager", None)
        if manager is None:
            if services:
                self.log("Session service manager is not configured; required services:", sorted(services))
            return
        self.service_manager = manager
        self._run_async(manager.ensure_services(services))

    def _run_async(self, awaitable: Any) -> None:
        loop = universalasync.get_event_loop()
        if loop.is_running():
            raise RuntimeError("Cannot switch session services while an event loop is running")
        loop.run_until_complete(awaitable)

    def _refresh_node_reservation(self, node: Any) -> None:
        self._set_node_reservation(node, self._resource_reservation_of(self.assigned_work[node]))

    def _refresh_node_backlog_seconds(self, node: Any) -> None:
        self.node_backlog_seconds[node] = sum(self._estimated_duration_seconds(item_metadata) for item_metadata in self.assigned_work[node].values())

    def _set_node_reservation(self, node: Any, reservation: SchedulerResource) -> None:
        previous = self.node_reservations.get(node, SchedulerResource())
        if previous == reservation:
            return
        self.node_reservations[node] = reservation
        self.resource_usage_in_use = SchedulerResource(
            cores=self.resource_usage_in_use.cores - previous.cores + reservation.cores,
            memory_bytes=self.resource_usage_in_use.memory_bytes - previous.memory_bytes + reservation.memory_bytes,
        )

    def _drop_node_reservation(self, node: Any) -> None:
        previous = self.node_reservations.pop(node, SchedulerResource())
        if previous == SchedulerResource():
            return
        self.resource_usage_in_use = SchedulerResource(
            cores=self.resource_usage_in_use.cores - previous.cores,
            memory_bytes=self.resource_usage_in_use.memory_bytes - previous.memory_bytes,
        )

    def _remove_item_from_indexes(self, nodeid: str, item_metadata: ScyllaResourceMetadata) -> None:
        services_queue = self.workqueue_by_services[item_metadata.services]
        bucket_key = _resource_bucket_key(item_metadata.resources)
        bucket = services_queue[bucket_key]
        bucket.remove(nodeid)
        if not bucket:
            del services_queue[bucket_key]
            if not services_queue:
                del self.workqueue_by_services[item_metadata.services]

    def _pending_of(self, workload: OrderedDict[str, ScyllaResourceMetadata]) -> int:
        return len(workload)

    def _shutdown_node(self, node: Any) -> None:
        if getattr(node, "shutting_down", False):
            return
        node.shutdown()
        self._record_timeline("node_shutdown", node=self._node_name(node))

    def _record_finish_if_done(self) -> None:
        if self.timeline.finished or not self.tests_finished:
            return
        self.timeline.finished = True
        self._record_timeline(
            "scheduler_finish",
            active_services=sorted(self.active_services or ()),
            resource_in_use=self._resource_usage_in_use().to_json(),
        )

    def _estimated_start_timestamp(self, duration: float) -> float | None:
        if duration <= 0:
            return None
        return time.time() - duration

    def _record_timeline(self, event: str, **fields: object) -> None:
        self.timeline.record(event, **fields)

    def _check_nodes_have_same_collection(self) -> bool:
        node_collection_items = list(self.registered_collections.items())
        first_node, first_collection = node_collection_items[0]
        same_collection = True
        for node, collection in node_collection_items[1:]:
            if collection == first_collection:
                continue
            same_collection = False
            msg = (
                f"Different tests collected between {self._node_name(first_node)} "
                f"and {self._node_name(node)}"
            )
            self.log(msg)
            if hasattr(self.config, "hook"):
                rep = pytest.CollectReport(nodeid=self._node_name(node), outcome="failed", longrepr=msg, result=[])
                self.config.hook.pytest_collectreport(report=rep)
        return same_collection

    def _node_name(self, node: Any) -> str:
        gateway = getattr(node, "gateway", None)
        return str(getattr(gateway, "id", node))
