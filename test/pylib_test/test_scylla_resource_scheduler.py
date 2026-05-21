#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from __future__ import annotations

import json
import pathlib
from collections import OrderedDict
from datetime import datetime
from types import SimpleNamespace

import pytest

from test import HOST_ID
import test.pylib.scylla_resource_scheduler as scheduler_module
from test.pylib.db.model import Metric, SystemResourceMetric, Test as TestRow
from test.pylib.db.writer import DEFAULT_DB_NAME, METRICS_TABLE, SYSTEM_RESOURCE_METRICS_TABLE, TESTS_TABLE, SQLiteWriter
from test.pylib.scylla_resource_scheduler import (
    ScyllaResourceMetadata,
    ScyllaResourceScheduler,
    SchedulerResource,
    archive_historical_resource_db_for_root,
    historical_resource_archive_paths_for_root,
    historical_resource_db_paths_for_root,
    load_historical_resource_history,
    plan_items_by_service_phase,
    queue_items_from_collection,
    scylla_resource_budget_failure,
    scylla_resource_metadata_for_item,
    scylla_resource_timeline_path,
)
from test.pylib.scylla_resources import ScyllaResourceLimit, scylla_resource_limit_from_markers, scylla_resource_runtime_limit
from test.pylib.session_services import SessionServiceManager


GIB = 1024 ** 3


class FakeMarkedNode:
    def __init__(self, *marks) -> None:
        self.marks = {mark.name: mark for mark in marks}

    def get_closest_marker(self, name: str):
        return self.marks.get(name)


class FakeItem(FakeMarkedNode):
    def __init__(self, nodeid: str, *marks, config=None) -> None:
        super().__init__(*marks)
        self.nodeid = nodeid
        self.path = pathlib.Path(nodeid.split("::", 1)[0])
        self.config = config


class IterOnlyMarkedNode:
    def __init__(self, *marks) -> None:
        self._marks = {mark.name: mark for mark in marks}

    def get_closest_marker(self, name: str):
        return None

    def iter_markers(self, name: str | None = None):
        if name is None:
            values = self._marks.values()
        else:
            mark = self._marks.get(name)
            values = () if mark is None else (mark,)
        return iter(values)


class FakeSuiteConfig:
    def __init__(self, name: str, cfg: dict, path: str | None = None) -> None:
        self.name = name
        self.cfg = cfg
        self.path = pathlib.Path(path or f"test/{name}")


class FakeConfig:
    def __init__(self, cpus: int = 2, memory: str = "8G", tmpdir: pathlib.Path | None = None) -> None:
        self.options = {
            "--scylla-resource-cpus": cpus,
            "--scylla-resource-memory": memory,
            "--tmpdir": str(tmpdir) if tmpdir is not None else None,
        }

    def getoption(self, name: str):
        return self.options.get(name)


class FakeWorker:
    def __init__(self, name: str) -> None:
        self.gateway = SimpleNamespace(id=name)
        self.shutting_down = False
        self.sent: list[list[int]] = []

    def send_runtest_some(self, indexes: list[int]) -> None:
        self.sent.append(indexes)

    def shutdown(self) -> None:
        self.shutting_down = True


class IndexlessCollection:
    def __init__(self, values: list[str]) -> None:
        self.values = list(values)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, IndexlessCollection):
            return self.values == other.values
        if isinstance(other, list):
            return self.values == other
        return NotImplemented

    def __getitem__(self, index: int) -> str:
        return self.values[index]

    def __len__(self) -> int:
        return len(self.values)

    def __iter__(self):
        return iter(self.values)

    def index(self, value: str) -> int:
        raise AssertionError("scheduler should use cached worker indexes")


class FakeServiceManager:
    def __init__(self) -> None:
        self.transitions: list[frozenset[str]] = []

    async def ensure_services(self, services) -> None:
        self.transitions.append(frozenset(services))


class FailingSessionServiceManager(SessionServiceManager):
    def __init__(self, fail_on: str) -> None:
        self.active_services = frozenset()
        self.fail_on = fail_on
        self.events: list[tuple[str, str | tuple[str, ...]]] = []

    async def _start_service(self, service: str) -> None:
        self.events.append(("start", service))
        if service == self.fail_on:
            raise RuntimeError("boom")

    async def _stop_service(self, service: str) -> None:
        self.events.append(("stop", service))
        if service == self.fail_on:
            raise RuntimeError("boom")

    def _write_environment(self) -> None:
        self.events.append(("write", tuple(sorted(self.active_services))))


class PartiallyFailingSessionServiceManager(SessionServiceManager):
    def __init__(self, fail_on: str) -> None:
        self.active_services = frozenset({"ldap", "s3"})
        self.fail_on = fail_on
        self.events: list[tuple[str, str | tuple[str, ...]]] = []
        self.running_services = set(self.active_services)

    async def _start_service(self, service: str) -> None:
        self.events.append(("start", service))
        self.running_services.add(service)

    async def _stop_service(self, service: str) -> None:
        self.events.append(("stop", service))
        if service == self.fail_on:
            raise RuntimeError("boom")
        self.running_services.discard(service)

    def _write_environment(self) -> None:
        self.events.append(("write", tuple(sorted(self.active_services))))


def mark(name: str, *args, **kwargs):
    return getattr(pytest.mark, name)(*args, **kwargs).mark


def make_scheduler(
        collection: list[str],
        metadata: dict[str, ScyllaResourceMetadata],
        cpus: int = 2,
        service_manager=None,
        tmpdir: pathlib.Path | None = None) -> tuple[ScyllaResourceScheduler, FakeWorker, FakeWorker]:
    scheduler = ScyllaResourceScheduler(FakeConfig(cpus=cpus, tmpdir=tmpdir), numnodes=2, resource_metadata=metadata, service_manager=service_manager)
    node1 = FakeWorker("gw0")
    node2 = FakeWorker("gw1")
    scheduler.add_node(node1)
    scheduler.add_node(node2)
    scheduler.add_node_collection(node1, collection)
    scheduler.add_node_collection(node2, collection)
    return scheduler, node1, node2


def make_single_node_scheduler(
        collection: list[str],
        metadata: dict[str, ScyllaResourceMetadata],
        cpus: int = 2,
        tmpdir: pathlib.Path | None = None) -> tuple[ScyllaResourceScheduler, FakeWorker]:
    scheduler = ScyllaResourceScheduler(FakeConfig(cpus=cpus, tmpdir=tmpdir), numnodes=1, resource_metadata=metadata)
    node = FakeWorker("gw0")
    scheduler.add_node(node)
    scheduler.add_node_collection(node, collection)
    return scheduler, node


def sent_indexes(worker: FakeWorker) -> list[int]:
    return [index for batch in worker.sent for index in batch]


def write_test_sample(
        writer: SQLiteWriter,
        directory: str,
        mode: str,
        test_name: str,
        run_id: int,
        *,
        time_taken: float,
        usage_sec: float,
        memory_peak: int,
        host_id: str = HOST_ID) -> None:
    test_id = writer.write_row(
        TestRow(host_id=host_id, architecture="x86_64", directory=directory, mode=mode, run_id=run_id, test_name=test_name),
        TESTS_TABLE,
    )
    writer.write_row(
        Metric(
            test_id=test_id,
            host_id=host_id,
            memory_peak=memory_peak,
            success=True,
            time_taken=time_taken,
            usage_sec=usage_sec,
        ),
        METRICS_TABLE,
    )


def write_system_sample(writer: SQLiteWriter, *, cpu: float, memory: float, host_id: str = HOST_ID) -> None:
    writer.write_row(
        SystemResourceMetric(host_id=host_id, cpu=cpu, memory=memory, timestamp=datetime.now()),
        SYSTEM_RESOURCE_METRICS_TABLE,
    )


def test_scylla_cores_marker_parsing() -> None:
    limit = scylla_resource_limit_from_markers(FakeMarkedNode(mark("scylla_cores", 3)))

    assert limit.cores == 3
    assert limit.memory_bytes is None
    assert not limit.allow_memory_override
    assert limit.enforce_usage_limits
    assert limit.as_manager_kwargs() == {"cores": 3, "memory_bytes": None, "allow_memory_override": False, "enforce_usage_limits": True}


def test_scylla_cores_unbounded_marker_parsing() -> None:
    limit = scylla_resource_limit_from_markers(FakeMarkedNode(mark("scylla_cores", 3, unbounded=True)))

    assert limit.cores == 3
    assert limit.memory_bytes is None
    assert not limit.allow_memory_override
    assert not limit.enforce_usage_limits


def test_scylla_resources_marker_parsing() -> None:
    limit = scylla_resource_limit_from_markers(FakeMarkedNode(mark("scylla_resources", cpu=2, mem="2G")))

    assert limit.cores == 2
    assert limit.memory_bytes == 2 * GIB
    assert limit.allow_memory_override
    assert limit.enforce_usage_limits


def test_scylla_resources_unbounded_marker_parsing() -> None:
    limit = scylla_resource_limit_from_markers(FakeMarkedNode(mark("scylla_resources", cpu=8, mem="12G", unbounded=True)))

    assert limit.cores == 8
    assert limit.memory_bytes == 12 * GIB
    assert limit.allow_memory_override
    assert not limit.enforce_usage_limits
    assert limit.as_manager_kwargs() == {
        "cores": 8,
        "memory_bytes": 12 * GIB,
        "allow_memory_override": True,
        "enforce_usage_limits": False,
    }


def test_scylla_resources_marker_lookup_uses_iter_markers() -> None:
    # Regression test for module-level pytestmark visibility in the scheduler path.
    limit = scylla_resource_limit_from_markers(IterOnlyMarkedNode(mark("scylla_resources", cpu=6, mem="3G")))

    assert limit == ScyllaResourceLimit(cores=6, memory_bytes=3 * GIB, allow_memory_override=True, enforce_usage_limits=True)


def test_unmarked_tests_get_no_runtime_resource_limit() -> None:
    limit = scylla_resource_runtime_limit(FakeMarkedNode())

    assert limit is None


def test_allow_unannotated_provisioning_marker_gets_unbounded_runtime_limit() -> None:
    limit = scylla_resource_runtime_limit(FakeMarkedNode(mark("allow_unannotated_scylla_provisioning")))

    assert limit == ScyllaResourceLimit(allow_memory_override=False, enforce_usage_limits=False)


def test_allow_unannotated_provisioning_marker_rejects_explicit_resource_marker() -> None:
    with pytest.raises(pytest.UsageError, match="allow_unannotated_scylla_provisioning"):
        scylla_resource_runtime_limit(
            FakeMarkedNode(
                mark("allow_unannotated_scylla_provisioning"),
                mark("scylla_resources", cpu=6, mem="3G"),
            )
        )


def test_explicit_marker_overrides_default_runtime_resource_limit() -> None:
    limit = scylla_resource_runtime_limit(FakeMarkedNode(mark("scylla_resources", cpu=6, mem="3G")))

    assert limit == ScyllaResourceLimit(cores=6, memory_bytes=3 * GIB, allow_memory_override=True, enforce_usage_limits=True)


def test_unbounded_resources_still_charge_scheduler() -> None:
    cluster_suite = FakeSuiteConfig("cluster", {"type": "Topology"})

    metadata = scylla_resource_metadata_for_item(
        FakeItem("test/cluster/test_a.py::test_one", mark("scylla_resources", cpu=8, mem="12G", unbounded=True)),
        cluster_suite,
        "dev",
        False,
    )

    assert metadata.resources == SchedulerResource(cores=8, memory_bytes=12 * GIB)


def test_scylla_resource_markers_are_mutually_exclusive() -> None:
    with pytest.raises(pytest.UsageError, match="Use either"):
        scylla_resource_limit_from_markers(FakeMarkedNode(mark("scylla_cores", 1), mark("scylla_resources", 1, "1G")))


def test_default_resource_classification() -> None:
    cqlpy_suite = FakeSuiteConfig("cqlpy", {"type": "Python"})
    cluster_suite = FakeSuiteConfig("cluster", {"type": "Topology"})

    cqlpy_metadata = scylla_resource_metadata_for_item(FakeItem("test/cqlpy/test_a.py::test_one"), cqlpy_suite, "dev", False)
    cluster_metadata = scylla_resource_metadata_for_item(FakeItem("test/cluster/test_a.py::test_one"), cluster_suite, "dev", False)
    single_node_metadata = scylla_resource_metadata_for_item(
        FakeItem("test/cluster/dtest/auth_test.py::test_one", mark("single_node")),
        cluster_suite,
        "dev",
        False,
    )
    without_scylla_metadata = scylla_resource_metadata_for_item(FakeItem("test/cqlpy/test_a.py::test_two", mark("without_scylla")), cqlpy_suite, "dev", False)

    assert cqlpy_metadata.resources == SchedulerResource(cores=1, memory_bytes=GIB)
    assert cqlpy_metadata.group_key == "module:dev:test/cqlpy/test_a.py"
    assert cluster_metadata.resources == SchedulerResource(cores=0.75, memory_bytes=3 * GIB)
    assert cluster_metadata.group_key == "item:test/cluster/test_a.py::test_one"
    assert single_node_metadata.resources == SchedulerResource(cores=0.25, memory_bytes=GIB)
    assert single_node_metadata.group_key == "item:test/cluster/dtest/auth_test.py::test_one"
    assert without_scylla_metadata.resources == SchedulerResource()


def test_cluster_profile_metadata_remains_itemized() -> None:
    cluster_suite = FakeSuiteConfig("cluster", {"type": "Topology"})
    first = scylla_resource_metadata_for_item(FakeItem("test/cluster/test_a.py::test_one", mark("scylla_cluster", nodes=2)), cluster_suite, "dev", False)
    second = scylla_resource_metadata_for_item(FakeItem("test/cluster/test_b.py::test_two", mark("scylla_cluster", nodes=2)), cluster_suite, "dev", False)

    assert first.cluster_profile_key is not None
    assert first.cluster_profile_key == second.cluster_profile_key
    assert first.cluster_reuse == "sequential"
    assert first.group_key == f"cluster:{first.cluster_profile_key}"
    assert first.resources == SchedulerResource(cores=0.5, memory_bytes=2 * GIB)

    workqueue = queue_items_from_collection(["a", "b"], {"a": first, "b": second})

    assert list(workqueue) == ["a", "b"]
    assert workqueue["a"].cluster_profile_key == first.cluster_profile_key
    assert workqueue["b"].cluster_profile_key == second.cluster_profile_key


def test_cluster_profile_cpu_scales_with_smp() -> None:
    cluster_suite = FakeSuiteConfig("cluster", {"type": "Topology"})

    metadata = scylla_resource_metadata_for_item(
        FakeItem(
            "test/cluster/test_profile.py::test_scaled",
            mark("scylla_cluster", nodes=[
                {"cmdline": ["--smp", "1"]},
                {"cmdline": ["--smp", "4"]},
            ]),
        ),
        cluster_suite,
        "dev",
        False,
    )

    assert metadata.resources == SchedulerResource(cores=0.625, memory_bytes=2 * GIB)


def test_cluster_profile_service_phase_stays_itemized() -> None:
    metadata = {
        "plain": ScyllaResourceMetadata("cluster:key", SchedulerResource(cores=1, memory_bytes=GIB), cluster_profile_key="key"),
        "s3": ScyllaResourceMetadata("cluster:key", SchedulerResource(cores=1, memory_bytes=GIB), frozenset({"s3"}), cluster_profile_key="key"),
    }

    workqueue = plan_items_by_service_phase(queue_items_from_collection(["plain", "s3"], metadata))

    assert list(workqueue) == ["plain", "s3"]
    assert [item_metadata.services for item_metadata in workqueue.values()] == [frozenset(), frozenset({"s3"})]


def test_cluster_profile_items_stay_on_same_worker_in_order() -> None:
    collection = ["a", "b", "c"]
    metadata = {
        nodeid: ScyllaResourceMetadata(
            "cluster:key",
            SchedulerResource(cores=1, memory_bytes=GIB),
            cluster_profile_key="key",
            cluster_reuse="sequential",
        )
        for nodeid in collection
    }

    scheduler, node1, node2 = make_scheduler(collection, metadata, cpus=2)

    scheduler.schedule()

    assert sent_indexes(node1) == [0, 1, 2]
    assert node2.sent == []
    assert node1.shutting_down is True


def test_scheduler_sends_shutdown_when_only_assigned_work_remains() -> None:
    collection = ["a", "b", "c"]
    metadata = {
        nodeid: ScyllaResourceMetadata(
            f"item:{nodeid}",
            SchedulerResource(cores=1, memory_bytes=GIB),
        )
        for nodeid in collection
    }

    scheduler, node = make_single_node_scheduler(collection, metadata, cpus=4)

    scheduler.schedule()

    assert node.shutting_down is True
    assert sent_indexes(node) == [0, 1, 2]
    assert len(scheduler.assigned_work[node]) == 3


def test_service_requirements_from_marker_and_suite_config() -> None:
    marked_metadata = scylla_resource_metadata_for_item(
        FakeItem("test/cqlpy/test_tools.py::test_s3", mark("requires_service", "s3")),
        FakeSuiteConfig("cqlpy", {"type": "Python"}),
        "dev",
        False,
    )
    suite_metadata = scylla_resource_metadata_for_item(
        FakeItem("test/boost/s3_test.cc::case"),
        FakeSuiteConfig("boost", {"requires_services": {"s3_test": ["s3"]}}, path="test/boost"),
        "dev",
        False,
    )

    assert marked_metadata.services == frozenset({"s3"})
    assert suite_metadata.services == frozenset({"s3"})


def test_debug_mode_charges_more_cpu() -> None:
    cqlpy_suite = FakeSuiteConfig("cqlpy", {"type": "Python"})

    metadata = scylla_resource_metadata_for_item(FakeItem("test/cqlpy/test_a.py::test_one"), cqlpy_suite, "debug", True)

    assert metadata.resources == SchedulerResource(cores=1.5, memory_bytes=GIB)


def test_non_cluster_tests_remain_itemized_even_with_shared_module_group_key() -> None:
    metadata = {
        "test/cqlpy/test_a.py::test_one.dev.1": ScyllaResourceMetadata(
            "module:dev:test/cqlpy/test_a.py",
            SchedulerResource(cores=1, memory_bytes=GIB),
        ),
        "test/cqlpy/test_a.py::test_two.dev.1": ScyllaResourceMetadata(
            "module:dev:test/cqlpy/test_a.py",
            SchedulerResource(cores=1, memory_bytes=GIB),
        ),
    }

    workqueue = queue_items_from_collection(list(metadata), metadata)

    assert list(workqueue) == list(metadata)


def test_item_queue_preserves_per_item_services() -> None:
    metadata = {
        "test/cqlpy/test_a.py::test_one.dev.1": ScyllaResourceMetadata(
            "module:dev:test/cqlpy/test_a.py",
            SchedulerResource(cores=1, memory_bytes=GIB),
        ),
        "test/cqlpy/test_a.py::test_two.dev.1": ScyllaResourceMetadata(
            "module:dev:test/cqlpy/test_a.py",
            SchedulerResource(cores=1, memory_bytes=GIB),
            frozenset({"s3"}),
        ),
    }

    workqueue = queue_items_from_collection(list(metadata), metadata)

    assert workqueue["test/cqlpy/test_a.py::test_one.dev.1"].services == frozenset()
    assert workqueue["test/cqlpy/test_a.py::test_two.dev.1"].services == frozenset({"s3"})


# Regression test for Scylla-backed non-cluster suites that were previously charged as free.
@pytest.mark.parametrize(
    ("suite_name", "nodeid", "expected_services"),
    [
        ("boost", "test/boost/s3_test.cc::test_one", frozenset({"s3"})),
        ("nodetool", "test/nodetool/test_compact.py::test_all_keyspaces", frozenset()),
        ("raft", "test/raft/raft_server_test.cc::test_one", frozenset()),
        ("vector_search", "test/vector_search/vector_store_client_test.cc::test_one", frozenset()),
        ("ldap", "test/ldap/role_manager_test.cc::test_one", frozenset({"ldap"})),
    ],
)
def test_non_cluster_scylla_backed_suites_are_charged_like_scylla_backed_tests(
    suite_name: str,
    nodeid: str,
    expected_services: frozenset[str],
) -> None:
    suite_cfg = {
        "type": "Python" if suite_name == "ldap" else "C++",
        "extra_scylla_cmdline_options": ["--reactor-backend=linux-aio"],
    }
    if suite_name == "boost":
        suite_cfg["requires_services"] = {"s3_test": ["s3"]}
    if suite_name == "ldap":
        suite_cfg["requires_services"] = ["ldap"]

    metadata = scylla_resource_metadata_for_item(FakeItem(nodeid), FakeSuiteConfig(suite_name, suite_cfg), "dev", False)

    assert metadata.group_key == f"module:dev:{nodeid.split('::', 1)[0]}"
    assert metadata.resources == SchedulerResource(cores=1, memory_bytes=GIB)
    assert metadata.services == expected_services


def test_scheduler_only_dispatches_fitting_work() -> None:
    collection = ["test_a", "test_b"]
    metadata = {
        "test_a": ScyllaResourceMetadata("test_a", SchedulerResource(cores=2, memory_bytes=GIB)),
        "test_b": ScyllaResourceMetadata("test_b", SchedulerResource(cores=1, memory_bytes=GIB)),
    }
    scheduler, node1, node2 = make_scheduler(collection, metadata, cpus=2)

    scheduler.schedule()

    assert node1.sent == [[0, 1]]
    assert node2.sent == []
    assert node1.shutting_down is True
    assert node2.shutting_down is True


def test_scheduler_releases_resources_after_completion() -> None:
    collection = ["test_a", "test_b", "test_c"]
    metadata = {
        "test_a": ScyllaResourceMetadata("test_a", SchedulerResource(cores=2, memory_bytes=GIB)),
        "test_b": ScyllaResourceMetadata("test_b", SchedulerResource(cores=1, memory_bytes=GIB)),
        "test_c": ScyllaResourceMetadata("test_c", SchedulerResource(cores=1, memory_bytes=GIB)),
    }
    scheduler, node1, node2 = make_scheduler(collection, metadata, cpus=2)
    scheduler.schedule()

    assert node1.sent == [[0, 1, 2]]

    scheduler.mark_test_complete(node1, 0)

    assert scheduler._resource_usage_in_use() == SchedulerResource(cores=1, memory_bytes=GIB)


def test_scheduler_sends_shutdown_for_single_dispatched_item() -> None:
    collection = ["a"]
    metadata = {
        nodeid: ScyllaResourceMetadata(
            nodeid,
            SchedulerResource(cores=1, memory_bytes=GIB),
        )
        for nodeid in collection
    }

    scheduler, node = make_single_node_scheduler(collection, metadata, cpus=4)

    scheduler.schedule()

    assert node.sent == [[0]]
    assert node.shutting_down is True
    assert list(scheduler.assigned_work[node]) == ["a"]


def test_scheduler_tracks_resource_usage_incrementally() -> None:
    collection = ["test_a", "test_b"]
    metadata = {
        "test_a": ScyllaResourceMetadata("test_a", SchedulerResource(cores=2, memory_bytes=GIB)),
        "test_b": ScyllaResourceMetadata("test_b", SchedulerResource(cores=1, memory_bytes=GIB)),
    }
    scheduler, node1, node2 = make_scheduler(collection, metadata, cpus=2)

    assert scheduler._resource_usage_in_use() == SchedulerResource()

    scheduler.schedule()

    assert scheduler._resource_usage_in_use() == SchedulerResource(cores=2, memory_bytes=GIB)

    scheduler.mark_test_complete(node1, 0)

    assert scheduler._resource_usage_in_use() == SchedulerResource(cores=1, memory_bytes=GIB)

    scheduler.mark_test_complete(node1, 1)

    assert scheduler._resource_usage_in_use() == SchedulerResource()
    assert node2.sent == []


def test_scheduler_dispatch_uses_cached_worker_indexes() -> None:
    collection = ["test_a", "test_b"]
    metadata = {
        "test_a": ScyllaResourceMetadata("test_a", SchedulerResource(cores=1, memory_bytes=GIB)),
        "test_b": ScyllaResourceMetadata("test_b", SchedulerResource(cores=1, memory_bytes=GIB)),
    }
    scheduler, node1, node2 = make_scheduler(collection, metadata, cpus=2)
    scheduler.registered_collections[node1] = IndexlessCollection(collection)
    scheduler.registered_collections[node2] = IndexlessCollection(collection)

    scheduler.schedule()

    assert node1.sent == [[0, 1]]
    assert node2.sent == []


def test_scheduler_prioritizes_lingering_low_cpu_items() -> None:
    collection = ["test_fast", "test_linger"]
    metadata = {
        "test_fast": ScyllaResourceMetadata(
            "test_fast",
            SchedulerResource(cores=1, memory_bytes=GIB),
            estimated_duration_seconds=1.0,
            observed_cpu_cores=1.0,
        ),
        "test_linger": ScyllaResourceMetadata(
            "test_linger",
            SchedulerResource(cores=0.5, memory_bytes=GIB),
            estimated_duration_seconds=20.0,
            observed_cpu_cores=0.02,
        ),
    }
    scheduler = ScyllaResourceScheduler(FakeConfig(cpus=2), numnodes=1, resource_metadata=metadata)
    node = FakeWorker("gw0")
    scheduler.add_node(node)
    scheduler.add_node_collection(node, collection)

    scheduler.schedule()

    assert node.sent == [[1, 0]]


def test_historical_resource_db_archive_roundtrip(tmp_path: pathlib.Path) -> None:
    db_path = tmp_path / DEFAULT_DB_NAME
    writer = SQLiteWriter(db_path)
    write_test_sample(
        writer,
        "cluster",
        "dev",
        "dtest/auth_test",
        1,
        time_taken=100.0,
        usage_sec=2.0,
        memory_peak=128 * 1024 ** 2,
    )
    write_system_sample(writer, cpu=33.0, memory=44.0)
    writer._connection.close()

    archive_path = archive_historical_resource_db_for_root(tmp_path)

    assert archive_path is not None
    assert not db_path.exists()
    assert historical_resource_db_paths_for_root(tmp_path) == [archive_path]

    history = load_historical_resource_history([archive_path])

    assert history.tests[("cluster", "dev", "dtest/auth_test")].sample_count == 1
    assert history.system_resource_metrics[HOST_ID].sample_count == 1
    assert history.system_resource_metrics[HOST_ID].avg_cpu == 33.0


def test_historical_resource_archive_paths_keep_newest_limit(tmp_path: pathlib.Path) -> None:
    history_dir = tmp_path / "history"
    history_dir.mkdir()
    active_db = tmp_path / DEFAULT_DB_NAME
    active_db.write_text("active", encoding="utf-8")

    archive_names = [
        "20260101T000001.000001Z-host.sqlite",
        "20260101T000002.000001Z-host.sqlite",
        "20260101T000003.000001Z-host.sqlite",
    ]
    for name in archive_names:
        (history_dir / name).write_text(name, encoding="utf-8")

    assert [path.name for path in historical_resource_archive_paths_for_root(tmp_path, limit=2)] == archive_names[-1:-3:-1]
    assert historical_resource_db_paths_for_root(tmp_path, limit=2)[0] == active_db


def test_historical_resource_history_loads_active_and_archived_dbs(tmp_path: pathlib.Path) -> None:
    history_dir = tmp_path / "history"
    history_dir.mkdir()

    active_db = tmp_path / DEFAULT_DB_NAME
    archive_db_1 = history_dir / "20260101T000002.000001Z-host.sqlite"
    archive_db_2 = history_dir / "20260101T000001.000001Z-host.sqlite"

    active_writer = SQLiteWriter(active_db)
    archive_writer_1 = SQLiteWriter(archive_db_1)
    archive_writer_2 = SQLiteWriter(archive_db_2)

    write_test_sample(
        active_writer,
        "cluster",
        "dev",
        "dtest/auth_test",
        1,
        time_taken=100.0,
        usage_sec=10.0,
        memory_peak=128 * 1024 ** 2,
    )
    write_system_sample(active_writer, cpu=20.0, memory=30.0)
    write_test_sample(
        archive_writer_1,
        "cluster",
        "dev",
        "dtest/auth_test",
        2,
        time_taken=100.0,
        usage_sec=20.0,
        memory_peak=192 * 1024 ** 2,
    )
    write_system_sample(archive_writer_1, cpu=40.0, memory=50.0)
    write_test_sample(
        archive_writer_2,
        "cluster",
        "dev",
        "dtest/other_test",
        3,
        time_taken=10.0,
        usage_sec=8.0,
        memory_peak=64 * 1024 ** 2,
    )
    write_system_sample(archive_writer_2, cpu=60.0, memory=70.0)

    paths = historical_resource_db_paths_for_root(tmp_path)
    history = load_historical_resource_history(paths)

    assert paths[0] == active_db
    assert [path.name for path in paths[1:]] == [archive_db_1.name, archive_db_2.name]
    assert history.tests[("cluster", "dev", "dtest/auth_test")].sample_count == 2
    assert history.tests[("cluster", "dev", "dtest/other_test")].sample_count == 1
    assert history.system_resource_metrics[HOST_ID].sample_count == 3
    assert history.system_resource_metrics[HOST_ID].avg_cpu == 40.0


@pytest.mark.parametrize(
    ("sample_count", "usage_sec", "time_taken", "expected_class"),
    [
        (20, 8.0, 40.0, "wait_heavy"),
        (20, 75.0, 100.0, "cpu_heavy"),
        (19, 8.0, 40.0, "balanced"),
    ],
)
def test_suite_class_derivation_thresholds(
    tmp_path: pathlib.Path,
    sample_count: int,
    usage_sec: float,
    time_taken: float,
    expected_class: str,
) -> None:
    db_path = tmp_path / DEFAULT_DB_NAME
    writer = SQLiteWriter(db_path)

    for run_id in range(sample_count):
        write_test_sample(
            writer,
            "cluster",
            "dev",
            "dtest/auth_test",
            run_id,
            time_taken=time_taken,
            usage_sec=usage_sec,
            memory_peak=128 * 1024 ** 2,
        )

    history = load_historical_resource_history([db_path])

    assert history.suites[("cluster", "dev")].sample_count == sample_count
    assert history.suites[("cluster", "dev")].suite_class == expected_class


def test_tail_mode_activation_threshold(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(scheduler_module, "TAIL_MODE_MIN_REMAINING_ITEMS", 2)
    monkeypatch.setattr(scheduler_module, "TAIL_MODE_FRACTION", 100)

    scheduler = ScyllaResourceScheduler(FakeConfig(cpus=2), numnodes=1, resource_metadata={})
    scheduler.collection = ["test_a", "test_b", "test_c"]
    scheduler.total_items = 3
    scheduler.workqueue = OrderedDict(
        (nodeid, ScyllaResourceMetadata(nodeid, SchedulerResource(cores=1, memory_bytes=GIB)))
        for nodeid in scheduler.collection
    )

    assert not scheduler._tail_mode()

    scheduler.workqueue.popitem(last=False)

    assert scheduler._tail_mode()


def test_tail_mode_reorders_long_predictions_after_activation(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(scheduler_module, "TAIL_MODE_MIN_REMAINING_ITEMS", 2)
    monkeypatch.setattr(scheduler_module, "TAIL_MODE_FRACTION", 100)
    monkeypatch.setattr(scheduler_module, "PREFETCH_MIN_ITEMS", 1)

    collection = ["test_short", "test_other", "test_long"]
    metadata = {
        "test_short": ScyllaResourceMetadata(
            "test_short",
            SchedulerResource(cores=2, memory_bytes=GIB),
            estimated_duration_seconds=5.0,
            observed_cpu_cores=1.0,
        ),
        "test_other": ScyllaResourceMetadata(
            "test_other",
            SchedulerResource(cores=2, memory_bytes=GIB),
            estimated_duration_seconds=6.0,
            observed_cpu_cores=1.0,
        ),
        "test_long": ScyllaResourceMetadata(
            "test_long",
            SchedulerResource(cores=2, memory_bytes=GIB),
            estimated_duration_seconds=50.0,
            observed_cpu_cores=1.0,
        ),
    }
    scheduler = ScyllaResourceScheduler(FakeConfig(cpus=2), numnodes=1, resource_metadata=metadata)
    node = FakeWorker("gw0")
    scheduler.add_node(node)
    scheduler.add_node_collection(node, collection)

    scheduler.schedule()

    assert node.sent == [[0]]

    scheduler.mark_test_complete(node, 0)

    assert node.sent == [[0], [2]]


def test_wait_heavy_tail_prefetch_behavior(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(scheduler_module, "TAIL_MODE_MIN_REMAINING_ITEMS", 4)
    monkeypatch.setattr(scheduler_module, "TAIL_MODE_FRACTION", 100)
    monkeypatch.setattr(scheduler_module, "PREFETCH_MAX_ITEMS", 2)
    monkeypatch.setattr(scheduler_module, "TAIL_WAIT_HEAVY_PREFETCH_MAX_ITEMS", 3)
    monkeypatch.setattr(scheduler_module, "PREFETCH_TARGET_SECONDS", 5.0)
    monkeypatch.setattr(scheduler_module, "TAIL_WAIT_HEAVY_PREFETCH_TARGET_SECONDS", 12.0)

    collection = ["test_a", "test_b", "test_c"]

    balanced_metadata = {
        nodeid: ScyllaResourceMetadata(
            nodeid,
            SchedulerResource(cores=0.5, memory_bytes=GIB),
            estimated_duration_seconds=4.0,
            suite_class="balanced",
        )
        for nodeid in collection
    }
    balanced_scheduler = ScyllaResourceScheduler(FakeConfig(cpus=2), numnodes=1, resource_metadata=balanced_metadata)
    balanced_node = FakeWorker("gw0")
    balanced_scheduler.add_node(balanced_node)
    balanced_scheduler.add_node_collection(balanced_node, collection)

    balanced_scheduler.schedule()

    assert balanced_node.sent == [[0, 1]]

    wait_heavy_metadata = {
        nodeid: ScyllaResourceMetadata(
            nodeid,
            SchedulerResource(cores=0.5, memory_bytes=GIB),
            estimated_duration_seconds=4.0,
            suite_class="wait_heavy",
        )
        for nodeid in collection
    }
    wait_heavy_scheduler = ScyllaResourceScheduler(FakeConfig(cpus=2), numnodes=1, resource_metadata=wait_heavy_metadata)
    wait_heavy_node = FakeWorker("gw1")
    wait_heavy_scheduler.add_node(wait_heavy_node)
    wait_heavy_scheduler.add_node_collection(wait_heavy_node, collection)

    wait_heavy_scheduler.schedule()

    assert wait_heavy_node.sent == [[0, 1, 2]]


def test_scheduler_writes_jsonl_timeline(tmp_path: pathlib.Path) -> None:
    collection = ["test_a"]
    metadata = {
        "test_a": ScyllaResourceMetadata("test_a", SchedulerResource(cores=1, memory_bytes=GIB), frozenset({"s3"})),
    }
    service_manager = FakeServiceManager()
    scheduler, node1, node2 = make_scheduler(collection, metadata, service_manager=service_manager, tmpdir=tmp_path)

    scheduler.schedule()
    node = node1 if node1.sent else node2
    scheduler.mark_test_complete(node, 0, duration=1.25)

    path = scylla_resource_timeline_path(scheduler.config)
    assert path is not None
    events = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]

    assert [event["event"] for event in events] == [
        "scheduler_start",
        "node_added",
        "node_added",
        "collection_registered",
        "collection_registered",
        "collection_ready",
        "service_phase",
        "dispatch",
        "node_shutdown",
        "node_shutdown",
        "test_complete",
        "item_complete",
        "service_phase",
        "scheduler_finish",
    ]

    dispatch = next(event for event in events if event["event"] == "dispatch")
    assert dispatch["nodeids"] == ["test_a"]
    assert dispatch["resources"] == {"cores": 1, "memory_bytes": GIB}
    assert dispatch["services"] == ["s3"]

    completion = next(event for event in events if event["event"] == "test_complete")
    assert completion["nodeid"] == "test_a"
    assert completion["duration_seconds"] == 1.25
    assert completion["estimated_start_timestamp"] is not None


def test_scheduler_runs_service_phase_after_service_free_phase() -> None:
    collection = ["test_plain", "test_s3_a", "test_s3_b"]
    metadata = {
        "test_plain": ScyllaResourceMetadata("test_plain", SchedulerResource(cores=1, memory_bytes=GIB)),
        "test_s3_a": ScyllaResourceMetadata("test_s3_a", SchedulerResource(cores=1, memory_bytes=GIB), frozenset({"s3"})),
        "test_s3_b": ScyllaResourceMetadata("test_s3_b", SchedulerResource(cores=1, memory_bytes=GIB), frozenset({"s3"})),
    }
    service_manager = FakeServiceManager()
    scheduler, node1, node2 = make_scheduler(collection, metadata, cpus=2, service_manager=service_manager)

    scheduler.schedule()

    assert service_manager.transitions == []
    assert node1.sent == [[0]] or node2.sent == [[0]]

    def complete(index: int) -> None:
        node = node1 if any(index in sent for sent in node1.sent) else node2
        scheduler.mark_test_complete(node, index)

    complete(0)

    assert service_manager.transitions[0] == frozenset({"s3"})
    assert [1, 2] in node1.sent or [1, 2] in node2.sent

    complete(1)
    complete(2)

    assert service_manager.transitions[-1] == frozenset()


def test_scheduler_shutdown_unblocks_last_service_free_item_before_phase_shift() -> None:
    collection = ["test_plain_a", "test_plain_b", "test_s3"]
    metadata = {
        "test_plain_a": ScyllaResourceMetadata("test_plain_a", SchedulerResource(cores=1, memory_bytes=GIB)),
        "test_plain_b": ScyllaResourceMetadata("test_plain_b", SchedulerResource(cores=1, memory_bytes=GIB)),
        "test_s3": ScyllaResourceMetadata("test_s3", SchedulerResource(cores=1, memory_bytes=GIB), frozenset({"s3"})),
    }
    service_manager = FakeServiceManager()
    scheduler, node1, node2 = make_scheduler(collection, metadata, cpus=2, service_manager=service_manager)

    scheduler.schedule()

    node = node1 if node1.sent else node2
    other = node2 if node is node1 else node1

    assert node.sent == [[0, 1]]
    assert other.sent == []
    assert node.shutting_down is False
    assert service_manager.transitions == []

    scheduler.mark_test_complete(node, 0)

    # The remaining service-free item cannot start unless the worker gets a
    # shutdown marker, because the next phase is still blocked on that item.
    assert node.shutting_down is True
    assert service_manager.transitions == []

    scheduler.mark_test_complete(node, 1)

    assert service_manager.transitions == [frozenset({"s3"})]
    assert other.sent == [[2]]


def test_deadlock_breaker_retires_only_one_worker_at_a_time() -> None:
    service_manager = FakeServiceManager()
    collection = ["plain_a", "plain_b", "test_s3"]
    metadata = {
        "plain_a": ScyllaResourceMetadata("plain_a", SchedulerResource(cores=1, memory_bytes=GIB)),
        "plain_b": ScyllaResourceMetadata("plain_b", SchedulerResource(cores=1, memory_bytes=GIB)),
        "test_s3": ScyllaResourceMetadata("test_s3", SchedulerResource(cores=1, memory_bytes=GIB), frozenset({"s3"})),
    }
    scheduler, node1, node2 = make_scheduler(collection, metadata, cpus=2, service_manager=service_manager)

    scheduler.collection = collection
    scheduler.total_items = len(collection)
    scheduler.assigned_work[node1].clear()
    scheduler.assigned_work[node2].clear()
    scheduler.assigned_work[node1]["plain_a"] = metadata["plain_a"]
    scheduler.assigned_work[node2]["plain_b"] = metadata["plain_b"]
    scheduler._refresh_node_reservation(node1)
    scheduler._refresh_node_backlog_seconds(node1)
    scheduler._refresh_node_reservation(node2)
    scheduler._refresh_node_backlog_seconds(node2)
    scheduler._set_workqueue(queue_items_from_collection(["test_s3"], metadata))
    scheduler.active_services = frozenset()

    scheduler._reschedule_all()

    assert len(scheduler.deadlock_break_nodes) == 1
    assert sum(int(node.shutting_down) for node in (node1, node2)) == 1

    scheduler._reschedule_all()

    assert len(scheduler.deadlock_break_nodes) == 1
    assert sum(int(node.shutting_down) for node in (node1, node2)) == 1


def test_scheduler_merges_service_sets_into_shared_phase() -> None:
    collection = ["test_ldap", "test_s3"]
    metadata = {
        "test_ldap": ScyllaResourceMetadata("test_ldap", SchedulerResource(), frozenset({"ldap"})),
        "test_s3": ScyllaResourceMetadata("test_s3", SchedulerResource(), frozenset({"s3"})),
    }
    service_manager = FakeServiceManager()
    scheduler, node1, node2 = make_scheduler(collection, metadata, service_manager=service_manager)

    scheduler.schedule()

    assert sorted(sent_indexes(node1) + sent_indexes(node2)) == [0, 1]
    assert service_manager.transitions == [frozenset({"ldap", "s3"})]

    first_node = node1 if any(0 in sent for sent in node1.sent) else node2
    second_node = node1 if any(1 in sent for sent in node1.sent) else node2
    scheduler.mark_test_complete(first_node, 0)
    scheduler.mark_test_complete(second_node, 1)

    assert service_manager.transitions[-1] == frozenset()


def test_historical_resource_usage_can_reduce_default_estimate(tmp_path: pathlib.Path) -> None:
    db_path = tmp_path / DEFAULT_DB_NAME
    writer = SQLiteWriter(db_path)
    for run_id in range(3):
        test_id = writer.write_row(
            TestRow(host_id=HOST_ID, architecture="x86_64", directory="cluster", mode="dev", run_id=run_id, test_name="dtest/auth_test"),
            TESTS_TABLE,
        )
        writer.write_row(
            Metric(
                test_id=test_id,
                host_id=HOST_ID,
                memory_peak=128 * 1024 ** 2,
                success=True,
                time_taken=100.0,
                usage_sec=2.0,
            ),
            METRICS_TABLE,
        )

    config = FakeConfig(tmpdir=tmp_path)
    suite = FakeSuiteConfig("cluster", {"type": "Topology"})
    metadata = scylla_resource_metadata_for_item(
        FakeItem("test/cluster/dtest/auth_test.py::test_one", mark("single_node"), config=config),
        suite,
        "dev",
        False,
        config,
    )

    assert metadata.resources == SchedulerResource(cores=0.125, memory_bytes=GIB // 2)


def test_historical_resource_usage_requires_enough_samples(tmp_path: pathlib.Path) -> None:
    db_path = tmp_path / DEFAULT_DB_NAME
    writer = SQLiteWriter(db_path)
    test_id = writer.write_row(
        TestRow(host_id=HOST_ID, architecture="x86_64", directory="cluster", mode="dev", run_id=1, test_name="dtest/auth_test"),
        TESTS_TABLE,
    )
    writer.write_row(
        Metric(
            test_id=test_id,
            host_id=HOST_ID,
            memory_peak=128 * 1024 ** 2,
            success=True,
            time_taken=100.0,
            usage_sec=2.0,
        ),
        METRICS_TABLE,
    )

    config = FakeConfig(tmpdir=tmp_path)
    suite = FakeSuiteConfig("cluster", {"type": "Topology"})
    metadata = scylla_resource_metadata_for_item(
        FakeItem("test/cluster/dtest/auth_test.py::test_one", mark("single_node"), config=config),
        suite,
        "dev",
        False,
        config,
    )

    assert metadata.resources == SchedulerResource(cores=0.25, memory_bytes=GIB)


def test_historical_resource_usage_aggregates_across_multiple_run_dbs(tmp_path: pathlib.Path) -> None:
    first_db = tmp_path / DEFAULT_DB_NAME
    second_db = tmp_path / "history" / "20260101T000001.000001Z-host.sqlite"
    second_db.parent.mkdir(parents=True, exist_ok=True)

    first_writer = SQLiteWriter(first_db)
    second_writer = SQLiteWriter(second_db)

    for run_id, writer in ((1, first_writer), (2, first_writer), (3, second_writer)):
        test_id = writer.write_row(
            TestRow(host_id=HOST_ID, architecture="x86_64", directory="cluster", mode="dev", run_id=run_id, test_name="dtest/auth_test"),
            TESTS_TABLE,
        )
        writer.write_row(
            Metric(
                test_id=test_id,
                host_id=HOST_ID,
                memory_peak=128 * 1024 ** 2,
                success=True,
                time_taken=100.0,
                usage_sec=2.0,
            ),
            METRICS_TABLE,
        )

    config = FakeConfig(tmpdir=tmp_path)
    suite = FakeSuiteConfig("cluster", {"type": "Topology"})
    metadata = scylla_resource_metadata_for_item(
        FakeItem("test/cluster/dtest/auth_test.py::test_one", mark("single_node"), config=config),
        suite,
        "dev",
        False,
        config,
    )

    assert metadata.resources == SchedulerResource(cores=0.125, memory_bytes=GIB // 2)


@pytest.mark.asyncio
async def test_service_manager_cleans_up_partial_start_failure() -> None:
    manager = FailingSessionServiceManager(fail_on="s3")

    with pytest.raises(RuntimeError, match="boom"):
        await manager.ensure_services({"ldap", "s3"})

    assert manager.active_services == frozenset()
    assert manager.events == [("start", "ldap"), ("start", "s3"), ("stop", "ldap"), ("write", ())]


@pytest.mark.asyncio
# Regression test for rollback when the first new service fails before any new service is active.
async def test_service_manager_preserves_previous_state_when_first_new_service_fails() -> None:
    manager = FailingSessionServiceManager(fail_on="s3")
    manager.active_services = frozenset({"ldap"})

    with pytest.raises(RuntimeError, match="boom"):
        await manager.ensure_services({"s3"})

    assert manager.active_services == frozenset({"ldap"})
    assert manager.events == [("stop", "ldap"), ("start", "s3"), ("start", "ldap"), ("write", ("ldap",))]


@pytest.mark.asyncio
async def test_service_manager_preserves_previous_state_when_stop_fails() -> None:
    # Regression test for session-service rollback when stopping an old service fails before any new service starts.
    manager = FailingSessionServiceManager(fail_on="ldap")
    manager.active_services = frozenset({"ldap"})

    with pytest.raises(RuntimeError, match="boom"):
        await manager.ensure_services({"s3"})

    assert manager.active_services == frozenset({"ldap"})
    assert manager.events == [("stop", "ldap"), ("write", ("ldap",))]


@pytest.mark.asyncio
async def test_service_manager_restores_partial_stop_progress_when_a_later_stop_fails() -> None:
    # Regression test for rollback when a later stop fails after an earlier service
    # has already been stopped successfully.
    manager = PartiallyFailingSessionServiceManager(fail_on="ldap")

    with pytest.raises(RuntimeError, match="boom"):
        await manager.ensure_services(())

    assert manager.running_services == {"ldap", "s3"}
    assert manager.active_services == frozenset({"ldap", "s3"})
    assert manager.events == [("stop", "s3"), ("stop", "ldap"), ("start", "s3"), ("write", ("ldap", "s3"))]


def test_scheduler_dispatches_oversized_work_unit_for_per_test_failure() -> None:
    collection = ["test_a"]
    metadata = {
        "test_a": ScyllaResourceMetadata("test_a", SchedulerResource(cores=3, memory_bytes=GIB)),
    }
    scheduler, node1, node2 = make_scheduler(collection, metadata, cpus=2)

    scheduler.schedule()

    assert node1.sent == [[0]] or node2.sent == [[0]]


def test_scheduler_serializes_oversized_work_units() -> None:
    """Regression test for oversized Scylla work units being dispatched on multiple workers at once."""

    collection = ["test_a", "test_b"]
    metadata = {
        "test_a": ScyllaResourceMetadata("test_a", SchedulerResource(cores=3, memory_bytes=GIB)),
        "test_b": ScyllaResourceMetadata("test_b", SchedulerResource(cores=3, memory_bytes=GIB)),
    }
    scheduler, node1, node2 = make_scheduler(collection, metadata, cpus=2)

    scheduler.schedule()

    assert (node1.sent == [[0]] and node2.sent == []) or (node1.sent == [] and node2.sent == [[0]])

    active_node = node1 if node1.sent else node2
    scheduler.mark_test_complete(active_node, 0)

    assert sorted(node1.sent + node2.sent) == [[0], [1]]


def test_oversized_work_unit_waits_for_idle_worker() -> None:
    collection = ["test_a", "test_b"]
    metadata = {
        "test_a": ScyllaResourceMetadata("test_a", SchedulerResource(cores=2, memory_bytes=GIB)),
        "test_b": ScyllaResourceMetadata("test_b", SchedulerResource(cores=3, memory_bytes=GIB)),
    }
    scheduler = ScyllaResourceScheduler(FakeConfig(cpus=2), numnodes=1, resource_metadata=metadata)
    node = FakeWorker("gw0")
    scheduler.add_node(node)
    scheduler.add_node_collection(node, collection)

    scheduler.schedule()

    assert node.sent == [[0]]

    scheduler.mark_test_complete(node, 0)

    assert node.sent == [[0], [1]]


def test_scylla_resource_budget_failure_message() -> None:
    failure = scylla_resource_budget_failure(
        "test_a",
        SchedulerResource(cores=3, memory_bytes=GIB),
        SchedulerResource(cores=2, memory_bytes=GIB),
    )

    assert failure == "Scylla resource group 'test_a' requires 3 CPU(s) and 1073741824 bytes, exceeding scheduler budget 2 CPU(s) and 1073741824 bytes"
