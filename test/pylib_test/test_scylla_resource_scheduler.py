#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from __future__ import annotations

import json
import pathlib
from types import SimpleNamespace

import pytest

from test import HOST_ID
from test.pylib.db.model import Metric, Test as TestRow
from test.pylib.db.writer import DEFAULT_DB_NAME, METRICS_TABLE, TESTS_TABLE, SQLiteWriter
from test.pylib.scylla_resource_scheduler import (
    ScyllaResourceMetadata,
    ScyllaResourceScheduler,
    SchedulerResource,
    plan_items_by_service_phase,
    queue_items_from_collection,
    scylla_resource_budget_failure,
    scylla_resource_metadata_for_item,
    scylla_resource_timeline_path,
)
from test.pylib.scylla_resources import scylla_resource_limit_from_markers
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
        "test/cqlpy/test_a.py::test_one.dev.1": ScyllaResourceMetadata("module:dev:test/cqlpy/test_a.py", SchedulerResource(cores=1, memory_bytes=GIB)),
        "test/cqlpy/test_a.py::test_two.dev.1": ScyllaResourceMetadata("module:dev:test/cqlpy/test_a.py", SchedulerResource(cores=1, memory_bytes=GIB)),
    }

    workqueue = queue_items_from_collection(list(metadata), metadata)

    assert list(workqueue) == list(metadata)


def test_item_queue_preserves_per_item_services() -> None:
    metadata = {
        "test/cqlpy/test_a.py::test_one.dev.1": ScyllaResourceMetadata("module:dev:test/cqlpy/test_a.py", SchedulerResource(cores=1, memory_bytes=GIB)),
        "test/cqlpy/test_a.py::test_two.dev.1": ScyllaResourceMetadata("module:dev:test/cqlpy/test_a.py", SchedulerResource(cores=1, memory_bytes=GIB), frozenset({"s3"})),
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

    assert node1.sent == [[0], [1]]
    assert node2.sent == []
    assert node1.shutting_down
    assert node2.shutting_down


def test_scheduler_releases_resources_after_completion() -> None:
    collection = ["test_a", "test_b", "test_c"]
    metadata = {
        "test_a": ScyllaResourceMetadata("test_a", SchedulerResource(cores=2, memory_bytes=GIB)),
        "test_b": ScyllaResourceMetadata("test_b", SchedulerResource(cores=1, memory_bytes=GIB)),
        "test_c": ScyllaResourceMetadata("test_c", SchedulerResource(cores=1, memory_bytes=GIB)),
    }
    scheduler, node1, node2 = make_scheduler(collection, metadata, cpus=2)
    scheduler.schedule()

    scheduler.mark_test_complete(node1, 0)

    assert [2] in node1.sent or [2] in node2.sent


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

    assert node1.sent == [[0], [1]]
    assert node2.sent == []


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
    assert [1] in node1.sent or [1] in node2.sent
    assert [2] in node1.sent or [2] in node2.sent

    complete(1)
    complete(2)

    assert service_manager.transitions[-1] == frozenset()


def test_scheduler_merges_service_sets_into_shared_phase() -> None:
    collection = ["test_ldap", "test_s3"]
    metadata = {
        "test_ldap": ScyllaResourceMetadata("test_ldap", SchedulerResource(), frozenset({"ldap"})),
        "test_s3": ScyllaResourceMetadata("test_s3", SchedulerResource(), frozenset({"s3"})),
    }
    service_manager = FakeServiceManager()
    scheduler, node1, node2 = make_scheduler(collection, metadata, service_manager=service_manager)

    scheduler.schedule()

    assert sorted(node1.sent + node2.sent) == [[0], [1]]
    assert service_manager.transitions == [frozenset({"ldap", "s3"})]

    first_node = node1 if [0] in node1.sent else node2
    second_node = node1 if [1] in node1.sent else node2
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
    assert (node1.sent == [[0], [1]] and node2.sent == []) or (node1.sent == [] and node2.sent == [[0], [1]])


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
