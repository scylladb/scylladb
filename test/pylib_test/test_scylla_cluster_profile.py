#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import json
from types import SimpleNamespace

import pytest

from test.pylib.internal_types import IPAddress, ServerInfo, ServerNum
from test.pylib.pool import Pool
from test.pylib.scylla_cluster import (
    ClusterProfileState,
    ScyllaClusterManager,
    ScyllaResourceLimit,
    ScyllaResourceUsage,
    scylla_cmdline_has_memory_override,
    scylla_resource_usage_from_cmdline,
)
from test.pylib.scylla_cluster_profile import (
    ScyllaClusterNodeProfile,
    ScyllaClusterProfile,
    scylla_cluster_profile_from_node,
)


class FakeNode:
    def __init__(self, *marks) -> None:
        self.marks = {mark.name: mark for mark in marks}

    def get_closest_marker(self, name: str):
        return self.marks.get(name)


class FakeSuiteConfig:
    name = "cluster"

    def __init__(self) -> None:
        self.cfg = {
            "extra_scylla_config_options": {"authenticator": "AllowAllAuthenticator"},
            "extra_scylla_cmdline_options": ["--logger-log-level", "raft=debug"],
        }


class FakeServer:
    def __init__(self, server_id: int) -> None:
        self.server_id = ServerNum(server_id)

    def server_info(self) -> ServerInfo:
        return ServerInfo(
            server_id=self.server_id,
            ip_addr=IPAddress(f"127.0.0.{self.server_id}"),
            rpc_address=IPAddress(f"127.0.0.{self.server_id}"),
            datacenter="dc1",
            rack=f"rack{self.server_id}",
            pid=1000 + int(self.server_id),
        )


class FakeCluster:
    def __init__(self) -> None:
        self.name = "fake-cluster"
        self.running = {}
        self.stopped = {}
        self.starting = {}
        self.removed = set()
        self.is_dirty = False
        self.cluster_profile_key = None
        self.cluster_profile_name = None
        self.cluster_reuse = None
        self.resource_limit = None
        self.resource_limit_history: list[object | None] = []
        self.resource_checks: list[tuple[ScyllaResourceUsage, bool]] = []
        self.keyspace_count = 0
        self.added = []
        self.before_test_calls: list[str] = []
        self.after_test_calls: list[tuple[str, bool]] = []
        self.logger = None

    async def add_server(self, **kwargs):
        self.is_dirty = True
        self.added.append(kwargs)
        server = FakeServer(len(self.running) + 1)
        self.running[server.server_id] = server
        return server.server_info()

    def setLogger(self, logger) -> None:
        self.logger = logger

    def before_test(self, name) -> None:
        self.before_test_calls.append(name)

    def after_test(self, name: str, success: bool) -> None:
        self.after_test_calls.append((name, success))

    def take_log_savepoint(self) -> None:
        pass

    def set_resource_limit(self, resource_limit) -> None:
        self.resource_limit = resource_limit
        self.resource_limit_history.append(resource_limit)

    def _resource_usage_from_test_cmdline(self, cmdline, version):
        return scylla_resource_usage_from_cmdline(cmdline or [])

    def _has_memory_override_from_test_cmdline(self, cmdline, version):
        return scylla_cmdline_has_memory_override(cmdline or [])

    def _check_resource_limit(self, additional: ScyllaResourceUsage, has_memory_override: bool) -> None:
        self.resource_checks.append((additional, has_memory_override))

    def _get_keyspace_count(self):
        return 7


@pytest.mark.asyncio
async def test_manager_applies_pending_resource_limit_after_cluster_selection(tmp_path) -> None:
    manager = ScyllaClusterManager("test", SimpleNamespace(), str(tmp_path), sock_path=str(tmp_path / "api"))
    dirty_cluster = FakeCluster()
    dirty_cluster.is_dirty = True
    clean_cluster = FakeCluster()
    clean_cluster.name = "selected-cluster"
    builds = iter([dirty_cluster, clean_cluster])

    async def build() -> FakeCluster:
        return next(builds)

    async def destroy(_cluster: FakeCluster) -> None:
        pass

    pool = Pool(2, build, destroy)
    manager.cluster = await pool.get()
    assert manager.cluster is dirty_cluster
    borrowed = await pool.get()
    assert borrowed is clean_cluster
    await pool.put(borrowed, False)
    manager.clusters = pool

    limit = ScyllaResourceLimit(
        cores=4,
        memory_bytes=8 * 1024 ** 3,
        allow_memory_override=False,
        enforce_usage_limits=True,
    )
    manager.current_resource_limit = limit

    await manager._before_test("test_pending_resource_limit_applies_after_selection")
    assert manager.cluster is clean_cluster
    assert clean_cluster.resource_limit_history == [limit]
    assert dirty_cluster.resource_limit_history == []
    assert manager.current_resource_limit is limit

    current_task = asyncio.current_task()
    assert current_task is not None
    manager.tasks_history[current_task] = SimpleNamespace(path_qs="/cluster/after-test/True")
    await manager._after_test(SimpleNamespace(match_info={"success": "True"}))
    assert clean_cluster.resource_limit_history[-1] is None
    assert manager.current_resource_limit is None


def mark(name: str, *args, **kwargs):
    return getattr(pytest.mark, name)(*args, **kwargs).mark


def make_node(config: dict[str, object] | None = None,
              cmdline: list[str] | None = None,
              property_file: dict[str, object] | None = None,
              server_encryption: str = "none") -> ScyllaClusterNodeProfile:
    return ScyllaClusterNodeProfile(
        config=config or {},
        cmdline=cmdline or [],
        property_file=property_file,
        server_encryption=server_encryption,
    )


def make_profile(*nodes: ScyllaClusterNodeProfile,
                 key: str = "profile-key",
                 name: str = "profile-name",
                 reuse: str = "sequential") -> ScyllaClusterProfile:
    return ScyllaClusterProfile(key=key, name=name, reuse=reuse, nodes=tuple(nodes))


def test_scylla_cluster_marker_parses_homogeneous_profile() -> None:
    profile = scylla_cluster_profile_from_node(
        FakeNode(mark("scylla_cluster", nodes=3, config={"enable_tablets": True}, cmdline=["--smp", "1"], property_file={"dc": "dc1", "rack": "r1"})),
        FakeSuiteConfig(),
    )

    assert profile is not None
    assert profile.reuse == "sequential"
    assert len(profile.nodes) == 3
    assert profile.nodes[0].config == {"enable_tablets": True}
    assert profile.nodes[0].cmdline == ["--smp", "1"]
    assert profile.nodes[0].property_file == {"dc": "dc1", "rack": "r1"}

    key = json.loads(profile.key)
    assert key["suite"]["extra_scylla_config_options"] == {"authenticator": "AllowAllAuthenticator"}
    assert key["nodes"][0]["config"] == {"authenticator": "AllowAllAuthenticator", "enable_tablets": True}


def test_scylla_cluster_marker_parses_heterogeneous_profile() -> None:
    normal_cfg = {"join_ring": True}
    zero_token_cfg = {"join_ring": False}

    profile = scylla_cluster_profile_from_node(FakeNode(mark("scylla_cluster", nodes=[
        {"config": normal_cfg, "property_file": {"dc": "dc1", "rack": "r1"}},
        {"config": zero_token_cfg, "property_file": {"dc": "dc1", "rack": "rz"}},
    ])))

    assert profile is not None
    assert [node.config for node in profile.nodes] == [normal_cfg, zero_token_cfg]
    assert [node.property_file for node in profile.nodes] == [{"dc": "dc1", "rack": "r1"}, {"dc": "dc1", "rack": "rz"}]


def test_prepare_markers_are_mapped_to_cluster_profiles() -> None:
    nodes_profile = scylla_cluster_profile_from_node(FakeNode(mark("prepare_3_nodes_cluster")))
    racks_profile = scylla_cluster_profile_from_node(FakeNode(mark("prepare_3_racks_cluster")))

    assert nodes_profile is not None
    assert len(nodes_profile.nodes) == 3
    assert [node.property_file for node in nodes_profile.nodes] == [None, None, None]

    assert racks_profile is not None
    assert [node.property_file for node in racks_profile.nodes] == [
        {"dc": "dc1", "rack": "rack1"},
        {"dc": "dc1", "rack": "rack2"},
        {"dc": "dc1", "rack": "rack3"},
    ]


def test_scylla_cluster_profile_key_is_stable_for_dict_order() -> None:
    first = scylla_cluster_profile_from_node(FakeNode(mark("scylla_cluster", nodes=1, config={"a": 1, "b": 2})))
    second = scylla_cluster_profile_from_node(FakeNode(mark("scylla_cluster", nodes=1, config={"b": 2, "a": 1})))

    assert first is not None
    assert second is not None
    assert first.key == second.key


def test_scylla_cluster_profile_rejects_non_literal_values() -> None:
    with pytest.raises(pytest.UsageError, match="literal JSON-compatible"):
        scylla_cluster_profile_from_node(FakeNode(mark("scylla_cluster", nodes=1, config={"bad": object()})))


@pytest.mark.asyncio
async def test_manager_profile_provisioning_resets_dirty_state(tmp_path) -> None:
    manager = ScyllaClusterManager("test", SimpleNamespace(), str(tmp_path), sock_path=str(tmp_path / "api"))
    cluster = FakeCluster()
    manager.cluster = cluster
    profile = make_profile(
        make_node(),
        make_node(config={"join_ring": False}, cmdline=["--smp", "1"], property_file={"dc": "dc1", "rack": "rz"}),
    )

    await manager._provision_cluster_profile(profile)

    assert not cluster.is_dirty
    assert cluster.cluster_profile_key == "profile-key"
    assert cluster.keyspace_count == 7
    assert len(cluster.added) == 2


def test_manager_cluster_candidate_for_profile_matches_clean_and_reused_cluster(tmp_path) -> None:
    manager = ScyllaClusterManager("test", SimpleNamespace(), str(tmp_path), sock_path=str(tmp_path / "api"))
    profile = make_profile(
        make_node(cmdline=["--smp", "1"]),
        make_node(cmdline=["--smp", "2"]),
        key="profile-key",
        name="profile-name",
    )

    clean = FakeCluster()
    matching = FakeCluster()
    matching.cluster_profile_key = profile.key
    matching.cluster_profile_name = profile.name
    matching.cluster_reuse = profile.reuse
    matching.running = {ServerNum(1): FakeServer(1), ServerNum(2): FakeServer(2)}
    incomplete = FakeCluster()
    incomplete.cluster_profile_key = profile.key
    incomplete.cluster_profile_name = profile.name
    incomplete.cluster_reuse = profile.reuse
    incomplete.running = {ServerNum(1): FakeServer(1)}
    wrong = FakeCluster()
    wrong.cluster_profile_key = "other"
    wrong.cluster_profile_name = "other"
    wrong.cluster_reuse = profile.reuse
    wrong.running = {ServerNum(1): FakeServer(1), ServerNum(2): FakeServer(2)}
    dirty = FakeCluster()
    dirty.is_dirty = True

    assert manager._cluster_candidate_for_profile(clean, None)
    assert manager._cluster_candidate_for_profile(clean, profile)
    assert manager._cluster_candidate_for_profile(matching, profile)
    assert not manager._cluster_candidate_for_profile(incomplete, profile)
    assert not manager._cluster_candidate_for_profile(wrong, profile)
    assert not manager._cluster_candidate_for_profile(dirty, profile)


def test_manager_cluster_profile_resource_limit_uses_effective_new_cluster_usage(tmp_path) -> None:
    manager = ScyllaClusterManager("test", SimpleNamespace(), str(tmp_path), sock_path=str(tmp_path / "api"))
    cluster = FakeCluster()
    manager.cluster = cluster
    profile = make_profile(
        make_node(cmdline=["--smp", "1"]),
        make_node(cmdline=["--smp", "2", "-m", "2G"]),
    )

    manager._check_cluster_profile_resource_limit(profile)

    assert cluster.resource_checks == [(ScyllaResourceUsage(cores=3, memory_bytes=3 * 1024 ** 3), True)]


def test_manager_cluster_profile_resource_limit_does_not_double_count_reused_cluster(tmp_path) -> None:
    manager = ScyllaClusterManager("test", SimpleNamespace(), str(tmp_path), sock_path=str(tmp_path / "api"))
    cluster = FakeCluster()
    profile = make_profile(
        make_node(cmdline=["--smp", "1"]),
        make_node(cmdline=["--smp", "2", "-m", "2G"]),
    )
    cluster.cluster_profile_key = profile.key
    cluster.cluster_profile_name = profile.name
    cluster.cluster_reuse = profile.reuse
    cluster.running = {ServerNum(1): FakeServer(1), ServerNum(2): FakeServer(2)}
    manager.cluster = cluster

    manager._check_cluster_profile_resource_limit(profile)

    assert cluster.resource_checks == [(ScyllaResourceUsage(), True)]


def test_manager_profile_add_request_returns_existing_servers(tmp_path) -> None:
    manager = ScyllaClusterManager("test", SimpleNamespace(), str(tmp_path), sock_path=str(tmp_path / "api"))
    profile = make_profile(
        make_node(),
        make_node(property_file={"dc": "dc1", "rack": "rack2"}),
    )
    manager.cluster = SimpleNamespace(
        is_dirty=False,
        cluster_profile_key=profile.key,
        cluster_reuse=profile.reuse,
        running={ServerNum(1): FakeServer(1), ServerNum(2): FakeServer(2)},
        stopped={},
        starting={},
        removed=set(),
    )
    manager.current_cluster_profile_state = ClusterProfileState(profile=profile)

    first = manager._profile_servers_for_add_request(manager._nodes_from_addservers_data({"servers_num": 1, "start": True, "server_encryption": "none"}))
    second = manager._profile_servers_for_add_request(manager._nodes_from_addservers_data({
        "servers_num": 1,
        "start": True,
        "server_encryption": "none",
        "property_file": {"dc": "dc1", "rack": "rack2"},
    }))

    assert [server["server_id"] for server in first] == [ServerNum(1)]
    assert [server["server_id"] for server in second] == [ServerNum(2)]
    assert manager.current_cluster_profile_state is not None
    assert manager.current_cluster_profile_state.consumed_nodes == 2
