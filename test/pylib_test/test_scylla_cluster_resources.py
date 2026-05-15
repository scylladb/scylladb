#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import logging
import pathlib
from types import SimpleNamespace

import pytest

import test.pylib.scylla_cluster as scylla_cluster_module
from test.pylib.scylla_cluster import (
    ScyllaCluster,
    ScyllaServer,
    ScyllaResourceLimit,
    ScyllaResourceUsage,
    ScyllaVersionDescription,
    scylla_cmdline_has_memory_override,
    scylla_resource_usage_from_cmdline,
)
from test.pylib.internal_types import ServerNum
from test.pylib.scylla_resources import parse_scylla_memory, scylla_resource_limit_from_payload


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("512M", 512 * 1024 ** 2),
        ("1G", 1024 ** 3),
        ("2GiB", 2 * 1024 ** 3),
        (1024, 1024),
    ],
)
def test_parse_scylla_memory(value, expected):
    assert parse_scylla_memory(value) == expected


@pytest.mark.parametrize(
    "cmdline",
    [
        ["-m", "2G"],
        ["-m2G"],
        ["--memory", "2G"],
        ["--memory=2G"],
    ],
)
def test_scylla_cmdline_has_memory_override(cmdline):
    assert scylla_cmdline_has_memory_override(cmdline)


def test_scylla_cmdline_has_memory_override_ignores_other_options():
    assert not scylla_cmdline_has_memory_override(["--max-task-backlog", "200", "--smp", "4"])


@pytest.mark.parametrize(
    ("cmdline", "usage"),
    [
        ([], ScyllaResourceUsage(cores=2, memory_bytes=1024 ** 3)),
        (["--smp", "1"], ScyllaResourceUsage(cores=1, memory_bytes=1024 ** 3)),
        (["--smp=4", "-m", "2G"], ScyllaResourceUsage(cores=4, memory_bytes=2 * 1024 ** 3)),
        (["--memory=512M"], ScyllaResourceUsage(cores=2, memory_bytes=512 * 1024 ** 2)),
    ],
)
def test_scylla_resource_usage_from_cmdline(cmdline, usage):
    assert scylla_resource_usage_from_cmdline(cmdline) == usage


def test_scylla_resource_limit_from_payload_prefers_numeric_memory_bytes() -> None:
    limit = scylla_resource_limit_from_payload({
        "cores": 4,
        "memory_bytes": 2 * 1024 ** 3,
        "allow_memory_override": True,
        "enforce_usage_limits": False,
    })

    assert limit == ScyllaResourceLimit(
        cores=4,
        memory_bytes=2 * 1024 ** 3,
        allow_memory_override=True,
        enforce_usage_limits=False,
    )


def test_scylla_resource_limit_rejects_excess_cores():
    cluster = ScyllaCluster(logging.getLogger(__name__), None, 0, lambda params: None)
    cluster.set_resource_limit(ScyllaResourceLimit(cores=2))

    with pytest.raises(RuntimeError, match="Scylla core limit exceeded"):
        cluster._check_resource_limit(ScyllaResourceUsage(cores=3), has_memory_override=False)


def test_unbounded_scylla_resource_limit_does_not_reject_usage():
    cluster = ScyllaCluster(logging.getLogger(__name__), None, 0, lambda params: None)
    cluster.set_resource_limit(ScyllaResourceLimit(cores=2, memory_bytes=1024 ** 3, enforce_usage_limits=False))

    cluster._check_resource_limit(ScyllaResourceUsage(cores=100, memory_bytes=100 * 1024 ** 3), has_memory_override=False)


def test_unbounded_scylla_resource_limit_still_controls_memory_override():
    cluster = ScyllaCluster(logging.getLogger(__name__), None, 0, lambda params: None)
    cluster.set_resource_limit(ScyllaResourceLimit(cores=2, memory_bytes=1024 ** 3, enforce_usage_limits=False))

    with pytest.raises(RuntimeError, match="Scylla memory overrides"):
        cluster._check_resource_limit(ScyllaResourceUsage(), has_memory_override=True)


def test_scylla_resource_limit_rejects_memory_override_without_resources_marker():
    cluster = ScyllaCluster(logging.getLogger(__name__), None, 0, lambda params: None)

    with pytest.raises(RuntimeError, match="Scylla memory overrides"):
        cluster._check_resource_limit(ScyllaResourceUsage(), has_memory_override=True)


def test_scylla_resource_limit_detects_global_memory_override():
    cluster = ScyllaCluster(
        logging.getLogger(__name__),
        None,
        0,
        lambda params: None,
        build_cmdline_options=lambda cmdline, version: ["--smp", "1", "-m", "1G", "--memory", "1G"],
        has_memory_override=lambda cmdline, version: True,
    )

    with pytest.raises(RuntimeError, match="Scylla memory overrides"):
        cluster._check_resource_limit(
            cluster._resource_usage_from_test_cmdline([], None),
            cluster._has_memory_override_from_test_cmdline([], None),
        )


# Regression: applying a resource limit must inspect the current running cluster state.
def test_set_resource_limit_rejects_existing_memory_override() -> None:
    cluster = ScyllaCluster(logging.getLogger(__name__), None, 0, lambda params: None)
    cluster.running = {
        ServerNum(1): SimpleNamespace(cmdline_options=["--smp", "1", "-m", "2G"]),
    }

    with pytest.raises(RuntimeError, match="Scylla memory overrides"):
        cluster.set_resource_limit(ScyllaResourceLimit(enforce_usage_limits=False))

    assert cluster.resource_limit is None


# Regression: updating a running server's command line must refresh cached memory-override state.
def test_update_cmdline_refreshes_memory_override_for_later_resource_checks(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(scylla_cluster_module, "SCYLLA_CMDLINE_OPTIONS", ["--smp", "2"])

    cluster = ScyllaCluster(logging.getLogger(__name__), None, 0, lambda params: None)
    cluster.set_resource_limit(ScyllaResourceLimit(allow_memory_override=True, enforce_usage_limits=False))

    server = ScyllaServer(
        mode="dev",
        version=ScyllaVersionDescription(path="/bin/true", config={}, argv=[]),
        vardir=tmp_path,
        logger=logging.getLogger(__name__),
        cluster_name="test-cluster",
        ip_addr="127.0.0.1",
        seeds=["127.0.0.1"],
        cmdline_options=["--smp", "1"],
        config_options={},
        property_file={},
        append_env={},
        server_encryption="none",
    )
    cluster.running[ServerNum(1)] = server

    assert not server.has_memory_override

    cluster.update_cmdline(ServerNum(1), ["-m", "2G"])

    assert server.has_memory_override

    with pytest.raises(RuntimeError, match="Scylla memory overrides"):
        cluster.set_resource_limit(ScyllaResourceLimit(enforce_usage_limits=False))

    assert cluster.resource_limit.allow_memory_override


# Regression: a command-line update that removes `-m` must be checked after merge, not on the raw patch.
def test_update_cmdline_allows_removing_memory_override_without_introducing_one(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(scylla_cluster_module, "SCYLLA_CMDLINE_OPTIONS", ["--smp", "2"])

    cluster = ScyllaCluster(logging.getLogger(__name__), None, 0, lambda params: None)
    cluster.set_resource_limit(ScyllaResourceLimit(enforce_usage_limits=False))

    server = ScyllaServer(
        mode="dev",
        version=ScyllaVersionDescription(path="/bin/true", config={}, argv=[]),
        vardir=tmp_path,
        logger=logging.getLogger(__name__),
        cluster_name="test-cluster",
        ip_addr="127.0.0.1",
        seeds=["127.0.0.1"],
        cmdline_options=["--smp", "1"],
        config_options={},
        property_file={},
        append_env={},
        server_encryption="none",
    )
    cluster.running[ServerNum(1)] = server

    assert not server.has_memory_override

    cluster.update_cmdline(ServerNum(1), ["-m", "__remove__"])

    assert server.cmdline_options == ["--smp", "1"]
    assert not server.has_memory_override


# Regression: rejecting a server-add request because of the resource budget must not dirty the cluster.
@pytest.mark.asyncio
async def test_add_server_rejects_resource_limit_without_dirtying_cluster() -> None:
    cluster = ScyllaCluster(logging.getLogger(__name__), None, 0, lambda params: None)
    cluster.resource_limit = ScyllaResourceLimit(cores=0, memory_bytes=0)

    with pytest.raises(RuntimeError, match="Scylla core limit exceeded"):
        await cluster.add_server(cmdline=["--smp", "1"])

    assert not cluster.is_dirty


# Regression: restarting a stopped server must honor the stored command line when no override is passed.
@pytest.mark.asyncio
async def test_server_start_rejects_restart_with_existing_memory_override() -> None:
    class FakeStoppedServer:
        def __init__(self) -> None:
            self.cmdline_options = ["--smp", "1", "-m", "2G"]
            self.ip_addr = "127.0.0.1"
            self.started = False

        def change_seeds(self, seeds) -> None:
            self.seeds = seeds

        async def start(self, **kwargs) -> None:
            self.started = True

    cluster = ScyllaCluster(logging.getLogger(__name__), None, 0, lambda params: None)
    cluster.resource_limit = ScyllaResourceLimit(enforce_usage_limits=False)
    server = FakeStoppedServer()
    cluster.stopped = {ServerNum(1): server}

    with pytest.raises(RuntimeError, match="Scylla memory overrides"):
        await cluster.server_start(ServerNum(1))

    assert not server.started


# Regression: rejecting a server restart because of the resource budget must not dirty the cluster.
@pytest.mark.asyncio
async def test_server_start_rejects_resource_limit_without_dirtying_cluster() -> None:
    class FakeStoppedServer:
        def __init__(self) -> None:
            self.cmdline_options = ["--smp", "1"]
            self.ip_addr = "127.0.0.1"
            self.started = False

        def change_seeds(self, seeds) -> None:
            self.seeds = seeds

        async def start(self, **kwargs) -> None:
            self.started = True

    cluster = ScyllaCluster(logging.getLogger(__name__), None, 0, lambda params: None)
    cluster.resource_limit = ScyllaResourceLimit(cores=0, memory_bytes=0)
    server = FakeStoppedServer()
    cluster.stopped = {ServerNum(1): server}

    with pytest.raises(RuntimeError, match="Scylla core limit exceeded"):
        await cluster.server_start(ServerNum(1))

    assert not cluster.is_dirty
    assert not server.started


# Regression: a restart-time cmdline override must remain visible to later resource-limit checks.
@pytest.mark.asyncio
async def test_server_start_persists_memory_override_for_later_resource_checks() -> None:
    class FakeStoppedServer:
        def __init__(self) -> None:
            self.cmdline_options = ["--smp", "1"]
            self.ip_addr = "127.0.0.1"
            self.started = False

        def change_seeds(self, seeds) -> None:
            self.seeds = seeds

        async def start(self, **kwargs) -> None:
            self.started = True

    cluster = ScyllaCluster(logging.getLogger(__name__), None, 0, lambda params: None)
    cluster.resource_limit = ScyllaResourceLimit(allow_memory_override=True, enforce_usage_limits=False)
    server = FakeStoppedServer()
    cluster.stopped = {ServerNum(1): server}

    await cluster.server_start(ServerNum(1), cmdline_options_override=["--smp", "1", "-m", "2G"])

    assert server.started

    with pytest.raises(RuntimeError, match="Scylla memory overrides"):
        cluster.set_resource_limit(ScyllaResourceLimit(enforce_usage_limits=False))


# Regression: resource-limit checks during an in-flight restart must see the override state immediately.
@pytest.mark.asyncio
async def test_server_start_publishes_memory_override_before_start_completes() -> None:
    start_entered = asyncio.Event()
    release_start = asyncio.Event()

    class FakeStoppedServer:
        def __init__(self) -> None:
            self.cmdline_options = ["--smp", "1"]
            self.ip_addr = "127.0.0.1"
            self.started = False

        def change_seeds(self, seeds) -> None:
            self.seeds = seeds

        async def start(self, **kwargs) -> None:
            start_entered.set()
            await release_start.wait()
            self.started = True

    cluster = ScyllaCluster(logging.getLogger(__name__), None, 0, lambda params: None)
    cluster.set_resource_limit(ScyllaResourceLimit(allow_memory_override=True, enforce_usage_limits=False))
    server = FakeStoppedServer()
    cluster.stopped = {ServerNum(1): server}

    task = asyncio.create_task(cluster.server_start(ServerNum(1), cmdline_options_override=["--smp", "1", "-m", "2G"]))
    await start_entered.wait()

    try:
        with pytest.raises(RuntimeError, match="Scylla memory overrides"):
            cluster.set_resource_limit(ScyllaResourceLimit(enforce_usage_limits=False))
    finally:
        release_start.set()
        await task
