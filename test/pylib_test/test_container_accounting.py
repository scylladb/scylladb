#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""Unit tests for charging containers to the worker that started them."""

from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

import test.pylib.container_accounting as ca
from test.pylib import sched_dir
from test.pylib.dockerized_service import register_container
from test.pylib.resource_gather import ResourceGatherOn


def _cgroup(path: Path, anon: int, mapped: int = 0) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    (path / "memory.stat").write_text(f"anon {anon}\nfile 123\nfile_mapped {mapped}\n")
    return path


@pytest.fixture
def world(tmp_path, monkeypatch):
    """A cgroup root with the tests' tree and a docker container, a /proc, and a registry."""
    root = tmp_path / "cgroup"
    monkeypatch.setattr(ca, "CGROUP_ROOT", root)
    monkeypatch.setattr(sched_dir, "_root", tmp_path / "sched")
    tests = root / "user.slice" / "tests"
    _cgroup(tests, 5_000)
    _cgroup(tests / "gw0", 1_000)
    _cgroup(tests / "gw1", 2_000)
    jvm = _cgroup(root / "system.slice" / "docker-abc.scope", 800_000, mapped=50_000)
    proc = tmp_path / "proc"
    (proc / "4242").mkdir(parents=True)
    (proc / "4242" / "cgroup").write_text("0::/system.slice/docker-abc.scope\n")
    return SimpleNamespace(root=root, tests=tests, jvm=jvm, proc=proc)


def test_a_registered_container_is_charged_to_its_worker(world, monkeypatch):
    real = ca.cgroup_of_pid
    monkeypatch.setattr(ca, "cgroup_of_pid", lambda pid: real(pid, world.proc))
    ca.register_container_pid(4242, worker="gw1")
    assert ca.container_cgroups("gw1") == [world.jvm]
    assert ca.container_cgroups("gw0") == []
    assert ca.all_container_cgroups() == {"gw1": [world.jvm]}


def test_the_cgroup_of_a_process_is_read_from_proc(world):
    assert ca.cgroup_of_pid(4242, world.proc) == world.jvm
    assert ca.cgroup_of_pid(9999, world.proc) is None


def test_a_container_that_is_gone_drops_out(world, monkeypatch):
    monkeypatch.setattr(ca, "cgroup_of_pid", lambda pid: world.jvm)
    ca.register_container_pid(4242, worker="gw0")
    for f in world.jvm.iterdir():
        f.unlink()
    world.jvm.rmdir()
    assert ca.container_cgroups("gw0") == []


def test_nothing_is_recorded_outside_a_worker_or_without_a_registry(world, monkeypatch):
    monkeypatch.setattr(ca, "cgroup_of_pid", lambda pid: world.jvm)
    monkeypatch.delenv("PYTEST_XDIST_WORKER", raising=False)
    ca.register_container_pid(4242)
    assert ca.all_container_cgroups() == {}
    monkeypatch.setattr(sched_dir, "_root", None)
    ca.register_container_pid(4242, worker="gw0")
    assert ca.container_cgroups("gw0") == []


def test_a_tests_anonymous_peak_includes_its_workers_containers(world, monkeypatch):
    """A learned peak must include the JVM, or the profile prices a migration test at its worker alone."""
    monkeypatch.setattr(ca, "cgroup_of_pid", lambda pid: world.jvm)
    ca.register_container_pid(4242, worker="gw1")
    gatherer = SimpleNamespace(cgroup_path=world.tests / "gw1", worker_id="gw1")
    assert ResourceGatherOn._read_anon_memory(gatherer) == 2_000 + 800_000
    other = SimpleNamespace(cgroup_path=world.tests / "gw0", worker_id="gw0")
    assert ResourceGatherOn._read_anon_memory(other) == 1_000


def test_a_container_dockerized_server_starts_is_registered(world, monkeypatch, tmp_path):
    """The worker that started a container through DockerizedServer is charged for it."""
    runtime = tmp_path / "podman"
    runtime.write_text("#!/bin/sh\necho 4242\n")        # `podman inspect --format {{.State.Pid}}`
    runtime.chmod(0o755)
    real = ca.cgroup_of_pid
    monkeypatch.setattr(ca, "cgroup_of_pid", lambda pid: real(pid, world.proc))
    monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw3")
    asyncio.run(register_container(runtime, "fake-gcs-server-1"))
    assert ca.container_cgroups("gw3") == [world.jvm]
    monkeypatch.delenv("PYTEST_XDIST_WORKER")
    asyncio.run(register_container(runtime, "s3mock-1"))
    assert ca.all_container_cgroups() == {"gw3": [world.jvm]}, "the controller's containers go uncharged"
