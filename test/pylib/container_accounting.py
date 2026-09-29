#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""Record which xdist worker started each container a test runs.

A test's memory is measured as its worker's cgroup: the worker, and every Scylla node it
starts, live under it.  A container does not.  The container runtime puts each container
in a cgroup of its own (libpod-<id>.scope for rootless podman, docker-<id>.scope for
docker), so the CPU and memory a container uses show up in no measurement of the test.

DockerizedServer registers every container it starts (see register_container_pid), and
the container's cgroup is recorded under the xdist worker that started it.  What those
cgroups hold is added to the worker's own (see worker_anon).  A container that has gone
takes its cgroup with it and drops out.
"""

from __future__ import annotations

import os
from collections.abc import Iterable
from pathlib import Path

from test.pylib import sched_dir

CGROUP_ROOT = Path("/sys/fs/cgroup")


def cgroup_of_pid(pid: int, proc: Path = Path("/proc")) -> Path | None:
    """The cgroup v2 directory of a process."""
    try:
        for line in (proc / str(pid) / "cgroup").read_text().splitlines():
            if line.startswith("0::"):
                return CGROUP_ROOT / line[3:].lstrip("/")
    except OSError:
        pass
    return None


def register_container_pid(pid: int | None, worker: str | None = None) -> None:
    """Record that the container whose main process is `pid` was started by this worker.

    Does nothing outside an xdist worker of a run that keeps a registry, or when the
    container's cgroup cannot be read.
    """
    registry = sched_dir.containers()
    worker = worker or os.environ.get("PYTEST_XDIST_WORKER")
    if registry is None or not worker or not pid:
        return
    cgroup = cgroup_of_pid(pid)
    if cgroup is None:
        return
    try:
        registry.mkdir(parents=True, exist_ok=True)
        with open(registry / f"{worker}.txt", "a") as f:
            f.write(f"{cgroup}\n")
    except OSError:
        pass            # accounting is best effort; a test must not fail over it


def container_cgroups(worker: str) -> list[Path]:
    """Cgroups of the containers this worker started that still exist."""
    registry = sched_dir.containers()
    if registry is None:
        return []
    try:
        lines = (registry / f"{worker}.txt").read_text().splitlines()
    except OSError:
        return []
    return [p for p in dict.fromkeys(Path(line) for line in lines if line) if p.is_dir()]


def all_container_cgroups() -> dict[str, list[Path]]:
    """worker -> cgroups of the containers it started that still exist."""
    registry = sched_dir.containers()
    if registry is None or not registry.is_dir():
        return {}
    return {f.stem: cgroups for f in registry.glob("*.txt") if (cgroups := container_cgroups(f.stem))}


def memory_stat(path: Path, field: str) -> float | None:
    """One field of a cgroup's memory.stat."""
    try:
        with open(path / "memory.stat") as f:
            for line in f:
                if line.startswith(field + " "):
                    return float(line.split()[1])
    except (OSError, ValueError, IndexError):
        pass
    return None


def worker_anon(cgroup: Path, containers: Iterable[Path] | None = None, worker: str | None = None) -> float | None:
    """Anonymous memory of a worker's cgroup plus that of the containers it started.

    The one definition of a worker's memory, for the peaks the profile learns and for the
    live readings the forecast subtracts them from: the two must be the same measure.
    `containers` defaults to what the registry holds for `worker`.
    """
    own = memory_stat(cgroup, "anon")
    if own is None:
        return None
    if containers is None:
        containers = container_cgroups(worker) if worker else []
    return own + sum(memory_stat(c, "anon") or 0.0 for c in containers)

