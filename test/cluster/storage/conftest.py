#!/usr/bin/python3
#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import pytest
import logging
import pathlib
import shutil
import subprocess
import uuid

from typing import Callable
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass

from test.cluster.conftest import PHASE_REPORT_KEY
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import gather_safely

logger = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def volumes_factory(pytestconfig, build_mode, request):
    hash = str(uuid.uuid4())
    base = pathlib.Path(f"{pytestconfig.getoption("tmpdir")}/{build_mode}/volumes/{hash}")
    volumes = []
    holder = None

    @dataclass
    class VolumeInfo:
        mount: pathlib.Path     # the volume itself
        workdir: pathlib.Path   # a symlink to it, for use as a server workdir
        log: pathlib.Path
        launcher: list[str]     # starts a server in the volume's namespace, in the workdir

    # Each volume is a size-limited tmpfs. Mounting it needs no root: the mounts
    # live in a private user+mount namespace kept alive by a holder process, and
    # the test reaches them through /proc/<holder>/root, which resolves paths in
    # the holder's mount namespace. A FUSE-backed volume (fuse2fs) serves every
    # file operation from a helper process, which crawls when the CI host is
    # overloaded (SCYLLADB-5038); tmpfs is served by the kernel. The mounts
    # disappear with the holder, so there is nothing to unmount. The holder waits
    # for its stdin to be closed, so it also goes away if the test process dies.
    #
    # A server uses the volume as its workdir through a symlink (scylla-<id>):
    # the test framework removes and recreates a server's workdir before starting
    # it, and removing a directory that is a mount point only in another mount
    # namespace succeeds and detaches the mount there. rmtree refuses to remove a
    # symlink, so the volume stays. The server log (scylla-<id>.log) is written
    # next to the symlink, outside the volume. Scylla does not accept a symlink
    # as e.g. its commitlog directory, so the volume itself is used everywhere else.
    #
    # Scylla canonicalizes some paths (e.g. of sstables it deletes), and outside
    # the holder's namespace that turns /proc/<holder>/root/... into a host path,
    # so servers run inside the namespace, where both resolve to the volume: they
    # are started through the volume's launcher. Entering a mount namespace resets
    # the working directory, which the server relies on, so the launcher sets it.
    # Anything else running Scylla on a volume (e.g. `scylla sstable`) has to run
    # inside the namespace as well.
    @contextmanager
    def wrapper(sizes: list[str]):
        nonlocal holder
        try:
            # unshare sets the namespaces up before it runs the command, so once the
            # holder says it is ready they can be entered.
            holder = subprocess.Popen(["unshare", "--user", "--map-root-user", "--mount", "--propagation", "private",
                                       "sh", "-c", "echo ready; exec cat > /dev/null"],
                                      stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
            if holder.stdout.readline().strip() != "ready":
                raise RuntimeError("Cannot create a user namespace for tmpfs volumes "
                                   "(are user namespaces enabled in this container?)")
            for id, size in enumerate(sizes):
                mountpoint = base / f".volume-{id}"
                mountpoint.mkdir(parents=True)
                result = subprocess.run(["nsenter", "-t", str(holder.pid), "--user", "--mount", "--preserve-credentials",
                                         "mount", "-t", "tmpfs", "-o", f"size={size}", "tmpfs", mountpoint],
                                        capture_output=True, text=True)
                if result.returncode != 0:
                    raise RuntimeError(f"Cannot mount a tmpfs volume in a user namespace (are user namespaces "
                                       f"enabled in this container?): {result.stderr.strip()}")
                mount = pathlib.Path(f"/proc/{holder.pid}/root{mountpoint}")
                workdir = base / f"scylla-{id}"
                workdir.symlink_to(mount)
                volumes.append(VolumeInfo(mount, workdir, workdir.with_name(f"{workdir.name}.log"),
                                          ["nsenter", "-t", str(holder.pid), "--user", "--mount", "--preserve-credentials",
                                           f"--wdns={workdir}", "--"]))
            yield volumes
        finally:
            pass
    yield wrapper

    # Optionally preserve data, then drop the volumes. Copying cannot be done in the finally
    # clause of the wrapper above as at that point test is not yet marked as failed. So the
    # copy and consequently volumes cleanup have to be done here.
    reports = request.node.stash[PHASE_REPORT_KEY]
    call_report = reports.get("call")
    test_failed = call_report is not None and call_report.failed
    preserve_data = test_failed or request.config.getoption("save_log_on_success")

    try:
        for id, volume in enumerate(volumes):
            if preserve_data:
                shutil.copytree(volume.mount, base.parent.parent / f"scylla-{hash}-{id}", ignore=shutil.ignore_patterns('commitlog*'))
                if volume.log.exists():
                    shutil.copyfile(volume.log, base.parent.parent / f"scylla-{hash}-{id}.log")
    finally:
        if holder:
            holder.kill()
            holder.wait()
            holder.stdin.close()
            holder.stdout.close()
        # Without the holder the symlinks dangle.
        for volume in volumes:
            volume.workdir.unlink(missing_ok=True)


@asynccontextmanager
async def space_limited_servers(manager: ScyllaClusterManager, volumes_factory: Callable, sizes: list[str], property_file=None, **server_args):
    servers = []
    cmdline = server_args.pop("cmdline", [])
    with volumes_factory(sizes) as volumes:
        try:
            if not property_file:
                property_file = [{"dc": "dc1", "rack": f"r{id}"} for id in range(len(volumes))]
            servers = [await manager.server_add(cmdline = [*cmdline, '--workdir', str(volume.workdir)],
                                                launcher=volume.launcher,
                                                property_file=property_file[id],
                                                **server_args) for id, volume in enumerate(volumes)]
            yield servers
        finally:
            # Stop servers to be able to unmount volumes
            await gather_safely(*(manager.server_stop(server.server_id, convict=False) for server in servers))
