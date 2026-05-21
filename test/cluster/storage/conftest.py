#!/usr/bin/python3
#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import pytest
import os
import pathlib
import shutil
import subprocess
import tempfile
import uuid

from typing import Callable
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass

from test.cluster.conftest import PHASE_REPORT_KEY
from test.pylib.manager_client import ManagerClient
from test.pylib.util import gather_safely


@pytest.fixture(scope="function")
def volumes_factory(pytestconfig, build_mode, request):
    hash = str(uuid.uuid4())
    artifact_base = pathlib.Path(f"{pytestconfig.getoption("tmpdir")}/{build_mode}/volumes/{hash}")
    artifact_base.mkdir(parents=True, exist_ok=True)
    base = pathlib.Path(tempfile.mkdtemp(prefix=f"scylla-volumes-{hash}-"))
    volumes = []

    @dataclass
    class VolumeInfo:
        img: pathlib.Path
        mount: pathlib.Path
        real_mount: pathlib.Path
        log: pathlib.Path

    @contextmanager
    def wrapper(sizes: list[str]):
        try:
            for id, size in enumerate(sizes):
                real_mount = base / f"scylla-{id}"
                real_mount.mkdir(parents=True)
                path = artifact_base / f"scylla-{id}"
                path.symlink_to(real_mount, target_is_directory=True)

                volume = VolumeInfo(
                    real_mount.with_name(f"{real_mount.name}.img"),
                    path,
                    real_mount,
                    path.with_name(f"{path.name}.volume.log"),
                )

                subprocess.run(["truncate", "-s", size, volume.img], check=True)
                with volume.log.open("a", encoding="utf-8") as log:
                    subprocess.run(["mkfs.ext4", volume.img], check=True, stdout=subprocess.DEVNULL, stderr=log)
                    # -o uid=... and -o gid=... to avoid root:root ownership of mounted files
                    # -o fakeroot to avoid permission denied errors on creating files inside docker
                    subprocess.run([
                        "fuse2fs",
                        "-o", f"uid={os.getuid()}",
                        "-o", f"gid={os.getgid()}",
                        "-o", "fakeroot",
                        volume.img,
                        volume.real_mount,
                    ], check=True, stdout=log, stderr=log)
                if not os.path.ismount(volume.real_mount):
                    raise RuntimeError(f"fuse2fs did not mount {volume.img} on {volume.real_mount}")
                volumes.append(volume)
            yield volumes
        finally:
            pass
    yield wrapper

    # Unmount volumes and optionally preserve data. Copying cannot be done in the finally
    # clause of the wrapper above as at that point test is not yet marked as failed. So the
    # copy and consequently volumes cleanup have to be done here.
    report = request.node.stash[PHASE_REPORT_KEY]
    test_failed = report.when == "call" and report.failed
    preserve_data = test_failed or request.config.getoption("save_log_on_success")

    for id, volume in enumerate(volumes):
        if preserve_data:
            shutil.copytree(volume.mount, artifact_base.parent.parent / f"scylla-{hash}-{id}", ignore=shutil.ignore_patterns('commitlog*', 'lost+found*'))
            shutil.copyfile(volume.log, artifact_base.parent.parent / f"scylla-{hash}-{id}.log")

        if os.path.ismount(volume.real_mount):
            subprocess.run(["fusermount3", "-u", volume.real_mount], check=True)
        os.unlink(volume.img)

    shutil.rmtree(base, ignore_errors=True)


@asynccontextmanager
async def space_limited_servers(manager: ManagerClient, volumes_factory: Callable, sizes: list[str], property_file=None, **server_args):
    servers = []
    cmdline = server_args.pop("cmdline", [])
    with volumes_factory(sizes) as volumes:
        try:
            if not property_file:
                property_file = [{"dc": "dc1", "rack": f"r{id}"} for id in range(len(volumes))]
            servers = [await manager.server_add(cmdline = [*cmdline, '--workdir', str(volume.mount)],
                                                property_file=property_file[id],
                                                **server_args) for id, volume in enumerate(volumes)]
            yield servers
        finally:
            # Stop servers to be able to unmount volumes
            await gather_safely(*(manager.server_stop(server.server_id) for server in servers))
