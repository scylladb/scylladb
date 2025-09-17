#!/usr/bin/python3
#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
import pytest
import os
import pathlib
import shutil
import subprocess
import uuid

from typing import Callable
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass

from test.pylib.manager_client import ManagerClient
from test.cluster.conftest import *
from test.pylib.util import gather_safely


@pytest.fixture(scope="function")
def volumes_factory(pytestconfig, build_mode, request):
    hash = str(uuid.uuid4())
    base = pathlib.Path(f"{pytestconfig.getoption("tmpdir")}/{build_mode}/volumes/{hash}")
    volumes = []

    @dataclass
    class VolumeInfo:
        img: pathlib.Path
        mount: pathlib.Path
        log: pathlib.Path

    @contextmanager
    def wrapper(sizes: list[str]):
        try:
            for id, size in enumerate(sizes):
                path = base / f"scylla-{id}"
                path.mkdir(parents=True)

                volume = VolumeInfo(path.with_name(f"{path.name}.img"), path, path.with_name(f"{path.name}.log"))

                subprocess.run(["truncate", "-s", size, volume.img], check=True)
                subprocess.run(["mkfs.ext4", volume.img], check=True, stdout=subprocess.DEVNULL)
                # -o uid=... and -o gid=... to avoid root:root ownership of mounted files
                # -o fakeroot to avoid permission denied errors on creating files inside docker
                subprocess.run(["fuse2fs", "-o", f"uid={os.getuid()}", "-o", f"gid={os.getgid()}", "-o", "fakeroot", volume.img, volume.mount], check=True)
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
            shutil.copytree(volume.mount, base.parent.parent / f"scylla-{hash}-{id}", ignore=shutil.ignore_patterns('commitlog*', 'lost+found*'))
            shutil.copyfile(volume.log, base.parent.parent / f"scylla-{hash}-{id}.log")

        subprocess.run(["fusermount3", "-u", volume.mount], check=True)
        os.unlink(volume.img)


@asynccontextmanager
async def space_limited_servers(manager: ManagerClient, volumes_factory: Callable, sizes: list[str], **server_args):
    servers = []
    cmdline = server_args.pop("cmdline", [])
    with volumes_factory(sizes) as volumes:
        try:
            servers = [await manager.server_add(cmdline = [*cmdline, '--workdir', str(volume.mount)],
                                                property_file={"dc": "dc1", "rack": f"r{id}"},
                                                **server_args) for id, volume in enumerate(volumes)]
            yield servers
        finally:
            # Stop servers to be able to unmount volumes
            await gather_safely(*(manager.server_stop(server.server_id) for server in servers))
