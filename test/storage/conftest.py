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

from typing import Callable, Generator
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass

from test.pylib.manager_client import ManagerClient
from test.cluster.conftest import *  # noqa: F403
from test.pylib.util import gather_safely


@pytest.fixture(scope="function")
def volumes_factory(pytestconfig, build_mode, request) -> Generator[dict[pathlib.Path, dict[str, str]], None, None]:
    hash = str(uuid.uuid4())
    base = pathlib.Path(f"{pytestconfig.getoption("tmpdir")}/{build_mode}/volumes/{hash}")
    topology = dict()

    @dataclass
    class VolumeInfo:
        img: pathlib.Path
        mount: pathlib.Path
        log: pathlib.Path

    @contextmanager
    def wrapper(topology_sizes: dict[str, dict[str, list[str]]]) -> Generator[dict[VolumeInfo, dict[str, str]], None, None]:
        """
        Create a set of temporary directories for the given topology sizes.

        Returns a dictionary mapping the directory paths to their datacenter and rack.
        """
        try:
            id = 0
            for dc, racks in topology_sizes.items():
                for rack, sizes in racks.items():
                    for size in sizes:
                        path = base / f"scylla-{id}"
                        path.mkdir(parents=True)

                        volume = VolumeInfo(path.with_name(f"{path.name}.img"), path, path.with_name(f"{path.name}.log"))

                        subprocess.run(["truncate", "-s", size, volume.img], check=True)
                        subprocess.run(["mkfs.ext4", volume.img], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                        # -o uid=... and -o gid=... to avoid root:root ownership of mounted files
                        # -o fakeroot to avoid permission denied errors on creating files inside docker
                        subprocess.run(["fuse2fs", "-o", f"uid={os.getuid()}", "-o", f"gid={os.getgid()}", "-o", "fakeroot", volume.img, volume.mount], check=True)
                        topology[id] = {"dc": dc, "rack": rack, "volume": volume}
                        id += 1
            yield topology
        finally:
            pass
    yield wrapper

    # Unmount volumes and optionally preserve data. Copying cannot be done in the finally
    # clause of the wrapper above as at that point test is not yet marked as failed. So the
    # copy and consequently volumes cleanup have to be done here.
    report = request.node.stash[PHASE_REPORT_KEY]
    test_failed = report.when == "call" and report.failed
    preserve_data = test_failed or request.config.getoption("save_log_on_success")

    for id, prop in topology.items():
        if preserve_data:
            shutil.copytree(prop["volume"].mount, base.parent.parent / f"scylla-{hash}-{id}", ignore=shutil.ignore_patterns('commitlog*', 'lost+found*'))
            shutil.copyfile(prop["volume"].log, base.parent.parent / f"scylla-{hash}-{id}.log")

        subprocess.run(["fusermount3", "-u", prop["volume"].mount], check=True)
        os.unlink(prop["volume"].img)

@asynccontextmanager
async def space_limited_servers(manager: ManagerClient, volumes_factory: Callable, topology_sizes: dict[str, dict[str, list[str]]], **server_args):
    """
    Context manager that creates and destroys a set of Scylla servers with limited disk space.  
    The servers are created with the given server_args and with volumes created according to topology_sizes.  
    The volumes are created using the volumes_factory fixture.

    :param manager: ManagerClient instance to use for creating and destroying servers.
    :param volumes_factory: volumes_factory fixture to use for creating volumes.
    :param server_args: additional arguments to pass to manager.server_add.
    :param topology_sizes: dictionary defining the topology and the size of the volumes for each server. Example:

        `{"dc1": {"r1": ["300M"], "r2": ["300M"], "r3": ["300M"]}, "dc2": {"r1": ["300M", "200M"], "r2": ["300M", "200M"]}}`
    """
    servers = []
    cmdline = server_args.pop("cmdline", [])
    with volumes_factory(topology_sizes) as topology:
        try:
            servers = [await manager.server_add(cmdline = [*cmdline, '--workdir', str(prop["volume"].mount)],
                                                property_file={"dc": prop["dc"], "rack": prop["rack"]},
                                                **server_args) for prop in topology.values()]
            yield servers
        finally:
            # Stop servers to be able to unmount volumes
            await gather_safely(*(manager.server_stop(server.server_id) for server in servers))
