#!/usr/bin/python3
#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
import pytest
import pathlib
import shutil
import subprocess
import uuid

from typing import Callable, Generator
from contextlib import asynccontextmanager, contextmanager

from test.pylib.manager_client import ManagerClient
from test.cluster.conftest import *  # noqa: F403


# Mounting volumes outside of the toolchain environment, requires root privileges. To overcome it,
# `unshare` is used to launch tests in a separate namespace. This is a temporary solution as it
# works with test.py but breaks the execution of the tests with pytest without test.py.
# FIXME: Find a more robust solution to ditch unshare.
@pytest.fixture(scope="function")
def volumes_factory(pytestconfig, build_mode, request) -> Generator[dict[pathlib.Path, dict[str, str]], None, None]:
    hash = str(uuid.uuid4())
    base = pathlib.Path(f"{pytestconfig.getoption("tmpdir")}/{build_mode}/volumes/{hash}")
    topology = dict()

    @contextmanager
    def wrapper(topology_sizes: dict[str, dict[str, list[str]]]) -> Generator[dict[pathlib.Path, tuple[str, str]], None, None]:
        try:
            id = 0
            for dc, racks in topology_sizes.items():
                for rack, sizes in racks.items():
                    for size in sizes:
                        path = base / f"scylla-{id}"
                        path.mkdir(parents=True)
                        subprocess.run(["sudo", "mount", "-o", f"size={size}", "-t", "tmpfs", "tmpfs", path], check=True)
                        topology[path] = {"dc": dc, "rack": rack}
                        id += 1
            yield topology
        finally:
            pass
    yield wrapper

    # The test in the storage module use unshare -mr as the launcher to be able to mount volumes.
    # This means that the entire content of the mount is only visible inside the namespace. To keep
    # the files on a test failure, we need to copy them out of the namespace.
    #
    # Copying cannot be done in the finally clause of the wrapper above as at that point test is not
    # yet marked as failed. So the copy has to be done here.
    #
    # Note that since volumes are created within unshare, they will be automatically unmounted and
    # files will be deleted so no additional cleanup is needed.
    report = request.node.stash[PHASE_REPORT_KEY]  # noqa: F405
    test_failed = report.when == "call" and report.failed
    preserve_data = test_failed or request.config.getoption("save_log_on_success")

    if preserve_data:
        for id, path in enumerate(list(topology.keys())):
            shutil.copytree(path, base.parent.parent / f"scylla-{hash}-{id}", ignore=shutil.ignore_patterns('commitlog*'))


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
            servers = [await manager.server_add(cmdline = [*cmdline, '--workdir', str(path)],
                                                property_file={"dc": prop["dc"], "rack": prop["rack"]},
                                                **server_args) for path, prop in topology.items()]
            yield servers
        finally:
            pass
