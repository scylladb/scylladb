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

from typing import Callable
from contextlib import asynccontextmanager, contextmanager

from test.pylib.manager_client import ManagerClient
from test.cluster.conftest import *


# Mounting volumes outside of the toolchain environment, requires root privileges. To overcome it,
# `unshare` is used to launch tests in a separate namespace. This is a temporary solution as it
# works with test.py but breaks the execution of the tests with pytest without test.py.
# FIXME: Find a more robust solution to ditch unshare.
@pytest.fixture(scope="function")
def volumes_factory(pytestconfig, build_mode, request):
    hash = str(uuid.uuid4())
    base = pathlib.Path(f"{pytestconfig.getoption("tmpdir")}/{build_mode}/volumes/{hash}")
    volumes = []

    @contextmanager
    def wrapper(sizes: list[str]):
        try:
            for id, size in enumerate(sizes):
                path = base / f"scylla-{id}"
                path.mkdir(parents=True)

                subprocess.run(["sudo", "mount", "-o", f"size={size}", "-t", "tmpfs", "tmpfs", path], check=True)
                volumes.append(path)
            yield volumes
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
    report = request.node.stash[PHASE_REPORT_KEY]
    test_failed = report.when == "call" and report.failed
    preserve_data = test_failed or request.config.getoption("save_log_on_success")

    if preserve_data:
        for id, path in enumerate(volumes):
            shutil.copytree(path, base.parent.parent / f"scylla-{hash}-{id}", ignore=shutil.ignore_patterns('commitlog*'))


@asynccontextmanager
async def space_limited_servers(manager: ManagerClient, volumes_factory: Callable, sizes: list[str], **server_args):
    servers = []
    cmdline = server_args.pop("cmdline", [])
    with volumes_factory(sizes) as volumes:
        try:
            servers = [await manager.server_add(cmdline = [*cmdline, '--workdir', str(path)],
                                                property_file={"dc": "dc1", "rack": f"r{id}"},
                                                **server_args) for id, path in enumerate(volumes)]
            yield servers
        finally:
            pass
