#!/usr/bin/python3
#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
import argparse
import pytest
import pathlib
import shutil

from test.pylib.manager_client import ManagerClient


def volume_paths(string) -> list[pathlib.Path]:
    if not string:
        return []

    volumes = [pathlib.Path(path).absolute() for path in string.split(';') if path]
    if len(volumes) != 3:
        raise argparse.ArgumentTypeError(f"Invalid number of volume. Must provide 3 semicolon separated paths. Got {len(volumes)}")

    if len(set(volumes)) != 3:
        raise argparse.ArgumentTypeError(f"Got duplicates {volumes}. Must provide 3 semicolon-separated unique paths")

    for path in volumes:
        if not (path.exists() and path.is_dir() and path.is_mount()):
            raise argparse.ArgumentTypeError(f"Received path ({path}) does not exist or is not a dictionary or is not a mounting point")

    return volumes


def pytest_addoption(parser):
    parser.addoption('--space-limited-dirs', action='store', type=volume_paths, default=None, dest='space_limited_dirs',
                     help='Three semicolon separated volumes which are space limited to 50mb')


def clean_space_limited_directory(path: pathlib.Path):
    for child in path.iterdir():
        if child.is_file():
            child.unlink()
        else:
            shutil.rmtree(child)


@pytest.fixture(scope="function")
def space_limited_directories(pytestconfig):
    space_directories = pytestconfig.getoption("space_limited_dirs")
    if not space_directories:
        pytest.skip("Space limited directories not available")

    for path in space_directories:
        clean_space_limited_directory(path)

    yield space_directories


@pytest.fixture(scope="function")
async def manager(manager: ManagerClient, space_limited_directories: list[pathlib.Path]):
    cmdline = ["--disk-space-monitor-normal-polling-interval-in-seconds", "1",
               "--critical-disk-utilization-level", "0.8",
               "--hinted-handoff-enabled", "0",
               "--logger-log-level", "compaction=debug",
               ]
    for id, path in enumerate(space_limited_directories, 1):
        await manager.server_add(cmdline=[*cmdline, '--workdir', str(path)], property_file={"dc": "dc1", "rack": f"r{id}"})

    yield manager
