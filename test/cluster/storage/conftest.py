#!/usr/bin/python3
#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import pytest
import logging
import os
import pathlib
import shutil
import subprocess
import time
import uuid

from typing import Callable
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass

from test.cluster.conftest import PHASE_REPORT_KEY
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import gather_safely

logger = logging.getLogger(__name__)

BLOCK_SIZE = 2048


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
                # 2 KB blocks: mkfs picks 1 KB for small images and fuse2fs then writes the image in 1 KB pieces, which is slow under load.
                # 4 KB would be faster still, but rounds every small sstable file up to 4 KB and uses more space than 1 KB blocks with
                # a journal once the node has some hundreds of files, which tips these tests over the critical disk utilization level.
                # No journal: fuse2fs does not use it, and it would take ~1.4 MB of the small volume.
                # The last block is marked bad to work around e2fsprogs 509da98991 (fixed in 1.47.4): freeing an extent that
                # ends at the last block fails with EXT2_ET_BAD_BLOCK_NUM, which can make fuse2fs remount the volume read-only.
                bad_blocks = path.with_name(f"{path.name}.bad_blocks")
                bad_blocks.write_text(f"{volume.img.stat().st_size // BLOCK_SIZE - 1}\n")
                subprocess.run(["mkfs.ext4", "-b", str(BLOCK_SIZE), "-O", "^has_journal", "-l", bad_blocks, volume.img], check=True, stdout=subprocess.DEVNULL)
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
    reports = request.node.stash[PHASE_REPORT_KEY]
    call_report = reports.get("call")
    test_failed = call_report is not None and call_report.failed
    preserve_data = test_failed or request.config.getoption("save_log_on_success")

    for id, volume in enumerate(volumes):
        if preserve_data:
            shutil.copytree(volume.mount, base.parent.parent / f"scylla-{hash}-{id}", ignore=shutil.ignore_patterns('commitlog*', 'lost+found*'))
            shutil.copyfile(volume.log, base.parent.parent / f"scylla-{hash}-{id}.log")

        retries = 10
        for attempt in range(retries):
            result = subprocess.run(["fusermount3", "-u", volume.mount], capture_output=True, text=True)
            if result.returncode == 0:
                break
            logger.warning("fusermount3 -u attempt %d/%d failed: %s", attempt + 1, retries, result.stderr.strip())
            if attempt + 1 == retries:
                raise subprocess.CalledProcessError(result.returncode, result.args, result.stdout, result.stderr)
            time.sleep(0.5)
        os.unlink(volume.img)


@asynccontextmanager
async def space_limited_servers(manager: ScyllaClusterManager, volumes_factory: Callable, sizes: list[str], property_file=None, **server_args):
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
            await gather_safely(*(manager.server_stop(server.server_id, convict=False) for server in servers))
