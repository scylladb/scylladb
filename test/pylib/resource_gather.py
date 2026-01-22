#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from __future__ import annotations

import asyncio
import getpass
import logging
import os
import platform
import shlex
import subprocess
import time
from abc import ABC
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from time import sleep
from types import SimpleNamespace
from typing import TYPE_CHECKING, Coroutine

import psutil

from multiprocessing import Event
from test import HOST_ID, TOP_SRC_DIR
from test.pylib.db.model import Metric, SystemResourceMetric, CgroupMetric, Test
from test.pylib.db.writer import (
    CGROUP_MEMORY_METRICS_TABLE,
    DEFAULT_DB_NAME,
    METRICS_TABLE,
    SYSTEM_RESOURCE_METRICS_TABLE,
    TESTS_TABLE,
    SQLiteWriter,
)
from test.pylib.util import get_xdist_worker_id

if TYPE_CHECKING:
    from typing import TextIO

    from test.pylib.suite.base import Test as TestPyTest

logger = logging.getLogger(__name__)


def get_current_cgroup() -> Path:
    """Get the current cgroup path for this process."""
    with open("/proc/self/cgroup", 'r') as f:
        cgroup_info = f.readlines()
    return Path(f"/sys/fs/cgroup/{cgroup_info[0].strip().split(':')[-1]}")

@lru_cache(maxsize=None)
def get_cgroup() -> Path:
    cgroup_name = get_current_cgroup()
    if cgroup_name.stem != 'resource_gather':
        cgroup_name = cgroup_name / 'resource_gather'
    return cgroup_name


CGROUP_INITIAL = get_cgroup()
CGROUP_TESTS = CGROUP_INITIAL.parent / 'tests'


class ResourceGather(ABC):
    def run_process(self,
                    args: list[str],
                    timeout: float,
                    output_file: Path,
                    cwd: Path | None = None,
                    env: dict | None = None) -> subprocess.Popen[str]:

        args = shlex.split(subprocess.list2cmdline(args))
        if env:
            env.update(os.environ)
        else:
            env = os.environ.copy()
        p = subprocess.Popen(
            args=args,
            bufsize=1,
            stdout=output_file.open(mode="w", encoding="utf-8"),
            stderr=subprocess.STDOUT,
            preexec_fn=self.put_process_to_cgroup,
            close_fds=True,
            cwd=cwd,
            env=env,
            text=True,
        )
        try:
            p.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            logger.critical(f"Process {args} timed out")
            p.kill()
            p.communicate()
        except KeyboardInterrupt:
            p.kill()
            raise
        return p

    def make_cgroup(self) -> None:
        pass

    def put_process_to_cgroup(self) -> None:
        os.setsid()

    def get_test_metrics(self) -> Metric:
        pass

    def write_metrics_to_db(self, metrics: Metric, success: bool = False) -> None:
        pass

    def remove_cgroup(self) -> None:
        pass

    def stop_monitoring(self):
        pass

    def cgroup_monitor(self):
        pass


class ResourceGatherOff(ResourceGather):
    pass

class ResourceGatherOn(ResourceGather):
    """Resource gatherer for C++ tests run via subprocess."""

    def __init__(self, temp_dir: Path, test: TestPyTest, worker_id: str | None = None):
        self.pool = ThreadPoolExecutor(max_workers=1)
        self.future = None
        self.event = Event()
        self.test = test
        self.worker_id = worker_id
        self.db_path = temp_dir / DEFAULT_DB_NAME
        self.sqlite_writer = SQLiteWriter(self.db_path)
        self.cgroup_name = f'{self.test.suite.name}.{self.test.shortname.replace("/", "_")}.{self.test.mode}.{self.test.id}'
        self.cgroup_path = Path(f'{CGROUP_TESTS}/{self.worker_id}/{self.cgroup_name}')
        self.logger = logging.getLogger(__name__)


        # Calculate relative directory path from repo root
        directory_path = str(test.suite.suite_path.relative_to(TOP_SRC_DIR))

        self.test_id: int = self.sqlite_writer.write_row_if_not_exist(
            Test(
                host_id=HOST_ID,
                architecture=platform.machine(),
                path=directory_path,
                file=test.suite.test_file_name,
                mode=test.mode,
                run_id=test.id,
                test_name=test.shortname,
            ),
            TESTS_TABLE)
    @staticmethod
    def _move_processes_to_cgroup(source_cgroup: Path, target_cgroup: Path) -> None:
        try:
            with open(source_cgroup / 'cgroup.procs', 'r') as f:
                pids = [line.strip() for line in f.readlines() if line.strip()]

            test_procs_path = target_cgroup / 'cgroup.procs'
            for pid in pids:
                try:
                    with open(test_procs_path, 'w') as f:
                        f.write(pid)
                except OSError as e:
                    # Process might have exited, that's OK
                    logger.debug(f"Could not move PID {pid} to test cgroup: {e}")
        except Exception as e:
            logger.warning(f"Failed to move processes to test cgroup: {e}")

    def stop_monitoring(self) -> None:
        self.event.set()
        self.future.result()
        self.pool.shutdown(wait=True)

    def cgroup_monitor(self) -> None:
        self.future = self.pool.submit(asyncio.run, self._monitor_cgroup())

    async def _monitor_cgroup(self) -> None:
        """Continuously monitors CPU and memory utilization."""
        memory_current = self.cgroup_path / 'memory.current'
        sqlite_writer = SQLiteWriter(self.db_path)
        try:
            while not self.event.is_set():
                timeline_record = CgroupMetric(
                    test_id=self.test_id,
                    host_id=HOST_ID,
                    memory=int(memory_current.read_text().strip()),
                    timestamp=datetime.now()
                )
                sqlite_writer.write_row(timeline_record, CGROUP_MEMORY_METRICS_TABLE)

                # Control the frequency of updates, for example, every 2 seconds
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            self.logger.info(f'cgroup monitoring job was cancelled')

    def make_cgroup(self) -> None:
        os.makedirs(self.cgroup_path, exist_ok=True)
        self._move_processes_to_cgroup(source_cgroup=get_current_cgroup(), target_cgroup=self.cgroup_path)

    def get_test_metrics(self) -> Metric:
        test_metrics: Metric = Metric(test_id=self.test_id, host_id=HOST_ID, worker_id=self.worker_id)
        test_metrics.time_taken = self.test.time_end - self.test.time_start
        test_metrics.time_start = datetime.fromtimestamp(self.test.time_start)
        test_metrics.time_end = datetime.fromtimestamp(self.test.time_end)
        test_metrics.success = self.test.success
        memory_peak = self.cgroup_path / 'memory.peak'
        if memory_peak.exists():
            with open(memory_peak, 'r') as file:
                test_metrics.memory_peak = file.read()

        cpu_stat = self.cgroup_path / 'cpu.stat'
        if cpu_stat.exists():
            with open(cpu_stat, 'r', ) as file:
                self._parse_cpu_stat(file, test_metrics)
        return test_metrics

    def run_process(self,
                    args: list[str],
                    timeout: float,
                    output_file: Path,
                    cwd: Path | None = None,
                    env: dict | None = None) -> subprocess.Popen[str]:

        self.test.time_start = time.time()
        self.cgroup_monitor()
        try:
            p = super().run_process(args=args, timeout=timeout, output_file=output_file, cwd=cwd, env=env)
        finally:
            self.test.time_end = time.time()
            self.stop_monitoring()
        return p

    def write_metrics_to_db(self, metrics: Metric, success: bool = False) -> None:
        metrics.success = success
        self.sqlite_writer.write_row(metrics, METRICS_TABLE)

    def put_process_to_cgroup(self) -> None:
        super().put_process_to_cgroup()
        try:
            pid = os.getpid()
            with open(self.cgroup_path / 'cgroup.procs', "a") as cgroup:
                cgroup.write(str(pid))
        except Exception as e:
            logger.warning('Test %s is not moved to cgroup: %s', self.test, e)

    def remove_cgroup(self) -> None:
        self._move_processes_to_cgroup(source_cgroup=self.cgroup_path,
                                       target_cgroup=CGROUP_INITIAL / self.worker_id / 'default')
        try:
            os.rmdir(self.cgroup_path)
        except OSError as e:
            logger.warning(f'Can\'t delete cgroup directory: {e.strerror}' )

    @staticmethod
    def _parse_cpu_stat(file: TextIO, metrics: Metric) -> None:
        # Map the values from cpu.state to the model. Keys are values from cpu.stat
        stats = {'user_usec': 'user_sec', 'system_usec': 'system_sec', 'usage_usec': 'usage_sec'}
        for line in file.readlines():
            stat, value = line.split(' ')
            if stat in stats.keys():
                setattr(metrics, stats[stat], float(value) / 1_000_000)


def get_resource_gather(temp_dir: Path, is_switched_on: bool, test: TestPyTest | SimpleNamespace, worker_id: str | None = None) -> ResourceGather:
    """Get resource gatherer for C++ tests run via subprocess."""
    if is_switched_on:
        return ResourceGatherOn(temp_dir, test, worker_id)
    else:
        return ResourceGatherOff()


def _is_cgroup_rw() -> bool:
    with open('/proc/mounts', 'r') as f:
        for line in f.readlines():
            if 'cgroup2' in line :
                options = line.split(' ')[3].split(',')
                if 'rw' in options:
                    return True
                else:
                    return False

def propagate_subtree_controls(group: Path):
    with open(group / 'cgroup.controllers', 'r') as f:
        controllers = f.readline().strip()
    controllers = " ".join(map(lambda x: f"+{x}", controllers.split(" ")))
    with open(group / 'cgroup.subtree_control', 'w') as f:
        f.write(controllers)


def setup_cgroup(is_required: bool) -> None:
    if is_required:
        # check where the process is executed in podman or in docker
        is_podman = os.access("/run/.containerenv", os.F_OK)
        is_docker = os.access("/.dockerenv", os.F_OK)

        if _is_cgroup_rw() and is_docker:
            subprocess.run(
                [
                    "sudo",
                    "mount",
                    "-o",
                    "remount,rw",
                    "/sys/fs/cgroup",
                ],
                check=True,
            )

        if is_docker:
            cmd = ["sudo", "chown", "-R", f"{getpass.getuser()}:{getpass.getuser()}", '/sys/fs/cgroup']
            subprocess.run(cmd, check=True)

        configured = False
        for directory in [CGROUP_INITIAL, CGROUP_TESTS]:
            if not directory.exists():
                directory.mkdir()
            else:
                configured = True


        if not configured:
            with open(CGROUP_INITIAL.parent / 'cgroup.procs') as f:
                processes = [line.strip() for line in f.readlines()]

            for process in processes:
                with open(CGROUP_INITIAL / 'cgroup.procs', "w") as f:
                    f.write(str(process))

            for group in [CGROUP_INITIAL.parent, CGROUP_TESTS]:
                propagate_subtree_controls(group)


def setup_worker_cgroup() -> None:
    worker_id = get_xdist_worker_id() or "master"
    # this method is creating the worker cgroup, but the main cgroup is created in the master thread, so this is just to
    # avoid race conditions
    for i in range(10):
        if CGROUP_TESTS.exists():
            break
        time.sleep(0.5)
    worker_cgroup_path = CGROUP_TESTS / worker_id
    worker_cgroup_path_default = worker_cgroup_path / 'default'
    for group in [worker_cgroup_path, worker_cgroup_path_default]:
        if not group.exists():
            group.mkdir()
    else:
        propagate_subtree_controls(worker_cgroup_path)


def monitor_resources(stop: Event, tmpdir: Path) -> None:
    """Continuously monitors CPU and memory utilization."""
    sqlite_writer = SQLiteWriter(tmpdir / DEFAULT_DB_NAME)
    while not stop.is_set():
        timeline_record = SystemResourceMetric(
            host_id=HOST_ID,
            cpu=psutil.cpu_percent(interval=0.1),
            memory=psutil.virtual_memory().percent,
            timestamp=datetime.now(),
        )

        sqlite_writer.write_row(timeline_record, SYSTEM_RESOURCE_METRICS_TABLE)

        # Control the frequency of updates, for example, every 2 seconds
        sleep(2)

def run_resource_watcher(is_required: bool, stop_event: Event, tmpdir: Path) -> None:
    if is_required:
        monitor_resources(stop_event, tmpdir)
    return None
