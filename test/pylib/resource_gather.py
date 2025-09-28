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
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING

import psutil

from test import HOST_ID
from test.pylib.db.model import Metric, SystemResourceMetric, CgroupMetric, Test
from test.pylib.db.writer import (
    CGROUP_MEMORY_METRICS_TABLE,
    DEFAULT_DB_NAME,
    METRICS_TABLE,
    SYSTEM_RESOURCE_METRICS_TABLE,
    TESTS_TABLE,
    SQLiteWriter,
)

if TYPE_CHECKING:
    from asyncio import Task, Event
    from typing import TextIO

    from test.pylib.suite.base import Test as TestPyTest

logger = logging.getLogger(__name__)

@lru_cache(maxsize=None)
def get_cgroup() -> Path:
    cgroup_path = f"/proc/{os.getpid()}/cgroup"
    with open(cgroup_path, 'r') as f:
        cgroup_info = f.readlines()
    # Extract the relative cgroup for the process and make it absolute and add where the test.py process should be
    # placed in.
    # This can be used to manipulate the cgroup's controllers
    cgroup_name = Path(f"/sys/fs/cgroup/{cgroup_info[0].strip().split(':')[-1]}")
    if cgroup_name.stem != 'resource_gather':
        cgroup_name = cgroup_name / 'resource_gather'
    return cgroup_name


CGROUP_INITIAL = get_cgroup()
CGROUP_TESTS = CGROUP_INITIAL.parent / 'tests'


class ResourceGather(ABC):
    def __init__(self, test: TestPyTest):
        # get the event loop for the current thread or create a new one if there's none
        try:
            self.loop = asyncio.get_running_loop()
            self.own_loop = False
        except RuntimeError:
            self.loop = asyncio.new_event_loop()
            self.own_loop = True
        self.test = test
        self.db_path = self.test.suite.log_dir.parent / DEFAULT_DB_NAME
        standardized_name = self.test.shortname.replace("/", "_")
        self.cgroup_path = Path(
            f"{CGROUP_TESTS}/{self.test.suite.name}.{standardized_name}.{self.test.mode}.{self.test.id}"
        )
        self.logger = logging.getLogger(__name__)

    def __del__(self):
        if self.own_loop:
            self.loop.close()

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

    def cgroup_monitor(self, test_event: Event) -> Task:
        pass

    def remove_cgroup(self) -> None:
        pass


class ResourceGatherOff(ResourceGather):
    def cgroup_monitor(self, test_event: Event) -> Task:
        return self.loop.create_task(no_monitor())


class ResourceGatherOn(ResourceGather):
    def __init__(self, test: TestPyTest):
        super().__init__(test)
        self.sqlite_writer = SQLiteWriter(self.db_path)
        self.test_id: int = self.sqlite_writer.write_row_if_not_exist(
            Test(
                host_id=HOST_ID,
                architecture=platform.machine(),
                directory=test.suite.name,
                mode=test.mode,
                run_id=test.id,
                test_name=test.shortname,
            ),
            TESTS_TABLE)

    def make_cgroup(self) -> None:
        os.makedirs(self.cgroup_path, exist_ok=True)

    def get_test_metrics(self) -> Metric:
        test_metrics: Metric = Metric(test_id=self.test_id, host_id=HOST_ID)
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
        stop_monitoring = asyncio.Event()

        self.test.time_start = time.time()
        test_resource_watcher = self.cgroup_monitor(test_event=stop_monitoring)
        try:
            p = super().run_process(args=args, timeout=timeout, output_file=output_file, cwd=cwd, env=env)
        finally:
            stop_monitoring.set()
            self.test.time_end = time.time()
            self.loop.run_until_complete(asyncio.gather(test_resource_watcher))
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
        try:
            os.rmdir(self.cgroup_path)
        except OSError as e:
            logger.warning(f'Can\'t delete cgroup directory: {e.strerror}' )

    def cgroup_monitor(self, test_event: Event) -> Task:
        return self.loop.create_task(self._monitor_cgroup(test_event))

    async def _monitor_cgroup(self, test_event: Event) -> None:
        """Continuously monitors CPU and memory utilization."""
        try:
            while not test_event.is_set():
                with  open(self.cgroup_path / 'memory.current', 'r') as memory_current:
                    timeline_record = CgroupMetric(
                        test_id=self.test_id,
                        host_id=HOST_ID,
                        memory=int(memory_current.read()),
                        timestamp=datetime.now()
                    )

                    self.sqlite_writer.write_row(timeline_record, CGROUP_MEMORY_METRICS_TABLE)

                # Control the frequency of updates, for example, every 2 seconds
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            self.logger.info(f'cgroup monitoring job was cancelled')

    @staticmethod
    def _parse_cpu_stat(file: TextIO, metrics: Metric) -> None:
        # Map the values from cpu.state to the model. Keys are values from cpu.stat
        stats = {'user_usec': 'user_sec', 'system_usec': 'system_sec', 'usage_usec': 'usage_sec'}
        for line in file.readlines():
            stat, value = line.split(' ')
            if stat in stats.keys():
                setattr(metrics, stats[stat], float(value) / 1_000_000)


def get_resource_gather(is_switched_on: bool, test: TestPyTest | SimpleNamespace) -> ResourceGather:
    if is_switched_on:
        return ResourceGatherOn(test)
    else:
        return ResourceGatherOff(test)


def _is_cgroup_rw() -> bool:
    with open('/proc/mounts', 'r') as f:
        for line in f.readlines():
            if 'cgroup2' in line :
                options = line.split(' ')[3].split(',')
                if 'rw' in options:
                    return True
                else:
                    return False


def setup_cgroup(is_required: bool) -> None:
    if is_required:
        # check where the process is executed in podman or in docker
        is_podman = os.access("/run/.containerenv", os.F_OK)
        is_docker = os.access("/.dockerenv", os.F_OK)

        if _is_cgroup_rw() and is_docker:
            subprocess.run(
                [
                    "mount",
                    "-o",
                    "remount,rw",
                    "/sys/fs/cgroup",
                ],
                check=True,
            )

        if is_podman or is_docker:
            subprocess.run(['chown', '-R', f"{getpass.getuser()}:{getpass.getuser()}", '/sys/fs/cgroup'],
                           check=True)

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

            with open(CGROUP_INITIAL.parent / 'cgroup.controllers', "r") as f:
                controllers = f.readline()
            controllers = " ".join(map(lambda x: f"+{x}", controllers.split(" ")))

            with open(CGROUP_INITIAL.parent / 'cgroup.subtree_control', "w") as f:
                f.write(controllers)

            with open(CGROUP_TESTS / 'cgroup.subtree_control', "w") as f:
                f.write(controllers)


async def monitor_resources(cancel_event: Event, stop_event: Event, tmpdir: Path) -> None:
    """Continuously monitors CPU and memory utilization."""
    sqlite_writer = SQLiteWriter(tmpdir / DEFAULT_DB_NAME)
    while not cancel_event.is_set() and not stop_event.is_set():
        timeline_record = SystemResourceMetric(
            host_id=HOST_ID,
            cpu=psutil.cpu_percent(interval=0.1),
            memory=psutil.virtual_memory().percent,
            timestamp=datetime.now(),
        )

        sqlite_writer.write_row(timeline_record, SYSTEM_RESOURCE_METRICS_TABLE)

        # Control the frequency of updates, for example, every 2 seconds
        await asyncio.sleep(2)


async def no_monitor() -> None:
    pass


def run_resource_watcher(is_required: bool, cancel_event: Event, stop_event:Event, tmpdir: str) -> Task:
    if is_required:
        return asyncio.create_task(monitor_resources(cancel_event, stop_event, Path(tmpdir)))
    return asyncio.create_task(no_monitor())
