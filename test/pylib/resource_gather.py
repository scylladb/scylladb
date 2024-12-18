#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import asyncio
import getpass
import logging
import os
import platform
import subprocess
from abc import ABC
from asyncio import Task, Event
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import TextIO

import psutil

from test.pylib.db.model import Metric, SystemResourceMetric, CgroupMetric, Test
from test.pylib.db.writer import DATE_TIME_TEMPLATE, SQLiteWriter, SYSTEM_RESOURCE_METRICS_TABLE, METRICS_TABLE, \
    DEFAULT_DB_NAME, CGROUP_MEMORY_METRICS_TABLE, TESTS_TABLE

@lru_cache(maxsize=None)
def get_cgroup() -> Path:
    cgroup_path = f"/proc/{os.getpid()}/cgroup"
    with open(cgroup_path, 'r') as f:
        cgroup_info = f.readlines()
    # Extract the relative cgroup for the process and make it absolute and add where the test.py process should be
    # placed in.
    # This can be used to manipulate the cgroup's controllers
    return Path(f"/sys/fs/cgroup/{cgroup_info[0].strip().split(':')[-1]}/initial")

CGROUP_INITIAL: Path = get_cgroup()
CGROUP_TESTS: Path = CGROUP_INITIAL.parent / 'tests'
cancel_event_global = None
stop_event_global = None

class ResourceGather(ABC):
    def __init__(self, test, tmp_dir: str):
        self.test = test
        self.db_path = Path(tmp_dir) / DEFAULT_DB_NAME
        standardized_name = self.test.shortname.replace("/", "_")
        self.cgroup_path = Path(
            f"{CGROUP_TESTS}/{self.test.suite.name}.{standardized_name}.{self.test.suite.mode}.{self.test.id}"
        )
        self.logger = logging.getLogger(__name__)

    def make_cgroup(self):
        pass

    def put_process_to_cgroup(self):
        os.setsid()

    def get_test_metrics(self) -> Metric:
        pass

    def write_metrics_to_db(self, metrics: Metric, success: bool = False) -> None:
        pass

    def cgroup_monitor(self, test_event: Event):
        pass

    def remove_cgroup(self):
        pass


class ResourceGatherOff(ResourceGather):
    def cgroup_monitor(self, test_event) -> Task:
        return asyncio.create_task(no_monitor())


class ResourceGatherOn(ResourceGather):
    def __init__(self, test, tmp_dir: str):
        super().__init__(test, tmp_dir)
        self.sqlite_writer = SQLiteWriter(self.db_path)
        self.test_id: int = self.sqlite_writer.write_row_if_not_exist(
            Test(
                architecture=platform.machine(),
                directory=test.suite.name,
                mode=test.mode,
                run_id=test.id,
                test_name=test.shortname
            ),
            TESTS_TABLE)

    def make_cgroup(self):
        os.makedirs(self.cgroup_path, exist_ok=True)

    def get_test_metrics(self) -> Metric:
        test_metrics: Metric = Metric(test_id=self.test_id)
        test_metrics.time_taken = self.test.time_end - self.test.time_start
        test_metrics.time_start = datetime.fromtimestamp(self.test.time_start).strftime(DATE_TIME_TEMPLATE)
        test_metrics.time_end = datetime.fromtimestamp(self.test.time_end).strftime(DATE_TIME_TEMPLATE)
        test_metrics.success = self.test.success
        with open(self.cgroup_path / 'memory.peak', 'r') as file:
            test_metrics.memory_peak = file.read()

        if (self.cgroup_path / 'cpu.stat').exists():
            with open(self.cgroup_path / 'cpu.stat', 'r', ) as file:
                self._parse_cpu_stat(file, test_metrics)
        return test_metrics

    def write_metrics_to_db(self, metrics: Metric, success: bool = False) -> None:
        metrics.success = success
        self.sqlite_writer.write_row(metrics, METRICS_TABLE)


    def put_process_to_cgroup(self):
        super().put_process_to_cgroup()
        pid = os.getpid()
        with open(self.cgroup_path / 'cgroup.procs', "a") as cgroup:
            cgroup.write(str(pid))

    def remove_cgroup(self):
        os.rmdir(self.cgroup_path)

    def cgroup_monitor(self, test_event: Event) -> Task:
        return asyncio.create_task(self._monitor_cgroup(test_event))

    async def _monitor_cgroup(self, test_event: Event) -> None:
        """Continuously monitors CPU and memory utilization."""
        try:
            while not test_event.is_set():
                with  open(self.cgroup_path / 'memory.current', 'r') as memory_current:
                    timeline_record = CgroupMetric(
                        test_id=self.test_id,
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


def get_resource_gather(is_switched_on: bool, test, tmpdir: str) -> ResourceGather:
    if is_switched_on:
        return ResourceGatherOn(test, tmpdir)
    else:
        return ResourceGatherOff(test, tmpdir)

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
                    "sudo",
                    "mount",
                    "-o",
                    "remount,rw",
                    "/sys/fs/cgroup",
                ],
                check=True,
            )

        if is_podman or is_docker:
            subprocess.run(['sudo', 'chown', '-R', f"{getpass.getuser()}:{getpass.getuser()}", '/sys/fs/cgroup'],
                           check=True)

        for directory in [CGROUP_INITIAL, CGROUP_TESTS]:
            if directory.exists():
                os.rmdir(directory)
            directory.mkdir()

        with open(CGROUP_INITIAL.parent / 'cgroup.procs') as f:
            processes = [x.strip() for x in f.readlines()]

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


async def monitor_resources(cancel_event: asyncio.Event, stop_event: asyncio.Event, tmpdir: Path) -> None:
    """Continuously monitors CPU and memory utilization."""
    sqlite_writer = SQLiteWriter(tmpdir / DEFAULT_DB_NAME)
    while not cancel_event.is_set() and not stop_event.is_set():
        timeline_record = SystemResourceMetric(
            cpu=psutil.cpu_percent(interval=0.1),
            memory=psutil.virtual_memory().percent,
            timestamp=datetime.now()
        )

        sqlite_writer.write_row(timeline_record, SYSTEM_RESOURCE_METRICS_TABLE)

        # Control the frequency of updates, for example, every 2 seconds
        await asyncio.sleep(2)


async def no_monitor():
    pass


def run_resource_watcher(is_required, cancel_event, stop_event, tmpdir: str) -> Task:
    if is_required:
        return asyncio.create_task(monitor_resources(cancel_event, stop_event, Path(tmpdir)))
    return asyncio.create_task(no_monitor())
