import asyncio
import getpass
import logging
import os
import platform
import subprocess
from abc import ABC
from asyncio import Task
from datetime import datetime
from pathlib import Path
from typing import TextIO

import psutil
from attrs import asdict

from test.pylib.db.model import TestMetricRecord, ResourceTimelineRecord
from test.pylib.db.writer import DATE_TIME_TEMPLATE, SQLiteWriter, RESOURCE_TIMELINE_TABLE, METRICS_TABLE, \
    DEFAULT_DB_NAME

CGROUP_INITIAL: Path = Path('/sys/fs/cgroup/initial')
CGROUP_TESTS: Path = CGROUP_INITIAL.parent / 'tests'


class ResourceGather(ABC):
    def __init__(self, test, tmp_dir: str):
        self.test = test
        self.db_path = Path(tmp_dir) / DEFAULT_DB_NAME
        standardized_name = self.test.shortname.replace("/", "_")
        self.cgroup_path = Path(
            f"{CGROUP_TESTS}/{self.test.suite.name}.{standardized_name}.{self.test.suite.mode}.{self.test.id}"
        )

    def make_cgroup(self):
        pass

    def put_process_to_cgroup(self, pid):
        pass

    def get_test_metrics(self) -> TestMetricRecord:
        pass

    def write_metrics_to_db(self, metrics: TestMetricRecord, success: bool = False) -> None:
        pass


class ResourceGatherOff(ResourceGather):
    pass


class ResourceGatherOn(ResourceGather):
    def __init__(self, test, tmp_dir: str):
        super().__init__(test, tmp_dir)
        self.sqlite_writer = SQLiteWriter(self.db_path)

    def make_cgroup(self):
        os.makedirs(self.cgroup_path, exist_ok=True)

    def put_process_to_cgroup(self, pid):
        try:
            with open(self.cgroup_path / 'cgroup.procs', "a") as cgroup:
                cgroup.write(str(pid))
        except ProcessLookupError:
            logging.getLogger().log(
                level=logging.WARNING,
                msg=f"Process finished too quick. No metrics will be logged",
            )

    def get_test_metrics(self) -> TestMetricRecord:
        test_metrics = TestMetricRecord()
        uname_split = self.test.uname.split('.')
        test_metrics.directory = uname_split[0]
        test_metrics.run_id = uname_split[-1]
        test_metrics.test_name = self.test.uname
        test_metrics.mode = self.test.mode
        test_metrics.architecture = platform.machine()
        test_metrics.time_taken = round(int(self.test.time_end - self.test.time_start))
        test_metrics.time_start = datetime.fromtimestamp(self.test.time_start).strftime(DATE_TIME_TEMPLATE)
        test_metrics.time_end = datetime.fromtimestamp(self.test.time_end).strftime(DATE_TIME_TEMPLATE)
        test_metrics.success = self.test.success
        with open(self.cgroup_path / 'memory.peak', 'r') as file:
            test_metrics.memory_peak = file.read()

        if (self.cgroup_path / 'cpu.stat').exists():
            with open(self.cgroup_path / 'cpu.stat', 'r', ) as file:
                self._parse_cpu_stat(file, test_metrics)
        return test_metrics

    def write_metrics_to_db(self, metrics: TestMetricRecord, success: bool = False) -> None:
        metrics.success = success
        metrics_dict = asdict(metrics, filter=lambda attr, value: value is not None)
        self.sqlite_writer.write_row(metrics_dict, METRICS_TABLE)

    @staticmethod
    def _parse_cpu_stat(file: TextIO, metrics: TestMetricRecord):
        for stat in ['user_usec', 'system_usec', 'usage_usec']:
            for line in file.readlines():
                if line.startswith(stat):
                    setattr(metrics, stat, float(line.split()[-1]))


def get_resource_gather(is_switched_on, test, tmpdir: str) -> ResourceGather:
    if is_switched_on:
        return ResourceGatherOn(test, tmpdir)
    else:
        return ResourceGatherOff(test, tmpdir)


def setup_cgroup(is_required: bool):
    if is_required:
        # check that we are in podman or in docker
        is_podman = os.access("/run/.containerenv", os.F_OK)
        if is_podman:
            subprocess.run(['sudo', 'chown', '-R', f"{getpass.getuser()}:{getpass.getuser()}", '/sys/fs/cgroup'],
                           check=True)
        else:
            subprocess.run(
                [
                    "sudo",
                    "mount",
                    "-o",
                    "remount,rw,memory_recursiveprot,nsdelegate",
                    "/sys/fs/cgroup",
                ],
                check=True,
            )

        for directory in [CGROUP_INITIAL, CGROUP_TESTS]:
            directory.mkdir()

        with open(CGROUP_INITIAL / 'cgroup.procs', "w") as f:
            f.write(str(os.getpid()))

        with open(CGROUP_INITIAL.parent / 'cgroup.controllers', "r") as f:
            controllers = f.readline()
        controllers = " ".join(map(lambda x: f"+{x}", controllers.split(" ")))

        with open(CGROUP_INITIAL.parent / 'cgroup.subtree_control', "w") as f:
            f.write(controllers)

        with open(CGROUP_TESTS / 'cgroup.subtree_control', "w") as f:
            f.write(controllers)


async def monitor_resources(cancel_event: asyncio.Event, stop_event: asyncio.Event, tmpdir: Path):
    """Continuously monitors CPU and memory utilization."""
    sqlite_writer = SQLiteWriter(tmpdir / DEFAULT_DB_NAME)
    timeline_record = ResourceTimelineRecord()
    while not cancel_event.is_set() and not stop_event.is_set():
        timeline_record.cpu = psutil.cpu_percent(interval=1)
        timeline_record.memory = psutil.virtual_memory().percent
        timeline_record.timestamp = datetime.now().strftime(DATE_TIME_TEMPLATE)

        sqlite_writer.write_row(asdict(timeline_record), RESOURCE_TIMELINE_TABLE)

        # Control the frequency of updates (e.g., every 2 seconds)
        await asyncio.sleep(2)


async def no_monitor():
    pass


def run_resource_watcher(is_required, cancel_event, stop_event, tmpdir: str) -> Task:
    if is_required:
        return asyncio.create_task(monitor_resources(cancel_event, stop_event, Path(tmpdir)))
    return asyncio.create_task(no_monitor())
