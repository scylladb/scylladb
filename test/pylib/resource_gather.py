#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from __future__ import annotations

import getpass
import logging
import os
import platform
import shlex
import sqlite3
import subprocess
import time
from abc import ABC
from concurrent.futures.thread import ThreadPoolExecutor
from contextlib import closing
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from statistics import fmean, median, quantiles
from time import sleep
from types import SimpleNamespace
from typing import TYPE_CHECKING

import psutil

from threading import Event
from test import HOST_ID, TOP_SRC_DIR
from test.pylib.internal_types import SeastarIOMetricName
from test.pylib.db.model import HostInfo, Metric, ResourceUtilization, SystemResourceMetric, CgroupMetric, Test
from test.pylib.db.writer import (
    CGROUP_MEMORY_METRICS_TABLE,
    DEFAULT_DB_NAME,
    HOST_INFO_TABLE,
    METRICS_TABLE,
    RESOURCE_UTILIZATION_TABLE,
    SYSTEM_RESOURCE_METRICS_TABLE,
    TESTS_TABLE,
    SQLiteWriter,
)

if TYPE_CHECKING:
    from typing import IO, TextIO

logger = logging.getLogger(__name__)


def get_current_cgroup() -> Path:
    """Get the current cgroup path for this process."""
    with open("/proc/self/cgroup", 'r') as f:
        cgroup_info = f.readlines()
    return Path(f"/sys/fs/cgroup/{cgroup_info[0].strip().split(':')[-1]}")

SCYLLA_TEST_CGROUP_BASE_ENV = 'SCYLLA_TEST_CGROUP_BASE'


@lru_cache(maxsize=None)
def get_cgroup() -> Path:
    # Use the env var when set so that xdist worker subprocesses (which are spawned
    # after the master has moved itself to tests/master/default) still compute the
    # correct top-level cgroup path rather than one nested inside the master's cgroup.
    env_val = os.environ.get(SCYLLA_TEST_CGROUP_BASE_ENV)
    base = Path(env_val) if env_val else get_current_cgroup()
    if base.stem != 'resource_gather':
        base = base / 'resource_gather'
    return base


CGROUP_INITIAL = get_cgroup()
CGROUP_TESTS = CGROUP_INITIAL.parent / 'tests'


class ResourceGather(ABC):

    def setup_test_tracking(self) -> None:
        pass

    def put_process_to_cgroup(self) -> None:
        pass

    def get_test_metrics(self, seastar_io: dict[str, int] | None = None) -> Metric:
        pass

    def write_metrics_to_db(self, metrics: Metric, success: bool = False) -> None:
        pass

    def teardown_test_tracking(self) -> None:
        pass

    def stop_monitoring(self) -> None:
        pass

    def cgroup_monitor(self) -> None:
        pass


class ResourceGatherRecord(ResourceGather):
    """Writes test records and timing metrics to the DB, but performs no cgroup operations.

    Used when --gather-metrics is OFF so all tests still appear in the tests table.
    """

    def __init__(self, temp_dir: Path, test: SimpleNamespace, worker_id: str | None = None):
        self.test = test
        self.worker_id = worker_id or "master"
        self.db_path = temp_dir / DEFAULT_DB_NAME
        self.sqlite_writer = SQLiteWriter(self.db_path)
        self.logger = logging.getLogger(__name__)

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

    def get_test_metrics(self, seastar_io: dict[str, int] | None = None) -> Metric:
        test_metrics = Metric(test_id=self.test_id, worker_id=self.worker_id)
        if seastar_io:
            test_metrics.seastar_read_bytes = seastar_io.get(SeastarIOMetricName.READ_BYTES, 0)
            test_metrics.seastar_read_ops = seastar_io.get(SeastarIOMetricName.READ_OPS, 0)
            test_metrics.seastar_write_bytes = seastar_io.get(SeastarIOMetricName.WRITE_BYTES, 0)
            test_metrics.seastar_write_ops = seastar_io.get(SeastarIOMetricName.WRITE_OPS, 0)
        test_metrics.time_taken = self.test.time_end - self.test.time_start
        test_metrics.time_start = datetime.fromtimestamp(self.test.time_start)
        test_metrics.time_end = datetime.fromtimestamp(self.test.time_end)
        test_metrics.success = self.test.success
        return test_metrics

    def write_metrics_to_db(self, metrics: Metric, success: bool = False) -> None:
        metrics.success = success
        self.sqlite_writer.write_row(metrics, METRICS_TABLE)

    def teardown_test_tracking(self) -> None:
        self.sqlite_writer.close()


class ResourceGatherOn(ResourceGatherRecord):
    """Resource gatherer that tracks worker-level cgroup memory and CPU metrics.

    Uses the worker's cgroup (CGROUP_TESTS/{worker_id}) which hierarchically includes
    all Scylla node processes running under that worker, giving accurate memory readings.
    """

    def __init__(self, temp_dir: Path, test: SimpleNamespace, worker_id: str | None = None):
        super().__init__(temp_dir, test, worker_id)
        self.pool = ThreadPoolExecutor(max_workers=1)
        self.future = None
        self.stop_event = Event()
        self.cgroup_path = CGROUP_TESTS / self.worker_id
        self._memory_peak_fd: IO | None = None
        self._cpu_stat_start: dict[str, float] | None = None

    def stop_monitoring(self) -> None:
        self.stop_event.set()
        if self.future is not None:
            self.future.result()
            self.pool.shutdown(wait=True)

    def cgroup_monitor(self) -> None:
        self.future = self.pool.submit(self._monitor_cgroup)

    def _monitor_cgroup(self) -> None:
        """Continuously monitors cgroup memory utilization every second."""
        memory_current = self.cgroup_path / 'memory.current'
        sqlite_writer = SQLiteWriter(self.db_path)
        try:
            while not self.stop_event.is_set():
                try:
                    timeline_record = CgroupMetric(
                        test_id=self.test_id,
                        memory=int(memory_current.read_text().strip()),
                        timestamp=datetime.now()
                    )
                    sqlite_writer.write_row(timeline_record, CGROUP_MEMORY_METRICS_TABLE)
                except Exception as e:
                    self.logger.debug(f"Could not read cgroup memory for {self.cgroup_path}: {e}")
                self.stop_event.wait(1)
        finally:
            sqlite_writer.close()

    def setup_test_tracking(self) -> None:
        # memory.peak's per-FD tracker is reset by *writing* a non-empty string to the
        # FD, not by opening it: a read-only open returns the cgroup's lifetime
        # watermark, which for an xdist worker is whatever the heaviest test before
        # this one reached.  Reset here so later reads through this FD give the peak
        # during this test alone.  Kernels without the writable memory.peak keep the
        # old watermark semantics -- an upper bound rather than a per-test figure.
        #
        # The write is unbuffered: on such a kernel the file has no write handler,
        # and cgroupfs reports that only once the write reaches it.  A buffered
        # write would keep the payload pending, so the error would surface from
        # flush() *and again* from close() in the except branch below, escaping it.
        memory_peak_path = self.cgroup_path / 'memory.peak'
        if memory_peak_path.exists():
            try:
                self._memory_peak_fd = open(memory_peak_path, 'rb+', buffering=0)
                self._memory_peak_fd.write(b'reset')
            except OSError as e:
                self.logger.debug("Could not reset %s, memory_peak will be a cgroup "
                                  "lifetime watermark: %s", memory_peak_path, e)
                if self._memory_peak_fd is not None:
                    self._memory_peak_fd.close()
                self._memory_peak_fd = open(memory_peak_path, 'rb', buffering=0)

        # Snapshot cpu.stat at the start of the test. Unlike memory.peak, cpu.stat
        # has no per-FD reset mechanism — values are cumulative for the cgroup's
        # lifetime. We subtract this snapshot from the end-of-test reading to get
        # per-test CPU usage.
        cpu_stat_path = self.cgroup_path / 'cpu.stat'
        if cpu_stat_path.exists():
            with open(cpu_stat_path, 'r') as f:
                self._cpu_stat_start = self._read_cpu_stat(f)

    def get_test_metrics(self, seastar_io: dict[str, int] | None = None) -> Metric:
        test_metrics = super().get_test_metrics(seastar_io)
        if self._memory_peak_fd is not None:
            try:
                self._memory_peak_fd.seek(0)
                test_metrics.memory_peak = int(self._memory_peak_fd.read().strip())
            except Exception as e:
                self.logger.warning(f"Could not read memory.peak for {self.cgroup_path}: {e}")

        cpu_stat_path = self.cgroup_path / 'cpu.stat'
        if cpu_stat_path.exists() and self._cpu_stat_start is not None:
            with open(cpu_stat_path, 'r') as f:
                cpu_stat_end = self._read_cpu_stat(f)
            for stat, attr in self._CPU_STAT_FIELDS.items():
                start_val = self._cpu_stat_start.get(stat, 0.0)
                end_val = cpu_stat_end.get(stat, 0.0)
                setattr(test_metrics, attr, end_val - start_val)

        return test_metrics

    def teardown_test_tracking(self) -> None:
        if self._memory_peak_fd is not None:
            self._memory_peak_fd.close()
            self._memory_peak_fd = None
        self._cpu_stat_start = None
        super().teardown_test_tracking()

    # Maps cpu.stat keys to Metric attribute names. Values in cpu.stat are in
    # microseconds; we convert to seconds when assigning to the Metric.
    _CPU_STAT_FIELDS = {
        'user_usec': 'user_sec',
        'system_usec': 'system_sec',
        'usage_usec': 'usage_sec',
    }

    @staticmethod
    def _read_cpu_stat(file: TextIO) -> dict[str, float]:
        """Read cpu.stat and return the relevant counters converted to seconds."""
        result: dict[str, float] = {}
        for line in file.readlines():
            parts = line.split(' ', 1)
            if len(parts) == 2 and parts[0] in ResourceGatherOn._CPU_STAT_FIELDS:
                result[parts[0]] = float(parts[1]) / 1_000_000
        return result


def _get_cpu_model() -> str:
    try:
        with open("/proc/cpuinfo") as f:
            for line in f:
                if line.startswith("model name"):
                    return line.split(":", 1)[1].strip()
    except OSError:
        pass
    # aarch64 /proc/cpuinfo has no "model name" line; lscpu decodes the
    # implementer/part ids into one (e.g. "Neoverse-N1").  LC_ALL=C keeps the
    # label we match untranslated: lscpu calls setlocale() and its labels go
    # through gettext, so "Model name" is localized where util-linux
    # translations are installed.
    try:
        lscpu = subprocess.run(["lscpu"], capture_output=True, text=True, check=True,
                               env={**os.environ, "LC_ALL": "C"})
        for line in lscpu.stdout.splitlines():
            if line.startswith("Model name"):
                return line.split(":", 1)[1].strip()
    except (OSError, subprocess.CalledProcessError):
        pass
    return platform.processor() or "unknown"


def gather_host_info() -> HostInfo:
    """Collect static hardware information about the current host."""
    cpu_model = _get_cpu_model()
    cpu_cores = psutil.cpu_count(logical=False) or os.cpu_count() or 0
    ram_bytes = psutil.virtual_memory().total
    return HostInfo(host_id=HOST_ID, cpu_model=cpu_model, cpu_cores=cpu_cores, ram_bytes=ram_bytes)


def get_resource_gather(temp_dir: Path, is_switched_on: bool, test: SimpleNamespace, worker_id: str | None = None) -> ResourceGather:
    """Return a resource gatherer for the given test. Always creates a test record in the DB."""
    if is_switched_on:
        return ResourceGatherOn(temp_dir, test, worker_id)
    else:
        return ResourceGatherRecord(temp_dir, test, worker_id)


def _is_cgroup_rw() -> bool:
    with open('/proc/mounts', 'r') as f:
        for line in f.readlines():
            if 'cgroup2' in line:
                options = line.split(' ')[3].split(',')
                return 'rw' in options
    return False

def propagate_subtree_controls(group: Path):
    # Only enable the memory controller. cpu.stat is available without
    # enabling the cpu controller (it's base cgroup v2 accounting).
    # Enabling all controllers (cpu, io, pids, etc.) adds unnecessary
    # per-operation kernel overhead to child processes - in particular,
    # the io controller adds accounting to every I/O operation.
    with open(group / "cgroup.controllers", "r") as f:
        if "memory" not in f.readline().split():
            return
    with open(group / "cgroup.subtree_control", "w") as f:
        f.write("+memory")


def setup_cgroup(is_required: bool) -> None:
    if is_required:
        # Export the cgroup base path as an env var so that xdist worker subprocesses
        # inherit it. Workers are spawned after the master has already moved itself into
        # tests/master/default, so without this env var they would compute CGROUP_INITIAL
        # relative to that nested cgroup instead of the original top-level scope.
        os.environ[SCYLLA_TEST_CGROUP_BASE_ENV] = str(CGROUP_INITIAL.parent)

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

            propagate_subtree_controls(CGROUP_INITIAL.parent)

        # Always ensure CGROUP_TESTS has subtree controls enabled so that worker
        # sub-cgroups and per-test cgroups can use memory tracking.
        propagate_subtree_controls(CGROUP_TESTS)


def setup_worker_cgroup() -> None:
    from test.pylib.util import get_xdist_worker_id
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
    propagate_subtree_controls(worker_cgroup_path)
    # Move the current worker process into the worker's default leaf cgroup.
    # Scylla processes spawned by the test (via ScyllaClusterManager) will inherit
    # this cgroup. The worker-level cgroup (CGROUP_TESTS/{worker_id}) is used for
    # hierarchical memory monitoring and captures all descendant processes.
    try:
        with open(worker_cgroup_path_default / 'cgroup.procs', 'w') as f:
            f.write(str(os.getpid()))
    except Exception as e:
        logger.warning(f"Could not move worker process to cgroup {worker_cgroup_path_default}: {e}")


class SystemResourceMonitor:
    """Continuously monitors CPU and memory utilization."""
    def __init__(self, tmpdir: Path):
        self.tmpdir = tmpdir
        self.stop_event = Event()
        self.thread = ThreadPoolExecutor(max_workers=1)

    def start(self) -> None:
        self.thread.submit(self._monitor_resources, self.tmpdir)

    def stop(self) -> None:
        self.stop_event.set()
        self.thread.shutdown(wait=True)

    def _monitor_resources(self, tmpdir: Path) -> None:
        sqlite_writer = SQLiteWriter(tmpdir / DEFAULT_DB_NAME)
        try:
            _ = psutil.cpu_percent()  # first non-blocking call returns meaningless 0.0. Skip it.
            while not self.stop_event.wait(timeout=2.0):
                vm = psutil.virtual_memory()
                timeline_record = SystemResourceMetric(
                    host_id=HOST_ID,
                    cpu=psutil.cpu_percent(),
                    memory_free=vm.free,
                    memory_available=vm.available,
                    memory_used=vm.used,
                    memory_active=vm.active,
                    memory_inactive=vm.inactive,
                    memory_buffers=vm.buffers,
                    timestamp=datetime.now(),
                )
                sqlite_writer.write_row(timeline_record, SYSTEM_RESOURCE_METRICS_TABLE)
        finally:
            sqlite_writer.close()


# The utilization a test run is meant to hold the machine at: below it the builder idles
# and the run takes longer than it has to, above it the tests contend for the machine and
# begin to time out. The score is the percentage of the run that was spent inside it.
SCORE_BAND = (80.0, 90.0)


def _summary(values: list[float]) -> tuple[float, float, float, float, float]:
    """Return the average, the median, the p95, the p99 and the score of the samples."""
    low, high = SCORE_BAND
    score = sum(low <= value <= high for value in values) * 100 / len(values)
    if len(values) == 1:
        p95 = p99 = values[0]
    else:
        percentiles = quantiles(values, n=100, method='inclusive')
        p95, p99 = percentiles[94], percentiles[98]
    return fmean(values), median(values), p95, p99, score


def summarize_resource_utilization(temp_dir: Path) -> ResourceUtilization | None:
    """Aggregate the run's host-wide CPU/memory samples into a single final record.

    Writes the record to the metrics database and returns it, or None when there is
    nothing to summarize: a session that ran no test, one too short to be sampled, or
    a database that was never created.
    """
    db_path = temp_dir / DEFAULT_DB_NAME
    if not db_path.exists():
        return None

    # The database is this host's own - it is named after its id - so nothing below
    # filters by host: every row in it was written by this run, on this machine.
    with closing(sqlite3.connect(db_path)) as connection:
        host_info = connection.execute(f'SELECT ram_bytes FROM {HOST_INFO_TABLE}').fetchone()

        # Only what was sampled while tests were running counts: the sampler also covers
        # the build mode preparation before the first test and the cleanup after the last
        # one, and those idle stretches drag every figure of the run towards zero.
        first_test, last_test = connection.execute(
            f'SELECT min(time_start), max(time_end) FROM {METRICS_TABLE}').fetchone()
        samples = connection.execute(
            f'SELECT cpu, memory_available FROM {SYSTEM_RESOURCE_METRICS_TABLE} '
            f'WHERE timestamp BETWEEN ? AND ?',
            (first_test, last_test)).fetchall() if first_test and last_test else []
        if not samples and first_test and last_test:
            # A run so short that no sample fell between its first and its last test:
            # summarize what there is rather than nothing. A session that ran no test at
            # all has no window, and gets no record - its samples describe an idle
            # machine, not a test run.
            samples = connection.execute(
                f'SELECT cpu, memory_available FROM {SYSTEM_RESOURCE_METRICS_TABLE}').fetchall()

        # The samples are host-wide, so all modes of the run share one record. On CI a
        # run covers a single mode (one database per architecture and mode), which is
        # what makes the figures per-mode there; a local multi-mode run gets them joined.
        # ponytail: comma-joined modes. Splitting them needs every sample attributed to
        # the tests running at its timestamp - an indexed join, if it is ever asked for.
        modes = [row[0] for row in connection.execute(
            f'SELECT DISTINCT mode FROM {TESTS_TABLE} ORDER BY mode')]

    if not samples or host_info is None:
        return None
    ram_bytes = host_info[0]

    cpu_avg, cpu_median, cpu_p95, cpu_p99, cpu_score = _summary([row[0] for row in samples])
    memory_avg, memory_median, memory_p95, memory_p99, memory_score = _summary(
        [(ram_bytes - row[1]) * 100 / ram_bytes for row in samples])

    record = ResourceUtilization(
        host_id=HOST_ID,
        architecture=platform.machine(),
        mode=','.join(modes),
        samples=len(samples),
        cpu_avg=cpu_avg,
        cpu_median=cpu_median,
        cpu_p95=cpu_p95,
        cpu_p99=cpu_p99,
        cpu_score=cpu_score,
        memory_avg=memory_avg,
        memory_median=memory_median,
        memory_p95=memory_p95,
        memory_p99=memory_p99,
        memory_score=memory_score,
        timestamp=datetime.now(),
    )

    sqlite_writer = SQLiteWriter(db_path)
    try:
        sqlite_writer.write_row(record, RESOURCE_UTILIZATION_TABLE)
    finally:
        sqlite_writer.close()
    return record
