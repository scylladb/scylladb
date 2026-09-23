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
import subprocess
import time
from abc import ABC
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from time import sleep
from types import SimpleNamespace
from typing import TYPE_CHECKING

import psutil

from threading import Event
from test import HOST_ID, TOP_SRC_DIR
from test.pylib.db.model import HostInfo, Metric, SystemResourceMetric, CgroupMetric, Test
from test.pylib.db.writer import (
    CGROUP_MEMORY_METRICS_TABLE,
    DEFAULT_DB_NAME,
    HOST_INFO_TABLE,
    METRICS_TABLE,
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

    def get_test_metrics(self) -> Metric:
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

    def get_test_metrics(self) -> Metric:
        test_metrics = Metric(test_id=self.test_id, host_id=HOST_ID, worker_id=self.worker_id)
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
                        host_id=HOST_ID,
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
        # Open a fresh FD on memory.peak so the kernel resets its per-FD peak tracker
        # to the current memory. Reading this FD later returns the peak memory since it
        # was opened, i.e., the peak during this test only.
        memory_peak_path = self.cgroup_path / 'memory.peak'
        if memory_peak_path.exists():
            self._memory_peak_fd = open(memory_peak_path, 'r')

        # Snapshot cpu.stat at the start of the test. Unlike memory.peak, cpu.stat
        # has no per-FD reset mechanism — values are cumulative for the cgroup's
        # lifetime. We subtract this snapshot from the end-of-test reading to get
        # per-test CPU usage.
        cpu_stat_path = self.cgroup_path / 'cpu.stat'
        if cpu_stat_path.exists():
            with open(cpu_stat_path, 'r') as f:
                self._cpu_stat_start = self._read_cpu_stat(f)

    def get_test_metrics(self) -> Metric:
        test_metrics = super().get_test_metrics()
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


def _read_file_or_reason(path: Path) -> str:
    """Read *path* as text, or describe why it could not be read."""
    try:
        return path.read_text().strip()
    except OSError as exc:
        return f"<unavailable: {exc}>"


# Name of the small sidecar file _snapshot_oom_kill_baseline() writes under
# baseline_dir/<worker_id>/, recording the oom_kill counter at
# worker-cgroup-creation time (see setup_worker_cgroup()). It cannot live
# inside the cgroup directory itself: cgroupfs is a kernfs-backed virtual
# filesystem that only exposes the kernel's fixed set of control files per
# cgroup (memory.events, cgroup.procs, ...) and does not support creating
# arbitrary new files.
_OOM_KILL_BASELINE_FILENAME = "oom_kill.baseline"


def _read_memory_events_counters(cgroup_dir: Path) -> dict[str, int] | None:
    """Parse cgroup v2's memory.events in *cgroup_dir* into {name: value}.

    Returns None if the file could not be read at all (e.g. no cgroup was
    ever created there, which happens when the harness was run without
    --gather-metrics — see setup_worker_cgroup()).
    """
    try:
        text = (cgroup_dir / "memory.events").read_text()
    except OSError:
        return None
    counters: dict[str, int] = {}
    for line in text.splitlines():
        name, _, value = line.partition(" ")
        value = value.strip()
        if value.lstrip("-").isdigit():
            counters[name] = int(value)
    return counters


def _mkdir_parents(path: Path) -> None:
    """Indirection over Path.mkdir() so tests can monkeypatch it scoped to
    this module, rather than patching the shared pathlib.Path class
    process-wide."""
    path.mkdir(parents=True, exist_ok=True)


def _snapshot_oom_kill_baseline(cgroup_dir: Path, baseline_dir: Path) -> None:
    """Record the current oom_kill counter under baseline_dir, once, at
    worker-cgroup-creation time (called from setup_worker_cgroup()).

    memory.events is cumulative for the whole life of the cgroup, and cgroup
    v2 aggregates it recursively across descendants, so a worker's counter
    also includes every Scylla process it has spawned across earlier tests.
    Recording a baseline here lets gather_oom_kill_evidence() report the
    delta since *this* worker started, rather than a lifetime total that
    could misattribute an old or unrelated kill to a later, unrelated death.

    The baseline is stored under baseline_dir, keyed by cgroup_dir's name
    (the worker id) -- deliberately outside the cgroup hierarchy, since
    cgroupfs does not support creating arbitrary new files (see
    _OOM_KILL_BASELINE_FILENAME).
    """
    counters = _read_memory_events_counters(cgroup_dir)
    baseline = counters.get("oom_kill", 0) if counters is not None else 0
    baseline_path = baseline_dir / cgroup_dir.name / _OOM_KILL_BASELINE_FILENAME
    try:
        _mkdir_parents(baseline_path.parent)
        baseline_path.write_text(str(baseline))
    except OSError as exc:
        logger.warning(f"Could not snapshot oom_kill baseline for {cgroup_dir} at {baseline_path}: {exc}")


def _oom_kill_evidence(cgroup_dir: Path, baseline_dir: Path | None) -> str:
    """Human-readable oom_kill/oom evidence for *cgroup_dir*.

    Reports the delta since worker-cgroup-creation time when a baseline was
    recorded by _snapshot_oom_kill_baseline() under baseline_dir; otherwise
    (baseline_dir is None, or no baseline was recorded there -- e.g. the
    controller's own cgroup, which never gets a baseline) falls back to the
    raw lifetime-cumulative counters and says so explicitly, so a reader
    never mistakes a cumulative count for evidence about one specific death.
    """
    counters = _read_memory_events_counters(cgroup_dir)
    if counters is None:
        return f"<unavailable: could not read {cgroup_dir / 'memory.events'}>"
    oom_kill = counters.get("oom_kill", 0)
    oom = counters.get("oom", 0)
    baseline_path = baseline_dir / cgroup_dir.name / _OOM_KILL_BASELINE_FILENAME if baseline_dir is not None else None
    try:
        if baseline_path is None:
            raise OSError("no baseline_dir given")
        baseline = int(baseline_path.read_text().strip())
    except (OSError, ValueError):
        return (f"oom={oom} oom_kill={oom_kill} (cumulative for this cgroup's lifetime; "
                f"no baseline recorded, may include earlier or descendant-process kills)")
    return f"oom={oom} oom_kill={oom_kill} (delta since this worker started: {oom_kill - baseline}; baseline was {baseline})"


def gather_oom_kill_evidence(worker_id: str | None, baseline_dir: Path | None = None) -> str:
    """Collect circumstantial evidence for whether a dead xdist worker was killed externally.

    Reads the dead worker's own cgroup memory.events/memory.current (only
    present when the harness was run with --gather-metrics — see
    setup_worker_cgroup()), the controller's own cgroup memory.events/
    memory.current, and /proc/meminfo into one human-readable string suitable
    for logging. No size cap is needed: these are small, fixed-format
    snapshots (a handful of lines each), not an unbounded kernel log.
    """
    sections: list[str] = []

    if worker_id is not None:
        worker_cgroup = CGROUP_TESTS / worker_id
        sections += [
            f"--- {worker_id} worker cgroup ---",
            f"memory.events: {_oom_kill_evidence(worker_cgroup, baseline_dir)}",
            f"memory.current: {_read_file_or_reason(worker_cgroup / 'memory.current')}",
        ]

    try:
        controller_cgroup = get_current_cgroup()
        controller_section = (
            f"memory.events: {_oom_kill_evidence(controller_cgroup, None)}\n"
            f"memory.current: {_read_file_or_reason(controller_cgroup / 'memory.current')}"
        )
    except (OSError, IndexError) as exc:
        controller_section = f"<unavailable: {exc}>"

    sections += [
        "--- controller cgroup ---",
        controller_section,
        "--- /proc/meminfo ---",
        _read_file_or_reason(Path("/proc/meminfo")),
    ]

    return "\n".join(sections)


def gather_controller_snapshot() -> str:
    """Return a short, timestamped snapshot of the controller's own cgroup memory state.

    Used by runner.py's pytest_runtest_logreport-piggybacked periodic
    snapshot (see there for why it exists): the same small, fixed-format
    cgroup v2 files gather_oom_kill_evidence() reads, just for the
    controller's own cgroup, refreshed on disk periodically so something
    useful survives even if the controller process itself is later killed
    (a case pytest_testnodedown() cannot cover, since nothing is left alive
    to run that hook once the controller is gone).
    """
    try:
        controller_cgroup = get_current_cgroup()
    except (OSError, IndexError) as exc:
        return f"<unavailable: {exc}>"
    return (
        f"timestamp: {datetime.now().isoformat()}\n"
        f"memory.events: {_oom_kill_evidence(controller_cgroup, None)}\n"
        f"memory.current: {_read_file_or_reason(controller_cgroup / 'memory.current')}\n"
    )


def gather_host_info() -> HostInfo:
    """Collect static hardware information about the current host."""
    try:
        cpu_model = "unknown"
        with open("/proc/cpuinfo") as f:
            for line in f:
                if line.startswith("model name"):
                    cpu_model = line.split(":", 1)[1].strip()
                    break
    except OSError:
        cpu_model = platform.processor() or "unknown"

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


def setup_worker_cgroup(oom_kill_baseline_dir: Path) -> None:
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
    # Baseline the oom_kill counter now, at this worker's cgroup-creation
    # time, so gather_oom_kill_evidence() can later report the delta since
    # this worker started rather than the cgroup's lifetime-cumulative total.
    _snapshot_oom_kill_baseline(worker_cgroup_path, oom_kill_baseline_dir)
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
