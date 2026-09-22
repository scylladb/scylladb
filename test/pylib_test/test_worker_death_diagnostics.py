#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""Tests for the diagnostics captured when a pytest-xdist worker dies
unexpectedly (SCYLLADB-4527).

A worker that is killed by the OOM-killer or a cgroup memory limit leaves no
traceback and no core, so there is nothing in the usual test artifacts to
confirm or refute an external kill. gather_oom_kill_evidence() and the
pytest_testnodedown hook that calls it exist to capture that evidence --
cgroup v2 memory.events/memory.current -- while it's still available.
_maybe_snapshot_controller_cgroup() covers the complementary case of the
controller process itself being killed, for which no hook can fire after
the fact.
"""

import logging
import types
from unittest.mock import Mock

import pytest

from test.pylib import resource_gather
from test.pylib import runner


def test_gather_oom_kill_evidence_includes_dead_worker_cgroup_events(monkeypatch, tmp_path):
    # CGROUP_TESTS models real cgroupfs: only pre-existing kernel-provided
    # files (memory.events, memory.current here) are ever written into it in
    # these tests, never the oom_kill baseline -- cgroupfs does not support
    # creating arbitrary new files. The baseline lives in a separate,
    # ordinary writable directory.
    monkeypatch.setattr(resource_gather, "CGROUP_TESTS", tmp_path / "cgroup")
    baseline_dir = tmp_path / "baseline"
    worker_cgroup = tmp_path / "cgroup" / "gw14"
    worker_cgroup.mkdir(parents=True)
    (worker_cgroup / "memory.events").write_text("oom 0\noom_kill 1\n")
    resource_gather._snapshot_oom_kill_baseline(worker_cgroup, baseline_dir)
    # A kill happens after the baseline was recorded.
    (worker_cgroup / "memory.events").write_text("oom 0\noom_kill 2\n")
    (worker_cgroup / "memory.current").write_text("123456\n")

    evidence = resource_gather.gather_oom_kill_evidence("gw14", baseline_dir)

    assert "oom_kill=2" in evidence
    assert "delta since this worker started: 1" in evidence
    assert "123456" in evidence
    assert "MemTotal" in evidence


def test_gather_oom_kill_evidence_baseline_absorbs_preexisting_oom_kill_count(monkeypatch, tmp_path):
    """A nonzero oom_kill counter already present before this worker's cgroup
    was created (e.g. left over from an earlier reused slot) must not be
    reported as a new kill attributable to this worker's death.
    """
    monkeypatch.setattr(resource_gather, "CGROUP_TESTS", tmp_path / "cgroup")
    baseline_dir = tmp_path / "baseline"
    worker_cgroup = tmp_path / "cgroup" / "gw14"
    worker_cgroup.mkdir(parents=True)
    # Pre-existing kills from before this worker started.
    (worker_cgroup / "memory.events").write_text("oom 0\noom_kill 3\n")
    resource_gather._snapshot_oom_kill_baseline(worker_cgroup, baseline_dir)
    # No new kill happens during this worker's life: the counter is unchanged.

    evidence = resource_gather.gather_oom_kill_evidence("gw14", baseline_dir)

    assert "oom_kill=3" in evidence
    assert "delta since this worker started: 0" in evidence


def test_gather_oom_kill_evidence_without_baseline_is_labeled_cumulative(monkeypatch, tmp_path):
    """A worker cgroup that predates the baseline mechanism (or whose
    baseline file could not be written) falls back to reporting the raw
    counter, explicitly labeled as cumulative rather than implied to be new.
    """
    monkeypatch.setattr(resource_gather, "CGROUP_TESTS", tmp_path / "cgroup")
    baseline_dir = tmp_path / "baseline"
    worker_cgroup = tmp_path / "cgroup" / "gw14"
    worker_cgroup.mkdir(parents=True)
    (worker_cgroup / "memory.events").write_text("oom 0\noom_kill 5\n")

    evidence = resource_gather.gather_oom_kill_evidence("gw14", baseline_dir)

    assert "oom_kill=5" in evidence
    assert "cumulative for this cgroup's lifetime" in evidence
    assert "no baseline recorded" in evidence


def test_snapshot_oom_kill_baseline_survives_unwritable_baseline_dir(monkeypatch, tmp_path, caplog):
    """cgroupfs is a kernfs-backed virtual filesystem: it only exposes the
    kernel's fixed set of control files per cgroup and never supports
    creating arbitrary new files. This models the baseline-storage directory
    itself being unwritable (e.g. a permissions problem, or a path under a
    read-only mount) and asserts the failure is swallowed -- not fatal -- but
    now visible as a WARNING, rather than silently disabling the delta
    feature at logger.debug.
    """
    worker_cgroup = tmp_path / "gw14"
    worker_cgroup.mkdir()
    (worker_cgroup / "memory.events").write_text("oom 0\noom_kill 1\n")

    baseline_dir = tmp_path / "baseline"

    def _raise_mkdir(*args, **kwargs):
        raise OSError("Read-only file system")

    monkeypatch.setattr(resource_gather.Path, "mkdir", _raise_mkdir)

    with caplog.at_level(logging.WARNING):
        resource_gather._snapshot_oom_kill_baseline(worker_cgroup, baseline_dir)

    assert any(
        "Could not snapshot oom_kill baseline" in record.getMessage() and record.levelno == logging.WARNING
        for record in caplog.records
    )
    assert not (baseline_dir / "gw14").exists()


def test_gather_oom_kill_evidence_survives_cgroup_unavailable(monkeypatch, tmp_path):
    monkeypatch.setattr(resource_gather, "CGROUP_TESTS", tmp_path / "nonexistent")

    def _raise_get_current_cgroup():
        raise OSError("no such cgroup")

    monkeypatch.setattr(resource_gather, "get_current_cgroup", _raise_get_current_cgroup)

    evidence = resource_gather.gather_oom_kill_evidence("gw14", tmp_path / "baseline")

    assert "<unavailable:" in evidence
    assert "MemTotal" in evidence


def test_testnodedown_logs_evidence_for_unexpected_death(monkeypatch, caplog):
    monkeypatch.setattr(runner, "gather_oom_kill_evidence", lambda worker_id, baseline_dir=None: "fake evidence")
    fake_node = types.SimpleNamespace(
        gateway=types.SimpleNamespace(id="gw14"),
        workerinfo={"pid": 4242},
    )

    with caplog.at_level(logging.WARNING):
        runner.pytest_testnodedown(node=fake_node, error="Not properly terminated")

    assert any(
        "gw14" in record.getMessage() and "4242" in record.getMessage() and "Not properly terminated" in record.getMessage()
        for record in caplog.records
    )


def test_testnodedown_silent_on_clean_shutdown(monkeypatch, caplog):
    evidence_mock = Mock()
    monkeypatch.setattr(runner, "gather_oom_kill_evidence", evidence_mock)
    fake_node = types.SimpleNamespace(
        gateway=types.SimpleNamespace(id="gw14"),
        workerinfo={"pid": 4242},
    )

    with caplog.at_level(logging.WARNING):
        runner.pytest_testnodedown(node=fake_node, error=None)

    assert caplog.records == []
    evidence_mock.assert_not_called()


class _FakeConfig:
    def __init__(self, tmpdir):
        self._tmpdir = tmpdir

    def getoption(self, name):
        assert name == "--tmpdir"
        return str(self._tmpdir)


def _snapshot_path(tmp_path):
    return tmp_path / runner.PYTEST_LOG_FOLDER / runner.CONTROLLER_SNAPSHOT_FILENAME


def test_maybe_snapshot_controller_cgroup_writes_and_throttles(monkeypatch, tmp_path):
    monkeypatch.delenv("PYTEST_XDIST_WORKER", raising=False)
    monkeypatch.setattr(runner, "_pytest_config", _FakeConfig(tmp_path))
    monkeypatch.setattr(runner, "_last_controller_snapshot_time", 0.0)

    contents = iter(["snapshot-1", "snapshot-2", "snapshot-3"])
    monkeypatch.setattr(runner, "gather_controller_snapshot", lambda: next(contents))

    fake_now = [1000.0]
    monkeypatch.setattr(runner, "_now", lambda: fake_now[0])

    # First call: interval has "elapsed" relative to the 0.0 initial state, so it writes.
    runner._maybe_snapshot_controller_cgroup()
    path = _snapshot_path(tmp_path)
    assert path.read_text() == "snapshot-1"

    # Second call within the throttle window: must not rewrite the file.
    fake_now[0] += 1
    runner._maybe_snapshot_controller_cgroup()
    assert path.read_text() == "snapshot-1"

    # Third call after the throttle window has elapsed: refreshes the file.
    fake_now[0] += runner.CONTROLLER_SNAPSHOT_INTERVAL_SECONDS
    runner._maybe_snapshot_controller_cgroup()
    assert path.read_text() == "snapshot-2"


def test_maybe_snapshot_controller_cgroup_skips_in_xdist_worker(monkeypatch, tmp_path):
    monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw3")
    monkeypatch.setattr(runner, "_pytest_config", _FakeConfig(tmp_path))
    monkeypatch.setattr(runner, "_last_controller_snapshot_time", 0.0)
    monkeypatch.setattr(runner, "gather_controller_snapshot", lambda: pytest.fail("should not be called in a worker"))

    runner._maybe_snapshot_controller_cgroup()

    assert not _snapshot_path(tmp_path).exists()


def test_maybe_snapshot_controller_cgroup_noop_before_configure(monkeypatch, tmp_path):
    monkeypatch.delenv("PYTEST_XDIST_WORKER", raising=False)
    monkeypatch.setattr(runner, "_pytest_config", None)
    monkeypatch.setattr(runner, "_last_controller_snapshot_time", 0.0)
    monkeypatch.setattr(runner, "gather_controller_snapshot", lambda: pytest.fail("should not be called without config"))

    runner._maybe_snapshot_controller_cgroup()

    assert not _snapshot_path(tmp_path).exists()
