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
pytest_testnodedown hook that calls it exist to capture that evidence -- a
dmesg tail and cgroup memory.events -- while it's still available.
"""

import logging
import subprocess
import types
from unittest.mock import Mock

import pytest

from test.pylib import resource_gather
from test.pylib import runner


def _fake_dmesg_result(stdout: str = "", returncode: int = 0, stderr: str = "") -> subprocess.CompletedProcess:
    return subprocess.CompletedProcess(args=["dmesg", "--ctime"], returncode=returncode, stdout=stdout, stderr=stderr)


def test_gather_oom_kill_evidence_includes_dead_worker_cgroup_events(monkeypatch, tmp_path):
    monkeypatch.setattr(resource_gather, "CGROUP_TESTS", tmp_path)
    worker_cgroup = tmp_path / "gw14"
    worker_cgroup.mkdir()
    (worker_cgroup / "memory.events").write_text("oom_kill 1\n")

    monkeypatch.setattr(
        resource_gather.subprocess, "run",
        lambda *a, **k: _fake_dmesg_result(stdout="[hh:mm:ss] Killed process 12345 (pytest)\n"))

    evidence = resource_gather.gather_oom_kill_evidence("gw14")

    assert "oom_kill 1" in evidence
    assert "Killed process 12345 (pytest)" in evidence
    assert "MemTotal" in evidence


@pytest.mark.parametrize("run_stub", [
    Mock(side_effect=FileNotFoundError("dmesg: command not found")),
    Mock(return_value=_fake_dmesg_result(returncode=1, stderr="dmesg: read kernel buffer failed: Permission denied")),
])
def test_gather_oom_kill_evidence_survives_dmesg_and_cgroup_unavailable(monkeypatch, tmp_path, run_stub):
    monkeypatch.setattr(resource_gather, "CGROUP_TESTS", tmp_path / "nonexistent")
    monkeypatch.setattr(resource_gather.subprocess, "run", run_stub)

    evidence = resource_gather.gather_oom_kill_evidence("gw14")

    assert "<unavailable:" in evidence


def test_testnodedown_logs_evidence_for_unexpected_death(monkeypatch, caplog):
    monkeypatch.setattr(runner, "gather_oom_kill_evidence", lambda worker_id: "fake evidence")
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
