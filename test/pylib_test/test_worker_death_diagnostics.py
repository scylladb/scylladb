#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""Tests for the cgroup evidence that the controller logs when a pytest-xdist
worker dies unexpectedly (SCYLLADB-4527).
"""

import logging
import types
from unittest.mock import Mock

import pytest

from test.pylib import resource_gather
from test.pylib import runner


@pytest.fixture
def cgroup_tests(monkeypatch, tmp_path):
    path = tmp_path / "cgroup"
    path.mkdir()
    monkeypatch.setattr(resource_gather, "CGROUP_TESTS", path)
    monkeypatch.setattr(runner, "CGROUP_TESTS", path)
    return path


def _make_worker_cgroup(cgroup_tests, oom_kill):
    worker_cgroup = cgroup_tests / "gw14"
    worker_cgroup.mkdir()
    (worker_cgroup / "memory.events").write_text(f"low 0\nhigh 0\nmax 0\noom 0\noom_kill {oom_kill}\noom_group_kill 0\n")
    return worker_cgroup


def _fake_node():
    return types.SimpleNamespace(
        gateway=types.SimpleNamespace(id="gw14"),
        config=types.SimpleNamespace(stash=pytest.Stash(), getoption=lambda name, default=None: default),
        workerinput={},
    )


def test_read_oom_kill(tmp_path):
    (tmp_path / "memory.events").write_text("oom 1\noom_kill 3\n")

    assert resource_gather.read_oom_kill(tmp_path) == 3
    assert resource_gather.read_oom_kill(tmp_path / "missing") is None


def test_gather_oom_kill_evidence_reports_delta_since_baseline(cgroup_tests):
    worker_cgroup = _make_worker_cgroup(cgroup_tests, oom_kill=3)
    (worker_cgroup / "memory.current").write_text("123456\n")

    evidence = resource_gather.gather_oom_kill_evidence("gw14", oom_kill_baseline=1)

    assert "oom_kill delta since the worker started: 2" in evidence
    assert "oom_kill 3" in evidence
    assert "123456" in evidence
    assert "MemTotal" in evidence


def test_gather_oom_kill_evidence_without_baseline_reports_unknown_delta(cgroup_tests):
    _make_worker_cgroup(cgroup_tests, oom_kill=5)

    evidence = resource_gather.gather_oom_kill_evidence("gw14", oom_kill_baseline=None)

    assert "oom_kill delta since the worker started: unknown" in evidence
    assert "oom_kill 5" in evidence


def test_gather_oom_kill_evidence_survives_cgroup_unavailable(monkeypatch, cgroup_tests):
    def _raise_get_current_cgroup():
        raise OSError("no such cgroup")

    monkeypatch.setattr(resource_gather, "get_current_cgroup", _raise_get_current_cgroup)

    evidence = resource_gather.gather_oom_kill_evidence("gw14", oom_kill_baseline=0)

    assert "oom_kill delta since the worker started: unknown" in evidence
    assert "<unavailable:" in evidence
    assert "MemTotal" in evidence


def test_configure_node_records_baseline_of_existing_cgroup(cgroup_tests):
    _make_worker_cgroup(cgroup_tests, oom_kill=4)
    node = _fake_node()

    runner.pytest_configure_node(node=node)

    assert node.config.stash[runner.OOM_KILL_BASELINES_KEY] == {"gw14": 4}


def test_configure_node_records_zero_for_new_cgroup(cgroup_tests):
    node = _fake_node()

    runner.pytest_configure_node(node=node)

    assert node.config.stash[runner.OOM_KILL_BASELINES_KEY] == {"gw14": 0}


def test_testnodedown_logs_evidence_for_unexpected_death(monkeypatch, caplog):
    evidence_mock = Mock(return_value="fake evidence")
    monkeypatch.setattr(runner, "gather_oom_kill_evidence", evidence_mock)
    node = _fake_node()
    node.config.stash[runner.OOM_KILL_BASELINES_KEY] = {"gw14": 2}

    with caplog.at_level(logging.WARNING):
        runner.pytest_testnodedown(node=node, error="Not properly terminated")

    evidence_mock.assert_called_once_with("gw14", 2)
    assert any(
        "gw14" in record.getMessage() and "Not properly terminated" in record.getMessage()
        and "fake evidence" in record.getMessage()
        for record in caplog.records
    )


def test_testnodedown_survives_evidence_failure(monkeypatch, caplog):
    monkeypatch.setattr(runner, "gather_oom_kill_evidence", Mock(side_effect=RuntimeError("boom")))

    with caplog.at_level(logging.WARNING):
        runner.pytest_testnodedown(node=_fake_node(), error="Not properly terminated")

    assert any("Failed to gather diagnostics" in record.getMessage() for record in caplog.records)


def test_testnodedown_silent_on_clean_shutdown(monkeypatch, caplog):
    evidence_mock = Mock()
    monkeypatch.setattr(runner, "gather_oom_kill_evidence", evidence_mock)

    with caplog.at_level(logging.WARNING):
        runner.pytest_testnodedown(node=_fake_node(), error=None)

    assert caplog.records == []
    evidence_mock.assert_not_called()
