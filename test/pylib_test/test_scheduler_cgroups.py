#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""The dynamic scheduler stops the session when it cannot have its cgroups."""

from types import SimpleNamespace

import pytest

from test.pylib import runner


def make_config(gather_metrics: bool):
    options = {"--dynamic-scheduler": True, "--gather-metrics": gather_metrics, "dist": "load"}
    return SimpleNamespace(getoption=lambda name, default=None: options.get(name, default))


def test_runs_on_when_cgroups_are_set_up(monkeypatch):
    monkeypatch.setattr(runner, "setup_cgroup", lambda is_required: None)
    monkeypatch.setattr(runner, "setup_worker_cgroup", lambda: True)
    runner.require_scheduler_cgroups(make_config(gather_metrics=False))


def test_stops_when_the_cgroup_tree_cannot_be_made(monkeypatch):
    def fail(is_required):
        raise PermissionError("[Errno 13] Permission denied: '/sys/fs/cgroup/x'")
    monkeypatch.setattr(runner, "setup_cgroup", fail)
    monkeypatch.setattr(runner, "setup_worker_cgroup", lambda: True)
    with pytest.raises(pytest.exit.Exception) as e:
        runner.require_scheduler_cgroups(make_config(gather_metrics=False))
    assert e.value.returncode == pytest.ExitCode.USAGE_ERROR
    assert "PermissionError" in str(e.value)
    assert "add --no-dynamic-scheduler\n" in str(e.value)


def test_stops_when_test_py_cannot_move_into_its_cgroup(monkeypatch):
    monkeypatch.setattr(runner, "setup_cgroup", lambda is_required: None)
    monkeypatch.setattr(runner, "setup_worker_cgroup", lambda: False)
    with pytest.raises(pytest.exit.Exception) as e:
        runner.require_scheduler_cgroups(make_config(gather_metrics=True))
    assert "could not move test.py into its cgroup" in str(e.value)
    # --gather-metrics needs the cgroups too, so it has to go as well
    assert "add --no-dynamic-scheduler --no-gather-metrics" in str(e.value)
