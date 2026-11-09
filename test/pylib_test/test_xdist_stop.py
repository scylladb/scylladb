#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Tests for the xdist_stop plugin.

Uses pytester to run an xdist session in a subprocess: one test fails and
the rest are slow, so the number of tests that still run after the failure
shows whether the workers stopped early.  The slow tests wait for a marker
written by the failing test, so the count does not depend on how fast the
failing worker is.

test_without_plugin_workers_drain_their_queues fails once pytest-xdist
itself stops the workers (pytest-dev/pytest-xdist#1395); then the plugin
and these tests can be removed.
"""

import os
import pathlib
import textwrap

import pytest

pytest_plugins = ["pytester"]

NUM_SLOW_TESTS = 40

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]

_TESTS = textwrap.dedent(f"""\
    import pathlib
    import time
    import pytest

    FAILED = pathlib.Path(__file__).parent / "failed"

    def test_fails_first():
        FAILED.touch()
        assert False

    @pytest.mark.parametrize("i", range({NUM_SLOW_TESTS}))
    def test_slow(i):
        deadline = time.monotonic() + 10
        while not FAILED.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        time.sleep(0.2)
""")

_COLLECTION_ERROR = "raise RuntimeError('collection error')\n"


def _run(pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch, *args: str) -> pytest.RunResult:
    # The inner session runs in a temporary directory, so it can only import
    # test.pylib if the repository root is on the path.
    monkeypatch.setenv("PYTHONPATH", os.pathsep.join(filter(None, [str(_REPO_ROOT), os.environ.get("PYTHONPATH")])))
    pytester.makepyfile(test_suite=_TESTS)
    return pytester.runpytest_subprocess("-p", "xdist", "-n", "2", "--dist=worksteal", "-p", "no:cacheprovider", *args)


def test_without_plugin_workers_drain_their_queues(pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch) -> None:
    result = _run(pytester, monkeypatch, "--maxfail=1")
    outcomes = result.parseoutcomes()
    assert outcomes.get("failed") == 1
    # The worker that failed stops on its own --maxfail, but the other one
    # runs its whole half of the queue before the session stops.
    assert outcomes.get("passed", 0) >= NUM_SLOW_TESTS // 2 - 2


def test_workers_stop_soon_after_maxfail(pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch) -> None:
    result = _run(pytester, monkeypatch, "--maxfail=1", "-p", "test.pylib.xdist_stop")
    outcomes = result.parseoutcomes()
    assert outcomes.get("failed") == 1
    result.stdout.fnmatch_lines(["*stopping after 1 failures*"])
    # Each worker passes at most the test it was running when the stop
    # arrived, out of NUM_SLOW_TESTS queued.
    assert outcomes.get("passed", 0) <= 2


def test_collection_errors_count_toward_maxfail(pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch) -> None:
    # xdist counts a collection error once per worker, so with 2 workers it
    # plus the test failure reach --maxfail=3.
    pytester.makepyfile(test_broken=_COLLECTION_ERROR)
    result = _run(pytester, monkeypatch, "--maxfail=3", "-p", "test.pylib.xdist_stop")
    outcomes = result.parseoutcomes()
    assert outcomes.get("failed") == 1
    result.stdout.fnmatch_lines(["*stopping after 3 failures*"])
    assert outcomes.get("passed", 0) <= 2
