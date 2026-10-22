#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Tests for the guarantees of the cluster lifecycle that only show when
something has already gone wrong, and that the default test run never
exercises:

- the after-cluster cleanups of test/cluster's scylla_cluster_teardowns run
  once the cluster is recycled (SCYLLADB-2471);
- a cluster whose fixture never recycled it is swept up at session finish;
- a recycled cluster's server logs outlive the report of the phase that
  recycled it, so a failure in a module's last teardown still gets them;
- a module whose server exited on its own fails, with the server log.
"""

import asyncio
import pathlib
import signal
import textwrap
from functools import partial
from types import SimpleNamespace

import pytest

from test.pylib import runner
from test.pylib.runner import CLUSTER_KEY, recycle_leftover_clusters

pytest_plugins = ["pytester"]


def test_cluster_teardowns_run_after_the_cluster_is_recycled(pytester: pytest.Pytester) -> None:
    """The real scylla_cluster/scylla_cluster_teardowns fixtures, with the
    cluster factory stubbed: the cleanups must observe a recycled cluster,
    and fire in LIFO order."""
    pytester.makeconftest(textwrap.dedent("""\
        from contextlib import asynccontextmanager
        import pytest
        # Registers the two fixtures under test with this conftest.
        from test.cluster.conftest import scylla_cluster, scylla_cluster_teardowns

        class StubCluster:
            recycled = False

            async def recycle(self):
                self.recycled = True

        @pytest.fixture
        def testpy_cluster_factory():
            @asynccontextmanager
            async def lease(node, test_name):
                cluster = StubCluster()
                try:
                    yield cluster
                finally:
                    await cluster.recycle()
            return lease

        @pytest.fixture
        def testpy_test_name():
            return "stub::test"
        """))
    pytester.makeini(
        "[pytest]\n"
        "addopts = -p no:sugar -p no:xdist\n"
        "asyncio_mode = auto\n"
        "asyncio_default_fixture_loop_scope = session\n"
    )
    pytester.makepyfile(textwrap.dedent("""\
        import pathlib

        async def test_it(scylla_cluster, scylla_cluster_teardowns):
            log = pathlib.Path("teardowns.log")

            def record(name):
                def cleanup():
                    with log.open("a") as f:
                        f.write(f"{name} recycled={scylla_cluster.recycled}\\n")
                return cleanup

            scylla_cluster_teardowns.append(record("first"))
            scylla_cluster_teardowns.append(record("second"))
        """))
    result = pytester.runpytest()
    result.assert_outcomes(passed=1)
    assert (pytester.path / "teardowns.log").read_text().splitlines() == [
        "second recycled=True",
        "first recycled=True",
    ]


class StubCluster:
    """Records the sweep's calls; optionally fails stop() the way a wait on
    a foreign loop does."""

    def __init__(self, fail_stop: bool = False) -> None:
        self.fail_stop = fail_stop
        self.stops = 0
        self.recycles = 0

    async def stop(self) -> None:
        self.stops += 1
        if self.fail_stop:
            raise RuntimeError("attached to a different loop")

    async def recycle(self) -> None:
        self.recycles += 1


def test_leftover_clusters_are_recycled_at_session_finish(pytester: pytest.Pytester) -> None:
    """Clusters still stashed on a test item or its module get stopped and
    recycled exactly once; a stop() failure does not prevent the recycle;
    an entry the factory already reset to None is left alone."""
    pytester.makeini("[pytest]\nasyncio_default_fixture_loop_scope = session\n")
    item_a, item_b = pytester.getitems(textwrap.dedent("""\
        def test_a(): pass
        def test_b(): pass
        """))
    per_test = StubCluster()
    shared = StubCluster(fail_stop=True)
    item_a.stash[CLUSTER_KEY] = per_test
    item_a.parent.stash[CLUSTER_KEY] = shared   # the module node, shared by both items
    item_b.stash[CLUSTER_KEY] = None            # recycled normally: nothing to do

    swept = asyncio.run(recycle_leftover_clusters(SimpleNamespace(items=[item_a, item_b])))

    assert swept == 2

    assert (per_test.stops, per_test.recycles) == (1, 1)
    assert (shared.stops, shared.recycles) == (1, 1)


# Registers the real module-scoped scylla_cluster fixture, its cluster factory
# and the report hook, with just enough of test.py's options, stash entries
# and fixtures around them to run without a suite.
HARNESS_CONFTEST = textwrap.dedent("""\
    import logging
    import pathlib
    from types import SimpleNamespace

    import pytest

    from test.pylib.runner import (BUILD_MODE, TEST_SUITE, coverage_env, pytest_runtest_makereport, scylla_cluster,
                                   testpy_cluster_factory)

    def pytest_addoption(parser):
        parser.addoption("--tmpdir")
        parser.addoption("--save-log-on-success", action="store_true", dest="save_log_on_success")
        parser.addoption("--coverage", action="store_true")
        parser.addoption("--extra-scylla-cmdline-options", default="")

    def pytest_collection_modifyitems(items):
        for item in items:
            item.stash[BUILD_MODE] = "dev"
            item.parent.stash[TEST_SUITE] = SimpleNamespace(cfg={})

    @pytest.fixture(scope="module")
    def build_mode():
        return "dev"

    @pytest.fixture(scope="module")
    def suite_log_dir(request):
        return pathlib.Path(request.config.getoption("--tmpdir"), "dev")

    @pytest.fixture(scope="module")
    def scylla_binary():
        return "scylla"

    @pytest.fixture(scope="module")
    def testpy_uname(request):
        return request.module.__name__

    @pytest.fixture(scope="module")
    def testpy_logger(testpy_uname):
        return logging.getLogger(testpy_uname)
    """)


class LogWritingCluster:
    """Stands in for ScyllaCluster: one server, whose directory and log are named after the module, and a record of
    the server logs already on disk when each cluster is created."""

    save_log_on_success = False

    def __init__(self, events: list[str], exit_status: int | None, logger, vardir: pathlib.Path, **kwargs) -> None:
        self.events = events
        self.exit_status = exit_status
        self.name = logger.name
        self.workdir = vardir / self.name
        self.servers = self.running = {}

    async def add_server(self) -> None:
        self.workdir.mkdir(parents=True)
        log = self.workdir.with_suffix(".log")
        self.events.append(f"create {self.name} next to {sorted(p.name for p in log.parent.glob('*.log'))}")
        log.write_text("server log\n")
        self.servers[1] = SimpleNamespace(workdir=self.workdir, log_filename=log, cmd=SimpleNamespace(returncode=None))

    def take_log_savepoint(self) -> None:
        pass

    async def recycle(self) -> None:
        for server in self.running.values():
            server.cmd.returncode = -signal.SIGKILL if self.exit_status is None else self.exit_status


def run_with_stub_clusters(pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch, exit_status: int | None = None,
                           **modules: str) -> tuple[pytest.RunResult, list[str], pathlib.Path]:
    """Run the modules against the harness, with every server ending with exit_status when recycled, or with
    SIGKILL's if it is None; return the result, the cluster events and the failed_test dir."""
    events = []
    monkeypatch.setattr(runner, "ScyllaCluster", partial(LogWritingCluster, events, exit_status))
    pytester.makeconftest(HARNESS_CONFTEST)
    pytester.makeini(
        "[pytest]\n"
        "addopts = -p no:sugar -p no:xdist\n"
        "asyncio_mode = auto\n"
        "asyncio_default_fixture_loop_scope = session\n"
    )
    pytester.makepyfile(**modules)
    tmpdir = pytester.path / "testlog"
    (tmpdir / "pytest_tests_logs").mkdir(parents=True)
    return pytester.runpytest(f"--tmpdir={tmpdir}"), events, tmpdir / "dev" / "failed_test"


def test_failed_teardown_keeps_the_logs_of_a_recycled_cluster(pytester: pytest.Pytester,
                                                              monkeypatch: pytest.MonkeyPatch) -> None:
    """A module whose last teardown fails after its cluster was recycled gets
    the server log in failed_test/; a passing module's cluster is deleted
    before the next module's cluster is created, and the failed one after
    its log is copied."""
    result, events, failed_test_dir = run_with_stub_clusters(
        pytester, monkeypatch,
        test_a="def test_a(scylla_cluster): pass\n",
        test_b=textwrap.dedent("""\
            import pytest

            @pytest.fixture(scope="module")
            def failing_teardown(scylla_cluster):
                yield
                raise RuntimeError("deliberate teardown failure")

            def test_b(failing_teardown): pass
            """),
    )

    result.assert_outcomes(passed=2, errors=1)
    assert sorted(p.name for p in failed_test_dir.iterdir()) == ["test_b"]
    assert (failed_test_dir / "test_b" / "test_b.log").read_text() == "server log\n"
    assert "deliberate teardown failure" in (failed_test_dir / "test_b" / "stacktrace.txt").read_text()
    assert events == ["create test_a next to []", "create test_b next to []"]
    assert sorted(p.name for p in failed_test_dir.parent.iterdir()) == ["failed_test"]


def test_module_whose_server_exited_on_its_own_fails(pytester: pytest.Pytester,
                                                     monkeypatch: pytest.MonkeyPatch) -> None:
    """A server that exited with SIGABRT's status before recycle() stopped it
    fails the module's last teardown, and its log lands in failed_test/; the
    passing module of the test above is the twin without an exit status."""
    result, _, failed_test_dir = run_with_stub_clusters(
        pytester, monkeypatch, exit_status=-6,
        test_c="def test_c(scylla_cluster): pass\n",
    )

    result.assert_outcomes(passed=1, errors=1)
    result.stdout.fnmatch_lines(["E *RuntimeError: namespace(*) exited with status code -6"])
    assert (failed_test_dir / "test_c" / "test_c.log").read_text() == "server log\n"
    assert sorted(p.name for p in failed_test_dir.parent.iterdir()) == ["failed_test"]
