#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Tests for the two guarantees of the per-test cluster lifecycle that only
show when something has already gone wrong, and that the default test run
never exercises:

- the after-cluster cleanups of test/cluster's scylla_cluster_teardowns run
  once the cluster is recycled (SCYLLADB-2471);
- a cluster whose fixture never recycled it is swept up at session finish.
"""

import asyncio
import textwrap
from types import SimpleNamespace

import pytest

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
