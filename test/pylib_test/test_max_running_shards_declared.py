#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""Every cluster test must say how much of the machine it uses.

test/cluster is the one suite where a claim can go missing: everywhere else it
is a constant applied at collection, from a conftest's pytestmark or a C++
case's own `-c`.  A cluster test's claim is a marker somebody wrote, and a
scheduler reading the markers is only as good as their coverage.

It asks pytest what exists rather than reading the source, since only
collection sees every marker.
"""

from __future__ import annotations

import pytest

from test import ALL_MODES
from test.pylib.marker_index import build_index
from test.pylib.running_shards import MARKER


# No scheduler will place these.  A skipped test never reaches the manager
# fixture (skip is also what skip_mode, skip_bug and skip_env apply), CI does
# not run non_gating at all, and no_parallel gets a step of its own.  skipif is
# not here: its condition is evaluated at setup, so the marker alone does not
# say whether the test runs.
EXEMPT_MARKERS = frozenset({"skip", "non_gating", "no_parallel"})


# The suite pins some tests to one mode (run_in_release, run_in_debug), so a
# single collection does not see them all -- and a mode this does not look at is
# a mode where a claim can go missing.  Every mode --mode accepts, for that
# reason.  Collection needs no build, so asking for a mode is fine even though
# CI runs the framework tests before building.
MODES = sorted(ALL_MODES)


@pytest.fixture(scope="module", params=MODES)
def cluster_test_index(request: pytest.FixtureRequest, tmp_path_factory: pytest.TempPathFactory):
    """The tests under test/cluster in one mode, and the markers they carry.

    The selection is fixed here rather than taken from the run, so narrowing the
    outer run with -k cannot narrow what this checks.
    """
    return build_index(["--mode", request.param, "test/cluster"],
                       tmpdir=tmp_path_factory.mktemp(f"marker_index_{request.param}"))


def test_the_index_is_not_empty(cluster_test_index):
    """Guard the guard: an empty collection would make everything below vacuous."""
    assert len(cluster_test_index) > 100, \
        f"only {len(cluster_test_index)} tests collected from test/cluster -- collection is broken, " \
        f"so the check below proves nothing"


def test_every_cluster_test_declares_max_running_shards(cluster_test_index):
    missing = sorted(test["nodeid"] for test in cluster_test_index
                     if test[MARKER] is None and not EXEMPT_MARKERS & set(test["markers"]))
    assert not missing, (
        f"{len(missing)} test(s) under test/cluster do not declare {MARKER}:\n"
        + "\n".join(f"    {nodeid}" for nodeid in missing)
        + f"\n\nEvery cluster test has to state the peak shards it runs, so a scheduler can\n"
          f"weigh it. Add @pytest.mark.{MARKER}(n), or let the test itself write it:\n"
          f"    ./test.py --mode dev <the test>\n"
          f"    ./test/pylib/update_max_running_shards_markers.py testlog/sqlite_*.db\n"
          f"See docs/dev/testing.md."
    )

