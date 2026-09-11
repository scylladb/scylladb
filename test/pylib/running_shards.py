#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""The peak cluster capacity a test uses, counted in shards.

``@pytest.mark.max_running_shards(n)`` limits how many Scylla shards a test
runs at the same time.  It is enforced: the cluster refuses the server that
would break the claim, before that server exists.

Shards, not servers: one server with ``--smp 8`` costs much more than one with
``--smp 1``.  A test without the marker is not restricted, so a new test can
run before anyone measures it.  What a claim means for scheduling is up to the
scheduler that reads it.
"""

from __future__ import annotations

import pytest


MARKER = "max_running_shards"


class RunningShardsExceeded(Exception):
    """A test tried to run more shards at once than it claimed."""


def claimed_shards(item: pytest.Item) -> int | None:
    """The test's claim, or None if it has no marker.

    Raises if the marker is not one positive whole number, so a typo fails the
    run instead of quietly meaning nothing.
    """
    marker = item.get_closest_marker(MARKER)
    if marker is None:
        return None
    amount = marker.args[0] if marker.args else marker.kwargs.get("amount")
    if len(marker.args) + len(marker.kwargs) != 1 or type(amount) is not int or amount < 1:
        raise ValueError(f"{item.nodeid}: bad {MARKER} marker, "
                         f"write it as @pytest.mark.{MARKER}(6)")
    return amount


class RunningShards:
    """A cluster's claim, and the peak it actually ran.

    A cluster belongs to one test, so the claim is set when the cluster is
    created and never changed again.
    """

    def __init__(self) -> None:
        self.claim: int | None = None
        self.high_water_mark = 0

    def reserve(self, running: int, adding: int, what: str) -> None:
        """Account for `adding` shards about to start on top of `running`.

        Raises before any of them starts.  So a refused reservation leaves
        nothing to undo, and never puts the shards on the machine at all.  That
        is what stops a broken claim from taking the whole run down.
        """
        total = running + adding
        if self.claim is not None and total > self.claim:
            raise RunningShardsExceeded(
                f"{what} would run {total} shards at once ({running} + {adding}), over this "
                f"test's claim of {MARKER}={self.claim}. Keep the test within its claim, or "
                f"raise the claim to @pytest.mark.{MARKER}({total})")
        self.high_water_mark = max(self.high_water_mark, total)
