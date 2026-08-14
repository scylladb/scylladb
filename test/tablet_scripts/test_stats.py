#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from __future__ import annotations

import pytest

from tablets.stats import StatsAggregator
from tablets.stats import overcommit
from tablets.stats import share


def test_share_of_nothing_is_nothing() -> None:
    """
    A report divides by a total it did not choose, so an empty selection has to give a
    share rather than raise.
    """
    assert share(25, 100) == 0.25
    assert share(0, 0) == 0
    assert share(25, 0) == 0


def test_overcommit_reads_peers_which_all_hold_nothing_as_balanced() -> None:
    """
    They are evenly spread, which is what the report is asking about: an empty table sits at
    the average on every shard it is on. Only a value with nothing to compare it to, or one
    above an average of zero, is left undefined.
    """
    assert overcommit(150, 100) == 1.5
    assert overcommit(0, 100) == 0
    assert overcommit(0, 0) == 1.0
    assert overcommit(150, 0) is None
    assert overcommit(None, 0) is None
    assert overcommit(None, 100) is None


def test_stats_aggregator_accumulates_as_values_are_added() -> None:
    sizes = StatsAggregator()

    for size in (100, 300, 200):
        sizes.add(size)

    assert (sizes.count, sizes.total, sizes.min, sizes.max) == (3, 600, 100, 300)
    assert sizes.avg() == pytest.approx(200)
    # The largest value against the average, and against the total.
    assert sizes.ovc() == pytest.approx(1.5)
    assert sizes.max_frac() == pytest.approx(300 / 600)


def test_stats_aggregator_merges_what_another_was_given() -> None:
    """
    Merging says the same as having added every value to one aggregator.
    """
    sizes = StatsAggregator.of([100, 300])
    sizes.merge(StatsAggregator.of([200, 50]))

    assert (sizes.count, sizes.total, sizes.min, sizes.max) == (4, 650, 50, 300)


def test_stats_aggregator_merging_nothing_changes_nothing() -> None:
    sizes = StatsAggregator.of([100])
    sizes.merge(StatsAggregator())

    assert (sizes.count, sizes.total, sizes.min, sizes.max) == (1, 100, 100, 100)


def test_stats_aggregator_of_nothing_has_nothing_to_report() -> None:
    sizes = StatsAggregator()

    assert (sizes.count, sizes.total, sizes.min, sizes.max) == (0, 0, None, None)
    assert sizes.avg() == 0
    assert sizes.ovc() is None
    assert sizes.max_frac() == 0


def test_stats_aggregator_of_zeros_is_balanced() -> None:
    """
    Values which are all 0 are all at the average, so they are evenly spread.
    """
    sizes = StatsAggregator()

    sizes.add(0)
    sizes.add(0)

    assert sizes.max == 0
    assert sizes.avg() == 0
    assert sizes.ovc() == 1.0
    assert sizes.max_frac() == 0
