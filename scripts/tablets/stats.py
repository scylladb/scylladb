#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""
Accumulators for the measures the reports take over their rows.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass


def share(value: float, total: float) -> float:
    """
    What part of a total a value takes, or 0 when the total is nothing to take a part of.
    """
    return value / total if total else 0


def overcommit(value: float | None, average: float) -> float | None:
    """
    How far a value is from an average, as a ratio.

    Peers which all hold nothing are evenly spread, not unmeasurable: an empty table sits at
    the average on every shard it is on, so it reports as balanced rather than as a blank.
    None when there is nothing to compare at all: no value, or one above an average of zero,
    which peers holding nothing cannot produce.
    """
    if average:
        return value / average if value is not None else None
    return 1.0 if value == 0 else None


@dataclass
class StatsAggregator:
    """
    Accumulates values as they are seen, so a caller measuring a set does not have to keep
    it, nor walk it once per measure.

    An aggregator which was given nothing has no min or max, an average of 0, and no
    overcommit to speak of.
    """
    count: int = 0
    total: float = 0
    min: float | None = None
    max: float | None = None

    @classmethod
    def of(cls, values: Iterable[float]) -> StatsAggregator:
        """
        An aggregator of values a caller already holds.
        """
        stats = cls()
        for value in values:
            stats.add(value)
        return stats

    def add(self, value: float) -> None:
        self.count += 1
        self.total += value
        self.min = value if self.min is None else min(self.min, value)
        self.max = value if self.max is None else max(self.max, value)

    def merge(self, other: StatsAggregator) -> None:
        """
        Takes in everything another aggregator was given, as if it had been added here.
        """
        self.count += other.count
        self.total += other.total
        if other.min is not None:
            self.min = other.min if self.min is None else min(self.min, other.min)
        if other.max is not None:
            self.max = other.max if self.max is None else max(self.max, other.max)

    def avg(self) -> float:
        return self.total / self.count if self.count else 0

    def max_frac(self) -> float:
        """
        The share of the total the largest value takes, which is what a bar scaled against
        the values is drawn to. 0 when nothing was added.
        """
        return share(self.max, self.total)

    def ovc(self) -> float | None:
        """
        Overcommit.
        How far the largest value overcommits the average of them all.
        """
        return overcommit(self.max, self.avg())
