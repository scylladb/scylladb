#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""The run configuration a scheduler is allowed to change.

``RunConfig`` is built from the command line only.  It never takes a scheduler,
so it can be constructed and unit-tested with no scheduler in sight.  A
scheduler changes it through a closed set of named setters, so what a run ended
up with is one object, and every run says which scheduler produced it.
"""

import dataclasses
from typing import TYPE_CHECKING

from test.pylib.scheduling import SchedulerError

if TYPE_CHECKING:
    import argparse

    from test.pylib.scheduling.scheduler import Scheduler


DEFAULT_DIST = "worksteal"

@dataclasses.dataclass
class RunConfig:
    """Everything the execution module needs in order to run the tests.

    What a scheduler may read:

    ``options``
        the parsed ``test.py`` command line, as the user gave it.
    ``concurrency``, ``dist``
        the values the run would use right now, so a scheduler can decide
        relative to them instead of starting from nothing.
    """

    options: argparse.Namespace
    concurrency: int
    dist: str = DEFAULT_DIST

    @classmethod
    def defaults(cls, options: argparse.Namespace) -> RunConfig:
        """The starting point: the command line, with no scheduler involved yet."""
        return cls(options=options, concurrency=options.jobs)

    # --- everything a scheduler may change -----------------------------------
    #
    # This list is meant to be short.  Adding to it means changing RunConfig and
    # the execution module on purpose.  It never means changing test.py.

    def set_concurrency(self, n: int) -> None:
        """Set how many tests may run at once."""
        if n < 1:
            raise SchedulerError(f"concurrency must be at least 1, got {n}")
        self.concurrency = n

    def set_dist(self, mode: str) -> None:
        """Set which xdist distribution mode to use."""
        self.dist = mode
