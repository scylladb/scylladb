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

from __future__ import annotations

import dataclasses
import json
import pathlib
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import argparse

    from test.pylib.scheduling.scheduler import Scheduler


DEFAULT_DIST = "worksteal"


class SchedulerError(Exception):
    """A scheduler refused to schedule this run.

    Raised out of ``configure()``, which is how a scheduler turns down a
    command line it cannot honour.  ``test.py`` turns it into a one-line error
    naming the scheduler instead of a traceback, so the message should say what
    is wrong and what to do about it.
    """


@dataclasses.dataclass
class RunConfig:
    """Everything the execution module needs to run the tests.

    Read surface for a scheduler:

    ``options``
        the parsed ``test.py`` command line, exactly as the user gave it.
    ``concurrency``, ``dist``
        the defaults as they currently stand, so a scheduler can decide
        relative to them.
    """

    options: argparse.Namespace
    concurrency: int
    dist: str = DEFAULT_DIST

    @classmethod
    def defaults(cls, options: argparse.Namespace) -> RunConfig:
        """The starting point: the command line, no scheduler."""
        return cls(options=options, concurrency=options.jobs)

    # --- the closed vocabulary of changes ------------------------------------
    #
    # Adding a call here is a deliberate change to RunConfig and to the
    # execution module — never to test.py.

    def set_concurrency(self, n: int) -> None:
        """How many tests may run at once."""
        if n < 1:
            raise SchedulerError(f"concurrency must be at least 1, got {n}")
        self.concurrency = n

    def set_dist(self, mode: str) -> None:
        """Which xdist distribution mode to use."""
        self.dist = mode

    # --- what the run says about the scheduler that ran it -------------------

    def as_json(self) -> str:
        """This run's configuration, for the ``config`` column.

        The whole command line goes in, so a recorded run can be read back
        without the schema growing a column per option.  Anything JSON cannot
        hold is stored as its repr rather than failing a run before it starts.
        """
        return json.dumps({"concurrency": self.concurrency,
                           "dist": self.dist,
                           "options": vars(self.options)},
                          sort_keys=True, default=repr)

    def log_scheduler(self, scheduler: Scheduler) -> None:
        """Say what scheduled this run, before the tests start.

        One line always, so a CI job's console log names the scheduler that
        produced whatever happened next; the whole configuration in the metrics
        DB under ``--gather-metrics``, so past runs can be queried by
        scheduler.  Without either, "we switched something and CI got weird" is
        unfalsifiable.
        """
        plugin = f" plugin={scheduler.plugin}" if scheduler.plugin else ""
        print(f"scheduler: {scheduler.name}@{scheduler.version}{plugin}")
        if self.options.gather_metrics:
            # Deliberately not guarded: a run that proceeds after failing to
            # record what scheduled it is the situation this exists to prevent,
            # and analysing that run's failure later would have nothing to go
            # on.  Metrics were asked for, so failing to collect them stops the
            # run rather than quietly producing an unattributable one.
            self.record(scheduler)

    def record(self, scheduler: Scheduler) -> None:
        """Write one ``scheduler_runs`` row into this run's metrics DB."""
        # Imported here so that starting test.py does not pay for psutil and
        # sqlite when metrics are off.
        from test.pylib.db.model import SchedulerRun
        from test.pylib.db.writer import (
            DEFAULT_DB_NAME,
            HOST_INFO_TABLE,
            SCHEDULER_RUNS_TABLE,
            SQLiteWriter,
        )
        from test.pylib.resource_gather import gather_host_info

        tmpdir = pathlib.Path(self.options.tmpdir).absolute()
        tmpdir.mkdir(parents=True, exist_ok=True)
        writer = SQLiteWriter(tmpdir / DEFAULT_DB_NAME)
        try:
            # Every table references host_info, and pytest — which normally
            # writes that row — has not started yet.  Idempotent either way.
            host = gather_host_info()
            writer.write_row_if_not_exist(host, HOST_INFO_TABLE, id_column="host_id")
            writer.write_row(
                SchedulerRun(host_id=host.host_id,
                             name=scheduler.name,
                             version=scheduler.version,
                             plugin=scheduler.plugin,
                             config=self.as_json(),
                             timestamp=datetime.now()),
                SCHEDULER_RUNS_TABLE)
        finally:
            writer.close()
