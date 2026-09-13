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
import json
import pathlib
from datetime import datetime
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

    # --- what the run says about the scheduler that ran it -------------------

    def as_json(self, scheduler: Scheduler) -> str:
        """What the scheduler decided, for the ``config`` column.

        That is the xdist parameters the run ended up with, plus whatever the
        scheduler reports about its own setup.

        The rest of the command line is left out on purpose.  It is not what the
        scheduler decided, and it is not ours to keep: ``--pytest-arg`` can pass
        this repo's ``--auth_password`` and ``--aws-secret-key``, and this DB is
        collected as a build artifact.

        A value JSON cannot store is written as its repr, so a bad value cannot
        stop a run before it starts.

        This is what the scheduler decided, which is not always what pytest
        ran.  ``--pytest-arg`` is passed on unparsed and lands last on the
        command line, so a ``-n`` or ``--dist`` in it wins over the values
        here, and this row will not know.
        """
        return json.dumps({"concurrency": self.concurrency,
                           "dist": self.dist,
                           "parameters": scheduler.parameters()},
                          sort_keys=True, default=repr)

    def log_scheduler(self, scheduler: Scheduler) -> None:
        """Say what scheduled this run, before the tests start.

        One line is always printed, so the console log of a CI job names the
        scheduler that produced whatever happened next.  With
        ``--gather-metrics`` the decisions also go into the metrics DB, so past
        runs can be looked up by scheduler.

        Without both, "we switched something and CI got weird" is a claim nobody
        can check.
        """
        plugin = f" plugin={scheduler.plugin}" if scheduler.plugin else ""
        print(f"scheduler: {scheduler.name}@{scheduler.version}{plugin}")
        if self.options.gather_metrics:
            # On purpose, this is not wrapped in try/except.  Metrics were
            # asked for.  A run that keeps going after failing to record what
            # scheduled it is exactly what this code exists to prevent: if it
            # then fails, nobody can tell what ran it.
            self.record(scheduler)

    def record(self, scheduler: Scheduler) -> None:
        """Write one ``scheduler_runs`` row into this run's metrics DB."""
        # Imported here, not at the top, so that starting test.py does not load
        # psutil and sqlite when metrics are off.
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
            # Every table points at host_info, and the row is normally written
            # by pytest, which has not started yet.  Writing it here is safe:
            # whoever gets there first wins.
            host = gather_host_info()
            writer.write_row_if_not_exist(host, HOST_INFO_TABLE, id_column="host_id")
            writer.write_row(
                SchedulerRun(host_id=host.host_id,
                             name=scheduler.name,
                             version=scheduler.version,
                             plugin=scheduler.plugin,
                             config=self.as_json(scheduler),
                             timestamp=datetime.now()),
                SCHEDULER_RUNS_TABLE)
        finally:
            writer.close()
