#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Scheduler platform for ``test.py``.

A scheduler decides how the selected tests are spread over the machine. One is
picked per run with ``./test.py --scheduler=<name>``. The default,
``passthrough``, decides nothing of its own and lets pytest-xdist distribute the
tests.

See ``docs/dev/test-scheduler.md`` for how to write a scheduler.
"""


class SchedulerError(Exception):
    """A scheduler refused to schedule this run.

    Raised from ``configure()``. This is how a scheduler turns down a command
    line it cannot work with. ``test.py`` prints it as a single line naming the
    scheduler, not as a traceback, so the message should say what is wrong and
    what to do about it.
    """
