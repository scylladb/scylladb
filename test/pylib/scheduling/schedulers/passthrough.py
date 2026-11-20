#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""``passthrough``: leave the distribution to xdist."""

from test.pylib.scheduling.scheduler import Scheduler


class Passthrough(Scheduler):
    """Let xdist distribute the tests, work-stealing.

    The tests are still scheduled. xdist gives each worker the next test, and
    lets an idle worker take work from a busy one. This scheduler decides
    nothing of its own about that.

    It overrides neither half. There is no ``configure()``, so the number of
    jobs and the distribution mode are the ones the command line asked for, and
    there is no ``plugin``, so nothing extra is loaded.
    """

    name = "passthrough"
