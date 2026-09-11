#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""``passthrough`` — the scheduler that schedules nothing."""

from test.pylib.scheduling.scheduler import Scheduler


class Passthrough(Scheduler):
    """Make no decisions and hand the run to xdist, as test.py always has.

    It overrides nothing at all: no ``configure()``, so the run is configured
    exactly as the command line asked, and no ``plugin``, so no extra code is
    loaded.  Its pytest argv is byte-identical to the one produced before the
    scheduler platform existed, which makes it the known-good baseline to roll
    back to.
    """

    name = "passthrough"
