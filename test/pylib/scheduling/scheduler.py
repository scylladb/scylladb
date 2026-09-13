#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""What a scheduler is: the interface ``test.py`` expects.

A scheduler is a subclass of :class:`Scheduler` listed in the registry.  It is
created once per run, with no arguments.  ``test.py`` then:

* reads ``name`` and ``version``, to say what scheduled the run,
* calls ``configure()`` once, before pytest starts,
* loads ``plugin`` inside pytest, if there is one.

That is the whole interface.  Only ``name`` is required.  Everything else has a
default that does nothing, so a scheduler writes down only the part it uses.

A scheduler has two halves, and both are optional:

``configure(cfg)``
    Runs before pytest starts.  It can only set things that must be known
    before the pytest process exists, such as how many tests run at once.

``plugin``
    A pytest plugin.  It runs inside the pytest session, on the controller and
    on the workers.  This is where real scheduling happens.  It is also the only
    place that can see what the tests say about themselves.

See ``docs/dev/test-scheduler.md``.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from test.pylib.scheduling.config import RunConfig


class Scheduler:
    """Base class for schedulers.

    Subclass it, set ``name``, and override the part you need.  Every default
    here does nothing, so a subclass that overrides nothing behaves like
    ``passthrough``: xdist distributes the tests and no scheduler code runs.

    Override ``configure()`` to decide something before pytest starts.  Set
    ``plugin`` to run code inside the pytest session.
    """

    #: What the scheduler does.  Not a role, so never "default", and not a
    #: setting it passes on, so never "worksteal".  This is also the key it is
    #: registered under.  Which scheduler is the default is kept in the
    #: registry instead.  Required: a subclass that forgets it fails when the
    #: registry is built.
    name: str

    #: Raise this whenever the scheduler starts behaving differently, so that a
    #: failure weeks later can be traced back.  Adding a new scheduler is
    #: better than changing one that CI already uses.
    version: str = "1"

    #: Dotted module path of the scheduler's pytest plugin, if it has one.
    plugin: str | None = None

    def configure(self, cfg: RunConfig) -> None:
        """Decide what must be decided before pytest starts.

        Called once per run.  The default decides nothing, so a scheduler that
        only has a plugin is already complete.

        Return nothing.  Say what you want by changing *cfg*, using the setters
        it offers.  Do not run anything, build a command line, import xdist, or
        change global state here.  This method decides; it does not act.

        Raise :class:`~test.pylib.scheduling.SchedulerError` to turn down
        a command line this scheduler cannot work with.  Write the message
        yourself: only the scheduler knows what it could not do.
        """
