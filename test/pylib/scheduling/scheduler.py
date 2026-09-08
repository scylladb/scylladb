#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""What a scheduler is: the interface ``test.py`` expects.

A scheduler is a subclass of :class:`Scheduler` in the registry, instantiated
once per run with no arguments.  ``test.py`` reads ``name`` and ``version`` to
say what scheduled the run, calls ``configure()`` once before pytest starts,
and loads ``plugin`` inside pytest if there is one.  That is the whole
interface.

Everything except ``name`` has a default that does nothing, so a scheduler
writes down only the half it actually uses:

``configure(cfg)``
    runs in ``test.py``, *before* pytest starts — pre-run decisions only, such
    as concurrency and distribution mode.  It gets a
    :class:`~test.pylib.scheduling.config.RunConfig` and changes it through the
    named setters there; it returns nothing, must not execute anything, and may
    raise :class:`~test.pylib.scheduling.config.SchedulerError` to abort.

``plugin``
    a pytest plugin, loaded *inside* pytest on the controller and the workers —
    everything else: dispatch, collection, per-test hooks, fixtures,
    worker-side enforcement.  This is where real scheduling lives, and it is
    also the only place that sees what the tests declare about themselves.

See ``docs/dev/test-scheduler.md``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from test.pylib.scheduling.config import RunConfig


class Scheduler:
    """Base class for schedulers.  Its defaults are ``passthrough``'s behaviour."""

    #: What the scheduler does — never a role (``default``) and never a
    #: parameter it happens to pass through.  It is also the key it is
    #: registered under, and which scheduler is the default is a separate fact
    #: kept in the registry.  Required: a subclass that forgets it fails when
    #: the registry is built.
    name: str

    #: Bumped whenever the scheduler's behaviour changes, so that a failure
    #: weeks later is attributable.  Prefer adding a new scheduler over
    #: changing an existing one in place.
    version: str = "1"

    #: Dotted module path of the scheduler's pytest plugin, if it has one.
    plugin: str | None = None

    def configure(self, cfg: RunConfig) -> None:
        """Decide what has to be decided before pytest starts.

        Called once per run.  The default decides nothing, which is what makes
        a scheduler that only ships a plugin a complete scheduler.

        An override returns nothing — everything it wants is expressed as
        recorded changes on *cfg*, so those changes can be attributed
        afterwards — and must not execute anything, build a command line,
        import xdist, or mutate global state.  It decides; it does not act.

        Raise :class:`~test.pylib.scheduling.config.SchedulerError` to turn
        down a command line this scheduler cannot honour; the message is the
        scheduler's to write, since only it knows what could not be honoured.
        """
