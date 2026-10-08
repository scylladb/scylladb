#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""<tmpdir>/sched: scratch the processes of one run share.

Every process of a run, the controller and each xdist worker, points this module at the
run's --tmpdir from pytest_configure.  The controller empties the directory at session
start and removes it at session end, so nothing in it outlives the run.
"""

from __future__ import annotations

import shutil
from pathlib import Path

_root: Path | None = None


def configure(tmpdir: str | Path) -> None:
    global _root
    _root = Path(tmpdir).absolute() / "sched"


def prepare() -> None:
    """Start the run with an empty directory."""
    if _root is not None:
        shutil.rmtree(_root, ignore_errors=True)
        _root.mkdir(parents=True, exist_ok=True)


def remove() -> None:
    if _root is not None:
        shutil.rmtree(_root, ignore_errors=True)


def _path(name: str) -> Path | None:
    return _root / name if _root is not None else None


def boost_list_cache() -> Path | None:
    """Listings of the boost test binaries, shared by the workers."""
    return _path("boost_list_cache")


def containers() -> Path | None:
    """The cgroups of the containers each worker started, one file per worker."""
    return _path("containers")


def evictions() -> Path | None:
    """Flags telling a worker to skip the test it holds, one file per worker.

    When no waiting test fits in memory, the dynamic scheduler sends an idle worker home to
    free what that worker holds.  But an idle xdist worker already has its next test, and
    the controller cannot take a test back: the shutdown that ends the worker is also what
    makes it run the test it holds.  So before the shutdown the controller writes the held
    test's node id to a file named after the worker.  The worker finds it before running
    that test, skips it without reporting anything, and exits; the controller then queues
    the test again for a worker that has room.  Killing the worker instead would make
    xdist count a crash and start a replacement.
    """
    return _path("evictions")
