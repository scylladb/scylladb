#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Name to scheduler class.

Adding a scheduler is one module plus one entry here.  If it ever costs more
than that, the abstraction failed.

Entries are *classes*, not instances: a module-level instance would be a
singleton shared by every run in the process, which unit tests trip over.
"""

from __future__ import annotations

from test.pylib.scheduling.scheduler import Scheduler
from test.pylib.scheduling.schedulers.passthrough import Passthrough

#: Every scheduler, keyed by the name it declares — so a registration cannot
#: disagree with the name a run reports, and a scheduler that forgot to declare
#: one fails here rather than halfway through a run.
SCHEDULERS: dict[str, type[Scheduler]] = {cls.name: cls for cls in (
    Passthrough,
)}

#: Which scheduler is used when ``--scheduler`` is not given.  A separate fact
#: from any scheduler's name: changing it renames nothing and invalidates no
#: old run report.
DEFAULT = Passthrough.name

#: ``--scheduler=list`` prints the registry instead of running anything.
LIST_KEYWORD = "list"


def known_names() -> list[str]:
    return sorted(SCHEDULERS)


def get_scheduler(name: str) -> Scheduler:
    """Instantiate the named scheduler, once, for this run.

    The name has already been checked against this registry by ``--scheduler``
    itself, which is where a user can get it wrong.
    """
    return SCHEDULERS[name]()


def summary(cls: type[Scheduler]) -> str:
    """The scheduler's one-line description: the first line of its docstring."""
    return (cls.__doc__ or "").strip().splitlines()[0] if cls.__doc__ else ""


def describe() -> str:
    """Human-readable registry listing for ``--scheduler=list``."""
    width = max(len(name) for name in SCHEDULERS)
    return "\n".join(["Available schedulers:", *(
        f"  {name:<{width}}  v{SCHEDULERS[name].version}  {summary(SCHEDULERS[name])}"
        f"{'  [default]' if name == DEFAULT else ''}"
        for name in known_names())])
