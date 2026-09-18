#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Name to scheduler class.

Adding a scheduler should be one module plus one entry here.  If it ever costs
more than that, the platform is wrong and the platform should be fixed.

Entries are classes, not instances.  An instance here would be shared by every
run in the process, which unit tests trip over.
"""

from test.pylib.scheduling.scheduler import Scheduler
from test.pylib.scheduling.schedulers.passthrough import Passthrough

#: Every scheduler, keyed by the name it declares.  Keying on the class means
#: the registry cannot disagree with the name a run reports, and a scheduler
#: that forgot to set one fails here instead of halfway through a run.
SCHEDULERS: dict[str, type[Scheduler]] = {cls.name: cls for cls in (
    Passthrough,
)}

#: Which scheduler runs when ``--scheduler`` is not given.  This is separate
#: from the names, so changing it renames nothing and makes no old run report
#: wrong.
DEFAULT = Passthrough.name

#: ``--scheduler=list`` prints the registry instead of running anything.
LIST_KEYWORD = "list"


def known_names() -> list[str]:
    return sorted(SCHEDULERS)


def get_scheduler(name: str) -> Scheduler:
    """Create the named scheduler, once, for this run."""
    # No check on the name: ``--scheduler`` has already made it against this
    # registry, which is where a user can get it wrong.
    return SCHEDULERS[name]()


def summary(cls: type[Scheduler]) -> str:
    """One line about the scheduler: the first line of its docstring."""
    lines = (cls.__doc__ or "").strip().splitlines()
    return lines[0] if lines else ""


def describe() -> str:
    """Human-readable registry listing for ``--scheduler=list``."""
    width = max(len(name) for name in SCHEDULERS)
    return "\n".join(["Available schedulers:", *(
        f"  {name:<{width}}  v{SCHEDULERS[name].version}  {summary(SCHEDULERS[name])}"
        f"{'  [default]' if name == DEFAULT else ''}"
        for name in known_names())])
