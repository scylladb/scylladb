#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Name to scheduler class.

Adding a scheduler should be one module plus one entry here. If it ever costs
more than that, the platform is wrong and the platform should be fixed.

Entries are classes, not instances. An instance here would be shared by every
run in the process, which unit tests trip over.
"""

from test.pylib.scheduling.scheduler import Scheduler
from test.pylib.scheduling.schedulers.passthrough import Passthrough

# Every scheduler, keyed by the name it declares. Keying on the class means
# the registry cannot disagree with the name a run reports, and a scheduler
# that forgot to set one fails here instead of halfway through a run.
SCHEDULERS: dict[str, type[Scheduler]] = {cls.name: cls for cls in (
    Passthrough,
)}

# Which scheduler runs when ``--scheduler`` is not given. This is separate
# from the names, so changing it renames nothing and makes no old run report
# wrong.
DEFAULT = Passthrough.name

# ``--scheduler=list`` prints the registry instead of running anything.
LIST_KEYWORD = "list"


def describe() -> str:
    """Human-readable registry listing for ``--scheduler=list``."""
    width = max(len(name) for name in SCHEDULERS)
    lines = ["Available schedulers:"]
    for name, cls in sorted(SCHEDULERS.items()):
        # One line about the scheduler: the first line of its docstring.
        summary = (cls.__doc__ or "").strip().split("\n")[0]
        default = "  [default]" if name == DEFAULT else ""
        lines.append(f"  {name:<{width}}  v{cls.version}  {summary}{default}")
    return "\n".join(lines)
