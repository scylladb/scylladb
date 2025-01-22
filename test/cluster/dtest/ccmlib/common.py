#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from __future__ import annotations

import time
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable


logger = logging.getLogger("ccm")


class CCMError(Exception):
    ...


class ArgumentError(CCMError):
    ...


def wait_for(func: Callable, timeout: int, first: float = 0.0, step: float = 1.0) -> bool:
    """Wait until func() evaluates to True.

    If func() evaluates to True before timeout expires, return True.  Otherwise, return False.
    """
    deadline = time.perf_counter() + timeout

    time.sleep(first)

    while time.perf_counter() < deadline:
        if func():
            return True
        time.sleep(step)

    return False
