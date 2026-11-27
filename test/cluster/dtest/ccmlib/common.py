#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from __future__ import annotations

import asyncio
import inspect
import time
import logging
from functools import wraps
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine
    from typing import Any


logger = logging.getLogger("ccm")


BIN_DIR = "bin"


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


def run_sync[R](coro: Coroutine[Any, Any, R]) -> R:
    """Run a coroutine to completion from synchronous code, on a fresh event loop."""

    return asyncio.run(coro, loop_factory=asyncio.new_event_loop)


class Blocking:
    """Synchronous view of an async object: calls that return a coroutine run it with run_sync()."""

    def __init__(self, obj: Any) -> None:
        object.__setattr__(self, "__wrapped__", obj)

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self.__wrapped__, name)
        if not callable(attr):
            return attr

        @wraps(attr)
        def call(*args: Any, **kwargs: Any) -> Any:
            result = attr(*args, **kwargs)
            if inspect.iscoroutine(result):
                return run_sync(result)
            if inspect.isawaitable(result):
                raise TypeError(f"{name}() returned {result!r}, which Blocking cannot run")
            return result

        return call

    def __setattr__(self, name: str, value: Any) -> None:
        setattr(self.__wrapped__, name, value)
