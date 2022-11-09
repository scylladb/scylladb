#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
import time
import asyncio
from typing import Callable, Awaitable, Optional, TypeVar, Generic

unique_name_prefix = 'test_'
T = TypeVar('T')


def unique_name():
    current_ms = int(round(time.time() * 1000))
    # If unique_name() is called twice in the same millisecond...
    if unique_name.last_ms >= current_ms:
        current_ms = unique_name.last_ms + 1
    unique_name.last_ms = current_ms
    return unique_name_prefix + str(current_ms)


async def wait_for(
        pred: Callable[[], Awaitable[Optional[T]]],
        deadline: float, period: float = 1) -> T:
    while True:
        assert(time.time() < deadline), "Deadline exceeded, failing test."
        res = await pred()
        if res is not None:
            return res
        await asyncio.sleep(period)


unique_name.last_ms = 0
