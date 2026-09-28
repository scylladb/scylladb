#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from collections.abc import Iterable
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, call

import pytest
from cassandra.policies import FallthroughRetryPolicy  # type: ignore
from cassandra.protocol import OverloadedErrorMessage  # type: ignore

from test.cluster.mv import util


class _FakeSession:
    def __init__(self, results: Iterable[Any]) -> None:
        self.results = iter(results)
        self.calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    async def run_async(self, *args: Any, **kwargs: Any) -> Any:
        self.calls.append((args, kwargs))
        result = next(self.results)
        if isinstance(result, BaseException):
            raise result
        return result


def _overloaded_error() -> OverloadedErrorMessage:
    return OverloadedErrorMessage(0x1001, "overloaded", {})


async def test_run_with_overload_retries_reuses_host_and_arguments(monkeypatch: pytest.MonkeyPatch) -> None:
    result = object()
    cql = _FakeSession([_overloaded_error(), _overloaded_error(), result])
    statement = SimpleNamespace(retry_policy=None)
    host = object()
    execution_profile = object()
    sleep = AsyncMock()
    monkeypatch.setattr(util.asyncio, "sleep", sleep)

    actual = await util.run_with_overload_retries(cql, statement, [1], host=host, execution_profile=execution_profile)

    assert actual is result
    assert isinstance(statement.retry_policy, FallthroughRetryPolicy)
    assert cql.calls == [
        ((statement, [1]), {"host": host, "execution_profile": execution_profile}),
        ((statement, [1]), {"host": host, "execution_profile": execution_profile}),
        ((statement, [1]), {"host": host, "execution_profile": execution_profile}),
    ]
    assert sleep.await_args_list == [call(util.RETRY_DELAY), call(util.RETRY_DELAY)]


async def test_run_with_overload_retries_rethrows_other_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    error = RuntimeError("request failed")
    cql = _FakeSession([error, object()])
    sleep = AsyncMock()
    monkeypatch.setattr(util.asyncio, "sleep", sleep)

    with pytest.raises(RuntimeError) as exc_info:
        await util.run_with_overload_retries(cql, SimpleNamespace(retry_policy=None), host=object())

    assert exc_info.value is error
    sleep.assert_not_awaited()


async def test_run_with_overload_retries_is_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    cql = _FakeSession(_overloaded_error() for _ in range(util.MAX_RETRIES + 1))
    sleep = AsyncMock()
    monkeypatch.setattr(util.asyncio, "sleep", sleep)

    with pytest.raises(OverloadedErrorMessage):
        await util.run_with_overload_retries(cql, SimpleNamespace(retry_policy=None), host=object())

    assert len(cql.calls) == util.MAX_RETRIES + 1
    assert sleep.await_count == util.MAX_RETRIES
