#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from __future__ import annotations

import pytest

from test.pylib.pool import Pool


@pytest.mark.asyncio
async def test_get_if_returns_matching_pooled_object() -> None:
    build_calls: list[str] = []

    async def build(label: str) -> str:
        build_calls.append(label)
        return label

    async def destroy(_obj: str) -> None:
        pass

    pool = Pool(2, build, destroy)
    first = await pool.get("first")
    second = await pool.get("second")
    await pool.put(first, False)
    await pool.put(second, False)

    result = await pool.get_if(lambda obj: obj == "second", "unused")

    assert result == "second"
    assert build_calls == ["first", "second"]


@pytest.mark.asyncio
async def test_get_if_builds_new_object_when_pool_has_capacity() -> None:
    build_calls: list[str] = []

    async def build(label: str) -> str:
        build_calls.append(label)
        return label

    async def destroy(_obj: str) -> None:
        pass

    pool = Pool(2, build, destroy)
    pooled = await pool.get("pooled")
    await pool.put(pooled, False)

    result = await pool.get_if(lambda obj: obj == "match", "match")

    assert result == "match"
    assert build_calls == ["pooled", "match"]


@pytest.mark.asyncio
async def test_get_if_falls_back_to_nonmatching_pooled_object_when_full() -> None:
    build_calls: list[str] = []

    async def build(label: str) -> str:
        build_calls.append(label)
        return label

    async def destroy(_obj: str) -> None:
        pass

    pool = Pool(1, build, destroy)
    pooled = await pool.get("pooled")
    await pool.put(pooled, False)

    result = await pool.get_if(lambda obj: obj == "match", "match")

    assert result == "pooled"
    assert build_calls == ["pooled"]


@pytest.mark.asyncio
async def test_get_if_builds_new_object_when_pool_is_unbounded() -> None:
    build_calls: list[str] = []

    async def build(label: str) -> str:
        build_calls.append(label)
        return label

    async def destroy(_obj: str) -> None:
        pass

    pool = Pool(None, build, destroy)
    pooled = await pool.get("pooled")
    await pool.put(pooled, False)

    result = await pool.get_if(lambda obj: obj == "match", "match")

    assert result == "match"
    assert build_calls == ["pooled", "match"]
