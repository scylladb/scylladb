#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from __future__ import annotations

from dataclasses import dataclass

import pytest

from test.pylib.runner import BUILD_MODE, RUN_ID


@dataclass
class ProfileReuseState:
    workdir: str | None = None
    dirty_triggered: bool = False


PROFILE_STATE: dict[tuple[str, int], ProfileReuseState] = {}
PLAIN_STATE: dict[tuple[str, int], str] = {}


def _repeat_key(request: pytest.FixtureRequest) -> tuple[str, int]:
    build_mode = request.node.stash.get(BUILD_MODE, "default")
    run_id = request.node.stash.get(RUN_ID, 0)
    return build_mode, run_id


async def _first_server_workdir(manager) -> str:
    servers = await manager.running_servers()
    assert len(servers) == 1
    return await manager.server_get_workdir(servers[0].server_id)


@pytest.mark.scylla_cluster(nodes=1)
async def test_profile_cluster_reuses_same_clean_cluster(manager, request: pytest.FixtureRequest) -> None:
    state = PROFILE_STATE.setdefault(_repeat_key(request), ProfileReuseState())

    workdir = await _first_server_workdir(manager)
    assert not await manager.is_dirty()

    if state.workdir is None:
        state.workdir = workdir
    else:
        assert workdir == state.workdir


@pytest.mark.scylla_cluster(nodes=1)
async def test_profile_cluster_mutation_marks_cluster_dirty(manager, request: pytest.FixtureRequest) -> None:
    state = PROFILE_STATE.setdefault(_repeat_key(request), ProfileReuseState())

    workdir = await _first_server_workdir(manager)
    if state.workdir is None:
        state.workdir = workdir
    else:
        assert workdir == state.workdir

    server_id = (await manager.running_servers())[0].server_id
    await manager.server_update_cmdline(server_id, ["--smp", "3"])

    state.dirty_triggered = True
    assert await manager.is_dirty()


@pytest.mark.scylla_cluster(nodes=1)
async def test_profile_cluster_gets_discarded_after_dirty_test(manager, request: pytest.FixtureRequest) -> None:
    state = PROFILE_STATE[_repeat_key(request)]
    workdir = await _first_server_workdir(manager)
    assert state.workdir is not None
    assert state.dirty_triggered
    assert workdir != state.workdir
    assert not await manager.is_dirty()


async def test_plain_cluster_reuses_item_level_cluster(manager, request: pytest.FixtureRequest) -> None:
    key = _repeat_key(request)

    cluster_str = manager.current_cluster_str
    assert cluster_str is not None
    assert not await manager.is_dirty()

    if key not in PLAIN_STATE:
        PLAIN_STATE[key] = cluster_str
    else:
        assert cluster_str == PLAIN_STATE[key]


async def test_plain_cluster_reuses_the_same_clean_cluster_again(manager, request: pytest.FixtureRequest) -> None:
    key = _repeat_key(request)
    cluster_str = manager.current_cluster_str
    assert cluster_str is not None

    if key not in PLAIN_STATE:
        PLAIN_STATE[key] = cluster_str
    else:
        assert cluster_str == PLAIN_STATE[key]
    assert not await manager.is_dirty()
