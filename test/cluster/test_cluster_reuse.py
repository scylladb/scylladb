#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from __future__ import annotations

from dataclasses import dataclass
import fcntl
import json
from pathlib import Path

import pytest

from test.pylib.runner import BUILD_MODE, RUN_ID


pytestmark = pytest.mark.xdist_group("cluster_reuse")


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


def _state_dir(request: pytest.FixtureRequest) -> Path:
    path = Path(request.config.getoption("--tmpdir")).absolute() / "pytest_log"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _state_key(request: pytest.FixtureRequest) -> str:
    build_mode, run_id = _repeat_key(request)
    return f"{build_mode}:{run_id}"


def _load_shared_state(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _store_shared_state(path: Path, state: dict[str, object]) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(state, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def _read_profile_state(request: pytest.FixtureRequest) -> ProfileReuseState:
    path = _state_dir(request) / "cluster_reuse_profile_state.json"
    raw = _load_shared_state(path).get(_state_key(request), {})
    if not isinstance(raw, dict):
        return ProfileReuseState()
    return ProfileReuseState(
        workdir=str(raw.get("workdir")) if raw.get("workdir") is not None else None,
        dirty_triggered=bool(raw.get("dirty_triggered", False)),
    )


def _write_profile_state(request: pytest.FixtureRequest, state: ProfileReuseState) -> None:
    path = _state_dir(request) / "cluster_reuse_profile_state.json"
    lock_path = path.with_suffix(path.suffix + ".lock")
    with lock_path.open("a+", encoding="utf-8") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        raw = _load_shared_state(path)
        raw[_state_key(request)] = {
            "workdir": state.workdir,
            "dirty_triggered": state.dirty_triggered,
        }
        _store_shared_state(path, raw)
        fcntl.flock(lock, fcntl.LOCK_UN)


def _read_plain_state(request: pytest.FixtureRequest) -> str | None:
    path = _state_dir(request) / "cluster_reuse_plain_state.json"
    raw = _load_shared_state(path).get(_state_key(request))
    return str(raw) if raw is not None else None


def _write_plain_state(request: pytest.FixtureRequest, cluster_str: str) -> None:
    path = _state_dir(request) / "cluster_reuse_plain_state.json"
    lock_path = path.with_suffix(path.suffix + ".lock")
    with lock_path.open("a+", encoding="utf-8") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        raw = _load_shared_state(path)
        raw[_state_key(request)] = cluster_str
        _store_shared_state(path, raw)
        fcntl.flock(lock, fcntl.LOCK_UN)


async def _first_server_workdir(manager) -> str:
    servers = await manager.running_servers()
    assert len(servers) == 1
    return await manager.server_get_workdir(servers[0].server_id)


@pytest.mark.scylla_cluster(nodes=1)
async def test_profile_cluster_reuses_same_clean_cluster(manager, request: pytest.FixtureRequest) -> None:
    state = _read_profile_state(request)

    workdir = await _first_server_workdir(manager)
    assert not await manager.is_dirty()

    if state.workdir is None:
        state.workdir = workdir
    else:
        assert workdir == state.workdir
    _write_profile_state(request, state)


@pytest.mark.scylla_cluster(nodes=1)
@pytest.mark.scylla_resources(cpu=2, mem="1G")
async def test_profile_cluster_mutation_marks_cluster_dirty(manager, request: pytest.FixtureRequest) -> None:
    state = _read_profile_state(request)

    workdir = await _first_server_workdir(manager)
    if state.workdir is None:
        state.workdir = workdir
    else:
        assert workdir == state.workdir

    server_id = (await manager.running_servers())[0].server_id
    await manager.server_update_cmdline(server_id, ["--smp", "3"])

    state.dirty_triggered = True
    assert await manager.is_dirty()
    _write_profile_state(request, state)


@pytest.mark.scylla_cluster(nodes=1)
@pytest.mark.scylla_resources(cpu=2, mem="1G")
async def test_profile_cluster_gets_discarded_after_dirty_test(manager, request: pytest.FixtureRequest) -> None:
    state = _read_profile_state(request)
    workdir = await _first_server_workdir(manager)
    assert state.workdir is not None
    assert state.dirty_triggered
    assert workdir != state.workdir
    assert not await manager.is_dirty()


async def test_plain_cluster_reuses_item_level_cluster(manager, request: pytest.FixtureRequest) -> None:
    cluster_str = manager.current_cluster_str
    assert cluster_str is not None
    assert not await manager.is_dirty()

    previous = _read_plain_state(request)
    if previous is None:
        _write_plain_state(request, cluster_str)
    else:
        assert cluster_str == previous


async def test_plain_cluster_reuses_the_same_clean_cluster_again(manager, request: pytest.FixtureRequest) -> None:
    cluster_str = manager.current_cluster_str
    assert cluster_str is not None

    previous = _read_plain_state(request)
    if previous is None:
        _write_plain_state(request, cluster_str)
    else:
        assert cluster_str == previous
    assert not await manager.is_dirty()
