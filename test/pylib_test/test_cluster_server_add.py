#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Tests for what server_add() and servers_add() do around add_server().

add_server() and the driver calls are stubbed: no Scylla process is involved,
only the arguments the cluster passes on and the caller-side work around them.
"""

import asyncio
import inspect
import pathlib
import re
from collections import OrderedDict
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Any

import pytest

from test.pylib.internal_types import IPAddress, ServerInfo, ServerNum, ServerUpState
from test.pylib.scylla_cluster import ReplaceConfig, ScyllaCluster
from test.pylib_test._cluster import make_cluster


ADD_SERVER = inspect.signature(ScyllaCluster.add_server)
BOTH = pytest.mark.parametrize("method", ["server_add", "servers_add"])


@dataclass
class Calls:
    """What the stubs saw, and how the add_server() stub behaves."""

    added: list[dict[str, Any]] = field(default_factory=list)
    connected: list[dict[str, Any]] = field(default_factory=list)
    updated: int = 0
    events: list[str] = field(default_factory=list)
    fail: Exception | None = None
    hang: bool = False


@pytest.fixture
async def cluster(tmp_path: pathlib.Path) -> AsyncIterator[tuple[ScyllaCluster, Calls]]:
    """A ScyllaCluster running on its own thread, with add_server() and the driver stubbed."""
    cluster = make_cluster(tmp_path)
    calls = Calls()

    async def add_server(*args: Any, **kwargs: Any) -> ServerInfo:
        bound = ADD_SERVER.bind(cluster, *args, **kwargs)
        bound.apply_defaults()
        calls.added.append({name: value for name, value in bound.arguments.items() if name != "self"})
        calls.events.append("add")
        if calls.fail:
            raise calls.fail
        if calls.hang:
            await asyncio.Event().wait()
        n = len(calls.added)
        return ServerInfo(ServerNum(n), IPAddress(f"127.0.0.{n}"), IPAddress(f"127.0.0.{n}"), "dc1", "rack1", n)

    async def driver_connect(**kwargs: Any) -> None:
        calls.connected.append(kwargs)

    def driver_update() -> None:
        calls.updated += 1

    async def get_host_ip(server_id: ServerNum) -> IPAddress:
        return IPAddress(f"10.0.0.{server_id}")

    async def others_not_see_server(server_ip: IPAddress, interval: float = 45.) -> None:
        calls.events.append(f"unseen {server_ip}")

    cluster.add_server = add_server  # type: ignore[method-assign]
    cluster.driver_connect = driver_connect  # type: ignore[method-assign]
    cluster._driver_update = driver_update  # type: ignore[method-assign]
    cluster.get_host_ip = get_host_ip  # type: ignore[method-assign]
    cluster.others_not_see_server = others_not_see_server  # type: ignore[method-assign]
    try:
        async with cluster.run_in_thread():
            yield cluster, calls
    finally:
        await cluster.recycle()


async def add_one(cluster: ScyllaCluster, method: str, **kwargs: Any) -> ServerInfo:
    """Add one server with server_add() or servers_add()."""
    if method == "server_add":
        return await cluster.server_add(**kwargs)
    [info] = await cluster.servers_add(**kwargs)
    return info


@BOTH
async def test_arguments_reach_add_server(cluster, method: str) -> None:
    cluster, calls = cluster
    info = await add_one(cluster, method,
                         cmdline=["--smp", "1"],
                         config={"key": "value"},
                         property_file={"dc": "dc2", "rack": "r2"},
                         seeds=[IPAddress("127.0.0.9")],
                         server_encryption="all")

    assert info.server_id == 1
    [added] = calls.added
    assert added["cmdline"] == ["--smp", "1"]
    assert added["config"] == {"key": "value"}
    assert added["property_file"] == {"dc": "dc2", "rack": "r2"}
    assert added["seeds"] == [IPAddress("127.0.0.9")]
    assert added["server_encryption"] == "all"
    assert added["start"] is True
    assert added["replace_cfg"] is None
    assert added["expected_error"] is None
    assert added["expected_server_up_state"] is ServerUpState.SERVING
    assert calls.connected == [{}]


async def test_server_add_property_file_may_be_a_dict_subclass(cluster) -> None:
    cluster, calls = cluster
    await cluster.server_add(property_file=OrderedDict(dc="dc2", rack="r2"))

    assert calls.added[0]["property_file"] == {"dc": "dc2", "rack": "r2"}


@BOTH
async def test_expected_server_up_state(cluster, method: str) -> None:
    cluster, calls = cluster
    await add_one(cluster, method, expected_server_up_state=ServerUpState.CQL_ALTERNATOR_QUERIED)

    assert calls.added[0]["expected_server_up_state"] is ServerUpState.CQL_ALTERNATOR_QUERIED


@BOTH
async def test_launcher(cluster, method: str) -> None:
    cluster, calls = cluster
    await add_one(cluster, method, launcher=["nsenter", "--target", "1"])

    assert calls.added[0]["launcher"] == ["nsenter", "--target", "1"]


@BOTH
async def test_without_driver_the_up_state_is_capped(cluster, method: str) -> None:
    cluster, calls = cluster
    await add_one(cluster, method, connect_driver=False)

    assert calls.added[0]["expected_server_up_state"] is ServerUpState.HOST_ID_QUERIED
    assert not calls.connected
    assert not calls.updated


@BOTH
async def test_connected_driver_is_refreshed(cluster, method: str) -> None:
    cluster, calls = cluster
    cluster.cql = object()  # type: ignore[assignment]
    await add_one(cluster, method)

    assert calls.updated == 1
    assert not calls.connected


@BOTH
async def test_driver_connect_opts(cluster, method: str) -> None:
    cluster, calls = cluster
    await add_one(cluster, method, driver_connect_opts={"control_connection_fallback": True})

    assert calls.connected == [{"control_connection_fallback": True}]


@BOTH
async def test_expected_error(cluster, method: str) -> None:
    cluster, calls = cluster
    await add_one(cluster, method, expected_error="bad (thing)")

    assert calls.added[0]["expected_error"] == "bad (thing)"
    assert re.escape("bad (thing)") in cluster.ignore_log_patterns
    assert not calls.connected


@BOTH
async def test_failure_is_wrapped(cluster, method: str) -> None:
    cluster, calls = cluster
    calls.fail = ValueError("no such option")
    with pytest.raises(RuntimeError, match="^Failed to add server") as failure:
        await add_one(cluster, method)

    assert isinstance(failure.value.__cause__, ValueError)


@BOTH
async def test_timeout(cluster, method: str) -> None:
    cluster, calls = cluster
    calls.hang = True
    with pytest.raises(RuntimeError, match="^Failed to add server") as failure:
        # Fail instead of hanging if the timeout is not applied.
        async with asyncio.timeout(10):
            await add_one(cluster, method, timeout=0.1)

    assert isinstance(failure.value.__cause__, TimeoutError)


@BOTH
async def test_replace_waits_until_the_replaced_server_is_seen_dead(cluster, method: str) -> None:
    cluster, calls = cluster
    replace_cfg = ReplaceConfig(replaced_id=ServerNum(7), reuse_ip_addr=False, use_host_id=False,
                                ignore_dead_nodes=[IPAddress("10.0.0.8")])
    await add_one(cluster, method, replace_cfg=replace_cfg)

    assert calls.events == ["unseen 10.0.0.7", "unseen 10.0.0.8", "add"]
    assert calls.added[0]["replace_cfg"] is replace_cfg


async def test_servers_add_replaces_only_one_server(cluster) -> None:
    cluster, calls = cluster
    replace_cfg = ReplaceConfig(replaced_id=ServerNum(7), reuse_ip_addr=False, use_host_id=False, wait_dead=False)
    with pytest.raises(AssertionError, match="one server"):
        await cluster.servers_add(2, replace_cfg=replace_cfg)

    assert not calls.added


async def test_servers_add_auto_rack_dc(cluster) -> None:
    cluster, calls = cluster
    infos = await cluster.servers_add(3, auto_rack_dc="dc1")

    assert len(infos) == 3
    assert [added["property_file"] for added in calls.added] == [
        {"dc": "dc1", "rack": "rack1"}, {"dc": "dc1", "rack": "rack2"}, {"dc": "dc1", "rack": "rack3"}]
