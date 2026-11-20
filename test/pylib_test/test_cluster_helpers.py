#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Tests for ScyllaCluster's topology requests, gossip waits and other small helpers.

The servers are stand-ins and the REST client is stubbed: no Scylla process is
involved, only what the cluster does around the requests.
"""

import pathlib
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any

import pytest

from test.pylib.internal_types import HostID, IPAddress, ServerInfo, ServerNum
from test.pylib.scylla_cluster import ScyllaCluster
from test.pylib_test._cluster import make_cluster


class FakeServer:
    """The parts of a ScyllaServer the cluster touches here."""

    def __init__(self, server_id: int, *, running: bool) -> None:
        self.server_id = ServerNum(server_id)
        self.ip_addr = IPAddress(f"127.0.0.{server_id}")
        self.rpc_address = self.ip_addr
        self.config = {"rpc_address": self.ip_addr}
        self.log_filename = pathlib.Path(f"/logs/{server_id}.log")
        self.is_running = running
        self.log_file = None
        self.maintenance_socket_dir = SimpleNamespace(cleanup=lambda: None)

    async def get_host_id(self, api: Any) -> HostID:
        return HostID(f"host-{self.server_id}")

    def change_ip(self, ip_addr: IPAddress) -> None:
        self.ip_addr = ip_addr

    def change_rpc_address(self, rpc_address: IPAddress) -> None:
        self.config["rpc_address"] = rpc_address

    def server_info(self) -> ServerInfo:
        return ServerInfo(self.server_id, self.ip_addr, self.rpc_address, "dc1", "rack1", 0)

    async def stop(self) -> None:
        self.is_running = False


@dataclass
class Api:
    """REST requests the cluster made, and how they behave."""

    requests: list[tuple[str, tuple]] = field(default_factory=list)
    error: str | None = None
    alive: list[list[IPAddress]] = field(default_factory=list)

    def stub(self, name: str):
        async def request(*args: Any, **kwargs: Any) -> None:
            self.requests.append((name, args))
            if self.error:
                raise RuntimeError(self.error)
        return request

    async def get_alive_endpoints(self, server_ip: IPAddress) -> list[IPAddress]:
        return self.alive.pop(0) if len(self.alive) > 1 else self.alive[0]


@pytest.fixture
async def cluster(tmp_path: pathlib.Path) -> AsyncIterator[tuple[ScyllaCluster, Api, list[ServerNum]]]:
    """A running cluster with two running stand-in servers, 1 and 2, and a stopped one, 3."""
    cluster = make_cluster(tmp_path)
    api = Api()
    stopped: list[ServerNum] = []
    for name in ("remove_node", "decommission_node", "rebuild_node",
                 "disable_tablet_balancing", "enable_tablet_balancing"):
        setattr(cluster.api, name, api.stub(name))
    cluster.api.get_alive_endpoints = api.get_alive_endpoints

    async def stop_server(server_id: ServerNum, gracefully: bool) -> None:
        stopped.append(server_id)

    async def lease_host() -> str:
        return "127.0.0.50"

    async def release_host(host: Any) -> None:
        pass

    cluster._stop_server = stop_server
    cluster.host_registry.lease_host = lease_host
    cluster.host_registry.release_host = release_host
    cluster.running.update({ServerNum(1): FakeServer(1, running=True), ServerNum(2): FakeServer(2, running=True)})
    cluster.stopped[ServerNum(3)] = FakeServer(3, running=False)
    try:
        async with cluster.run_in_thread():
            yield cluster, api, stopped
    finally:
        await cluster.recycle()


def request(cluster: ScyllaCluster, op: str, expected_error: str | None):
    """Remove server 3 through server 1, or decommission or rebuild server 2."""
    if op == "remove_node":
        return cluster._remove_node(ServerNum(1), ServerNum(3), [], expected_error)
    return getattr(cluster, f"_{op}")(ServerNum(2), expected_error)


OPS = pytest.mark.parametrize("op", ["remove_node", "decommission_node", "rebuild_node"])


@OPS
async def test_request_succeeds(cluster, op: str) -> None:
    cluster, api, stopped = cluster
    await request(cluster, op, None)

    assert [name for name, _ in api.requests] == [op]
    if op == "remove_node":
        assert cluster.removed == {ServerNum(3)}
        assert not stopped
    else:
        assert stopped == [ServerNum(2)]


@OPS
async def test_request_fails_as_expected(cluster, op: str) -> None:
    cluster, api, stopped = cluster
    api.error = "boom: no quorum"
    await request(cluster, op, "boom")

    assert not cluster.removed
    assert not stopped


@OPS
async def test_request_fails(cluster, op: str) -> None:
    cluster, api, stopped = cluster
    api.error = "boom: no quorum"
    with pytest.raises(RuntimeError, match=r"failed.*boom: no quorum"):
        await request(cluster, op, None)

    assert not cluster.removed
    assert not stopped


@OPS
async def test_request_fails_with_another_error(cluster, op: str) -> None:
    cluster, api, stopped = cluster
    api.error = "boom: no quorum"
    with pytest.raises(RuntimeError, match="did not contain expected error"):
        await request(cluster, op, "timed out")

    assert not cluster.removed
    assert not stopped


@OPS
async def test_request_succeeds_unexpectedly(cluster, op: str) -> None:
    cluster, api, stopped = cluster
    with pytest.raises(RuntimeError, match="succeeded when it should have failed"):
        await request(cluster, op, "boom")

    assert cluster.removed == ({ServerNum(3)} if op == "remove_node" else set())
    assert stopped == ([ServerNum(2)] if op == "decommission_node" else [])


async def test_server_sees_others(cluster) -> None:
    cluster, api, _ = cluster
    api.alive = [[IPAddress("127.0.0.1")], [IPAddress("127.0.0.1"), IPAddress("127.0.0.2"), IPAddress("127.0.0.3")]]
    await cluster.server_sees_others(ServerNum(1), 2, interval=5)

    assert not api.alive[1:]


async def test_server_sees_or_not_another_server(cluster) -> None:
    cluster, api, _ = cluster
    api.alive = [[IPAddress("127.0.0.1"), IPAddress("127.0.0.2")]]
    await cluster.server_sees_other_server(IPAddress("127.0.0.1"), IPAddress("127.0.0.2"), interval=5)
    await cluster.server_not_sees_other_server(IPAddress("127.0.0.1"), IPAddress("127.0.0.3"), interval=5)
    with pytest.raises(AssertionError, match="timed out"):
        await cluster.server_not_sees_other_server(IPAddress("127.0.0.1"), IPAddress("127.0.0.2"), interval=0.1)


@pytest.mark.parametrize("op", ["server_change_ip", "server_change_rpc_address"])
async def test_change_address(cluster, op: str) -> None:
    cluster, _, _ = cluster
    address = await getattr(cluster, op)(ServerNum(3))

    assert address == IPAddress("127.0.0.50")
    assert address in cluster.leased_ips
    server = cluster.stopped[ServerNum(3)]
    assert (server.ip_addr if op == "server_change_ip" else server.config["rpc_address"]) == address
    with pytest.raises(AssertionError, match="is running"):
        await getattr(cluster, op)(ServerNum(1))


async def test_get_table_or_view_id(cluster) -> None:
    cluster, _, _ = cluster
    queries = []

    async def run_async(query: str) -> list[SimpleNamespace]:
        queries.append(query)
        return [SimpleNamespace(id="view-id")] if "system_schema.views" in query else []

    cluster.cql = SimpleNamespace(run_async=run_async)
    try:
        assert await cluster.get_table_or_view_id("ks", "mv") == "view-id"
        assert await cluster.get_view_id("ks", "mv") == "view-id"
        with pytest.raises(IndexError):
            await cluster.get_table_id("ks", "mv")
    finally:
        cluster.cql = None
    assert "table_name = 'mv'" in queries[0] and "view_name = 'mv'" in queries[1]


@pytest.mark.parametrize("op", ["disable_tablet_balancing", "enable_tablet_balancing"])
async def test_tablet_balancing(cluster, op: str) -> None:
    cluster, api, _ = cluster
    await getattr(cluster, op)()
    assert api.requests == [(op, (IPAddress("127.0.0.1"),))]

    cluster.running.clear()
    with pytest.raises(RuntimeError, match="No running servers"):
        await getattr(cluster, op)()
