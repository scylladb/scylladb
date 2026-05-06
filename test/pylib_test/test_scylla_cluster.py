#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import logging
import pathlib

import pytest

from test.pylib.host_registry import Host
from test.pylib.internal_types import IPAddress, ServerInfo, ServerNum
from test.pylib.scylla_cluster import ScyllaCluster


class FakeHostRegistry:
    def __init__(self) -> None:
        self.next_host_id = 0
        self.released_hosts: list[Host] = []

    async def lease_host(self) -> Host:
        self.next_host_id += 1
        return Host(f"127.0.0.{self.next_host_id}")

    async def release_host(self, host: Host) -> None:
        self.released_hosts.append(host)


class SlowInstallingServer:
    def __init__(self, server_id: ServerNum, ip_addr: IPAddress) -> None:
        self.server_id = server_id
        self.ip_addr = ip_addr
        self.rpc_address = ip_addr
        self.datacenter = "dc1"
        self.rack = "rack1"
        self.workdir = pathlib.Path(f"server-{server_id}")
        self.install_started = asyncio.Event()
        self.finish_install = asyncio.Event()
        self.started = False
        self.stop_calls = 0
        self.stop_gracefully_calls = 0

    async def install(self) -> None:
        self.install_started.set()
        await self.finish_install.wait()

    async def install_and_start(self, api, expected_error=None, expected_server_up_state=None, before_start=None) -> None:
        await self.install()
        try:
            if before_start is not None:
                before_start()
            self.started = True
        except BaseException:
            await self.stop()
            raise

    async def stop(self) -> None:
        self.stop_calls += 1

    async def stop_gracefully(self) -> None:
        self.stop_gracefully_calls += 1

    def server_info(self) -> ServerInfo:
        return ServerInfo(self.server_id, self.ip_addr, self.rpc_address, self.datacenter, self.rack, 0)


def make_cluster() -> tuple[ScyllaCluster, FakeHostRegistry, dict[str, SlowInstallingServer]]:
    host_registry = FakeHostRegistry()
    created_servers: dict[str, SlowInstallingServer] = {}

    def create_server(params: ScyllaCluster.CreateServerParams) -> SlowInstallingServer:
        server = SlowInstallingServer(ServerNum(1), params.ip_addr)
        created_servers["server"] = server
        return server

    cluster = ScyllaCluster(logging.getLogger("test_scylla_cluster"), host_registry, replicas=0, create_server=create_server)
    cluster.is_running = True
    return cluster, host_registry, created_servers


async def start_adding_server(cluster: ScyllaCluster, created_servers: dict[str, SlowInstallingServer]) -> tuple[asyncio.Task, SlowInstallingServer]:
    add_task = asyncio.create_task(cluster.add_server())
    while "server" not in created_servers:
        await asyncio.sleep(0)
    server = created_servers["server"]
    await server.install_started.wait()
    assert cluster.starting[server.server_id] is server
    return add_task, server


@pytest.mark.asyncio
@pytest.mark.parametrize("stop_method", ["stop", "stop_gracefully"])
async def test_cluster_stop_cancels_server_still_installing(stop_method: str) -> None:
    """Regression test for stopping a cluster while add_server() is still installing a server."""
    cluster, host_registry, created_servers = make_cluster()
    add_task, server = await start_adding_server(cluster, created_servers)

    await getattr(cluster, stop_method)()
    assert not cluster.starting

    server.finish_install.set()
    with pytest.raises(RuntimeError, match="stopped while it was being added"):
        await add_task

    assert not server.started
    assert not cluster.running
    assert not cluster.starting
    assert cluster.stopped[server.server_id] is server
    assert not cluster.leased_ips
    assert host_registry.released_hosts == [Host(server.ip_addr)]


@pytest.mark.asyncio
async def test_server_stop_cancels_server_still_installing() -> None:
    """Regression test for stopping a specific starting server before its process exists."""
    cluster, host_registry, created_servers = make_cluster()
    add_task, server = await start_adding_server(cluster, created_servers)

    await cluster.server_stop(server.server_id, gracefully=False)
    assert not cluster.starting
    assert cluster.stopped[server.server_id] is server

    server.finish_install.set()
    with pytest.raises(RuntimeError, match="stopped while it was being added"):
        await add_task

    assert not server.started
    assert not cluster.running
    assert not cluster.starting
    assert cluster.stopped[server.server_id] is server
    assert not cluster.leased_ips
    assert host_registry.released_hosts == [Host(server.ip_addr)]
