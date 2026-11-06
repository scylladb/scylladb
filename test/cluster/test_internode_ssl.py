#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
Which messaging (internode) ports a node listens on, depending on
internode_encryption and the storage_port / ssl_storage_port options.

Ported from scylla-dtest internode_ssl_test.py (the putget dtests of that
file are ported in test_tls_putget.py).
"""

import asyncio
import logging
import socket
import time

import pytest

from test.cluster.util import stop_servers_gracefully
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for

logger = logging.getLogger(__name__)

DEFAULT_PORT = 7000
DEFAULT_SSL_PORT = 7001


@pytest.fixture(autouse=True)
async def stop_nodes_gracefully(manager: ScyllaClusterManager):
    """Like the dtests' teardown, stop the nodes gracefully at the end of
    each test."""
    yield
    await stop_servers_gracefully(manager)


def is_port_used(ip: str, port: int) -> bool:
    with socket.socket() as sock:
        return sock.connect_ex((ip, port)) == 0


async def verify_listen_ports(manager: ScyllaClusterManager, server, log, mark, expected_ports: list[int]) -> None:
    """The dtest's restart_and_verify_listen_ports() after the node start:
    the node logs that it listens on the expected ports and they are in
    use, the node does not crash, then it is killed."""
    logger.debug(f"Expected listen ports: {expected_ports}")
    pattern = "|".join(str(port) for port in expected_ports)
    res = await log.grep(f"Starting.*Messaging Service on.*port ({pattern})", from_mark=mark)
    assert len(res) == len(expected_ports), \
        f"The listened ports are not same as expected! Expected ports: {expected_ports}\nReal listened ports: {res}"

    for port in expected_ports:
        async def port_used():
            return True if is_port_used(server.ip_addr, port) else None
        await wait_for(port_used, time.time() + 2, period=0.5)

    # Wait a while and check if Aborting/Segfault occurred
    await asyncio.sleep(2)
    res = await log.grep("Aborting on shard |Segmentation fault on shard ", from_mark=mark)
    assert not len(res), str(res)
    await manager.server_stop(server.server_id, convict=False)


# Ported from scylla-dtest internode_ssl_test.py::TestInternodeSSL::test_listen_ports_conf.
# A single node is started with every combination of storage_port and
# ssl_storage_port (0, a non-default port, or removed from scylla.yaml) and
# listens on the ports the internode_encryption mode implies; it is killed
# after each start.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("internode_encryption", ["none", "all", "dc", "rack"])
async def test_internode_listen_ports(manager: ScyllaClusterManager, internode_encryption: str):
    def to_ports(port, default_values=None):
        if port is None:
            if default_values is None:
                return []
            else:
                return default_values
        if port == 0:
            return []
        return [port]

    def to_expected_ports(storage_port, ssl_storage_port):
        if internode_encryption == "none":
            return to_ports(storage_port, [DEFAULT_PORT])
        elif internode_encryption == "all":
            return to_ports(ssl_storage_port, [DEFAULT_SSL_PORT])
        else:
            return to_ports(storage_port, [DEFAULT_PORT]) + to_ports(ssl_storage_port, [DEFAULT_SSL_PORT])

    # As in the dtest, the cluster's internode encryption options reach the
    # node's scylla.yaml only with the first configuration change below: the
    # node first starts without server_encryption_options, with storage_port
    # 7000 as ccm sets it.
    server = await manager.server_add(config={"endpoint_snitch": "SimpleSnitch",
                                              "tablets_mode_for_new_keyspaces": "disabled",
                                              "storage_port": DEFAULT_PORT,
                                              "server_encryption_options": {}})
    log = await manager.server_open_log(server.server_id)
    # Test default configuration, ccm only sets storage_port: 7000
    await verify_listen_ports(manager, server, log, None, [DEFAULT_PORT])

    await manager.server_update_config(server.server_id, "server_encryption_options", {
        "internode_encryption": internode_encryption,
        "certificate": "conf/scylla.crt",
        "keyfile": "conf/scylla.key",
        "truststore": "conf/scyllacadb.pem",
    }, reload=False)
    # None removes the specified option from the configuration file
    # change the non-zero port numbers so they are different from the
    # default settings for the sanity sake.
    for storage_port in [0, 7003, None]:
        for ssl_storage_port in [0, 7004, None]:
            for key, port in [("storage_port", storage_port), ("ssl_storage_port", ssl_storage_port)]:
                if port is None:
                    await manager.server_remove_config_option(server.server_id, key, reload=False)
                else:
                    await manager.server_update_config(server.server_id, key, port, reload=False)
            mark = await log.mark()
            await manager.server_start(server.server_id)
            await verify_listen_ports(manager, server, log, mark, to_expected_ports(storage_port, ssl_storage_port))
