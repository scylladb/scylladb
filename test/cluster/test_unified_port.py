# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

import asyncio
from pathlib import Path
import socket
import ssl

import pytest

from test.cqlpy.test_unified_port import build_pp2_header, build_shard_selection_header, send_options_and_get_supported
from test.cqlpy import test_unified_port as protocol_tests
from test.pylib.scylla_cluster_manager import ScyllaClusterManager


UNIFIED_PORT = 29042
CERTIFICATE = Path(__file__).parents[1] / "pylib/resources/scylla.crt"
TLS_CONFIG = {
    "enabled": True,
    "certificate": "conf/scylla.crt",
    "keyfile": "conf/scylla.key",
}


def connect_unified(host, tls, proxy, shard):
    with socket.create_connection((host, UNIFIED_PORT), timeout=10) as sock:
        tcp_source_port = sock.getsockname()[1]
        if proxy:
            sock.sendall(build_pp2_header(src_addr="192.0.2.10", src_port=12345,
                                         dst_addr=host, dst_port=UNIFIED_PORT))
        if shard is not None:
            sock.sendall(build_shard_selection_header(shard, tls=tls))
        if tls:
            context = ssl.create_default_context(cafile=str(CERTIFICATE))
            # The test certificate identifies example.com. Verify both its chain and name.
            with context.wrap_socket(sock, server_hostname="example.com") as connection:
                supported = send_options_and_get_supported(connection)
        else:
            supported = send_options_and_get_supported(sock)
        return supported, tcp_source_port


@pytest.mark.parametrize("encryption", ["plain-only", "tls-only", "both"])
@pytest.mark.parametrize("routing", ["balanced", "source-port"])
@pytest.mark.parametrize("proxy_mode", ["disabled", "optional", "required"])
async def test_unified_port_policies(manager: ScyllaClusterManager, encryption, routing, proxy_mode):
    """Verify transport policy and routing independently, on two serving shards."""
    server = await manager.server_add(config={
        "native_transport_port": 9042,
        "native_transport_port_ssl": 9142,
        "native_shard_aware_transport_port": 19042,
        "native_shard_aware_transport_port_ssl": 19142,
        "client_encryption_options": TLS_CONFIG,
        "native_transport_unified_port": {
            "port": UNIFIED_PORT,
            "encryption": encryption,
            "shard_routing": {
                "default": routing,
                "ppv2": {
                    "mode": proxy_mode,
                    "trusted_proxies": {"interface_subnets": [], "addresses": ["*"]},
                },
            },
        },
    })
    for tls in (False, True):
        for proxy in (False, True):
            for shard in (None, 0, 1):
                allowed = ((encryption == "both" or tls == (encryption == "tls-only"))
                           and (proxy_mode != "required" or proxy)
                           and (proxy_mode != "disabled" or not proxy))
                if not allowed:
                    with pytest.raises((ConnectionError, ssl.SSLError)):
                        await asyncio.to_thread(connect_unified, server.ip_addr, tls, proxy, shard)
                    continue
                supported, source_port = await asyncio.to_thread(connect_unified, server.ip_addr, tls, proxy, shard)
                assert supported["SCYLLA_UNIFIED_PORT"] == [str(UNIFIED_PORT)]
                assert int(supported["SCYLLA_NR_SHARDS"][0]) == 2
                actual = int(supported["SCYLLA_SHARD"][0])
                if shard is not None:
                    assert actual == shard
                elif routing == "source-port":
                    assert actual == (12345 if proxy else source_port) % 2
                else:
                    assert actual in (0, 1)

    if routing == "balanced" and proxy_mode == "optional":
        if encryption == "both":
            # Exercise the standalone protocol checks against a known TLS/proxy setup.
            for check in (
                protocol_tests.test_malformed_shard_selection_header,
                protocol_tests.test_truncated_pp2_header,
                protocol_tests.test_invalid_pp2_signature,
                protocol_tests.test_pp2_unsupported_address_family,
                protocol_tests.test_all_shards_reachable,
                protocol_tests.test_legacy_tls,
                protocol_tests.test_shard_selection_with_tls,
                protocol_tests.test_pp2_plus_shard_selection_plus_tls,
                protocol_tests.test_driver_connection,
            ):
                await asyncio.to_thread(check, server.ip_addr, UNIFIED_PORT)
        elif encryption == "plain-only":
            await asyncio.to_thread(protocol_tests.test_tls_flag_rejected_by_plain_only_listener, server.ip_addr, UNIFIED_PORT)
