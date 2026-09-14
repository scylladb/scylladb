# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""
A regression test for the per-protocol listener options in scylla.yaml.

Scylla has always been told what client-facing sockets to open with a set of
per-protocol options - native_transport_port, native_transport_port_ssl,
native_shard_aware_transport_port(_ssl), their proxy-protocol siblings,
alternator_port, alternator_https_port and friends - whose combined meaning is
anything but obvious: whether a port exists at all, and whether it is encrypted,
depends on which of its neighbours were set, and on whether they were set to the
same value. Deployments in the field depend on every one of those rules, so this
file pins them down: it sets those options and asserts which sockets the
node ends up with, and which encryption each of them speaks.

IMPORTANT - do not migrate this file to the "listeners" option. It exists
precisely to keep testing the per-protocol options that "listeners" supersedes; it
must keep passing unchanged both before and after the introduction of
"listeners", and it must therefore never mention that option. Tests for the
"listeners" option live in test_listeners.py.

The one test here that is NOT a regression test is
test_shard_aware_port_advertisement, which is marked as such: what a listener
advertises in its SUPPORTED reply is the one thing the "listeners" work did
change. See its docstring.
"""

from __future__ import annotations

import logging
from typing import Any, NamedTuple, TYPE_CHECKING

from test.cluster.listener_utils import (
    Endpoint,
    TLS_OPTIONS,
    alternator_list_tables,
    assert_not_listening,
    assert_request_refused,
    cql_options,
    wait_for_listening,
)

if TYPE_CHECKING:
    from test.pylib.internal_types import ServerInfo
    from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

# The ports the legacy options default to. They are part of the contract: a node
# told nothing at all must open exactly these, and no others.
DEFAULT_CQL_PORT = 9042
DEFAULT_CQL_SSL_PORT = 9142
DEFAULT_SHARD_AWARE_PORT = 19042
DEFAULT_SHARD_AWARE_SSL_PORT = 19142
# Alternator is off unless a port is named; these are the ports its
# documentation suggests, and nothing may listen on them by default.
CUSTOMARY_ALTERNATOR_PORT = 8000
CUSTOMARY_ALTERNATOR_HTTPS_PORT = 8043

# Ports the tests name explicitly - deliberately unusual ones, so that nothing
# can be seen to work by accident on a well-known port. They are outside the
# ephemeral port range, so that they cannot clash with an outgoing connection.
CQL_PORT = 23101
SHARD_AWARE_PORT = 23102
CQL_SSL_PORT = 23103
SHARD_AWARE_SSL_PORT = 23104
CQL_MERGED_PORT = 23105
SHARD_AWARE_MERGED_PORT = 23106
CQL_SSL_ONLY_PORT = 23107
SHARD_AWARE_SSL_ONLY_PORT = 23108
CQL_PROXY_PORT = 23109
SHARD_AWARE_PROXY_PORT = 23110
CQL_SSL_PROXY_PORT = 23111
SHARD_AWARE_SSL_PROXY_PORT = 23112
ALTERNATOR_PORT = 23113
ALTERNATOR_HTTPS_PORT = 23114
ALTERNATOR_PROXY_PORT = 23115
ALTERNATOR_HTTPS_PROXY_PORT = 23116

# client_encryption_options with encryption actually turned on: "enabled" is
# what makes the legacy configuration open encrypted CQL ports at all.
CLIENT_ENCRYPTION_ON = {'enabled': True} | TLS_OPTIONS

ALTERNATOR_OPTIONS = {
    'alternator_encryption_options': TLS_OPTIONS,
    'alternator_write_isolation': 'only_rmw_uses_lwt',
    'alternator_enforce_authorization': False,
    'alternator_warn_authorization': False,
}

# Every CQL port option at once, each named separately, with encryption on. The
# default ports are kept for the plain listeners so that the harness's own
# driver can still connect.
ALL_CQL_PORTS = {
    'client_encryption_options': CLIENT_ENCRYPTION_ON,
    'native_transport_port': DEFAULT_CQL_PORT,
    'native_shard_aware_transport_port': DEFAULT_SHARD_AWARE_PORT,
    'native_transport_port_ssl': CQL_SSL_PORT,
    'native_shard_aware_transport_port_ssl': SHARD_AWARE_SSL_PORT,
    'native_transport_port_proxy_protocol': CQL_PROXY_PORT,
    'native_shard_aware_transport_port_proxy_protocol': SHARD_AWARE_PROXY_PORT,
    'native_transport_port_ssl_proxy_protocol': CQL_SSL_PROXY_PORT,
    'native_shard_aware_transport_port_ssl_proxy_protocol': SHARD_AWARE_SSL_PROXY_PORT,
}


class Cql(NamedTuple):
    """A CQL listener the configuration is expected to produce."""
    port: int
    tls: bool = False
    proxy_protocol: bool = False


class Alternator(NamedTuple):
    """An Alternator listener the configuration is expected to produce."""
    port: int
    tls: bool = False
    proxy_protocol: bool = False


async def check_cql(address: str, expected: Cql) -> None:
    """The listener is there and speaks CQL over the encryption it was
    configured with - and only over that one.
    """
    endpoint = Endpoint(address, expected.port, expected.tls, expected.proxy_protocol)
    logger.info("Checking the CQL listener %s", endpoint)
    assert 'CQL_VERSION' in await cql_options(endpoint)
    # An encrypted listener refuses a plaintext client, and a plaintext listener
    # refuses a TLS handshake.
    await assert_request_refused(cql_options(endpoint._replace(tls=not expected.tls)))


async def check_alternator(address: str, expected: Alternator) -> None:
    """The listener is there, serves the DynamoDB API over the encryption it was
    configured with, and refuses the other one.
    """
    endpoint = Endpoint(address, expected.port, expected.tls, expected.proxy_protocol)
    logger.info("Checking the Alternator listener %s", endpoint)
    assert 'TableNames' in await alternator_list_tables(endpoint)
    await assert_request_refused(alternator_list_tables(endpoint._replace(tls=not expected.tls)))


async def check_listeners(address: str, expected: list[Cql | Alternator],
                          absent: list[int]) -> None:
    for listener in expected:
        if isinstance(listener, Cql):
            await check_cql(address, listener)
        else:
            await check_alternator(address, listener)
    for port in absent:
        logger.info("Checking that nothing listens on %s:%d", address, port)
        await assert_not_listening(Endpoint(address, port))


async def start_node_without_plain_cql(manager: ScyllaClusterManager,
                                       config: dict[str, Any], ready_port: int) -> ServerInfo:
    """A node whose configuration leaves no plain CQL port to connect to.

    Several of the legacy rules do exactly that, and the harness's own driver
    connects in plain, so such a node has to be started without it and waited
    for by hand.
    """
    server = await manager.server_add(config=config, connect_driver=False)
    await wait_for_listening(Endpoint(server.ip_addr, ready_port))
    return server


async def test_default_ports(manager: ScyllaClusterManager) -> None:
    """Told nothing, a node opens the plain CQL port and the plain shard-aware
    port, and nothing else - no TLS ports, no Alternator.
    """
    server = await manager.server_add(config={})

    # The harness's driver came in through the default port.
    cql = manager.get_cql()
    await cql.run_async("SELECT key FROM system.local WHERE key = 'local'")

    await check_listeners(server.ip_addr, [
        Cql(DEFAULT_CQL_PORT),
        Cql(DEFAULT_SHARD_AWARE_PORT),
    ], absent=[
        # Encryption is off, so the SSL ports do not exist even though the
        # options that name them have non-zero defaults.
        DEFAULT_CQL_SSL_PORT,
        DEFAULT_SHARD_AWARE_SSL_PORT,
        # Alternator is off until a port is named for it.
        CUSTOMARY_ALTERNATOR_PORT,
        CUSTOMARY_ALTERNATOR_HTTPS_PORT,
    ])


async def test_plain_ports_move(manager: ScyllaClusterManager) -> None:
    """native_transport_port and native_shard_aware_transport_port move the
    plain listeners, and the defaults are left behind.
    """
    server = await start_node_without_plain_cql(manager, {
        'native_transport_port': CQL_PORT,
        'native_shard_aware_transport_port': SHARD_AWARE_PORT,
    }, ready_port=CQL_PORT)

    await check_listeners(server.ip_addr, [
        Cql(CQL_PORT),
        Cql(SHARD_AWARE_PORT),
    ], absent=[
        DEFAULT_CQL_PORT,
        DEFAULT_SHARD_AWARE_PORT,
        DEFAULT_CQL_SSL_PORT,
        DEFAULT_SHARD_AWARE_SSL_PORT,
    ])


async def test_encryption_with_separate_ssl_ports(manager: ScyllaClusterManager) -> None:
    """With encryption on and the SSL ports named separately from the plain
    ones, a node serves both: the plain ports stay plain, and the SSL ports are
    added beside them.
    """
    server = await manager.server_add(config={
        'client_encryption_options': CLIENT_ENCRYPTION_ON,
        'native_transport_port': DEFAULT_CQL_PORT,
        'native_shard_aware_transport_port': DEFAULT_SHARD_AWARE_PORT,
        'native_transport_port_ssl': CQL_SSL_PORT,
        'native_shard_aware_transport_port_ssl': SHARD_AWARE_SSL_PORT,
    })

    await check_listeners(server.ip_addr, [
        Cql(DEFAULT_CQL_PORT),
        Cql(DEFAULT_SHARD_AWARE_PORT),
        Cql(CQL_SSL_PORT, tls=True),
        Cql(SHARD_AWARE_SSL_PORT, tls=True),
    ], absent=[
        DEFAULT_CQL_SSL_PORT,
        DEFAULT_SHARD_AWARE_SSL_PORT,
    ])


async def test_encryption_without_ssl_ports_encrypts_the_plain_ports(
        manager: ScyllaClusterManager) -> None:
    """Turning encryption on without naming the SSL ports does not add ports -
    it encrypts the ones that are there. This is the upgrade surprise the option
    documentation warns about, and it must keep behaving exactly this way.
    """
    server = await start_node_without_plain_cql(manager, {
        'client_encryption_options': CLIENT_ENCRYPTION_ON,
    }, ready_port=DEFAULT_CQL_PORT)

    await check_listeners(server.ip_addr, [
        # The default ports, now encrypted, and plaintext refused on them.
        Cql(DEFAULT_CQL_PORT, tls=True),
        Cql(DEFAULT_SHARD_AWARE_PORT, tls=True),
    ], absent=[
        # The SSL options' defaults are not ports the node ever opens on its own.
        DEFAULT_CQL_SSL_PORT,
        DEFAULT_SHARD_AWARE_SSL_PORT,
    ])


async def test_encryption_with_ssl_port_equal_to_plain_port(
        manager: ScyllaClusterManager) -> None:
    """Naming the same port as both the plain and the SSL port encrypts that one
    port, rather than trying to open it twice.
    """
    server = await start_node_without_plain_cql(manager, {
        'client_encryption_options': CLIENT_ENCRYPTION_ON,
        'native_transport_port': CQL_MERGED_PORT,
        'native_transport_port_ssl': CQL_MERGED_PORT,
        'native_shard_aware_transport_port': SHARD_AWARE_MERGED_PORT,
        'native_shard_aware_transport_port_ssl': SHARD_AWARE_MERGED_PORT,
    }, ready_port=CQL_MERGED_PORT)

    await check_listeners(server.ip_addr, [
        Cql(CQL_MERGED_PORT, tls=True),
        Cql(SHARD_AWARE_MERGED_PORT, tls=True),
    ], absent=[
        DEFAULT_CQL_PORT,
        DEFAULT_SHARD_AWARE_PORT,
        DEFAULT_CQL_SSL_PORT,
        DEFAULT_SHARD_AWARE_SSL_PORT,
    ])


async def test_ssl_ports_only(manager: ScyllaClusterManager) -> None:
    """Naming only the SSL ports, with encryption on, leaves the plain ports out
    altogether - the node is reachable over TLS and nothing else.
    """
    server = await start_node_without_plain_cql(manager, {
        'client_encryption_options': CLIENT_ENCRYPTION_ON,
        'native_transport_port_ssl': CQL_SSL_ONLY_PORT,
        'native_shard_aware_transport_port_ssl': SHARD_AWARE_SSL_ONLY_PORT,
    }, ready_port=CQL_SSL_ONLY_PORT)

    await check_listeners(server.ip_addr, [
        Cql(CQL_SSL_ONLY_PORT, tls=True),
        Cql(SHARD_AWARE_SSL_ONLY_PORT, tls=True),
    ], absent=[
        DEFAULT_CQL_PORT,
        DEFAULT_SHARD_AWARE_PORT,
        DEFAULT_CQL_SSL_PORT,
        DEFAULT_SHARD_AWARE_SSL_PORT,
    ])


async def test_proxy_protocol_ports(manager: ScyllaClusterManager) -> None:
    """The proxy-protocol ports are listeners of their own, encrypted or not
    according to which option named them, and they live beside the ordinary
    ports rather than replacing them.
    """
    server = await manager.server_add(config=ALL_CQL_PORTS)

    await check_listeners(server.ip_addr, [
        Cql(DEFAULT_CQL_PORT),
        Cql(DEFAULT_SHARD_AWARE_PORT),
        Cql(CQL_SSL_PORT, tls=True),
        Cql(SHARD_AWARE_SSL_PORT, tls=True),
        Cql(CQL_PROXY_PORT, proxy_protocol=True),
        Cql(SHARD_AWARE_PROXY_PORT, proxy_protocol=True),
        Cql(CQL_SSL_PROXY_PORT, tls=True, proxy_protocol=True),
        Cql(SHARD_AWARE_SSL_PROXY_PORT, tls=True, proxy_protocol=True),
    ], absent=[
        DEFAULT_CQL_SSL_PORT,
        DEFAULT_SHARD_AWARE_SSL_PORT,
    ])


async def test_ssl_proxy_protocol_ports_need_encryption(manager: ScyllaClusterManager) -> None:
    """The encrypted proxy-protocol ports depend on client encryption being on:
    named while it is off, they are silently not opened. The plain
    proxy-protocol port is opened all the same.
    """
    server = await manager.server_add(config={
        'native_transport_port_proxy_protocol': CQL_PROXY_PORT,
        'native_transport_port_ssl_proxy_protocol': CQL_SSL_PROXY_PORT,
        'native_shard_aware_transport_port_ssl_proxy_protocol': SHARD_AWARE_SSL_PROXY_PORT,
    })

    await check_listeners(server.ip_addr, [
        Cql(DEFAULT_CQL_PORT),
        Cql(CQL_PROXY_PORT, proxy_protocol=True),
    ], absent=[
        CQL_SSL_PROXY_PORT,
        SHARD_AWARE_SSL_PROXY_PORT,
        DEFAULT_CQL_SSL_PORT,
        DEFAULT_SHARD_AWARE_SSL_PORT,
    ])


async def test_alternator_ports(manager: ScyllaClusterManager) -> None:
    """Each of the four Alternator port options opens a listener of its own,
    with the encryption and the proxy protocol its name implies.
    """
    server = await manager.server_add(config=ALTERNATOR_OPTIONS | {
        'alternator_port': ALTERNATOR_PORT,
        'alternator_https_port': ALTERNATOR_HTTPS_PORT,
        'alternator_port_proxy_protocol': ALTERNATOR_PROXY_PORT,
        'alternator_https_port_proxy_protocol': ALTERNATOR_HTTPS_PROXY_PORT,
    })

    await check_listeners(server.ip_addr, [
        Alternator(ALTERNATOR_PORT),
        Alternator(ALTERNATOR_HTTPS_PORT, tls=True),
        Alternator(ALTERNATOR_PROXY_PORT, proxy_protocol=True),
        Alternator(ALTERNATOR_HTTPS_PROXY_PORT, tls=True, proxy_protocol=True),
        # Alternator does not disturb the CQL listeners.
        Cql(DEFAULT_CQL_PORT),
    ], absent=[
        CUSTOMARY_ALTERNATOR_PORT,
        CUSTOMARY_ALTERNATOR_HTTPS_PORT,
    ])


async def test_alternator_https_port_alone(manager: ScyllaClusterManager) -> None:
    """Naming only the HTTPS port serves Alternator over TLS and not in plain -
    unlike CQL, the Alternator ports are independent of one another, and an
    unnamed one simply isn't there.
    """
    server = await manager.server_add(config=ALTERNATOR_OPTIONS | {
        'alternator_https_port': ALTERNATOR_HTTPS_PORT,
    })

    await check_listeners(server.ip_addr, [
        Alternator(ALTERNATOR_HTTPS_PORT, tls=True),
    ], absent=[
        ALTERNATOR_PORT,
        ALTERNATOR_PROXY_PORT,
        ALTERNATOR_HTTPS_PROXY_PORT,
        CUSTOMARY_ALTERNATOR_PORT,
    ])


async def test_shard_aware_port_advertisement(manager: ScyllaClusterManager) -> None:
    """Each listener hands its clients over to the shard-aware listener it can
    actually reach - the one that agrees with it on both encryption and the
    proxy protocol - and says so under the SUPPORTED key matching its own
    encryption.

    NOT A REGRESSION TEST, and the only such test in this file. Before the
    "listeners" option, SCYLLA_SHARD_AWARE_PORT and SCYLLA_SHARD_AWARE_PORT_SSL
    were a property of the node rather than of the listener: both were taken
    straight from native_shard_aware_transport_port(_ssl), advertised only if
    the option had been set explicitly, and advertised identically by every
    listener - the shard-aware ones, and the proxy-protocol ones which no plain
    shard-aware port could serve. So this test would have failed before the
    patch, on purpose: it pins down the behaviour that replaced that.
    """
    server = await manager.server_add(config=ALL_CQL_PORTS)

    async def advertised(endpoint: Endpoint) -> dict[str, list[str]]:
        supported = await cql_options(endpoint)
        return {key: value for key, value in supported.items()
                if key in ('SCYLLA_SHARD_AWARE_PORT', 'SCYLLA_SHARD_AWARE_PORT_SSL')}

    address = server.ip_addr

    assert await advertised(Endpoint(address, DEFAULT_CQL_PORT)) == \
            {'SCYLLA_SHARD_AWARE_PORT': [str(DEFAULT_SHARD_AWARE_PORT)]}
    assert await advertised(Endpoint(address, CQL_SSL_PORT, tls=True)) == \
            {'SCYLLA_SHARD_AWARE_PORT_SSL': [str(SHARD_AWARE_SSL_PORT)]}
    assert await advertised(Endpoint(address, CQL_PROXY_PORT, proxy_protocol=True)) == \
            {'SCYLLA_SHARD_AWARE_PORT': [str(SHARD_AWARE_PROXY_PORT)]}
    assert await advertised(Endpoint(address, CQL_SSL_PROXY_PORT, tls=True, proxy_protocol=True)) == \
            {'SCYLLA_SHARD_AWARE_PORT_SSL': [str(SHARD_AWARE_SSL_PROXY_PORT)]}

    # A shard-aware listener has nobody to hand its clients over to.
    for endpoint in (Endpoint(address, DEFAULT_SHARD_AWARE_PORT),
                     Endpoint(address, SHARD_AWARE_SSL_PORT, tls=True),
                     Endpoint(address, SHARD_AWARE_PROXY_PORT, proxy_protocol=True),
                     Endpoint(address, SHARD_AWARE_SSL_PROXY_PORT, tls=True, proxy_protocol=True)):
        assert await advertised(endpoint) == {}


async def test_shard_aware_port_is_not_advertised_when_there_is_none(
        manager: ScyllaClusterManager) -> None:
    """A listener with no shard-aware sibling of its own kind advertises no
    shard-aware port at all.
    """
    server = await manager.server_add(config={
        'native_transport_port_proxy_protocol': CQL_PROXY_PORT,
    })

    supported = await cql_options(Endpoint(server.ip_addr, CQL_PROXY_PORT, proxy_protocol=True))
    assert 'SCYLLA_SHARD_AWARE_PORT' not in supported
    assert 'SCYLLA_SHARD_AWARE_PORT_SSL' not in supported
