# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""
Tests for the "listeners" configuration option, which spells out the
client-facing sockets to listen on, and overrides the per-protocol options
(native_transport_port, alternator_port and friends).

The listeners are spread over two addresses leased from the harness, so that
several CQL - and several Alternator - listeners serve side by side, on
addresses and ports no other test is using.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, NamedTuple, TYPE_CHECKING

import pytest

from test.cluster.listener_utils import (
    connect,
    cql_options_on,
    Endpoint,
    TLS_OPTIONS,
    alternator_list_tables,
    assert_not_listening,
    assert_request_refused,
    cql_options,
)
from test.cluster.util import reconnect_driver
from test.pylib.rest_client import read_barrier
from test.pylib.util import wait_for
from test.pylib.host_registry import HostRegistry

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from test.pylib.internal_types import ServerInfo
    from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

# The CQL port on the server's own address, which the test harness connects to.
# It is the one listener the test doesn't get to choose.
DEFAULT_CQL_PORT = 9042

# Ports for the listeners the test does get to choose - deliberately unusual
# ones, to show that nothing assumes the well-known ports. They are outside the
# ephemeral port range, so that they cannot clash with an outgoing connection of
# ours that happens to be bound to a leased address.
CQL_PORT = 23011
CQL_SHARD_AWARE_PORT = 24127
CQL_TLS_PORT = 25439
CQL_SHARD_AWARE_TLS_PORT = 26591
CQL_PROXY_PORT = 27653
ALTERNATOR_PORT = 28711
ALTERNATOR_TLS_PROXY_PORT = 29819
# Configured through a per-protocol option, which the "listeners" option
# overrides - nothing may listen on it.
IGNORED_LEGACY_PORT = 31063


class Deployment(NamedTuple):
    server: ServerInfo
    # The "listeners" option the server was configured with, and where each of
    # its listeners can be reached.
    listeners: dict[str, dict[str, Any]]
    endpoints: dict[str, Endpoint]


def make_listeners(first: str, second: str) -> dict[str, dict[str, Any]]:
    """The listeners to configure, spread over the two given addresses.

    Both protocols are served on both addresses, so every listener has a
    same-protocol sibling listening elsewhere at the same time.
    """
    return {
        # The harness's own connection, on the server's own address.
        'cql_default': {'protocol': 'cql', 'port': DEFAULT_CQL_PORT},

        'cql': {'protocol': 'cql', 'address': first, 'port': CQL_PORT,
                'shard_aware_listener': 'cql_shard_aware'},
        'cql_shard_aware': {'protocol': 'cql', 'address': first, 'port': CQL_SHARD_AWARE_PORT,
                            'shard_aware': True},
        'cql_proxy_protocol': {'protocol': 'cql', 'address': first, 'port': CQL_PROXY_PORT,
                               'proxy_protocol': True, 'keepalive': True},
        # TLS options spelled out in the listener itself...
        'cql_ssl': {'protocol': 'cql', 'address': second, 'port': CQL_TLS_PORT, 'tls': TLS_OPTIONS,
                    'shard_aware_listener': 'cql_shard_aware_ssl'},
        # ... or taken from client_encryption_options, by asking for plain "tls: true".
        'cql_shard_aware_ssl': {'protocol': 'cql', 'address': second, 'port': CQL_SHARD_AWARE_TLS_PORT,
                                'shard_aware': True, 'tls': True},

        'alternator': {'protocol': 'alternator', 'address': first, 'port': ALTERNATOR_PORT},
        # TLS options taken from alternator_encryption_options.
        'alternator_https_proxy_protocol': {'protocol': 'alternator', 'address': second,
                                            'port': ALTERNATOR_TLS_PROXY_PORT,
                                            'tls': True, 'proxy_protocol': True},
    }


def as_cluster_config_value(listeners: dict[str, dict[str, Any]]) -> str:
    """The listeners as the "listeners" cluster config option takes them: a YAML
    mapping in flow style, so that the whole set fits on one line.

    Written out here rather than with a YAML library so that the test states the
    exact text it sends, and so that a change in how Scylla canonicalizes a value
    shows up as a test failure rather than being papered over by a re-serialization
    that happens to agree.
    """
    def scalar(value: Any) -> str:
        if isinstance(value, bool):
            return 'true' if value else 'false'
        if isinstance(value, dict):
            return '{' + ', '.join(f'{k}: {scalar(v)}' for k, v in value.items()) + '}'
        return str(value)

    return '{' + ', '.join(f'{name}: {scalar(listener)}' for name, listener in listeners.items()) + '}'


def as_cql_literal(listeners: dict[str, dict[str, Any]]) -> str:
    """The listeners as a CQL literal, the other way the option can be given them:
    names unquoted, string values quoted, numbers and booleans bare.

    Used to give a node its listeners in the first place, while changing them later
    goes through as_cluster_config_value(), so that both spellings are exercised.
    """
    def value(v: Any) -> str:
        if isinstance(v, bool):
            return 'true' if v else 'false'
        if isinstance(v, int):
            return str(v)
        if isinstance(v, dict):
            return '{' + ', '.join(f'{k}: {value(x)}' for k, x in v.items()) + '}'
        return "'" + str(v).replace("'", "''") + "'"

    return value(listeners)


async def set_listeners(manager: ScyllaClusterManager, server: ServerInfo,
                        listeners: dict[str, dict[str, Any]]) -> None:
    """Give the node its listeners, and restart it so that it serves them.

    The option is read when a node starts, so a node that is already running keeps
    the listeners it started with until it is restarted.
    """
    host_id = await manager.get_host_id(server.server_id)
    cql = manager.get_cql()
    await cql.run_async(
            f"ALTER NODE {host_id} WITH listeners = {as_cql_literal(listeners)}")
    # Set now rather than in the node's initial configuration: until the listeners are in
    # place there is nothing to override it, and the node would come up on a port the
    # harness does not know about. From the restart on it is ignored, which is what
    # test_per_protocol_options_are_ignored checks.
    await manager.server_update_config(server.server_id, 'native_transport_port', IGNORED_LEGACY_PORT)
    await manager.server_restart(server.server_id)
    # A session that was connected before the restart keeps reconnecting to the node for a
    # while afterwards, so queries through it fail in ways that have nothing to do with the
    # listeners (scylladb/python-driver#295).
    await reconnect_driver(manager)


def make_config() -> dict[str, Any]:
    return {
        'client_encryption_options': TLS_OPTIONS,
        'alternator_encryption_options': TLS_OPTIONS,
        'alternator_write_isolation': 'only_rmw_uses_lwt',
        'alternator_enforce_authorization': False,
        'alternator_warn_authorization': False,
    }


def endpoints_of(listeners: dict[str, dict[str, Any]], default_address: str) -> dict[str, Endpoint]:
    return {name: Endpoint(address=listener.get('address', default_address),
                           port=listener['port'],
                           tls=bool(listener.get('tls', False)),
                           proxy_protocol=listener.get('proxy_protocol', False))
            for name, listener in listeners.items()}


@pytest.fixture(scope="function")
async def leased_addresses() -> AsyncIterator[tuple[str, str]]:
    """Two addresses of our own, so that the listeners below cannot collide with
    a server - or a listener - of a concurrently running test.
    """
    registry = HostRegistry()
    addresses = [await registry.lease_host() for _ in range(2)]
    try:
        yield tuple(addresses)
    finally:
        for address in addresses:
            await registry.release_host(address)


@pytest.fixture(scope="function")
async def deployment(manager: ScyllaClusterManager,
                     leased_addresses: tuple[str, str]) -> Deployment:
    listeners = make_listeners(*leased_addresses)
    # The node comes up on the per-protocol options, which is how the harness reaches
    # it to set the option in the first place; the restart inside set_listeners() is
    # what puts the configured listeners in place.
    server = await manager.server_add(config=make_config())
    await set_listeners(manager, server, listeners)
    return Deployment(server=server, listeners=listeners,
                      endpoints=endpoints_of(listeners, server.ip_addr))


async def test_cql_listeners(manager: ScyllaClusterManager, deployment: Deployment) -> None:
    """Every configured CQL listener serves CQL, on the address and port it was
    given - several of them, on two addresses, at the same time.
    """
    # The default listener is the one the test harness itself connected through.
    cql = manager.get_cql()
    await cql.run_async("SELECT key FROM system.local WHERE key = 'local'")

    for name, endpoint in deployment.endpoints.items():
        if deployment.listeners[name]['protocol'] != 'cql':
            continue
        logger.info("Checking the CQL listener %s on %s", name, endpoint)
        assert 'CQL_VERSION' in await cql_options(endpoint)


async def test_alternator_listeners(deployment: Deployment) -> None:
    """Every configured Alternator listener serves the DynamoDB API, on the
    address and port it was given.
    """
    for name, endpoint in deployment.endpoints.items():
        if deployment.listeners[name]['protocol'] != 'alternator':
            continue
        logger.info("Checking the Alternator listener %s on %s", name, endpoint)
        assert 'TableNames' in await alternator_list_tables(endpoint)


async def test_cql_shard_aware_listener_is_advertised(deployment: Deployment) -> None:
    """A listener advertises the shard-aware listener it names, and only that
    one, under the key matching that listener's own encryption.
    """
    endpoints = deployment.endpoints

    supported = await cql_options(endpoints['cql'])
    assert supported['SCYLLA_SHARD_AWARE_PORT'] == [str(CQL_SHARD_AWARE_PORT)]
    assert 'SCYLLA_SHARD_AWARE_PORT_SSL' not in supported

    supported = await cql_options(endpoints['cql_ssl'])
    assert supported['SCYLLA_SHARD_AWARE_PORT_SSL'] == [str(CQL_SHARD_AWARE_TLS_PORT)]
    assert 'SCYLLA_SHARD_AWARE_PORT' not in supported

    # A listener that names no shard-aware listener offers no shard-aware port.
    for name in ('cql_default', 'cql_proxy_protocol'):
        supported = await cql_options(endpoints[name])
        assert 'SCYLLA_SHARD_AWARE_PORT' not in supported
        assert 'SCYLLA_SHARD_AWARE_PORT_SSL' not in supported


async def test_cql_listener_without_proxy_protocol(deployment: Deployment) -> None:
    """A listener that wasn't asked to expect a PROXY protocol header doesn't
    accept one - the header is taken for a (malformed) CQL frame.
    """
    with pytest.raises((AssertionError, asyncio.IncompleteReadError, asyncio.TimeoutError, OSError)):
        await asyncio.wait_for(cql_options(deployment.endpoints['cql'], proxy_protocol=True),
                               timeout=30)


async def test_per_protocol_options_are_ignored(deployment: Deployment) -> None:
    """With "listeners" set, the per-protocol options are not honored."""
    await assert_not_listening(Endpoint(deployment.server.ip_addr, IGNORED_LEGACY_PORT))


async def test_listeners_are_reconfigurable(manager: ScyllaClusterManager,
                                            deployment: Deployment,
                                            leased_addresses: tuple[str, str]) -> None:
    """A listener follows the address and port it is reconfigured with, and
    leaves the ones it had behind.

    Reconfiguring means setting the option again: the value stored for this node
    replaces the one before it wholesale, so a listener left out of the new value
    is gone.
    """
    first, second = leased_addresses
    moved = Endpoint(second, IGNORED_LEGACY_PORT)

    listeners = dict(deployment.listeners)
    listeners['cql'] = {'protocol': 'cql', 'address': moved.address, 'port': moved.port}
    # The listener it used to hand its clients over to goes away with it.
    del listeners['cql_shard_aware']

    await set_listeners(manager, deployment.server, listeners)

    assert 'CQL_VERSION' in await cql_options(moved)
    await assert_not_listening(deployment.endpoints['cql'])
    await assert_not_listening(deployment.endpoints['cql_shard_aware'])
    # The listeners that weren't touched are still there.
    assert 'CQL_VERSION' in await cql_options(deployment.endpoints['cql_ssl'])
    assert 'TableNames' in await alternator_list_tables(deployment.endpoints['alternator'])


async def test_encrypted_listeners_refuse_plaintext(deployment: Deployment) -> None:
    """A listener configured with TLS speaks nothing but TLS: a client that
    skips the handshake and starts talking the protocol gets nowhere.
    """
    await assert_request_refused(cql_options(deployment.endpoints['cql_ssl'], tls=False))
    await assert_request_refused(cql_options(deployment.endpoints['cql_shard_aware_ssl'], tls=False))
    await assert_request_refused(
            alternator_list_tables(deployment.endpoints['alternator_https_proxy_protocol'], tls=False))


async def test_plaintext_listeners_refuse_tls(deployment: Deployment) -> None:
    """A listener configured without TLS doesn't accept a TLS handshake - the
    ClientHello is taken for the protocol's own bytes, and the handshake fails.
    """
    await assert_request_refused(cql_options(deployment.endpoints['cql'], tls=True))
    await assert_request_refused(cql_options(deployment.endpoints['cql_default'], tls=True))
    await assert_request_refused(alternator_list_tables(deployment.endpoints['alternator'], tls=True))

async def set_listeners_live(manager: ScyllaClusterManager, server: ServerInfo,
                             listeners: dict[str, dict[str, Any]]) -> None:
    """Change the listeners of a running node, without restarting it."""
    host_id = await manager.get_host_id(server.server_id)
    cql = manager.get_cql()
    await cql.run_async(
            f"ALTER NODE {host_id} WITH listeners = '{as_cluster_config_value(listeners)}'")
    await read_barrier(manager.api, server.ip_addr)


async def test_listeners_change_without_a_restart(manager: ScyllaClusterManager,
                                                  deployment: Deployment,
                                                  leased_addresses: tuple[str, str]) -> None:
    """A listener added to the option starts serving, and one taken out of it stops,
    on a node that keeps running throughout.
    """
    first, _ = leased_addresses
    added = Endpoint(first, IGNORED_LEGACY_PORT)

    listeners = dict(deployment.listeners)
    listeners['cql_added'] = {'protocol': 'cql', 'address': added.address, 'port': added.port}
    removed = deployment.endpoints['cql_proxy_protocol']
    del listeners['cql_proxy_protocol']

    await set_listeners_live(manager, deployment.server, listeners)

    async def added_is_serving() -> bool | None:
        try:
            return 'CQL_VERSION' in await cql_options(added) or None
        except Exception:
            return None

    await wait_for(added_is_serving, time.time() + 60)
    await assert_not_listening(removed)
    # The listeners nobody touched carried on serving.
    assert 'CQL_VERSION' in await cql_options(deployment.endpoints['cql'])
    assert 'CQL_VERSION' in await cql_options(deployment.endpoints['cql_ssl'])


async def test_a_listener_that_did_not_change_keeps_its_connections(
        manager: ScyllaClusterManager, deployment: Deployment,
        leased_addresses: tuple[str, str]) -> None:
    """Changing one listener leaves the connections of the others alone."""
    first, _ = leased_addresses
    kept = deployment.endpoints['cql']

    reader, writer = await connect(kept)
    try:
        listeners = dict(deployment.listeners)
        listeners['cql_added'] = {'protocol': 'cql', 'address': first, 'port': IGNORED_LEGACY_PORT}
        await set_listeners_live(manager, deployment.server, listeners)

        # The connection opened before the change still answers afterwards.
        assert 'CQL_VERSION' in await cql_options_on(reader, writer)
    finally:
        writer.close()
