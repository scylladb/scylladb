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
import socket
import time
from typing import Any, NamedTuple, TYPE_CHECKING

import pytest
from cassandra import InvalidRequest
from cassandra.connection import UnixSocketEndPoint
from cassandra.policies import WhiteListRoundRobinPolicy

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
from test.pylib.driver_utils import safe_driver_shutdown
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
DEFAULT_CQL_SHARD_AWARE_PORT = 19042

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
CQL_SHARD_AWARE_PROXY_PORT = 30187
# Where a shard-aware listener is moved to, or a second one opened.
CQL_SHARD_AWARE_MOVED_PORT = 30517
# Held by the test itself, so that a listener asked to open there can't.
OCCUPIED_PORT = 30293
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


async def test_listeners_on_the_same_port_are_rejected(manager: ScyllaClusterManager,
                                                      deployment: Deployment,
                                                      leased_addresses: tuple[str, str]) -> None:
    """Two listeners on the same address and port could never both be opened, so
    setting the option to such a set fails right away - whatever the protocols.
    """
    first, _ = leased_addresses
    listeners = dict(deployment.listeners)
    listeners['alternator_clash'] = {'protocol': 'alternator', 'address': first, 'port': CQL_PORT}
    host_id = await manager.get_host_id(deployment.server.server_id)
    with pytest.raises(InvalidRequest, match="'alternator_clash' and 'cql' are both on"):
        await manager.get_cql().run_async(
                f"ALTER NODE {host_id} WITH listeners = {as_cql_literal(listeners)}")


async def test_an_unknown_tls_option_is_rejected(manager: ScyllaClusterManager,
                                                deployment: Deployment) -> None:
    """A misspelled TLS option is rejected rather than ignored: ignoring a misspelled
    require_client_auth would serve the listener without checking client certificates.
    """
    listeners = dict(deployment.listeners)
    listeners['cql_typo'] = {'protocol': 'cql', 'port': OCCUPIED_PORT,
                             'tls': {'certificate': '/etc/scylla/db.crt', 'keyfile': '/etc/scylla/db.key',
                                     'require_client_aut': True}}
    host_id = await manager.get_host_id(deployment.server.server_id)
    with pytest.raises(InvalidRequest, match="unknown tls option 'require_client_aut'"):
        await manager.get_cql().run_async(
                f"ALTER NODE {host_id} WITH listeners = {as_cql_literal(listeners)}")


async def test_enabled_is_rejected_in_tls_options(manager: ScyllaClusterManager,
                                                 deployment: Deployment) -> None:
    """A tls map spelled out for a listener replaces the protocol's default options,
    so a map holding only "enabled" would quietly drop them; it is rejected, and
    "tls: true" is the way to ask for the defaults.
    """
    listeners = dict(deployment.listeners)
    listeners['cql_enabled'] = {'protocol': 'cql', 'port': OCCUPIED_PORT, 'tls': {'enabled': True}}
    host_id = await manager.get_host_id(deployment.server.server_id)
    with pytest.raises(InvalidRequest, match="'enabled' has no meaning"):
        await manager.get_cql().run_async(
                f"ALTER NODE {host_id} WITH listeners = {as_cql_literal(listeners)}")


@pytest.mark.parametrize("sibling, error", [
    (None, "which doesn't exist"),
    ({'protocol': 'cql', 'port': OCCUPIED_PORT}, "which isn't a shard-aware CQL listener"),
    ({'protocol': 'alternator', 'port': OCCUPIED_PORT}, "which isn't a shard-aware CQL listener"),
])
async def test_a_shard_aware_listener_that_cannot_work_is_rejected(manager: ScyllaClusterManager,
                                                                   deployment: Deployment,
                                                                   sibling: dict[str, Any] | None,
                                                                   error: str) -> None:
    """A shard_aware_listener that names no listener, or one that isn't shard-aware
    CQL, is rejected when the option is set, rather than stored to fail the node's
    next start.
    """
    listeners = dict(deployment.listeners)
    if sibling is not None:
        listeners['sibling'] = sibling
    listeners['cql'] = {**listeners['cql'], 'shard_aware_listener': 'sibling'}
    host_id = await manager.get_host_id(deployment.server.server_id)
    with pytest.raises(InvalidRequest, match=error):
        await manager.get_cql().run_async(
                f"ALTER NODE {host_id} WITH listeners = {as_cql_literal(listeners)}")


async def test_an_empty_list_is_not_an_empty_set_of_listeners(manager: ScyllaClusterManager,
                                                             deployment: Deployment) -> None:
    """The option is a map of listeners. "{}" is an empty one, but "[]" is a list, and
    is rejected rather than taken for an empty map.
    """
    host_id = await manager.get_host_id(deployment.server.server_id)
    with pytest.raises(InvalidRequest, match="expected a map"):
        await manager.get_cql().run_async(f"ALTER NODE {host_id} WITH listeners = []")


async def test_listeners_survive_a_describe_round_trip(manager: ScyllaClusterManager) -> None:
    """DESCRIBE SCHEMA WITH INTERNALS gives the option back as a statement that sets
    it again: replaying the dump restores what was stored.
    """
    await manager.server_add(config=make_config())
    cql = manager.get_cql()
    # The listeners the node derives from its per-protocol options anyway, under the same
    # names, so that setting the option and clearing it again changes no socket and the
    # harness's driver stays connected throughout.
    listeners = {
        'cql': {'protocol': 'cql', 'port': DEFAULT_CQL_PORT, 'shard_aware_listener': 'cql_shard_aware'},
        'cql_shard_aware': {'protocol': 'cql', 'port': DEFAULT_CQL_SHARD_AWARE_PORT, 'shard_aware': True},
    }
    await cql.run_async(f"ALTER CLUSTER WITH listeners = {as_cql_literal(listeners)}")

    async def stored() -> str | None:
        rows = await cql.run_async("SELECT configs FROM system_schema.scylla_clusters")
        return dict(rows[0].configs or {}).get('listeners') if rows else None

    value = await stored()
    assert value
    dump = [row.create_statement for row in await cql.run_async("DESC SCHEMA WITH INTERNALS")
            if row.type == 'cluster_config' and row.name == 'cluster']
    statements = [s for s in dump[0].splitlines() if 'listeners' in s]
    assert len(statements) == 1, dump

    await cql.run_async("ALTER CLUSTER WITH listeners = null")
    assert await stored() is None
    await cql.run_async(statements[0])
    assert await stored() == value


async def test_plaintext_listeners_refuse_tls(deployment: Deployment) -> None:
    """A listener configured without TLS doesn't accept a TLS handshake - the
    ClientHello is taken for the protocol's own bytes, and the handshake fails.
    """
    await assert_request_refused(cql_options(deployment.endpoints['cql'], tls=True))
    await assert_request_refused(cql_options(deployment.endpoints['cql_default'], tls=True))
    await assert_request_refused(alternator_list_tables(deployment.endpoints['alternator'], tls=True))

async def set_listeners_live(manager: ScyllaClusterManager, server: ServerInfo,
                             listeners: dict[str, dict[str, Any]], cql=None) -> None:
    """Change the listeners of a running node, without restarting it.

    The change goes through the harness's own session unless another is given -
    as it has to be when the change leaves that session nothing to connect to.
    """
    host_id = await manager.get_host_id(server.server_id)
    cql = cql or manager.get_cql()
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


async def test_alternator_listeners_change_without_a_restart(manager: ScyllaClusterManager,
                                                             deployment: Deployment,
                                                             leased_addresses: tuple[str, str]) -> None:
    """The Alternator listeners follow the option too, on a running node."""
    first, _ = leased_addresses
    added = Endpoint(first, IGNORED_LEGACY_PORT)

    listeners = dict(deployment.listeners)
    listeners['alternator_added'] = {'protocol': 'alternator', 'address': added.address,
                                     'port': added.port}
    removed = deployment.endpoints['alternator']
    del listeners['alternator']

    await set_listeners_live(manager, deployment.server, listeners)

    async def added_is_serving() -> bool | None:
        try:
            return 'TableNames' in await alternator_list_tables(added) or None
        except Exception:
            return None

    await wait_for(added_is_serving, time.time() + 60)
    await assert_not_listening(removed)
    # The encrypted Alternator listener nobody touched carried on serving.
    assert 'TableNames' in await alternator_list_tables(
            deployment.endpoints['alternator_https_proxy_protocol'])


async def test_alternator_server_follows_its_first_and_last_listener(
        manager: ScyllaClusterManager, deployment: Deployment) -> None:
    """Taking every Alternator listener away stops the Alternator server, and
    giving it a listener again starts it, on a node that keeps running
    throughout. That is also what lets a node that came up with no Alternator
    listeners - the default - start serving Alternator once some are configured.
    """
    cql = manager.get_cql()
    alternator_names = [name for name, listener in deployment.listeners.items()
                        if listener['protocol'] == 'alternator']

    no_alternator = {name: listener for name, listener in deployment.listeners.items()
                     if listener['protocol'] != 'alternator'}
    await set_listeners_live(manager, deployment.server, no_alternator)
    await wait_until_served(cql, 'alternator', False)
    for name in alternator_names:
        await assert_not_listening(deployment.endpoints[name])

    # First with an unencrypted listener alone, so that the encrypted one comes to a
    # server that started without any.
    only_unencrypted = dict(no_alternator, alternator=deployment.listeners['alternator'])
    await set_listeners_live(manager, deployment.server, only_unencrypted)
    await wait_until_served(cql, 'alternator', True)
    assert 'TableNames' in await alternator_list_tables(deployment.endpoints['alternator'])

    await set_listeners_live(manager, deployment.server, deployment.listeners)
    encrypted = deployment.endpoints['alternator_https_proxy_protocol']

    async def encrypted_is_serving() -> bool | None:
        try:
            return 'TableNames' in await alternator_list_tables(encrypted) or None
        except Exception:
            return None

    await wait_for(encrypted_is_serving, time.time() + 60)
    for name in alternator_names:
        assert 'TableNames' in await alternator_list_tables(deployment.endpoints[name])


async def test_an_alternator_listener_that_cannot_be_opened_does_not_hold_back_the_others(
        manager: ScyllaClusterManager, deployment: Deployment,
        leased_addresses: tuple[str, str]) -> None:
    """Like test_a_cql_listener_that_cannot_be_opened_does_not_hold_back_the_others,
    for Alternator.
    """
    first, _ = leased_addresses
    added = Endpoint(first, IGNORED_LEGACY_PORT)
    removed = deployment.endpoints['alternator']
    log = await manager.server_open_log(deployment.server.server_id)
    mark = await log.mark()

    with OccupiedPort(first, OCCUPIED_PORT) as occupied:
        listeners = dict(deployment.listeners)
        listeners['alternator_blocked'] = {'protocol': 'alternator', 'address': occupied.address,
                                           'port': occupied.port}
        listeners['alternator_added'] = {'protocol': 'alternator', 'address': added.address,
                                         'port': added.port}
        del listeners['alternator']
        await set_listeners_live(manager, deployment.server, listeners)

        await log.wait_for(f"Failed to listen for Alternator clients on {occupied.address}:{occupied.port}",
                           from_mark=mark, timeout=60)
        await log.wait_for("Could not open the Alternator listeners", from_mark=mark, timeout=60)

        assert 'TableNames' in await alternator_list_tables(added)
        await assert_not_listening(removed)
        assert 'TableNames' in await alternator_list_tables(
                deployment.endpoints['alternator_https_proxy_protocol'])
        served = await served_addresses(manager.get_cql(), 'alternator')
        assert f"{added.address}:{added.port}" in served
        assert f"{occupied.address}:{occupied.port}" not in served


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
        # The node applies the change in the background; once the added listener
        # serves, the change is done.
        added = Endpoint(first, IGNORED_LEGACY_PORT)

        async def added_is_serving() -> bool | None:
            try:
                return 'CQL_VERSION' in await cql_options(added) or None
            except Exception:
                return None
        await wait_for(added_is_serving, time.time() + 60)

        # The connection opened before the change still answers afterwards.
        assert 'CQL_VERSION' in await cql_options_on(reader, writer)
    finally:
        writer.close()


def advertised_shard_aware_port(supported: dict[str, list[str]]) -> tuple[str, int] | None:
    """The shard-aware port a SUPPORTED message offers, and whether it is the
    encrypted one, as the key it comes under."""
    for key in ('SCYLLA_SHARD_AWARE_PORT', 'SCYLLA_SHARD_AWARE_PORT_SSL'):
        if key in supported:
            return key, int(supported[key][0])
    return None


def move_the_shard_aware_listener(listeners: dict[str, dict[str, Any]]) -> None:
    listeners['cql_shard_aware'] = {**listeners['cql_shard_aware'], 'port': CQL_SHARD_AWARE_MOVED_PORT}


def encrypt_the_shard_aware_listener(listeners: dict[str, dict[str, Any]]) -> None:
    listeners['cql_shard_aware'] = {**listeners['cql_shard_aware'], 'tls': True}


def name_another_shard_aware_listener(listeners: dict[str, dict[str, Any]]) -> None:
    listeners['cql_shard_aware_2'] = {**listeners['cql_shard_aware'], 'port': CQL_SHARD_AWARE_MOVED_PORT}
    listeners['cql'] = {**listeners['cql'], 'shard_aware_listener': 'cql_shard_aware_2'}


def name_no_shard_aware_listener(listeners: dict[str, dict[str, Any]]) -> None:
    listeners['cql'] = {k: v for k, v in listeners['cql'].items() if k != 'shard_aware_listener'}


@pytest.mark.parametrize("change, expected", [
    (move_the_shard_aware_listener, ('SCYLLA_SHARD_AWARE_PORT', CQL_SHARD_AWARE_MOVED_PORT)),
    (encrypt_the_shard_aware_listener, ('SCYLLA_SHARD_AWARE_PORT_SSL', CQL_SHARD_AWARE_PORT)),
    (name_another_shard_aware_listener, ('SCYLLA_SHARD_AWARE_PORT', CQL_SHARD_AWARE_MOVED_PORT)),
    (name_no_shard_aware_listener, None),
])
async def test_a_listener_keeps_its_connections_when_its_shard_aware_listener_changes(
        manager: ScyllaClusterManager, deployment: Deployment, change, expected) -> None:
    """A listener advertises its shard-aware listener's port, and whether it is
    encrypted, to its clients. When that changes - the shard-aware listener moves or
    becomes encrypted, or the listener names another one or none - the listener's own
    socket did not change, so it stays open and keeps its connections, and only what
    it advertises changes, to new connections and existing ones alike.
    """
    kept = deployment.endpoints['cql']
    assert advertised_shard_aware_port(await cql_options(kept)) == ('SCYLLA_SHARD_AWARE_PORT', CQL_SHARD_AWARE_PORT)

    reader, writer = await connect(kept)
    try:
        listeners = dict(deployment.listeners)
        change(listeners)
        await set_listeners_live(manager, deployment.server, listeners)

        async def advertised_on_new_connections() -> bool | None:
            try:
                return advertised_shard_aware_port(await cql_options(kept)) == expected or None
            except Exception:
                return None
        await wait_for(advertised_on_new_connections, time.time() + 60)

        # The connection opened before the change is still open, and is told the same.
        assert advertised_shard_aware_port(await cql_options_on(reader, writer)) == expected
    finally:
        writer.close()


async def served_addresses(cql, protocol_server: str) -> list[str]:
    """Where system.protocol_servers says a protocol server listens - nowhere,
    when it isn't running."""
    rows = await cql.run_async(
            f"SELECT listen_addresses FROM system.protocol_servers WHERE name = '{protocol_server}'")
    return list(rows[0].listen_addresses or []) if rows else []


async def wait_until_served(cql, protocol_server: str, served: bool) -> None:
    async def done() -> bool | None:
        return (bool(await served_addresses(cql, protocol_server)) == served) or None
    await wait_for(done, time.time() + 60)


async def test_shard_aware_listener_must_agree_on_proxy_protocol(
        manager: ScyllaClusterManager, deployment: Deployment,
        leased_addresses: tuple[str, str]) -> None:
    """A listener can't hand its clients over to a shard-aware listener that
    disagrees with it about the PROXY protocol header: a client reconnects the way
    it connected, through the proxy or not, and the shard-aware listener would
    refuse it. Setting the option to a set of listeners that asks for that fails.
    """
    first, _ = leased_addresses
    listeners = dict(deployment.listeners)
    listeners['cql_shard_aware_proxy_protocol'] = {'protocol': 'cql', 'address': first,
                                                   'port': CQL_SHARD_AWARE_PROXY_PORT,
                                                   'shard_aware': True, 'proxy_protocol': True}
    listeners['cql'] = {**listeners['cql'], 'shard_aware_listener': 'cql_shard_aware_proxy_protocol'}
    host_id = await manager.get_host_id(deployment.server.server_id)
    with pytest.raises(InvalidRequest, match="which expects a PROXY protocol header"):
        await manager.get_cql().run_async(
                f"ALTER NODE {host_id} WITH listeners = {as_cql_literal(listeners)}")


async def test_cql_server_follows_its_first_and_last_listener(
        manager: ScyllaClusterManager, deployment: Deployment) -> None:
    """Taking every CQL listener away stops the CQL server, rather than leave it
    running with nothing to listen on while gossip tells the cluster it serves
    CQL; giving it a listener again starts it, on a node that keeps running
    throughout.

    With no CQL listener left, the option is changed back over the maintenance
    socket.
    """
    server = deployment.server
    socket_endpoint = UnixSocketEndPoint(await manager.server_get_maintenance_socket_path(server.server_id))
    cluster = manager.con_gen([socket_endpoint],
                              load_balancing_policy=WhiteListRoundRobinPolicy([socket_endpoint]))
    try:
        maintenance_cql = cluster.connect()
        cql_endpoints = [endpoint for name, endpoint in deployment.endpoints.items()
                         if deployment.listeners[name]['protocol'] == 'cql']

        no_cql = {name: listener for name, listener in deployment.listeners.items()
                  if listener['protocol'] != 'cql'}
        await set_listeners_live(manager, server, no_cql, cql=maintenance_cql)
        await wait_until_served(maintenance_cql, 'native transport', False)
        for endpoint in cql_endpoints:
            await assert_not_listening(endpoint)
        # The other protocol carried on regardless.
        assert 'TableNames' in await alternator_list_tables(deployment.endpoints['alternator'])

        await set_listeners_live(manager, server, deployment.listeners, cql=maintenance_cql)
        await wait_until_served(maintenance_cql, 'native transport', True)
        for endpoint in cql_endpoints:
            assert 'CQL_VERSION' in await cql_options(endpoint)
    finally:
        safe_driver_shutdown(cluster)
    await reconnect_driver(manager)


class OccupiedPort:
    """A port something other than Scylla is listening on, for as long as the
    context lasts."""
    def __init__(self, address: str, port: int) -> None:
        self.endpoint = Endpoint(address, port)
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def __enter__(self) -> Endpoint:
        self._socket.bind((self.endpoint.address, self.endpoint.port))
        self._socket.listen()
        return self.endpoint

    def __exit__(self, *exc) -> None:
        self._socket.close()


async def test_a_cql_listener_that_cannot_be_opened_does_not_hold_back_the_others(
        manager: ScyllaClusterManager, deployment: Deployment,
        leased_addresses: tuple[str, str]) -> None:
    """Whether a socket can be bound is only known by binding it. A listener that
    can't be is reported and left out, and the rest of the change is applied all
    the same.
    """
    first, _ = leased_addresses
    added = Endpoint(first, IGNORED_LEGACY_PORT)
    removed = deployment.endpoints['cql_proxy_protocol']
    log = await manager.server_open_log(deployment.server.server_id)
    mark = await log.mark()

    with OccupiedPort(first, OCCUPIED_PORT) as occupied:
        listeners = dict(deployment.listeners)
        listeners['cql_blocked'] = {'protocol': 'cql', 'address': occupied.address, 'port': occupied.port}
        listeners['cql_added'] = {'protocol': 'cql', 'address': added.address, 'port': added.port}
        del listeners['cql_proxy_protocol']
        await set_listeners_live(manager, deployment.server, listeners)

        await log.wait_for(f"Failed to listen for CQL clients on {occupied.address}:{occupied.port}",
                           from_mark=mark, timeout=60)
        await log.wait_for("Could not open the CQL listeners", from_mark=mark, timeout=60)

        assert 'CQL_VERSION' in await cql_options(added)
        await assert_not_listening(removed)
        assert 'CQL_VERSION' in await cql_options(deployment.endpoints['cql'])
        served = await served_addresses(manager.get_cql(), 'native transport')
        assert f"{added.address}:{added.port}" in served
        assert f"{occupied.address}:{occupied.port}" not in served
async def test_a_listener_on_an_address_already_listened_on_does_not_disturb_it(
        manager: ScyllaClusterManager, deployment: Deployment) -> None:
    """Two listeners spelled differently - one leaving the address out, one spelling
    it - can be on the same address and port, which only shows when the second fails
    to open. It must fail without disturbing the first: its connections keep being
    offered the shard-aware listener the first one names.
    """
    server = deployment.server
    kept = deployment.endpoints['cql_default']
    log = await manager.server_open_log(server.server_id)

    # Give the listener on the default address a shard-aware listener to advertise,
    # so that a configuration overwritten by the second one would show.
    listeners = dict(deployment.listeners)
    listeners['cql_default_shard_aware'] = {'protocol': 'cql', 'port': CQL_SHARD_AWARE_MOVED_PORT,
                                            'shard_aware': True}
    listeners['cql_default'] = {**listeners['cql_default'], 'shard_aware_listener': 'cql_default_shard_aware'}
    await set_listeners_live(manager, server, listeners)
    expected = ('SCYLLA_SHARD_AWARE_PORT', CQL_SHARD_AWARE_MOVED_PORT)

    async def advertised() -> bool | None:
        try:
            return advertised_shard_aware_port(await cql_options(kept)) == expected or None
        except Exception:
            return None
    await wait_for(advertised, time.time() + 60)

    reader, writer = await connect(kept)
    try:
        mark = await log.mark()
        listeners['cql_alias'] = {'protocol': 'cql', 'address': server.ip_addr, 'port': DEFAULT_CQL_PORT}
        await set_listeners_live(manager, server, listeners)
        await log.wait_for(f"Failed to listen for CQL clients on {server.ip_addr}:{DEFAULT_CQL_PORT} as cql_alias",
                           from_mark=mark, timeout=60)

        assert advertised_shard_aware_port(await cql_options(kept)) == expected
        assert advertised_shard_aware_port(await cql_options_on(reader, writer)) == expected
    finally:
        writer.close()


async def test_a_cql_server_restarted_on_a_running_node_opens_what_it_can(
        manager: ScyllaClusterManager, deployment: Deployment,
        leased_addresses: tuple[str, str]) -> None:
    """A listener that could not be opened stays in the set the node serves. Only the
    start with the node insists on every listener opening: a CQL server stopped and
    started again on a running node opens the others, rather than fail to start.
    """
    first, _ = leased_addresses
    server = deployment.server
    log = await manager.server_open_log(server.server_id)
    socket_endpoint = UnixSocketEndPoint(await manager.server_get_maintenance_socket_path(server.server_id))
    cluster = manager.con_gen([socket_endpoint],
                              load_balancing_policy=WhiteListRoundRobinPolicy([socket_endpoint]))
    try:
        maintenance_cql = cluster.connect()
        with OccupiedPort(first, OCCUPIED_PORT) as occupied:
            mark = await log.mark()
            listeners = dict(deployment.listeners)
            listeners['cql_blocked'] = {'protocol': 'cql', 'address': occupied.address, 'port': occupied.port}
            await set_listeners_live(manager, server, listeners, cql=maintenance_cql)
            await log.wait_for(f"Failed to listen for CQL clients on {occupied.address}:{occupied.port}",
                               from_mark=mark, timeout=60)

            await manager.api.client.delete("/storage_service/native_transport", host=server.ip_addr)
            await wait_until_served(maintenance_cql, 'native transport', False)

            mark = await log.mark()
            await manager.api.client.post("/storage_service/native_transport", host=server.ip_addr)
            await wait_until_served(maintenance_cql, 'native transport', True)
            await log.wait_for(f"Failed to listen for CQL clients on {occupied.address}:{occupied.port}",
                               from_mark=mark, timeout=60)
            assert 'CQL_VERSION' in await cql_options(deployment.endpoints['cql'])
            assert f"{occupied.address}:{occupied.port}" not in await served_addresses(maintenance_cql, 'native transport')
    finally:
        safe_driver_shutdown(cluster)
    await reconnect_driver(manager)




@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_a_listener_change_after_the_cql_server_stopped_for_good_is_ignored(
        manager: ScyllaClusterManager, deployment: Deployment,
        leased_addresses: tuple[str, str]) -> None:
    """Draining a node stops its CQL server for good, but a change to the listeners
    can still reach it afterwards - as one did on a node shutting down right after it
    started, with the startup delivery of the option still on its way. The change
    has nothing left to apply to, and is ignored rather than reported as a failure.
    """
    first, _ = leased_addresses
    server = deployment.server
    log = await manager.server_open_log(server.server_id)
    injection = 'cql_controller_apply_listeners_wait'
    await manager.api.enable_injection(server.ip_addr, injection, one_shot=True)

    listeners = dict(deployment.listeners)
    listeners['cql_added'] = {'protocol': 'cql', 'address': first, 'port': IGNORED_LEGACY_PORT}
    mark = await log.mark()
    await set_listeners_live(manager, server, listeners)
    await manager.api.wait_for_injection_enter(server.ip_addr, injection)

    await manager.api.drain(server.ip_addr)
    await manager.api.message_injection(server.ip_addr, injection)

    await log.wait_for("The CQL server was stopped for good, ignoring the change to its listeners",
                       from_mark=mark, timeout=60)
    assert not await log.grep("Failed to apply the CQL listeners", from_mark=mark)
    await manager.server_stop_gracefully(server.server_id)
