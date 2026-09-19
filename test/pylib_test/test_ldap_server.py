#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import logging
import socket
import subprocess

import pytest

from test.pylib.host_registry import HostRegistry
from test.pylib.ldap_server import ToxiproxyPortInUse, can_connect, start_ldap, try_something_backoff

TP_PORT = 8474


@pytest.fixture
async def host():
    """Leases an IP of its own, so that tests sharing the fixed toxiproxy port can't collide."""
    registry = HostRegistry()
    leased = await registry.lease_host()
    yield leased
    await registry.release_host(leased)


async def test_reclaims_a_leaked_toxiproxy(host, tmp_path, caplog):
    """A toxiproxy-server leaked by a previous run must be killed, not talked to (SCYLLADB-2300)."""
    leaked = subprocess.Popen(['toxiproxy-server', '-host', str(host), '-port', str(TP_PORT)],
                              stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    instance_root = tmp_path / 'ldap_instances'
    (instance_root / '5000').mkdir(parents=True)  # makes start_ldap give up right after the reclaim
    try:
        assert try_something_backoff(lambda: can_connect((host, TP_PORT))), 'the leftover never came up'
        with caplog.at_level(logging.WARNING), pytest.raises(FileExistsError):
            start_ldap(host=host, port=5000, instance_root=instance_root, toxiproxy_byte_limit=10)
        assert f'Killing toxiproxy-server {leaked.pid}' in caplog.text
        leaked.wait(timeout=10)  # psutil reaped it already, so its exit status says nothing
        assert not can_connect((host, TP_PORT)), 'a toxiproxy-server survived the reclaim'
    finally:
        leaked.kill()
        leaked.wait()


async def test_refuses_to_start_when_the_port_is_held_by_something_else(host, tmp_path):
    """Only our own leftovers are reclaimed; anything else on the port is reported, not killed."""
    squatter = socket.socket()
    squatter.bind((host, TP_PORT))
    squatter.listen(16)  # start_ldap probes the port twice and never gets accepted, so leave slack
    try:
        with pytest.raises(ToxiproxyPortInUse):
            start_ldap(host=host, port=5000, instance_root=tmp_path / 'ldap_instances', toxiproxy_byte_limit=10)
    finally:
        squatter.close()
