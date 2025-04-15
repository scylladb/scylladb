#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import asyncio
import pytest
import tempfile
import time
from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for_cql_and_get_hosts
from test.cluster.util import reconnect_driver
from test.pylib.tablets import get_all_tablet_replicas

@pytest.mark.asyncio
@pytest.fixture(scope="function")
def workdir():
    with tempfile.TemporaryDirectory() as tmp_dir:
        yield tmp_dir

async def test_file_streaming_respects_encryption(request, manager: ManagerClient, workdir):
    cfg = {
        'tablets_mode_for_new_keyspaces': 'enabled',
    }

    cmdline = ['--smp=1']
    servers = []
    servers.append(await manager.server_add(config=cfg, cmdline=cmdline))
    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    cql = manager.cql
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    cql.execute("CREATE KEYSPACE ks WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1};")
    cql.execute(f"""CREATE TABLE ks.t(pk text primary key) WITH scylla_encryption_options = {{
        'cipher_algorithm' : 'AES/ECB/PKCS5Padding',
        'secret_key_strength' : 128,
        'key_provider': 'LocalFileSystemKeyProviderFactory',
        'secret_key_file': '{workdir}/data_encryption_key'
    }}""")
    cql.execute("INSERT INTO ks.t(pk) VALUES('alamakota')")

    servers.append(await manager.server_add(config=cfg, cmdline=cmdline))

    tablet_replicas = await get_all_tablet_replicas(manager, servers[0], 'ks', 't')
    host_ids = await asyncio.gather(*[manager.get_host_id(s.server_id) for s in servers])
    await manager.api.move_tablet(servers[0].ip_addr, "ks", "t", host_ids[0], 0, host_ids[1], 0, tablet_replicas[0][0])

    rows = cql.execute("SELECT * from ks.t WHERE pk = 'alamakota'")
    assert len(list(rows)) == 1
