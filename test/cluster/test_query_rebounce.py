#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import asyncio
import pytest
import logging
import time

from test.pylib.internal_types import IPAddress
from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import inject_error
from test.cluster.util import new_test_keyspace


logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_query_rebounce(manager: ManagerClient):
    """
    Issue https://github.com/scylladb/scylladb/issues/15465.

    Test emulating several LWT(Lightweight Transaction) query rebounces. Currently, the code
    that processes queries does not expect that a query may be rebounced more than once.
    It was impossible with the VNodes, but with intruduction of the Tablets, data can be moved
    between shards by the balancer thus a query can be rebounced to different shards multiple times.

    1) Create a keyspace and a table.
    2) Insert some data.
    3) Inject an error to force a rebounce 2 times.
    4) Update the data with a LWT query. The update will fail on this step with the current implementation.
    5) Check the result of update.

    """

    cmdline = [
        '--logger-log-level', 'raft=trace',
    ]

    servers = await manager.servers_add(1, cmdline=cmdline)

    servers = await manager.running_servers()
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "with replication = {'class': 'SimpleStrategy', 'replication_factor': 1} and tablets = {'enabled': false};") as ks:
        await cql.run_async(f"create table {ks}.lwt (a int, b int, primary key(a));")

        await cql.run_async(f"insert into {ks}.lwt (a,b ) values (1, 10);")
        await cql.run_async(f"insert into {ks}.lwt (a,b ) values (2, 20);")

        errs = [manager.api.enable_injection(s.ip_addr, "forced_bounce_to_shard_counter", one_shot=False,
                                            parameters={'value': '2'})
                for s in servers]

        await asyncio.gather(*errs)

        await cql.run_async(f"update {ks}.lwt set b = 11 where a = 1 if b = 10;")

        rows = await cql.run_async(f"select b from {ks}.lwt where a = 1;")

        assert rows[0].b == 11
