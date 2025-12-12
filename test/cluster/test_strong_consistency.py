from test.pylib.manager_client import ManagerClient
from test.pylib.util import gather_safely, wait_for
from test.cluster.util import new_test_keyspace
from test.pylib.internal_types import ServerInfo

import pytest
import logging
import time
import uuid


logger = logging.getLogger(__name__)


async def wait_for_leader(manager: ManagerClient, s: ServerInfo, group_id: str):
    async def get_leader_host_id():
        result = await manager.api.get_raft_leader(s.ip_addr, group_id)
        return None if uuid.UUID(result).int == 0 else result
    return await wait_for(get_leader_host_id, time.time() + 60)


@pytest.mark.asyncio
async def test_basic_write_read(manager: ManagerClient):

    logger.info("Bootstrapping cluster")
    config = {
        'experimental_features': ['strongly-consistent-tables']
    }
    servers = await manager.servers_add(3, config=config, auto_rack_dc='my_dc')
    (cql, hosts) = await manager.get_ready_cql(servers)

    logger.info("Load host_id-s for servers")
    host_ids = await gather_safely(*[manager.get_host_id(s.server_id) for s in servers])

    def host_by_host_id(host_id):
        for hid, host in zip(host_ids, hosts):
            if hid == host_id:
                return host
        raise RuntimeError(f"Can't find host for host_id {host_id}")

    logger.info("Creating a strongly-consisteny keyspace")
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND tablets = {'initial': 1} AND consistency = 'local'") as ks:
        logger.info("Creating a table")
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")

        logger.info("Select raft group id for the tablet")
        table_id = await manager.get_table_id(ks, 'test')
        rows = await cql.run_async(f"SELECT raft_group FROM system.tablets where table_id = {table_id}")
        group_id = str(rows[0].raft_group)

        logger.info(f"Get current leader for the group {group_id}")
        leader_host_id = await wait_for_leader(manager, servers[0], group_id)
        leader_host = host_by_host_id(leader_host_id)

        logger.info(f"Run INSERT statement on the leader {leader_host}")
        await cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES (10, 20)", host=leader_host)

        logger.info(f"Run SELECT statement on the leader {leader_host}")
        rows = await cql.run_async(f"SELECT * FROM {ks}.test WHERE pk = 10;")
        assert len(rows) == 1
        row = rows[0]
        assert row.pk == 10
        assert row.c == 20
