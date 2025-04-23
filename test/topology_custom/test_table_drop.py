import asyncio
from test.pylib.manager_client import ManagerClient
from test.topology.util import new_test_keyspace
import pytest

@pytest.mark.asyncio
async def test_drop_table_during_streaming_receiver_side(manager: ManagerClient):
    servers = [await manager.server_add(config={
        'error_injections_at_startup': ['stream_mutation_fragments_table_dropped'],
        'enable_repair_based_node_ops': False,
        'enable_user_defined_functions': False,
        'force_gossip_topology_changes': True
    }) for _ in range(2)]

@pytest.mark.asyncio
async def test_drop_table_during_flush(manager: ManagerClient):
    servers = [await manager.server_add() for _ in range(2)]

    await manager.api.enable_injection(servers[0].ip_addr, "flush_tables_on_all_shards_table_drop", True)

    cql = manager.get_cql()
    async with new_test_keyspace(cql, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({k}, {k%3});") for k in range(64)])
        await manager.api.keyspace_flush(servers[0].ip_addr, ks, "test")
