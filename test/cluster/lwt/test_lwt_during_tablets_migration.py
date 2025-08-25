import asyncio
import logging
import random

import pytest
from test.cluster.conftest import skip_mode
from test.cluster.util import new_test_keyspace
from test.pylib.manager_client import ManagerClient
from test.pylib.tablets import get_tablet_replicas

from test.cluster.lwt.lwt_common import (
    BaseLWTTester, get_token_for_pk, get_host_map, pick_non_replica_server,
    DEFAULT_WORKERS, DEFAULT_NUM_KEYS
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Test constants
WORKLOAD_SEC = 30


async def continuous_tablet_migrations(manager: ManagerClient, servers, ks: str, tbl: str,
                                       duration_sec: int, pks, pause_range=(0.5, 2.0)):
    """
    Continuously migrate tablets between servers for the specified duration.
    """
    logger.info("Starting continuous tablet migrations for %s seconds", duration_sec)
    start = asyncio.get_event_loop().time()
    migration_count = 0

    host_map = await get_host_map(manager, servers)

    while asyncio.get_event_loop().time() - start < duration_sec:
        try:
            sample_pk = random.choice(pks)
            token = await get_token_for_pk(manager.get_cql(), ks, tbl, sample_pk)

            replicas = await get_tablet_replicas(manager, servers[0], ks, tbl, token)
            if not replicas:
                logger.info("No replicas for token=%s, skipping", token)
                await asyncio.sleep(1.0)
                continue

            src_host_id, src_shard = random.choice(replicas)
            src_server = host_map.get(src_host_id)
            if not src_server:
                host_map = await get_host_map(manager, servers)
                src_server = host_map.get(src_host_id)
                if not src_server:
                    await asyncio.sleep(1.0)
                    continue

            replica_hids = {h for (h, _shard) in replicas}
            dst_server = await pick_non_replica_server(manager, servers, replica_hids)
            if not dst_server:
                logger.info("No non-replica destination for token=%s, skipping", token)
                await asyncio.sleep(1.0)
                continue

            dst_hid = await manager.get_host_id(dst_server.server_id)
            await manager.api.move_tablet(src_server.ip_addr, ks, tbl,
                                          src_host_id, src_shard,
                                          dst_hid, 0, token)
            migration_count += 1
            logger.info("Completed migration #%d (token=%s -> %s)", migration_count,
                        token, dst_server.ip_addr)

            await asyncio.sleep(random.uniform(*pause_range))

        except Exception as e:
            logger.warning("Migration attempt failed: %s, continuing...", e)
            await asyncio.sleep(1.0)

    logger.info("Completed %d tablet migrations in %s seconds", migration_count, duration_sec)


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
@skip_mode('debug', 'debug mode is too slow for this test')
async def test_multi_column_lwt_during_migration(manager: ManagerClient):
    """Test multi-column LWT pattern during continuous tablet migrations"""

    # Setup cluster
    cfg = {
        "tablets_mode_for_new_keyspaces": "enabled",
        "rf_rack_valid_keyspaces": False,
    }

    servers = await manager.servers_add(6, config=cfg, auto_rack_dc='dc1')
    for server in servers:
        await manager.api.disable_tablet_balancing(server.ip_addr)

    async with new_test_keyspace(
        manager,
        "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2} AND tablets = {'initial': 5}"
    ) as ks:
        tester = BaseLWTTester(manager, ks, "lwt_table", num_workers=DEFAULT_WORKERS, num_keys=DEFAULT_NUM_KEYS)
        await tester.create_schema()
        await tester.initialize_rows()
        tester.prepare_statements()
        await tester.start_workers()

        try:
            # Run continuous tablet migrations concurrently with the LWT workload
            logger.info("Starting concurrent LWT workload and tablet migrations for %s seconds", WORKLOAD_SEC)
            migration_task = asyncio.create_task(
                continuous_tablet_migrations(manager, servers, ks, tester.tbl, WORKLOAD_SEC, tester.schema.pks)
            )
            await asyncio.wait_for(migration_task, timeout=WORKLOAD_SEC + 5)

        finally:
            await tester.stop_workers()

        await tester.verify_consistency()
        logger.info("Multi-column LWT during continuous migrations test completed successfully")
