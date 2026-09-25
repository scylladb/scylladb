#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import logging

import pytest

from test.cluster.auth_cluster import extra_scylla_config_options as auth_config
from test.pylib.object_storage import Storage, StorageFactory, keyspace_options
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)


@pytest.fixture
async def object_storage(object_storage_factory: StorageFactory) -> Storage:
    return await object_storage_factory("s3")


async def test_shutdown_after_object_storage_reads_under_service_level(manager: ScyllaClusterManager, object_storage: Storage):
    """A node that served reads of a table stored on S3 under a user service
    level must still shut down cleanly.

    The service level controller destroys the scheduling group of every
    service level when it stops, while the object storage clients keep
    connections bound to the group their requests ran in. Closing such a
    connection after the group is gone crashed the node during shutdown.

    Reproduces SCYLLADB-4669.
    """
    cfg = auth_config | {'object_storage_endpoints': object_storage.create_endpoint_conf()}
    server = await manager.server_add(config=cfg)
    cql = manager.get_cql()

    await cql.run_async("CREATE SERVICE LEVEL sl_bulk WITH shares = 500")
    await cql.run_async("ATTACH SERVICE LEVEL sl_bulk TO cassandra")
    # A connection picks up the role's service level when it logs in.
    await manager.driver_connect(server=server)
    cql = manager.get_cql()

    await cql.run_async(f"CREATE KEYSPACE ks {keyspace_options(object_storage)}")
    await cql.run_async("CREATE TABLE ks.t (pk int PRIMARY KEY, v int)")
    await cql.run_async("INSERT INTO ks.t (pk, v) VALUES (1, 1)")
    await manager.api.flush_keyspace(server.ip_addr, "ks")

    before = await manager.metrics.query(server.ip_addr)
    rows = await cql.run_async("SELECT * FROM ks.t WHERE pk = 1 BYPASS CACHE")
    assert [(r.pk, r.v) for r in rows] == [(1, 1)]
    after = await manager.metrics.query(server.ip_addr)

    # The S3 client keeps one http client per scheduling group and labels its counters with the group.
    labels = {'class': 'sl:sl_bulk'}
    reads = (after.get('scylla_object_storage_total_read_requests', labels) or 0) \
        - (before.get('scylla_object_storage_total_read_requests', labels) or 0)
    logger.info("object storage read requests issued by the query: %s", reads)
    assert reads > 0

    await manager.server_stop_gracefully(server.server_id)
