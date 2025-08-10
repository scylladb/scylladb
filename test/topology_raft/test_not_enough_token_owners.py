#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
import pytest
import logging
import time

from test.pylib.manager_client import ManagerClient
from test.pylib.util import unique_name, wait_for_cql_and_get_hosts


@pytest.mark.asyncio
async def test_not_enough_token_owners(manager: ManagerClient):
    """
    Test that:
    - the first node in the cluster cannot be a zero-token node
    - removenode and decommission of the only token owner fail in the presence of zero-token nodes
    - removenode and decommission of a token owner fail in the presence of zero-token nodes if the number of token
      owners would fall below the RF of some keyspace using tablets
    """
    logging.info('Trying to add a zero-token server as the first server in the cluster')
    await manager.server_add(config={'join_ring': False},
                             expected_error='Cannot start the first node in the cluster as zero-token')

    logging.info('Adding the first server')
    server_a = await manager.server_add()

    logging.info('Adding two zero-token servers')
    # The second server is needed only to preserve the Raft majority.
    server_b = (await manager.servers_add(2, config={'join_ring': False}))[0]

    logging.info(f'Trying to decommission the only token owner {server_a}')
    await manager.decommission_node(server_a.server_id,
                                    expected_error='Cannot decommission the last token-owning node in the cluster')

    logging.info(f'Stopping {server_a}')
    await manager.server_stop_gracefully(server_a.server_id)

    logging.info(f'Trying to remove the only token owner {server_a} by {server_b}')
    await manager.remove_node(server_b.server_id, server_a.server_id,
                              expected_error='cannot be removed because it is the last token-owning node in the cluster')

    logging.info(f'Starting {server_a}')
    await manager.server_start(server_a.server_id)

    logging.info('Adding a normal server')
    await manager.server_add()

    cql = manager.get_cql()

    await wait_for_cql_and_get_hosts(cql, [server_a], time.time() + 60)

    ks_name = unique_name()
    await cql.run_async(f"""CREATE KEYSPACE {ks_name} WITH replication = {{'class': 'NetworkTopologyStrategy',
                             'replication_factor': 2}} AND tablets = {{ 'enabled': true }}""")
    await cql.run_async(f'CREATE TABLE {ks_name}.tbl (pk int PRIMARY KEY, v int)')
    await cql.run_async(f'INSERT INTO {ks_name}.tbl (pk, v) VALUES (1, 1)')

    # FIXME: Once scylladb/scylladb#16195 is fixed, we will have to replace the expected error message.
    # A similar change may be needed for remove_node below.
    logging.info(f'Trying to decommission {server_a} - one of the two token owners')
    await manager.decommission_node(server_a.server_id, expected_error='Unable to find new replica for tablet')

    logging.info(f'Stopping {server_a}')
    await manager.server_stop_gracefully(server_a.server_id)

    logging.info(f'Trying to remove {server_a}, one of the two token owners, by {server_b}')
    await manager.remove_node(server_b.server_id, server_a.server_id,
                              expected_error='Unable to find new replica for tablet')
