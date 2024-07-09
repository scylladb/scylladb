#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import logging
import pytest

from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import inject_error, read_barrier
from test.topology.conftest import skip_mode


logger = logging.getLogger(__name__)


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_table_desc_read_barrier(manager: ManagerClient) -> None:
    """
    Regression test for #19213.

    Test that the schema is always updated after read barrier.

    Arrange:
        - start 2 nodes
        - create a table
        - inject raft delay on the first node
        - disable the schema agreement wait
    Act:
        - alter the table (change schema) on the second node
        - trigger the read barrier
    Assert:
        - the schema retrieved on both hosts matches
    """
    servers = await manager.servers_add(2)

    logging.info("Waiting until driver connects to every server")
    cql, hosts = await manager.get_ready_cql(servers)

    logger.info("Creating keyspace and table")
    await cql.run_async("create keyspace ks with replication = "
                        "{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}")
    await cql.run_async("create table ks.t (pk int primary key)")

    logger.info("Disabling the schema agreement wait")
    assert hasattr(cql.cluster, "max_schema_agreement_wait")
    cql.cluster.max_schema_agreement_wait = 0

    async with inject_error(manager.api, servers[0].ip_addr, 'group0_state_machine::delay_apply'):
        logger.info("Altering table")
        sec_host = next(h for h in hosts if h.address == servers[1].ip_addr)
        await cql.run_async("alter table ks.t add s1 int", host=sec_host)

        # wait for the first node to see the latest state (after the delay ends)
        await read_barrier(manager.api, servers[0].ip_addr)

        # verify that there is no schema difference after the read barrier
        desc_schema = [await cql.run_async("DESC SCHEMA", host=h) for h in hosts]
        assert desc_schema[0] == desc_schema[1]
