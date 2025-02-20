#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
import asyncio
from datetime import datetime, timezone
from functools import partial
import json
import logging
import time
import pytest

from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for, wait_for_cql_and_get_hosts
from test.cluster.conftest import skip_mode
from test.cluster.util import disable_schema_agreement_wait, new_test_keyspace, new_test_table

logger = logging.getLogger(__name__)


def check_tombstone_gc_mode(cql, table, mode):
    s = list(cql.execute(f"DESC {table}"))[0].create_statement
    assert f"'mode': '{mode}'" in s


def get_expected_tombstone_gc_mode(rf, tablets):
    return "repair" if tablets and rf > 1 else "timeout"


@pytest.mark.asyncio
@pytest.mark.parametrize("rf", [1, 2])
@pytest.mark.parametrize("tablets", [True, False])
async def test_default_tombstone_gc(manager: ManagerClient, rf: int, tablets: bool):
    _ = [await manager.server_add() for _ in range(2)]
    cql = manager.get_cql()
    tablets_enabled = "true" if tablets else "false"
    async with new_test_keyspace(manager, f"with replication = {{ 'class': 'NetworkTopologyStrategy', 'replication_factor': {rf}}} and tablets = {{ 'enabled': {tablets_enabled} }}") as keyspace:
        async with new_test_table(manager, keyspace, "p int primary key, x int") as table:
            check_tombstone_gc_mode(cql, table, get_expected_tombstone_gc_mode(rf, tablets))


@pytest.mark.asyncio
@pytest.mark.parametrize("rf", [1, 2])
@pytest.mark.parametrize("tablets", [True, False])
async def test_default_tombstone_gc_does_not_override(manager: ManagerClient, rf: int, tablets: bool):
    _ = [await manager.server_add() for _ in range(2)]
    cql = manager.get_cql()
    tablets_enabled = "true" if tablets else "false"
    async with new_test_keyspace(manager, f"with replication = {{ 'class': 'NetworkTopologyStrategy', 'replication_factor': {rf}}} and tablets = {{ 'enabled': {tablets_enabled} }}") as keyspace:
        async with new_test_table(manager, keyspace, "p int primary key, x int", " with tombstone_gc = {'mode': 'disabled'}") as table:
            await cql.run_async(f"ALTER TABLE {table} add y int")
            check_tombstone_gc_mode(cql, table, "disabled")


@pytest.mark.asyncio
async def test_group0_tombstone_gc(manager: ManagerClient):
    """
    Regression test for #15607.

    Test that the tombstones are being cleaned after the group 0 catching up.

    Test #1:
        Arrange:
            - start 3 nodes
        Act:
            - create new group0 tombstones by updating the schema (create/alter/delete randome tables)
        Assert:
            - the tombstones are cleaned up eventually

    Test #2:
        Arrange:
            - stop one of the nodes
        Act:
            - create new group0 tombstones by updating the schema (create/alter/delete randome tables)
        Assert:
            - the tombstones are not cleaned up (one of the nodes is down, so they can't catch up)

    Test #3:
        Arrange:
            - start the node again
        Act:
            - tombstones exist from the previous test
        Assert:
            - the tombstones are cleaned up eventually
    """
    cmdline = [
        # disabling caches as the tombstones still remain in the cache even after the compaction
        # (the alternative would be to drop the caches after the compaction or to filter the mutation fragments listing)
        '--enable-cache', '0',
    ]
    cfg = {
        'group0_tombstone_gc_refresh_interval_in_ms': 1000,  # this is 1 hour by default
    }
    servers = [await manager.server_add(cmdline=cmdline, config=cfg) for _ in range(3)]

    cql = manager.get_cql()
    hosts = [(await wait_for_cql_and_get_hosts(cql, [s], time.time() + 60))[0]
             for s in servers]

    host_primary = hosts[0]

    # create/alter/drop a few tables
    async def alter_system_schema(keyspace=None, table_count=3):
        if not keyspace:
            async with new_test_keyspace(manager, "with replication = { 'class': 'NetworkTopologyStrategy', 'replication_factor': 2}", host=host_primary) as keyspace:
                alter_system_schema(keyspace, table_count)
                return

        for _ in range(table_count):
            async with new_test_table(manager, keyspace, "p int primary key, x int", host=host_primary, reuse_tables=False) as table:
                await cql.run_async(f"ALTER TABLE {table} add y int")

    def get_tombstone(row):
        if row.metadata is None:
            return None
        metadata = json.loads(row.metadata)
        return metadata.get("tombstone")

    async def list_tombstones(tombstone_mark, host):
        tombstones = {}
        for tbl in ("tables", "columns"):
            res = list(cql.execute(f"SELECT * FROM MUTATION_FRAGMENTS(system_schema.{tbl})", host=host))
            tombstones[tbl] = []
            for row in res:
                tombstone = get_tombstone(row)
                if tombstone and datetime.fromtimestamp(float(tombstone["timestamp"])/1_000_000, timezone.utc) < tombstone_mark:
                    tombstones[tbl].append(tombstone)
        return tombstones

    async def tombstone_gc_completed(tombstone_mark):
        # flush and compact the keyspace
        await asyncio.gather(*[manager.api.keyspace_flush(srv.ip_addr, "system_schema")
                               for srv in servers])
        await asyncio.gather(*[manager.api.keyspace_compaction(srv.ip_addr, "system_schema")
                               for srv in servers])

        # check the remanining tombstones
        tombstones_count_total = 0
        tombstones_per_host = await asyncio.gather(*[list_tombstones(tombstone_mark, host)
                                                     for host in hosts])
        for tombstones in tombstones_per_host:
            for tbl in tombstones.keys():
                tombstones_remaining = tombstones[tbl]
                tombstones_count = len(tombstones_remaining)
                tombstones_count_total += tombstones_count
                logger.info(f"{tbl} tombstones remaining: {tombstones_count}")
        if tombstones_count_total != 0:
            return None
        return True

    # should usually run much faster than 30s, but left some margin to avoid flakiness
    async def verify_tombstone_gc(tombstone_mark, timeout=30):
        # wait for 2 sec to let the current tombstones fully expire
        await asyncio.sleep(2)

        deadline = time.time() + timeout

        # perform a single change to generate a new state_id and set the previous tombstones to expire
        # (this is needed because we deduct 1s of the tombstone expiration time to account for the changes coming
        # in the same second)
        await alter_system_schema(table_count=1)

        await wait_for(partial(tombstone_gc_completed, tombstone_mark), deadline)

    with disable_schema_agreement_wait(cql):
        async with new_test_keyspace(manager, "with replication = { 'class': 'NetworkTopologyStrategy', 'replication_factor': 2}", host=host_primary) as keyspace:
            await alter_system_schema(keyspace)
            tombstone_mark = datetime.now(timezone.utc)

            # test #1: the tombstones are cleaned up eventually
            await verify_tombstone_gc(tombstone_mark)

            # shut down one server
            down_server = servers.pop()
            down_host = hosts.pop()
            await manager.server_stop_gracefully(down_server.server_id)
            await asyncio.gather(*[manager.server_not_sees_other_server(srv.ip_addr, down_server.ip_addr)
                                   for srv in servers])

            await alter_system_schema(keyspace)
            tombstone_mark = datetime.now(timezone.utc)

            # test #2: the tombstones are not cleaned up when one node is down
            with pytest.raises(AssertionError, match="Deadline exceeded"):
                # waiting for shorter time (5s normally enough for a successful case, we expect the timeout here)
                await verify_tombstone_gc(tombstone_mark, timeout=5)

            # start the server again
            await manager.server_start(down_server.server_id)
            await asyncio.gather(*[manager.server_sees_other_server(srv.ip_addr, down_server.ip_addr)
                                   for srv in servers])

            servers.append(down_server)
            hosts.append(down_host)

            # make sure the hosts are available (avoid test flakiness because of ConnectionError)
            await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

            # test #3: the tombstones are cleaned up after the node is started again
            await verify_tombstone_gc(tombstone_mark)


@pytest.mark.asyncio
@skip_mode('release', "test only needs to run once - allowing only the 'dev' mode")
@skip_mode('debug', "test only needs to run once - allowing only the 'dev' mode")
async def test_group0_state_id_failure(manager: ManagerClient):
    """
    Issue #21117 regression test.

    Make sure that the "endpoint_state_map does not contain endpoint" warning is not triggered when the state_id is updated.

    Arrange:
        - start 1 node
    Act:
        - restart the node
    Assert:
        - the "endpoint_state_map does not contain endpoint" didn't appear in the log
    """

    # Start the node

    srv = await manager.server_add()

    # Restart the node

    await manager.server_restart(srv.server_id)

    # Check the log

    log = await manager.server_open_log(srv.server_id)
    matches = await log.grep(r"Fail to apply application_state.*endpoint_state_map does not contain endpoint .* application_states = {GROUP0_STATE_ID")

    assert not matches, "The 'endpoint_state_map does not contain endpoint' warning appeared in the log"
