#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# The pinned plan reaches a later page only through the paging state, so these
# tests page alternately through two coordinators. Refs #18992

import logging
import time

import pytest
from cassandra.protocol import InvalidRequest  # type: ignore
from cassandra.query import SimpleStatement  # type: ignore

from test.cluster.util import new_test_keyspace
from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for, wait_for_cql_and_get_hosts, wait_for_view

logger = logging.getLogger(__name__)

RF2 = "with replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}"


async def wait_until_index_visible(cql, hosts, keyspace, table, index_name, present):
    """Waits until every node agrees on whether the index exists, so that a node
    resolving the pinned plan against a stale index set cannot be raced with."""
    async def agreed():
        for host in hosts:
            rows = await cql.run_async(
                    f"SELECT index_name FROM system_schema.indexes WHERE keyspace_name = '{keyspace}'"
                    f" AND table_name = '{table}' AND index_name = '{index_name}'", host=host)
            if bool(rows) != present:
                return None
        return True
    await wait_for(agreed, time.time() + 60, label=f"index {index_name} present={present} everywhere")


async def fill(cql, table, rows):
    insert = cql.prepare(f"INSERT INTO {table} (p, v) VALUES (?, ?)")
    for p, v in rows:
        await cql.run_async(insert, [p, v])


async def test_resumed_page_keeps_its_plan_on_another_coordinator(manager: ManagerClient) -> None:
    servers = await manager.servers_add(2, auto_rack_dc="dc1")
    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    async with new_test_keyspace(manager, RF2) as ks:
        table = f"{ks}.tbl"
        await cql.run_async(f"CREATE TABLE {table} (p int PRIMARY KEY, v int)")
        # A sparse match, so a page served by the wrong plan shows up as a wrong
        # row set rather than as the right one by accident.
        await fill(cql, table, [(i, 17 if i % 4 == 0 else 23) for i in range(40)])

        stmt = SimpleStatement(f"SELECT p FROM {table} WHERE v = 17 ALLOW FILTERING", fetch_size=3)
        expected = sorted(row.p for row in cql.execute(stmt))
        assert len(expected) > 3

        page = cql.execute(stmt, host=hosts[0])
        assert page.has_more_pages
        got = list(page.current_rows)
        logger.info("first page: %d rows, base table scanned, coordinator %s", len(got), hosts[0])

        await cql.run_async(f"CREATE INDEX idx ON {table}(v)")
        await wait_for_view(cql, "idx_index", len(servers))
        await wait_until_index_visible(cql, hosts, ks, "tbl", "idx", present=True)
        logger.info("an index that could serve this query now exists on every node")

        node = 1
        while page.has_more_pages:
            host = hosts[node % len(hosts)]
            page = cql.execute(stmt, host=host, paging_state=page.paging_state)
            logger.info("resumed page: %d rows, coordinator %s", len(page.current_rows), host)
            got.extend(page.current_rows)
            node += 1

        assert expected == sorted(row.p for row in got)


async def test_resuming_a_dropped_index_is_refused_on_another_coordinator(manager: ManagerClient) -> None:
    servers = await manager.servers_add(2, auto_rack_dc="dc1")
    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    async with new_test_keyspace(manager, RF2) as ks:
        table = f"{ks}.tbl"
        await cql.run_async(f"CREATE TABLE {table} (p int PRIMARY KEY, v int)")
        await fill(cql, table, [(i, 17) for i in range(40)])
        await cql.run_async(f"CREATE INDEX idx ON {table}(v)")
        await wait_for_view(cql, "idx_index", len(servers))
        await wait_until_index_visible(cql, hosts, ks, "tbl", "idx", present=True)

        # ALLOW FILTERING, so a node that re-planned rather than keeping the
        # pinned plan would carry on off a base-table scan instead of failing.
        stmt = SimpleStatement(f"SELECT p FROM {table} WHERE v = 17 ALLOW FILTERING", fetch_size=7)
        page = cql.execute(stmt, host=hosts[0])
        assert page.has_more_pages
        logger.info("first page: %d rows, index scanned, coordinator %s", len(page.current_rows), hosts[0])

        await cql.run_async(f"DROP INDEX {ks}.idx")
        await wait_until_index_visible(cql, hosts, ks, "tbl", "idx", present=False)
        logger.info("index dropped everywhere; the saved position belongs to a view that is gone")

        with pytest.raises(InvalidRequest, match="no longer available"):
            cql.execute(stmt, host=hosts[1], paging_state=page.paging_state)
