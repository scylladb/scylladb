# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
import time

from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for


def watch_schema_change_events(cql) -> list:
    """Collects the SCHEMA_CHANGE events pushed to the control connection."""
    events: list = []
    cql.cluster.control_connection._connection.register_watchers({"SCHEMA_CHANGE": events.append})
    return events


def table_events(events, keyspace, table) -> list:
    def matches(event):
        options = event.get("options", event)
        return options.get("keyspace") == keyspace and options.get("table") == table

    return [event for event in events if matches(event)]


async def wait_for_table_events(events, keyspace, table, expected_count):
    """Waits until exactly expected_count events arrived for keyspace.table.

    Waiting for at least that many and then checking the count means an extra
    event fails the test as soon as it arrives, rather than at the deadline.
    """
    async def enough_events():
        matching = table_events(events, keyspace, table)
        return matching if len(matching) >= expected_count else None

    matching = await wait_for(enough_events, time.time() + 60,
                              label=f"{expected_count} event(s) for {keyspace}.{table}")
    assert len(matching) == expected_count, \
        f"expected {expected_count} events for {keyspace}.{table}, got {len(matching)}: {matching}"
    return matching


async def create_test_table(cql):
    await cql.run_async("CREATE KEYSPACE schema_events WITH replication = "
                        "{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}")
    await cql.run_async("CREATE TABLE schema_events.table1 (pk int primary key, value int)")


async def test_schema_change_events_only_for_client_visible_changes(manager: ManagerClient):
    servers = await manager.servers_add(1)
    cql, _ = await manager.get_ready_cql(servers)
    await create_test_table(cql)

    events = watch_schema_change_events(cql)

    # A property-only change must not be broadcast. Instead of waiting for an
    # event that should never arrive, follow it with a column change and require
    # that the column change is the first event we see - events reach the
    # connection in order, so a spurious one would show up ahead of it.
    await cql.run_async("ALTER TABLE schema_events.table1 WITH comment = 'not client visible'")
    await cql.run_async("ALTER TABLE schema_events.table1 ADD value2 int")
    await wait_for_table_events(events, "schema_events", "table1", 1)

    # Index changes are broadcast even though no column changed.
    await cql.run_async("CREATE INDEX table1_idx ON schema_events.table1 (value)")
    await wait_for_table_events(events, "schema_events", "table1", 2)

    await cql.run_async("DROP INDEX schema_events.table1_idx")
    await wait_for_table_events(events, "schema_events", "table1", 3)


async def test_schema_change_events_for_materialized_views(manager: ManagerClient):
    servers = await manager.servers_add(1)
    cql, _ = await manager.get_ready_cql(servers)
    await create_test_table(cql)
    await cql.run_async("CREATE MATERIALIZED VIEW schema_events.mv1 AS SELECT * FROM schema_events.table1 "
                        "WHERE value IS NOT NULL AND pk IS NOT NULL PRIMARY KEY (value, pk)")

    events = watch_schema_change_events(cql)

    # Views follow the same rule. The view selects all columns, so adding one to
    # the base table changes the view's columns too, and both are broadcast -
    # while the preceding property-only change on the view is not.
    await cql.run_async("ALTER MATERIALIZED VIEW schema_events.mv1 WITH comment = 'not client visible'")
    await cql.run_async("ALTER TABLE schema_events.table1 ADD value2 int")
    await wait_for_table_events(events, "schema_events", "table1", 1)
    await wait_for_table_events(events, "schema_events", "mv1", 1)


async def test_schema_change_events_legacy_broadcast(manager: ManagerClient):
    servers = await manager.servers_add(1, config={"broadcast_schema_change_events_for_all_updates": True})
    cql, _ = await manager.get_ready_cql(servers)
    await create_test_table(cql)

    events = watch_schema_change_events(cql)

    await cql.run_async("ALTER TABLE schema_events.table1 WITH comment = 'not client visible'")
    await wait_for_table_events(events, "schema_events", "table1", 1)
