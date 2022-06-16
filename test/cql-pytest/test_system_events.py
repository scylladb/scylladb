# -*- coding: utf-8 -*-
# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import pytest
import util

def get_system_events(cql):
    events = dict()
    rows = list(cql.execute("SELECT category, started_at, type, params FROM system.events"))
    assert(len(rows) > 0)
    for row in rows:
        if not row.category in events:
            events[row.category] = dict()
        if not row.type in events[row.category]:
            events[row.category][row.type] = dict()
        events[row.category][row.type][row.started_at] = row.params
    return events

# Check reading the system.events table, which should list major system events
# like start/stop.
def test_system_events(scylla_only, cql, this_dc):
    events = get_system_events(cql)
    assert len(events['system']['start']) > 0

    test_keyspace = ""
    with util.new_test_keyspace(cql, f"WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : 1 }}") as test_keyspace:
        events = get_system_events(cql)
        keyspace_found = False
        search_str = f"keyspace_name={test_keyspace}"
        for e in events['database']['create_keyspace'].values():
            keyspace_found = search_str in e
        assert keyspace_found, f"'{test_keyspace}' not found in: {events['database']['create_keyspace']}"

    events = get_system_events(cql)
    keyspace_found = False
    search_str = f"keyspace_name={test_keyspace}"
    for e in events['database']['drop_keyspace'].values():
        keyspace_found = search_str in e
    assert keyspace_found, f"'{test_keyspace}' not found in: {events['database']['drop_keyspace']}"
