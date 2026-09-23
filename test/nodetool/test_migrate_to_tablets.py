#
# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from test.nodetool.rest_api_mock import expected_request

HOST1 = "99d8de76-3954-4727-911a-6a07251b180c"
HOST2 = "0b5fd6f6-9670-4faf-a480-ad58cf119007"


def _status_lines(res):
    """Tabulate pads every column, so compare with runs of spaces collapsed."""
    return [" ".join(line.split()) for line in res.stdout.splitlines()]


def test_status_shows_addresses(nodetool):
    response = {
        "keyspace": "ks",
        "status": "migrating_to_tablets",
        "nodes": [
            {"host_id": HOST1, "endpoint": "10.0.0.1",
             "current_mode": "tablets", "intended_mode": "tablets"},
            {"host_id": HOST2, "endpoint": "10.0.0.2",
             "current_mode": "vnodes", "intended_mode": "tablets"},
        ],
    }
    res = nodetool("migrate-to-tablets", "status", "ks", expected_requests=[
        expected_request("GET", "/storage_service/vnode_tablet_migrations/keyspaces/ks",
                         response=response)])

    assert _status_lines(res) == [
        "Keyspace: ks",
        "Status: migrating_to_tablets",
        "",
        "Nodes:",
        "Host ID Address Status",
        f"{HOST1} 10.0.0.1 uses tablets",
        f"{HOST2} 10.0.0.2 migrating to tablets",
    ]


def test_status_empty_address(nodetool):
    """The server reports an empty string when the address map doesn't know the node."""
    response = {
        "keyspace": "ks",
        "status": "migrating_to_tablets",
        "nodes": [
            {"host_id": HOST1, "endpoint": "",
             "current_mode": "vnodes", "intended_mode": "vnodes"},
        ],
    }
    res = nodetool("migrate-to-tablets", "status", "ks", expected_requests=[
        expected_request("GET", "/storage_service/vnode_tablet_migrations/keyspaces/ks",
                         response=response)])

    assert f"{HOST1} ? uses vnodes" in _status_lines(res)
