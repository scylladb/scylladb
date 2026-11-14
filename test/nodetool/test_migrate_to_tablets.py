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
        "direction": "tablets",
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
        f"{HOST1} 10.0.0.1 Restarted",
        f"{HOST2} 10.0.0.2 Needs restart",
    ]


def test_status_empty_address(nodetool):
    """The server reports an empty string when the address map doesn't know the node."""
    response = {
        "keyspace": "ks",
        "status": "migrating_to_tablets",
        "direction": "tablets",
        "nodes": [
            {"host_id": HOST1, "endpoint": "",
             "current_mode": "vnodes", "intended_mode": "vnodes"},
        ],
    }
    res = nodetool("migrate-to-tablets", "status", "ks", expected_requests=[
        expected_request("GET", "/storage_service/vnode_tablet_migrations/keyspaces/ks",
                         response=response)])

    assert f"{HOST1} ? Restarted" in _status_lines(res)


def test_start_whole_keyspace(nodetool):
    nodetool("migrate-to-tablets", "start", "ks", expected_requests=[
        expected_request("POST", "/storage_service/vnode_tablet_migrations/keyspaces/ks")])


def test_start_single_table(nodetool):
    nodetool("migrate-to-tablets", "start", "ks.t1", expected_requests=[
        expected_request("POST", "/storage_service/vnode_tablet_migrations/keyspaces/ks",
                         params={"table": "t1"})])


def test_status_lists_migrating_tables(nodetool):
    response = {
        "keyspace": "ks",
        "status": "migrating_to_tablets",
        "direction": "tablets",
        "nodes": [
            {"host_id": HOST1, "endpoint": "10.0.0.1",
             "current_mode": "tablets", "intended_mode": "tablets"},
            {"host_id": HOST2, "endpoint": "10.0.0.2",
             "current_mode": "vnodes", "intended_mode": "tablets"},
        ],
        "tables": [
            {"table": "t1", "prepared": True, "finalized": True},
            {"table": "t2", "prepared": True, "finalized": False},
            {"table": "t3", "prepared": False, "finalized": False},
        ],
    }
    res = nodetool("migrate-to-tablets", "status", "ks", expected_requests=[
        expected_request("GET", "/storage_service/vnode_tablet_migrations/keyspaces/ks",
                         response=response)])

    lines = _status_lines(res)
    # t1 is finalized and t3 was never started, so only t2 is migrating.
    assert "Migrating Tables: t2" in lines
    assert "Direction: tablets" in lines
    assert f"{HOST1} 10.0.0.1 Restarted" in lines
    assert f"{HOST2} 10.0.0.2 Needs restart" in lines


def test_status_lists_all_when_whole_keyspace_migrates(nodetool):
    response = {
        "keyspace": "ks",
        "status": "migrating_to_tablets",
        "direction": "tablets",
        "nodes": [
            {"host_id": HOST1, "endpoint": "10.0.0.1",
             "current_mode": "vnodes", "intended_mode": "tablets"},
        ],
        "tables": [
            {"table": "t1", "prepared": True, "finalized": False},
            {"table": "t2", "prepared": True, "finalized": False},
        ],
    }
    res = nodetool("migrate-to-tablets", "status", "ks", expected_requests=[
        expected_request("GET", "/storage_service/vnode_tablet_migrations/keyspaces/ks",
                         response=response)])

    assert "Migrating Tables: All" in _status_lines(res)


def test_status_omits_the_line_when_nothing_migrates(nodetool):
    response = {
        "keyspace": "ks",
        "status": "vnodes",
        "nodes": [],
        "tables": [
            {"table": "t1", "prepared": True, "finalized": True},
            {"table": "t2", "prepared": False, "finalized": False},
        ],
    }
    res = nodetool("migrate-to-tablets", "status", "ks", expected_requests=[
        expected_request("GET", "/storage_service/vnode_tablet_migrations/keyspaces/ks",
                         response=response)])

    assert not any(l.startswith("Migrating Tables:") for l in _status_lines(res))


def test_status_without_tables_field(nodetool):
    """A keyspace that is not migrating has no per-table section."""
    response = {
        "keyspace": "ks",
        "status": "vnodes",
        "nodes": [
            {"host_id": HOST1, "endpoint": "10.0.0.1",
             "current_mode": "vnodes", "intended_mode": "vnodes"},
        ],
    }
    res = nodetool("migrate-to-tablets", "status", "ks", expected_requests=[
        expected_request("GET", "/storage_service/vnode_tablet_migrations/keyspaces/ks",
                         response=response)])

    assert "Tables:" not in _status_lines(res)


def test_finalize_whole_keyspace(nodetool):
    nodetool("migrate-to-tablets", "finalize", "ks", expected_requests=[
        expected_request("POST", "/storage_service/vnode_tablet_migrations/keyspaces/ks/finalization")])


def test_finalize_single_table(nodetool):
    nodetool("migrate-to-tablets", "finalize", "ks.t1", expected_requests=[
        expected_request("POST", "/storage_service/vnode_tablet_migrations/keyspaces/ks/finalization",
                         params={"table": "t1"})])


def test_start_rejects_a_bare_dot(nodetool):
    res = nodetool("migrate-to-tablets", "start", "ks.", expected_requests=[], check_return_code=False)
    assert "expected <keyspace> or <keyspace>.<table>" in res.stderr


def test_status_direction_on_rollback(nodetool):
    """Rolling back, the keyspace status still reads migrating_to_tablets, so the
    direction is what tells the operator which way it is going."""
    response = {
        "keyspace": "ks",
        "status": "migrating_to_tablets",
        "direction": "vnodes",
        "nodes": [
            {"host_id": HOST1, "endpoint": "10.0.0.1",
             "current_mode": "tablets", "intended_mode": "vnodes"},
        ],
        "tables": [
            {"table": "t1", "prepared": True, "finalized": False},
        ],
    }
    res = nodetool("migrate-to-tablets", "status", "ks", expected_requests=[
        expected_request("GET", "/storage_service/vnode_tablet_migrations/keyspaces/ks",
                         response=response)])

    lines = _status_lines(res)
    assert "Direction: vnodes" in lines
    assert f"{HOST1} 10.0.0.1 Needs restart" in lines


def test_status_tablet_info_for_finalized_table_in_vnode_keyspace(nodetool):
    """A finalized table converges while its keyspace still uses vnodes.

    The keyspace's own status says 'vnodes' there, so the client has to go by
    whether the server reported a tablet section rather than by the keyspace.
    """
    response = {
        "keyspace": "ks",
        "status": "vnodes",
        "nodes": [],
        "tables": [
            {"table": "t1", "prepared": True, "finalized": True},
            {"table": "t2", "prepared": False, "finalized": False},
        ],
        "tablets": {
            "pow2_convergence": {
                "status": "in_progress",
                "tables_converging": 1,
                "tables_total": 1,
                "tables": [
                    {"table": "t1", "converging": True,
                     "current_tablet_count": 6, "target_pow2_tablet_count": 4},
                ],
            },
        },
    }
    res = nodetool("migrate-to-tablets", "status", "ks", "--with-tablet-status",
                   expected_requests=[
                       expected_request("GET", "/storage_service/vnode_tablet_migrations/keyspaces/ks",
                                        params={"include": "tablet_status"},
                                        response=response)])

    lines = _status_lines(res)
    assert "Pow2 tablet layout convergence: in_progress" in lines
    assert not any("not applicable" in l for l in lines)


def test_status_tablet_info_not_applicable_without_tablets(nodetool):
    """Nothing migrated yet, so there is no tablet section to report."""
    response = {
        "keyspace": "ks",
        "status": "vnodes",
        "nodes": [],
        "tables": [{"table": "t1", "prepared": False, "finalized": False}],
    }
    res = nodetool("migrate-to-tablets", "status", "ks", "--with-tablet-status",
                   expected_requests=[
                       expected_request("GET", "/storage_service/vnode_tablet_migrations/keyspaces/ks",
                                        params={"include": "tablet_status"},
                                        response=response)])

    assert any("not applicable" in l for l in _status_lines(res))
