#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from __future__ import annotations

from uuid import UUID

from tablets.topology import Anonymizer
from tablets.topology import TabletMap
from tablets.topology import Topology
from tablets.topology import resolve_table_id


def test_anonymize_table_names_replaces_names_with_deterministic_unique_aliases() -> None:
    topo = Topology()
    anonymizer = Anonymizer()
    table1 = UUID("11111111-1111-1111-1111-111111111111")
    table2 = UUID("22222222-2222-2222-2222-222222222222")
    table3 = UUID("33333333-3333-3333-3333-333333333333")

    topo._tables[table1] = ("customer_a", "users")
    topo._tables[table2] = ("customer_a", "orders")
    topo._tables[table3] = ("customer_b", "users")

    topo.set_anonymizer(anonymizer)

    assert topo.get_table_name(table2) == "ks1.table1"
    assert topo.get_table_name(table1) == "ks1.table2"
    assert topo.get_table_name(table3) == "ks2.table1"
    assert len({topo.get_table_name(table1), topo.get_table_name(table2), topo.get_table_name(table3)}) == 3
    assert topo._tables[table1] == ("customer_a", "users")
    assert topo._tables[table2] == ("customer_a", "orders")
    assert topo._tables[table3] == ("customer_b", "users")


def test_anonymize_table_names_is_idempotent() -> None:
    topo = Topology()
    anonymizer = Anonymizer()
    table1 = UUID("11111111-1111-1111-1111-111111111111")
    topo._tables[table1] = ("customer_a", "users")

    topo.set_anonymizer(anonymizer)
    first_name = topo.get_table_name(table1)
    topo.set_anonymizer(anonymizer)

    assert topo.get_table_name(table1) == first_name


def test_anonymize_table_names_only_extends_with_new_tables() -> None:
    topo = Topology()
    anonymizer = Anonymizer()
    table1 = UUID("11111111-1111-1111-1111-111111111111")
    table2 = UUID("22222222-2222-2222-2222-222222222222")

    topo._tables[table1] = ("customer_a", "users")
    topo.set_anonymizer(anonymizer)

    assert topo.get_table_name(table1) == "ks1.table1"

    topo._tables[table2] = ("customer_a", "orders")
    topo.set_anonymizer(anonymizer)

    assert topo.get_table_name(table1) == "ks1.table1"
    assert topo.get_table_name(table2) == "ks1.table2"


def test_resolve_table_id_accepts_unique_bare_table_name() -> None:
    topo = Topology()
    table1 = UUID("11111111-1111-1111-1111-111111111111")
    table2 = UUID("22222222-2222-2222-2222-222222222222")
    topo._tables[table1] = ("ks1", "users")
    topo._tables[table2] = ("ks2", "orders")
    topo._tablet_maps[table1] = TabletMap(table1)
    topo._tablet_maps[table2] = TabletMap(table2)

    assert resolve_table_id(topo, "users") == table1
    assert resolve_table_id(topo, "ks1.users") == table1


def test_resolve_table_id_rejects_ambiguous_bare_table_name() -> None:
    topo = Topology()
    table1 = UUID("11111111-1111-1111-1111-111111111111")
    table2 = UUID("22222222-2222-2222-2222-222222222222")
    topo._tables[table1] = ("ks1", "users")
    topo._tables[table2] = ("ks2", "users")
    topo._tablet_maps[table1] = TabletMap(table1)
    topo._tablet_maps[table2] = TabletMap(table2)

    try:
        resolve_table_id(topo, "users")
    except Exception as exc:
        assert str(exc) == "Ambiguous table name: users"
    else:
        assert False, "Expected ambiguous bare table name to fail"
