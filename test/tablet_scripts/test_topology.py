#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from __future__ import annotations

from uuid import UUID

import pytest

from tablets.topology import Anonymizer
from tablets.topology import TOKEN_RING_SIZE
from tablets.topology import Tablet
from tablets.topology import TabletMap
from tablets.topology import Topology
from tablets.topology import iter_token_fractions
from tablets.topology import resolve_table_id


TABLE1_ID = UUID("11111111-1111-1111-1111-111111111111")
TABLE2_ID = UUID("22222222-2222-2222-2222-222222222222")
TABLE3_ID = UUID("33333333-3333-3333-3333-333333333333")


def test_anonymize_table_names_replaces_names_with_deterministic_unique_aliases() -> None:
    topo = Topology()
    anonymizer = Anonymizer()

    topo._tables[TABLE1_ID] = ("customer_a", "users")
    topo._tables[TABLE2_ID] = ("customer_a", "orders")
    topo._tables[TABLE3_ID] = ("customer_b", "users")

    topo.set_anonymizer(anonymizer)

    assert topo.get_table_name(TABLE2_ID) == "ks1.table1"
    assert topo.get_table_name(TABLE1_ID) == "ks1.table2"
    assert topo.get_table_name(TABLE3_ID) == "ks2.table1"
    assert len({topo.get_table_name(TABLE1_ID), topo.get_table_name(TABLE2_ID), topo.get_table_name(TABLE3_ID)}) == 3
    assert topo._tables[TABLE1_ID] == ("customer_a", "users")
    assert topo._tables[TABLE2_ID] == ("customer_a", "orders")
    assert topo._tables[TABLE3_ID] == ("customer_b", "users")


def test_anonymize_table_names_is_idempotent() -> None:
    topo = Topology()
    anonymizer = Anonymizer()
    topo._tables[TABLE1_ID] = ("customer_a", "users")

    topo.set_anonymizer(anonymizer)
    first_name = topo.get_table_name(TABLE1_ID)
    topo.set_anonymizer(anonymizer)

    assert topo.get_table_name(TABLE1_ID) == first_name


def test_anonymize_table_names_only_extends_with_new_tables() -> None:
    topo = Topology()
    anonymizer = Anonymizer()

    topo._tables[TABLE1_ID] = ("customer_a", "users")
    topo.set_anonymizer(anonymizer)

    assert topo.get_table_name(TABLE1_ID) == "ks1.table1"

    topo._tables[TABLE2_ID] = ("customer_a", "orders")
    topo.set_anonymizer(anonymizer)

    assert topo.get_table_name(TABLE1_ID) == "ks1.table1"
    assert topo.get_table_name(TABLE2_ID) == "ks1.table2"


def test_resolve_table_id_accepts_unique_bare_table_name() -> None:
    topo = Topology()
    topo._tables[TABLE1_ID] = ("ks1", "users")
    topo._tables[TABLE2_ID] = ("ks2", "orders")
    topo._tablet_maps[TABLE1_ID] = TabletMap(TABLE1_ID)
    topo._tablet_maps[TABLE2_ID] = TabletMap(TABLE2_ID)

    assert resolve_table_id(topo, "users") == TABLE1_ID
    assert resolve_table_id(topo, "ks1.users") == TABLE1_ID


def test_resolve_table_id_rejects_ambiguous_bare_table_name() -> None:
    topo = Topology()
    topo._tables[TABLE1_ID] = ("ks1", "users")
    topo._tables[TABLE2_ID] = ("ks2", "users")
    topo._tablet_maps[TABLE1_ID] = TabletMap(TABLE1_ID)
    topo._tablet_maps[TABLE2_ID] = TabletMap(TABLE2_ID)

    with pytest.raises(Exception, match=r"^Ambiguous table name: users$"):
        resolve_table_id(topo, "users")


def test_a_lone_tablet_owns_the_whole_token_ring() -> None:
    """
    A single tablet wraps around to itself, which the span arithmetic reads as owning
    nothing. Every table of a cluster can look like that, hiding all token load.
    """
    tablet = Tablet(last_token=1234, replicas=[])

    assert list(iter_token_fractions([tablet])) == [(tablet, 1.0)]


def test_token_fractions_span_the_ring_and_wrap_at_the_last_tablet() -> None:
    quarter = TOKEN_RING_SIZE // 4
    # Boundaries are the tablets' last tokens, the first spanning the wrap from the last.
    tablets = [Tablet(last_token=-(TOKEN_RING_SIZE // 2) + quarter * i, replicas=[]) for i in range(1, 5)]

    fractions = [fraction for _, fraction in iter_token_fractions(tablets)]

    assert fractions == [0.25, 0.25, 0.25, 0.25]
    assert sum(fractions) == 1.0


def test_no_tablets_yields_no_token_fractions() -> None:
    assert list(iter_token_fractions([])) == []
