#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from __future__ import annotations

from uuid import UUID

import pytest

from tablets import table_summary
from tablets.filters import filter_args
from tablets.filters import get_tablet_filter
from tablets.filters import select_all
from tablets.render_utils import PresentationOptions
from tablets.stats import StatsAggregator
from tablets.topology import Host
from tablets.topology import Tablet
from tablets.topology import TabletMap
from tablets.topology import Topology


TABLE_ID = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
HOST1_ID = UUID("11111111-1111-1111-1111-111111111111")
HOST2_ID = UUID("22222222-2222-2222-2222-222222222222")


@pytest.fixture
def topology() -> Topology:
    """
    One table over two tablets: the first replicated on both hosts, the second only on host2.
    """
    topo = Topology()
    topo._tables[TABLE_ID] = ("ks", "tbl")
    topo._hosts[HOST1_ID] = Host(id=HOST1_ID, shard_count=2, ip="10.0.0.1", dc="dc1", rack="rack1")
    topo._hosts[HOST2_ID] = Host(id=HOST2_ID, shard_count=2, ip="10.0.0.2", dc="dc1", rack="rack1")
    topo._tablet_maps[TABLE_ID] = TabletMap(
        table=TABLE_ID,
        tablets=[
            Tablet(last_token=0, replicas=[(HOST1_ID, 0), (HOST2_ID, 1)]),
            Tablet(last_token=1 << 63, replicas=[(HOST2_ID, 1)]),
        ],
    )
    topo._tablet_sizes[(TABLE_ID, 0)] = {HOST1_ID: 100, HOST2_ID: 50}
    topo._tablet_sizes[(TABLE_ID, 1 << 63)] = {HOST2_ID: 80}
    return topo


def test_measure_table_uses_cluster_filters(topology: Topology) -> None:
    args = filter_args(host="10.0.0.1")
    accepts_tablet = get_tablet_filter(args, topology)

    row = table_summary.measure_table(topology, TABLE_ID, "ks.tbl", accepts_tablet,
                                      table_summary.SizeMode.REPLICATED)

    # The second tablet has no replica left, so it keeps its place in token space at size 0.
    assert (row.sizes.count, row.sizes.total) == (1, 100)
    assert row.token_space == [100, 0]


def test_measure_table_skips_a_table_the_filter_left_nothing_of(topology: Topology) -> None:
    # No replica sits on shard 5, so nothing of the table is shown and it gets no row.
    accepts_tablet = get_tablet_filter(filter_args(shard=5), topology)

    assert table_summary.measure_table(topology, TABLE_ID, "ks.tbl", accepts_tablet,
                                       table_summary.SizeMode.REPLICATED) is None


def table_row(name: str, sizes: list[int]) -> table_summary.TableRow:
    return table_summary.TableRow(name=name, table_id=TABLE_ID, sizes=StatsAggregator.of(sizes),
                                  token_space=[])


def test_sort_table_rows_supports_requested_keys() -> None:
    even = table_row("ks.even", sizes=[40, 40])
    uneven = table_row("ks.uneven", sizes=[90, 10])
    empty = table_row("ks.empty", sizes=[0, 0])
    rows = [even, uneven, empty]

    assert table_summary.sort_table_rows(rows, "name") == [empty, even, uneven]
    assert table_summary.sort_table_rows(rows, "size") == [uneven, even, empty]
    assert table_summary.sort_table_rows(rows, "tablet.avg") == [uneven, even, empty]
    # ks.empty has no overcommit to report, so it sorts below tables that do.
    assert table_summary.sort_table_rows(rows, "tablet.ovc") == [uneven, even, empty]


def test_build_all_row_aggregates_table_summary() -> None:
    """
    Sizes and counts add up across the tables; overcommit does not, so the worst table's
    is the one ALL reports.
    """
    all_row = table_summary.build_all_row([
        table_row("ks.big", sizes=[75, 25]),
        table_row("ks.small", sizes=[60]),
        # An empty table has no overcommit to report, which must not stand for the worst.
        table_row("ks.empty", sizes=[0]),
    ], PresentationOptions(human_readable=False))

    assert all_row == [
        "ALL",
        "160",          # 100 + 60 + 0
        "100.00",
        None,
        4,              # 2 + 1 + 1 tablets
        "+50.00",       # ks.big's 75 against its average of 50; ks.small has a single tablet
        "40",           # 160 bytes over 4 tablets
        "75",
        None,
    ]


@pytest.mark.parametrize("size_mode, count, total, largest, ovc", [
    # The first tablet holds 100 and 50 bytes on its two replicas, the second 80 on its one.
    # Replicated, that is three replicas of 100, 50 and 80; un-replicated, two tablets of 75
    # and 80, since a tablet's replicas each hold a copy of what it holds once.
    pytest.param(table_summary.SizeMode.REPLICATED, 3, 230, 100, 100 / (230 / 3), id="replicated"),
    pytest.param(table_summary.SizeMode.UNREPLICATED, 2, 155, 80, 80 / (155 / 2), id="un-replicated"),
])
def test_measure_table_measures_the_size_mode_s_unit(
        topology: Topology, size_mode: table_summary.SizeMode,
        count: int, total: float, largest: float, ovc: float) -> None:
    """
    A replicated report sums all tablet replicas, an un-replicated one takes the average
    tablet replica size for a given tablet, and each counts what it sizes. Overcommit
    follows, so a replicated report also sees replicas of one tablet differing in size.
    """
    row = table_summary.measure_table(topology, TABLE_ID, "ks.tbl", select_all, size_mode)

    assert row.sizes.count == count
    assert row.sizes.total == total
    assert row.sizes.max == largest
    assert row.sizes.ovc() == pytest.approx(ovc)
    # The token space is un-replicated whichever unit the sizes are of, so that its shape is
    # that of the data rather than of its placement.
    assert row.token_space == [75, 80]
