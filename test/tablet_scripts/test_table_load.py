#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from __future__ import annotations

from argparse import Namespace
from uuid import UUID

import pytest

from conftest import load_tablet_script
from tablets.render_utils import PresentationOptions
from tablets.topology import Host
from tablets.topology import Tablet
from tablets.topology import TabletMap
from tablets.topology import Topology


table_load = load_tablet_script("table-load")


def test_build_ranges_uses_cluster_filters() -> None:
    table_id = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
    host1_id = UUID("11111111-1111-1111-1111-111111111111")
    host2_id = UUID("22222222-2222-2222-2222-222222222222")
    topo = Topology()
    topo._tables[table_id] = ("ks", "tbl")
    topo._hosts[host1_id] = Host(id=host1_id, shard_count=2, ip="10.0.0.1", dc="dc1", rack="rack1")
    topo._hosts[host2_id] = Host(id=host2_id, shard_count=2, ip="10.0.0.2", dc="dc1", rack="rack1")
    tablet_map = TabletMap(
        table=table_id,
        tablets=[
            Tablet(last_token=0, replicas=[(host1_id, 0), (host2_id, 1)]),
            Tablet(last_token=1 << 63, replicas=[(host2_id, 1)]),
        ],
    )
    topo._tablet_maps[table_id] = tablet_map
    topo._tablet_sizes[(table_id, 0)] = {host1_id: 100, host2_id: 50}
    topo._tablet_sizes[(table_id, 1 << 63)] = {host2_id: 80}

    args = Namespace(host="10.0.0.1", shard=None, rack=None, dc=None)
    ranges = table_load.build_ranges(table_id, tablet_map, topo, args)

    assert [(load.last_token, load.tablet_count, load.size, load.replica_sizes, load.replicas) for load in ranges] == [
        (0, 1, 100, [100], [(host1_id, 0)]),
    ]


def test_sort_ranges_supports_requested_keys() -> None:
    ranges = [
        table_load.TabletLoadRange(last_token=30, tablet_count=1, size=100, token_frac=0.1, replica_sizes=[80, 20], replicas=[]),
        table_load.TabletLoadRange(last_token=10, tablet_count=1, size=200, token_frac=0.3, replica_sizes=[100, 100], replicas=[]),
        table_load.TabletLoadRange(last_token=20, tablet_count=1, size=50, token_frac=0.2, replica_sizes=[50], replicas=[]),
    ]

    assert [load.last_token for load in table_load.sort_ranges(ranges, "last-token")] == [30, 10, 20]
    assert [load.last_token for load in table_load.sort_ranges(ranges, "tokens")] == [10, 20, 30]
    assert [load.last_token for load in table_load.sort_ranges(ranges, "size")] == [10, 30, 20]
    assert [load.last_token for load in table_load.sort_ranges(ranges, "ovc.rep")] == [30, 10, 20]


def test_compute_printed_scales_handles_empty_ranges() -> None:
    # Regression: per-replica scaling used to call max() on an empty sequence when
    # every replica was filtered out, raising ValueError.
    for per_replica in (True, False):
        assert table_load.compute_printed_scales([], per_replica, 0) == ([], 0, 0)


def test_compute_printed_scales_per_replica_vs_range() -> None:
    ranges = [
        table_load.TabletLoadRange(last_token=10, tablet_count=1, size=300, token_frac=0.5, replica_sizes=[100, 200], replicas=[]),
        table_load.TabletLoadRange(last_token=20, tablet_count=1, size=100, token_frac=0.5, replica_sizes=[100], replicas=[]),
    ]
    total_size = 400

    printed_sizes, avg_printed_size, max_frac = table_load.compute_printed_scales(ranges, True, total_size)
    assert printed_sizes == [100, 200, 100]
    assert avg_printed_size == pytest.approx(400 / 3)
    assert max_frac == pytest.approx(200 / 400)

    printed_sizes, avg_printed_size, max_frac = table_load.compute_printed_scales(ranges, False, total_size)
    assert printed_sizes == [300, 100]
    assert avg_printed_size == pytest.approx(200)
    assert max_frac == pytest.approx(300 / 400)


def per_replica_rows(csv: bool) -> list[list]:
    """
    Detail rows of a two replica tablet, rendered per replica.
    """
    table_id = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
    host1_id = UUID("11111111-1111-1111-1111-111111111111")
    host2_id = UUID("22222222-2222-2222-2222-222222222222")
    topo = Topology()
    topo._tables[table_id] = ("ks", "tbl")
    topo._hosts[host1_id] = Host(id=host1_id, shard_count=2, ip="10.0.0.1", dc="dc1", rack="rack1")
    topo._hosts[host2_id] = Host(id=host2_id, shard_count=2, ip="10.0.0.2", dc="dc1", rack="rack2")

    load_range = table_load.TabletLoadRange(
        last_token=1234,
        tablet_count=1,
        token_frac=1.0,
        size=150,
        replica_sizes=[100, 50],
        replicas=[(host1_id, 0), (host2_id, 1)],
    )
    args = Namespace(per_replica=True, max_ranges=None, tokens_hist=False)
    options = PresentationOptions(csv=csv)
    shape = table_load.build_shape(args, tablet_count=1)

    return table_load.build_detail_rows([load_range], shape, args.per_replica, options, topo,
                                        total_size=150, max_token_frac=1.0, avg_printed_size=75,
                                        max_frac=1.0)


def test_per_replica_csv_repeats_only_the_last_token() -> None:
    """
    A csv row identifies its tablet, so the token is not left to the row above it. The tablet's
    measures are not repeated: summing such a column has to stay correct.
    """
    rows = per_replica_rows(csv=True)

    assert [row[0] for row in rows] == [1234, 1234]              # last token
    assert [row[1] for row in rows] == ["100.0000", None]        # tokens [%]


def test_per_replica_table_leaves_continuation_rows_blank() -> None:
    rows = per_replica_rows(csv=False)

    assert [row[0] for row in rows] == [1234, None]
    assert [row[1] for row in rows] == ["100.0000", None]
