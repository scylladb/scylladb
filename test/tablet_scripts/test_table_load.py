#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from __future__ import annotations

from argparse import Namespace
from dataclasses import replace

import pytest

from tablets import table_load
from tablets.filters import filter_args
from tablets.filters import get_tablet_filter
from tablets.render_utils import PresentationOptions

from test.tablet_scripts.topology_building import *


def test_build_ranges_uses_cluster_filters() -> None:
    topology = build_topology(
        hosts={
            HOST1_ID: HostSpec("10.0.0.1", "dc1", "rack1"),
            HOST2_ID: HostSpec("10.0.0.2", "dc1", "rack1"),
        },
        tables={TABLE1_ID: ("ks", "tbl")},
        # The first tablet is replicated on both hosts, the second only on host2.
        tablets={TABLE1_ID: [
            (0, {(HOST1_ID, 0): 100, (HOST2_ID, 1): 50}),
            (1 << 63, {(HOST2_ID, 1): 80}),
        ]},
    )

    args = filter_args(host="10.0.0.1")
    ranges = table_load.build_ranges(TABLE1_ID, topology.get_tablet_map(TABLE1_ID), topology,
                                     get_tablet_filter(args, topology))

    assert [(load.last_token, load.tablet_count, load.size, load.replica_sizes, load.replicas) for load in ranges] == [
        (0, 1, 100, [100], [(HOST1_ID, 0)]),
    ]


def test_sort_ranges_supports_requested_keys() -> None:
    ranges = [
        table_load.make_load_range(last_token=30, tablet_count=1, token_frac=0.1, replica_sizes=[80, 20], replicas=[]),
        table_load.make_load_range(last_token=10, tablet_count=1, token_frac=0.3, replica_sizes=[100, 100], replicas=[]),
        table_load.make_load_range(last_token=20, tablet_count=1, token_frac=0.2, replica_sizes=[50], replicas=[]),
    ]

    assert [load.last_token for load in table_load.sort_ranges(ranges, "token")] == [30, 10, 20]
    assert [load.last_token for load in table_load.sort_ranges(ranges, "tokens")] == [10, 20, 30]
    assert [load.last_token for load in table_load.sort_ranges(ranges, "size")] == [10, 30, 20]
    assert [load.last_token for load in table_load.sort_ranges(ranges, "ovc.rep")] == [30, 10, 20]


def test_summarize_rows_handles_no_rows() -> None:
    # Regression: per-replica scaling used to call max() on an empty sequence when
    # every replica was filtered out, raising ValueError.
    summary = table_load.summarize_rows([])

    assert (summary.sizes.count, summary.sizes.total, summary.sizes.avg(), summary.sizes.max_frac()) == (0, 0, 0, 0)


@pytest.mark.parametrize("per_replica, sizes, avg_size, max_frac", [
    pytest.param(True, [100, 200, 100], 400 / 3, 200 / 400, id="per-replica"),
    pytest.param(False, [300, 100], 200, 300 / 400, id="per-range"),
])
def test_build_ranges_splits_per_replica(per_replica: bool, sizes: list[int],
                                         avg_size: float, max_frac: float) -> None:
    """
    A per-replica report measures each replica's size; a range report the replicated size
    of the whole range. Either way the shares are of the same total.
    """
    topology = build_topology(
        hosts={
            HOST1_ID: HostSpec("10.0.0.1", "dc1", "rack1"),
            HOST2_ID: HostSpec("10.0.0.2", "dc1", "rack1"),
        },
        tables={TABLE1_ID: ("ks", "tbl")},
        tablets={TABLE1_ID: [
            (0, {(HOST1_ID, 0): 100, (HOST2_ID, 1): 200}),
            (1 << 63, {(HOST2_ID, 1): 100}),
        ]},
    )

    ranges = table_load.build_ranges(TABLE1_ID, topology.get_tablet_map(TABLE1_ID), topology,
                                     per_replica=per_replica)
    summary = table_load.summarize_rows(ranges)

    assert [row.size for row in ranges] == sizes
    # A split range says which replica it stands for; an unsplit one stands for them all.
    assert [row.replica_idx for row in ranges] == ([0, 1, 0] if per_replica else [None, None])
    assert summary.sizes.total == 400
    assert summary.sizes.avg() == pytest.approx(avg_size)
    assert summary.sizes.max_frac() == pytest.approx(max_frac)


def test_summarize_rows_scales_token_bars_by_the_widest_range() -> None:
    """
    The token bars scale against the widest range shown, however many rows a range takes:
    splitting it over its replicas must not make it count for more.
    """
    wide = table_load.make_load_range(last_token=10, tablet_count=1, token_frac=0.75,
                                      replica_sizes=[100, 200], replicas=[])
    narrow = table_load.make_load_range(last_token=20, tablet_count=1, token_frac=0.25,
                                        replica_sizes=[100], replicas=[])

    summary = table_load.summarize_rows([replace(wide, size=100, replica_idx=0),
                                         replace(wide, size=200, replica_idx=1),
                                         narrow])

    assert summary.max_token_frac() == pytest.approx(0.75)


def per_replica_rows(csv: bool) -> list[list]:
    """
    Detail rows of a two replica tablet, rendered per replica.
    """
    # The replicas sit in different racks, which the per-replica rows report.
    topo = build_topology({
        HOST1_ID: HostSpec("10.0.0.1", "dc1", "rack1"),
        HOST2_ID: HostSpec("10.0.0.2", "dc1", "rack2"),
    })

    load_range = table_load.make_load_range(
        last_token=1234,
        tablet_count=1,
        token_frac=1.0,
        replica_sizes=[100, 50],
        replicas=[(HOST1_ID, 0), (HOST2_ID, 1)],
    )
    args = Namespace(per_replica=True, max_ranges=None, tokens_hist=False)
    options = PresentationOptions(csv=csv)
    shape = table_load.build_shape(args, tablet_count=1)

    # Split per replica, as build_ranges() does under --per-replica.
    rows = [replace(load_range, size=size, replica_idx=idx)
            for idx, size in enumerate(load_range.replica_sizes)]
    summary = table_load.summarize_rows(rows)

    return table_load.build_rows(rows, shape, options, topo, summary)


@pytest.mark.parametrize("csv, last_token, token_pct", [
    pytest.param(True, [1234, 1234], ["100.0000", "100.0000"], id="csv_repeats_the_tablets_columns"),
    pytest.param(False, [1234, None], ["100.0000", None], id="table_leaves_continuation_rows_blank"),
])
def test_per_replica_rows_identify_their_tablet(csv: bool, last_token: list[int | None],
                                                token_pct: list[str | None]) -> None:
    """
    A csv row stands on its own, so it carries the tablet's columns even when the row above
    named the same tablet; summing the token share then counts every replica copy, as
    cluster_load.py does. A rendered table reads them off the row above instead.
    """
    rows = per_replica_rows(csv=csv)

    assert [row[0] for row in rows] == last_token
    assert [row[1] for row in rows] == token_pct        # tokens [%]
