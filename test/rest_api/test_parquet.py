# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Tests for the two Parquet REST endpoints:
#
#   GET /column_family/storage_format/{keyspace:table}
#       what a table's SSTables are actually encoded as, right now
#   GET /storage_service/estimate_parquet_ratios?keyspace=&cf=&rows=
#       a sampled prediction of how much smaller Parquet would be
#
# See docs/dev/parquet-storage-format.md sections 8.4 and 8.6.
#############################################################################

import pytest
import requests

from ..cqlpy.util import new_test_table


def as_dict(resp):
    """Both /column_family endpoints return the generic "mapper" model: a JSON
    array of {"key": ..., "value": ...} objects, with every value a string."""
    resp.raise_for_status()
    return {e['key']: e['value'] for e in resp.json()}


def storage_format(rest_api, table):
    keyspace, name = table.split('.')
    return as_dict(rest_api.send(
        "GET", f"column_family/storage_format/{keyspace}:{name}"))


def flush(rest_api, table):
    keyspace, name = table.split('.')
    resp = rest_api.send("POST", f"storage_service/keyspace_flush/{keyspace}",
                         {"cf": name})
    resp.raise_for_status()


# The breakdown reports six keys, and the SSTable counts and byte totals must
# follow the table's storage_format. `converged` is the summary a user acts on,
# so its exact vocabulary is pinned: "native", "parquet" or "mixed".
def test_storage_format_breakdown(cql, rest_api, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int") as table:
        # An empty table has no SSTables of either kind. It reports "native"
        # rather than "mixed" or an error -- worth pinning, because "no
        # Parquet yet" and "converged to native" are the same answer here.
        b = storage_format(rest_api, table)
        assert set(b) == {'native_bytes', 'parquet_bytes', 'native_sstables',
                          'parquet_sstables', 'parquet_fraction', 'converged'}
        assert b['native_sstables'] == '0' and b['parquet_sstables'] == '0'
        assert b['converged'] == 'native'
        assert b['parquet_fraction'] == '0'

        # A native flush moves the native counters only.
        for i in range(20):
            cql.execute(f"INSERT INTO {table} (pk, v) VALUES ({i}, {i})")
        flush(rest_api, table)
        b = storage_format(rest_api, table)
        assert int(b['native_sstables']) > 0
        assert b['parquet_sstables'] == '0'
        assert int(b['native_bytes']) > 0
        assert b['converged'] == 'native'

        # After an ALTER, the next flush is Parquet, so the table is genuinely
        # format-mixed and must be reported as such.
        cql.execute(f"ALTER TABLE {table} WITH storage_format = 'parquet'")
        for i in range(20, 40):
            cql.execute(f"INSERT INTO {table} (pk, v) VALUES ({i}, {i})")
        flush(rest_api, table)
        b = storage_format(rest_api, table)
        assert int(b['parquet_sstables']) > 0
        assert int(b['native_sstables']) > 0
        assert b['converged'] == 'mixed'
        assert 0.0 < float(b['parquet_fraction']) < 1.0

        # A major compaction rewrites everything in the table's format, so the
        # table converges and the mixed state disappears.
        resp = rest_api.send("POST",
                             f"storage_service/keyspace_compaction/{test_keyspace}",
                             {"cf": table.split('.')[1]})
        resp.raise_for_status()
        b = storage_format(rest_api, table)
        assert b['native_sstables'] == '0'
        assert int(b['parquet_sstables']) > 0
        assert b['converged'] == 'parquet'
        assert float(b['parquet_fraction']) == 1.0


# A table whose SSTables are all Parquet from the start reports converged
# immediately, with no native bytes at all.
def test_storage_format_breakdown_all_parquet(cql, rest_api, test_keyspace,
                                              scylla_only):
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int",
                        " WITH storage_format = 'parquet'") as table:
        for i in range(20):
            cql.execute(f"INSERT INTO {table} (pk, v) VALUES ({i}, {i})")
        flush(rest_api, table)
        b = storage_format(rest_api, table)
        assert b['native_sstables'] == '0'
        assert int(b['parquet_sstables']) > 0
        assert b['native_bytes'] == '0'
        assert int(b['parquet_bytes']) > 0
        assert b['converged'] == 'parquet'
        assert float(b['parquet_fraction']) == 1.0


# The malformed-name and missing-table cases, matching the conventions the rest
# of /column_family follows.
def test_storage_format_breakdown_bad_name(cql, rest_api, test_keyspace,
                                           scylla_only):
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int") as table:
        name = table.split('.')[1]

        resp = rest_api.send("GET", "column_family/storage_format/")
        assert resp.status_code == requests.codes.not_found

        # A dot instead of a colon is the classic mistake.
        resp = rest_api.send("GET", f"column_family/storage_format/{test_keyspace}.{name}")
        assert resp.status_code == requests.codes.bad_request

        resp = rest_api.send("GET", f"column_family/storage_format/{test_keyspace}:{name}XXX")
        assert resp.status_code == requests.codes.bad_request


# The estimator samples real rows out of a real SSTable and reports, for each
# of the three folding levels, how many bytes Parquet and the row format would
# take. It needs something on disk to sample.
def test_estimate_parquet_ratios(cql, rest_api, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace,
                        "pk int, ck int, v1 int, v2 text, PRIMARY KEY (pk, ck)") as table:
        name = table.split('.')[1]

        # Before any flush there is nothing to sample, and the endpoint says so
        # rather than inventing a ratio.
        resp = rest_api.send("GET", "storage_service/estimate_parquet_ratios",
                             {"keyspace": test_keyspace, "cf": name})
        assert resp.status_code == requests.codes.internal_server_error
        assert "no SSTables to sample" in resp.text

        for pk in range(50):
            for ck in range(10):
                cql.execute(f"INSERT INTO {table} (pk, ck, v1, v2) VALUES "
                            f"({pk}, {ck}, {pk * ck}, 'row-{pk}-{ck}')")
        flush(rest_api, table)

        resp = rest_api.send("GET", "storage_service/estimate_parquet_ratios",
                             {"keyspace": test_keyspace, "cf": name})
        resp.raise_for_status()
        results = resp.json()

        # Exactly one result per folding level, in order.
        assert [r['folding_level'] for r in results] == ['L0', 'L1', 'L2']
        for r in results:
            assert r['rows_sampled'] > 0
            assert r['parquet_bytes'] > 0
            assert r['sstable_bytes'] > 0
        # The endpoint samples ONE SSTable, and a flush writes one SSTable per
        # shard, so `rows_sampled` is a per-shard share of the 500 rows written
        # -- not 500. Asserting a specific number here would be a
        # shard-count-dependent flake.
        assert all(0 < r['rows_sampled'] <= 500 for r in results)

        # 500 rows is well under the default 20000-row cap, so whichever
        # SSTable was picked was sampled in full: `rows_sampled` equals that
        # SSTable's own row count. Only in that case does the reported ratio
        # reduce to the quotient of the two byte columns, and that identity
        # holds however the rows are spread across shards.
        for r in results:
            assert r['ratio'] == pytest.approx(
                r['parquet_bytes'] / r['sstable_bytes'], rel=1e-3)

        # `rows` caps the sample. Asking for very few rows must actually sample
        # fewer -- otherwise the parameter is decorative and a user cannot
        # bound the cost of the estimate.
        resp = rest_api.send("GET", "storage_service/estimate_parquet_ratios",
                             {"keyspace": test_keyspace, "cf": name, "rows": 10})
        resp.raise_for_status()
        capped = resp.json()
        assert [r['folding_level'] for r in capped] == ['L0', 'L1', 'L2']
        for r in capped:
            assert 0 < r['rows_sampled'] <= 10
        assert capped[0]['rows_sampled'] < results[0]['rows_sampled']

        # NOT asserted: that the capped estimate's `ratio` is close to the full
        # one's. It is tempting -- the endpoint normalises per row on both
        # sides specifically so that a small sample still predicts the whole
        # table, and re-breaking that normalisation is a real regression this
        # would seem to catch. It does not work at a 10-row sample. A Parquet
        # image carries a fixed footer and per-column-chunk overhead that does
        # not shrink with the row count, so at 10 rows the per-row Parquet cost
        # is dominated by that constant and the ratio is legitimately several
        # times the full-sample one. The band needed to accommodate that is
        # wider than the error it would detect, which makes the assertion
        # either flaky or vacuous depending on which way it is tuned. Left out
        # deliberately rather than tuned into something that cannot fail; the
        # per-row normalisation is asserted in the C++ tests instead.
        #
        # (Two forms of it were tried against a build with the normalisation
        # deliberately re-broken. `pytest.approx(rel=2.0)` passed because a
        # relative tolerance admits everything down to zero. An explicit
        # two-sided [full/3, full*3] band also passed, which is what exposed
        # the fixed-overhead effect above: the sample's ratio does not move the
        # way the naive model predicts.)


# A keyspace or table that does not exist must be an error, not an empty
# estimate that reads as "Parquet would save nothing".
def test_estimate_parquet_ratios_missing_table(cql, rest_api, test_keyspace,
                                               scylla_only):
    resp = rest_api.send("GET", "storage_service/estimate_parquet_ratios",
                         {"keyspace": test_keyspace, "cf": "nosuchtable"})
    assert resp.status_code != requests.codes.ok
