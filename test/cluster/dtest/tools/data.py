#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import logging

from cassandra import ConsistencyLevel
from cassandra.concurrent import execute_concurrent_with_args

from test.cluster.dtest.dtest_class import create_cf


logger = logging.getLogger(__name__)


def create_c1c2_table(session, cf="cf", read_repair=None, debug_query=True, compaction=None, caching=True, speculative_retry=None):  # noqa: PLR0913
    create_cf(session, cf, columns={"c1": "text", "c2": "text"}, read_repair=read_repair, debug_query=debug_query, compaction=compaction, caching=caching, speculative_retry=speculative_retry)


def insert_c1c2(  # noqa: PLR0913
    session,
    keys=None,
    n=None,
    consistency=ConsistencyLevel.QUORUM,
    c1_values=None,
    c2_values=None,
    ks="ks",
    cf="cf",
    concurrency=20,
):
    if (keys is None and n is None) or (keys is not None and n is not None):
        raise ValueError(f"Expected exactly one of 'keys' or 'n' arguments to not be None; got keys={keys}, n={n}")
    if (not c1_values and c2_values) or (c1_values and not c2_values):
        raise ValueError('Expected the "c1_values" and "c2_values" variables be empty or contain list of string')
    if n:
        keys = list(range(n))
    if c1_values and c2_values:
        statement = session.prepare(f"INSERT INTO {ks}.{cf} (key, c1, c2) VALUES (?, ?, ?)")
        statement.consistency_level = consistency
        execute_concurrent_with_args(session, statement, map(lambda x, y, z: [f"k{x}", y, z], keys, c1_values, c2_values), concurrency=concurrency)
    else:
        statement = session.prepare(f"INSERT INTO {ks}.{cf} (key, c1, c2) VALUES (?, 'value1', 'value2')")
        statement.consistency_level = consistency

        execute_concurrent_with_args(session, statement, [[f"k{k}"] for k in keys], concurrency=concurrency)
