#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import pytest


def test_shardof(scylla_types):
    res = scylla_types("shardof", "--full-compound", "-t", "UTF8Type", "-t", "SimpleDateType", "-t", "UUIDType", "--shards=7",
                       "000d66696c655f696e7374616e63650004800049190010c61a3321045941c38e5675255feb0196")
    assert res.stdout == "(file_instance, 2021-03-27, c61a3321-0459-41c3-8e56-75255feb0196): token: -5043005771368701888, shard: 1\n"


def test_shardof_multiple_values(scylla_types):
    res = scylla_types("shardof", "--full-compound", "-t", "Int32Type", "-t", "UTF8Type", "--shards=8",
                       "0004000000010003616263", "0004000000020003616263")
    assert res.stdout.splitlines() == [
        "(1, abc): token: 8771735466527499816, shard: 5",
        "(2, abc): token: -3504390351319460166, shard: 6",
    ]


@pytest.mark.parametrize("ignore_msb_bits,shard", [(0, 7), (4, 4), (12, 5)])
def test_shardof_ignore_msb_bits(scylla_types, ignore_msb_bits, shard):
    res = scylla_types("shardof", "--full-compound", "-t", "Int32Type", "-t", "UTF8Type", "--shards=8",
                       f"--ignore-msb-bits={ignore_msb_bits}", "0004000000010003616263")
    assert res.stdout == f"(1, abc): token: 8771735466527499816, shard: {shard}\n"


def test_shardof_missing_shards(scylla_types_fails_with):
    scylla_types_fails_with("shardof", "--full-compound", "-t", "Int32Type", "000400000001",
                            error="error: missing mandatory argument --shards")


@pytest.mark.parametrize("compound_args", [[], ["--prefix-compound"]])
def test_shardof_requires_full_compound(scylla_types_fails_with, compound_args):
    scylla_types_fails_with("shardof", *compound_args, "-t", "Int32Type", "--shards=8", "00000001",
                            error="shardof action requires --full-compound (--partition-key) or --legacy-composite (--legacy-partition-key) input")


def test_shardof_legacy_composite(scylla_types):
    res = scylla_types("shardof", "--legacy-composite", "-t", "Int32Type", "-t", "UTF8Type", "--shards=8", "00040000000100000361626300")
    assert res.stdout == "(1, abc): token: 8771735466527499816, shard: 5\n"


def test_shardof_schema_file(scylla_types, schema_file):
    res = scylla_types("shardof", "--schema-file", schema_file, "--partition-key", "--shards=8", "0004000000010003616263")
    assert res.stdout == "(1, abc): token: 8771735466527499816, shard: 5\n"


@pytest.mark.parametrize("compound_option", ["--full-compound", "--legacy-composite"])
def test_shardof_text(scylla_types, compound_option):
    """With -f text, all values make up a single partition key."""
    res = scylla_types("shardof", compound_option, "-t", "Int32Type", "-t", "UTF8Type", "--shards=8", "-f", "text", "--", "1", "abc")
    assert res.stdout == "(1, abc): token: 8771735466527499816, shard: 5\n"
