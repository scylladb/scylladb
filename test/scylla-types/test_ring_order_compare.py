#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import pytest


# Tokens:
# (1, abc): 8771735466527499816
# (2, abc): -3504390351319460166
KEY1 = "0004000000010003616263"
KEY2 = "0004000000020003616263"
LEGACY_KEY1 = "00040000000100000361626300"
LEGACY_KEY2 = "00040000000200000361626300"
# How ring-order-compare prints the keys: with their tokens.
RING1 = "{token: 8771735466527499816, key: (1, abc)}"
RING2 = "{token: -3504390351319460166, key: (2, abc)}"


@pytest.mark.parametrize("lhs,rhs,result", [
    (KEY1, KEY2, f"{RING1} > {RING2}"),
    (KEY2, KEY1, f"{RING2} < {RING1}"),
    (KEY1, KEY1, f"{RING1} == {RING1}"),
])
def test_ring_order_compare(scylla_types, lhs, rhs, result):
    res = scylla_types("ring-order-compare", "--full-compound", "-t", "Int32Type", "-t", "UTF8Type", lhs, rhs)
    assert res.stdout == f"{result}\n"


def test_ring_order_compare_vs_compare(scylla_types):
    """Ring order (token order) is different from the order of the components."""
    args = ["--full-compound", "-t", "Int32Type", "-t", "UTF8Type", KEY1, KEY2]
    assert scylla_types("compare", *args).stdout == "(1, abc) < (2, abc)\n"
    assert scylla_types("ring-order-compare", *args).stdout == f"{RING1} > {RING2}\n"


def test_ring_order_compare_legacy_composite(scylla_types):
    res = scylla_types("ring-order-compare", "--legacy-composite", "-t", "Int32Type", "-t", "UTF8Type", LEGACY_KEY1, LEGACY_KEY2)
    assert res.stdout == f"{RING1} > {RING2}\n"


@pytest.mark.parametrize("values", [[KEY1], [KEY1, KEY2, KEY1]])
def test_ring_order_compare_wrong_number_of_values(scylla_types_fails_with, values):
    scylla_types_fails_with("ring-order-compare", "--full-compound", "-t", "Int32Type", "-t", "UTF8Type", *values,
                            error=f"expected 2 values, got {len(values)}")


@pytest.mark.parametrize("compound_args", [[], ["--prefix-compound"]])
def test_ring_order_compare_requires_full_compound(scylla_types_fails_with, compound_args):
    scylla_types_fails_with("ring-order-compare", *compound_args, "-t", "Int32Type", "00000001", "00000002",
                            error="ring-order-compare action requires --full-compound (--partition-key) or --legacy-composite"
                                  " (--legacy-partition-key) input")


def test_ring_order_compare_schema_file(scylla_types, schema_file):
    res = scylla_types("ring-order-compare", "--schema-file", schema_file, "--partition-key", KEY1, KEY2)
    assert res.stdout == f"{RING1} > {RING2}\n"


@pytest.mark.parametrize("compound_option", ["--full-compound", "--legacy-composite"])
def test_ring_order_compare_text(scylla_types, compound_option):
    res = scylla_types("ring-order-compare", compound_option, "-t", "Int32Type", "-t", "UTF8Type", "-f", "text", "--",
                       "1", "abc", "2", "abc")
    assert res.stdout == f"{RING1} > {RING2}\n"


def test_ring_order_compare_text_odd_number_of_values(scylla_types_fails_with):
    scylla_types_fails_with("ring-order-compare", "--full-compound", "-t", "Int32Type", "-f", "text", "--", "1", "2", "3",
                            error="error: expected the number of unserialized values (3) to be divisible by 2")
