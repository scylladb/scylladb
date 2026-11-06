/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#define BOOST_TEST_MODULE sstable_position_test

#include <boost/test/unit_test.hpp>

#include "sstables/sstable_position.hh"

#include <fmt/format.h>

using sstables::sstable_position;
using sstables::sstable_position_offset;

BOOST_AUTO_TEST_CASE(logical_add_sub_roundtrip) {
    auto a = sstable_position::from_logical(100);
    auto b = sstable_position::from_logical(30);

    auto off = a - b;
    BOOST_CHECK_EQUAL(off.to_logical(), 70);
    // b + (a - b) == a
    BOOST_CHECK((b + off) == a);
    BOOST_CHECK_EQUAL((b + off).to_logical(), 100);
}

BOOST_AUTO_TEST_CASE(logical_ordering) {
    BOOST_CHECK(sstable_position::from_logical(1) < sstable_position::from_logical(2));
    BOOST_CHECK(sstable_position::from_logical(2) > sstable_position::from_logical(1));
    BOOST_CHECK(sstable_position::from_logical(5) == sstable_position::from_logical(5));
}

BOOST_AUTO_TEST_CASE(physical_ordering_ignores_extent_hint) {
    // Two positions with identical (chunk_position, offset_within_chunk) but
    // different extent_hint must compare equal.
    auto p1 = sstable_position::from_physical(1000, 4096, 17);
    auto p2 = sstable_position::from_physical(1000, 8192, 17);
    BOOST_CHECK(p1 == p2);
    BOOST_CHECK((p1 <=> p2) == std::weak_ordering::equivalent);

    // chunk_position dominates the ordering.
    BOOST_CHECK(sstable_position::from_physical(1000, 4096, 999)
              < sstable_position::from_physical(2000, 4096, 0));
    // then offset_within_chunk.
    BOOST_CHECK(sstable_position::from_physical(1000, 4096, 10)
              < sstable_position::from_physical(1000, 4096, 20));
}

BOOST_AUTO_TEST_CASE(physical_add_sub_roundtrip) {
    // Partition start in chunk at compressed offset 1000; row in a later chunk
    // at compressed offset 5000, chunk on-disk size 4096, 42 bytes into the
    // decompressed chunk.
    auto partition_start = sstable_position::from_physical(1000, 4096, 0);
    auto row = sstable_position::from_physical(5000, 4096, 42);

    auto off = row - partition_start;
    // chunk_position of the offset is *relative*: 5000 - 1000.
    BOOST_CHECK(off.holds_physical());
    BOOST_CHECK_EQUAL(off.as_physical().chunk_position, 4000);
    // extent_hint and offset_within_chunk are absolute (taken from the row).
    BOOST_CHECK_EQUAL(off.as_physical().extent_hint, 4096);
    BOOST_CHECK_EQUAL(off.as_physical().offset_within_chunk, 42);

    // partition_start + (row - partition_start) == row
    auto recovered = partition_start + off;
    BOOST_CHECK(recovered == row);
    BOOST_CHECK_EQUAL(recovered.as_physical().chunk_position, 5000);
    BOOST_CHECK_EQUAL(recovered.as_physical().extent_hint, 4096);
    BOOST_CHECK_EQUAL(recovered.as_physical().offset_within_chunk, 42);
}

BOOST_AUTO_TEST_CASE(physical_offset_same_chunk_is_zero) {
    auto partition_start = sstable_position::from_physical(1000, 4096, 8);
    auto row = sstable_position::from_physical(1000, 4096, 100);

    auto off = row - partition_start;
    // Same chunk => relative chunk_position is 0.
    BOOST_CHECK_EQUAL(off.as_physical().chunk_position, 0);
    BOOST_CHECK_EQUAL(off.as_physical().offset_within_chunk, 100);
    BOOST_CHECK((partition_start + off) == row);
}

BOOST_AUTO_TEST_CASE(accessors_and_predicates) {
    auto l = sstable_position::from_logical(7);
    BOOST_CHECK(!l.is_physical());
    BOOST_CHECK_EQUAL(l.to_logical(), 7);

    auto p = sstable_position::from_physical(10, 20, 30);
    BOOST_CHECK(p.is_physical());
    BOOST_CHECK_EQUAL(p.as_physical().chunk_position, 10);
    BOOST_CHECK_EQUAL(p.as_physical().extent_hint, 20);
    BOOST_CHECK_EQUAL(p.as_physical().offset_within_chunk, 30);

    auto lo = sstable_position_offset::from_logical(3);
    BOOST_CHECK(!lo.holds_physical());
    BOOST_CHECK_EQUAL(lo.to_logical(), 3);
    auto po = sstable_position_offset::from_physical(1, 2, 3);
    BOOST_CHECK(po.holds_physical());
}

BOOST_AUTO_TEST_CASE(formatting) {
    BOOST_CHECK_EQUAL(fmt::format("{}", sstable_position::from_logical(42)), "42");
    BOOST_CHECK_EQUAL(fmt::format("{}", sstable_position::from_physical(1, 2, 3)),
        "{chunk_position=1, extent_hint=2, offset_within_chunk=3}");

    BOOST_CHECK_EQUAL(fmt::format("{}", sstable_position_offset::from_logical(9)), "9");
    BOOST_CHECK_EQUAL(fmt::format("{}", sstable_position_offset::from_physical(4, 5, 6)),
        "{chunk_position=4, extent_hint=5, offset_within_chunk=6}");
}
