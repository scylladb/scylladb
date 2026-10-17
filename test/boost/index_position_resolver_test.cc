/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#define BOOST_TEST_MODULE index_position_resolver_test

#include <boost/test/unit_test.hpp>

#include "sstables/mx/index_position_resolver.hh"

using sstables::sstable_position;
using sstables::mc::index_position_resolver;

namespace {

// Chunks of 100 uncompressed bytes each. Chunk i starts at 1000 + 60 * i on
// disk and has an extent of 60.
index_position_resolver::chunk test_chunk(uint64_t i) {
    return {
        .uncompressed_start = 100 * i,
        .uncompressed_length = 100,
        .chunk_start = 1000 + 60 * i,
        .extent = 60,
    };
}

void check_physical(const std::optional<sstable_position>& pos, int64_t chunk_position, int64_t offset_within_chunk) {
    BOOST_REQUIRE(pos.has_value());
    BOOST_REQUIRE(pos->is_physical());
    BOOST_CHECK_EQUAL(pos->as_physical().chunk_position, chunk_position);
    BOOST_CHECK_EQUAL(pos->as_physical().extent_hint, 60);
    BOOST_CHECK_EQUAL(pos->as_physical().offset_within_chunk, offset_within_chunk);
}

} // anonymous namespace

BOOST_AUTO_TEST_CASE(logical_positions_are_known_immediately) {
    index_position_resolver r(false);
    auto pos = r.resolve(12345);
    BOOST_REQUIRE(pos.has_value());
    BOOST_REQUIRE(!pos->is_physical());
    BOOST_CHECK_EQUAL(pos->to_logical(), 12345);
}

BOOST_AUTO_TEST_CASE(physical_position_waits_for_its_chunk) {
    index_position_resolver r(true);
    BOOST_CHECK(!r.resolve(0).has_value());
    r.add_chunk(test_chunk(0));
    check_physical(r.resolve(0), 1000, 0);
    check_physical(r.resolve(99), 1000, 99);
    BOOST_CHECK(!r.resolve(100).has_value());
    r.add_chunk(test_chunk(1));
    r.add_chunk(test_chunk(2));
    check_physical(r.resolve(150), 1060, 50);
    check_physical(r.resolve(299), 1120, 99);
    BOOST_CHECK(!r.resolve(300).has_value());
}

BOOST_AUTO_TEST_CASE(chunk_end_belongs_to_next_chunk) {
    index_position_resolver r(true);
    r.add_chunk(test_chunk(0));
    r.add_chunk(test_chunk(1));
    // Not (chunk 0, offset 100).
    check_physical(r.resolve(100), 1060, 0);
}

BOOST_AUTO_TEST_CASE(discard_keeps_the_chunk_of_the_watermark) {
    index_position_resolver r(true);
    for (uint64_t i = 0; i < 4; ++i) {
        r.add_chunk(test_chunk(i));
    }
    BOOST_CHECK_EQUAL(r.held_chunks(), 4);
    r.discard_before(150);
    BOOST_CHECK_EQUAL(r.held_chunks(), 3);
    check_physical(r.resolve(150), 1060, 50);
    // A chunk that ends at the watermark contains no position at or after it.
    r.discard_before(300);
    BOOST_CHECK_EQUAL(r.held_chunks(), 1);
    check_physical(r.resolve(300), 1180, 0);
    // Discarding past the reported chunks forgets all of them, and the later
    // chunks are still resolved.
    r.discard_before(1000);
    BOOST_CHECK_EQUAL(r.held_chunks(), 0);
    BOOST_CHECK(!r.resolve(1000).has_value());
    for (uint64_t i = 4; i < 11; ++i) {
        r.add_chunk(test_chunk(i));
    }
    check_physical(r.resolve(1000), 1600, 0);
}
