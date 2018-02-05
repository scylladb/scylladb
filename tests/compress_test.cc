/*
 * Copyright (C) 2017 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>

#include "sstables/compress.hh"

BOOST_AUTO_TEST_CASE(segmented_offsets_basic_functionality) {
    sstables::compression::segmented_offsets offsets;

    // f = 20, c = 10, n = 8
    offsets.init(1 << 10);

    sstables::compression::segmented_offsets::writer writer = offsets.get_writer();
    sstables::compression::segmented_offsets::accessor accessor = offsets.get_accessor();

    writer.push_back(0);
    writer.push_back(100);
    writer.push_back(200);
    writer.push_back(300);
    writer.push_back(400);
    writer.push_back(500);
    writer.push_back(600);
    writer.push_back(700);

    BOOST_REQUIRE_EQUAL(accessor.at(0), 0);
    BOOST_REQUIRE_EQUAL(accessor.at(1), 100);
    BOOST_REQUIRE_EQUAL(accessor.at(2), 200);
    BOOST_REQUIRE_EQUAL(accessor.at(3), 300);
    BOOST_REQUIRE_EQUAL(accessor.at(4), 400);
    BOOST_REQUIRE_EQUAL(accessor.at(5), 500);
    BOOST_REQUIRE_EQUAL(accessor.at(6), 600);
    BOOST_REQUIRE_EQUAL(accessor.at(7), 700);

    const uint64_t largest_base{0x00000000000fe000};
    const uint64_t trailing_zeroes{0x00000000000fff00};
    const uint64_t all_ones{0x00000000000fffff};

    writer.push_back(largest_base);
    writer.push_back(trailing_zeroes);
    writer.push_back(all_ones);

    BOOST_REQUIRE_EQUAL(accessor.at(0), 0);
    BOOST_REQUIRE_EQUAL(accessor.at(1), 100);
    BOOST_REQUIRE_EQUAL(accessor.at(2), 200);
    BOOST_REQUIRE_EQUAL(accessor.at(3), 300);
    BOOST_REQUIRE_EQUAL(accessor.at(4), 400);
    BOOST_REQUIRE_EQUAL(accessor.at(5), 500);
    BOOST_REQUIRE_EQUAL(accessor.at(6), 600);
    BOOST_REQUIRE_EQUAL(accessor.at(7), 700);
    BOOST_REQUIRE_EQUAL(accessor.at(8), largest_base);
    BOOST_REQUIRE_EQUAL(accessor.at(9), trailing_zeroes);
    BOOST_REQUIRE_EQUAL(accessor.at(10), all_ones);
}

BOOST_AUTO_TEST_CASE(segmented_offsets_more_buckets) {
    sstables::compression::segmented_offsets offsets;
    offsets.init(1 << 9);

    sstables::compression::segmented_offsets::writer writer = offsets.get_writer();
    sstables::compression::segmented_offsets::accessor accessor = offsets.get_accessor();

    const std::size_t size = 0x0000000000100000;

    for (std::size_t i = 0; i < size; ++i) {
        writer.push_back(i);
    }

    BOOST_REQUIRE_EQUAL(offsets.size(), size);

    for (std::size_t i = 0; i < size; ++i) {
        BOOST_REQUIRE_EQUAL(accessor.at(i), i);
    }
}

BOOST_AUTO_TEST_CASE(segmented_offsets_iterator) {
    sstables::compression::segmented_offsets offsets;
    offsets.init(1 << 14);

    sstables::compression::segmented_offsets::writer writer = offsets.get_writer();
    sstables::compression::segmented_offsets::accessor accessor = offsets.get_accessor();

    const std::size_t size = 0x0000000000100000;

    for (std::size_t i = 0; i < size; ++i) {
        writer.push_back(i);
    }

    BOOST_REQUIRE_EQUAL(offsets.size(), size);

    std::size_t i{0};
    for (auto offset : offsets) {
        BOOST_REQUIRE_EQUAL(offset, i);
        ++i;
    }

    for (std::size_t i = 0; i < size; i += 1024) {
        BOOST_REQUIRE_EQUAL(accessor.at(i), i);
    }
}

BOOST_AUTO_TEST_CASE(segmented_offsets_overflow_detection) {
    sstables::compression::segmented_offsets offsets;
    offsets.init(1 << 8);

    sstables::compression::segmented_offsets::writer writer = offsets.get_writer();

    const uint64_t overflown_base_offset{0x0000000000100000};
    BOOST_REQUIRE_THROW(writer.push_back(overflown_base_offset), std::invalid_argument);

    const uint64_t good_base_offset{0x00000000000f0000};
    BOOST_REQUIRE_NO_THROW(writer.push_back(good_base_offset));

    const uint64_t overflown_segment_offset{0x00000000000fffff};
    BOOST_REQUIRE_THROW(writer.push_back(overflown_segment_offset), std::invalid_argument);

    const uint64_t good_segment_offset{0x00000000000f0001};
    BOOST_REQUIRE_NO_THROW(writer.push_back(good_segment_offset));
}

BOOST_AUTO_TEST_CASE(segmented_offsets_corner_cases) {
    sstables::compression::segmented_offsets offsets;
    offsets.init(1 << 12);

    sstables::compression::segmented_offsets::writer writer = offsets.get_writer();
    sstables::compression::segmented_offsets::accessor accessor = offsets.get_accessor();

    const std::size_t size = 0x0000000000100000;

    for (std::size_t i = 0; i < size; ++i) {
        writer.push_back(i);
    }

    // Random at() to a position just before a bucket boundary, then do an
    // incremental at() to read the next offset.
    BOOST_REQUIRE(accessor.at(4079) == 4079);
    BOOST_REQUIRE(accessor.at(4080) == 4080);
}
