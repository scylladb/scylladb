/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>

#include <array>
#include <vector>

#include "sstables/compress.hh"
#include "sstables/exceptions.hh"

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

BOOST_AUTO_TEST_CASE(chunk_length_field_bits_and_size) {
    // A valid compressed length is at most compressed_chunk_length_limit, so a
    // chunk-length field stores the length in exactly the minimal number of bits
    // that can hold that limit, rounded up to ceil(bits / 8) bytes. The 87381/
    // 87382 pair straddles the boundary where the limit crosses 2^17.
    for (uint32_t uncompressed_chunk_length : {1u << 10, 1u << 12, 1u << 16, (1u << 16) + 1, 87381u, 87382u}) {
        BOOST_TEST_CONTEXT("uncompressed_chunk_length=" << uncompressed_chunk_length) {
            const uint64_t limit = sstables::compressed_chunk_length_limit(uncompressed_chunk_length);
            const auto bits = sstables::chunk_length_field_bits(uncompressed_chunk_length);
            // Enough bits to represent any length up to the limit, and not one more.
            BOOST_CHECK_GT(uint64_t(1) << bits, limit);
            BOOST_CHECK_LT(uint64_t(1) << (bits - 1), limit);
            BOOST_CHECK_EQUAL(sstables::chunk_length_field_size(uncompressed_chunk_length),
                    (bits + 7) / 8);
        }
    }
}

BOOST_AUTO_TEST_CASE(chunk_length_field_roundtrip) {
    const uint32_t uncompressed_chunk_length = 1u << 12; // 4096
    const uint32_t limit = sstables::compressed_chunk_length_limit(uncompressed_chunk_length); // 6144
    const size_t size = sstables::chunk_length_field_size(uncompressed_chunk_length);
    std::vector<char> buf(size + 4, char(0xAA)); // extra bytes must stay untouched

    // A real compressed length is at most compressed_chunk_length_limit, so it fits
    // in chunk_length_field_bits bits and is accepted by the reader.
    for (uint32_t len : {0u, 1u, limit, uncompressed_chunk_length}) {
        BOOST_TEST_CONTEXT("len=" << len) {
            sstables::write_chunk_length_field(buf.data(), uncompressed_chunk_length, len);
            BOOST_CHECK_EQUAL(sstables::read_chunk_length_field(buf.data(), uncompressed_chunk_length), len);
            // The field must not spill past chunk_length_field_size bytes.
            for (size_t i = size; i < buf.size(); ++i) {
                BOOST_CHECK_EQUAL(static_cast<unsigned char>(buf[i]), 0xAAu);
            }
        }
    }
}

BOOST_AUTO_TEST_CASE(chunk_length_field_rejects_out_of_range) {
    // A length exceeding compressed_chunk_length_limit signals corruption; the
    // reader must throw rather than size a read from a bogus length. We can only
    // materialise such a value on disk by hand, since the writer never emits it.
    // The limit (36) is not a power of two, so chunk_length_field_bits leaves room
    // (2^6 == 64 > 36) to encode an out-of-range value that the reader rejects.
    const uint32_t uncompressed_chunk_length = 24; // limit 36, 6 bits
    const uint64_t limit = sstables::compressed_chunk_length_limit(uncompressed_chunk_length);
    const size_t bits = sstables::chunk_length_field_bits(uncompressed_chunk_length);
    const size_t size = sstables::chunk_length_field_size(uncompressed_chunk_length);
    std::vector<char> buf(size, 0);

    // Craft a raw field value just past the limit.
    const uint64_t bad = limit + 1;
    for (size_t i = 0; i < size; ++i) {
        buf[i] = static_cast<char>(static_cast<uint8_t>(bad >> (8 * i)));
    }
    BOOST_CHECK_LT(bad, uint64_t(1) << bits); // fits in the field, but exceeds the bound

    BOOST_CHECK_THROW(
            sstables::read_chunk_length_field(buf.data(), uncompressed_chunk_length),
            sstables::malformed_sstable_exception);
}
