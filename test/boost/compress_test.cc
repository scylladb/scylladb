/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>

#include <array>
#include <ranges>
#include <string>
#include <string_view>
#include <vector>

#include "sstables/compress.hh"
#include "sstables/exceptions.hh"
#include "sstables/framed_chunk.hh"

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

// The largest frame is the vint size of the largest valid compressed length.
// The codec rejects a chunk length whose largest frame wouldn't fit in
// max_chunk_frame_len bytes.
BOOST_AUTO_TEST_CASE(framed_chunk_codec_max_frame_len) {
    for (auto [kib, expected] : {std::pair{1u, 2u}, {2u, 2u}, {4u, 2u}, {8u, 2u}, {16u, 3u}, {32u, 3u}, {64u, 3u}, {128u, 3u}}) {
        BOOST_TEST_CONTEXT("chunk_length=" << kib << " KiB") {
            const auto codec = sstables::framed_chunk_codec::for_framed_chunks(kib * 1024);
            BOOST_CHECK_EQUAL(codec.max_frame_len(), expected);
            BOOST_CHECK_EQUAL(codec.frame_len(sstables::compressed_chunk_length_limit(kib * 1024)), expected);
        }
    }
    // An unframed codec has no frames.
    const sstables::framed_chunk_codec unframed(sstables::sstable_version_types::mt, 4096);
    BOOST_CHECK(!unframed.framed());
    BOOST_CHECK_EQUAL(unframed.max_frame_len(), 0);
    BOOST_CHECK_EQUAL(unframed.extent(100), 104);
    // The largest compressed length of an 8 MiB chunk takes 4 bytes.
    BOOST_CHECK_THROW(sstables::framed_chunk_codec::for_framed_chunks(8u << 20), sstables::malformed_sstable_exception);
}

// Writes a footer that holds `len` just before `chunk_end`.
static void write_footer(const sstables::framed_chunk_codec& codec, char* chunk_end, uint64_t len) {
    std::array<char, sstables::max_chunk_frame_len> header;
    codec.write_header(header.data(), len);
    const auto frame_len = codec.frame_len(len);
    std::reverse_copy(header.begin(), header.begin() + frame_len, chunk_end - frame_len);
}

// For lengths at the boundaries of the vint sizes: the frames have the vint
// size of the length, the footer holds the bytes of the header in reverse
// order, both decode to the length, and write_frames() writes no other byte.
BOOST_AUTO_TEST_CASE(chunk_frames_roundtrip) {
    const uint32_t uncompressed_chunk_length = 128 * 1024;
    const auto codec = sstables::framed_chunk_codec::for_framed_chunks(uncompressed_chunk_length);
    const uint64_t limit = sstables::compressed_chunk_length_limit(uncompressed_chunk_length);
    for (uint64_t len : {uint64_t(0), uint64_t(1), uint64_t(127), uint64_t(128), uint64_t(16383), uint64_t(16384), limit}) {
        BOOST_TEST_CONTEXT("len=" << len) {
            const uint32_t expected_frame_len = len < 128 ? 1 : len < 16384 ? 2 : 3;
            BOOST_CHECK_EQUAL(codec.frame_len(len), expected_frame_len);
            BOOST_CHECK_EQUAL(codec.extent(len), len + 2 * expected_frame_len + sizeof(uint32_t));

            // Guard bytes around the chunk must stay untouched.
            constexpr size_t guard = 4;
            std::vector<char> buf(guard + codec.extent(len) + guard, char(0xAA));
            char* chunk = buf.data() + guard;
            codec.write_frames(chunk, len);
            for (size_t i = 0; i < guard; ++i) {
                BOOST_CHECK_EQUAL(static_cast<unsigned char>(buf[i]), 0xAAu);
                BOOST_CHECK_EQUAL(static_cast<unsigned char>(buf[buf.size() - 1 - i]), 0xAAu);
            }

            const std::string_view whole(chunk, codec.extent(len));
            const auto header = whole.substr(0, expected_frame_len);
            const auto footer = whole.substr(whole.size() - expected_frame_len);
            BOOST_CHECK(std::ranges::equal(header, footer | std::views::reverse));

            // The decoders accept views that extend past the frame.
            const auto h = codec.decode_header(whole);
            BOOST_CHECK_EQUAL(h.compressed_len, len);
            BOOST_CHECK_EQUAL(h.len, expected_frame_len);
            const auto f = codec.decode_footer(whole);
            BOOST_CHECK_EQUAL(f.compressed_len, len);
            BOOST_CHECK_EQUAL(f.len, expected_frame_len);
            BOOST_CHECK_EQUAL(codec.frame_len_from_boundary_byte(header.front()), expected_frame_len);
            BOOST_CHECK_EQUAL(codec.frame_len_from_boundary_byte(footer.back()), expected_frame_len);
        }
    }
}

// A frame signals corruption if it holds a length above
// compressed_chunk_length_limit, if it is longer than the largest frame, if it
// isn't the shortest encoding of its length, or if it is cut short. The
// decoders must throw rather than let a reader size a read from such a frame.
BOOST_AUTO_TEST_CASE(chunk_frames_reject_invalid) {
    using sstables::malformed_sstable_exception;
    const auto codec = sstables::framed_chunk_codec::for_framed_chunks(4096); // limit 6144
    BOOST_REQUIRE_EQUAL(codec.max_frame_len(), 2);
    auto bytes = [] (std::initializer_list<uint8_t> b) {
        return std::string(b.begin(), b.end());
    };

    // Valid: 6144 = 0x1800, in two bytes.
    BOOST_CHECK_EQUAL(codec.decode_header(bytes({0x80 | 0x18, 0x00})).compressed_len, 6144);
    // The length exceeds the limit: 6145.
    BOOST_CHECK_THROW(codec.decode_header(bytes({0x80 | 0x18, 0x01})), malformed_sstable_exception);
    // The length 5, in two bytes instead of one.
    BOOST_CHECK_THROW(codec.decode_header(bytes({0x80, 0x05})), malformed_sstable_exception);
    BOOST_CHECK_THROW(codec.decode_footer(bytes({0x05, 0x80})), malformed_sstable_exception);
    // A three-byte frame, longer than the largest frame of this codec.
    BOOST_CHECK_THROW(codec.frame_len_from_boundary_byte(char(0xc0)), malformed_sstable_exception);
    BOOST_CHECK_THROW(codec.decode_header(bytes({0xc0, 0x00, 0x05})), malformed_sstable_exception);
    // The first byte announces two bytes, but only one is there.
    BOOST_CHECK_THROW(codec.decode_header(bytes({0x80})), malformed_sstable_exception);
    BOOST_CHECK_THROW(codec.decode_footer(bytes({0x80})), malformed_sstable_exception);
    // No bytes at all.
    BOOST_CHECK_THROW(codec.decode_header(""), malformed_sstable_exception);
    BOOST_CHECK_THROW(codec.decode_footer(""), malformed_sstable_exception);
}

// A reader can parse a framed chunk from its whole bytes, from the length in
// its header and the bytes after the header, or from the length in its footer
// and the bytes before the footer. All three give the same parts, and all three
// reject a footer that doesn't hold the same length as the header.
BOOST_AUTO_TEST_CASE(framed_chunk_parse_variants) {
    const uint32_t uncompressed_chunk_length = 4096;
    const auto codec = sstables::framed_chunk_codec::for_framed_chunks(uncompressed_chunk_length);
    for (uint32_t len : {5u, 200u}) {
        BOOST_TEST_CONTEXT("len=" << len) {
            const size_t frame_len = codec.frame_len(len);
            std::vector<char> chunk(codec.extent(len), 'x');
            codec.write_frames(chunk.data(), len);
            const std::string_view whole(chunk.data(), chunk.size());
            const auto after_header = whole.substr(frame_len);
            const auto before_footer = whole.substr(0, whole.size() - frame_len);

            auto check_parts = [&] (const sstables::framed_chunk& c) {
                BOOST_CHECK_EQUAL(c.extent(), whole.size());
                BOOST_CHECK(c.header() == whole.substr(0, frame_len));
                BOOST_CHECK(c.compressed == whole.substr(frame_len, len));
                BOOST_CHECK(c.footer() == whole.substr(whole.size() - frame_len));
            };
            check_parts(codec.parse(whole));
            check_parts(codec.parse_after_header(len, after_header));
            check_parts(codec.parse_before_footer(before_footer, len));

            // The bytes don't match the length that the caller read from a frame.
            BOOST_CHECK_THROW(codec.parse_after_header(len + 1, after_header), sstables::malformed_sstable_exception);
            BOOST_CHECK_THROW(codec.parse_before_footer(before_footer, len + 1), sstables::malformed_sstable_exception);

            // The footer holds a different length than the header.
            write_footer(codec, chunk.data() + chunk.size(), len - 1);
            BOOST_CHECK_THROW(codec.parse(whole), sstables::malformed_sstable_exception);
            BOOST_CHECK_THROW(codec.parse_after_header(len, after_header), sstables::malformed_sstable_exception);

            // The header holds a different length than the footer.
            codec.write_frames(chunk.data(), len);
            codec.write_header(chunk.data(), len - 1);
            BOOST_CHECK_THROW(codec.parse_before_footer(before_footer, len), sstables::malformed_sstable_exception);
        }
    }

    // The last byte of the footer announces a longer frame than the header has.
    const uint32_t len = 5;
    std::vector<char> chunk(codec.extent(len), 'x');
    codec.write_frames(chunk.data(), len);
    chunk.back() = char(0x80);
    BOOST_CHECK_THROW(codec.parse(std::string_view(chunk.data(), chunk.size())), sstables::malformed_sstable_exception);
}
