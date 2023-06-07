/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>

#include "sstables/compress.hh"
#include "compress.hh"
#include "utils/hashers.hh"
#include "utils/managed_bytes.hh"
#include "utils/fragment_range.hh"

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

// generates data that is not repetetive, yet predictable. I.e. 
// same every run. Because random fails s**s.
bytes generate_data(std::string_view seed, size_t size) {
    bytes res(bytes::initialized_later{}, size);

    sha256_hasher sha1;

    auto o = res.begin();
    auto e = res.end();
    auto src = seed;

    while (o != e) {
        auto tmp = sha256_hasher::calculate(src);
        auto n = std::copy_n(tmp.begin(), std::min<size_t>(tmp.size(), std::distance(o, e)), o);
        src = std::string_view(reinterpret_cast<const char*>(&*o), reinterpret_cast<const char*>(&*n));
        o = n;
    }

    return res;
}

static fragmented_temporary_buffer make_fragmented(const bytes_ostream& src) {
    fragmented_temporary_buffer res = fragmented_temporary_buffer::allocate_to_fit(src.size_bytes());
    auto out = res.get_ostream();
    for (auto&& v : src.fragments()) {
        out.write(reinterpret_cast<const char*>(v.data()), v.size());
    }
    return res;
}

static managed_bytes make_managed(const bytes_ostream& src) {
    managed_bytes res(managed_bytes::initialized_later(), src.size_bytes());
    managed_bytes_mutable_view view(res);
    for (auto&& v : src.fragments()) {
        while (!v.empty()) {
            auto frag = view.current_fragment();
            auto n = std::min(frag.size(), v.size());
            std::copy_n(v.begin(), n, frag.begin());
            view.remove_prefix(n);
            v.remove_prefix(n);
        }
    }
    return res;
}

static void test_compressor(const compressor& c) {
    static std::string_view sources[] = {
        "babiankaka",
        "somethingisrottenindenmark",
        "IcantDanceAlone", 
    };
    // ensure some source data sets are greater than 128k to force actual fragmentation
    static size_t sizes[] = { 837, 4975, 87623, 314 * 1024, 792 * 1024 };

    for (auto&& src : sources) {
        for (auto size : sizes) {
            bytes data = generate_data(src, size);

            // step 1: compress/decompress linear

            {
                auto req_size = c.compress_max_size(data.size());
                bytes tmp1(bytes::initialized_later{}, req_size);
                bytes tmp2(bytes::initialized_later{}, size);

                auto n = c.compress(reinterpret_cast<const char*>(data.data()), data.size(), reinterpret_cast<char*>(tmp1.data()), tmp1.size());
                c.uncompress(reinterpret_cast<const char*>(tmp1.data()), n, reinterpret_cast<char*>(tmp2.data()), tmp2.size());

                BOOST_REQUIRE_EQUAL(data, tmp2);
            }

            // step 2: compress/decompress fragmented_temporary_buffer
            {
                fragmented_temporary_buffer tmp(reinterpret_cast<const char*>(data.data()), data.size());
                bytes_ostream os;

                c.compress(tmp, os);

                {
                    auto ftb = make_fragmented(os);
                    bytes_ostream os1;
                    c.uncompress(ftb, os1);

                    auto res = os1.linearize();
                    BOOST_REQUIRE_EQUAL(data, res);
                }
                {
                    auto mb = make_managed(os);
                    bytes_ostream os1;
                    c.uncompress(mb, os1);

                    auto res = os1.linearize();
                    BOOST_REQUIRE_EQUAL(data, res);
                }
            }

            // step 3: compress/decompress managed_bytes
            {
                managed_bytes tmp(data);
                bytes_ostream os;

                c.compress(tmp, os);

                {
                    auto ftb = make_fragmented(os);
                    bytes_ostream os1;
                    c.uncompress(ftb, os1);

                    auto res = os1.linearize();
                    BOOST_REQUIRE_EQUAL(data, res);
                }
                {
                    auto mb = make_managed(os);
                    bytes_ostream os1;
                    c.uncompress(mb, os1);

                    auto res = os1.linearize();
                    BOOST_REQUIRE_EQUAL(data, res);
                }
            }
        }
    }
}

BOOST_AUTO_TEST_CASE(test_deflate_compressor) {
    test_compressor(*compressor::deflate);
}

BOOST_AUTO_TEST_CASE(test_lz4_compressor) {
    test_compressor(*compressor::lz4);
}

BOOST_AUTO_TEST_CASE(test_snappy_compressor) {
    test_compressor(*compressor::snappy);
}

BOOST_AUTO_TEST_CASE(test_zstd_compressor) {
    auto z = compressor::create("ZstdCompressor", [](auto&&) { return std::nullopt; });
    test_compressor(*z);
}
