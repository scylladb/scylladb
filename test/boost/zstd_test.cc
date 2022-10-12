/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "compress.hh"
#include <seastar/core/memory.hh>
#include <seastar/testing/thread_test_case.hh>
#include <bit>

shared_ptr<compressor> make_compressor(int level, size_t chunk_size_kb) {
    return compressor::create({
        {"sstable_compression", "org.apache.cassandra.io.compress.ZstdCompressor"},
        {"compression_level", std::to_string(level)},
        {"chunk_length_in_kb", std::to_string(chunk_size_kb)}
    });
}

std::vector<char> get_random_data(size_t size) {
    std::vector<char> v(size);
    for (size_t i = 0; i < size; ++i) {
        v[i] = i % 128;
    }
    return v;
}

std::vector<char> compress(compressor& c, const std::vector<char>& in) {
    auto out = std::vector<char>(c.compress_max_size(in.size()));
    size_t out_size = c.compress(in.data(), in.size(), out.data(), out.size());
    BOOST_CHECK_LT(out_size, out.size());
    out.resize(out_size);
    return out;
}

std::vector<char> uncompress(const compressor& c, const std::vector<char>& in, size_t out_size) {
    auto out = std::vector<char>(out_size);
    size_t actual_out_size = c.uncompress(in.data(), in.size(), out.data(), out.size());
    BOOST_CHECK_EQUAL(actual_out_size, out.size());
    return out;
}

void test_roundtrip(compressor& c, size_t input_size) {
    auto raw = get_random_data(input_size);
    auto compressed = compress(c, raw);
    auto uncompressed = uncompress(c, compressed, raw.size());
    BOOST_CHECK_EQUAL(raw, uncompressed);
}

SEASTAR_THREAD_TEST_CASE(test_buffer_correctness) {
    for (int level : {3, 22}) {
        std::vector<shared_ptr<compressor>> compressors;
        for (size_t chunk_size_kb : {4, 64, 16, 2*1024, 128, 4, 4*1024, 1024}) {
            compressors.push_back(make_compressor(level, chunk_size_kb));
            test_roundtrip(*compressors.back(), chunk_size_kb * 1024);
            for (auto& c : compressors) {
                test_roundtrip(*c, 1024);
            }
        }
        compressors.clear();
    }
}

#ifndef SEASTAR_DEFAULT_ALLOCATOR
SEASTAR_THREAD_TEST_CASE(test_buffer_reuse) {
    constexpr int level = 3;
    constexpr int chunk_size_kb = 4;
    auto c = make_compressor(level, chunk_size_kb);
    test_roundtrip(*c, 4*1024);

    size_t allocs_before = seastar::memory::stats().mallocs();
    test_roundtrip(*c, 4*1024);
    size_t allocs_after = seastar::memory::stats().mallocs();

    size_t expected_allocs = allocs_before + 3; // 3 allocs for the raw, compressed and uncompressed buffers, 0 allocs for the compressor.
    BOOST_CHECK_EQUAL(allocs_after, expected_allocs);
}

SEASTAR_THREAD_TEST_CASE(test_decompressor_buffer_sharing) {
    constexpr int level = 3;
    constexpr int chunk_size_kb = 4;
    auto raw = get_random_data(chunk_size_kb*1024);
    auto compressed = compress(*make_compressor(level, chunk_size_kb), raw);

    std::vector<shared_ptr<compressor>> compressors;
    compressors.reserve(1000);
    size_t memory_before = seastar::memory::stats().allocated_memory();
    for (size_t i = 0; i < 1000; ++i) {
        compressors.push_back(make_compressor(level, 1024));
        auto uncompressed = uncompress(*compressors.back(), compressed, raw.size());
        BOOST_CHECK_EQUAL(raw, uncompressed);
    }
    size_t memory_after = seastar::memory::stats().allocated_memory();
    BOOST_CHECK_LT(memory_after, memory_before + 1024*1024);
}

SEASTAR_THREAD_TEST_CASE(test_compressor_buffer_sharing) {
    constexpr int level = 3;
    constexpr int chunk_size_kb = 4;
    auto raw = get_random_data(chunk_size_kb*1024);

    std::vector<shared_ptr<compressor>> compressors;
    compressors.reserve(1000);
    size_t memory_before = seastar::memory::stats().allocated_memory();
    for (size_t i = 0; i < 1000; ++i) {
        compressors.push_back(make_compressor(level, 1024));
        compress(*compressors.back(), raw);
        size_t memory_after = seastar::memory::stats().allocated_memory();
    }
    size_t memory_after = seastar::memory::stats().allocated_memory();
    BOOST_CHECK_LT(memory_after, memory_before + 4*1024*1024);
}

SEASTAR_THREAD_TEST_CASE(test_decompressor_doesnt_allocate_cctx) {
    constexpr int level = 22;
    constexpr int chunk_size_kb = 4*1024;
    size_t memory_start = seastar::memory::stats().allocated_memory();
    {
        auto raw = get_random_data(1024);
        auto compressed = compress(*make_compressor(level, chunk_size_kb), raw);
        size_t memory_before = seastar::memory::stats().allocated_memory();
        auto decompressor = make_compressor(level, chunk_size_kb);
        uncompress(*decompressor, compressed, raw.size());
        // Check that decompressor doesn't allocate compression contexts.
        size_t memory_after = seastar::memory::stats().allocated_memory();
        BOOST_CHECK_LT(memory_after, memory_before + 1024*1024);
    }
    size_t memory_end = seastar::memory::stats().allocated_memory();
    // Check that there are no big leftovers from a big compression context.
    BOOST_CHECK_LT(memory_end, memory_start + 4*1024*1024);
}
#endif
