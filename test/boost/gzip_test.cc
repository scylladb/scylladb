/*
 * Copyright 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <boost/test/unit_test.hpp>
#include <seastar/core/thread.hh>
#include <seastar/testing/test_case.hh>
#include "utils/gzip.hh"
#include <libdeflate.h>
#include <cstring>

using namespace seastar;

namespace {

// Helper function to compress data with gzip
std::vector<char> gzip_compress(const std::vector<char>& data) {
    auto* compressor = libdeflate_alloc_compressor(6);
    if (!compressor) {
        throw std::bad_alloc();
    }
    
    size_t max_compressed_size = libdeflate_gzip_compress_bound(compressor, data.size());
    std::vector<char> compressed(max_compressed_size);
    
    size_t actual_size = libdeflate_gzip_compress(
        compressor,
        data.data(),
        data.size(),
        compressed.data(),
        compressed.size()
    );
    
    libdeflate_free_compressor(compressor);
    
    compressed.resize(actual_size);
    return compressed;
}

// Convert vector to chunked_content
rjson::chunked_content to_chunked_content(const std::vector<char>& data) {
    rjson::chunked_content result;
    temporary_buffer<char> buf(data.size());
    std::memcpy(buf.get_write(), data.data(), data.size());
    result.push_back(std::move(buf));
    return result;
}

// Convert chunked_content to vector
std::vector<char> from_chunked_content(const rjson::chunked_content& chunks) {
    std::vector<char> result;
    for (const auto& chunk : chunks) {
        result.insert(result.end(), chunk.begin(), chunk.end());
    }
    return result;
}

} // anonymous namespace

SEASTAR_TEST_CASE(test_ungzip_simple) {
    return async([] {
        // Test simple gzip compression/decompression
        std::vector<char> original_data = {'H', 'e', 'l', 'l', 'o', ',', ' ', 'W', 'o', 'r', 'l', 'd', '!'};
        
        auto compressed = gzip_compress(original_data);
        auto chunked_compressed = to_chunked_content(compressed);
        
        auto decompressed_chunks = utils::ungzip(std::move(chunked_compressed), 1024).get();
        auto decompressed = from_chunked_content(decompressed_chunks);
        
        BOOST_REQUIRE_EQUAL(decompressed.size(), original_data.size());
        BOOST_CHECK(std::equal(decompressed.begin(), decompressed.end(), original_data.begin()));
    });
}

SEASTAR_TEST_CASE(test_ungzip_empty) {
    return async([] {
        // Test empty input
        std::vector<char> original_data;
        auto compressed = gzip_compress(original_data);
        auto chunked_compressed = to_chunked_content(compressed);
        
        auto decompressed_chunks = utils::ungzip(std::move(chunked_compressed), 1024).get();
        auto decompressed = from_chunked_content(decompressed_chunks);
        
        BOOST_CHECK_EQUAL(decompressed.size(), 0);
    });
}

SEASTAR_TEST_CASE(test_ungzip_large_data) {
    return async([] {
        // Test with larger data that compresses well
        std::vector<char> original_data(10000, 'A');
        original_data.insert(original_data.end(), 10000, 'B');
        original_data.insert(original_data.end(), 10000, 'C');
        
        auto compressed = gzip_compress(original_data);
        auto chunked_compressed = to_chunked_content(compressed);
        
        auto decompressed_chunks = utils::ungzip(std::move(chunked_compressed), 100000).get();
        auto decompressed = from_chunked_content(decompressed_chunks);
        
        BOOST_REQUIRE_EQUAL(decompressed.size(), original_data.size());
        BOOST_CHECK(std::equal(decompressed.begin(), decompressed.end(), original_data.begin()));
    });
}

SEASTAR_TEST_CASE(test_ungzip_concatenated) {
    return async([] {
        // Test multiple concatenated gzip files
        std::vector<char> data1 = {'H', 'e', 'l', 'l', 'o'};
        std::vector<char> data2 = {'W', 'o', 'r', 'l', 'd'};
        
        auto compressed1 = gzip_compress(data1);
        auto compressed2 = gzip_compress(data2);
        
        // Concatenate the compressed data
        std::vector<char> concatenated;
        concatenated.insert(concatenated.end(), compressed1.begin(), compressed1.end());
        concatenated.insert(concatenated.end(), compressed2.begin(), compressed2.end());
        
        auto chunked_compressed = to_chunked_content(concatenated);
        
        auto decompressed_chunks = utils::ungzip(std::move(chunked_compressed), 1024).get();
        auto decompressed = from_chunked_content(decompressed_chunks);
        
        // Should decompress to "HelloWorld"
        std::vector<char> expected;
        expected.insert(expected.end(), data1.begin(), data1.end());
        expected.insert(expected.end(), data2.begin(), data2.end());
        
        BOOST_REQUIRE_EQUAL(decompressed.size(), expected.size());
        BOOST_CHECK(std::equal(decompressed.begin(), decompressed.end(), expected.begin()));
    });
}

SEASTAR_TEST_CASE(test_ungzip_multiple_concatenated) {
    return async([] {
        // Test multiple concatenated gzip files (more than 2)
        std::vector<std::vector<char>> parts = {
            {'A', 'B', 'C'},
            {'D', 'E', 'F'},
            {'G', 'H', 'I'},
            {'J', 'K', 'L'}
        };
        
        std::vector<char> concatenated;
        std::vector<char> expected;
        
        for (const auto& part : parts) {
            auto compressed = gzip_compress(part);
            concatenated.insert(concatenated.end(), compressed.begin(), compressed.end());
            expected.insert(expected.end(), part.begin(), part.end());
        }
        
        auto chunked_compressed = to_chunked_content(concatenated);
        
        auto decompressed_chunks = utils::ungzip(std::move(chunked_compressed), 1024).get();
        auto decompressed = from_chunked_content(decompressed_chunks);
        
        BOOST_REQUIRE_EQUAL(decompressed.size(), expected.size());
        BOOST_CHECK(std::equal(decompressed.begin(), decompressed.end(), expected.begin()));
    });
}

SEASTAR_TEST_CASE(test_ungzip_invalid_magic) {
    return async([] {
        // Test invalid gzip magic bytes
        std::vector<char> bad_data = {0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
        auto chunked = to_chunked_content(bad_data);
        
        BOOST_CHECK_THROW(
            utils::ungzip(std::move(chunked), 1024).get(),
            std::runtime_error
        );
    });
}

SEASTAR_TEST_CASE(test_ungzip_truncated) {
    return async([] {
        // Test truncated gzip data
        std::vector<char> original_data = {'H', 'e', 'l', 'l', 'o'};
        auto compressed = gzip_compress(original_data);
        
        // Truncate the compressed data
        compressed.resize(compressed.size() / 2);
        auto chunked = to_chunked_content(compressed);
        
        BOOST_CHECK_THROW(
            utils::ungzip(std::move(chunked), 1024).get(),
            std::runtime_error
        );
    });
}

SEASTAR_TEST_CASE(test_ungzip_corrupted) {
    return async([] {
        // Test corrupted gzip data
        std::vector<char> original_data = {'H', 'e', 'l', 'l', 'o', ' ', 'W', 'o', 'r', 'l', 'd'};
        auto compressed = gzip_compress(original_data);
        
        // Corrupt some bytes in the middle
        if (compressed.size() > 20) {
            compressed[15] ^= 0xFF;
            compressed[16] ^= 0xFF;
        }
        
        auto chunked = to_chunked_content(compressed);
        
        BOOST_CHECK_THROW(
            utils::ungzip(std::move(chunked), 1024).get(),
            std::runtime_error
        );
    });
}

SEASTAR_TEST_CASE(test_ungzip_junk_appended) {
    return async([] {
        // Test gzip data with junk appended
        std::vector<char> original_data = {'H', 'e', 'l', 'l', 'o'};
        auto compressed = gzip_compress(original_data);
        
        // Append junk
        std::vector<char> junk = {'J', 'U', 'N', 'K'};
        compressed.insert(compressed.end(), junk.begin(), junk.end());
        
        auto chunked = to_chunked_content(compressed);
        
        BOOST_CHECK_THROW(
            utils::ungzip(std::move(chunked), 1024).get(),
            std::runtime_error
        );
    });
}

SEASTAR_TEST_CASE(test_ungzip_length_limit_exceeded) {
    return async([] {
        // Test length limit enforcement
        std::vector<char> original_data(1000, 'A');
        auto compressed = gzip_compress(original_data);
        auto chunked = to_chunked_content(compressed);
        
        // Set limit lower than actual size
        BOOST_CHECK_THROW(
            utils::ungzip(std::move(chunked), 500).get(),
            std::runtime_error
        );
    });
}

SEASTAR_TEST_CASE(test_ungzip_length_limit_exact) {
    return async([] {
        // Test that exact limit works
        std::vector<char> original_data(1000, 'B');
        auto compressed = gzip_compress(original_data);
        auto chunked = to_chunked_content(compressed);
        
        // Set limit to exact size
        auto decompressed_chunks = utils::ungzip(std::move(chunked), 1000).get();
        auto decompressed = from_chunked_content(decompressed_chunks);
        
        BOOST_CHECK_EQUAL(decompressed.size(), 1000);
    });
}

SEASTAR_TEST_CASE(test_ungzip_very_short_input) {
    return async([] {
        // Test with input too short to be valid gzip
        std::vector<char> bad_data = {0x1f, 0x8b};
        auto chunked = to_chunked_content(bad_data);
        
        BOOST_CHECK_THROW(
            utils::ungzip(std::move(chunked), 1024).get(),
            std::runtime_error
        );
    });
}

SEASTAR_TEST_CASE(test_ungzip_empty_input) {
    return async([] {
        // Test with completely empty input
        rjson::chunked_content empty;
        
        BOOST_CHECK_THROW(
            utils::ungzip(std::move(empty), 1024).get(),
            std::runtime_error
        );
    });
}

SEASTAR_TEST_CASE(test_ungzip_chunked_input) {
    return async([] {
        // Test with input split across multiple chunks
        std::vector<char> original_data = {'H', 'e', 'l', 'l', 'o', ' ', 'W', 'o', 'r', 'l', 'd', '!'};
        auto compressed = gzip_compress(original_data);
        
        // Split compressed data into multiple chunks
        rjson::chunked_content chunked;
        size_t chunk_size = compressed.size() / 3 + 1;
        for (size_t i = 0; i < compressed.size(); i += chunk_size) {
            size_t this_chunk_size = std::min(chunk_size, compressed.size() - i);
            temporary_buffer<char> buf(this_chunk_size);
            std::memcpy(buf.get_write(), compressed.data() + i, this_chunk_size);
            chunked.push_back(std::move(buf));
        }
        
        auto decompressed_chunks = utils::ungzip(std::move(chunked), 1024).get();
        auto decompressed = from_chunked_content(decompressed_chunks);
        
        BOOST_REQUIRE_EQUAL(decompressed.size(), original_data.size());
        BOOST_CHECK(std::equal(decompressed.begin(), decompressed.end(), original_data.begin()));
    });
}
