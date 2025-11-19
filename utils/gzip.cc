/*
 * Copyright 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "utils/gzip.hh"
#include <libdeflate.h>
#include <seastar/core/coroutine.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/thread.hh>
#include <stdexcept>

using namespace seastar;

namespace utils {

namespace {

// Maximum size for a single output chunk (1 MB)
constexpr size_t MAX_OUTPUT_CHUNK_SIZE = 1024 * 1024;

// Build a contiguous buffer from chunks for decompression
// This is necessary because libdeflate requires complete input
// We build the buffer incrementally and free input chunks as we go
std::vector<char> build_input_buffer(rjson::chunked_content& chunks) {
    size_t total_size = 0;
    for (const auto& chunk : chunks) {
        total_size += chunk.size();
    }
    
    std::vector<char> result;
    result.reserve(total_size);
    
    for (auto& chunk : chunks) {
        result.insert(result.end(), chunk.begin(), chunk.end());
        // Free the chunk immediately after copying to save memory
        chunk = temporary_buffer<char>();
    }
    chunks.clear();
    
    return result;
}

} // anonymous namespace

future<rjson::chunked_content> ungzip(rjson::chunked_content&& compressed_body, size_t length_limit) {
    // Use thread context for potentially blocking operations
    return seastar::async([compressed_body = std::move(compressed_body), length_limit] () mutable {
        if (compressed_body.empty()) {
            throw std::runtime_error("Invalid gzip data: empty input");
        }
        
        // Build input buffer from chunks, freeing them as we go
        // Unfortunately, libdeflate requires the complete compressed input at once
        std::vector<char> compressed_data = build_input_buffer(compressed_body);
        
        if (compressed_data.empty()) {
            throw std::runtime_error("Invalid gzip data: empty input");
        }
        
        // Create decompressor
        auto* decompressor = libdeflate_alloc_decompressor();
        if (!decompressor) {
            throw std::bad_alloc();
        }
        
        // RAII wrapper for decompressor
        auto decompressor_deleter = [](libdeflate_decompressor* d) {
            if (d) {
                libdeflate_free_decompressor(d);
            }
        };
        std::unique_ptr<libdeflate_decompressor, decltype(decompressor_deleter)> decompressor_guard(
            decompressor, decompressor_deleter);
        
        rjson::chunked_content result;
        size_t total_decompressed = 0;
        size_t input_offset = 0;
        
        // Process potentially multiple concatenated gzip members
        // libdeflate_gzip_decompress handles all gzip format details (headers, trailers, etc.)
        while (input_offset < compressed_data.size()) {
            const char* current_input = compressed_data.data() + input_offset;
            size_t remaining_input = compressed_data.size() - input_offset;
            
            // Check if we've reached the limit before starting decompression
            if (total_decompressed >= length_limit) {
                throw std::runtime_error("Decompressed data exceeds length limit");
            }
            
            // Allocate output buffer - start with a reasonable size and grow if needed
            // Limit chunk size to avoid allocating too much at once
            const size_t initial_chunk_size = std::min({
                size_t(MAX_OUTPUT_CHUNK_SIZE),
                length_limit - total_decompressed,
                remaining_input * 10  // Heuristic: decompressed size often < 10x compressed
            });
            std::vector<char> output_buffer(initial_chunk_size);
            
            size_t actual_in_bytes = 0;
            size_t actual_out_bytes = 0;
            
            // Try decompression with progressively larger output buffers if needed
            libdeflate_result res;
            size_t max_output_size = length_limit - total_decompressed;
            
            for (size_t attempt = 0; attempt < 10; ++attempt) {
                res = libdeflate_gzip_decompress(
                    decompressor,
                    current_input,
                    remaining_input,
                    output_buffer.data(),
                    output_buffer.size(),
                    &actual_in_bytes,
                    &actual_out_bytes
                );
                
                if (res == LIBDEFLATE_SUCCESS) {
                    break;
                } else if (res == LIBDEFLATE_INSUFFICIENT_SPACE) {
                    // Need a larger output buffer
                    size_t new_size = std::min(output_buffer.size() * 2, max_output_size);
                    if (new_size <= output_buffer.size()) {
                        throw std::runtime_error("Decompressed data exceeds length limit");
                    }
                    output_buffer.resize(new_size);
                } else {
                    // Other error (bad data, short input, etc.)
                    break;
                }
            }
            
            if (res != LIBDEFLATE_SUCCESS) {
                if (res == LIBDEFLATE_BAD_DATA) {
                    throw std::runtime_error("Invalid gzip data: corrupt or truncated");
                } else if (res == LIBDEFLATE_SHORT_OUTPUT) {
                    throw std::runtime_error("Decompressed data exceeds length limit");
                } else if (res == LIBDEFLATE_INSUFFICIENT_SPACE) {
                    throw std::runtime_error("Decompressed data exceeds length limit");
                } else {
                    throw std::runtime_error("Gzip decompression failed");
                }
            }
            
            // libdeflate_gzip_decompress returns how many bytes were consumed
            // This includes the entire gzip member (header, compressed data, and trailer)
            if (actual_in_bytes == 0) {
                throw std::runtime_error("Invalid gzip data: no bytes consumed");
            }
            
            // Check total size limit
            total_decompressed += actual_out_bytes;
            if (total_decompressed > length_limit) {
                throw std::runtime_error("Decompressed data exceeds length limit");
            }
            
            // Move decompressed data into temporary_buffer chunks
            // Split into reasonably-sized chunks to avoid holding too much contiguous memory
            size_t offset = 0;
            while (offset < actual_out_bytes) {
                size_t chunk_size = std::min(MAX_OUTPUT_CHUNK_SIZE, actual_out_bytes - offset);
                temporary_buffer<char> chunk(chunk_size);
                std::memcpy(chunk.get_write(), output_buffer.data() + offset, chunk_size);
                result.push_back(std::move(chunk));
                offset += chunk_size;
            }
            
            // Move to the next gzip member
            input_offset += actual_in_bytes;
            
            // Yield to the reactor periodically
            seastar::thread::maybe_yield();
        }
        
        // Check if we consumed all input
        if (input_offset != compressed_data.size()) {
            throw std::runtime_error("Invalid gzip data: unconsumed trailing data");
        }
        
        return result;
    });
}

} // namespace utils
