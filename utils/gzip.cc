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

// GZIP header constants
constexpr uint8_t GZIP_ID1 = 0x1f;
constexpr uint8_t GZIP_ID2 = 0x8b;
constexpr uint8_t GZIP_CM_DEFLATE = 0x08;

// GZIP header flags
constexpr uint8_t GZIP_FTEXT = 0x01;
constexpr uint8_t GZIP_FHCRC = 0x02;
constexpr uint8_t GZIP_FEXTRA = 0x04;
constexpr uint8_t GZIP_FNAME = 0x08;
constexpr uint8_t GZIP_FCOMMENT = 0x10;

// Linearize chunked_content into a contiguous buffer
std::vector<char> linearize_chunked_content(const rjson::chunked_content& chunks) {
    size_t total_size = 0;
    for (const auto& chunk : chunks) {
        total_size += chunk.size();
    }
    
    std::vector<char> result;
    result.reserve(total_size);
    
    for (const auto& chunk : chunks) {
        result.insert(result.end(), chunk.begin(), chunk.end());
    }
    
    return result;
}

// Read a uint16_t in little-endian format
uint16_t read_le16(const char* data) {
    const auto* p = reinterpret_cast<const uint8_t*>(data);
    return static_cast<uint16_t>(p[0]) | (static_cast<uint16_t>(p[1]) << 8);
}

// Read a uint32_t in little-endian format
uint32_t read_le32(const char* data) {
    const auto* p = reinterpret_cast<const uint8_t*>(data);
    return static_cast<uint32_t>(p[0]) | 
           (static_cast<uint32_t>(p[1]) << 8) |
           (static_cast<uint32_t>(p[2]) << 16) |
           (static_cast<uint32_t>(p[3]) << 24);
}

// Parse gzip header and return the offset to the compressed data
// Throws on invalid header
size_t parse_gzip_header(const char* data, size_t data_size) {
    if (data_size < 10) {
        throw std::runtime_error("Invalid gzip data: too short for header");
    }
    
    auto* p = reinterpret_cast<const uint8_t*>(data);
    
    // Check magic bytes
    if (p[0] != GZIP_ID1 || p[1] != GZIP_ID2) {
        throw std::runtime_error("Invalid gzip data: bad magic bytes");
    }
    
    // Check compression method
    if (p[2] != GZIP_CM_DEFLATE) {
        throw std::runtime_error("Invalid gzip data: unsupported compression method");
    }
    
    uint8_t flags = p[3];
    size_t offset = 10; // Skip fixed header
    
    // Skip FEXTRA
    if (flags & GZIP_FEXTRA) {
        if (offset + 2 > data_size) {
            throw std::runtime_error("Invalid gzip data: truncated FEXTRA");
        }
        uint16_t xlen = read_le16(data + offset);
        offset += 2;
        offset += xlen;
        if (offset > data_size) {
            throw std::runtime_error("Invalid gzip data: truncated FEXTRA");
        }
    }
    
    // Skip FNAME (null-terminated string)
    if (flags & GZIP_FNAME) {
        while (offset < data_size && p[offset] != 0) {
            offset++;
        }
        if (offset >= data_size) {
            throw std::runtime_error("Invalid gzip data: truncated FNAME");
        }
        offset++; // Skip null terminator
    }
    
    // Skip FCOMMENT (null-terminated string)
    if (flags & GZIP_FCOMMENT) {
        while (offset < data_size && p[offset] != 0) {
            offset++;
        }
        if (offset >= data_size) {
            throw std::runtime_error("Invalid gzip data: truncated FCOMMENT");
        }
        offset++; // Skip null terminator
    }
    
    // Skip FHCRC
    if (flags & GZIP_FHCRC) {
        offset += 2;
        if (offset > data_size) {
            throw std::runtime_error("Invalid gzip data: truncated FHCRC");
        }
    }
    
    return offset;
}

} // anonymous namespace

future<rjson::chunked_content> ungzip(rjson::chunked_content&& compressed_body, size_t length_limit) {
    // Use thread context for potentially blocking operations
    return seastar::async([compressed_body = std::move(compressed_body), length_limit] () mutable {
        // Linearize the chunked input
        std::vector<char> compressed_data = linearize_chunked_content(compressed_body);
        
        // Free the input chunks to save memory
        compressed_body.clear();
        
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
        while (input_offset < compressed_data.size()) {
            const char* current_input = compressed_data.data() + input_offset;
            size_t remaining_input = compressed_data.size() - input_offset;
            
            // Parse the gzip header to check validity
            try {
                parse_gzip_header(current_input, remaining_input);
            } catch (const std::runtime_error& e) {
                // If we've successfully decompressed at least one member, 
                // then trailing junk is an error
                if (total_decompressed > 0) {
                    throw std::runtime_error(std::string("Invalid gzip data: junk after gzip data: ") + e.what());
                }
                throw;
            }
            
            // Allocate output buffer with an initial guess
            // We'll use a generous initial size and grow if needed
            const size_t initial_chunk_size = std::min(size_t(1024 * 1024), length_limit - total_decompressed);
            std::vector<char> output_buffer(initial_chunk_size);
            
            size_t actual_in_bytes = 0;
            size_t actual_out_bytes = 0;
            
            // Try decompression with progressively larger output buffers
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
            // This includes header, compressed data, and trailer
            if (actual_in_bytes == 0) {
                throw std::runtime_error("Invalid gzip data: no bytes consumed");
            }
            
            // Check total size limit
            total_decompressed += actual_out_bytes;
            if (total_decompressed > length_limit) {
                throw std::runtime_error("Decompressed data exceeds length limit");
            }
            
            // Move decompressed data into a temporary_buffer and add to result
            if (actual_out_bytes > 0) {
                temporary_buffer<char> chunk(actual_out_bytes);
                std::memcpy(chunk.get_write(), output_buffer.data(), actual_out_bytes);
                result.push_back(std::move(chunk));
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
