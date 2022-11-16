/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <zlib.h>
#include <libdeflate.h>
#include "utils/gz/crc_combine.hh"

template<typename Checksum>
concept ChecksumUtils = requires(const char* input, size_t size, uint32_t checksum) {
    { Checksum::init_checksum() } -> std::same_as<uint32_t>;
    { Checksum::checksum(input, size) } -> std::same_as<uint32_t>;
    { Checksum::checksum(checksum, input, size) } -> std::same_as<uint32_t>;
    { Checksum::checksum_combine(checksum, checksum, size) } -> std::same_as<uint32_t>;

    // Tells whether checksum_combine() should be preferred over checksum().
    // For same checksummers it's faster to re-feed the buffer to checksum() than to
    // combine the checksum of the buffer.
    { Checksum::prefer_combine() } -> std::same_as<bool>;
};

struct adler32_utils {
    inline static uint32_t init_checksum() {
        return adler32(0, Z_NULL, 0);
    }

    inline static uint32_t checksum(const char* input, size_t input_len) {
        auto init = adler32(0, Z_NULL, 0);
        return checksum(init, input, input_len);
    }

    inline static uint32_t checksum(uint32_t prev, const char* input, size_t input_len) {
        // yuck, zlib uses unsigned char while we use char :-(
        return adler32(prev, reinterpret_cast<const unsigned char *>(input),
                input_len);
    }

    inline static uint32_t checksum_combine(uint32_t first, uint32_t second, size_t input_len2) {
        return adler32_combine(first, second, input_len2);
    }

    static constexpr bool prefer_combine() { return true; }
};

struct zlib_crc32_checksummer {
    inline static uint32_t init_checksum() {
        return crc32(0, Z_NULL, 0);
    }

    inline static uint32_t checksum(const char* input, size_t input_len) {
        auto init = crc32(0, Z_NULL, 0);
        return checksum(init, input, input_len);
    }

    inline static uint32_t checksum(uint32_t prev, const char* input, size_t input_len) {
        // yuck, zlib uses unsigned char while we use char :-(
        return crc32(prev, reinterpret_cast<const unsigned char *>(input),
                input_len);
    }

    inline static uint32_t checksum_combine(uint32_t first, uint32_t second, size_t input_len2) {
        return crc32_combine(first, second, input_len2);
    }

    static constexpr bool prefer_combine() { return false; } // crc32_combine() is very slow
};

struct libdeflate_crc32_checksummer {
    static uint32_t init_checksum() {
        return 0;
    }

    static uint32_t checksum(const char* input, size_t input_len) {
        return checksum(init_checksum(), input, input_len);
    }

    static uint32_t checksum(uint32_t prev, const char* input, size_t input_len) {
        return libdeflate_crc32(prev, input, input_len);
    }

    static uint32_t checksum_combine(uint32_t first, uint32_t second, size_t input_len2) {
        return zlib_crc32_checksummer::checksum_combine(first, second, input_len2);
    }

    static constexpr bool prefer_combine() { return false; }
};

template<typename Checksum>
inline uint32_t checksum_combine_or_feed(uint32_t first, uint32_t second, const char* input, size_t input_len) {
    if constexpr (Checksum::prefer_combine()) {
        return Checksum::checksum_combine(first, second, input_len);
    } else {
        return Checksum::checksum(first, input, input_len);
    }
}

struct crc32_utils {
    static uint32_t init_checksum() { return libdeflate_crc32_checksummer::init_checksum(); }

    static uint32_t checksum(const char* input, size_t input_len) {
        return libdeflate_crc32_checksummer::checksum(input, input_len);
    }

    static uint32_t checksum(uint32_t prev, const char* input, size_t input_len) {
        return libdeflate_crc32_checksummer::checksum(prev, input, input_len);
    }

    static uint32_t checksum_combine(uint32_t first, uint32_t second, size_t input_len2) {
        return fast_crc32_combine(first, second, input_len2);
    }

    static constexpr bool prefer_combine() {
        return fast_crc32_combine_optimized();
    }
};
