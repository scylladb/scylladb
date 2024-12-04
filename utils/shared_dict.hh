/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#define ZSTD_STATIC_LINKING_ONLY
#include <zstd.h>
#define LZ4_STATIC_LINKING_ONLY
#include <lz4.h>

#include "utils/UUID.hh"

#include <memory>

namespace utils {

using sha256_type = std::array<std::byte, 32>;

// For performance reasons (cache pressure), it is desirable to have only
// one instance of a particular dictionary on a node.
//
// `shared_dict` takes a raw dictionary buffer (which preferably contains
// a dictionary in zstd format, but any content is fine), and wraps around
// it with compressor-specific dictionary types. (Each compressor attached
// some algorithm-specific hash indices and entropy tables to it).
//
// This way different compressors and decompressors can share the same
// raw dictionary buffer.
//
// Dictionaries are always read-only, so it's fine (and strongly preferable)
// to share this object between shards.
struct shared_dict {
    struct dict_id {
        uint64_t timestamp = 0;
        UUID origin_node{};
        sha256_type content_sha256{};
        bool operator==(const dict_id&) const = default;
    };
    dict_id id{};
    std::vector<std::byte> data;
    std::unique_ptr<ZSTD_DDict, decltype(&ZSTD_freeDDict)> zstd_ddict{nullptr, ZSTD_freeDDict};
    std::unique_ptr<ZSTD_CDict, decltype(&ZSTD_freeCDict)> zstd_cdict{nullptr, ZSTD_freeCDict};
    std::unique_ptr<LZ4_stream_t, decltype(&LZ4_freeStream)> lz4_cdict{nullptr, LZ4_freeStream};
    std::span<const std::byte> lz4_ddict;
    // I got burned by an LZ4 bug (`<` used instead of `<=`) once when dealing with exactly 64 kiB,
    // prefixes, so I'm using 64 kiB - 1 because of the trauma.
    // But 64 kiB would probably work for this use case too.
    constexpr static size_t max_lz4_dict_size = 64 * 1024 - 1;
    shared_dict() = default;
    shared_dict(std::span<const std::byte> d, uint64_t timestamp, UUID origin_node, int zstd_compression_level = 1);
};

} // namespace utils
