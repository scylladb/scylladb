/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <seastar/core/byteorder.hh>
#include <fmt/format.h>
#include "dht/decorated_key.hh"
#include "replica/logstor/key_utils.hh"
#include "mutation/canonical_mutation.hh"
#include "mutation/timestamp.hh"

namespace replica::logstor {

struct log_segment_id {
    uint32_t value;

    bool operator==(const log_segment_id& other) const noexcept = default;
    auto operator<=>(const log_segment_id& other) const noexcept = default;
};

struct log_location {
    log_segment_id segment;
    uint32_t offset;
    uint32_t size;

    bool operator==(const log_location& other) const noexcept = default;
};

struct primary_index_key {
    // Keep the same serialized 20-byte layout as the full key hash while caching
    // the token separately for the primary index hot path. On disk this is still:
    //   [ 8-byte token prefix ][ remaining hash suffix ]
    static constexpr size_t token_prefix_size = sizeof(int64_t);
    static constexpr size_t key_hash_suffix_size = key_hash_size - token_prefix_size;
    using key_hash_suffix = std::array<uint8_t, key_hash_suffix_size>;

    int64_t _token_raw = 0;
    key_hash_suffix _hash_suffix{};

    primary_index_key() = default;

    explicit primary_index_key(key_hash hash)
        : _token_raw(token_from_key_hash(hash).raw()) {
        std::copy_n(hash.begin() + token_prefix_size, _hash_suffix.size(), _hash_suffix.begin());
    }

    primary_index_key(const schema& s, const dht::decorated_key& dk);

    // For testing only - constructs a primary_index_key with the given token from the decorated_key,
    // bypassing the normal token-from-hash check.
    static primary_index_key with_forced_token(const schema& s, const dht::decorated_key& dk) {
        primary_index_key key(compute_key_hash(s, dk.key().view()));
        key._token_raw = dk.token().raw();
        return key;
    }

    dht::token token() const noexcept {
        return dht::token::from_int64(_token_raw);
    }

    key_hash hash() const noexcept {
        key_hash full_hash;
        write_be<int64_t>(reinterpret_cast<char*>(full_hash.data()), _token_raw);
        std::copy(_hash_suffix.begin(), _hash_suffix.end(), full_hash.begin() + token_prefix_size);
        return full_hash;
    }

    bool operator==(const primary_index_key& other) const noexcept {
        return _token_raw == other._token_raw && _hash_suffix == other._hash_suffix;
    }

    std::strong_ordering operator<=>(const primary_index_key& other) const noexcept {
        if (auto cmp = dht::tri_compare_raw(_token_raw, other._token_raw); cmp != 0) {
            return cmp;
        }
        return _hash_suffix <=> other._hash_suffix;
    }
};

struct index_entry {
    log_location location;
    api::timestamp_type timestamp;

    bool operator==(const index_entry& other) const noexcept = default;
};

struct log_record_header {
    primary_index_key key;
    api::timestamp_type timestamp;
    table_id table;
};

struct log_record {
    log_record_header header;
    canonical_mutation mut;
};

struct segment_sequence {
    uint64_t value;

    bool operator==(const segment_sequence& other) const noexcept = default;
    auto operator<=>(const segment_sequence& other) const noexcept = default;

    segment_sequence& operator++() noexcept {
        ++value;
        return *this;
    }

    segment_sequence operator++(int) noexcept {
        segment_sequence tmp = *this;
        ++value;
        return tmp;
    }

    segment_sequence operator+(uint64_t increment) const noexcept {
        return segment_sequence{value + increment};
    }
};

enum class segment_kind : uint8_t {
    mixed = 0,
    full = 1,
};

}

// Format specialization declarations and implementations
template <>
struct fmt::formatter<replica::logstor::log_segment_id> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const replica::logstor::log_segment_id& id, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "segment({})", id.value);
    }
};

template <>
struct fmt::formatter<replica::logstor::log_location> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const replica::logstor::log_location& loc, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{{segment:{}, offset:{}, size:{}}}",
                             loc.segment, loc.offset, loc.size);
    }
};

template <>
struct fmt::formatter<replica::logstor::primary_index_key> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const replica::logstor::primary_index_key& key, FormatContext& ctx) const {
        auto out = fmt::format_to(ctx.out(), "{{token: {}, key_hash: ", key.token());
        auto hash = key.hash();
        for (auto b : hash) {
            out = fmt::format_to(out, "{:02x}", b);
        }
        return fmt::format_to(out, "}}");
    }
};

template <>
struct fmt::formatter<replica::logstor::segment_sequence> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const replica::logstor::segment_sequence& seq, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "sseq({})", seq.value);
    }
};
