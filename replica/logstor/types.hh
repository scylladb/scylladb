/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#pragma once

#include <cstdint>
#include <fmt/format.h>
#include "mutation/canonical_mutation.hh"
#include "replica/logstor/utils.hh"

namespace replica::logstor {

struct log_segment_id {
    uint32_t value;

    bool operator==(const log_segment_id& other) const noexcept = default;
};

struct log_location {
    log_segment_id segment;
    uint32_t offset;
    uint32_t size;

    bool operator==(const log_location& other) const noexcept = default;
};

struct index_key {
    static constexpr size_t digest_size = 20;

    std::array<uint8_t, digest_size> digest;

    bool operator==(const index_key& other) const noexcept = default;
    auto operator<=>(const index_key& other) const noexcept = default;
};

using record_generation = generation_base<uint16_t>;

struct index_entry {
    log_location location;
    record_generation generation;

    bool operator==(const index_entry& other) const noexcept = default;
};

struct log_record {
    index_key key;
    record_generation generation;
    canonical_mutation mut;
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
struct fmt::formatter<replica::logstor::index_key> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const replica::logstor::index_key& key, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{:02x}", fmt::join(key.digest, ""));
    }
};
