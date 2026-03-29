/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include <cstdint>
#include <fmt/format.h>
#include "mutation/canonical_mutation.hh"
#include "replica/logstor/utils.hh"
#include "dht/decorated_key.hh"

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
    dht::decorated_key dk;
};

using record_generation = generation_base<uint16_t>;
using segment_generation = generation_base<uint16_t>;

struct index_entry {
    log_location location;
    record_generation generation;

    bool operator==(const index_entry& other) const noexcept = default;
};

struct log_record_header {
    primary_index_key key;
    record_generation generation;
    table_id table;
};

struct log_record {
    log_record_header header;
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
struct fmt::formatter<replica::logstor::primary_index_key> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const replica::logstor::primary_index_key& key, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{}", key.dk);
    }
};
