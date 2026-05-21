/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include <cstdint>
#include <fmt/format.h>
#include "dht/decorated_key.hh"
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
    dht::decorated_key dk;
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

template <>
struct fmt::formatter<replica::logstor::segment_sequence> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const replica::logstor::segment_sequence& seq, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "sseq({})", seq.value);
    }
};
