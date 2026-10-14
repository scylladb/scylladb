/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include <compare>
#include <cstdint>
#include <fmt/format.h>

// The identifiers and positions of segments and of the records in them. They depend on nothing
// else in logstor, so that headers included nearly everywhere, such as replica/database.hh,
// can take them without pulling in the rest of logstor.

namespace replica::logstor {

struct log_segment_id {
    uint32_t value;

    bool operator==(const log_segment_id& other) const noexcept = default;
    auto operator<=>(const log_segment_id& other) const noexcept = default;
};

struct segment_position {
    log_segment_id segment;
    uint32_t offset;

    bool operator==(const segment_position& other) const noexcept = default;
};

// Where a record frame lives. `size` is its frame size: the record frame header, the record header
// and the value, without the padding that follows the frame inside a chunk. It is what
// segment_manager::read() reads and what the space accounting counts.
struct record_location {
    log_segment_id segment;
    uint32_t offset;
    uint32_t size;

    bool operator==(const record_location& other) const noexcept = default;
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

template <>
struct fmt::formatter<replica::logstor::log_segment_id> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const replica::logstor::log_segment_id& id, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "segment({})", id.value);
    }
};

template <>
struct fmt::formatter<replica::logstor::segment_position> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const replica::logstor::segment_position& pos, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{{segment:{}, offset:{}}}", pos.segment, pos.offset);
    }
};

template <>
struct fmt::formatter<replica::logstor::record_location> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const replica::logstor::record_location& loc, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{{segment:{}, offset:{}, size:{}}}",
                             loc.segment, loc.offset, loc.size);
    }
};

template <>
struct fmt::formatter<replica::logstor::segment_sequence> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const replica::logstor::segment_sequence& seq, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "sseq({})", seq.value);
    }
};
