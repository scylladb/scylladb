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
#include "replica/logstor/key_utils.hh"
#include "bytes_ostream.hh"
#include "mutation/timestamp.hh"

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

struct primary_index_key {
    dht::token _token;
    key_hash _hash;

    primary_index_key() = default;

    primary_index_key(dht::token token, key_hash hash)
        : _token(std::move(token))
        , _hash(std::move(hash)) {}

    explicit primary_index_key(const dht::decorated_key& dk);

    const dht::token& token() const noexcept {
        return _token;
    }

    const key_hash& hash() const noexcept {
        return _hash;
    }

    bool operator==(const primary_index_key& other) const noexcept = default;
    auto operator<=>(const primary_index_key& other) const noexcept = default;
};

struct index_entry {
    record_location location;
    api::timestamp_type timestamp;

    bool operator==(const index_entry& other) const noexcept = default;
};

struct record_header {
    dht::decorated_key key;
    api::timestamp_type timestamp;
    table_id table;

    // The key as the primary index knows it. Compaction and recovery derive it from the header,
    // since a record carries the partition key itself and never its hash.
    primary_index_key index_key() const {
        return primary_index_key(key);
    }

    bool operator==(const record_header& other) const noexcept {
        return key.token() == other.key.token()
            && key.key().representation() == other.key.key().representation()
            && timestamp == other.timestamp
            && table == other.table;
    }
};

// The value of a record: the partition it holds, encoded. The bytes are opaque everywhere but in
// replica/logstor/record_value.hh, where the two functions that encode and decode them live.
class record_value {
    bytes_ostream _data;

public:
    record_value() = default;
    explicit record_value(bytes_ostream data) noexcept
        : _data(std::move(data))
    { }
    // A copy of the value's bytes, as read from a record on disk.
    explicit record_value(bytes_view data) {
        _data.write(data);
    }

    size_t size() const noexcept { return _data.size(); }
    const bytes_ostream& representation() const noexcept { return _data; }

    bool operator==(const record_value& other) const noexcept { return _data == other._data; }
};

struct log_record {
    record_header header;
    record_value value;
};

struct log_record_bytes_view {
    bytes_view header;
    bytes_view value;
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

class space_accounting_subscriber {
public:
    virtual ~space_accounting_subscriber() = default;
    virtual void on_add_record(record_location location) noexcept = 0;
    virtual void on_free_record(record_location location) noexcept = 0;
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
struct fmt::formatter<replica::logstor::primary_index_key> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const replica::logstor::primary_index_key& key, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{{token: {}, key_hash: {:016x}{:016x}}}", key.token(), key.hash().high64, key.hash().low64);
    }
};

template <>
struct fmt::formatter<replica::logstor::segment_sequence> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const replica::logstor::segment_sequence& seq, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "sseq({})", seq.value);
    }
};
