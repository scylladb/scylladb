/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <functional>
#include <compare>

#include "timestamp.hh"
#include "gc_clock.hh"
#include "utils/hashing.hh"

/**
 * Represents deletion operation. Can be commuted with other tombstones via apply() method.
 * Can be empty.
 */
struct tombstone final {
    api::timestamp_type timestamp;
    gc_clock::time_point deletion_time;

    tombstone(api::timestamp_type timestamp, gc_clock::time_point deletion_time)
        : timestamp(timestamp)
        , deletion_time(deletion_time)
    { }

    tombstone()
        : tombstone(api::missing_timestamp, {})
    { }

    std::strong_ordering operator<=>(const tombstone& t) const = default;
    bool operator==(const tombstone&) const = default;

    explicit operator bool() const {
        return timestamp != api::missing_timestamp;
    }

    void apply(const tombstone& t) noexcept {
        if (*this < t) {
            *this = t;
        }
    }

    // See reversibly_mergeable.hh
    void apply_reversibly(tombstone& t) noexcept {
        std::swap(*this, t);
        apply(t);
    }

    // See reversibly_mergeable.hh
    void revert(tombstone& t) noexcept {
        std::swap(*this, t);
    }

    tombstone operator+(const tombstone& t) {
        auto result = *this;
        result.apply(t);
        return result;
    }
};

template <>
struct fmt::formatter<tombstone> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const tombstone& t, FormatContext& ctx) const {
        if (t) {
            return fmt::format_to(ctx.out(),
                                  "{{tombstone: timestamp={}, deletion_time={}}}",
                                  t.timestamp, t.deletion_time.time_since_epoch().count());
        } else {
            return fmt::format_to(ctx.out(),
                                  "{{tombstone: none}}");
        }
     }
};

static inline std::ostream& operator<<(std::ostream& out, const tombstone& t) {
    fmt::print(out, "{}", t);
    return out;
}

template<>
struct appending_hash<tombstone> {
    template<typename Hasher>
    void operator()(Hasher& h, const tombstone& t) const {
        feed_hash(h, t.timestamp);
        feed_hash(h, t.deletion_time);
    }
};

// Determines whether tombstone may be GC-ed.
using can_gc_fn = std::function<bool(tombstone)>;

extern can_gc_fn always_gc;
