/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "clocks-impl.hh"
#include "utils/hashing.hh"

#include <seastar/core/lowres_clock.hh>

#include <chrono>
#include <optional>

class gc_clock final {
public:
    using base = seastar::lowres_system_clock;
    using rep = int64_t;
    using period = std::ratio<1, 1>; // seconds
    using duration = std::chrono::duration<rep, period>;
    using time_point = std::chrono::time_point<gc_clock, duration>;

    static constexpr auto is_steady = base::is_steady;

    static constexpr std::time_t to_time_t(time_point t) {
        return std::chrono::duration_cast<std::chrono::seconds>(t.time_since_epoch()).count();
    }

    static constexpr time_point from_time_t(std::time_t t) {
        return time_point(std::chrono::duration_cast<duration>(std::chrono::seconds(t)));
    }

    static time_point now() noexcept {
        return time_point(std::chrono::duration_cast<duration>(base::now().time_since_epoch())) + get_clocks_offset();
    }

    static int32_t as_int32(duration d) {
        auto count = d.count();
        int32_t count_32 = static_cast<int32_t>(count);
        if (count_32 != count) {
            throw std::runtime_error("Duration too big");
        }
        return count_32;
    }

    static int32_t as_int32(time_point tp) {
        return as_int32(tp.time_since_epoch());
    }
};

using expiry_opt = std::optional<gc_clock::time_point>;
using ttl_opt = std::optional<gc_clock::duration>;

// 20 years in seconds
static constexpr gc_clock::duration max_ttl = gc_clock::duration{20 * 365 * 24 * 60 * 60};

template<>
struct appending_hash<gc_clock::time_point> {
    template<typename Hasher>
    void operator()(Hasher& h, gc_clock::time_point t) const noexcept {
        // Remain backwards-compatible with the 32-bit duration::rep (refs #4460).
        uint64_t d64 = t.time_since_epoch().count();
        feed_hash(h, uint32_t(d64 & 0xffff'ffff));
        uint32_t msb = d64 >> 32;
        if (msb) {
            feed_hash(h, msb);
        }
    }
};


namespace ser {

// Forward-declaration - defined in serializer.hh, to avoid including it here.

template <typename Output>
void serialize_gc_clock_duration_value(Output& out, int64_t value);

template <typename Input>
int64_t deserialize_gc_clock_duration_value(Input& in);

template <typename T>
struct serializer;

template <>
struct serializer<gc_clock::duration> {
    template <typename Input>
    static gc_clock::duration read(Input& in) {
        return gc_clock::duration(deserialize_gc_clock_duration_value(in));
    }

    template <typename Output>
    static void write(Output& out, gc_clock::duration d) {
        serialize_gc_clock_duration_value(out, d.count());
    }

    template <typename Input>
    static void skip(Input& in) {
        read(in);
    }
};

}

template<>
struct fmt::formatter<gc_clock::time_point> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(gc_clock::time_point, fmt::format_context& ctx) const -> decltype(ctx.out());
};
