/*
 * Copyright (C) 2026-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */
#pragma once

// Lightweight header with the small POD types from tracing/tracing.hh
// that are needed by reader_permit.hh, query/query-request.hh, and
// other widely-included headers.  Does NOT pull in the heavy tracing
// machinery (sharded<tracing>, reactor, metrics, ...).

#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>
#include <chrono>
#include <utility>
#include <fmt/format.h>
#include "utils/UUID.hh"
#include "enum_set.hh"
#include "seastarx.hh"

namespace tracing {

using elapsed_clock = std::chrono::steady_clock;

enum class trace_type : uint8_t {
    NONE,
    QUERY,
    REPAIR,
};

extern std::vector<sstring> trace_type_names;

inline const sstring& type_to_string(trace_type t) {
    return trace_type_names.at(static_cast<int>(t));
}

/**
 * Returns a TTL for a given trace type
 * @param t trace type
 *
 * @return TTL
 */
inline std::chrono::seconds ttl_by_type(const trace_type t) {
    switch (t) {
    case trace_type::NONE:
    case trace_type::QUERY:
        return std::chrono::seconds(86400); // 1 day
    case trace_type::REPAIR:
        return std::chrono::seconds(604800); // 7 days
    default:
        // unknown type value - must be a SW bug
        throw std::invalid_argument("unknown trace type: " + std::to_string(int(t)));
    }
}

/**
 * @brief represents an ID of a single tracing span.
 *
 * Currently span ID is a random 64-bit integer.
 */
class span_id {
private:
    uint64_t _id = illegal_id;

public:
    static constexpr uint64_t illegal_id = 0;

public:
    span_id() = default;
    uint64_t get_id() const {
        return _id;
    }
    span_id(uint64_t id)
        : _id(id) {
    }

    /**
     * @return New span_id with a random legal value
     */
    static span_id make_span_id();
};

// !!!!IMPORTANT!!!!
//
// The enum_set based on this enum is serialized using IDL, therefore new items
// should always be added to the end of this enum - never before the existing
// ones.
//
// Otherwise this may break IDL's backward compatibility.
enum class trace_state_props { write_on_close, primary, log_slow_query, full_tracing, ignore_events };

using trace_state_props_set = enum_set<super_enum<trace_state_props, trace_state_props::write_on_close, trace_state_props::primary,
        trace_state_props::log_slow_query, trace_state_props::full_tracing, trace_state_props::ignore_events>>;

class trace_info {
public:
    utils::UUID session_id;
    trace_type type;
    bool write_on_close;
    trace_state_props_set state_props;
    uint32_t slow_query_threshold_us; // in microseconds
    uint32_t slow_query_ttl_sec;      // in seconds
    span_id parent_id;
    uint64_t start_ts_us = 0u; // sentinel value (== "unset")

public:
    trace_info(utils::UUID sid, trace_type t, bool w_o_c, trace_state_props_set s_p, uint32_t slow_query_threshold, uint32_t slow_query_ttl, span_id p_id,
            uint64_t s_t_u)
        : session_id(std::move(sid))
        , type(t)
        , write_on_close(w_o_c)
        , state_props(s_p)
        , slow_query_threshold_us(slow_query_threshold)
        , slow_query_ttl_sec(slow_query_ttl)
        , parent_id(std::move(p_id))
        , start_ts_us(s_t_u) {
        state_props.set_if<trace_state_props::write_on_close>(write_on_close);
    }
};

} // namespace tracing

// Forward declaration only - full definition is in tracing/trace_state.hh
namespace tracing {
class trace_state_ptr;
}

template <>
struct fmt::formatter<tracing::span_id> {
    constexpr auto parse(format_parse_context& ctx) {
        return ctx.begin();
    }
    auto format(const tracing::span_id& id, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{}", id.get_id());
    }
};
