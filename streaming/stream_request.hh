/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <seastar/core/sstring.hh>
#include "interval.hh"
#include "partition_range_compat.hh"
#include <vector>

namespace streaming {

class stream_request {
public:
    using token = dht::token;
    sstring keyspace;
    dht::token_range_vector ranges;
    // For compatibility with <= 1.5, we send wrapping ranges (though they will never wrap).
    std::vector<wrapping_interval<token>> ranges_compat() const {
        return ::compat::wrap(ranges);
    }
    std::vector<sstring> column_families;
    stream_request() = default;
    stream_request(sstring _keyspace, dht::token_range_vector _ranges, std::vector<sstring> _column_families)
        : keyspace(std::move(_keyspace))
        , ranges(std::move(_ranges))
        , column_families(std::move(_column_families)) {
    }
    stream_request(sstring _keyspace, std::vector<wrapping_interval<token>> _ranges, std::vector<sstring> _column_families)
        : stream_request(std::move(_keyspace), ::compat::unwrap(std::move(_ranges)), std::move(_column_families)) {
    }
};

} // namespace streaming

template <> struct fmt::formatter<streaming::stream_request> : fmt::formatter<string_view> {
    auto format(const streaming::stream_request&, fmt::format_context& ctx) const -> decltype(ctx.out());
};
