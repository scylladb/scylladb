/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <seastar/core/sstring.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/net/api.hh>
#include <algorithm>
#include <chrono>
#include <vector>

namespace vector_search {

inline seastar::sstring response_content_to_sstring(const std::vector<seastar::temporary_buffer<char>>& buffers) {
    size_t total_size = 0;
    for (const auto& buf : buffers) {
        total_size += buf.size();
    }
    auto result = seastar::uninitialized_string(total_size);
    size_t pos = 0;
    for (const auto& buf : buffers) {
        std::copy(buf.get(), buf.get() + buf.size(), result.data() + pos);
        pos += buf.size();
    }
    return result;
}

inline seastar::net::tcp_keepalive_params get_keepalive_parameters(std::chrono::milliseconds timeout) {
    constexpr unsigned min_count = 1;
    constexpr unsigned max_count = 3;
    constexpr auto min_interval = std::chrono::seconds(1);

    auto timeout_s = std::chrono::duration_cast<std::chrono::seconds>(timeout);

    if (timeout_s < min_interval) {
        return seastar::net::tcp_keepalive_params{
                .idle = std::chrono::seconds(0),
                .interval = min_interval,
                .count = min_count,
        };
    }

    auto count = std::max(min_count, std::min(max_count, static_cast<unsigned>(timeout_s / min_interval) - 1));
    auto interval = std::max(min_interval, timeout_s / (count + 1));
    auto idle = timeout_s - (interval * count);
    if (idle < std::chrono::seconds(0)) {
        idle = std::chrono::seconds(0);
    }

    return seastar::net::tcp_keepalive_params{
            .idle = idle,
            .interval = interval,
            .count = count,
    };
}

} // namespace vector_search
