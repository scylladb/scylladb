/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/sstring.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/net/api.hh>
#include <vector>

namespace vector_search {

inline seastar::sstring response_content_to_sstring(const std::vector<seastar::temporary_buffer<char>>& buffers) {
    seastar::sstring result;
    for (const auto& buf : buffers) {
        result.append(buf.get(), buf.size());
    }
    return result;
}

inline seastar::net::tcp_keepalive_params get_keepalive_parameters(std::chrono::milliseconds timeout) {
    constexpr unsigned retry_count = 3;
    constexpr auto min_interval = std::chrono::seconds(1);

    // Divide total time by (count + 2) to balance Probing vs. Idle (~60/40)
    auto target_duration = std::chrono::duration_cast<std::chrono::seconds>(timeout);
    auto interval = target_duration / (retry_count + 2);

    // Ensure minimum 1s interval
    if (interval < min_interval) {
        interval = min_interval;
    }

    auto idle = target_duration - (interval * retry_count);

    return seastar::net::tcp_keepalive_params{
            .idle = idle,
            .interval = interval,
            .count = retry_count,
    };
}


} // namespace vector_search
