/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/sstring.hh>
#include <seastar/core/temporary_buffer.hh>
#include <vector>

namespace vector_search {

inline seastar::sstring response_content_to_sstring(const std::vector<seastar::temporary_buffer<char>>& buffers) {
    seastar::sstring result;
    for (const auto& buf : buffers) {
        result.append(buf.get(), buf.size());
    }
    return result;
}

} // namespace vector_search
