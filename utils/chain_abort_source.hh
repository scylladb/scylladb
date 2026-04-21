/*
* Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <seastar/core/abort_source.hh>

using namespace seastar;

namespace utils {

// Subscribe target to source and return the corresponding subscription.
//
// If the passed source has already been triggered, it will immediately trigger target.
inline auto chain_abort_source(abort_source& target, abort_source& source) {
    if (source.abort_requested()) {
        target.request_abort_ex(source.abort_requested_exception_ptr());
    }

    return source.subscribe([&target] (const std::optional<std::exception_ptr>& eptr) noexcept {
        target.request_abort_ex(eptr.value_or(target.get_default_exception()));
    });
}

}
