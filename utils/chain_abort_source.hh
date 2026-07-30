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

/// Subscribe dependent_as to base_as and return the corresponding subscription.
///
/// If the passed base_as has already been triggered, it will immediately
/// trigger dependent_as.
[[nodiscard]]
inline optimized_optional<abort_source::subscription> chain_abort_source(
        abort_source& dependent_as,
        abort_source& base_as)
{
    if (base_as.abort_requested()) {
        dependent_as.request_abort_ex(base_as.abort_requested_exception_ptr());
    }

    return base_as.subscribe([&dependent_as] (const std::optional<std::exception_ptr>& eptr) noexcept {
        dependent_as.request_abort_ex(eptr.value_or(dependent_as.get_default_exception()));
    });
}

/// Subscribe dependent_as to base_as and return the corresponding subscription.
/// If the passed base_as is a nullptr, return a null optional.
///
/// If the passed base_as has already been triggered, it will immediately
/// trigger dependent_as.
[[nodiscard]]
inline optimized_optional<abort_source::subscription> chain_abort_source(
        abort_source& dependent_as,
        abort_source* base_as)
{
    return base_as == nullptr
            ? optimized_optional<abort_source::subscription>()
            : chain_abort_source(dependent_as, *base_as);
}

}
