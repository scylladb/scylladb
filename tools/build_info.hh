/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#pragma once

#include "build_mode.hh"

namespace tools::build_info {
enum class build_type { debug = 0, release, dev, sanitize, coverage };

constexpr build_type get_build_type() {
#if defined(SCYLLA_BUILD_MODE_DEBUG)
    return build_type::debug;
#elif defined(SCYLLA_BUILD_MODE_RELEASE)
    return build_type::release;
#elif defined(SCYLLA_BUILD_MODE_DEV)
    return build_type::dev;
#elif defined(SCYLLA_BUILD_MODE_SANITIZE)
    return build_type::sanitize;
#elif defined(SCYLLA_BUILD_MODE_COVERAGE)
    return build_type::coverage;
#else
    static_assert(false, "Unknown build type!");
#endif
}

constexpr bool is_release_build() {
    return get_build_type() == build_type::release;
}

constexpr bool is_debug_build() {
    return get_build_type() == build_type::debug;
}
} // namespace tools::build_info
