/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

// Define std::source_location, introduced in clang 15, even in clang 14
// where it was called std::experimental::source_location.
//
// When we don't need to support clang 14 any more, this file and all
// its inclusions can be removed.

#pragma once

#if defined(__clang_major__) && __clang_major__ <= 14

#include <experimental/source_location>

namespace std {
    using source_location = std::experimental::source_location;
}
#endif
