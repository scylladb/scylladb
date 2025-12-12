// Copyright 2024-present ScyllaDB
// SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

#pragma once

#include <cassert>
#include <seastar/core/format.hh>
#include "utils/on_internal_error.hh"

/// Like assert(), but independent of NDEBUG. Active in all build modes.
#define SCYLLA_ASSERT(x) do { if (!(x)) { __assert_fail(#x, __FILE__, __LINE__, __PRETTY_FUNCTION__); } } while (0)

/// Exception-throwing assertion based on on_internal_error()
///
/// Unlike SCYLLA_ASSERT which crashes the process, scylla_assert() throws an
/// exception (or aborts depending on configuration). This prevents cluster-wide
/// crashes and loss of availability by allowing graceful error handling.
///
/// Use this instead of SCYLLA_ASSERT in contexts where throwing exceptions is safe.
/// DO NOT use in:
/// - noexcept functions
/// - destructors
/// - contexts with special exception-safety requirements
#define scylla_assert(condition, ...) \
    do { \
        if (!(condition)) [[unlikely]] { \
            ::utils::on_internal_error(::seastar::format("Assertion failed: {} at {}:{} in {}" __VA_OPT__(": {}"), \
                #condition, __FILE__, __LINE__, __PRETTY_FUNCTION__ __VA_OPT__(,) __VA_ARGS__)); \
        } \
    } while (0)
