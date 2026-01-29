// Copyright 2024-present ScyllaDB
// SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

#pragma once

#include <cassert>

/// Like assert(), but independent of NDEBUG. Active in all build modes.
#define SCYLLA_ASSERT(x) do { if (!(x)) { __assert_fail(#x, __FILE__, __LINE__, __PRETTY_FUNCTION__); } } while (0)

/// throwing_assert() is like SCYLLA_ASSERT() - active in all build modes -
/// but uses __assert_fail_on_internal_error() instead of __assert_fail().
/// This gives throwing_assert() two important benefits over SCYLLA_ASSERT():
/// 1. As its name suggests, throwing_assert() throws an exception instead
///    of crashing the process on failure (except in tests, which deliberately
///    enable seastar::set_abort_on_internal_error()).
///    The exception still provides the guarantee that code won't proceed past
///    the assertion if the expression is false. But it will often kill just
///    one operation instead of the entire server, and usually this exception
///    gets reported cleanly to the end-user, who can avoid using the failing
///    operation until the bug is fixed - instead of losing the entire cluster
///    without knowing why.
/// 2. on_internal_error() logs the error to the regular log and also logs a
///    backtrace - unlike SCYLLA_ASSERT() which writes to stderr and doesn't
///    log a backtrace.
/// Notes:
/// * when throwing_assert() is used in a noexcept context, the exception
///   thrown by utils::on_internal_error() will still cause std::terminate()
///   to be called - so it behaves like SCYLLA_ASSERT() but with better
///   logging but also an uglier backtrace in the debugger.
/// * The type of exception thrown by throwing_assert() is determined by
///   seastar::on_internal_error. It is currently std::runtime_error, but this
///   is not guaranteed and should not be relied on.
#define throwing_assert(x) do { if (!(x)) { ::utils::__assert_fail_on_internal_error(#x, __FILE__, __LINE__, __PRETTY_FUNCTION__); } } while (0)

/// Used by the throwing_assert() macro to report assertion failures using
/// utils::on_internal_error().
namespace utils {
[[noreturn]] void __assert_fail_on_internal_error(const char* expr, const char* file, int line, const char* function);
}