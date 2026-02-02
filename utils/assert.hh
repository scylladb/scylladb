// Copyright 2024-present ScyllaDB
// SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

#pragma once

#include <cassert>

/// Like assert(), with two important differences:
/// 1. Independent of NDEBUG - active in all build modes. So code using it is
///    guaranteed not to proceed past an assertion if the expression is false.
/// 2. Uses __assert_fail_on_internal_error() instead of __assert_fail().
///    This means that instead of crashing on assertion failure, it uses
///    utils::on_internal_error(). This logs the error with a backtrace and
///    throws an exception (unless seastar::set_abort_on_internal_error() is
///    enabled). The exception still provides the guarantee mentioned above
///    that code won't proceed past the assertion if the expression is false
///    but it may kill just one operation instead of the entire server, and
///    usually this exception gets reported cleanly to the end-user, who can
///    avoid using that operation until the bug is fixed - instead of losing
///    the entire cluster without knowing why.
///
/// Note: when SCYLLA_ASSERT is used in a noexcept context, the exception
/// thrown by utils::on_internal_error() will still cause std::terminate() to
/// be called. In such cases the behavior is effectively similar to the old
/// assert() that crashes the process on assertion failure.
#define SCYLLA_ASSERT(x) do { if (!(x)) { ::utils::__assert_fail_on_internal_error(#x, __FILE__, __LINE__, __PRETTY_FUNCTION__); } } while (0)

/// Used by the SCYLLA_ASSERT() macro to report assertion failures using
/// on_internal_error().
namespace utils {
[[noreturn]] void __assert_fail_on_internal_error(const char* expr, const char* file, int line, const char* function);
} // namespace utils
