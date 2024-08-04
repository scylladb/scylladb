/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

// Seastar's on_internal_error() is a replacement for SCYLLA_ASSERT(). Instead of
// crashing like SCYLLA_ASSERT(), on_internal_error() logs a message with a
// backtrace and throws an exception (and optionally also crashes - this can
// be useful for testing). However, Seastar's function is inconvenient because
// it requires specifying a logger. This makes it hard to call it from source
// files which don't already have a logger, or in code in a header file.
//
// So here we provide Scylla's version of on_internal_error() which uses a
// single logger for all internal errors - with no need to specify a logger
// object to each call.

#pragma once

#include "utils/assert.hh"
#include <string_view>

namespace utils {

/// Report an internal error
///
/// Depending on the value passed to seastar::set_abort_on_internal_error(),
/// this will either abort or throw a std::runtime_error.
/// In both cases an error will be logged, containing \p reason and the
/// current backtrace.
[[noreturn]] void on_internal_error(std::string_view reason);

}