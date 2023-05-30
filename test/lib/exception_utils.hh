/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <functional>
#include <seastar/core/sstring.hh>
#include <seastar/util/source_location-compat.hh>

#include "seastarx.hh"

namespace exception_predicate {

/// Makes an exception predicate that applies \p check function to verify the exception and \p err
/// function to create an error message if the check fails.
extern std::function<bool(const std::exception&)> make(
        std::function<bool(const std::exception&)> check,
        std::function<sstring(const std::exception&)> err);

/// Returns a predicate that will check if the exception message contains the given fragment.
extern std::function<bool(const std::exception&)> message_contains(
        const sstring& fragment,
        const seastar::compat::source_location& loc = seastar::compat::source_location::current());

/// Returns a predicate that will check if the exception message equals the given text.
extern std::function<bool(const std::exception&)> message_equals(
        const sstring& text,
        const seastar::compat::source_location& loc = seastar::compat::source_location::current());

/// Returns a predicate that will check if the exception message matches the given regular expression.
extern std::function<bool(const std::exception&)> message_matches(
        const std::string& regex,
        const seastar::compat::source_location& loc = seastar::compat::source_location::current());

} // namespace exception_predicate
