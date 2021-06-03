/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <experimental/source_location>
#include <functional>
#include <seastar/core/sstring.hh>

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
        const std::experimental::source_location& loc = std::experimental::source_location::current());

/// Returns a predicate that will check if the exception message equals the given text.
extern std::function<bool(const std::exception&)> message_equals(
        const sstring& text,
        const std::experimental::source_location& loc = std::experimental::source_location::current());

} // namespace exception_predicate
