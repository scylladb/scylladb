/*
 * Copyright (C) 2019 ScyllaDB
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
#include <fmt/format.h>
#include <seastar/core/sstring.hh>

#include "exceptions/exceptions.hh"

namespace exception_predicate {

/// Makes an exception predicate that applies \p check function to verify the exception and \p err
/// function to create an error message if the check fails.
inline auto make(std::function<bool(const std::exception&)> check,
                 std::function<sstring(const std::exception&)> err) {
    return [check = std::move(check), err = std::move(err)](const std::exception& e) {
               const bool status = check(e);
               BOOST_CHECK_MESSAGE(status, err(e));
               return status;
           };
}

/// Returns a predicate that will check if the exception message contains the given fragment.
inline auto message_contains(
        const sstring& fragment,
        const std::experimental::source_location& loc = std::experimental::source_location::current()) {
    return make([=](const auto& e) { return sstring(e.what()).find(fragment) != sstring::npos; },
                [=](const auto& e) {
                    return fmt::format("Message '{}' doesn't contain '{}'\n{}:{}: invoked here",
                                       e.what(), fragment, loc.file_name(), loc.line());
                });
}

/// Returns a predicate that will check if the exception message equals the given text.
inline auto message_equals(
        const sstring& text,
        const std::experimental::source_location& loc = std::experimental::source_location::current()) {
    return make([=](const auto& e) { return text == e.what(); },
                [=](const auto& e) {
                    return fmt::format("Message '{}' doesn't equal '{}'\n{}:{}: invoked here",
                                       e.what(), text, loc.file_name(), loc.line());
                });
}

} // namespace exception_predicate
