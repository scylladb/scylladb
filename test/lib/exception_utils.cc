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

#include "test/lib/exception_utils.hh"

#include <boost/test/unit_test.hpp>
#include <fmt/format.h>
#include <fmt/ostream.h>

std::function<bool(const std::exception&)> exception_predicate::make(
        std::function<bool(const std::exception&)> check,
        std::function<sstring(const std::exception&)> err) {
    return [check = std::move(check), err = std::move(err)] (const std::exception& e) {
               const bool status = check(e);
               BOOST_CHECK_MESSAGE(status, err(e));
               return status;
           };
}

std::function<bool(const std::exception&)> exception_predicate::message_contains(
        const sstring& fragment,
        const std::experimental::source_location& loc) {
    return make([=] (const std::exception& e) { return sstring(e.what()).find(fragment) != sstring::npos; },
                [=] (const std::exception& e) {
                    return fmt::format("Message '{}' doesn't contain '{}'\n{}:{}: invoked here",
                                       e.what(), fragment, loc.file_name(), loc.line());
                });
}

std::function<bool(const std::exception&)> exception_predicate::message_equals(
        const sstring& text,
        const std::experimental::source_location& loc) {
    return make([=] (const std::exception& e) { return text == e.what(); },
                [=] (const std::exception& e) {
                    return fmt::format("Message '{}' doesn't equal '{}'\n{}:{}: invoked here",
                                       e.what(), text, loc.file_name(), loc.line());
                });
}
