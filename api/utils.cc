/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <algorithm>
#include <cctype>
#include <stdexcept>

#include "api/utils.hh"
#include "marshal_exception.hh"

namespace api {

utils::UUID parse_uuid_param(std::string_view name, std::string_view value) {
    try {
        return utils::UUID{value};
    } catch (const marshal_exception& e) {
        throw httpd::bad_param_exception{fmt::format("{}: {}", name, e.what())};
    }
}

bool parse_bool_param(std::string_view name, std::string_view value) {
    const auto equals_ignoring_case = [value] (std::string_view lowercase_word) {
        return std::ranges::equal(value, lowercase_word, [] (unsigned char a, char b) {
            return std::tolower(a) == b;
        });
    };
    for (std::string_view s : {"true", "yes", "1"}) {
        if (equals_ignoring_case(s)) {
            return true;
        }
    }
    for (std::string_view s : {"false", "no", "0"}) {
        if (equals_ignoring_case(s)) {
            return false;
        }
    }
    throw httpd::bad_param_exception{fmt::format("{}: not a boolean, expected true/false, yes/no or 1/0: '{}'", name, value)};
}

gms::inet_address parse_inet_address_param(std::string_view name, const sstring& value) {
    try {
        return gms::inet_address{value};
    } catch (const std::invalid_argument& e) {
        throw httpd::bad_param_exception{fmt::format("{}: {}", name, e.what())};
    }
}

}  // namespace api
