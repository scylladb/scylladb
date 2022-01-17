/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <iosfwd>
#include <optional>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>

#include <seastar/core/print.hh>
#include <seastar/core/sstring.hh>

#include "seastarx.hh"

namespace auth {

enum class authentication_option {
    password,
    options
};

std::ostream& operator<<(std::ostream&, authentication_option);

using authentication_option_set = std::unordered_set<authentication_option>;

using custom_options = std::unordered_map<sstring, sstring>;

struct authentication_options final {
    std::optional<sstring> password;
    std::optional<custom_options> options;
};

inline bool any_authentication_options(const authentication_options& aos) noexcept {
    return aos.password || aos.options;
}

class unsupported_authentication_option : public std::invalid_argument {
public:
    explicit unsupported_authentication_option(authentication_option k)
            : std::invalid_argument(format("The {} option is not supported.", k)) {
    }
};

}
