/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

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

template <>
struct fmt::formatter<auth::authentication_option> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const auth::authentication_option a, FormatContext& ctx) const {
        using enum auth::authentication_option;
        switch (a) {
        case password:
            return formatter<string_view>::format("PASSWORD", ctx);
        case options:
            return formatter<string_view>::format("OPTIONS", ctx);
        }
        std::abort();
    }
};
