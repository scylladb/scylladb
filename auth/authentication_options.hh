/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <optional>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>
#include <variant>

#include <seastar/core/format.hh>
#include <seastar/core/sstring.hh>

#include "seastarx.hh"

namespace auth {

enum class authentication_option {
    password,
    hashed_password,
    options
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
        case hashed_password:
            return formatter<string_view>::format("HASHED PASSWORD", ctx);
        case options:
            return formatter<string_view>::format("OPTIONS", ctx);
        }
        std::abort();
    }
};

namespace auth {

using authentication_option_set = std::unordered_set<authentication_option>;

using custom_options = std::unordered_map<sstring, sstring>;

struct password_option {
    sstring password;
};

/// Used exclusively for restoring roles.
struct hashed_password_option {
    sstring hashed_password;
};

struct authentication_options final {
    std::optional<std::variant<password_option, hashed_password_option>> credentials;
    std::optional<custom_options> options;
};

inline bool any_authentication_options(const authentication_options& aos) noexcept {
    return aos.options || aos.credentials;
}

class unsupported_authentication_option : public std::invalid_argument {
public:
    explicit unsupported_authentication_option(authentication_option k)
            : std::invalid_argument(format("The {} option is not supported.", k)) {
    }
};

}
