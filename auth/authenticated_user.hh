/*
 * Copyright (C) 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <string_view>
#include <functional>
#include <optional>

#include <seastar/core/sstring.hh>

#include "seastarx.hh"

namespace auth {

///
/// A type-safe wrapper for the name of a logged-in user, or a nameless (anonymous) user.
///
class authenticated_user final {
public:
    ///
    /// An anonymous user has no name.
    ///
    std::optional<sstring> name{};

    ///
    /// An anonymous user.
    ///
    authenticated_user() = default;
    explicit authenticated_user(std::string_view name);
    friend bool operator==(const authenticated_user&, const authenticated_user&) noexcept = default;
};

const authenticated_user& anonymous_user() noexcept;

inline bool is_anonymous(const authenticated_user& u) noexcept {
    return u == anonymous_user();
}

}

///
/// The user name, or "anonymous".
///
template <>
struct fmt::formatter<auth::authenticated_user> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const auth::authenticated_user& u, FormatContext& ctx) const {
        if (u.name) {
            return fmt::format_to(ctx.out(), "{}", *u.name);
        } else {
            return fmt::format_to(ctx.out(), "{}", "anonymous");
        }
    }
};

namespace std {

template <>
struct hash<auth::authenticated_user> final {
    size_t operator()(const auth::authenticated_user &u) const {
        return std::hash<std::optional<sstring>>()(u.name);
    }
};

}
