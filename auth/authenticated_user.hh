/*
 */

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
#include <iosfwd>
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
};

///
/// The user name, or "anonymous".
///
std::ostream& operator<<(std::ostream&, const authenticated_user&);

inline bool operator==(const authenticated_user& u1, const authenticated_user& u2) noexcept {
    return u1.name == u2.name;
}

inline bool operator!=(const authenticated_user& u1, const authenticated_user& u2) noexcept {
    return !(u1 == u2);
}

const authenticated_user& anonymous_user() noexcept;

inline bool is_anonymous(const authenticated_user& u) noexcept {
    return u == anonymous_user();
}

}

namespace std {

template <>
struct hash<auth::authenticated_user> final {
    size_t operator()(const auth::authenticated_user &u) const {
        return std::hash<std::optional<sstring>>()(u.name);
    }
};

}
