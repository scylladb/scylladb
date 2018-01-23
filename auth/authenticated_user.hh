/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2016 ScyllaDB
 *
 * Modified by ScyllaDB
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

#include <experimental/string_view>
#include <functional>
#include <iosfwd>
#include <optional>

#include <seastar/core/sstring.hh>

#include "seastarx.hh"
#include "stdx.hh"

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
    explicit authenticated_user(stdx::string_view name);
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
