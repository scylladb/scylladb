/*
 * Copyright (C) 2018 ScyllaDB
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

class role_or_anonymous final {
public:
    std::optional<sstring> name{};

    role_or_anonymous() = default;
    role_or_anonymous(stdx::string_view name) : name(name) {
    }
};

std::ostream& operator<<(std::ostream&, const role_or_anonymous&);

bool operator==(const role_or_anonymous&, const role_or_anonymous&) noexcept;

inline bool operator!=(const role_or_anonymous& mr1, const role_or_anonymous& mr2) noexcept {
    return !(mr1 == mr2);
}

bool is_anonymous(const role_or_anonymous&) noexcept;

}

namespace std {

template <>
struct hash<auth::role_or_anonymous> {
    size_t operator()(const auth::role_or_anonymous& mr) const {
        return hash<std::optional<sstring>>()(mr.name);
    }
};

}
