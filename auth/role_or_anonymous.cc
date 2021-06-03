/*
 * Copyright (C) 2018-present ScyllaDB
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

#include "auth/role_or_anonymous.hh"

#include <iostream>

namespace auth {

std::ostream& operator<<(std::ostream& os, const role_or_anonymous& mr) {
    os << mr.name.value_or("<anonymous>");
    return os;
}

bool operator==(const role_or_anonymous& mr1, const role_or_anonymous& mr2) noexcept {
    return mr1.name == mr2.name;
}

bool is_anonymous(const role_or_anonymous& mr) noexcept {
    return !mr.name.has_value();
}

}
