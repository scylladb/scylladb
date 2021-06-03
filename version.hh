
/*
 * Copyright (C) 2015-present ScyllaDB
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

#include <seastar/core/sstring.hh>
#include <seastar/core/print.hh>
#include <tuple>

#include "seastarx.hh"

namespace version {
class version {
    std::tuple<uint16_t, uint16_t, uint16_t> _version;
public:
    version(uint16_t x, uint16_t y = 0, uint16_t z = 0): _version(std::make_tuple(x, y, z)) {}

    sstring to_sstring() {
        return format("{:d}.{:d}.{:d}", std::get<0>(_version), std::get<1>(_version), std::get<2>(_version));
    }

    static version current() {
        static version v(3, 0, 8);
        return v;
    }

    bool operator==(version v) const {
        return _version == v._version;
    }

    bool operator!=(version v) const {
        return _version != v._version;
    }

    bool operator<(version v) const {
        return _version < v._version;
    }
    bool operator<=(version v) {
        return _version <= v._version;
    }
    bool operator>(version v) {
        return _version > v._version;
    }
    bool operator>=(version v) {
        return _version >= v._version;
    }
};

inline const sstring& release() {
    static thread_local auto str_ver = version::current().to_sstring();
    return str_ver;
}
}
