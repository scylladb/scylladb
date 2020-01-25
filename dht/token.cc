/*
 * Copyright (C) 2020 ScyllaDB
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

#include <algorithm>
#include <limits>
#include <ostream>
#include <utility>

#include "dht/token.hh"
#include "dht/i_partitioner.hh"

namespace dht {

static const token min_token{ token::kind::before_all_keys, std::array<uint8_t, 8>{} };
static const token max_token{ token::kind::after_all_keys, std::array<uint8_t, 8>{} };

const token&
minimum_token() {
    return min_token;
}

const token&
maximum_token() {
    return max_token;
}

int tri_compare(token_view t1, token_view t2) {
    if (t1._kind < t2._kind) {
            return -1;
    } else if (t1._kind > t2._kind) {
            return 1;
    } else if (t1._kind == token_kind::key) {
        return global_partitioner().tri_compare(t1, t2);
    }
    return 0;
}

std::ostream& operator<<(std::ostream& out, const token& t) {
    if (t._kind == token::kind::after_all_keys) {
        out << "maximum token";
    } else if (t._kind == token::kind::before_all_keys) {
        out << "minimum token";
    } else {
        out << global_partitioner().to_sstring(t);
    }
    return out;
}

} // namespace dht
