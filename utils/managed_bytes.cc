
/*
 * Copyright 2015-present ScyllaDB
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


#include "managed_bytes.hh"

std::unique_ptr<bytes_view::value_type[]>
managed_bytes::do_linearize_pure() const {
    auto b = _u.ptr;
    auto data = std::unique_ptr<bytes_view::value_type[]>(new bytes_view::value_type[b->size]);
    auto e = data.get();
    while (b) {
        e = std::copy_n(b->data, b->frag_size, e);
        b = b->next;
    }
    return data;
}

sstring to_hex(const managed_bytes& b) {
    return to_hex(managed_bytes_view(b));
}

sstring to_hex(const managed_bytes_opt& b) {
    return !b ? "null" : to_hex(*b);
}

std::ostream& operator<<(std::ostream& os, const managed_bytes_opt& b) {
    if (b) {
        return os << *b;
    }
    return os << "null";
}
