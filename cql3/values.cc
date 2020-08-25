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

#include "cql3/values.hh"

namespace cql3 {

std::ostream& operator<<(std::ostream& os, const raw_value_view& value) {
    seastar::visit(value._data, [&] (fragmented_temporary_buffer::view v) {
        os << "{ value: ";
        using boost::range::for_each;
        for_each(v, [&os] (bytes_view bv) { os << bv; });
        os << " }";
    }, [&] (null_value) {
        os << "{ null }";
    }, [&] (unset_value) {
        os << "{ unset }";
    });
    return os;
}

raw_value_view raw_value::to_view() const {
    switch (_data.index()) {
    case 0:  return raw_value_view::make_value(fragmented_temporary_buffer::view(bytes_view{std::get<bytes>(_data)}));
    case 1:  return raw_value_view::make_null();
    default: return raw_value_view::make_unset_value();
    }
}

raw_value raw_value::make_value(const raw_value_view& view) {
    if (view.is_null()) {
        return make_null();
    }
    if (view.is_unset_value()) {
        return make_unset_value();
    }
    return make_value(linearized(*view));
}

}