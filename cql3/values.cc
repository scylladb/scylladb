/*
 * Copyright (C) 2020-present ScyllaDB
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
    seastar::visit(value._data, [&] (FragmentedView auto v) {
        os << "{ value: ";
        for (bytes_view frag : fragment_range(v)) {
            os << frag;
        }
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
    case 0:  return raw_value_view::make_value(managed_bytes_view(bytes_view(std::get<bytes>(_data))));
    case 1:  return raw_value_view::make_value(managed_bytes_view(std::get<managed_bytes>(_data)));
    case 2:  return raw_value_view::make_null();
    default: return raw_value_view::make_unset_value();
    }
}

raw_value raw_value::make_value(const raw_value_view& view) {
    switch (view._data.index()) {
    case 0:  return raw_value::make_value(managed_bytes(std::get<fragmented_temporary_buffer::view>(view._data)));
    case 1:  return raw_value::make_value(managed_bytes(std::get<managed_bytes_view>(view._data)));
    case 2:  return raw_value::make_null();
    default: return raw_value::make_unset_value();
    }
}

raw_value_view raw_value_view::make_temporary(raw_value&& value) {
    switch (value._data.index()) {
    case 0:  return raw_value_view(managed_bytes(std::get<bytes>(value._data)));
    case 1:  return raw_value_view(std::move(std::get<managed_bytes>(value._data)));
    default: return raw_value_view::make_null();
    }
}

raw_value_view::raw_value_view(managed_bytes&& tmp) {
    _temporary_storage = make_lw_shared<managed_bytes>(std::move(tmp));
    _data = managed_bytes_view(*_temporary_storage);
}

}
