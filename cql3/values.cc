/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
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
    case 2:  return raw_value_view::make_null();
    case 3: return raw_value_view::make_unset_value();
    default: throw std::runtime_error(fmt::format("raw_value_view::make_temporary bad index: {}", value._data.index()));
    }
}

raw_value_view::raw_value_view(managed_bytes&& tmp) {
    _temporary_storage = make_lw_shared<managed_bytes>(std::move(tmp));
    _data = managed_bytes_view(*_temporary_storage);
}

}
