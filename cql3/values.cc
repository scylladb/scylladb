/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "cql3/values.hh"

auto fmt::formatter<cql3::raw_value_view>::format(const cql3::raw_value_view& value, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    auto out = ctx.out();
    return seastar::visit(value._data, [&] (FragmentedView auto v) {
        out = fmt::format_to(out, "{{ value: ");
        for (bytes_view frag : fragment_range(v)) {
            out = fmt::format_to(out, "{}", fmt_hex(frag));
        }
        return fmt::format_to(out, " }}");
    }, [&] (cql3::null_value) {
        return fmt::format_to(out, "{{ null }}");
    });
}

namespace cql3 {

raw_value_view raw_value::view() const {
    switch (_data.index()) {
    case 0:  return raw_value_view::make_value(managed_bytes_view(bytes_view(std::get<bytes>(_data))));
    case 1:  return raw_value_view::make_value(managed_bytes_view(std::get<managed_bytes>(_data)));
    case 2:  return raw_value_view::make_null();
    default: throw std::runtime_error(fmt::format("raw_value_view::view bad index: {}", _data.index()));
    }
}

raw_value raw_value::make_value(const raw_value_view& view) {
    switch (view._data.index()) {
    case 0:  return raw_value::make_value(managed_bytes(std::get<fragmented_temporary_buffer::view>(view._data)));
    case 1:  return raw_value::make_value(managed_bytes(std::get<managed_bytes_view>(view._data)));
    case 2:  return raw_value::make_null();
    default: throw std::runtime_error(fmt::format("raw_value_view::make_value bad index: {}", view._data.index()));
    }
}

raw_value_view raw_value_view::make_temporary(raw_value&& value) {
    switch (value._data.index()) {
    case 0:  return raw_value_view(managed_bytes(std::get<bytes>(value._data)));
    case 1:  return raw_value_view(std::move(std::get<managed_bytes>(value._data)));
    case 2:  return raw_value_view::make_null();
    default: throw std::runtime_error(fmt::format("raw_value_view::make_temporary bad index: {}", value._data.index()));
    }
}

raw_value_view::raw_value_view(managed_bytes&& tmp) {
    _temporary_storage = make_lw_shared<managed_bytes>(std::move(tmp));
    _data = managed_bytes_view(*_temporary_storage);
}

bool operator==(const raw_value& v1, const raw_value& v2) {
    if (v1.is_null() && v2.is_null()) {
        return true; // note: this is not CQL comparison which would return NULL here
    }

    if (v1.is_value() && v2.is_value()) {
        return v1.view().with_value([&] (const FragmentedView auto& v1v) {
            return v2.view().with_value([&] (const FragmentedView auto& v2v) {
                return compare_unsigned(v1v, v2v) == 0;
            });
        });
    }

    return false;
}

}
