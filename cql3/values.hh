/*
 * Copyright (C) 2017 ScyllaDB
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

#include "types.hh"
#include "bytes.hh"

#include <boost/variant.hpp>

#include <experimental/optional>

namespace cql3 {

struct null_value {
};

struct unset_value {
};

/// \brief View to a raw CQL protocol value.
///
/// \see raw_value
struct raw_value_view {
    boost::variant<bytes_view, null_value, unset_value> _data;

    raw_value_view(null_value&& data)
        : _data{std::move(data)}
    {}
    raw_value_view(unset_value&& data)
        : _data{std::move(data)}
    {}
    raw_value_view(bytes_view&& data)
        : _data{std::move(data)}
    {}
    raw_value_view(const bytes_view& data)
        : _data{data}
    {}
public:
    static raw_value_view make_null() {
        return raw_value_view{std::move(null_value{})};
    }
    static raw_value_view make_unset_value() {
        return raw_value_view{std::move(unset_value{})};
    }
    static raw_value_view make_value(bytes_view &&view) {
        return raw_value_view{std::move(view)};
    }
    static raw_value_view make_value(const bytes_view& view) {
        return raw_value_view{view};
    }
    bool is_null() const {
        return _data.which() == 1;
    }
    bool is_unset_value() const {
        return _data.which() == 2;
    }
    bool is_value() const {
        return _data.which() == 0;
    }
    bytes_view_opt data() const {
        if (_data.which() == 0) {
            return boost::get<bytes_view>(_data);
        }
        return {};
    }
    explicit operator bool() const {
        return _data.which() == 0;
    }
    const bytes_view* operator->() const {
        return &boost::get<bytes_view>(_data);
    }
    const bytes_view& operator*() const {
        return boost::get<bytes_view>(_data);
    }
};

/// \brief Raw CQL protocol value.
///
/// The `raw_value` type represents an uninterpreted value from the CQL wire
/// protocol. A raw value can hold either a null value, an unset value, or a byte
/// blob that represents the value.
class raw_value {
    boost::variant<bytes, null_value, unset_value> _data;

    raw_value(null_value&& data)
        : _data{std::move(data)}
    {}
    raw_value(unset_value&& data)
        : _data{std::move(data)}
    {}
    raw_value(bytes&& data)
        : _data{std::move(data)}
    {}
    raw_value(const bytes& data)
        : _data{data}
    {}
public:
    static raw_value make_null() {
        return raw_value{std::move(null_value{})};
    }
    static raw_value make_unset_value() {
        return raw_value{std::move(unset_value{})};
    }
    static raw_value make_value(const raw_value_view& view) {
        if (view.is_null()) {
            return make_null();
        }
        if (view.is_unset_value()) {
            return make_unset_value();
        }
        return make_value(to_bytes(*view));
    }
    static raw_value make_value(bytes&& bytes) {
        return raw_value{std::move(bytes)};
    }
    static raw_value make_value(const bytes& bytes) {
        return raw_value{bytes};
    }
    static raw_value make_value(const bytes_opt& bytes) {
        if (bytes) {
            return make_value(*bytes);
        }
        return make_null();
    }
    bool is_null() const {
        return _data.which() == 1;
    }
    bool is_unset_value() const {
        return _data.which() == 2;
    }
    bool is_value() const {
        return _data.which() == 0;
    }
    bytes_opt data() const {
        if (_data.which() == 0) {
            return boost::get<bytes>(_data);
        }
        return {};
    }
    explicit operator bool() const {
        return _data.which() == 0;
    }
    const bytes* operator->() const {
        return &boost::get<bytes>(_data);
    }
    const bytes& operator*() const {
        return boost::get<bytes>(_data);
    }
    raw_value_view to_view() const {
        switch (_data.which()) {
        case 0:  return raw_value_view::make_value(bytes_view{boost::get<bytes>(_data)});
        case 1:  return raw_value_view::make_null();
        default: return raw_value_view::make_unset_value();
        }
    }
};

}

inline bytes_opt to_bytes_opt(const cql3::raw_value_view& view) {
    return to_bytes_opt(view.data());
}

inline bytes_opt to_bytes_opt(const cql3::raw_value& value) {
    return value.data();
}
