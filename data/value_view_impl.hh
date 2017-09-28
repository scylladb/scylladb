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

#include "data/cell.hh"

namespace data {

template<mutable_view is_mutable>
inline typename basic_value_view<is_mutable>::iterator& basic_value_view<is_mutable>::iterator::operator++()
{
    if (!_next) {
        _view = fragment_type();
    } else if (_left > maximum_external_chunk_length) {
        cell::chunk_context ctx(_next);
        auto v = cell::external_chunk::make_view(_next, ctx);
        _next = static_cast<uint8_t*>(v.template get<cell::tags::chunk_next>(ctx).load());
        _view = v.template get<cell::tags::chunk_data>(ctx);
        _left -= cell::maximum_external_chunk_length;
    } else {
        cell::last_chunk_context ctx(_next);
        auto v = cell::external_last_chunk::make_view(_next, ctx);
        _view = v.template get<cell::tags::chunk_data>(ctx);
        _next = nullptr;
    }
    return *this;
}

template<mutable_view is_mutable>
inline bool basic_value_view<is_mutable>::operator==(const basic_value_view& other) const noexcept
{
    // We can assume that all values are fragmented exactly in the same way.
    auto it1 = begin();
    auto it2 = other.begin();
    while (it1 != end() && it2 != other.end()) {
        if (*it1 != *it2) {
            return false;
        }
        ++it1;
        ++it2;
    }
    return it1 == end() && it2 == other.end();
}

template<mutable_view is_mutable>
inline bool basic_value_view<is_mutable>::operator==(bytes_view bv) const noexcept
{
    bool equal = true;
    using boost::range::for_each;
    for_each(*this, [&] (bytes_view fragment) {
        if (fragment.size() > bv.size()) {
            equal = false;
        } else {
            auto bv_frag = bv.substr(0, fragment.size());
            equal = equal && fragment == bv_frag;
            bv.remove_prefix(fragment.size());
        }
    });
    return equal && bv.empty();
}

template<mutable_view is_mutable>
inline bytes basic_value_view<is_mutable>::linearize() const
{
    bytes b(bytes::initialized_later(), size_bytes());
    auto it = b.begin();
    for (auto fragment : *this) {
        it = boost::copy(fragment, it);
    }
    return b;
}

template<mutable_view is_mutable>
template<typename Function>
inline decltype(auto) basic_value_view<is_mutable>::with_linearized(Function&& fn) const
{
    bytes b;
    bytes_view bv;
    if (is_fragmented()) {
        b = linearize();
        bv = b;
    } else {
        bv = _first_fragment;
    }
    return fn(bv);
}

inline std::ostream& operator<<(std::ostream& os, value_view vv)
{
    using boost::range::for_each;
    for_each(vv, [&os] (bytes_view fragment) {
        os << fragment;
    });
    return os;
}

}
