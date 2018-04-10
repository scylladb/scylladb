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

#include <experimental/string_view>
#include <seastar/core/sstring.hh>

#include "stdx.hh"

template<typename CharT>
class basic_mutable_view {
    CharT* _begin = nullptr;
    CharT* _end = nullptr;
public:
    using value_type = CharT;
    using pointer = CharT*;
    using iterator = CharT*;
    using const_iterator = CharT*;

    basic_mutable_view() = default;

    template<typename U, U N>
    basic_mutable_view(basic_sstring<CharT, U, N>& str)
        : _begin(str.begin())
        , _end(str.end())
    { }

    basic_mutable_view(CharT* ptr, size_t length)
        : _begin(ptr)
        , _end(ptr + length)
    { }

    operator stdx::basic_string_view<CharT>() const noexcept {
        return stdx::basic_string_view<CharT>(begin(), size());
    }

    CharT& operator[](size_t idx) const { return _begin[idx]; }

    iterator begin() const { return _begin; }
    iterator end() const { return _end; }

    CharT* data() const { return _begin; }
    size_t size() const { return _end - _begin; }
    bool empty() const { return _begin == _end; }

    void remove_prefix(size_t n) {
        _begin += n;
    }
    void remove_suffix(size_t n) {
        _end -= n;
    }
};
