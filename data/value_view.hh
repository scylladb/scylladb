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

#include "utils/fragment_range.hh"

namespace data {

/// View of a cell value
///
/// `basic_value_view` is a non-owning reference to a, possibly fragmented,
/// opaque value of a cell. It behaves like an immutable range of fragments.
///
/// Moreover, there are functions that linearise the value in order to ease the
/// integration with the pre-existing code. Nevertheless, using them should be
/// avoided.
///
/// \note For now `basic_value_view` is used by regular atomic cells, counters
/// and collections. This is due to the fact that counters and collections
/// haven't been fully transitioned to the IMR yet and still use custom
/// serialisation formats. Once this is resolved `value_view` can be used
/// exclusively by regular atomic cells.
template<mutable_view is_mutable>
class basic_value_view {
public:
    static constexpr size_t maximum_internal_storage_length = 8 * 1024;
    static constexpr size_t maximum_external_chunk_length = 8 * 1024;

    using fragment_type = std::conditional_t<is_mutable == mutable_view::no,
                                             bytes_view, bytes_mutable_view>;
    using raw_pointer_type = std::conditional_t<is_mutable == mutable_view::no,
                                                const uint8_t*, uint8_t*>;
private:
    size_t _remaining_size;
    fragment_type _first_fragment;
    raw_pointer_type _next;
public:
    basic_value_view(fragment_type first, size_t remaining_size, raw_pointer_type next)
        : _remaining_size(remaining_size), _first_fragment(first), _next(next)
    { }

    explicit basic_value_view(fragment_type first)
        : basic_value_view(first, 0, nullptr)
    { }

    /// Iterator over fragments
    class iterator {
        fragment_type _view;
        raw_pointer_type _next;
        size_t _left;
    public:
        using iterator_category	= std::forward_iterator_tag;
        using value_type = fragment_type;
        using pointer = const fragment_type*;
        using reference = const fragment_type&;
        using difference_type = std::ptrdiff_t;

        iterator(fragment_type bv, size_t total, raw_pointer_type next) noexcept
            : _view(bv), _next(next), _left(total) { }

        const fragment_type& operator*() const {
            return _view;
        }
        const fragment_type* operator->() const {
            return &_view;
        }

        iterator& operator++();
        iterator operator++(int) {
            auto it = *this;
            operator++();
            return it;
        }

        bool operator==(const iterator& other) const {
            return _view.data() == other._view.data();
        }
        bool operator!=(const iterator& other) const {
            return !(*this == other);
        }
    };

    using const_iterator = iterator;

    auto begin() const {
        return iterator(_first_fragment, _remaining_size, _next);
    }
    auto end() const {
        return iterator(fragment_type(), 0, nullptr);
    }

    bool operator==(const basic_value_view& other) const noexcept;
    bool operator==(bytes_view bv) const noexcept;

    /// Total size of the value
    size_t size_bytes() const noexcept {
        return _first_fragment.size() + _remaining_size;
    }

    bool empty() const noexcept {
        return _first_fragment.empty();
    }

    bool is_fragmented() const noexcept {
        return bool(_next);
    }

    fragment_type first_fragment() const noexcept {
        return _first_fragment;
    }

    bytes linearize() const;

    template<typename Function>
    decltype(auto) with_linearized(Function&& fn) const;
};

using value_view = basic_value_view<mutable_view::no>;
using value_mutable_view = basic_value_view<mutable_view::yes>;

}
