
/*
 * Copyright 2015 ScyllaDB
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

managed_bytes::managed_bytes(managed_bytes_view o) noexcept : managed_bytes(initialized_later(), o.size()) {
    if (!external()) {
        auto dst = _u.small.data;
        while (o.size()) {
            auto bv = o._current_fragment;
            auto n = bv.size();
            memcpy(dst, bv.data(), n);
            dst += n;
            o.remove_prefix(n);
        }
        return;
    }
    auto b = _u.ptr;
    auto dst = b->data;
    size_t dst_size = b->frag_size;
    auto advance_dst = [&] (size_t n) {
        if (n < dst_size) {
            dst += n;
            dst_size -= n;
        } else {
            assert(n == dst_size);
            b = b->next;
            if (b) {
                dst = b->data;
                dst_size = b->frag_size;
            } else {
                dst = nullptr;
                dst_size = 0;
            }
        }
    };
    while (o.size()) {
        auto bv = o._current_fragment;
        auto n = std::min(bv.size(), dst_size);
        memcpy(dst, bv.data(), n);
        advance_dst(n);
        o.remove_prefix(n);
    }
    assert(!b);
}

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

static_assert(std::is_nothrow_default_constructible_v<managed_bytes_view_base>);
static_assert(std::is_nothrow_copy_constructible_v<managed_bytes_view_base>);
static_assert(std::is_nothrow_move_constructible_v<managed_bytes_view_base>);

static_assert(std::is_nothrow_default_constructible_v<managed_bytes_view_fragment_iterator>);
static_assert(std::is_nothrow_copy_constructible_v<managed_bytes_view_fragment_iterator>);

static_assert(std::is_nothrow_default_constructible_v<managed_bytes_view>);
static_assert(std::is_nothrow_copy_constructible_v<managed_bytes_view>);
static_assert(std::is_nothrow_move_constructible_v<managed_bytes_view>);

bytes
to_bytes(managed_bytes_view v) {
    return v.to_bytes();
}

bytes
to_bytes(const managed_bytes& b) {
    return managed_bytes_view(b).to_bytes();
}
int compare_unsigned(const managed_bytes_view v1, const managed_bytes_view v2) {
    auto rv1 = v1.as_fragment_range();
    auto rv2 = v2.as_fragment_range();
    auto it1 = rv1.begin();
    auto it2 = rv2.begin();
    while (auto n = std::min(it1->size(), it2->size())) {
        auto d = memcmp(it1->data(), it2->data(), n);
        if (d) {
            return d;
        }
        it1.remove_prefix(n);
        it2.remove_prefix(n);
    }
    if (it1->size()) {
        return 1;
    }
    if (it2->size()) {
        return -1;
    }
    return 0;
}

std::ostream& operator<<(std::ostream& os, const managed_bytes& b) {
    return os << managed_bytes_view(b);
}
