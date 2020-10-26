
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

managed_bytes::managed_bytes(const managed_bytes_view o) : managed_bytes(initialized_later(), o.size()) {
    auto it = o.begin();
    if (!external()) {
        auto dst = _u.small.data;
        while (it != o.end()) {
            auto n = it->size();
            memcpy(dst, it->data(), n);
            dst += n;
            it.remove_prefix(n);
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
    while (it != o.end()) {
        auto n = std::min(it->size(), dst_size);
        memcpy(dst, it->data(), n);
        advance_dst(n);
        it.remove_prefix(n);
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

managed_bytes::const_iterator managed_bytes::begin() const noexcept {
    return managed_bytes_view(*this).begin();
}

managed_bytes::const_iterator managed_bytes::end() const noexcept {
    return managed_bytes_view(*this).end();
}

static_assert(std::is_nothrow_default_constructible_v<managed_bytes_view_base>);
static_assert(std::is_nothrow_copy_constructible_v<managed_bytes_view_base>);
static_assert(std::is_nothrow_move_constructible_v<managed_bytes_view_base>);

static_assert(std::is_nothrow_default_constructible_v<managed_bytes_view::fragment_iterator>);
static_assert(std::is_nothrow_copy_constructible_v<managed_bytes_view::fragment_iterator>);

static_assert(std::is_nothrow_default_constructible_v<managed_bytes_view>);
static_assert(std::is_nothrow_copy_constructible_v<managed_bytes_view>);
static_assert(std::is_nothrow_move_constructible_v<managed_bytes_view>);

managed_bytes_view::managed_bytes_view(const managed_bytes& mb) noexcept {
    if (mb._u.small.size != -1) {
        _current_fragment = bytes_view(mb._u.small.data, mb._u.small.size);
        _size = mb._u.small.size;
    } else {
        auto p = mb._u.ptr;
        _current_fragment = bytes_view(p->data, p->frag_size);
        _next_fragments = p->next;
        _size = p->size;
    }
}

bytes_view::value_type
managed_bytes_view::operator[](size_t idx) const noexcept {
    if (idx < _current_fragment.size()) {
        return _current_fragment[idx];
    }
    idx -= _current_fragment.size();
    auto f = _next_fragments;
    while (idx >= f->frag_size) {
        idx -= f->frag_size;
        f = f->next;
    }
    return f->data[idx];
}

void
managed_bytes_view::do_linearize_pure(bytes_view::value_type* data) const noexcept {
    auto e = std::copy_n(_current_fragment.data(), _current_fragment.size(), data);
    auto b = _next_fragments;
    while (b) {
        e = std::copy_n(b->data, b->frag_size, e);
        b = b->next;
    }
}

bytes
managed_bytes_view::to_bytes() const {
    bytes ret(bytes::initialized_later(), size());
    do_linearize_pure(ret.begin());
    return ret;
}

bytes
to_bytes(managed_bytes_view v) {
    return v.to_bytes();
}

bytes
to_bytes(const managed_bytes& b) {
    return managed_bytes_view(b).to_bytes();
}

void managed_bytes_view_base::remove_prefix(size_t n) {
    do {
        if (n < _current_fragment.size()) {
            _current_fragment = bytes_view(_current_fragment.data() + n, _current_fragment.size() - n);
            _size -= n;
            return;
        }
        auto trim = _current_fragment.size();
        n -= trim;
        _size -= trim;
        if (_next_fragments) {
            if (_size >= _next_fragments->frag_size) {
                _current_fragment = bytes_view(_next_fragments->data, _next_fragments->frag_size);
                _next_fragments = _next_fragments->next;
            } else {
                // We may need to cut the fragments short
                // if `substr` truncated _size
                _current_fragment = bytes_view(_next_fragments->data, _size);
                _next_fragments = nullptr;
            }
        } else {
            _current_fragment = {};
            if (n) {
               throw std::runtime_error("Reached end of managed_bytes_view");
            }
        }
    } while(n);
}

managed_bytes_view
managed_bytes_view::substr(size_t offset, size_t len) const {
    managed_bytes_view ret = *this;
    ret.remove_prefix(offset);
    if (len < ret.size()) {
        if (!ret._next_fragments) {
            ret._current_fragment = bytes_view(ret._current_fragment.data(), len);
        }
        ret._size = len;
    }
    return ret;
}

bool
managed_bytes_view::operator==(const managed_bytes_view& x) const noexcept {
    if (size() != x.size()) {
        return false;
    }

    auto it1 = begin();
    auto it2 = x.begin();
    while (auto n = std::min(it1->size(), it2->size())) {
        if (memcmp(it1->data(), it2->data(), n)) {
            return false;
        }
        it1.remove_prefix(n);
        it2.remove_prefix(n);
    }
    assert(it1 == end());
    assert(it2 == x.end());
    return true;
}

bool
managed_bytes_view::operator!=(const managed_bytes_view& x) const noexcept {
    return !operator==(x);
}

int compare_unsigned(const managed_bytes_view v1, const managed_bytes_view v2) noexcept {
    auto it1 = v1.begin();
    auto it2 = v2.begin();
    while (it1 != v1.end() && it2 != v2.end()) {
        auto n = std::min(it1->size(), it2->size());
        auto d = memcmp(it1->data(), it2->data(), n);
        if (d) {
            return d;
        }
        it1.remove_prefix(n);
        it2.remove_prefix(n);
    }
    if (it1 != v1.end()) {
        return 1;
    }
    if (it2 != v2.end()) {
        return -1;
    }
    return 0;
}

std::ostream& operator<<(std::ostream& os, const managed_bytes_view& v) {
    for (auto it = v.begin(); it != v.end(); it++) {
        os << *it;
    }
    return os;
}

std::ostream& operator<<(std::ostream& os, const managed_bytes& b) {
    return os << managed_bytes_view(b);
}
