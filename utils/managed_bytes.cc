
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

managed_bytes::managed_bytes(managed_bytes_view o) : managed_bytes(initialized_later(), o.size()) {
    // FIXME: implement
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
                _current_fragment = bytes_view(_next_fragments->data, _next_fragments->frag_size);
                _next_fragments = _next_fragments->next;
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
    // FIXME: implement
    return managed_bytes_view();
}

bool
managed_bytes_view::operator==(const managed_bytes_view& x) const {
    // FIXME: implement
    return true;
}

bool
managed_bytes_view::operator!=(const managed_bytes_view& x) const {
    // FIXME: implement
    return true;
}

int compare_unsigned(const managed_bytes_view v1, const managed_bytes_view v2) {
    // FIXME: implement
    return 0;
}

std::ostream& operator<<(std::ostream& os, const managed_bytes_view& v) {
    // FIXME: implement
    return os;
}

std::ostream& operator<<(std::ostream& os, const managed_bytes& b) {
    return os << managed_bytes_view(b);
}
