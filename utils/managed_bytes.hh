
/*
 * Copyright 2015 Cloudius Systems
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

#include <stdint.h>
#include <memory>
#include "bytes.hh"
#include "utils/allocation_strategy.hh"

struct blob_storage {
    using size_type = uint32_t;
    using char_type = bytes_view::value_type;

    blob_storage** backref;
    size_type size;
    size_type frag_size;
    blob_storage* next;
    char_type data[];

    blob_storage(blob_storage** backref, size_type size, size_type frag_size) noexcept
        : backref(backref)
        , size(size)
        , frag_size(frag_size)
        , next(nullptr)
    {
        *backref = this;
    }

    blob_storage(blob_storage&& o) noexcept
        : backref(o.backref)
        , size(o.size)
        , frag_size(o.frag_size)
        , next(o.next)
    {
        *backref = this;
        o.next = nullptr;
        if (next) {
            next->backref = &next;
        }
        memcpy(data, o.data, size);
    }
} __attribute__((packed));

// A managed version of "bytes" (can be used with LSA).
class managed_bytes {
    static constexpr size_t max_inline_size = 15;
    struct small_blob {
        bytes_view::value_type data[max_inline_size];
        int8_t size; // -1 -> use blob_storage
    };
    union {
        blob_storage* ptr;
        small_blob small;
    } _u;
    static_assert(sizeof(small_blob) > sizeof(blob_storage*), "inline size too small");
private:
    bool external() const {
        return _u.small.size < 0;
    }
    size_t max_seg(allocation_strategy& alctr) {
        return alctr.preferred_max_contiguous_allocation() - sizeof(blob_storage);
    }
    void free_chain(blob_storage* p) {
        auto& alctr = current_allocator();
        while (p) {
            auto n = p->next;
            alctr.destroy(p);
            p = n;
        }
    }
public:
    using size_type = blob_storage::size_type;
    struct initialized_later {};

    managed_bytes() {
        _u.small.size = 0;
    }

    managed_bytes(const blob_storage::char_type* ptr, size_type size)
        : managed_bytes(bytes_view(ptr, size)) {}

    managed_bytes(const bytes& b) : managed_bytes(static_cast<bytes_view>(b)) {}

    managed_bytes(initialized_later, size_type size) {
        if (size <= max_inline_size) {
            _u.small.size = size;
        } else {
            _u.small.size = -1;
            auto& alctr = current_allocator();
            auto maxseg = max_seg(alctr);
            auto now = std::min(size_t(size), maxseg);
            void* p = alctr.alloc(&standard_migrator<blob_storage>::object,
                sizeof(blob_storage) + now, alignof(blob_storage));
            auto first = new (p) blob_storage(&_u.ptr, size, now);
            auto last = first;
            size -= now;
            try {
                while (size) {
                    auto now = std::min(size_t(size), maxseg);
                    void* p = alctr.alloc(&standard_migrator<blob_storage>::object,
                        sizeof(blob_storage) + now, alignof(blob_storage));
                    last = new (p) blob_storage(&last->next, 0, now);
                    size -= now;
                }
            } catch (...) {
                free_chain(first);
                throw;
            }
        }
    }

    managed_bytes(bytes_view v) : managed_bytes(initialized_later(), v.size()) {
        auto p = v.data();
        auto s = v.size();
        if (!external()) {
            memcpy(_u.small.data, p, s);
            return;
        }
        auto b = _u.ptr;
        while (s) {
            memcpy(b->data, p, b->frag_size);
            p += b->frag_size;
            s -= b->frag_size;
            b = b->next;
        }
        assert(!b);
    }

    managed_bytes(std::initializer_list<bytes::value_type> b) : managed_bytes(b.begin(), b.size()) {}

    ~managed_bytes() {
        if (external()) {
            free_chain(_u.ptr);
        }
    }

    managed_bytes(const managed_bytes& o) : managed_bytes(initialized_later(), o.size()) {
        if (!external()) {
            memcpy(data(), o.data(), size());
            return;
        }
        auto s = size();
        blob_storage* const* next_src = &o._u.ptr;
        blob_storage* blob_src = nullptr;
        size_type size_src = 0;
        size_type offs_src = 0;
        blob_storage** next_dst = &_u.ptr;
        blob_storage* blob_dst = nullptr;
        size_type size_dst = 0;
        size_type offs_dst = 0;
        while (s) {
            if (!size_src) {
                blob_src = *next_src;
                next_src = &blob_src->next;
                size_src = blob_src->frag_size;
                offs_src = 0;
            }
            if (!size_dst) {
                blob_dst = *next_dst;
                next_dst = &blob_dst->next;
                size_dst = blob_dst->frag_size;
                offs_dst = 0;
            }
            auto now = std::min(size_src, size_dst);
            memcpy(blob_dst->data + offs_dst, blob_src->data + offs_src, now);
            s -= now;
            offs_src += now; size_src -= now;
            offs_dst += now; size_dst -= now;
        }
        assert(size_src == 0 && size_dst == 0);
    }

    managed_bytes(managed_bytes&& o) noexcept
        : _u(o._u)
    {
        if (external()) {
            if (_u.ptr) {
                _u.ptr->backref = &_u.ptr;
            }
        }
        o._u.small.size = 0;
    }

    managed_bytes& operator=(managed_bytes&& o) {
        if (this != &o) {
            this->~managed_bytes();
            new (this) managed_bytes(std::move(o));
        }
        return *this;
    }

    managed_bytes& operator=(const managed_bytes& o) {
        if (this != &o) {
            // FIXME: not exception safe
            this->~managed_bytes();
            new (this) managed_bytes(o);
        }
        return *this;
    }

    bool operator==(const managed_bytes& o) const {
        return static_cast<bytes_view>(*this) == static_cast<bytes_view>(o);
    }

    bool operator!=(const managed_bytes& o) const {
        return !(*this == o);
    }

    operator bytes_view() const {
        return { data(), size() };
    }

    bytes_view::value_type& operator[](size_type index) {
        return data()[index];
    }

    const bytes_view::value_type& operator[](size_type index) const {
        return data()[index];
    }

    size_type size() const {
        if (external()) {
            return _u.ptr->size;
        } else {
            return _u.small.size;
        }
    }

    const blob_storage::char_type* begin() const {
        return data();
    }

    const blob_storage::char_type* end() const {
        return data() + size();
    }

    blob_storage::char_type* begin() {
        return data();
    }

    blob_storage::char_type* end() {
        return data() + size();
    }

    bool empty() const {
        return _u.small.size == 0;
    }

    blob_storage::char_type* data() {
        if (external()) {
            assert(!_u.ptr->next);  // must be linearized
            return _u.ptr->data;
        } else {
            return _u.small.data;
        }
    }

    const blob_storage::char_type* data() const {
        return const_cast<managed_bytes*>(this)->data();
    }

    void linearize() {
        if (!external() || !_u.ptr->next) {
            return;
        }
        auto& alctr = current_allocator();
        auto size = _u.ptr->size;
        void* p = alctr.alloc(&standard_migrator<blob_storage>::object,
            sizeof(blob_storage) + size, alignof(blob_storage));
        auto old = _u.ptr;
        auto blob = new (p) blob_storage(&_u.ptr, size, size);
        auto pos = size_type(0);
        while (old) {
            memcpy(blob->data + pos, old->data, old->frag_size);
            pos += old->frag_size;
            auto next = old->next;
            alctr.destroy(old);
            old = next;
        }
        assert(pos == size);
    }

    void scatter() {
        if (!external()) {
            return;
        }
        if (_u.ptr->size <= max_seg(current_allocator())) {
            return;
        }
        *this = managed_bytes(*this);
    }
};

namespace std {

template <>
struct hash<managed_bytes> {
    size_t operator()(managed_bytes v) const {
        return hash<bytes_view>()(v);
    }
};

}
