
/*
 * Copyright (C) 2015-present ScyllaDB
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
#include "utils/fragment_range.hh"
#include <seastar/util/alloc_failure_injector.hh>
#include <unordered_map>
#include <type_traits>

template <mutable_view is_mutable_view>
class managed_bytes_basic_view;
using managed_bytes_view = managed_bytes_basic_view<mutable_view::no>;
using managed_bytes_mutable_view = managed_bytes_basic_view<mutable_view::yes>;

struct blob_storage {
    struct [[gnu::packed]] ref_type {
        blob_storage* ptr;

        ref_type() {}
        ref_type(blob_storage* ptr) : ptr(ptr) {}
        operator blob_storage*() const { return ptr; }
        blob_storage* operator->() const { return ptr; }
        blob_storage& operator*() const { return *ptr; }
    };
    using size_type = uint32_t;
    using char_type = bytes_view::value_type;

    ref_type* backref;
    size_type size;
    size_type frag_size;
    ref_type next;
    char_type data[];

    blob_storage(ref_type* backref, size_type size, size_type frag_size) noexcept
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
        memcpy(data, o.data, frag_size);
    }

    size_t storage_size() const noexcept {
        return sizeof(*this) + frag_size;
    }
} __attribute__((packed));

// A managed version of "bytes" (can be used with LSA).
class managed_bytes {
    static constexpr size_t max_inline_size = 15;
    struct small_blob {
        bytes_view::value_type data[max_inline_size];
        int8_t size; // -1 -> use blob_storage
    };
    union u {
        u() {}
        ~u() {}
        blob_storage::ref_type ptr;
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
    void free_chain(blob_storage* p) noexcept {
        auto& alctr = current_allocator();
        while (p) {
            auto n = p->next;
            alctr.destroy(p);
            p = n;
        }
    }
    bytes_view::value_type& value_at_index(blob_storage::size_type index) {
        if (!external()) {
            return _u.small.data[index];
        }
        blob_storage* a = _u.ptr;
        while (index >= a->frag_size) {
            index -= a->frag_size;
            a = a->next;
        }
        return a->data[index];
    }
    std::unique_ptr<bytes_view::value_type[]> do_linearize_pure() const;

public:
    using size_type = blob_storage::size_type;
    struct initialized_later {};

    managed_bytes() {
        _u.small.size = 0;
    }

    managed_bytes(const blob_storage::char_type* ptr, size_type size)
        : managed_bytes(bytes_view(ptr, size)) {}

    explicit managed_bytes(const bytes& b) : managed_bytes(static_cast<bytes_view>(b)) {}

    template <FragmentedView View>
    explicit managed_bytes(View v);

    managed_bytes(initialized_later, size_type size) {
        memory::on_alloc_point();
        if (size <= max_inline_size) {
            _u.small.size = size;
        } else {
            _u.small.size = -1;
            auto& alctr = current_allocator();
            auto maxseg = max_seg(alctr);
            auto now = std::min(size_t(size), maxseg);
            void* p = alctr.alloc<blob_storage>(sizeof(blob_storage) + now);
            auto first = new (p) blob_storage(&_u.ptr, size, now);
            auto last = first;
            size -= now;
            try {
                while (size) {
                    auto now = std::min(size_t(size), maxseg);
                    void* p = alctr.alloc<blob_storage>(sizeof(blob_storage) + now);
                    last = new (p) blob_storage(&last->next, 0, now);
                    size -= now;
                }
            } catch (...) {
                free_chain(first);
                throw;
            }
        }
    }

    explicit managed_bytes(bytes_view v) : managed_bytes(initialized_later(), v.size()) {
        if (!external()) {
            // Workaround for https://github.com/scylladb/scylla/issues/4086
            #pragma GCC diagnostic push
            #pragma GCC diagnostic ignored "-Warray-bounds"
            std::copy(v.begin(), v.end(), _u.small.data);
            #pragma GCC diagnostic pop
            return;
        }
        auto p = v.data();
        auto s = v.size();
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

    ~managed_bytes() noexcept {
        if (external()) {
            free_chain(_u.ptr);
        }
    }

    managed_bytes(const managed_bytes& o) : managed_bytes(initialized_later(), o.size()) {
        if (!o.external()) {
            _u.small = o._u.small;
            return;
        }
        auto s = size();
        const blob_storage::ref_type* next_src = &o._u.ptr;
        blob_storage* blob_src = nullptr;
        size_type size_src = 0;
        size_type offs_src = 0;
        blob_storage::ref_type* next_dst = &_u.ptr;
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
            // _u.ptr cannot be null
            _u.ptr->backref = &_u.ptr;
        }
        o._u.small.size = 0;
    }

    managed_bytes& operator=(managed_bytes&& o) noexcept {
        if (this != &o) {
            this->~managed_bytes();
            new (this) managed_bytes(std::move(o));
        }
        return *this;
    }

    managed_bytes& operator=(const managed_bytes& o) {
        if (this != &o) {
            managed_bytes tmp(o);
            this->~managed_bytes();
            new (this) managed_bytes(std::move(tmp));
        }
        return *this;
    }

    bool operator==(const managed_bytes& o) const {
        if (size() != o.size()) {
            return false;
        }
        if (!external()) {
            return std::equal(_u.small.data, _u.small.data + _u.small.size, o._u.small.data);
        } else {
            auto a = _u.ptr;
            auto a_data = a->data;
            auto a_remain = a->frag_size;
            a = a->next;
            auto b = o._u.ptr;
            auto b_data = b->data;
            auto b_remain = b->frag_size;
            b = b->next;
            while (a_remain || b_remain) {
                auto now = std::min(a_remain, b_remain);
                if (bytes_view(a_data, now) != bytes_view(b_data, now)) {
                    return false;
                }
                a_data += now;
                a_remain -= now;
                if (!a_remain && a) {
                    a_data = a->data;
                    a_remain = a->frag_size;
                    a = a->next;
                }
                b_data += now;
                b_remain -= now;
                if (!b_remain && b) {
                    b_data = b->data;
                    b_remain = b->frag_size;
                    b = b->next;
                }
            }
            return true;
        }
    }

    bool operator!=(const managed_bytes& o) const {
        return !(*this == o);
    }

    bytes_view::value_type& operator[](size_type index) {
        return value_at_index(index);
    }

    const bytes_view::value_type& operator[](size_type index) const {
        return const_cast<const bytes_view::value_type&>(
                const_cast<managed_bytes*>(this)->value_at_index(index));
    }

    size_type size() const {
        if (external()) {
            return _u.ptr->size;
        } else {
            return _u.small.size;
        }
    }

    bool empty() const {
        return _u.small.size == 0;
    }

    // Returns the amount of external memory used.
    size_t external_memory_usage() const {
        if (external()) {
            size_t mem = 0;
            blob_storage* blob = _u.ptr;
            while (blob) {
                mem += blob->frag_size + sizeof(blob_storage);
                blob = blob->next;
            }
            return mem;
        }
        return 0;
    }

    // Returns the minimum possible amount of external memory used by a managed_bytes
    // of the same size as us.
    // In other words, it returns the amount of external memory that would used by this
    // managed_bytes if all data was allocated in one big fragment.
    size_t minimal_external_memory_usage() const {
        if (external()) {
            return sizeof(blob_storage) + _u.ptr->size;
        } else {
            return 0;
        }
    }

    template <std::invocable<bytes_view> Func>
    std::invoke_result_t<Func, bytes_view> with_linearized(Func&& func) const {
        const bytes_view::value_type* start = nullptr;
        size_t size = 0;
        if (!external()) {
            start = _u.small.data;
            size = _u.small.size;
        } else if (!_u.ptr->next) {
            start = _u.ptr->data;
            size = _u.ptr->size;
        }
        if (start) {
            return func(bytes_view(start, size));
        } else {
            auto data = do_linearize_pure();
            return func(bytes_view(data.get(), _u.ptr->size));
        }
    }

    template <mutable_view is_mutable_view>
    friend class managed_bytes_basic_view;
};

template <mutable_view is_mutable>
class managed_bytes_basic_view {
public:
    using fragment_type = std::conditional_t<is_mutable == mutable_view::yes, bytes_mutable_view, bytes_view>;
    using owning_type = std::conditional_t<is_mutable == mutable_view::yes, managed_bytes, const managed_bytes>;
    using value_type = typename fragment_type::value_type;
private:
    fragment_type _current_fragment = {};
    blob_storage* _next_fragments = nullptr;
    size_t _size = 0;
public:
    managed_bytes_basic_view() = default;
    managed_bytes_basic_view(const managed_bytes_basic_view&) = default;
    managed_bytes_basic_view(owning_type& mb) {
        if (mb._u.small.size != -1) {
            _current_fragment = fragment_type(mb._u.small.data, mb._u.small.size);
            _size = mb._u.small.size;
        } else {
            auto p = mb._u.ptr;
            _current_fragment = fragment_type(p->data, p->frag_size);
            _next_fragments = p->next;
            _size = p->size;
        }
    }
    managed_bytes_basic_view(fragment_type bv)
        : _current_fragment(bv)
        , _size(bv.size()) {
    }
    size_t size() const { return _size; }
    size_t size_bytes() const { return _size; }
    bool empty() const { return _size == 0; }
    fragment_type current_fragment() const { return _current_fragment; }
    void remove_prefix(size_t n) {
        while (n >= _current_fragment.size() && n > 0) {
            n -= _current_fragment.size();
            remove_current();
        }
        _size -= n;
        _current_fragment.remove_prefix(n);
    }
    void remove_current() {
        _size -= _current_fragment.size();
        if (_size) {
            _current_fragment = fragment_type(_next_fragments->data, _next_fragments->frag_size);
            _next_fragments = _next_fragments->next;
            _current_fragment = _current_fragment.substr(0, _size);
        } else {
            _current_fragment = fragment_type();
        }
    }
    managed_bytes_basic_view prefix(size_t len) const {
        managed_bytes_basic_view v = *this;
        v._size = len;
        v._current_fragment = v._current_fragment.substr(0, len);
        return v;
    }
    managed_bytes_basic_view substr(size_t offset, size_t len) const {
        size_t end = std::min(offset + len, _size);
        managed_bytes_basic_view v = prefix(end);
        v.remove_prefix(offset);
        return v;
    }
    const auto& front() const { return _current_fragment.front(); }
    auto& front() { return _current_fragment.front(); }
    const value_type& operator[](size_t index) const {
        auto v = *this;
        v.remove_prefix(index);
        return v.current_fragment().front();
    }
    bytes linearize() const {
        return linearized(*this);
    }
    bool is_linearized() {
        return _current_fragment.size() == _size;
    }

    // Allow casting mutable views to immutable views.
    friend class managed_bytes_basic_view<mutable_view::no>;
    managed_bytes_basic_view(const managed_bytes_basic_view<mutable_view::yes>& other)
    requires (is_mutable == mutable_view::no)
        : _current_fragment(other._current_fragment.data(), other._current_fragment.size())
        , _next_fragments(other._next_fragments)
        , _size(other._size)
    {}
};
static_assert(FragmentedView<managed_bytes_view>);
static_assert(FragmentedMutableView<managed_bytes_mutable_view>);

using managed_bytes_opt = std::optional<managed_bytes>;
using managed_bytes_view_opt = std::optional<managed_bytes_view>;

inline bytes to_bytes(const managed_bytes& v) {
    return linearized(managed_bytes_view(v));
}
inline bytes to_bytes(managed_bytes_view v) {
    return linearized(v);
}

template<FragmentedView View>
inline managed_bytes::managed_bytes(View v) : managed_bytes(initialized_later(), v.size_bytes()) {
    managed_bytes_mutable_view self(*this);
    write_fragmented(self, v);
}

template<>
struct appending_hash<managed_bytes_view> {
    template<Hasher Hasher>
    void operator()(Hasher& h, managed_bytes_view v) const {
        feed_hash(h, v.size_bytes());
        for (bytes_view frag : fragment_range(v)) {
            h.update(reinterpret_cast<const char*>(frag.data()), frag.size());
        }
    }
};

namespace std {
template <>
struct hash<managed_bytes_view> {
    size_t operator()(managed_bytes_view v) const {
        bytes_view_hasher h;
        appending_hash<managed_bytes_view>{}(h, v);
        return h.finalize();
    }
};
template <>
struct hash<managed_bytes> {
    size_t operator()(const managed_bytes& v) const {
        return hash<managed_bytes_view>{}(v);
    }
};
} // namespace std

sstring to_hex(const managed_bytes& b);
sstring to_hex(const managed_bytes_opt& b);

// The operators below are used only by tests.

inline bool operator==(const managed_bytes_view& a, const managed_bytes_view& b) {
    return a.size_bytes() == b.size_bytes() && compare_unsigned(a, b) == 0;
}

inline std::ostream& operator<<(std::ostream& os, const managed_bytes_view& v) {
    for (bytes_view frag : fragment_range(v)) {
        os << to_hex(frag);
    }
    return os;
}
inline std::ostream& operator<<(std::ostream& os, const managed_bytes& b) {
    return (os << managed_bytes_view(b));
}
std::ostream& operator<<(std::ostream& os, const managed_bytes_opt& b);
