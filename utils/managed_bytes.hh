
/*
 * Copyright (C) 2015 ScyllaDB
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
#include <seastar/core/unaligned.hh>
#include <seastar/util/alloc_failure_injector.hh>
#include <unordered_map>
#include <type_traits>
#include <iosfwd>

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
} __attribute__((packed));

class managed_bytes_view;

// A managed version of "bytes" (can be used with LSA).
class managed_bytes {
    static thread_local std::unordered_map<const blob_storage*, std::unique_ptr<bytes_view::value_type[]>> _lc_state;
    struct linearization_context {
        unsigned _nesting = 0;
        // Map from first blob_storage address to linearized version
        // We use the blob_storage address to be insentive to moving
        // a managed_bytes object.
        // linearization_context is entered often in the fast path, but it is
        // actually used only in rare (slow) cases.
        std::unordered_map<const blob_storage*, std::unique_ptr<bytes_view::value_type[]>>* _state_ptr = nullptr;
        void enter() {
            ++_nesting;
        }
        void leave() {
            if (!--_nesting && _state_ptr) {
                _state_ptr->clear();
                _state_ptr = nullptr;
            }
        }
        void forget(const blob_storage* p) noexcept;
    };
    static thread_local linearization_context _linearization_context;
public:
    struct linearization_context_guard {
        linearization_context_guard() {
            _linearization_context.enter();
        }
        ~linearization_context_guard() {
            _linearization_context.leave();
        }
    };
private:
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
        if (p->next && _linearization_context._nesting) {
            _linearization_context.forget(p);
        }
        auto& alctr = current_allocator();
        while (p) {
            auto n = p->next;
            alctr.destroy(p);
            p = n;
        }
    }
    const bytes_view::value_type* read_linearize() const {
        seastar::memory::on_alloc_point();
        if (!external()) {
            return _u.small.data;
        } else  if (!_u.ptr->next) {
            return _u.ptr->data;
        } else {
            return do_linearize();
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
    const bytes_view::value_type* do_linearize() const;
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
        memory::on_alloc_point();
        if (size <= max_inline_size) {
            _u.small.size = size;
        } else {
            _u.small.size = -1;
            auto& alctr = current_allocator();
            auto maxseg = max_seg(alctr);
            auto now = std::min(size_t(size), maxseg);
            void* p = alctr.alloc(&get_standard_migrator<blob_storage>(),
                sizeof(blob_storage) + now, alignof(blob_storage));
            auto first = new (p) blob_storage(&_u.ptr, size, now);
            auto last = first;
            size -= now;
            try {
                while (size) {
                    auto now = std::min(size_t(size), maxseg);
                    void* p = alctr.alloc(&get_standard_migrator<blob_storage>(),
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

    managed_bytes(managed_bytes_view);

    managed_bytes(bytes_view v) : managed_bytes(initialized_later(), v.size()) {
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

    operator bytes_view() const {
        return { data(), size() };
    }

    bool is_fragmented() const {
        return external() && _u.ptr->next;
    }

    operator bytes_mutable_view() {
        assert(!is_fragmented());
        return { data(), size() };
    };

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
        return read_linearize();
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

    template <std::invocable<> Func>
    friend std::result_of_t<Func()> with_linearized_managed_bytes(Func&& func);

    friend class managed_bytes_view;
};

class managed_bytes_view {
    bytes_view _current_fragment = {};
    blob_storage* _next_fragments = nullptr;
    size_t _size = 0;
public:
    managed_bytes_view() = default;
    managed_bytes_view(const managed_bytes&);
    managed_bytes_view(bytes_view);
    explicit managed_bytes_view(const bytes&);
    size_t size() const { return _size; }
    bool empty() const { return _size == 0; }
    bytes_view::value_type operator[](size_t idx) const;
    void remove_prefix(size_t prefix_len);
    managed_bytes_view substr(size_t offset, size_t len) const;
    bytes to_bytes() const;
    bool operator==(const managed_bytes_view& x) const;
    bool operator!=(const managed_bytes_view& x) const;

    template <std::invocable<bytes_view> Func>
    std::invoke_result_t<Func, bytes_view> with_linearized(Func&& func) const {
        return func(bytes_view());
    }
    friend std::ostream& operator<<(std::ostream& os, const managed_bytes_view& v);
};

// These are used to resolve ambiguities because managed_bytes_view can be converted to managed_bytes and vice versa
inline bool operator==(const managed_bytes& a, const managed_bytes_view& b) {
    return managed_bytes_view(a) == b;
}

inline bool operator==(const managed_bytes_view& a, const managed_bytes& b) {
    return a == managed_bytes_view(b);
}

inline bool operator!=(const managed_bytes& a, const managed_bytes_view& b) {
    return managed_bytes_view(a) != b;
}

inline bool operator!=(const managed_bytes_view& a, const managed_bytes& b) {
    return a != managed_bytes_view(b);
}

// These are used to resolve ambiguities because bytes_view can be converted to managed_bytes and managed_bytes_view
inline bool operator==(const bytes_view& a, const managed_bytes_view& b) {
    return managed_bytes_view(a) == b;
}

inline bool operator==(const managed_bytes_view& a, const bytes_view& b) {
    return a == managed_bytes_view(b);
}

inline bool operator!=(const bytes_view& a, const managed_bytes_view& b) {
    return managed_bytes_view(a) != b;
}

inline bool operator!=(const managed_bytes_view& a, const bytes_view& b) {
    return a != managed_bytes_view(b);
}

inline
managed_bytes_view::managed_bytes_view(bytes_view bv)
        : _current_fragment(bv)
        , _size(bv.size()) {
}

inline
managed_bytes_view::managed_bytes_view(const bytes& b)
        : managed_bytes_view(bytes_view(b)) {
}

bytes to_bytes(const managed_bytes& b);
bytes to_bytes(managed_bytes_view v);
int compare_unsigned(const managed_bytes_view v1, const managed_bytes_view v2);

// Run func() while ensuring that reads of managed_bytes objects are
// temporarlily linearized
template <std::invocable<> Func>
inline
std::result_of_t<Func()>
with_linearized_managed_bytes(Func&& func) {
    managed_bytes::linearization_context_guard g;
    return func();
}

namespace std {

template <>
struct hash<managed_bytes> {
    size_t operator()(const managed_bytes& v) const {
        return 3; // FIXME!
        //return hash<bytes_view>()(v);
    }
};

}

// blob_storage is a variable-size type
inline
size_t
size_for_allocation_strategy(const blob_storage& bs) {
    return sizeof(bs) + bs.frag_size;
}

template<>
struct appending_hash<managed_bytes_view> {
    template<typename Hasher>
    void operator()(Hasher& h, managed_bytes_view v) const {
        feed_hash(h, v.size());
        //h.update(reinterpret_cast<const char*>(v.begin()), v.size() * sizeof(bytes_view::value_type));
    }
};

std::ostream& operator<<(std::ostream& os, const managed_bytes& b);
std::ostream& operator<<(std::ostream& os, const managed_bytes_view& v);
