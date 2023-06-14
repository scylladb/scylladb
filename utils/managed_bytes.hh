
/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
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
#include <utility>

class bytes_ostream;

template <mutable_view is_mutable_view>
class managed_bytes_basic_view;
using managed_bytes_view = managed_bytes_basic_view<mutable_view::no>;
using managed_bytes_mutable_view = managed_bytes_basic_view<mutable_view::yes>;

struct blob_storage {
    struct [[gnu::packed]] ref_type {
        blob_storage* ptr = nullptr;

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
    friend class bytes_ostream;
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
    bool external() const noexcept {
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

    explicit managed_bytes(blob_storage* data) {
        _u.small.size = -1;
        _u.ptr.ptr = data;
        data->backref = &_u.ptr;
    }
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

    explicit managed_bytes(bytes_view v) : managed_bytes(single_fragmented_view(v)) {};

    managed_bytes(std::initializer_list<bytes::value_type> b) : managed_bytes(b.begin(), b.size()) {}

    ~managed_bytes() noexcept {
        if (external()) {
            free_chain(_u.ptr);
        }
    }

    managed_bytes(const managed_bytes& o);

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

    bool operator==(const managed_bytes& o) const;

    bytes_view::value_type& operator[](size_type index);
    const bytes_view::value_type& operator[](size_type index) const;

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
    size_t external_memory_usage() const noexcept {
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
    size_t minimal_external_memory_usage() const noexcept {
        if (external()) {
            return sizeof(blob_storage) + _u.ptr->size;
        } else {
            return 0;
        }
    }

    template <std::invocable<bytes_view> Func>
    std::invoke_result_t<Func, bytes_view> with_linearized(Func&& func) const;

    template <mutable_view is_mutable_view>
    friend class managed_bytes_basic_view;
};

template <mutable_view is_mutable>
class managed_bytes_basic_view {
public:
    using fragment_type = std::conditional_t<is_mutable == mutable_view::yes, bytes_mutable_view, bytes_view>;
    using owning_type = std::conditional_t<is_mutable == mutable_view::yes, managed_bytes, const managed_bytes>;
    using value_type = typename fragment_type::value_type;
    using value_type_maybe_const = std::conditional_t<is_mutable == mutable_view::yes, value_type, const value_type>;
private:
    fragment_type _current_fragment = {};
    blob_storage* _next_fragments = nullptr;
    size_t _size = 0;
private:
    managed_bytes_basic_view(fragment_type current_fragment, blob_storage* next_fragments, size_t size)
        : _current_fragment(current_fragment)
        , _next_fragments(next_fragments)
        , _size(size) {
    }
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
    value_type_maybe_const& front() const { return _current_fragment.front(); }
    value_type_maybe_const& operator[](size_t index) const {
        auto v = *this;
        v.remove_prefix(index);
        return v.current_fragment().front();
    }
    bytes linearize() const {
        return linearized(*this);
    }
    bool is_linearized() const {
        return _current_fragment.size() == _size;
    }

    // Allow casting mutable views to immutable views.
    template <mutable_view Other>
    friend class managed_bytes_basic_view;

    template <mutable_view Other>
    managed_bytes_basic_view(const managed_bytes_basic_view<Other>& other)
    requires (is_mutable == mutable_view::no) && (Other == mutable_view::yes)
        : _current_fragment(other._current_fragment.data(), other._current_fragment.size())
        , _next_fragments(other._next_fragments)
        , _size(other._size)
    {}

    template <std::invocable<bytes_view> Func>
    std::invoke_result_t<Func, bytes_view> with_linearized(Func&& func) const {
        bytes b;
        auto bv = std::invoke([&] () -> bytes_view {
            if (is_linearized()) {
                return _current_fragment;
            } else {
                b = linearize();
                return b;
            }
        });
        return func(bv);
    }

    friend managed_bytes_basic_view<mutable_view::no> build_managed_bytes_view_from_internals(bytes_view current_fragment, blob_storage* next_fragment, size_t size);
};
static_assert(FragmentedView<managed_bytes_view>);
static_assert(FragmentedMutableView<managed_bytes_mutable_view>);

inline bool operator==(const managed_bytes_view& a, const managed_bytes_view& b) {
    return a.size_bytes() == b.size_bytes() && compare_unsigned(a, b) == 0;
}

using managed_bytes_opt = std::optional<managed_bytes>;
using managed_bytes_view_opt = std::optional<managed_bytes_view>;

inline bytes to_bytes(const managed_bytes& v) {
    return linearized(managed_bytes_view(v));
}
inline bytes to_bytes(managed_bytes_view v) {
    return linearized(v);
}

/// Converts a possibly fragmented managed_bytes_opt to a
/// linear bytes_opt.
///
/// \note copies data
bytes_opt to_bytes_opt(const managed_bytes_opt&);

/// Converts a linear bytes_opt to a possibly fragmented
/// managed_bytes_opt.
///
/// \note copies data
managed_bytes_opt to_managed_bytes_opt(const bytes_opt&);

template<FragmentedView View>
inline managed_bytes::managed_bytes(View v) : managed_bytes(initialized_later(), v.size_bytes()) {
    managed_bytes_mutable_view self(*this);
    write_fragmented(self, v);
}

inline
managed_bytes_view
build_managed_bytes_view_from_internals(bytes_view current_fragment, blob_storage* next_fragment, size_t size) {
    return managed_bytes_view(current_fragment, next_fragment, size);
}

inline bytes_view::value_type& managed_bytes::operator[](size_type index) {
    return const_cast<bytes_view::value_type&>(std::as_const(*this)[index]);
}

inline const bytes_view::value_type& managed_bytes::operator[](size_type index) const {
    if (!external()) {
        return _u.small.data[index];
    }
    managed_bytes_view self(*this);
    return self[index];
}

template <std::invocable<bytes_view> Func>
std::invoke_result_t<Func, bytes_view> managed_bytes::with_linearized(Func&& func) const {
    return ::with_linearized(managed_bytes_view(*this), func);
}

inline bool managed_bytes::operator==(const managed_bytes& o) const {
    return managed_bytes_view(*this) == managed_bytes_view(o);
}

inline managed_bytes::managed_bytes(const managed_bytes& o) : managed_bytes() {
    if (!o.external()) {
        _u = o._u;
    } else {
        *this = managed_bytes(initialized_later(), o.size());
        managed_bytes_mutable_view self(*this);
        write_fragmented(self, managed_bytes_view(o));
    }
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
