
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
#include "utils/fragment_range.hh"
#include <seastar/util/alloc_failure_injector.hh>
#include <unordered_map>
#include <type_traits>
#include "utils/bit_cast.hh"

template <mutable_view is_mutable_view>
class managed_bytes_basic_view;
using managed_bytes_view = managed_bytes_basic_view<mutable_view::no>;
using managed_bytes_mutable_view = managed_bytes_basic_view<mutable_view::yes>;

// The layout of blob_storage::storage is as follows:
// If this is the last blob_storage in the list, there is only data.
// If this is not the last blob_storage in the list, the first bytes of `storage`
// contain a ref_type pointing to the next fragment, and all the following bytes
// are data.
// A blob_storage can learn whether it's the last by looking at the least significant
// bit of the size field in *backref.
struct blob_storage {
    using size_type = uint32_t;
    using char_type = bytes_view::value_type;

    struct [[gnu::packed]] ref_type {
        blob_storage* ptr = nullptr;
        size_type x = 0; // The least significant bit marks whether the fragment pointed to by ptr is not last. (1 = not last, 0 = last)
                         // The other 31 bits are the size of the fragment pointed to by ptr.
        ref_type() = default;
        void reset(blob_storage* p, size_type frag_size, bool has_next) {
            x = (frag_size << 1) + has_next;
            ptr = p;
        }
        size_type frag_size() const { return x >> 1; }
        bool has_next() const { return x & 1; }
        ref_type& next() const { return reinterpret_cast<ref_type&>(ptr->storage); };
        char_type* data() const { return &ptr->storage[has_next() ? sizeof(ref_type) : 0]; }
        size_t full_size() const { return static_cast<size_t>(frag_size()) + sizeof(blob_storage) + (has_next() ? sizeof(ref_type) : 0); }
    };

    ref_type* backref;
    char_type storage[];

    blob_storage(blob_storage&& o) noexcept
        : backref(o.backref)
    {
        backref->ptr = this;
        size_type frag_size = backref->frag_size();
        if (backref->has_next()) {
            // We are not the last fragment, so we need to update our successor in the list.
            memcpy(storage, o.storage, sizeof(ref_type) + frag_size);
            ref_type& next_ref = *reinterpret_cast<ref_type*>(storage);
            next_ref.ptr->backref = &next_ref;
        } else {
            // We are the last fragment.
            memcpy(storage, o.storage, frag_size);
        }
    }
} __attribute__((packed));

// A managed version of "bytes" (can be used with LSA).
class managed_bytes {
    // The layout of _storage is as follows:
    // If the value stored in managed_bytes fits inline (it's smaller than sizeof(_storage)),
    // then the last byte of _storage contains the size of this value shifted by 1, and the first
    // bytes of _storage contain the value.
    // If the value does not fit in line, it's allocated externally. In that case,
    // the beginning of _storage contains a blob_storage::ref_type pointing to the first
    // fragment, and the end of _storage contains a processed size of this value. The size is shifted
    // by 1, the least significant bit is set to 1, and the result is stored in big endian at the last
    // bytes.
    // This scheme allows to differentiate between external and internal storage by looking at the least
    // significant bit of the last byte of _storage. If that bit is 0, the storage is internal,
    // otherwise it's external.
    char _storage[16];
    static_assert(sizeof(_storage) >= sizeof(blob_storage::ref_type) + sizeof(blob_storage::size_type), "inline size too small");
    static constexpr size_t max_inline_size = sizeof(_storage) - 1;
private:
    bool external() const {
        return _storage[sizeof(_storage) - 1] & 1;
    }
    size_t internal_size() const {
        return _storage[sizeof(_storage) - 1] >> 1;
    }
    size_t external_size() const {
        blob_storage::size_type raw = read_unaligned<blob_storage::size_type>(&_storage[sizeof(_storage) - sizeof(blob_storage::size_type)]);
        return net::ntoh(raw) >> 1;
    }
    char* internal_size_slot() {
        return &_storage[sizeof(_storage) - 1];
    }
    char* external_size_slot() {
        return &_storage[sizeof(_storage) - sizeof(blob_storage::size_type)];
    }
    blob_storage::ref_type& first_fragment() {
        return *reinterpret_cast<blob_storage::ref_type*>(_storage);
    }
    const blob_storage::ref_type& first_fragment() const {
        return *reinterpret_cast<const blob_storage::ref_type*>(_storage);
    }
    size_t max_alloc(allocation_strategy& alctr) {
        return alctr.preferred_max_contiguous_allocation();
    }
    void free_chain(blob_storage::ref_type& p) noexcept {
        auto& alctr = current_allocator();
        while (p.has_next()) {
            blob_storage::ref_type n = p.next();
            alctr.destroy(p.ptr);
            p = n;
            // `ptr` can be null iff an allocation failed during construction.
            if (p.ptr) {
                p.ptr->backref = &p;
            }
        }
        // `ptr` can be null iff an allocation failed during construction.
        if (p.ptr) {
            alctr.destroy(p.ptr);
        }
    }
    bytes_view::value_type& value_at_index(blob_storage::size_type index) {
        if (!external()) {
            return reinterpret_cast<bytes_view::value_type&>(_storage[index]);
        }
        const blob_storage::ref_type* a = &first_fragment();
        while (index >= a->frag_size()) {
            index -= a->frag_size();
            a = &a->next();
        }
        return a->data()[index];
    }
    std::unique_ptr<bytes_view::value_type[]> do_linearize_pure() const;

public:
    struct initialized_later {};

    managed_bytes() {
        *internal_size_slot() = 0;
    }

    managed_bytes(const blob_storage::char_type* ptr, blob_storage::size_type size)
        : managed_bytes(bytes_view(ptr, size)) {}

    explicit managed_bytes(const bytes& b) : managed_bytes(static_cast<bytes_view>(b)) {}

    template <FragmentedView View>
    explicit managed_bytes(View v);

    managed_bytes(initialized_later, blob_storage::size_type size) {
        memory::on_alloc_point();
        if (size <= max_inline_size) {
            *internal_size_slot() = size << 1;
        } else {
            if (size >= size_t(2) * 1024 * 1024 * 1024) {
                throw std::bad_alloc();
            }
            write_unaligned<blob_storage::blob_storage::size_type>(external_size_slot(), net::hton((size << 1) | 1));
            auto& alctr = current_allocator();
            size_t maxalloc = max_alloc(alctr);
            blob_storage::ref_type* last = &first_fragment();
            new (last) blob_storage::ref_type();
            try {
                while (size > maxalloc - sizeof(blob_storage)) {
                    auto frag_size = maxalloc - sizeof(blob_storage) - sizeof(blob_storage::ref_type);
                    auto p = static_cast<blob_storage*>(alctr.alloc(&get_standard_migrator<blob_storage>(), maxalloc, alignof(blob_storage)));
                    p->backref = last;
                    new (p->storage) blob_storage::ref_type();
                    last->reset(p, frag_size, true);
                    last = reinterpret_cast<blob_storage::ref_type*>(p->storage);
                    size -= frag_size;
                }
                size_t alloc_size = sizeof(blob_storage) + size;
                auto p = static_cast<blob_storage*>(alctr.alloc(&get_standard_migrator<blob_storage>(), alloc_size, alignof(blob_storage)));
                p->backref = last;
                last->reset(p, size, false);
            } catch (...) {
                free_chain(first_fragment());
                throw;
            }
        }
    }

    explicit managed_bytes(bytes_view v) : managed_bytes(initialized_later(), v.size()) {
        if (!external()) {
            std::copy(v.begin(), v.end(), _storage);
            return;
        }
        const bytes_view::value_type* p = v.data();
        size_t s = v.size();
        blob_storage::ref_type* b = &first_fragment();
        while (s) {
            memcpy(b->data(), p, b->frag_size());
            p += b->frag_size();
            s -= b->frag_size();
            b = &b->next();
        }
    }

    managed_bytes(std::initializer_list<bytes::value_type> b) : managed_bytes(b.begin(), b.size()) {}

    ~managed_bytes() noexcept {
        if (external()) {
            free_chain(first_fragment());
        }
    }

    managed_bytes(const managed_bytes& o);

    managed_bytes(managed_bytes&& o) noexcept {
        std::memcpy(_storage, o._storage, sizeof(_storage));
        *o.internal_size_slot() = 0;
        if (external()) {
            first_fragment().ptr->backref = &first_fragment();
        }
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

    bool is_fragmented() const {
        return external() && first_fragment().has_next();
    }

    bytes_view::value_type& operator[](blob_storage::size_type index) {
        return value_at_index(index);
    }

    const bytes_view::value_type& operator[](blob_storage::size_type index) const {
        return const_cast<const bytes_view::value_type&>(
                const_cast<managed_bytes*>(this)->value_at_index(index));
    }

    blob_storage::size_type size() const {
        if (external()) {
            return external_size();
        } else {
            return internal_size();
        }
    }

    bool empty() const {
        return _storage[sizeof(_storage) - 1] == 0;
    }

    // Returns the amount of external memory used.
    size_t external_memory_usage() const {
        if (external()) {
            size_t mem = 0;
            const blob_storage::ref_type* blob = &first_fragment();
            while (blob->has_next()) {
                mem += blob->frag_size() + sizeof(blob_storage) + sizeof(blob_storage::ref_type);
                blob = &blob->next();
            }
            mem += blob->frag_size() + sizeof(blob_storage);
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
            return sizeof(blob_storage) + external_size();
        } else {
            return 0;
        }
    }

    template <std::invocable<bytes_view> Func>
    std::invoke_result_t<Func, bytes_view> with_linearized(Func&& func) const {
        const bytes_view::value_type* start = nullptr;
        size_t size = 0;
        if (!external()) {
            start = reinterpret_cast<const bytes_view::value_type*>(_storage);
            size = internal_size();
        } else if (!first_fragment().has_next()) {
            start = reinterpret_cast<const bytes_view::value_type*>(first_fragment().data());
            size = first_fragment().frag_size();
        }
        if (start) {
            return func(bytes_view(start, size));
        } else {
            auto data = do_linearize_pure();
            return func(bytes_view(data.get(), external_size()));
        }
    }

    template <mutable_view is_mutable_view>
    friend class managed_bytes_basic_view;
};

// blob_storage is a variable-size type
inline
size_t
size_for_allocation_strategy(const blob_storage& bs) {
    return bs.backref->full_size();
}

template <mutable_view is_mutable>
class managed_bytes_basic_view {
public:
    using fragment_type = std::conditional_t<is_mutable == mutable_view::yes, bytes_mutable_view, bytes_view>;
    using owning_type = std::conditional_t<is_mutable == mutable_view::yes, managed_bytes, const managed_bytes>;
    using value_type = typename fragment_type::value_type;
private:
    fragment_type _current_fragment = {};
    const blob_storage::ref_type* _next_fragments = nullptr;
    size_t _size = 0;
public:
    managed_bytes_basic_view() = default;
    managed_bytes_basic_view(const managed_bytes_basic_view&) = default;
    managed_bytes_basic_view(owning_type& mb) {
        using byte_type = std::conditional_t<is_mutable == mutable_view::yes, value_type, const value_type>;
        if (!mb.external()) {
            _current_fragment = fragment_type(reinterpret_cast<byte_type*>(mb._storage), mb.internal_size());
            _size = _current_fragment.size();
        } else {
            _size = mb.external_size();
            _next_fragments = &mb.first_fragment();
            _current_fragment = fragment_type(reinterpret_cast<byte_type*>(_next_fragments->data()), _next_fragments->frag_size());
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
            _next_fragments = &_next_fragments->next();
            _current_fragment = fragment_type(reinterpret_cast<value_type*>(_next_fragments->data()), _next_fragments->frag_size());
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
    bool is_fragmented() const {
        return _size != _current_fragment.size();
    }
    bytes linearize() const {
        return linearized(*this);
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

inline managed_bytes::managed_bytes(const managed_bytes& o) : managed_bytes(initialized_later(), o.size()) {
    if (!o.external()) {
        memcpy(_storage, o._storage, sizeof(_storage));
        return;
    }
    auto self = managed_bytes_mutable_view(*this);
    write_fragmented(self, managed_bytes_view(o));
}

inline bool managed_bytes::operator==(const managed_bytes& o) const {
    if (size() != o.size()) {
        return false;
    }
    if (!external()) {
        return std::equal(_storage, _storage + internal_size(), o._storage);
    } else {
        return compare_unsigned(managed_bytes_view(*this), managed_bytes_view(o)) == 0;
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
