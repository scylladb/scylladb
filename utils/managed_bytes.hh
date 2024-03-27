
/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <stdint.h>
#include "bytes.hh"
#include "utils/allocation_strategy.hh"
#include "utils/fragment_range.hh"
#include <seastar/util/alloc_failure_injector.hh>
#include <type_traits>
#include <utility>

class bytes_ostream;

template <mutable_view is_mutable_view>
class managed_bytes_basic_view;
using managed_bytes_view = managed_bytes_basic_view<mutable_view::no>;
using managed_bytes_mutable_view = managed_bytes_basic_view<mutable_view::yes>;

// Used to store managed_bytes data in layout 3. (See the doc comment of managed_bytes).
// Also used as the underlying storage for bytes_ostream.
//
// The storage for these "fragmented buffer" types is provided by a chain
// (linked list) of multi_chunk_blob_storage objects.
struct multi_chunk_blob_storage {
    // Stored inline in managed_bytes.
    struct [[gnu::packed]] ref_type {
        multi_chunk_blob_storage* ptr = nullptr;

        ref_type() {}
        ref_type(multi_chunk_blob_storage* ptr) : ptr(ptr) {}
        operator multi_chunk_blob_storage*() const { return ptr; }
        multi_chunk_blob_storage* operator->() const { return ptr; }
        multi_chunk_blob_storage& operator*() const { return *ptr; }
    };
    using size_type = uint32_t;
    using char_type = bytes_view::value_type;

    // Backref is needed to update the parent's pointer to us when we are
    // migrated during memory defragmentation.
    // (See the docs of allocation_strategy).
    ref_type* backref;

    // These fields have two different meanings:
    // 1. In bytes_ostream:
    // - `size` is the size of this fragment (== the size of the trailing data[] below).
    // - `frag_size` is the number of *used* (written) bytes in fragment.
    // 2. In managed_bytes:
    // - `size` in the first multi_chunk_blob_storage in the list is the size of the entire fragmented
    // buffer (the sum of all data[]s in the chain).
    // - `frag_size` is the data[] size of the current fragment (this multi_chunk_blob_storage).
    size_type size;
    size_type frag_size;

    // Pointer to the next fragment in the list. If we are the last fragment, it's null.
    ref_type next;

    // The storage provided by this fragment.
    char_type data[];

    multi_chunk_blob_storage(ref_type* backref, size_type size, size_type frag_size) noexcept
        : backref(backref)
        , size(size)
        , frag_size(frag_size)
        , next(nullptr)
    {
        *backref = this;
    }

    multi_chunk_blob_storage(multi_chunk_blob_storage&& o) noexcept
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

    // Valid only in the managed_bytes interpretation.
    // As long as bytes_ostream is always allocated in the standard allocator,
    // and storage_size() is never called on objects in the standard allocator,
    // it's okay.
    size_t storage_size() const noexcept {
        return sizeof(*this) + frag_size;
    }
} __attribute__((packed));

// Used to store managed_bytes data in layout 2. (See the docs of managed_bytes).
struct [[gnu::packed]] single_chunk_blob_storage {
    using size_type = uint32_t;
    using char_type = bytes_view::value_type;

    // Stored inline in managed_bytes.
    // Note the [[packed]]. It allows ref_type to be stored unaligned
    // in the `union` in managed_bytes. (It wouldn't fit otherwise).
    struct [[gnu::packed]] ref_type {
        // managed_bytes has enough spare bytes to store the size inline,
        // so we do that to save a few bytes in the external allocation.
        single_chunk_blob_storage* ptr = nullptr;
        size_type size = 0;
    };

    // Backref is needed to update the parent's pointer to us when we are
    // migrated during memory defragmentation.
    // (See the docs of allocation_strategy).
    ref_type* backref;

    // The storage provided by this fragment.
    char_type data[];

    single_chunk_blob_storage(ref_type* backref, size_type size) noexcept
        : backref(backref)
    {
        backref->ptr = this;
        backref->size = size;
    }

    single_chunk_blob_storage(single_chunk_blob_storage&& o) noexcept
        : backref(o.backref)
    {
        backref->ptr = this;
        memcpy(data, o.data, backref->size);
    }

    size_t storage_size() const noexcept {
        return sizeof(*this) + backref->size;
    }
};

// A managed version of "bytes" (can be used with LSA).
//
// Sometimes also used as a general-purpose fragmented buffer outside of LSA context,
// but this is not recommended, because it's too easy to accidentally destroy it
// in a different allocator than it was allocated in, which can break the program
// in a hard-to-predict way.
//
// managed_bytes has three storage layouts:
// 1. Inline.
//    Used for data which fits into max_inline_size.
// 2. External contiguous. (Single-allocation).
//    Used for data which fits into preferred_max_contiguous_allocation().
//    (At the moment of writing: 128 kiB and 12.8 kiB in LSA).
//    The storage is a single single_chunk_blob_storage object.
// 3. External fragmented. (Multi-allocation).
//    Used for everything else.
//    The storage is a chain of multi_chunk_blob_storage objects.
//
// Layout 2 exists as an optimization for the most common allocation sizes (several bytes).
// There is nothing which prevents implementing these with layout 3, but layout 3 stores slightly
// more metadata in the allocated buffer (pointer to the next fragment, size of the current fragment),
// which adds up to a big overhead when used with small allocations.
// E.g. 8-byte values are allocated externally -- each has additional 1 byte of flags and 8 bytes
// of timestamp, so it's 17 bytes in total and that doesn't fit into inline storage.
// And adding 16 bytes to each 17-byte cell is a big waste.
//
// The code of `class managed_bytes` is responsible for allocating and freeing the storage.
// Code responsible for reading and writing it is in managed_bytes_basic_view.
// The implementation details of these two classes are entangled.
class managed_bytes {
    friend class bytes_ostream;
    static constexpr size_t max_inline_size = 15;

    // The current layout is discerned by `inline_size`:
    // >0 -> layout 1 (inline). In this case, the value of `inline_size` holds the data size.
    // -1 -> layout 2 (single_chunk_blob_storage)
    // -2 -> layout 3 (multi_chunk_blob_storage)
    union u {
        u() {}
        ~u() {}
        bytes_view::value_type inline_data[max_inline_size]; // Stores the data directly. Size is in inline_size.
        single_chunk_blob_storage::ref_type single_chunk_ref; // Points to external storage and stores the data size.
        multi_chunk_blob_storage::ref_type multi_chunk_ref; // Points to external storage.
    } _u;
    int8_t _inline_size = 0;

private:
    bool is_multi_chunk() const noexcept {
        return _inline_size < -1;
    }
    bool is_single_chunk() const noexcept {
        return _inline_size == -1;
    }
    bool is_inline() const noexcept {
        return _inline_size >= 0;
    }
    size_t max_seg(allocation_strategy& alctr) {
        return alctr.preferred_max_contiguous_allocation() - std::max(sizeof(multi_chunk_blob_storage), sizeof(single_chunk_blob_storage));
    }
    void free_chain(multi_chunk_blob_storage* p) noexcept {
        auto& alctr = current_allocator();
        while (p) {
            auto n = p->next;
            alctr.destroy(p);
            p = n;
        }
    }

    explicit managed_bytes(multi_chunk_blob_storage* data) {
        _inline_size = -2;
        _u.multi_chunk_ref.ptr = data;
        data->backref = &_u.multi_chunk_ref;
    }
public:
    using size_type = multi_chunk_blob_storage::size_type;
    struct initialized_later {};

    managed_bytes() = default;

    managed_bytes(const multi_chunk_blob_storage::char_type* ptr, size_type size)
        : managed_bytes(bytes_view(ptr, size)) {}

    explicit managed_bytes(const bytes& b) : managed_bytes(static_cast<bytes_view>(b)) {}

    template <FragmentedView View>
    explicit managed_bytes(View v);

    managed_bytes(initialized_later, size_type size) {
        memory::on_alloc_point();
        if (size <= max_inline_size) {
            _inline_size = size;
        } else {
          auto& alctr = current_allocator();
          auto maxseg = max_seg(alctr);
          if (size < maxseg) {
            _inline_size = -1;
            void* p = alctr.alloc<single_chunk_blob_storage>(sizeof(single_chunk_blob_storage) + size);
            new (p) single_chunk_blob_storage(&_u.single_chunk_ref, size);
          } else {
            _inline_size = -2;
            auto maxseg = max_seg(alctr);
            auto now = std::min(size_t(size), maxseg);
            void* p = alctr.alloc<multi_chunk_blob_storage>(sizeof(multi_chunk_blob_storage) + now);
            auto first = new (p) multi_chunk_blob_storage(&_u.multi_chunk_ref, size, now);
            auto last = first;
            size -= now;
            try {
                while (size) {
                    auto now = std::min(size_t(size), maxseg);
                    void* p = alctr.alloc<multi_chunk_blob_storage>(sizeof(multi_chunk_blob_storage) + now);
                    last = new (p) multi_chunk_blob_storage(&last->next, 0, now);
                    size -= now;
                }
            } catch (...) {
                free_chain(first);
                throw;
            }
          }
        }
    }

    explicit managed_bytes(bytes_view v) : managed_bytes(single_fragmented_view(v)) {};

    managed_bytes(std::initializer_list<bytes::value_type> b) : managed_bytes(b.begin(), b.size()) {}

    ~managed_bytes() noexcept {
        if (is_multi_chunk()) {
            free_chain(_u.multi_chunk_ref);
        } else if (is_single_chunk()) {
            auto& alctr = current_allocator();
            alctr.destroy(_u.single_chunk_ref.ptr);
        }
    }

    // Defined later in the file because it depends on managed_bytes_mutable_view.
    managed_bytes(const managed_bytes& o);

    managed_bytes(managed_bytes&& o) noexcept {
        // Microoptimization: we use memcpy instead of assignments because
        // the compiler refuses the merge the load/stores otherwise for some reason.
        std::memcpy(reinterpret_cast<char*>(this), &o, sizeof(managed_bytes));
        o._inline_size = 0;
        if (is_multi_chunk()) {
            _u.multi_chunk_ref.ptr->backref = &_u.multi_chunk_ref;
        } else if (is_single_chunk()) {
            _u.single_chunk_ref.ptr->backref = &_u.single_chunk_ref;
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

    // Defined later in the file because these depend on managed_bytes_mutable_view.
    bool operator==(const managed_bytes& o) const;
    bytes_view::value_type& operator[](size_type index);
    const bytes_view::value_type& operator[](size_type index) const;

    size_type size() const {
        if (is_multi_chunk()) {
            return _u.multi_chunk_ref->size;
        } else if (is_single_chunk()) {
            return _u.single_chunk_ref.size;
        } else {
            return _inline_size;
        }
    }

    bool empty() const {
        return _inline_size == 0;
    }

    // Returns the amount of external memory used.
    size_t external_memory_usage() const noexcept {
        if (is_multi_chunk()) {
            size_t mem = 0;
            multi_chunk_blob_storage* blob = _u.multi_chunk_ref;
            while (blob) {
                mem += blob->frag_size + sizeof(multi_chunk_blob_storage);
                blob = blob->next;
            }
            return mem;
        } else if (is_single_chunk()) {
            return _u.single_chunk_ref.size + sizeof(single_chunk_blob_storage);
        }
        return 0;
    }

    // Returns the minimum possible amount of external memory used by a managed_bytes
    // of the same size as us.
    // In other words, it returns the amount of external memory that would used by this
    // managed_bytes if all data was allocated in one big fragment.
    size_t minimal_external_memory_usage() const noexcept {
        if (is_inline()) {
            return 0;
        } else {
            return sizeof(single_chunk_blob_storage) + size();
        }
    }

    // Defined later in the file because it depends on managed_bytes_mutable_view.
    template <std::invocable<bytes_view> Func>
    std::invoke_result_t<Func, bytes_view> with_linearized(Func&& func) const;

    template <mutable_view is_mutable_view>
    friend class managed_bytes_basic_view;
};
// Sanity check.
static_assert(sizeof(managed_bytes) == 16);

template <mutable_view is_mutable>
class managed_bytes_basic_view {
public:
    using fragment_type = std::conditional_t<is_mutable == mutable_view::yes, bytes_mutable_view, bytes_view>;
    using owning_type = std::conditional_t<is_mutable == mutable_view::yes, managed_bytes, const managed_bytes>;
    using value_type = typename fragment_type::value_type;
    using value_type_maybe_const = std::conditional_t<is_mutable == mutable_view::yes, value_type, const value_type>;
private:
    fragment_type _current_fragment = {};
    multi_chunk_blob_storage* _next_fragments = nullptr;
    size_t _size = 0;
private:
    managed_bytes_basic_view(fragment_type current_fragment, multi_chunk_blob_storage* next_fragments, size_t size)
        : _current_fragment(current_fragment)
        , _next_fragments(next_fragments)
        , _size(size) {
    }
public:
    managed_bytes_basic_view() = default;
    managed_bytes_basic_view(const managed_bytes_basic_view&) = default;
    managed_bytes_basic_view(owning_type& mb) {
        if (mb.is_inline()) {
            _current_fragment = fragment_type(mb._u.inline_data, mb._inline_size);
            _size = mb._inline_size;
        } else if (mb.is_single_chunk()) {
            auto p = mb._u.single_chunk_ref.ptr;
            _current_fragment = fragment_type(p->data, mb._u.single_chunk_ref.size);
            _next_fragments = nullptr;
            _size = _current_fragment.size();
        } else {
            multi_chunk_blob_storage* p = mb._u.multi_chunk_ref;
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

    friend managed_bytes_basic_view<mutable_view::no> build_managed_bytes_view_from_internals(bytes_view current_fragment, multi_chunk_blob_storage* next_fragment, size_t size);
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
build_managed_bytes_view_from_internals(bytes_view current_fragment, multi_chunk_blob_storage* next_fragment, size_t size) {
    return managed_bytes_view(current_fragment, next_fragment, size);
}

inline bytes_view::value_type& managed_bytes::operator[](size_type index) {
    return const_cast<bytes_view::value_type&>(std::as_const(*this)[index]);
}

inline const bytes_view::value_type& managed_bytes::operator[](size_type index) const {
    if (is_inline()) {
        return _u.inline_data[index];
    } else if (is_single_chunk()) {
        return _u.single_chunk_ref.ptr->data[index];
    } else {
        managed_bytes_view self(*this);
        return self[index];
    }
}

template <std::invocable<bytes_view> Func>
std::invoke_result_t<Func, bytes_view> managed_bytes::with_linearized(Func&& func) const {
    return ::with_linearized(managed_bytes_view(*this), func);
}

inline bool managed_bytes::operator==(const managed_bytes& o) const {
    return managed_bytes_view(*this) == managed_bytes_view(o);
}

inline managed_bytes::managed_bytes(const managed_bytes& o) {
    if (o.is_inline()) {
        _inline_size = o._inline_size;
        _u = o._u;
    } else if (o.is_single_chunk()) {
        memory::on_alloc_point();
        auto& alctr = current_allocator();
        void* p = alctr.alloc<single_chunk_blob_storage>(sizeof(single_chunk_blob_storage) + o._u.single_chunk_ref.size);
        new (p) single_chunk_blob_storage(&_u.single_chunk_ref, o._u.single_chunk_ref.size);
        memcpy(_u.single_chunk_ref.ptr->data, o._u.single_chunk_ref.ptr->data, o._u.single_chunk_ref.size);
        _inline_size = -1;
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

// The formatters below are used only by tests.
template <> struct fmt::formatter<managed_bytes_view> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const managed_bytes_view& v, FormatContext& ctx) const {
        auto out = ctx.out();
        for (bytes_view frag : fragment_range(v)) {
            out = fmt::format_to(out, "{}", fmt_hex(frag));
        }
        return out;
    }
};
inline std::ostream& operator<<(std::ostream& os, const managed_bytes_view& v) {
    fmt::print(os, "{}", v);
    return os;
}

template <> struct fmt::formatter<managed_bytes> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const managed_bytes& b, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{}", managed_bytes_view(b));
    }
};
inline std::ostream& operator<<(std::ostream& os, const managed_bytes& b) {
    fmt::print(os, "{}", b);
    return os;
}

template <> struct fmt::formatter<managed_bytes_opt> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const managed_bytes_opt& opt, FormatContext& ctx) const {
        if (opt) {
            return fmt::format_to(ctx.out(), "{}", *opt);
        }
        return fmt::format_to(ctx.out(), "null");
    }
};
