/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <boost/range/iterator_range.hpp>

#include "bytes.hh"
#include "utils/assert.hh"
#include "utils/managed_bytes.hh"
#include <seastar/core/simple-stream.hh>
#include <seastar/core/loop.hh>
#include <bit>
#include <concepts>

/**
 * Utility for writing data into a buffer when its final size is not known up front.
 *
 * Internally the data is written into a chain of chunks allocated on-demand.
 * No resizing of previously written data happens.
 *
 */
class bytes_ostream {
public:
    using size_type = bytes::size_type;
    using value_type = bytes::value_type;
    using fragment_type = bytes_view;
    static constexpr size_type max_chunk_size() { return max_alloc_size() - sizeof(chunk); }
private:
    static_assert(sizeof(value_type) == 1, "value_type is assumed to be one byte long");
    // Note: while appending data, chunk::size refers to the allocated space in the chunk,
    //       and chunk::frag_size refers to the currently occupied space in the chunk.
    //       After building, the first chunk::size is the whole object size, and chunk::frag_size
    //       doesn't change. This fits with managed_bytes interpretation.
    using chunk = multi_chunk_blob_storage;
    static constexpr size_type default_chunk_size{512};
    static constexpr size_type max_alloc_size() { return 128 * 1024; }
private:
    chunk::ref_type _begin;
    chunk* _current;
    size_type _size;
    size_type _initial_chunk_size = default_chunk_size;
public:
    class fragment_iterator {
    public:
        using iterator_category = std::input_iterator_tag;
        using value_type = bytes_view;
        using difference_type = std::ptrdiff_t;
        using pointer = bytes_view*;
        using reference = bytes_view&;

        struct implementation {
            chunk* current_chunk;
        };
    private:
        chunk* _current = nullptr;
    public:
        fragment_iterator() = default;
        fragment_iterator(chunk* current) : _current(current) {}
        fragment_iterator(const fragment_iterator&) = default;
        fragment_iterator& operator=(const fragment_iterator&) = default;
        bytes_view operator*() const {
            return { _current->data, _current->frag_size };
        }
        bytes_view operator->() const {
            return *(*this);
        }
        fragment_iterator& operator++() {
            _current = _current->next;
            return *this;
        }
        fragment_iterator operator++(int) {
            fragment_iterator tmp(*this);
            ++(*this);
            return tmp;
        }
        bool operator==(const fragment_iterator&) const = default;
        implementation extract_implementation() const {
            return implementation {
                .current_chunk = _current,
            };
        }
    };
    using const_iterator = fragment_iterator;

    class output_iterator {
    public:
        using iterator_category = std::output_iterator_tag;
        using difference_type = std::ptrdiff_t;
        using value_type = bytes_ostream::value_type;
        using pointer = bytes_ostream::value_type*;
        using reference = bytes_ostream::value_type&;

        friend class bytes_ostream;

    private:
        bytes_ostream* _ostream = nullptr;

    private:
        explicit output_iterator(bytes_ostream& os) : _ostream(&os) { }

    public:
        reference operator*() const { return *_ostream->write_place_holder(1); }
        output_iterator& operator++() { return *this; }
        output_iterator operator++(int) { return *this; }
    };
private:
    inline size_type current_space_left() const {
        if (!_current) {
            return 0;
        }
        return _current->size - _current->frag_size;
    }
    // Figure out next chunk size.
    //   - must be enough for data_size + sizeof(chunk)
    //   - must be at least _initial_chunk_size
    //   - try to double each time to prevent too many allocations
    //   - should not exceed max_alloc_size, unless data_size requires so
    //   - will be power-of-two so the allocated memory can be fully utilized.
    size_type next_alloc_size(size_t data_size) const {
        auto next_size = _current
                ? _current->size * 2
                : _initial_chunk_size;
        next_size = std::min(next_size, max_alloc_size());
        auto r = std::max<size_type>(next_size, data_size + sizeof(chunk));
        return std::bit_ceil(r);
    }
    // Makes room for a contiguous region of given size.
    // The region is accounted for as already written.
    // size must not be zero.
    [[gnu::always_inline]]
    value_type* alloc(size_type size) {
        if (__builtin_expect(size <= current_space_left(), true)) {
            auto ret = _current->data + _current->frag_size;
            _current->frag_size += size;
            _size += size;
            return ret;
        } else {
            return alloc_new(size);
        }
    }
    [[gnu::noinline]]
    value_type* alloc_new(size_type size) {
            auto alloc_size = next_alloc_size(size);
            auto space = malloc(alloc_size);
            if (!space) {
                throw std::bad_alloc();
            }
            auto backref = _current ? &_current->next : &_begin;
            auto new_chunk = new (space) chunk(backref, alloc_size - sizeof(chunk), size);
            _current = new_chunk;
            _size += size;
            return _current->data;
    }
    [[gnu::noinline]]
    void free_chain(chunk* c) noexcept {
        while (c) {
            auto n = c->next;
            c->~chunk();
            ::free(c);
            c = n;
        }
    }
public:
    explicit bytes_ostream(size_t initial_chunk_size) noexcept
        : _begin()
        , _current(nullptr)
        , _size(0)
        , _initial_chunk_size(initial_chunk_size)
    { }

    bytes_ostream() noexcept : bytes_ostream(default_chunk_size) {}

    bytes_ostream(bytes_ostream&& o) noexcept
        : _begin(std::exchange(o._begin, {}))
        , _current(o._current)
        , _size(o._size)
        , _initial_chunk_size(o._initial_chunk_size)
    {
        o._current = nullptr;
        o._size = 0;
    }

    bytes_ostream(const bytes_ostream& o)
        : _begin()
        , _current(nullptr)
        , _size(0)
        , _initial_chunk_size(o._initial_chunk_size)
    {
        append(o);
    }

    ~bytes_ostream() {
        free_chain(_begin.ptr);
    }

    bytes_ostream& operator=(const bytes_ostream& o) {
        if (this != &o) {
            auto x = bytes_ostream(o);
            *this = std::move(x);
        }
        return *this;
    }

    bytes_ostream& operator=(bytes_ostream&& o) noexcept {
        if (this != &o) {
            this->~bytes_ostream();
            new (this) bytes_ostream(std::move(o));
        }
        return *this;
    }

    template <typename T>
    struct place_holder {
        value_type* ptr;
        // makes the place_holder looks like a stream
        seastar::simple_output_stream get_stream() {
            return seastar::simple_output_stream(reinterpret_cast<char*>(ptr), sizeof(T));
        }
    };

    // Returns a place holder for a value to be written later.
    template <std::integral T>
    inline
    place_holder<T>
    write_place_holder() {
        return place_holder<T>{alloc(sizeof(T))};
    }

    [[gnu::always_inline]]
    value_type* write_place_holder(size_type size) {
        return alloc(size);
    }

    // Writes given sequence of bytes
    [[gnu::always_inline]]
    inline void write(bytes_view v) {
        if (v.empty()) {
            return;
        }

        auto this_size = std::min(v.size(), size_t(current_space_left()));
        if (__builtin_expect(this_size, true)) {
            memcpy(_current->data + _current->frag_size, v.begin(), this_size);
            _current->frag_size += this_size;
            _size += this_size;
            v.remove_prefix(this_size);
        }

        while (!v.empty()) {
            auto this_size = std::min(v.size(), size_t(max_chunk_size()));
            std::copy_n(v.begin(), this_size, alloc_new(this_size));
            v.remove_prefix(this_size);
        }
    }

    [[gnu::always_inline]]
    void write(const char* ptr, size_t size) {
        write(bytes_view(reinterpret_cast<const signed char*>(ptr), size));
    }

    bool is_linearized() const {
        return !_begin || !_begin->next;
    }

    // Call only when is_linearized()
    bytes_view view() const {
        SCYLLA_ASSERT(is_linearized());
        if (!_current) {
            return bytes_view();
        }

        return bytes_view(_current->data, _size);
    }

    // Makes the underlying storage contiguous and returns a view to it.
    // Invalidates all previously created placeholders.
    bytes_view linearize() {
        if (is_linearized()) {
            return view();
        }

        auto space = malloc(_size + sizeof(chunk));
        if (!space) {
            throw std::bad_alloc();
        }

        auto old_begin = _begin;
        auto new_chunk = new (space) chunk(&_begin, _size, _size);

        auto dst = new_chunk->data;
        auto r = old_begin.ptr;
        while (r) {
            auto next = r->next;
            dst = std::copy_n(r->data, r->frag_size, dst);
            r->~chunk();
            ::free(r);
            r = next;
        }

        _current = new_chunk;
        _begin = std::move(new_chunk);
        return bytes_view(_current->data, _size);
    }

    // Returns the amount of bytes written so far
    size_type size() const {
        return _size;
    }

    // For the FragmentRange concept
    size_type size_bytes() const {
        return _size;
    }

    bool empty() const {
        return _size == 0;
    }

    void reserve(size_t size) {
        // FIXME: implement
    }

    void append(const bytes_ostream& o) {
        for (auto&& bv : o.fragments()) {
            write(bv);
        }
    }

    // Removes n bytes from the end of the bytes_ostream.
    // Beware of O(n) algorithm.
    void remove_suffix(size_t n) {
        _size -= n;
        auto left = _size;
        auto current = _begin.ptr;
        while (current) {
            if (current->frag_size >= left) {
                current->frag_size = left;
                _current = current;
                free_chain(current->next);
                current->next = nullptr;
                return;
            }
            left -= current->frag_size;
            current = current->next;
        }
    }

    // begin() and end() form an input range to bytes_view representing fragments.
    // Any modification of this instance invalidates iterators.
    fragment_iterator begin() const { return { _begin.ptr }; }
    fragment_iterator end() const { return { nullptr }; }

    output_iterator write_begin() { return output_iterator(*this); }

    boost::iterator_range<fragment_iterator> fragments() const {
        return { begin(), end() };
    }

    struct position {
        chunk* _chunk;
        size_type _offset;
    };

    position pos() const {
        return { _current, _current ? _current->frag_size : 0 };
    }

    // Returns the amount of bytes written since given position.
    // "pos" must be valid.
    size_type written_since(position pos) {
        chunk* c = pos._chunk;
        if (!c) {
            return _size;
        }
        size_type total = c->frag_size - pos._offset;
        c = c->next;
        while (c) {
            total += c->frag_size;
            c = c->next;
        }
        return total;
    }

    // Rollbacks all data written after "pos".
    // Invalidates all placeholders and positions created after "pos".
    void retract(position pos) {
        if (!pos._chunk) {
            *this = {};
            return;
        }
        _size -= written_since(pos);
        _current = pos._chunk;
        free_chain(_current->next);
        _current->next = nullptr;
        _current->frag_size = pos._offset;
    }

    void reduce_chunk_count() {
        // FIXME: This is a simplified version. It linearizes the whole buffer
        // if its size is below max_chunk_size. We probably could also gain
        // some read performance by doing "real" reduction, i.e. merging
        // all chunks until all but the last one is max_chunk_size.
        if (size() < max_chunk_size()) {
            linearize();
        }
    }

    bool operator==(const bytes_ostream& other) const {
        auto as = fragments().begin();
        auto as_end = fragments().end();
        auto bs = other.fragments().begin();
        auto bs_end = other.fragments().end();

        auto a = *as++;
        auto b = *bs++;
        while (!a.empty() || !b.empty()) {
            auto now = std::min(a.size(), b.size());
            if (!std::equal(a.begin(), a.begin() + now, b.begin(), b.begin() + now)) {
                return false;
            }
            a.remove_prefix(now);
            if (a.empty() && as != as_end) {
                a = *as++;
            }
            b.remove_prefix(now);
            if (b.empty() && bs != bs_end) {
                b = *bs++;
            }
        }
        return true;
    }

    // Makes this instance empty.
    //
    // The first buffer is not deallocated, so callers may rely on the
    // fact that if they write less than the initial chunk size between
    // the clear() calls then writes will not involve any memory allocations,
    // except for the first write made on this instance.
    void clear() {
        if (_begin.ptr) {
            _begin.ptr->frag_size = 0;
            _size = 0;
            free_chain(_begin.ptr->next);
            _begin.ptr->next = nullptr;
            _current = _begin.ptr;
        }
    }

    managed_bytes to_managed_bytes() && {
        if (_size) {
            _begin.ptr->size = _size;
            _current = nullptr;
            _size = 0;
            auto begin_ptr = _begin.ptr;
            _begin.ptr = nullptr;
            return managed_bytes(begin_ptr);
        } else {
            return managed_bytes();
        }
    }

    // Makes this instance empty using async continuations, while allowing yielding.
    //
    // The first buffer is not deallocated, so callers may rely on the
    // fact that if they write less than the initial chunk size between
    // the clear() calls then writes will not involve any memory allocations,
    // except for the first write made on this instance.
    future<> clear_gently() noexcept {
        if (!_begin.ptr) {
            return make_ready_future<>();
        }
        _begin->frag_size = 0;
        _current = _begin.ptr;
        _size = 0;
        return do_until([this] { return !_begin.ptr->next; }, [this] {
            auto second_chunk = _begin.ptr->next;
            auto next = second_chunk->next;
            second_chunk->~chunk();
            ::free(second_chunk);
            _begin->next = std::move(next);
            return make_ready_future<>();
        });
    }
};
