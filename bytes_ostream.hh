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

#include "types.hh"
#include "net/byteorder.hh"
#include "core/unaligned.hh"

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
private:
    static_assert(sizeof(value_type) == 1, "value_type is assumed to be one byte long");
    struct chunk {
        // FIXME: group fragment pointers to reduce pointer chasing when packetizing
        std::unique_ptr<chunk> next;
        size_type offset; // Also means "size" after chunk is closed
        size_type size;
        value_type data[0];
        void operator delete(void* ptr) { free(ptr); }
    };
    // FIXME: consider increasing chunk size as the buffer grows
    static constexpr size_type chunk_size{512};
    static constexpr size_type usable_chunk_size{chunk_size - sizeof(chunk)};
private:
    std::unique_ptr<chunk> _begin;
    chunk* _current;
    size_type _size;
public:
    class fragment_iterator : public std::iterator<std::input_iterator_tag, bytes_view> {
        chunk* _current;
    public:
        fragment_iterator(chunk* current) : _current(current) {}
        fragment_iterator(const fragment_iterator&) = default;
        fragment_iterator& operator=(const fragment_iterator&) = default;
        bytes_view operator*() const {
            return { _current->data, _current->offset };
        }
        bytes_view operator->() const {
            return *(*this);
        }
        fragment_iterator& operator++() {
            _current = _current->next.get();
            return *this;
        }
        fragment_iterator operator++(int) {
            fragment_iterator tmp(*this);
            ++(*this);
            return tmp;
        }
        bool operator==(const fragment_iterator& other) const {
            return _current == other._current;
        }
        bool operator!=(const fragment_iterator& other) const {
            return _current != other._current;
        }
    };
private:
    inline size_type current_space_left() const {
        if (!_current) {
            return 0;
        }
        return _current->size - _current->offset;
    }
    // Makes room for a contiguous region of given size.
    // The region is accounted for as already written.
    // size must not be zero.
    value_type* alloc(size_type size) {
        if (size <= current_space_left()) {
            auto ret = _current->data + _current->offset;
            _current->offset += size;
            _size += size;
            return ret;
        } else {
            auto alloc_size = size <= usable_chunk_size ? chunk_size : (size + sizeof(chunk));
            auto space = malloc(alloc_size);
            if (!space) {
                throw std::bad_alloc();
            }
            auto new_chunk = std::unique_ptr<chunk>(new (space) chunk());
            new_chunk->offset = size;
            new_chunk->size = alloc_size - sizeof(chunk);
            if (_current) {
                _current->next = std::move(new_chunk);
                _current = _current->next.get();
            } else {
                _begin = std::move(new_chunk);
                _current = _begin.get();
            }
            _size += size;
            return _current->data;
        };
    }
public:
    bytes_ostream() noexcept
        : _begin()
        , _current(nullptr)
        , _size(0)
    { }

    bytes_ostream(bytes_ostream&& o) noexcept
        : _begin(std::move(o._begin))
        , _current(o._current)
        , _size(o._size)
    {
        o._current = nullptr;
        o._size = 0;
    }

    bytes_ostream(const bytes_ostream& o)
        : _begin()
        , _current(nullptr)
        , _size(0)
    {
        append(o);
    }

    bytes_ostream& operator=(const bytes_ostream& o) {
        _size = 0;
        _current = nullptr;
        _begin = {};
        append(o);
        return *this;
    }

    bytes_ostream& operator=(bytes_ostream&& o) noexcept {
        _size = o._size;
        _begin = std::move(o._begin);
        _current = o._current;
        o._current = nullptr;
        o._size = 0;
        return *this;
    }

    template <typename T>
    struct place_holder {
        value_type* ptr;
    };

    // Writes given values in big-endian format
    template <typename T>
    inline
    std::enable_if_t<std::is_fundamental<T>::value, void>
    write(T val) {
        *reinterpret_cast<unaligned<T>*>(alloc(sizeof(T))) = net::hton(val);
    }

    // Returns a place holder for a value to be written later.
    template <typename T>
    inline
    std::enable_if_t<std::is_fundamental<T>::value, place_holder<T>>
    write_place_holder() {
        return place_holder<T>{alloc(sizeof(T))};
    }

    value_type* write_place_holder(size_type size) {
        return alloc(size);
    }

    // Writes given sequence of bytes
    inline void write(bytes_view v) {
        if (v.empty()) {
            return;
        }
        auto space_left = current_space_left();
        if (v.size() <= space_left) {
            memcpy(_current->data + _current->offset, v.begin(), v.size());
            _current->offset += v.size();
            _size += v.size();
        } else {
            if (space_left) {
                memcpy(_current->data + _current->offset, v.begin(), space_left);
                _current->offset += space_left;
                _size += space_left;
                v.remove_prefix(space_left);
            }
            memcpy(alloc(v.size()), v.begin(), v.size());
        }
    }

    // Writes given sequence of bytes with a preceding length component encoded in big-endian format
    inline void write_blob(bytes_view v) {
        assert((size_type)v.size() == v.size());
        write<size_type>(v.size());
        write(v);
    }

    // Writes given value into the place holder in big-endian format
    template <typename T>
    inline void set(place_holder<T> ph, T val) {
        *reinterpret_cast<unaligned<T>*>(ph.ptr) = net::hton(val);
    }

    bool is_linearized() const {
        return !_begin || !_begin->next;
    }

    // Call only when is_linearized()
    bytes_view view() const {
        assert(is_linearized());
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

        auto new_chunk = std::unique_ptr<chunk>(new (space) chunk());
        new_chunk->offset = _size;
        new_chunk->size = _size;

        auto dst = new_chunk->data;
        auto r = _begin.get();
        while (r) {
            auto next = r->next.get();
            dst = std::copy_n(r->data, r->offset, dst);
            r = next;
        }

        _current = new_chunk.get();
        _begin = std::move(new_chunk);
        return bytes_view(_current->data, _size);
    }

    // Returns the amount of bytes written so far
    size_type size() const {
        return _size;
    }

    bool empty() const {
        return _size == 0;
    }

    void reserve(size_t size) {
        // FIXME: implement
    }

    void append(const bytes_ostream& o) {
        if (o.size() > 0) {
            auto dst = alloc(o.size());
            auto r = o._begin.get();
            while (r) {
                dst = std::copy_n(r->data, r->offset, dst);
                r = r->next.get();
            }
        }
    }

    // begin() and end() form an input range to bytes_view representing fragments.
    // Any modification of this instance invalidates iterators.
    fragment_iterator begin() const { return { _begin.get() }; }
    fragment_iterator end() const { return { nullptr }; }

    boost::iterator_range<fragment_iterator> fragments() const {
        return { begin(), end() };
    }

    struct position {
        chunk* _chunk;
        size_type _offset;
    };

    position pos() const {
        return { _current, _current ? _current->offset : 0 };
    }

    // Returns the amount of bytes written since given position.
    // "pos" must be valid.
    size_type written_since(position pos) {
        chunk* c = pos._chunk;
        if (!c) {
            return _size;
        }
        size_type total = c->offset - pos._offset;
        c = c->next.get();
        while (c) {
            total += c->offset;
            c = c->next.get();
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
        _current->next = nullptr;
        _current->offset = pos._offset;
    }
};
