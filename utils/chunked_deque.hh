/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright (C) 2022-present ScyllaDB Ltd.
 */

#pragma once

#include <limits>
#include <memory>
#include <compare>

#include <seastar/core/circular_buffer_fixed_capacity.hh>

namespace utils {

/// \brief An unbounded double-ended queue of objects of type T akin to std::deque.
///
/// It provides operations to push items in both ends of the queue, and pop them
/// from either end of the queue - both operations are guaranteed O(1)
/// (not just amortized O(1)). The size() operation is also O(1).
/// chunked_deque also guarantees that the largest contiguous memory allocation
/// it does is O(1). The total memory used is of course, O(N).
///
/// Unlike std::deque's iterators, chunked_deque iterator addition, subtraction, and
/// array subscript are O(n) rather than O(1), unless the subscript falls in the
/// currently pointed chunk's boundaries.  Otherwise, the iterator skips over
/// complete chunks, but does that one at a time as they are linked in a doubly-
/// linked list.
template <typename T, size_t items_per_chunk = 128, int save_free_chunks = 1>
class chunked_deque {
    static_assert((items_per_chunk & (items_per_chunk - 1)) == 0,
            "chunked_deque chunk size must be power of two");

    union maybe_item {
        maybe_item() noexcept {}
        ~maybe_item() {}
        T data;
    };

    struct chunk : public circular_buffer_fixed_capacity<T, items_per_chunk> {
        using iterator = typename circular_buffer_fixed_capacity<T, items_per_chunk>::iterator;

        struct chunk* next;
        struct chunk* prev;

        bool full() const noexcept {
            return this->size() >= items_per_chunk;
        }
    };

    chunk* _front_chunk = nullptr;
    chunk* _back_chunk = nullptr;

    // We want an O(1) size but don't want to maintain a size() counter
    // because this will slow down every push and pop operation just for
    // the rare size() call. Instead, we just keep a count of chunks (which
    // doesn't change on every push or pop), from which we can calculate
    // size() when needed, and still be O(1).
    // This assumes the invariant that all middle chunks (except the front
    // and back) are always full.
    size_t _nchunks = 0;
    // A list of freed chunks, to support reserve() and to improve
    // performance of repeated push and pop, especially on an empty queue.
    // It is a performance/memory tradeoff how many freed chunks to keep
    // here (see save_free_chunks constant below).
    chunk* _free_chunks = nullptr;
    size_t _nfree_chunks = 0;
public:
    using value_type = T;
    using size_type = size_t;
    using difference_type = ssize_t;
    using reference = T&;
    using pointer = T*;
    using const_reference = const T&;
    using const_pointer = const T*;

    chunked_deque() noexcept = default;

    chunked_deque(const chunked_deque& X) = delete;
    chunked_deque& operator=(const chunked_deque&) = delete;

    chunked_deque(chunked_deque&& x) noexcept
        : _front_chunk(std::exchange(x._front_chunk, nullptr))
        , _back_chunk(std::exchange(x._back_chunk, nullptr))
        , _nchunks(std::exchange(x._nchunks, 0))
        , _free_chunks(std::exchange(x._free_chunks, nullptr))
        , _nfree_chunks(std::exchange(x._nfree_chunks, 0))
    {}
    chunked_deque& operator=(chunked_deque&& o) noexcept {
        if (&o != this) {
            this->~chunked_deque();
            new (this) chunked_deque(std::move(o));
        }
        return *this;
    }

    ~chunked_deque() {
        clear();
        shrink_to_fit();
    }

    template <typename... Args>
    inline void emplace_front(Args&&... args) {
        ensure_room_front();
        try {
            _front_chunk->emplace_front(std::forward<Args>(args)...);
        } catch (...) {
            undo_room_front();
            throw;
        }
    }
    inline void push_front(const T& data) {
        ensure_room_front();
        try {
            _front_chunk->push_front(data);
        } catch (...) {
            undo_room_front();
            throw;
        }
    }
    inline void push_front(T&& data) {
        ensure_room_front();
        try {
            _front_chunk->push_front(std::move(data));
        } catch (...) {
            undo_room_front();
            throw;
        }
    }
    inline T& front() noexcept {
        return _front_chunk->front();
    }
    inline const T& front() const noexcept {
        return _front_chunk->front();
    }
    inline void pop_front() noexcept {
        _front_chunk->pop_front();
        if (_front_chunk->empty()) {
            delete_front_chunk();
        }
    }

    template <typename... Args>
    inline void emplace_back(Args&&... args) {
        ensure_room_back();
        try {
            _back_chunk->emplace_back(std::forward<Args>(args)...);
        } catch (...) {
            undo_room_back();
            throw;
        }
    }
    inline void push_back(const T& data) {
        ensure_room_back();
        try {
            _back_chunk->push_back(data);
        } catch (...) {
            undo_room_back();
            throw;
        }
    }
    inline void push_back(T&& data) {
        ensure_room_back();
        try {
            _back_chunk->push_back(std::move(data));
        } catch (...) {
            undo_room_back();
            throw;
        }
    }
    inline T& back() noexcept {
        return _back_chunk->back();
    }
    inline const T& back() const noexcept {
        return _back_chunk->back();
    }
    inline void pop_back() noexcept {
        _back_chunk->pop_back();
        if (_back_chunk->empty()) {
            delete_back_chunk();
        }
    }

    size_type max_size() const noexcept {
        return std::numeric_limits<size_type>::max();
    }

    bool empty() const noexcept {
        return _front_chunk == nullptr;
    }

    size_t size() const noexcept {
        if (_front_chunk == nullptr) {
            return 0;
        } else if (_back_chunk == _front_chunk) {
            // Single chunk.
            return _front_chunk->size();
        } else {
            return _front_chunk->size() + _back_chunk->size()
                    + (_nchunks - 2) * items_per_chunk;
        }
    }

    void clear() noexcept {
        while (!empty()) {
            if constexpr (std::is_trivially_destructible_v<T>) {
                delete_front_chunk();
            } else {
                pop_front();
            }
        }
    }

    // reserve(n) ensures that at least (n - size()) further push calls can
    // be served without needing new memory allocation.
    // Calling pop()s between these push()es is also allowed and does not
    // alter this guarantee.
    // Note that reserve() does not reduce the amount of memory already
    // reserved - use shrink_to_fit() for that.
    void reserve(size_t n) {
        if (n <= size()) {
            return;
        }
        size_t need = n - size();
        if (_front_chunk) {
            size_t remaining = items_per_chunk - _front_chunk->size();
            if (remaining >= need) {
                return;
            }
            need -= remaining;
        }
        reserve_chunks((need + items_per_chunk - 1) / items_per_chunk);
    }


    // shrink_to_fit() frees memory held, but unused, by the queue. Such
    // unused memory might exist after pops, or because of reserve().
    void shrink_to_fit() noexcept {
        while (_free_chunks) {
            auto next = _free_chunks->next;
            delete _free_chunks;
            _free_chunks = next;
        }
        _nfree_chunks = 0;
    }

private:
    class iterator_base {
    public:

    protected:
        const chunked_deque* _deq = nullptr;

        iterator_base(const chunked_deque& deq) noexcept
            : _deq(&deq)
        {}

        const chunked_deque& deq() const noexcept {
            return *_deq;
        }

    public:
        iterator_base() = default;
    };

    template <typename ChunkType, bool IsForward>
    class basic_sentinel;

    template <typename ChunkType, bool IsForward>
    class basic_iterator : public iterator_base {
        using iterator_base::iterator_base;

        using chunk_ptr_type = ChunkType*;
        using chunk_iterator = std::conditional_t<std::is_const_v<ChunkType>,
                typename circular_buffer_fixed_capacity<T, items_per_chunk>::const_iterator,
                typename circular_buffer_fixed_capacity<T, items_per_chunk>::iterator>;

        using idx_type = ssize_t;
        static constexpr auto min_idx = std::numeric_limits<idx_type>::min();
        static constexpr auto max_idx = std::numeric_limits<idx_type>::max();

        friend class chunked_deque;
        friend class basic_sentinel<ChunkType, IsForward>;
    public:
        using iterator_category = std::random_access_iterator_tag;
        using value_type = std::conditional_t<std::is_const_v<ChunkType>, const T, T>;
        using pointer = value_type*;
        using reference = value_type&;
        using difference_type = ssize_t;

    private:
        struct chunk_iterators {
            chunk_iterator cur;
            chunk_iterator begin;
            chunk_iterator end;
        };

        chunk_ptr_type _chunk = nullptr;
        idx_type _idx = 0;
        std::optional<chunk_iterators> _chunk_iterators = {};

        const auto& chunk() const noexcept {
            return _chunk;
        }

        auto set_chunk(chunk_ptr_type c) noexcept {
            return _chunk = c;
        }

        const auto& idx() const noexcept {
            return _idx;
        }

        auto& idx() noexcept {
            return _idx;
        }

        auto set_idx(idx_type idx) noexcept {
            return _idx = idx;
        }

        const auto& chunk_iterators() const noexcept {
            return *_chunk_iterators;
        }

        auto& chunk_iterators() noexcept {
            return *_chunk_iterators;
        }

        void set_chunk_iterators(struct chunk_iterators&& cits) noexcept {
            _chunk_iterators = std::move(cits);
        }

        void reset_chunk_iterators() noexcept {
            _chunk_iterators.reset();
        }

    protected:
        static basic_iterator make_begin(const chunked_deque& deq) {
            auto ret = basic_iterator(deq);
            if constexpr (IsForward) {
                ret.set_idx(0);
                ret.set_front_chunk();
            } else {
                ret.set_idx(deq.size() - 1);
                ret.set_back_chunk();
            }
            return ret;
        }

        static basic_iterator make_end(const chunked_deque& deq) {
            auto ret = basic_iterator(deq);
            if constexpr (IsForward) {
                ret.set_idx(deq.size());
            } else {
                ret.set_idx(-1);
            }
            return ret;
        }

    public:
        basic_iterator() = default;

        template <typename OtherChunkType>
        bool operator==(const basic_sentinel<OtherChunkType, IsForward>& s) const noexcept {
            assert(this->_deq == s._deq);
            if constexpr (IsForward) {
                return idx() >= this->deq()->size();
            } else {
                return idx() < 0;
            }
        }

        template <typename OtherChunkType>
        bool operator!=(const basic_sentinel<OtherChunkType, IsForward>& s) const noexcept {
            return !operator==(s);
        }

        template <typename OtherChunkType>
        inline std::weak_ordering operator<=>(const basic_sentinel<OtherChunkType, IsForward>& rhs) const noexcept {
            assert(this->_deq == rhs._deq);
            return *this == rhs ? std::strong_ordering::equivalent : std::strong_ordering::less;
        }

        template <typename OtherChunkType>
        bool operator==(const basic_iterator<OtherChunkType, IsForward>& it) const noexcept {
            assert(this->_deq == it._deq);
            return this->idx() == it.idx();
        }

        template <typename OtherChunkType>
        bool operator!=(const basic_iterator<OtherChunkType, IsForward>& it) const noexcept {
            return !operator==(it);
        }

        template <typename OtherChunkType>
        std::strong_ordering operator<=>(const basic_iterator<OtherChunkType, IsForward>& rhs) const noexcept {
            assert(this->_deq == rhs._deq);
            if constexpr (IsForward) {
                return idx() <=> rhs.idx();
            } else {
                return rhs.idx() <=> idx();
            }
        }

        pointer operator->() const noexcept {
            return &*chunk_iterators().cur;
        }

        reference operator*() const noexcept {
            return *chunk_iterators().cur;
        }

        reference operator[](difference_type n) const noexcept {
            auto it = this->operator+(n);
            return *it;
        }

        // pre-increment
        basic_iterator& operator++() noexcept {
            if constexpr (IsForward) {
                return inc();
            } else {
                return dec();
            }
        }

        // post-increment
        basic_iterator operator++(int) noexcept {
            auto pre = *this;
            ++*this;
            return pre;
        }

        // pre-decrement
        basic_iterator& operator--() noexcept {
            if constexpr (IsForward) {
                return dec();
            } else {
                return inc();
            }
        }

        // post-decrement
        basic_iterator operator--(int) noexcept {
            auto pre = *this;
            --*this;
            return pre;
        }

        basic_iterator& operator+=(difference_type n) noexcept {
            if constexpr (IsForward) {
                return add(n);
            } else {
                return sub(n);
            }
        }

        basic_iterator operator+(difference_type n) const noexcept {
            auto it = *this;
            it += n;
            return it;
        }

        friend basic_iterator operator+(difference_type n, const basic_iterator& it) noexcept {
            return it + n;
        }

        basic_iterator& operator-=(difference_type n) noexcept {
            if constexpr (IsForward) {
                return sub(n);
            } else {
                return add(n);
            }
        }

        basic_iterator operator-(difference_type n) const noexcept {
            auto it = *this;
            it -= n;
            return it;
        }

        difference_type operator-(const basic_iterator& rhs) const noexcept {
            if constexpr (IsForward) {
                return idx() - rhs.idx();
            } else {
                return rhs.idx() - idx();
            }
        }

    private:
        basic_iterator& inc() noexcept {
            ++idx();
            if (!chunk()) {
                if (idx() == 0) {
                    set_front_chunk();
                }
            } else {
                if (++chunk_iterators().cur == chunk_iterators().end) [[unlikely]] {
                    set_next_chunk();
                }
            }
            return *this;
        }

        basic_iterator& dec() noexcept {
            auto prev_idx = idx()--;
            if (!chunk()) {
                if (prev_idx == this->deq().size()) {
                    set_back_chunk();
                }
            } else {
                if (chunk_iterators().cur == chunk_iterators().begin) [[unlikely]] {
                    set_prev_chunk();
                } else {
                    --chunk_iterators().cur;
                }
            }
            return *this;
        }

        basic_iterator& add(difference_type n) noexcept {
            if (n == 0) {
                return *this;
            }

            if (n < 0) {
                sub(-n);
                return *this;
            }

            if (!_chunk) [[unlikely]] {
                if (idx() >= this->deq().size() || idx() < -n) {
                    idx() += n;
                    return *this;
                }
                n += idx();
                set_idx(0);
                set_front_chunk();
            }
            while (n > 0 && chunk()) {
                auto in_chunk = chunk_iterators().end - chunk_iterators().cur;
                if (n < in_chunk) {
                    idx() += n;
                    chunk_iterators().cur += n;
                    return *this;
                }
                n -= in_chunk;
                idx() += in_chunk;
                set_next_chunk();
            }
            idx() += n;

            return *this;
        }

        basic_iterator& sub(difference_type n) noexcept {
            if (n == 0) {
                return *this;
            }

            if (n < 0) {
                return add(-n);
            }

            if (!_chunk) [[unlikely]] {
                if (idx() < 0 || idx() >= this->deq().size() + n) {
                    idx() -= n;
                    return *this;
                }
                auto next_idx = this->deq().size() - 1;
                n -= idx() - next_idx;
                set_idx(next_idx);
                set_back_chunk();
            }
            while (n > 0 && chunk()) {
                auto in_chunk = chunk_iterators().cur - chunk_iterators().begin;
                if (n <= in_chunk) {
                    idx() -= n;
                    chunk_iterators().cur -= n;
                    return *this;
                }
                n -= in_chunk + 1;
                idx() -= in_chunk + 1;
                set_prev_chunk();
            }
            idx() -= n;

            return *this;
        }

        chunk_ptr_type set_front_chunk() noexcept {
            chunk_ptr_type c = this->deq()._front_chunk;
            if (set_chunk(c)) {
                set_chunk_iterators({c->begin(), c->begin(), c->end()});
            } else {
                reset_chunk_iterators();
            }
            return c;
        }

        chunk_ptr_type set_back_chunk() noexcept {
            chunk_ptr_type c = this->deq()._back_chunk;
            if (set_chunk(c)) {
                set_chunk_iterators({c->end() - 1, c->begin(), c->end()});
            } else {
                reset_chunk_iterators();
            }
            return c;
        }

        chunk_ptr_type set_next_chunk() noexcept {
            chunk_ptr_type c = _chunk->next;
            if (set_chunk(c)) {
                set_chunk_iterators({c->begin(), c->begin(), c->end()});
            } else {
                reset_chunk_iterators();
            }
            return c;
        }

        chunk_ptr_type set_prev_chunk() noexcept {
            chunk_ptr_type c = _chunk->prev;
            if (set_chunk(c)) {
                set_chunk_iterators({c->end() - 1, c->begin(), c->end()});
            } else {
                reset_chunk_iterators();
            }
            return c;
        }
    };

    template <typename ChunkType, bool IsForward>
    class basic_sentinel : public iterator_base {
        using iterator_base::iterator_base;

        friend class chunked_deque;

    protected:
        inline operator basic_iterator<ChunkType, IsForward>() const noexcept {
            return basic_iterator<ChunkType, IsForward>::make_end(this->deq());
        }

    public:
        basic_sentinel() = default;

        template <typename OtherChunkType>
        bool operator==(const basic_iterator<OtherChunkType, IsForward>& it) const noexcept {
            assert(this->_deq == it._deq);
            if constexpr (IsForward) {
                return it.idx() >= it.deq().size();
            } else {
                return it.idx() < 0;
            }
        }

        template <typename OtherChunkType>
        bool operator!=(const basic_iterator<OtherChunkType, IsForward>& it) noexcept {
            return !operator==(it);
        }

        difference_type operator-(const basic_iterator<ChunkType, IsForward>& rhs) const noexcept {
            if constexpr (IsForward) {
                return this->deq().size() - rhs.idx();
            } else {
                return rhs.idx() - this->deq().size();
            }
        }
    };

    using sentinel = basic_sentinel<chunk, true>;
    using const_sentinel = basic_sentinel<const chunk, true>;
    using reverse_sentinel = basic_sentinel<chunk, false>;
    using const_reverse_sentinel = basic_sentinel<const chunk, false>;

public:
    using iterator = basic_iterator<chunk, true>;
    using const_iterator = basic_iterator<const chunk, true>;
    using reverse_iterator = basic_iterator<chunk, false>;
    using const_reverse_iterator = basic_iterator<const chunk, false>;

    static_assert(std::random_access_iterator<iterator>);
    static_assert(std::sentinel_for<sentinel, iterator>);

    inline iterator begin() noexcept { return iterator::make_begin(*this); }
    inline iterator end() noexcept { return sentinel(*this); }
    inline const_iterator begin() const noexcept { return const_iterator::make_begin(*this); }
    inline const_iterator end() const noexcept { return const_sentinel(*this); }
    inline const_iterator cbegin() const noexcept { return const_iterator::make_begin(*this); }
    inline const_iterator cend() const noexcept { return const_sentinel(*this); }

    inline reverse_iterator rbegin() noexcept { return reverse_iterator::make_begin(*this); }
    inline reverse_iterator rend() noexcept { return reverse_sentinel(*this); }
    inline const_reverse_iterator rbegin() const noexcept { return const_reverse_iterator::make_begin(*this); }
    inline const_reverse_iterator rend() const noexcept { return const_reverse_sentinel(*this); }
    inline const_reverse_iterator crbegin() const noexcept { return const_reverse_iterator::make_begin(*this); }
    inline const_reverse_iterator crend() const noexcept { return const_reverse_sentinel(*this); }

    reference operator[](size_type n) noexcept {
        auto it = begin();
        return it[n];
    }

    const_reference operator[](size_type n) const noexcept {
        auto it = cbegin();
        return it[n];
    }

    reference at(size_type n) {
        range_check(n);
        return this->operator[](n);
    }

    const_reference at(size_type n) const {
        range_check(n);
        return this->operator[](n);
    }

private:
    chunk* new_chunk();
    inline void ensure_room_front();
    inline void ensure_room_back();
    void undo_room_front() noexcept;
    void undo_room_back() noexcept;
    void delete_front_chunk() noexcept;
    void delete_back_chunk() noexcept;
    void reserve_chunks(size_t nchunks);

    void range_check(size_type n) {
        if (n >= size()) {
            throw std::out_of_range(fmt::format("chunked_deque range_check fails: n (which is {}) >= size (which is {})", n, size()));
        }
    }
};

template <typename T, size_t items_per_chunk, int save_free_chunks>
typename chunked_deque<T, items_per_chunk, save_free_chunks>::chunk*
chunked_deque<T, items_per_chunk, save_free_chunks>::new_chunk() {
    chunk* ret;
    if (_free_chunks) {
        ret = _free_chunks;
        _free_chunks = _free_chunks->next;
        --_nfree_chunks;
    } else {
        ret = new chunk;
    }
    _nchunks++;
    return ret;
}

template <typename T, size_t items_per_chunk, int save_free_chunks>
inline void
chunked_deque<T, items_per_chunk, save_free_chunks>::ensure_room_front() {
    // If we don't have a back chunk or it's full, we need to create a new one
    if (_front_chunk && !_front_chunk->full()) {
        return;
    }
    auto* n = new_chunk();
    n->prev = nullptr;
    if (_front_chunk) {
        _front_chunk->prev = n;
    } else {
        _back_chunk = n;
    }
    n->next = _front_chunk;
    _front_chunk = n;
}

template <typename T, size_t items_per_chunk, int save_free_chunks>
inline void
chunked_deque<T, items_per_chunk, save_free_chunks>::ensure_room_back() {
    // If we don't have a back chunk or it's full, we need to create a new one
    if (_back_chunk && !_back_chunk->full()) {
        return;
    }
    auto* n = new_chunk();
    n->next = nullptr;
    if (_back_chunk) {
        _back_chunk->next = n;
    } else {
        _front_chunk = n;
    }
    n->prev = _back_chunk;
    _back_chunk = n;
}

template <typename T, size_t items_per_chunk, int save_free_chunks>
void
chunked_deque<T, items_per_chunk, save_free_chunks>::undo_room_front() noexcept {
    if (!_front_chunk->empty()) {
        return;
    }
    auto* old = _front_chunk;
    _front_chunk = old->next;
    if (_front_chunk) {
        _front_chunk->prev = nullptr;
    } else {
        _back_chunk = nullptr;
    }
    --_nchunks;
    delete old;
}

template <typename T, size_t items_per_chunk, int save_free_chunks>
void
chunked_deque<T, items_per_chunk, save_free_chunks>::undo_room_back() noexcept {
    if (!_back_chunk->empty()) {
        return;
    }
    auto* old = _back_chunk;
    _back_chunk = old->prev;
    if (_back_chunk) {
        _back_chunk->next = nullptr;
    } else {
        _front_chunk = nullptr;
    }
    --_nchunks;
    delete old;
}

// Certain use cases may need to repeatedly allocate and free a chunk -
// an obvious example is an empty queue to which we push, and then pop,
// repeatedly. Another example is pushing and popping to a non-empty deque
// we push and pop at different chunks so we need to free and allocate a
// chunk every items_per_chunk operations.
// The solution is to keep a list of freed chunks instead of freeing them
// immediately. There is a performance/memory tradeoff of how many freed
// chunks to save: If we save them all, the queue can never shrink from
// its maximum memory use (this is how circular_buffer behaves).
// The ad-hoc choice made here is to limit the number of saved chunks to 1,
// but this could easily be made a configuration option.
template <typename T, size_t items_per_chunk, int save_free_chunks>
void
chunked_deque<T, items_per_chunk, save_free_chunks>::delete_front_chunk() noexcept {
    auto *next = _front_chunk->next;
    if (_nfree_chunks < save_free_chunks) {
        _front_chunk->next = _free_chunks;
        _free_chunks = _front_chunk;
        ++_nfree_chunks;
    } else {
        delete _front_chunk;
    }
    if ((_front_chunk = next)) {
        next->prev = nullptr;
    } else {
        _back_chunk = nullptr;
    }
    --_nchunks;
}

template <typename T, size_t items_per_chunk, int save_free_chunks>
void
chunked_deque<T, items_per_chunk, save_free_chunks>::delete_back_chunk() noexcept {
    auto *prev = _back_chunk->prev;
    if (_nfree_chunks < save_free_chunks) {
        _back_chunk->next = _free_chunks;
        _free_chunks = _back_chunk;
        ++_nfree_chunks;
    } else {
        delete _back_chunk;
    }
    if ((_back_chunk = prev)) {
        prev->next = nullptr;
    } else {
        _front_chunk = nullptr;
    }
    --_nchunks;
}

template <typename T, size_t items_per_chunk, int save_free_chunks>
void chunked_deque<T, items_per_chunk, save_free_chunks>::reserve_chunks(size_t needed_chunks) {
    // If we already have some freed chunks saved, we need to allocate fewer
    // additional chunks, or none at all
    if (needed_chunks <= _nfree_chunks) {
        return;
    }
    needed_chunks -= _nfree_chunks;
    while (needed_chunks--) {
        auto *c = new chunk;
        c->next = _free_chunks;
        _free_chunks = c;
        ++_nfree_chunks;
    }
}

} // namespace utils
