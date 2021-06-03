/*
 * Copyright (C) 2020-present ScyllaDB
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

#include <array>
#include <cassert>
#include <seastar/util/concepts.hh>

#include "utils/allocation_strategy.hh"
#include "utils/collection-concepts.hh"

template <typename T>
concept BoundsKeeper = requires (T val, bool bit) {
    { val.is_head() } noexcept -> std::same_as<bool>;
    { val.set_head(bit) } noexcept -> std::same_as<void>;
    { val.is_tail() } noexcept -> std::same_as<bool>;
    { val.set_tail(bit) } noexcept -> std::same_as<void>;
    { val.with_train() } noexcept -> std::same_as<bool>;
    { val.set_train(bit) } noexcept -> std::same_as<void>;
};

/*
 * A plain array of T-s that grows and shrinks by constructing a new
 * instances. Holds at least one element. Has facilities for sorting
 * the elements and for doing "container_of" by the given element
 * pointer. LSA-compactible.
 *
 * Important feature of the array is zero memory overhead -- it doesn't
 * keep its size/capacity onboard. The size is calculated each time by
 * walking the array of T-s and checking which one of them is the tail
 * element. Respectively, the T must keep head/tail flags on itself.
 */
template <typename T>
requires BoundsKeeper<T> && std::is_nothrow_move_constructible_v<T>
class intrusive_array {
    // Sanity constant to avoid infinite loops searching for tail
    static constexpr int max_len = std::numeric_limits<short int>::max();

    union maybe_constructed {
        maybe_constructed() { }
        ~maybe_constructed() { }
        T object;

        /*
         * Train is 1 or more allocated but unoccupied memory slots after 
         * the tail one. Being unused, this memory keeps the train length.
         * An array with the train is marked with the respective flag on 
         * the 0th element. Train is created by the erase() call and can 
         * be up to 65535 elements long
         *
         * Train length is included into the storage_size() to make 
         * allocator and compaction work correctly, but is not included 
         * into the number_of_elements(), so the array behaves just like
         * there's no train
         *
         * Respectively both grow and shrink constructors do not carry 
         * the train (and drop the bit from 0th element) and don't expect 
         * the memory for the new array to include one
         */
        unsigned short train_len;
        static_assert(sizeof(T) >= sizeof(unsigned short));
    };

    maybe_constructed   _data[1];

    size_t number_of_elements() const noexcept {
        for (int i = 0; i < max_len; i++) {
            if (_data[i].object.is_tail()) {
                return i + 1;
            }
        }

        std::abort();
    }

public:
    size_t storage_size() const noexcept {
        size_t nr = number_of_elements();
        if (_data[0].object.with_train()) {
            nr += _data[nr].train_len;
        }
        return nr * sizeof(T);
    }

    using iterator = T*;
    using const_iterator = const T*;

    /*
     * There are 3 constructing options for the array: initial, grow
     * and shrink.
     *
     * * initial just creates a 1-element array
     * * grow -- makes a new one moving all elements from the original
     * array and inserting the one (only one) more element at the given
     * position
     * * shrink -- also makes a new array skipping the not needed
     * element while moving them from the original one
     *
     * In all cases the enough big memory chunk must be provided by the
     * caller!
     *
     * Note, that none of them calls destructors on T-s, unlike vector.
     * This is because when the older array is destroyed it has no idea
     * about whether or not it was grown/shrunk and thus it destroys
     * T-s itself.
     */

    // Initial
    template <typename... Args>
    intrusive_array(Args&&... args) {
        new (&_data[0].object) T(std::forward<Args>(args)...);
        _data[0].object.set_head(true);
        _data[0].object.set_tail(true);
    }

    // Growing
    struct grow_tag {
        int add_pos;
    };

    template <typename... Args>
    intrusive_array(intrusive_array& from, grow_tag grow, Args&&... args) {
        // The add_pos is strongly _expected_ to be within bounds
        int i, off = 0;
        bool tail = false;

        for (i = 0; !tail; i++) {
            if (i == grow.add_pos) {
                off = 1;
                continue;
            }

            tail = from._data[i - off].object.is_tail();
            new (&_data[i].object) T(std::move(from._data[i - off].object));
        }

        assert(grow.add_pos <= i && i < max_len);

        new (&_data[grow.add_pos].object) T(std::forward<Args>(args)...);

        _data[0].object.set_head(true);
        _data[0].object.set_train(false);
        if (grow.add_pos == 0) {
            _data[1].object.set_head(false);
        }
        _data[i - off].object.set_tail(true);
        if (off == 0) {
            _data[i - 1].object.set_tail(false);
        }
    }

    // Shrinking
    struct shrink_tag {
        int del_pos;
    };

    intrusive_array(intrusive_array& from, shrink_tag shrink) {
        int i, off = 0;
        bool tail = false;

        for (i = 0; !tail; i++) {
            tail = from._data[i].object.is_tail();
            if (i == shrink.del_pos) {
                off = 1;
            } else {
                new (&_data[i - off].object) T(std::move(from._data[i].object));
            }
        }

        _data[0].object.set_head(true);
        _data[0].object.set_train(false);
        _data[i - off - 1].object.set_tail(true);
    }

    intrusive_array(const intrusive_array& other) = delete;
    intrusive_array(intrusive_array&& other) noexcept {
        bool tail = false;
        int i;

        for (i = 0; !tail; i++) {
            tail = other._data[i].object.is_tail();

            new (&_data[i].object) T(std::move(other._data[i].object));
        }

        if (_data[0].object.with_train()) {
            _data[i].train_len = other._data[i].train_len;
        }
    }

    ~intrusive_array() {
        bool tail = false;

        for (int i = 0; !tail; i++) {
            tail = _data[i].object.is_tail();
            _data[i].object.~T();
        }
    }

    /*
     * Drops the element in-place at position @pos and grows the
     * "train". To be used in places where reconstruction is not
     * welcome (e.g. because it throws)
     *
     * Single-elemented array cannot be erased from, just drop it
     * alltogether if needed
     */
    void erase(int pos) noexcept {
        assert(!is_single_element());
        assert(pos < max_len);

        bool with_train = _data[0].object.with_train();
        bool tail = _data[pos].object.is_tail();
        _data[pos].object.~T();

        if (tail) {
            assert(pos > 0);
            _data[pos - 1].object.set_tail(true);
        } else {
            while (!tail) {
                new (&_data[pos].object) T(std::move(_data[pos + 1].object));
                _data[pos + 1].object.~T();
                tail = _data[pos++].object.is_tail();
            }
            _data[0].object.set_head(true);
        }

        _data[0].object.set_train(true);
        unsigned short train_len = with_train ? _data[pos + 1].train_len : 0;
        assert(train_len < max_len);
        _data[pos].train_len = train_len + 1;
    }

    T& operator[](int pos) noexcept { return _data[pos].object; }
    const T& operator[](int pos) const noexcept { return _data[pos].object; }

    iterator begin() noexcept { return &_data[0].object; }
    const_iterator begin() const noexcept { return &_data[0].object; }
    const_iterator cbegin() const noexcept { return &_data[0].object; }
    iterator end() noexcept { return &_data[number_of_elements()].object; }
    const_iterator end() const noexcept { return &_data[number_of_elements()].object; }
    const_iterator cend() const noexcept { return &_data[number_of_elements()].object; }

    size_t index_of(iterator i) const noexcept { return i - &_data[0].object; }
    size_t index_of(const_iterator i) const noexcept { return i - &_data[0].object; }
    bool is_single_element() const noexcept { return _data[0].object.is_tail(); }

    // A helper for keeping the array sorted
    template <typename K, typename Compare>
    requires Comparable<K, T, Compare>
    const_iterator lower_bound(const K& val, Compare cmp, bool& match) const {
        int i = 0;

        do {
            auto x = cmp(_data[i].object, val);
            if (x >= 0) {
                match = (x == 0);
                break;
            }
        } while (!_data[i++].object.is_tail());

        return &_data[i].object;
    }

    template <typename K, typename Compare>
    requires Comparable<K, T, Compare>
    iterator lower_bound(const K& val, Compare cmp, bool& match) {
        return const_cast<iterator>(const_cast<const intrusive_array*>(this)->lower_bound(val, std::move(cmp), match));
    }

    template <typename K, typename Compare>
    requires Comparable<K, T, Compare>
    const_iterator lower_bound(const K& val, Compare cmp) const {
        bool match = false;
        return lower_bound(val, cmp, match);
    }

    template <typename K, typename Compare>
    requires Comparable<K, T, Compare>
    iterator lower_bound(const K& val, Compare cmp) {
        return const_cast<iterator>(const_cast<const intrusive_array*>(this)->lower_bound(val, std::move(cmp)));
    }

    // And its peer ... just to be used
    template <typename K, typename Compare>
    requires Comparable<K, T, Compare>
    const_iterator upper_bound(const K& val, Compare cmp) const {
        int i = 0;

        do {
            if (cmp(_data[i].object, val) > 0) {
                break;
            }
        } while (!_data[i++].object.is_tail());

        return &_data[i].object;
    }

    template <typename K, typename Compare>
    requires Comparable<K, T, Compare>
    iterator upper_bound(const K& val, Compare cmp) {
        return const_cast<iterator>(const_cast<const intrusive_array*>(this)->upper_bound(val, std::move(cmp)));
    }

    template <typename Func>
    requires Disposer<Func, T>
    void for_each(Func&& fn) noexcept {
        bool tail = false;

        for (int i = 0; !tail; i++) {
            tail = _data[i].object.is_tail();
            fn(&_data[i].object);
        }
    }

    size_t size() const noexcept { return number_of_elements(); }

    static intrusive_array& from_element(T* ptr, int& idx) noexcept {
        idx = 0;
        while (!ptr->is_head()) {
            assert(idx < max_len); // may the force be with us...
            idx++;
            ptr--;
        }

        static_assert(offsetof(intrusive_array, _data[0].object) == 0);
        return *reinterpret_cast<intrusive_array*>(ptr);
    }
};
