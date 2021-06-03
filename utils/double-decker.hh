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

#include <type_traits>
#include <seastar/util/concepts.hh>
#include "utils/bptree.hh"
#include "utils/intrusive-array.hh"
#include "utils/collection-concepts.hh"
#include <fmt/core.h>

/*
 * The double-decker is the ordered keeper of key:value pairs having
 * the pairs sorted by both key and value (key first).
 *
 * The keys collisions are expected to be rare enough to afford holding
 * the values in a sorted array with the help of linear algorithms.
 */

template <typename Key, typename T, typename Less, typename Compare, int NodeSize,
            bplus::key_search Search = bplus::key_search::binary, bplus::with_debug Debug = bplus::with_debug::no>
requires Comparable<T, T, Compare> && std::is_nothrow_move_constructible_v<T>
class double_decker {
public:
    using inner_array = intrusive_array<T>;
    using outer_tree = bplus::tree<Key, inner_array, Less, NodeSize, Search, Debug>;
    using outer_iterator = typename outer_tree::iterator;
    using outer_const_iterator = typename outer_tree::const_iterator;

private:
    outer_tree  _tree;

public:
    template <bool Const>
    class iterator_base {
        friend class double_decker;
        using outer_iterator = std::conditional_t<Const, typename double_decker::outer_const_iterator, typename double_decker::outer_iterator>;

    protected:
        outer_iterator _bucket;
        int _idx;

    public:
        iterator_base() = default;
        iterator_base(outer_iterator bkt, int idx) noexcept : _bucket(bkt), _idx(idx) {}

        using iterator_category = std::bidirectional_iterator_tag;
        using difference_type = ssize_t;
        using value_type = std::conditional_t<Const, const T, T>;
        using pointer = value_type*;
        using reference = value_type&;

        reference operator*() const noexcept { return (*_bucket)[_idx]; }
        pointer operator->() const noexcept { return &((*_bucket)[_idx]); }

        iterator_base& operator++() noexcept {
            if ((*_bucket)[_idx++].is_tail()) {
                _bucket++;
                _idx = 0;
            }

            return *this;
        }

        iterator_base operator++(int) noexcept {
            iterator_base cur = *this;
            operator++();
            return cur;
        }

        iterator_base& operator--() noexcept {
            if (_idx-- == 0) {
                _bucket--;
                _idx = _bucket->index_of(_bucket->end()) - 1;
            }

            return *this;
        }

        iterator_base operator--(int) noexcept {
            iterator_base cur = *this;
            operator--();
            return cur;
        }

        bool operator==(const iterator_base& o) const noexcept { return _bucket == o._bucket && _idx == o._idx; }
        bool operator!=(const iterator_base& o) const noexcept { return !(*this == o); }
    };

    using const_iterator = iterator_base<true>;

    class iterator final : public iterator_base<false> {
        friend class double_decker;
        using super = iterator_base<false>;

        iterator(const const_iterator&& other) noexcept : super(std::move(other._bucket), other._idx) {}

    public:
        iterator() noexcept : super() {}
        iterator(outer_iterator bkt, int idx) noexcept : super(bkt, idx) {}

        iterator(T* ptr) noexcept {
            inner_array& arr = inner_array::from_element(ptr, super::_idx);
            super::_bucket = outer_iterator(&arr);
        }

        template <typename Func>
        requires Disposer<Func, T>
        iterator erase_and_dispose(Less less, Func&& disp) noexcept {
            disp(&**this); // * to deref this, * to call operator*, & to get addr from ref

            if (super::_bucket->is_single_element()) {
                outer_iterator bkt = super::_bucket.erase(less);
                return iterator(bkt, 0);
            }

            bool tail = (*super::_bucket)[super::_idx].is_tail();
            super::_bucket->erase(super::_idx);
            if (tail) {
                super::_bucket++;
                super::_idx = 0;
            }

            return *this;
        }

        iterator erase(Less less) noexcept { return erase_and_dispose(less, bplus::default_dispose<T>); }
    };

    /*
     * Structure that shed some more light on how the lower_bound
     * actually found the bounding elements.
     */
    struct bound_hint {
        /*
         * Set to true if the element fully matched to the key
         * according to Compare
         */
        bool match;
        /*
         * Set to true if the bucket for the given key exists
         */
        bool key_match;
        /*
         * Set to true if the given key is more than anything
         * on the bucket and iterator was switched to the next
         * one (or when the key_match is false)
         */
        bool key_tail;

        /*
         * This helper says whether the emplace will invalidate (some)
         * iterators or not. Emplacing with !key_match will go and create
         * new node in B+ which doesn't invalidate iterators. In another
         * case some existing B+ data node will be reconstructed, so the
         * iterators on those nodes will become invalid.
         */
        bool emplace_keeps_iterators() const noexcept { return !key_match; }
    };

    iterator begin() noexcept { return iterator(_tree.begin(), 0); }
    const_iterator begin() const noexcept { return const_iterator(_tree.begin(), 0); }
    const_iterator cbegin() const noexcept { return const_iterator(_tree.begin(), 0); }

    iterator end() noexcept { return iterator(_tree.end(), 0); }
    const_iterator end() const noexcept { return const_iterator(_tree.end(), 0); }
    const_iterator cend() const noexcept { return const_iterator(_tree.end(), 0); }

    explicit double_decker(Less less) noexcept : _tree(less) { }

    double_decker(const double_decker& other) = delete;
    double_decker(double_decker&& other) noexcept : _tree(std::move(other._tree)) {}

    iterator insert(Key k, T value, Compare cmp) {
        std::pair<outer_iterator, bool> oip = _tree.emplace(std::move(k), std::move(value));
        outer_iterator& bkt = oip.first;
        int idx = 0;

        if (!oip.second) {
            /*
             * Unlikely, but in this case we reconstruct the array. The value
             * must not have been moved by emplace() above.
             */
            idx = bkt->index_of(bkt->lower_bound(value, cmp));
            size_t new_size = (bkt->size() + 1) * sizeof(T);
            bkt.reconstruct(new_size, *bkt,
                    typename inner_array::grow_tag{idx}, std::move(value));
        }

        return iterator(bkt, idx);
    }

    template <typename... Args>
    iterator emplace_before(iterator i, Key k, const bound_hint& hint, Args&&... args) {
        assert(!hint.match);
        outer_iterator& bucket = i._bucket;

        if (!hint.key_match) {
            /*
             * The most expected case -- no key conflict, respectively the
             * bucket is not found, and i points to the next one. Just go
             * ahead and emplace the new bucket before the i and push the
             * 0th element into it.
             */
            outer_iterator nb = bucket.emplace_before(std::move(k), _tree.less(), std::forward<Args>(args)...);
            return iterator(nb, 0);
        }

        /*
         * Key conflict, need to expand some inner vector, but still there
         * are two cases -- whether the bounding element is on k's bucket
         * or the bound search overflew and switched to the next one.
         */

        int idx = i._idx;

        if (hint.key_tail) {
            /*
             * The latter case -- i points to the next one. Need to shift
             * back and append the new element to its tail.
             */
            bucket--;
            idx = bucket->index_of(bucket->end());
        }

        size_t new_size = (bucket->size() + 1) * sizeof(T);
        bucket.reconstruct(new_size, *bucket,
                typename inner_array::grow_tag{idx}, std::forward<Args>(args)...);
        return iterator(bucket, idx);
    }

    template <typename K = Key>
    requires Comparable<K, T, Compare>
    const_iterator find(const K& key, Compare cmp) const {
        outer_const_iterator bkt = _tree.find(key);
        int idx = 0;

        if (bkt != _tree.end()) {
            bool match = false;
            idx = bkt->index_of(bkt->lower_bound(key, cmp, match));
            if (!match) {
                bkt = _tree.end();
                idx = 0;
            }
        }

        return const_iterator(bkt, idx);
    }

    template <typename K = Key>
    requires Comparable<K, T, Compare>
    iterator find(const K& k, Compare cmp) {
        return iterator(const_cast<const double_decker*>(this)->find(k, std::move(cmp)));
    }

    template <typename K = Key>
    requires Comparable<K, T, Compare>
    const_iterator lower_bound(const K& key, Compare cmp, bound_hint& hint) const {
        outer_const_iterator bkt = _tree.lower_bound(key, hint.key_match);

        hint.key_tail = false;
        hint.match = false;

        if (bkt == _tree.end() || !hint.key_match) {
            return const_iterator(bkt, 0);
        }

        int i = bkt->index_of(bkt->lower_bound(key, cmp, hint.match));

        if (i != 0 && (*bkt)[i - 1].is_tail()) {
            /*
             * The lower_bound is after the last element -- shift
             * to the net bucket's 0'th one.
             */
            bkt++;
            i = 0;
            hint.key_tail = true;
        }

        return const_iterator(bkt, i);
    }

    template <typename K = Key>
    requires Comparable<K, T, Compare>
    iterator lower_bound(const K& key, Compare cmp, bound_hint& hint) {
        return iterator(const_cast<const double_decker*>(this)->lower_bound(key, std::move(cmp), hint));
    }

    template <typename K = Key>
    requires Comparable<K, T, Compare>
    const_iterator lower_bound(const K& key, Compare cmp) const {
        bound_hint hint;
        return lower_bound(key, cmp, hint);
    }

    template <typename K = Key>
    requires Comparable<K, T, Compare>
    iterator lower_bound(const K& key, Compare cmp) {
        return iterator(const_cast<const double_decker*>(this)->lower_bound(key, std::move(cmp)));
    }

    template <typename K = Key>
    requires Comparable<K, T, Compare>
    const_iterator upper_bound(const K& key, Compare cmp) const {
        bool key_match;
        outer_const_iterator bkt = _tree.lower_bound(key, key_match);

        if (bkt == _tree.end() || !key_match) {
            return const_iterator(bkt, 0);
        }

        int i = bkt->index_of(bkt->upper_bound(key, cmp));

        if (i != 0 && (*bkt)[i - 1].is_tail()) {
            // Beyond the end() boundary
            bkt++;
            i = 0;
        }

        return const_iterator(bkt, i);
    }

    template <typename K = Key>
    requires Comparable<K, T, Compare>
    iterator upper_bound(const K& key, Compare cmp) {
        return iterator(const_cast<const double_decker*>(this)->upper_bound(key, std::move(cmp)));
    }

    template <typename Func>
    requires Disposer<Func, T>
    void clear_and_dispose(Func&& disp) noexcept {
        _tree.clear_and_dispose([&disp] (inner_array* arr) noexcept {
            arr->for_each(disp);
        });
    }

    void clear() noexcept { clear_and_dispose(bplus::default_dispose<T>); }

    template <typename Func>
    requires Disposer<Func, T>
    iterator erase_and_dispose(iterator begin, iterator end, Func&& disp) noexcept {
        bool same_bucket = begin._bucket == end._bucket;

        // Drop the tail of the starting bucket if it's not fully erased
        while (begin._idx != 0) {
            if (same_bucket) {
                if (begin == end) {
                    return begin;
                }
                end._idx--;
            }

            begin = begin.erase_and_dispose(_tree.less(), disp);
        }

        // Drop all the buckets in between
        outer_iterator nb = _tree.erase_and_dispose(begin._bucket, end._bucket, [&disp] (inner_array* arr) noexcept {
            arr->for_each(disp);
        });

        assert(nb == end._bucket);

        /*
         * Drop the head of the ending bucket. Every erased element is the 0th
         * one, when erased it will shift the rest left and reconstruct the array,
         * thus we cannot rely on the end to keep neither _bucket not _idx.
         *
         * Said that -- just erase the required number of elements. A corner case
         * when end points to the tree end is handled, _idx is 0 in this case.
         */
        iterator next(nb, 0);
        while (end._idx-- != 0) {
            next = next.erase_and_dispose(_tree.less(), disp);
        }

        return next;
    }

    iterator erase(iterator begin, iterator end) noexcept {
        return erase_and_dispose(begin, end, bplus::default_dispose<T>);
    }

    bool empty() const noexcept { return _tree.empty(); }

    static size_t estimated_object_memory_size_in_allocator(allocation_strategy& allocator, const T* obj) noexcept {
        /*
         * The T-s are merged together in array, so getting any run-time
         * value of a pointer would be wrong. So here's some guessing of
         * how much memory would this thing occupy in memory
         */
        return sizeof(typename outer_tree::data);
    }
};
