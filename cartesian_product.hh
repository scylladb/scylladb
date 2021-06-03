/*
 * Copyright (C) 2015-present ScyllaDB
 *
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

#include <vector>
#include <sys/types.h>

// Single-pass range over cartesian product of vectors.

// Note:
//    {a, b, c} x {1, 2} = {{a, 1}, {a, 2}, {b, 1}, {b, 2}, {c, 1}, {c, 2}}
template<typename T>
struct cartesian_product {
    const std::vector<std::vector<T>>& _vec_of_vecs;
public:
    class iterator {
    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = std::vector<T>;
        using difference_type = std::ptrdiff_t;
        using pointer = std::vector<T>*;
        using reference = std::vector<T>&;
    private:
        size_t _pos;
        const std::vector<std::vector<T>>* _vec_of_vecs;
        value_type _current;
        std::vector<typename std::vector<T>::const_iterator> _iterators;
    public:
        struct end_tag {};
        iterator(end_tag) : _pos(-1) {}
        iterator(const std::vector<std::vector<T>>& vec_of_vecs) : _pos(0), _vec_of_vecs(&vec_of_vecs) {
            _iterators.reserve(vec_of_vecs.size());
            for (auto&& vec : vec_of_vecs) {
                _iterators.push_back(vec.begin());
                if (vec.empty()) {
                    _pos = -1;
                    break;
                }
            }
        }
        value_type& operator*() {
            _current.clear();
            _current.reserve(_vec_of_vecs->size());
            for (auto& i : _iterators) {
                _current.emplace_back(*i);
            }
            return _current;
        }
        void operator++() {
            ++_pos;

            for (ssize_t i = _iterators.size() - 1; i >= 0; --i) {
                ++_iterators[i];
                if (_iterators[i] != (*_vec_of_vecs)[i].end()) {
                    return;
                }
                _iterators[i] = (*_vec_of_vecs)[i].begin();
            }

            // If we're here it means we've covered every combination
            _pos = -1;
        }
        bool operator==(const iterator& o) const { return _pos == o._pos; }
        bool operator!=(const iterator& o) const { return _pos != o._pos; }
    };
public:
    cartesian_product(const std::vector<std::vector<T>>& vec_of_vecs) : _vec_of_vecs(vec_of_vecs) {}
    iterator begin() { return iterator(_vec_of_vecs); }
    iterator end() { return iterator(typename iterator::end_tag()); }
};

template<typename T>
static inline
size_t cartesian_product_size(const std::vector<std::vector<T>>& vec_of_vecs) {
    size_t r = 1;
    for (auto&& vec : vec_of_vecs) {
        r *= vec.size();
    }
    return r;
}

template<typename T>
static inline
bool cartesian_product_is_empty(const std::vector<std::vector<T>>& vec_of_vecs) {
    for (auto&& vec : vec_of_vecs) {
        if (vec.empty()) {
            return true;
        }
    }
    return vec_of_vecs.empty();
}

template<typename T>
static inline
cartesian_product<T> make_cartesian_product(const std::vector<std::vector<T>>& vec_of_vecs) {
    return cartesian_product<T>(vec_of_vecs);
}
