/*
 * Copyright (C) 2014-present ScyllaDB
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
#include <unordered_set>

namespace utils {
/**
 * This class implements an add-only vector that ensures that the elements are
 * unique.
 *
 * This class provides a similar functionality to the Java's LinkedHashSet
 * class.
 */
template<typename T>
struct sequenced_set {
    typedef typename std::vector<T>::iterator iterator;

    void push_back(const T& val) {
        insert(val);
    }

    std::pair<iterator, bool> insert(const T& t) {
        auto r = _set.insert(t);
        if (r.second) {
            try {
                _vec.push_back(t);
                return std::make_pair(std::prev(_vec.end()), true);
            } catch (...) {
                _set.erase(r.first);
                throw;
            }
        }
        return std::make_pair(_vec.end(), false);
    }

    size_t size() {
        return _vec.size();
    }

    iterator begin() {
        return _vec.begin();
    }

    iterator end() {
        return _vec.end();
    }

    const std::vector<T>& get_vector() const {
        return _vec;
    }

    std::vector<T>& get_vector() {
        return _vec;
    }

    void reserve(size_t sz) {
        _set.reserve(sz);
        _vec.reserve(sz);
    }

private:
    std::unordered_set<T> _set;
    std::vector<T> _vec;
};
} // namespace utils

