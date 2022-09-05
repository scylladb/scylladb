/*
 * Copyright (C) 2014-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <vector>
#include <unordered_set>
#include <cstddef>

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

