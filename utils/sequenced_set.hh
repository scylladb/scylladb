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
class sequenced_set {
public:
    using value_type = T;
    using size_type = size_t;
    using iterator = typename std::vector<T>::iterator;
    using const_iterator = typename std::vector<T>::const_iterator;

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

    size_type size() const noexcept {
        return _vec.size();
    }

    iterator begin() noexcept {
        return _vec.begin();
    }

    iterator end() noexcept {
        return _vec.end();
    }

    const_iterator begin() const noexcept {
        return _vec.begin();
    }

    const_iterator end() const noexcept {
        return _vec.end();
    }

    const_iterator cbegin() const noexcept {
        return _vec.cbegin();
    }

    const_iterator cend() const noexcept {
        return _vec.cend();
    }

    const auto& get_vector() const noexcept {
        return _vec;
    }

    void reserve(size_type sz) {
        _set.reserve(sz);
        _vec.reserve(sz);
    }

private:
    std::unordered_set<T> _set;
    std::vector<T> _vec;
};
} // namespace utils

