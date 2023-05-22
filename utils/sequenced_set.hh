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
#include <iostream>

namespace utils {
/**
 * This class implements an add-only vector that ensures that the elements are
 * unique.
 *
 * This class provides a similar functionality to the Java's LinkedHashSet
 * class.
 */
template<typename T, typename VectorType>
class basic_sequenced_set {
public:
    using value_type = T;
    using size_type = size_t;
    using iterator = typename VectorType::iterator;
    using const_iterator = typename VectorType::const_iterator;

    basic_sequenced_set() = default;

    basic_sequenced_set(std::initializer_list<T> init)
        : _set(init)
        , _vec(init)
    { }

    explicit basic_sequenced_set(VectorType v)
        : _set(v.begin(), v.end())
        , _vec(std::move(v))
    { }

    template <typename InputIt>
    explicit basic_sequenced_set(InputIt first, InputIt last)
        : _set(first, last)
        , _vec(first, last)
    { }

    const T& operator[](size_t i) const noexcept {
        return _vec[i];
    }

    T& operator[](size_t i) noexcept {
        return _vec[i];
    }

    bool empty() const noexcept {
        return _vec.empty();
    }

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

    auto& front() const noexcept {
        return _vec.front();
    }

    auto& front() noexcept {
        return _vec.front();
    }

    auto& back() const noexcept {
        return _vec.back();
    }

    auto& back() noexcept {
        return _vec.back();
    }

    const auto& get_vector() const noexcept {
        return _vec;
    }

    const auto& get_set() const noexcept {
        return _set;
    }

    auto extract_vector() && noexcept {
        return std::move(_vec);
    }

    auto extract_set() && noexcept {
        return std::move(_set);
    }

    bool contains(const T& t) const noexcept {
        return _set.contains(t);
    }

    void reserve(size_type sz) {
        _set.reserve(sz);
        _vec.reserve(sz);
    }

    iterator erase(const_iterator pos) {
        auto val = *pos;
        auto it = _vec.erase(pos);
        _set.erase(val);
        return it;
    }

    // The implementation is not exception safe
    // so mark the method noexcept to terminate in case anything throws
    iterator erase(const_iterator first, const_iterator last) noexcept {
        for (auto it = first; it != last; ++it) {
            _set.erase(*it);
        }
        return _vec.erase(first, last);
    }

private:
    std::unordered_set<T> _set;
    VectorType _vec;
};

template <typename T>
using sequenced_set = basic_sequenced_set<T, std::vector<T>>;

} // namespace utils

namespace std {

template <typename T, typename VectorType>
ostream& operator<<(ostream& os, const utils::basic_sequenced_set<T, VectorType>& s) {
    return os << s.get_vector();
}

} // namespace std
