/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

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
        if (_set.find(val) != _set.end()) {
            return;
        }

        _set.insert(val);
        _vec.push_back(val);
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

    void reserve(size_t sz) {
        _set.reserve(sz);
        _vec.reserve(sz);
    }

private:
    std::unordered_set<T> _set;
    std::vector<T> _vec;
};
} // namespace utils

