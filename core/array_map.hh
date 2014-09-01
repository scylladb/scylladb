/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef ARRAY_MAP_HH_
#define ARRAY_MAP_HH_

#include <array>

// unordered_map implemented as a simple array

template <typename Value, size_t Max>
class array_map {
    std::array<Value, Max> _a;
public:
    array_map(std::initializer_list<std::pair<size_t, Value>> i) {
        for (auto kv : i) {
            _a[kv.first] = kv.second;
        }
    }
    Value& operator[](size_t key) { return _a[key]; }
    const Value& operator[](size_t key) const { return _a[key]; }
};



#endif /* ARRAY_MAP_HH_ */
