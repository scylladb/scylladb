/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

/*
 * This header file defines a hash function for all enum types, using the
 * standard hash function of the underlying type (such as int). This makes
 * it possible to use an enum type as a key for std::unordered_map, for
 * example.
 */

#include <type_traits>
#include <functional>
#include <cstddef>

namespace std {
    template<typename T>
    class hash {
        using sfinae = typename std::enable_if<std::is_enum<T>::value, T>::type;
    public:
        std::size_t operator()(const T& e) const {
            using utype = typename std::underlying_type<T>::type;
            return std::hash<utype>()(static_cast<utype>(e));
        }
    };
}
