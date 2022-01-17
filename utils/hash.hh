/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#ifndef UTILS_HASH_HH_
#define UTILS_HASH_HH_

#include <functional>

namespace utils {

// public for unit testing etc
inline size_t hash_combine(size_t left, size_t right) {
    return left + 0x9e3779b9 + (right << 6) + (right >> 2);
}

struct tuple_hash {
private:
    // CMH. Add specializations here to handle recursive tuples
    template<typename T>
    static size_t hash(const T& t) {
        return std::hash<T>()(t);
    }
    template<size_t index, typename...Types>
    struct hash_impl {
        size_t operator()(const std::tuple<Types...>& t, size_t a) const {
            return hash_impl<index-1, Types...>()(t, hash_combine(hash(std::get<index>(t)), a));
        }
        size_t operator()(const std::tuple<Types...>& t) const {
            return hash_impl<index-1, Types...>()(t, hash(std::get<index>(t)));
        }
    };
    template<class...Types>
    struct hash_impl<0, Types...> {
        size_t operator()(const std::tuple<Types...>& t, size_t a) const {
            return hash_combine(hash(std::get<0>(t)), a);
        }
        size_t operator()(const std::tuple<Types...>& t) const {
            return hash(std::get<0>(t));
        }
    };
public:
    // All the operator() implementations are templates, so this is transparent.
    using is_transparent = void;

    template<typename T1, typename T2>
    size_t operator()(const std::pair<T1, T2>& p) const {
        return hash_combine(hash(p.first), hash(p.second));
    }
    template<typename T1, typename T2>
    size_t operator()(const T1& t1, const T2& t2) const {
        return hash_combine(hash(t1), hash(t2));
    }
    template<typename... Args>
    size_t operator()(const std::tuple<Args...>& v) const;
};

template<typename... Args>
inline size_t tuple_hash::operator()(const std::tuple<Args...>& v) const {
    return hash_impl<std::tuple_size<std::tuple<Args...>>::value - 1, Args...>()(v);
}
template<>
inline size_t tuple_hash::operator()(const std::tuple<>& v) const {
    return 0;
}

}

#endif /* UTILS_HASH_HH_ */
