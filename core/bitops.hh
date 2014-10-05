/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef BITOPS_HH_
#define BITOPS_HH_

inline
constexpr unsigned count_leading_zeros(unsigned x) {
    return __builtin_clz(x);
}

inline
constexpr unsigned count_leading_zeros(unsigned long x) {
    return __builtin_clzl(x);
}

inline
constexpr unsigned count_leading_zeros(unsigned long long x) {
    return __builtin_clzll(x);
}

inline
constexpr unsigned count_trailing_zeros(unsigned x) {
    return __builtin_ctz(x);
}

inline
constexpr unsigned count_trailing_zeros(unsigned long x) {
    return __builtin_ctzl(x);
}

inline
constexpr unsigned count_trailing_zeros(unsigned long long x) {
    return __builtin_ctzll(x);
}

#endif /* BITOPS_HH_ */
