/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef BYTEORDER_HH_
#define BYTEORDER_HH_

#include <arpa/inet.h>  // for ntohs() and friends

inline uint64_t ntohq(uint64_t v) {
    return __builtin_bswap64(v);
}
inline uint64_t htonq(uint64_t v) {
    return __builtin_bswap64(v);
}

namespace net {

inline void ntoh() {}
inline void hton() {}

inline uint16_t ntoh(uint16_t x) { return ntohs(x); }
inline uint16_t hton(uint16_t x) { return htons(x); }
inline uint32_t ntoh(uint32_t x) { return ntohl(x); }
inline uint32_t hton(uint32_t x) { return htonl(x); }
inline uint64_t ntoh(uint64_t x) { return ntohq(x); }
inline uint64_t hton(uint64_t x) { return htonq(x); }

// Wrapper around a primitive type to provide an unaligned version.
// This is because gcc (correctly) doesn't allow binding an unaligned
// scalar variable to a reference, and (unfortunately) doesn't allow
// specifying unaligned references.
//
// So, packed<uint32_t>& is our way of passing a reference (or pointer)
// to a uint32_t around, without losing the knowledge about its alignment
// or lack thereof.
template <typename T>
struct packed {
    T raw;
    packed() = default;
    packed(T x) : raw(x) {}
    packed& operator=(const T& x) { raw = x; return *this; }
    operator T() const { return raw; }

    template <typename Adjuster>
    void adjust_endianness(Adjuster a) { a(raw); }
} __attribute__((packed));

template <typename T>
inline T ntoh(const packed<T>& x) {
    T v = x;
    return ntoh(v);
}

template <typename T>
inline T hton(const packed<T>& x) {
    T v = x;
    return hton(v);
}

template <typename T>
inline std::ostream& operator<<(std::ostream& os, const packed<T>& v) {
    auto x = v.raw;
    return os << x;
}

inline
void ntoh_inplace() {}
inline
void hton_inplace() {};

template <typename First, typename... Rest>
inline
void ntoh_inplace(First& first, Rest&... rest) {
    first = ntoh(first);
    ntoh_inplace(std::forward<Rest&>(rest)...);
}

template <typename First, typename... Rest>
inline
void hton_inplace(First& first, Rest&... rest) {
    first = hton(first);
    hton_inplace(std::forward<Rest&>(rest)...);
}

template <class T>
inline
T ntoh(const T& x) {
    T tmp = x;
    tmp.adjust_endianness([] (auto&&... what) { ntoh_inplace(std::forward<decltype(what)&>(what)...); });
    return tmp;
}

template <class T>
inline
T hton(const T& x) {
    T tmp = x;
    tmp.adjust_endianness([] (auto&&... what) { hton_inplace(std::forward<decltype(what)&>(what)...); });
    return tmp;
}

}

#endif /* BYTEORDER_HH_ */
