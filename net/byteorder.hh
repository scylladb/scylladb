/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef BYTEORDER_HH_
#define BYTEORDER_HH_

#include <arpa/inet.h>  // for ntohs() and friends

inline uint64_t ntohq(uint64_t v) {
    return __builtin_bswap64(v);
}

namespace net {

inline void ntoh() {}
inline void hton() {}

inline void ntoh(uint16_t& x) { x = ntohs(x); }
inline void hton(uint16_t& x) { x = htons(x); }
inline void ntoh(uint32_t& x) { x = ntohl(x); }
inline void hton(uint32_t& x) { x = htonl(x); }

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
inline void ntoh(packed<T>& x) {
    T v = x;
    ntoh(v);
    x = v;
}

template <typename T>
inline void hton(packed<T>& x) {
    T v = x;
    hton(v);
    x = v;
}

template <typename First, typename... Rest>
inline
void ntoh(First& first, Rest&... rest) {
    ntoh(first);
    ntoh(std::forward<Rest&>(rest)...);
}

template <typename First, typename... Rest>
inline
void hton(First& first, Rest&... rest) {
    hton(first);
    hton(std::forward<Rest&>(rest)...);
}

template <class T>
inline
void ntoh(T& x) {
    x.adjust_endianness([] (auto&... what) { ntoh(std::forward<decltype(what)&>(what)...); });
}

template <class T>
inline
void hton(T& x) {
    x.adjust_endianness([] (auto&... what) { hton(std::forward<decltype(what)&>(what)...); });
}

}

#endif /* BYTEORDER_HH_ */
