/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 *
 */

#ifndef IP_HH_
#define IP_HH_

#include <arpa/inet.h>
#include <cstdint>
#include <array>

namespace net {

inline uint64_t ntohq(uint64_t v) {
    return __builtin_bswap64(v);
}

uint16_t ip_checksum(void* data, size_t len);

inline void ntoh() {}
inline void hton() {}

inline void ntoh(uint16_t& x) { x = ntohs(x); }
inline void hton(uint16_t& x) { x = htons(x); }
inline void ntoh(uint32_t& x) { x = ntohl(x); }
inline void hton(uint32_t& x) { x = htonl(x); }

// gcc (correctly) won't bind a packed uint16_t to a reference,
// so bypass it by sending a pointer.
// FIXME: this is broken on machines that don't support unaligned
// loads with the normal instructions.
inline void ntoh(uint16_t* x) { *x = ntohs(*x); }
inline void hton(uint16_t* x) { *x = htons(*x); }
inline void ntoh(uint32_t* x) { *x = ntohl(*x); }
inline void hton(uint32_t* x) { *x = htonl(*x); }

template <typename First, typename... Rest>
inline
void ntoh(First&& first, Rest&&... rest) {
    ntoh(*(std::remove_reference_t<First>*)&first);
    ntoh(std::forward<Rest>(rest)...);
}

template <typename First, typename... Rest>
inline
void hton(First&& first, Rest... rest) {
    hton(*(std::remove_reference_t<First>*)&first);
    hton(std::forward<Rest>(rest)...);
}

template <class T>
inline
void ntoh(T& x) {
    x.adjust_endianness([] (auto&&... what) { ntoh(std::forward<decltype(what)>(what)...); });
}

template <class T>
inline
void hton(T& x) {
    x.adjust_endianness([] (auto&&... what) { hton(std::forward<decltype(what)>(what)...); });
}

struct eth_hdr {
    std::array<uint8_t, 6> dst_mac;
    std::array<uint8_t, 6>  src_mac;
    uint16_t eth_proto;
    template <typename Adjuster>
    auto adjust_endianness(Adjuster a) {
        return a(&eth_proto);
    }
} __attribute__((packed));

struct ip_hdr {
    uint8_t ihl : 4;
    uint8_t ver : 4;
    uint8_t dscp : 6;
    uint8_t ecn : 2;
    uint16_t len;
    uint16_t id;
    uint16_t frag;
    uint8_t ttl;
    uint8_t ip_proto;
    uint16_t csum;
    uint32_t src_ip;
    uint32_t dst_ip;
    uint8_t options[0];
    template <typename Adjuster>
    auto adjust_endianness(Adjuster a) {
        return a(&len, &id, &frag, &csum, &src_ip, &dst_ip);
    }
} __attribute__((packed));

struct icmp_hdr {
    uint8_t type;
    uint8_t code;
    uint16_t csum;
    uint32_t rest;
    template <typename Adjuster>
    auto adjust_endianness(Adjuster a) {
        return a(csum);
    }
} __attribute__((packed));

}

#endif /* IP_HH_ */
