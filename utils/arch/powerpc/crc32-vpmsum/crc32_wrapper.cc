/// This file has been taken from https://github.com/antonblanchard/crc32-vpmsum
/// Author: Anton Blanchard <anton@au.ibm.com>, IBM
#if defined(__PPC64__)

#include <stdint.h>
#include <stddef.h>

#include "utils/crc.hh"

#include "crc32_constants.hh"

extern "C" {
uint32_t __crc32_vpmsum(uint32_t crc, const uint8_t* p, size_t len);
}

namespace utils {
static constexpr size_t vmx_align = 16;
static constexpr size_t vmx_align_mask = vmx_align - 1;

#ifdef REFLECT
static uint32_t crc32_align(uint32_t crc, const uint8_t* p, size_t len)
{
    while (len--) {
        crc = crc_table[(crc ^ *p++) & 0xff] ^ (crc >> 8);
    }
    return crc;
}
#else
static uint32_t crc32_align(uint32_t crc, const uint8_t* p, size_t len)
{
    while (len--) {
        crc = crc_table[((crc >> 24) ^ *p++) & 0xff] ^ (crc << 8);
    }
    return crc;
}
#endif

uint32_t utils::crc32::crc32_vpmsum(uint32_t crc, const uint8_t* p, size_t len) {
    uint32_t prealign;
    uint32_t tail;

#ifdef CRC_XOR
    crc ^= 0xffffffff;
#endif

    if (len < vmx_align + vmx_align_mask) {
        crc = crc32_align(crc, p, len);
        goto out;
    }

    if ((unsigned long)p & vmx_align_mask) {
        prealign = vmx_align - ((unsigned long)p & vmx_align_mask);
        crc = crc32_align(crc, p, prealign);
        len -= prealign;
        p += prealign;
    }

    crc = __crc32_vpmsum(crc, p, len & ~vmx_align_mask);

    tail = len & vmx_align_mask;
    if (tail) {
        p += len & ~vmx_align_mask;
        crc = crc32_align(crc, p, tail);
    }

    out:
#ifdef CRC_XOR
    crc ^= 0xffffffff;
#endif

    return crc;
}
}
#endif
