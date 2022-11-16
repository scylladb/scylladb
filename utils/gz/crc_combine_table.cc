/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 */

#if defined(__x86_64__) || defined(__i386__) || defined(__aarch64__)

#include <array>

#include "crc_combine_table.hh"
#include "utils/clmul.hh"
#include "barrett.hh"

template <int bits>
static
constexpr
std::array<uint32_t, bits>
make_crc32_power_table() {
    std::array<uint32_t, bits> pows;
    pows[0] = 0x00800000; // x^8
    for (int i = 1; i < bits; ++i) {
        //   x^(2*N)          mod G(x)
        // = (x^N)*(x^N)      mod G(x)
        // = (x^N mod G(x))^2 mod G(x)
        pows[i] = crc32_fold_barrett_u64(clmul(pows[i - 1], pows[i - 1]) << 1);
    }
    return pows;
}

static
constexpr
std::array<uint32_t, 256>
make_crc32_table(int base, int radix_bits, uint32_t one, std::array<uint32_t, 32> pows) {
    std::array<uint32_t, 256> table;
    for (int i = 0; i < (1 << radix_bits); ++i) {
        uint32_t product = one;
        for (int j = 0; j < radix_bits; ++j) {
            if (i & (1 << j)) {
                product = crc32_fold_barrett_u64(clmul(product, pows[base + j]) << 1);
            }
        }
        table[i] = product;
    }
    return table;
}

static constexpr int bits = 32;
static constexpr int radix_bits = 8;
static constexpr uint32_t one = 0x80000000; // x^0
static constexpr auto pows = make_crc32_power_table<bits>(); // pows[i] = x^(2^i*8) mod G(x)

constinit std::array<uint32_t, 256> crc32_x_pow_radix_8_table_base_0 = make_crc32_table(0, radix_bits, one, pows);
constinit std::array<uint32_t, 256> crc32_x_pow_radix_8_table_base_8 = make_crc32_table(8, radix_bits, one, pows);
constinit std::array<uint32_t, 256> crc32_x_pow_radix_8_table_base_16 = make_crc32_table(16, radix_bits, one, pows);
constinit std::array<uint32_t, 256> crc32_x_pow_radix_8_table_base_24 = make_crc32_table(24, radix_bits, one, pows);

#else

#error "Not implemented for this CPU architecture."

#endif
