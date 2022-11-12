/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 */

#include <iostream>

#if defined(__x86_64__) || defined(__i386__) || defined(__aarch64__)

#include "utils/clmul.hh"
#include "barett.hh"

#include <seastar/core/print.hh>

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
        pows[i] = crc32_fold_barett_u64(clmul(pows[i - 1], pows[i - 1]) << 1);
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
                product = crc32_fold_barett_u64(clmul(product, pows[base + j]) << 1);
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

int main() {
    std::array<const std::array<uint32_t, 256>*, 4> tables = {
        &crc32_x_pow_radix_8_table_base_0,
        &crc32_x_pow_radix_8_table_base_8,
        &crc32_x_pow_radix_8_table_base_16,
        &crc32_x_pow_radix_8_table_base_24,
    };

    std::cout << "/*\n"
                 " * Generated with gen_crc_combine_table.cc\n"
                 " * DO NOT EDIT!\n"
                 " */\n"
                 "\n"
                  "#include \"utils/gz/crc_combine_table.hh\"\n"
                 "\n";


    for (int base = 0; base < bits; base += radix_bits) {
        std::cout << "std::array<uint32_t, " << (1<<radix_bits) << "> crc32_x_pow_radix_8_table_base_" << base << " = {";

        auto& table = *tables[base / radix_bits];

        for (int i = 0; i < (1 << radix_bits); ++i) {
            if (i % 4 == 0) {
                std::cout << "\n    ";
            }
            auto product = table[i];
            std::cout << seastar::format(" 0x{:0>8x},", product);
        }

        std::cout << "\n};\n\n";
    }
}

#else

int main() {
    std::cout << "/*\n"
                 " * Generated with gen_crc_combine_table.cc\n"
                 " * DO NOT EDIT!\n"
                 " */\n"
                 "\n"
                 "/* Not implemented for this CPU architecture. */\n"
                 "\n";
    return 0;
}

#endif
