/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

#include <iostream>

#if defined(__x86_64__) || defined(__i386__) || defined(__aarch64__)

#include "utils/clmul.hh"
#include "barett.hh"

#include <seastar/core/print.hh>

int main() {
    const int bits = 32;
    const int radix_bits = 8;
    const uint32_t one = 0x80000000; // x^0

    std::cout << "/*\n"
                 " * Generated with gen_crc_combine_table.cc\n"
                 " * DO NOT EDIT!\n"
                 " */\n"
                 "\n"
                 "#include \"utils/gz/crc_combine_table.hh\"\n"
                 "\n";

    uint32_t pows[bits]; // pows[i] = x^(2^i*8) mod G(x)
    pows[0] = 0x00800000; // x^8
    for (int i = 1; i < bits; ++i) {
        //   x^(2*N)          mod G(x)
        // = (x^N)*(x^N)      mod G(x)
        // = (x^N mod G(x))^2 mod G(x)
        pows[i] = crc32_fold_barett_u64(clmul(pows[i - 1], pows[i - 1]) << 1);
    }

    for (int base = 0; base < bits; base += radix_bits) {
        std::cout << "uint32_t crc32_x_pow_radix_8_table_base_" << base << "[" << (1<<radix_bits) << "] = {";

        for (int i = 0; i < (1 << radix_bits); ++i) {
            uint32_t product = one;
            for (int j = 0; j < radix_bits; ++j) {
                if (i & (1 << j)) {
                    product = crc32_fold_barett_u64(clmul(product, pows[base + j]) << 1);
                }
            }
            if (i % 4 == 0) {
                std::cout << "\n    ";
            }
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
