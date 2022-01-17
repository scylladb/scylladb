/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "utils/murmur_hash.hh"
#include "test/perf/perf.hh"

volatile uint64_t black_hole;

int main(int argc, char* argv[]) {
    const uint64_t seed = 0;
    auto src = bytes("0123412308129301923019283056789012345");

    uint64_t sink = 0;

    std::cout << "Timing fixed hash...\n";

    time_it([&] {
        std::array<uint64_t,2> dst;
        utils::murmur_hash::hash3_x64_128(src, seed, dst);
        sink += dst[0];
        sink += dst[1];
    });

    std::cout << "Timing iterator hash...\n";

    time_it([&] {
        std::array<uint64_t,2> dst;
        utils::murmur_hash::hash3_x64_128(src.begin(), src.size(), seed, dst);
        sink += dst[0];
        sink += dst[1];
    });

    black_hole = sink;
}
