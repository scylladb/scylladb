/*
 * Copyright (C) 2015-present ScyllaDB
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
