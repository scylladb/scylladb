/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
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

#include "large_bitset.hh"
#include <algorithm>
#include <seastar/core/align.hh>

large_bitset::large_bitset(size_t nr_bits) : _nr_bits(nr_bits) {
    auto nr_blocks = align_up(nr_bits, bits_per_block()) / bits_per_block();
    _storage.reserve(nr_blocks);
    size_t nr_ints = align_up(nr_bits, bits_per_int()) / bits_per_int();
    while (nr_ints) {
        auto now = std::min(ints_per_block(), nr_ints);
        _storage.push_back(std::make_unique<int_type[]>(now));
        std::fill_n(_storage.back().get(), now, 0);
        nr_ints -= now;
    }
}

void
large_bitset::clear() {
    size_t nr_ints = align_up(_nr_bits, bits_per_int()) / bits_per_int();
    auto bp = _storage.begin();
    while (nr_ints) {
        auto now = std::min(ints_per_block(), nr_ints);
        std::fill_n(bp++->get(), now, 0);
        nr_ints -= now;
    }
}
