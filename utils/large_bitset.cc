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

#include "large_bitset.hh"
#include <algorithm>
#include <seastar/core/align.hh>
#include <seastar/core/thread.hh>
#include "seastarx.hh"

using namespace seastar;

large_bitset::large_bitset(size_t nr_bits) : _nr_bits(nr_bits) {
    assert(thread::running_in_thread());

    const size_t orig_nr_ints = align_up(nr_bits, bits_per_int()) / bits_per_int();
    auto nr_ints = orig_nr_ints;
    while (nr_ints) {
        nr_ints = _storage.reserve_partial(nr_ints);
        if (need_preempt()) {
            thread::yield();
        }
    }
    nr_ints = orig_nr_ints;
    while (nr_ints) {
        _storage.push_back(0);
        --nr_ints;
        if (need_preempt()) {
            thread::yield();
        }
    }
}

void
large_bitset::clear() {
    assert(thread::running_in_thread());
    for (auto&& pos: _storage) {
        pos = 0;
        if (need_preempt()) {
            thread::yield();
        }
    }
}
