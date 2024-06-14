/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "large_bitset.hh"
#include <seastar/core/align.hh>
#include <seastar/core/thread.hh>

using namespace seastar;

large_bitset::large_bitset(size_t nr_bits) : _nr_bits(nr_bits) {
    assert(thread::running_in_thread());

    size_t nr_ints = align_up(nr_bits, bits_per_int()) / bits_per_int();
    while (_storage.capacity() != nr_ints) {
        _storage.reserve_partial(nr_ints);
        thread::maybe_yield();
    }
    while (nr_ints) {
        _storage.push_back(0);
        --nr_ints;
        thread::maybe_yield();
    }
}

void
large_bitset::clear() {
    assert(thread::running_in_thread());
    for (auto&& pos: _storage) {
        pos = 0;
        thread::maybe_yield();
    }
}
