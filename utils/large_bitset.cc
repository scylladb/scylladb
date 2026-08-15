/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "utils/assert.hh"
#include "large_bitset.hh"
#include "stall_free.hh"

#include <seastar/core/align.hh>
#include <seastar/core/thread.hh>

#include <algorithm>

using namespace seastar;

// Bound the preemption-check granularity to one chunk of the storage.
static constexpr size_t words_per_yield = large_bitset::storage_type::max_chunk_capacity();

large_bitset::large_bitset(size_t nr_bits) : _nr_bits(nr_bits) {
    SCYLLA_ASSERT(thread::running_in_thread());

    size_t nr_ints = align_up(nr_bits, bits_per_int()) / bits_per_int();
    utils::reserve_gently(_storage, nr_ints).get();
    // Fill one chunk per yield; capacity is already reserved.
    while (_storage.size() < nr_ints) {
        _storage.resize(std::min(nr_ints, _storage.size() + words_per_yield));
        thread::maybe_yield();
    }
}

void
large_bitset::clear() {
    SCYLLA_ASSERT(thread::running_in_thread());
    for (auto&& pos: _storage) {
        pos = 0;
        thread::maybe_yield();
    }
}
