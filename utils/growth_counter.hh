/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <cstdint>

#include <seastar/core/memory.hh>

namespace utils {

// Counts how much a counter that only grows, such as the bytes that a shard
// allocates, grows from the construction on. Pause it while other tasks run,
// and their growth is left out.
template <uint64_t (*Read)()>
class growth_counter {
    uint64_t _counted = 0;
    uint64_t _start = Read();
public:
    void pause() {
        _counted += Read() - _start;
    }
    void resume() {
        _start = Read();
    }
    // Not while paused.
    uint64_t value() const {
        return _counted + (Read() - _start);
    }
};

inline uint64_t total_bytes_allocated() {
    return seastar::memory::stats().total_bytes_allocated();
}

// Counts the bytes that this shard allocates, also those freed meanwhile.
using total_bytes_allocated_counter = growth_counter<total_bytes_allocated>;

}
