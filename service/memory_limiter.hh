/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 *
 * Copyright (C) 2021-present ScyllaDB
 *
 */

#pragma once

#include "seastarx.hh"
#include <seastar/core/semaphore.hh>

namespace service {

class memory_limiter final {
    size_t _mem_total;
    semaphore _sem;

public:
    memory_limiter(size_t available_memory) noexcept
        : _mem_total(available_memory / 10)
        , _sem(_mem_total) {}

    future<> stop() {
        return _sem.wait(_mem_total);
    }

    size_t total_memory() const noexcept { return _mem_total; }
    semaphore& get_semaphore() noexcept { return _sem; }
};

} // namespace service
