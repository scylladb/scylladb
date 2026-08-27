/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 *
 * Copyright (C) 2021-present ScyllaDB
 *
 */

#pragma once

#include "seastarx.hh"
#include <seastar/core/semaphore.hh>
#include <algorithm>

namespace service {

// Reserves a share of shard memory for admitting client requests, and caps how
// large a single request may be.
class memory_limiter final {
public:
    // Tag selecting a limiter that admits every request. The CQL maintenance
    // socket uses it: it is the operator's escape hatch, so it must stay usable
    // no matter how much memory user requests are holding. The request size
    // limit still applies.
    struct unlimited_tag {};

private:
    // A fraction of 0 would block every request forever, and more than the whole
    // shard is meaningless, so out-of-range values are clamped rather than rejected.
    static size_t budget(size_t available_memory, double fraction) noexcept {
        return available_memory * std::clamp(fraction, 0.01, 1.0);
    }

    size_t _max_request_size;
    size_t _mem_total;
    bool _unlimited;
    semaphore _sem;

public:
    memory_limiter(size_t available_memory, double fraction) noexcept
        : _max_request_size(budget(available_memory, fraction))
        , _mem_total(budget(available_memory, fraction))
        , _unlimited(false)
        , _sem(_mem_total) {}

    memory_limiter(unlimited_tag, size_t available_memory, double fraction) noexcept
        : _max_request_size(budget(available_memory, fraction))
        , _mem_total(semaphore::max_counter() / 2)
        , _unlimited(true)
        , _sem(_mem_total) {}

    future<> stop() {
        if (_unlimited) {
            return make_ready_future<>();
        }
        return _sem.wait(_mem_total);
    }

    // Memory reserved for admitting requests.
    size_t total_memory() const noexcept { return _mem_total; }

    // Largest single request accepted. Kept apart from total_memory() so that an
    // unlimited limiter still rejects absurdly large frames.
    size_t max_request_size() const noexcept { return _max_request_size; }

    semaphore& get_semaphore() noexcept { return _sem; }
};

} // namespace service
