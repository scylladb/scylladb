/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <cstdint>

namespace s3 {

// Per-shard client-side send brake. acquire() is awaited before a request is
// dispatched and holds it back while the endpoint is refusing us; on_throttled()
// reports that the endpoint asked us to slow down.
class throttling_controller {
public:
    virtual ~throttling_controller() = default;

    // Waits until the client may send. With an abort source the wait resolves
    // with seastar::sleep_aborted when it is triggered.
    virtual seastar::future<> acquire(seastar::abort_source* as) = 0;

    // The endpoint asked us to slow down.
    virtual void on_throttled() = 0;

    // For metrics.
    virtual uint64_t throttles() const = 0;
    // Times sending was held back after a throttling response.
    virtual uint64_t freezes() const = 0;
};

} // namespace s3
