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

    // One attempt reached an outcome. Both are reported, because the refused share is
    // measured over the attempts that got an answer -- a refusal counted against a
    // denominator of dispatches would land in a different sample than the dispatch it
    // answers.
    virtual void on_throttled() = 0;
    virtual void on_not_throttled() = 0;

    // For metrics.
    virtual uint64_t throttles() const = 0;
    // Times sending was held back because the refused share crossed the threshold.
    virtual uint64_t freezes() const = 0;
    // Estimated share of attempts the endpoint is currently refusing, in [0, 1].
    virtual double refused_ratio() const = 0;
};

} // namespace s3
