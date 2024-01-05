/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "../../reader_concurrency_semaphore.hh"
#include "schema/schema.hh"

namespace tests {

// Must be used in a seastar thread.
class reader_concurrency_semaphore_wrapper {
    std::unique_ptr<::reader_concurrency_semaphore> _semaphore;

public:
    reader_concurrency_semaphore_wrapper(const char* name = nullptr)
        : _semaphore(std::make_unique<::reader_concurrency_semaphore>(::reader_concurrency_semaphore::no_limits{}, name ? name : "test",
                    reader_concurrency_semaphore::register_metrics::no))
    { }
    ~reader_concurrency_semaphore_wrapper() {
        _semaphore->stop().get();
    }

    reader_concurrency_semaphore& semaphore() { return *_semaphore; };
    reader_permit make_permit() { return _semaphore->make_tracking_only_permit(nullptr, "test", db::no_timeout, {}); }
};

} // namespace tests
