/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "utils/s3/throttling_controller.hh"

#include <seastar/core/future.hh>

namespace s3 {

// A do-nothing throttling controller: acquire() never waits and outcome
// feedback is ignored. Used by tests (e.g. the fuzzing S3 proxy) that inject
// synthetic throttling responses and must not have the client's send rate
// throttled as a side effect.
class noop_throttling_controller final : public throttling_controller {
public:
    seastar::future<> acquire(seastar::abort_source* = nullptr) override {
        return seastar::make_ready_future<>();
    }
    void update_client_sending_rate(bool) override {}

    bool enabled() const override { return false; }
    double fill_rate() const override { return 0.0; }
    double measured_tx_rate() const override { return 0.0; }
};

} // namespace s3
