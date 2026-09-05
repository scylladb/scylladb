/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "utils/s3/throttling_controller.hh"

#include <seastar/core/lowres_clock.hh>
#include <chrono>
#include <cstdint>

namespace s3 {

// Holds sending back for a short interval after the endpoint refuses a request.
// Single-shard, so no locking.
class aws_throttling_controller final : public throttling_controller {
    // On a throttling response the controller stops admitting altogether for this
    // long.
    static constexpr seastar::lowres_clock::duration freeze_duration = std::chrono::milliseconds(4000);

    // Minimum quiet period after a freeze. The trigger fires per throttling response
    // and an episode delivers hundreds per second, so without this the controller
    // would stay frozen for the whole episode. Caps the duty cycle at
    // freeze_duration / (freeze_duration + gap).
    static constexpr seastar::lowres_clock::duration freeze_min_gap = std::chrono::milliseconds(7000);

    // Doubles as the re-arm anchor: the quiet gap below is measured from it, so
    // while it is in the future the freeze is running and no new one may start.
    seastar::lowres_clock::time_point _frozen_until{};
    uint64_t _freezes = 0;

    uint64_t _throttles = 0; // throttling responses observed, for metrics only

public:
    seastar::future<> acquire(seastar::abort_source* as) override;
    void on_throttled() override;

    uint64_t throttles() const override { return _throttles; }
    uint64_t freezes() const override { return _freezes; }
};

} // namespace s3
