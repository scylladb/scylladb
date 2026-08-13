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

// Holds sending back for a short interval once the endpoint refuses a sustained
// share of our requests. Single-shard, so no locking.
class aws_throttling_controller final : public throttling_controller {
    // Once the brake engages the controller stops admitting altogether for this long.
    static constexpr seastar::lowres_clock::duration freeze_duration = std::chrono::milliseconds(4000);

    // Outcomes folded into one sample of the refused share. Sampled on a count rather
    // than on a clock because the outcome rate is set by object size, not by the
    // client: 50 MiB parts carry 23 MB a request against roughly 51 MB/s of per-shard
    // bandwidth, so about 2 outcomes a second, while small objects were measured at
    // 120-500. No interval covers that spread -- one second divides by two at the low
    // end, where a sample could only read 0, 0.5 or 1, and by hundreds at the high one.
    // A fixed count gives every sample the same error and lets its duration float
    // instead: about 23 s for large parts, under a tenth of a second for small ones.
    static constexpr uint64_t outcomes_per_sample = 50;

    // Weight kept on the running estimate when a sample closes, following
    // seastar::update_moving_average. Kept short deliberately: with 50 outcomes behind
    // every sample the averaging already happens inside the sample, so the estimate only
    // has to smooth across samples and can stay responsive.
    static constexpr double ratio_ema_factor = 0.5;

    // Placed from S3's own request metrics over three fleet runs of this workload,
    // measured against attempts because that is the unit reported here. The endpoint
    // refused 21.2% / 21.7% / 21.5% of upload attempts over the whole path, and the two
    // upload passes sat near 9-11% and 28-30%. The threshold has to clear the lighter
    // pass by more than the estimate's own noise -- at 50 outcomes a sample that is
    // about 0.026, so 0.2 sits 3.3 sd above it -- and stay under the heavier one.
    static constexpr double ratio_threshold = 0.2;

    // Estimated share of attempts the endpoint is refusing. Rebuilt from zero after
    // every freeze, so it measures how the endpoint responded to what we sent since.
    double _refused_ratio = 0.0;

    // Open sample. Counted when an attempt reaches an outcome rather than when it is
    // dispatched, so a refusal and the attempt it answers always land in the same
    // sample and the ratio cannot exceed one.
    uint64_t _sample_outcomes = 0;
    uint64_t _sample_throttles = 0;

    seastar::lowres_clock::time_point _frozen_until{};
    uint64_t _freezes = 0;

    uint64_t _throttles = 0; // throttling responses observed, for metrics only

    // True while an outcome answers a request that was sent before the running freeze.
    // Nothing was offered during it, so such an outcome says nothing about how the
    // endpoint is treating us now.
    bool answers_pre_freeze_request() const;

    // Folds a finished sample into _refused_ratio, if one has finished.
    void close_sample();

public:
    seastar::future<> acquire(seastar::abort_source* as) override;
    void on_throttled() override;
    void on_not_throttled() override;

    uint64_t throttles() const override { return _throttles; }
    uint64_t freezes() const override { return _freezes; }
    double refused_ratio() const override { return _refused_ratio; }
};

} // namespace s3
