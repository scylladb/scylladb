/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once
#include <seastar/core/abort_source.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/semaphore.hh>

#include "seastarx.hh"
#include "utils/observable.hh"
#include "utils/disk_space_monitor.hh"
#include "utils/serialized_action.hh"

namespace service {

// Simple proportional controller to adjust shares for streaming based on the level of free capacity.
//
// The controller uses the following configuration options:
// - streaming_min_shares: minimum amount of shares for streaming.
// - streaming_max_shares: maximum amount of shares for streaming.
// - streaming_controller_disk_utilization_threshold: percentage of disk utilization at which
//   the streaming controller starts adjusting straming shares.
//
// When the node's disk utilization is less than `streaming_controller_disk_utilization_threshold`
// streaming is given `streaming_min_shares`.  Otherwise, the number of shares set for streaming
// is calculated using the following linear formula:
//   factor = (disk_utilization - threshold) / (1 - threshold)
//   streaming_min_shares + factor * (streaming_max_shares - streaming_min_shares)
class streaming_controller {
public:
    struct config {
        scheduling_group streaming_sg;
        utils::updateable_value<unsigned> min_shares;
        utils::updateable_value<unsigned> max_shares;
        utils::updateable_value<float> disk_utilization_threshold;
    };

    config _cfg;
    const utils::disk_space_monitor& _disk_space_monitor;
    abort_source& _abort_source;
    semaphore _sem;
    serialized_action _observe;
    utils::observer<unsigned> _min_shares_observer;
    utils::observer<unsigned> _max_shares_observer;
    utils::observer<float> _disk_utilization_threshold_observer;
    utils::disk_space_monitor::signal_connection_type _monitor_disk_space;
    optimized_optional<abort_source::subscription> _abort_subscription;

    streaming_controller(config cfg, utils::disk_space_monitor& dsm, abort_source& as);

    void validate_config() const;

    void abort() noexcept;
    future<> stop();

private:
    future<> adjust_if_needed() const;
};

} // namespace service
