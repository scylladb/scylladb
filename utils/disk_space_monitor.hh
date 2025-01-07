/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <filesystem>

#include <boost/signals2/connection.hpp>
#include <boost/signals2/signal_type.hpp>
#include <boost/signals2/dummy_mutex.hpp>

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/util/optimized_optional.hh>

#include "seastarx.hh"
#include "utils/updateable_value.hh"
#include "utils/phased_barrier.hh"

namespace utils {

// Instantiated only on shard 0
class disk_space_monitor {
public:
    using clock_type = lowres_clock;
    using signal_type = boost::signals2::signal_type<void (), boost::signals2::keywords::mutex_type<boost::signals2::dummy_mutex>>::type;
    using signal_callback_type = std::function<future<>(const disk_space_monitor&)>;
    using signal_connection_type = boost::signals2::scoped_connection;

    struct config {
        scheduling_group sched_group;
        updateable_value<int> normal_polling_interval;
        updateable_value<int> high_polling_interval;
        // Use high_polling_interval above this threshold
        updateable_value<float> polling_interval_threshold;
    };

private:
    abort_source _as;
    optimized_optional<abort_source::subscription> _as_sub;
    future<> _poller_fut = make_ready_future();
    utils::phased_barrier _signal_barrier;
    signal_type _signal_source;
    std::filesystem::space_info _space_info;
    std::filesystem::path _data_dir;
    config _cfg;

public:
    disk_space_monitor(abort_source& as, std::filesystem::path data_dir, config cfg);
    ~disk_space_monitor();

    future<> start();

    future<> stop() noexcept;

    const std::filesystem::path& data_dir() const noexcept {
        return _data_dir;
    }

    std::filesystem::space_info space() const noexcept {
        return _space_info;
    }

    float disk_utilization() const noexcept {
        return _space_info.capacity ? (float)(_space_info.capacity - _space_info.available) / _space_info.capacity : -1;
    }

    signal_connection_type listen(signal_callback_type callback);

private:
    future<> poll();

    future<std::filesystem::space_info> get_filesystem_space();

    clock_type::duration get_polling_interval() const noexcept;
};

} // namespace utils
