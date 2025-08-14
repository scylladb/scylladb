/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <filesystem>
#include <any>

#include <boost/signals2/connection.hpp>
#include <boost/signals2/signal_type.hpp>
#include <boost/signals2/dummy_mutex.hpp>

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/util/optimized_optional.hh>
#include <seastar/core/condition-variable.hh>

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
    using space_source_fn = std::function<future<std::filesystem::space_info>()>;

    using above_threshold = bool_class<struct above_threshold_tag>;
    using subscription_callback_type = noncopyable_function<future<> (above_threshold)>;

    struct config {
        scheduling_group sched_group;
        updateable_value<int> normal_polling_interval;
        updateable_value<int> high_polling_interval;
        // Use high_polling_interval above this threshold
        updateable_value<float> polling_interval_threshold;
        updateable_value<uint64_t> capacity_override; // 0 means no override.
    };

    struct subscription_trigger_options {
        // If set to true, the subscription will be triggered only when the threshold is crossed
        // (both ways). Otherwise, it will be triggered constantly.
        bool only_crossing_threshold { true };
        bool when_above_threshold { true };
        bool when_below_threshold { true };
    };
    // Required as a workaround to define default options in a method due to a bug in both GCC and clang
    //
    // void subscribe(subscription_trigger_options opt = {});  // does not compile
    // void subscribe(subscription_trigger_options opt = default_options()); // OK
    static subscription_trigger_options default_options() { return {}; }

    class subscription : public bi::list_base_hook<bi::link_mode<bi::auto_unlink>> {
        friend class disk_space_monitor;

        updateable_value<float> _threshold;
        subscription_callback_type _callback;
        above_threshold _threshold_reached = above_threshold::no;
        subscription_trigger_options _trigger_options;
    public:
        subscription() = default;

        explicit subscription(disk_space_monitor& monitor, updateable_value<float> threshold, subscription_callback_type cb, subscription_trigger_options opt)
            : _threshold(std::move(threshold))
            , _callback(std::move(cb))
            , _trigger_options(std::move(opt)) {
            monitor._subscriptions.push_back(*this);
        }

        subscription(subscription&& other) noexcept
            : _threshold(other._threshold)
            , _callback(std::move(other._callback)) {
            subscription_list_type::node_algorithms::swap_nodes(other.this_ptr(), this_ptr());
        }

        subscription& operator=(subscription&& other) noexcept
        {
            if (this == &other) {
                return *this;
            }
            unlink();

            _threshold = other._threshold;
            _callback = std::move(other._callback);

            subscription_list_type::node_algorithms::swap_nodes(other.this_ptr(), this_ptr());
            return *this;
        }

        future<> operator()(above_threshold threshold_reached) {
            return _callback(std::move(threshold_reached));
        }
    };

private:
    abort_source _as;
    optimized_optional<abort_source::subscription> _as_sub;
    future<> _poller_fut = make_ready_future();
    condition_variable _poll_cv;
    utils::phased_barrier _signal_barrier;
    signal_type _signal_source;
    std::filesystem::space_info _space_info;
    std::filesystem::path _data_dir;
    config _cfg;
    space_source_fn _space_source;
    std::any _capacity_observer;

    using subscription_list_type = bi::list<subscription, bi::constant_time_size<false>>;
    subscription_list_type _subscriptions;
    signal_connection_type _threshold_subscription;
    subscription _out_of_space_subscription;

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

    // Threshold is in range [0.0, 1.0], where 1.0 means 100% disk utilization.
    [[nodiscard]] subscription subscribe(updateable_value<float> threshold, subscription_callback_type cb, subscription_trigger_options opt = default_options());
    [[nodiscard]] subscription subscribe(float threshold, subscription_callback_type cb, subscription_trigger_options opt = default_options()) {
        return subscribe(updateable_value<float>(threshold), std::move(cb), std::move(opt));
    }

    // Registers a new space source function and returns an object that
    // restores the previous one when it goes out of scope.
    class space_source_registration {
        disk_space_monitor& _monitor;
        space_source_fn _prev_space_source;
    public:
        space_source_registration(disk_space_monitor& m);

        ~space_source_registration();
    };

    // Replaces default way of obtaining file system usage information.
    space_source_registration set_space_source(space_source_fn space_source) {
        auto ret = space_source_registration(*this);
        _space_source = std::move(space_source);
        return ret;
    }

    void trigger_poll() noexcept;

private:
    future<> poll();

    future<std::filesystem::space_info> get_filesystem_space();

    clock_type::duration get_polling_interval() const noexcept;

    friend class space_source_registration;
};

} // namespace utils
