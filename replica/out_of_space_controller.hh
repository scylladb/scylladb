/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/abort_source.hh>
#include <seastar/core/sharded.hh>
#include <seastar/util/bool_class.hh>

#include "utils/disk_space_monitor.hh"
#include "utils/updateable_value.hh"

namespace replica {

// Simple controller to notify database it is in the critical disk utilization zone. The action
// is based on the current disk utilization.
//
// The controller uses the following configuration options:
// - critical_disk_utilization_threshold - ratio of disk utilization at which the write throttling
//   controller notifies database
//
class out_of_space_controller {
public:
    struct config {
        utils::updateable_value<float> critical_disk_utilization_threshold;
    };

    using reached_critical_disk_utilization = bool_class<struct reached_critical_disk_utilization_tag>;
    using subscription_callback_type = noncopyable_function<future<void> (reached_critical_disk_utilization)>;

    class subscription : public bi::list_base_hook<bi::link_mode<bi::auto_unlink>> {
        friend class out_of_space_controller;

        subscription_callback_type _callback;
        bool _registered_not_triggered = true;
    public:
        subscription() = default;

        explicit subscription(out_of_space_controller& controller, subscription_callback_type cb) : _callback(std::move(cb)) {
            controller._subscriptions.push_back(*this);
        }

        subscription(subscription&& other) noexcept
            : _callback(std::move(other._callback))
        {
            subscription_list_type::node_algorithms::swap_nodes(other.this_ptr(), this_ptr());
        }

        subscription& operator=(subscription&& other) noexcept
        {
            if (this == &other) {
                return *this;
            }

            _callback = std::move(other._callback);
            unlink();
            subscription_list_type::node_algorithms::swap_nodes(other.this_ptr(), this_ptr());
            return *this;
        }

        future<> operator()(reached_critical_disk_utilization mode) {
            _registered_not_triggered = false;
            return _callback(std::move(mode));
        }
    };

private:
    using subscription_list_type = bi::list<subscription, bi::constant_time_size<false>>;
    subscription_list_type _subscriptions;

    config _cfg;
    bool _critical_disk_utilization_threshold_reached { false };
    abort_source& _abort_source;
    utils::disk_space_monitor& _dsm;
    utils::disk_space_monitor::signal_connection_type _dsm_subscription;

    future<> notify();
    future<> notify_only_newly_registered();

public:
    out_of_space_controller(utils::disk_space_monitor& dsm,  config cfg, abort_source& as);

    future<> stop();
    [[nodiscard]] subscription subscribe(subscription_callback_type cb);
    bool is_critical_disk_utilization_threshold_reached() const noexcept {
        return bool(_critical_disk_utilization_threshold_reached);
    }
};

} // namespace replica
