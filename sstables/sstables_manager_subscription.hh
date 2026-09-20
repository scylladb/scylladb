/*
 * Copyright (C) 2025-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <optional>

#include <boost/signals2/dummy_mutex.hpp>
#include <boost/signals2/signal_type.hpp>

#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>

#include "sstables/generation_type.hh"
#include "utils/assert.hh"

namespace bs2 = boost::signals2;

namespace sstables {

class sstables_manager;

enum class notification_event_type {
    // Note: other event types like "added" may be needed in the future
    deleted
};

// Notifications are delivered asynchronously: the handler callback may suspend
// (backup_task_impl::worker, for example, hops to the backup shard using
// smp::submit_to), and the future it returns is discarded by the signal that
// invokes it. Therefore disconnecting the subscription does not mean that no
// callback is still in flight, and destroying the handler right after
// disconnecting leaves the suspended callback with a dangling reference to it.
//
// To prevent that, each notification is delivered while holding the
// notifications gate below, and the handler must be stopped, using stop(),
// before it is destroyed.
class sstables_manager_event_handler {
    std::optional<boost::signals2::scoped_connection> _connection;
    seastar::gate _notifications;

    // Disconnecting alone does not wait for in-flight notifications,
    // so this is only for stop().
    void unsubscribe() {
        _connection.reset();
    }
public:
    virtual ~sstables_manager_event_handler() {
        SCYLLA_ASSERT(!_connection || _notifications.is_closed());
    }

    void subscribe(boost::signals2::scoped_connection&& c) {
        assert(!_connection);
        _connection.emplace(std::move(c));
    }

    // Disconnect from the sstables_manager and wait for all in-flight
    // notifications to complete.
    // Must be called, and waited for, before the handler is destroyed.
    future<> stop() noexcept {
        unsubscribe();
        return _notifications.close();
    }

    // Called by sstables_manager to deliver a notification.
    // Holds the notifications gate for the duration of the callback, so that
    // stop() waits for it, even though the future returned to the signal that
    // invokes it is discarded.
    future<> notify(sstables::generation_type gen, notification_event_type event) {
        if (auto gh = _notifications.try_hold()) {
            switch (event) {
            case notification_event_type::deleted:
                co_await deleted_sstable(gen);
            }
        }
    }

    // Note: other notifications like "added_sstables" may be needed in the future
    virtual future<> deleted_sstable(sstables::generation_type) const { return make_ready_future(); }
};

} // namespace sstables
