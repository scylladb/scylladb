/*
 * Copyright (C) 2024-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "utils/atomic_vector.hh"

using seastar::future;

namespace service {

class coordinator_event_subscriber {
public:
    virtual ~coordinator_event_subscriber() {}

    //TODO: add arguments
    virtual void on_tablet_migration_start() = 0;
    virtual void on_tablet_migration_finish() = 0;
    virtual void on_tablet_migration_abort() = 0;
};

class coordinator_event_notifier {
    atomic_vector<coordinator_event_subscriber*> _subscribers;

public:
    void register_subscriber(coordinator_event_subscriber* subscriber);
    future<> unregister_subscriber(coordinator_event_subscriber* subscriber) noexcept;

    future<> notify_tablet_migration_start();
    future<> notify_tablet_migration_finish();
    future<> notify_tablet_migration_abort();
};

}