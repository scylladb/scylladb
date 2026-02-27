/*
 * Copyright (C) 2025-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <optional>

#include <boost/signals2/dummy_mutex.hpp>
#include <boost/signals2/signal_type.hpp>

#include "sstables/generation_type.hh"

namespace bs2 = boost::signals2;

namespace sstables {

class sstables_manager;

class sstables_manager_event_handler {
    std::optional<boost::signals2::scoped_connection> _connection;
public:
    void subscribe(boost::signals2::scoped_connection&& c) {
        assert(!_connection);
        _connection.emplace(std::move(c));
    }

    void unsubscribe() {
        _connection.reset();
    }

    // Note: other notifications like "added_sstables" may be needed in the future
    virtual future<> deleted_sstable(sstables::generation_type) const { return make_ready_future(); }
};

} // namespace sstables
