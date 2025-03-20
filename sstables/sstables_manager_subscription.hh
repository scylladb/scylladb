/*
 * Copyright (C) 2025-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <boost/signals2/dummy_mutex.hpp>
#include <boost/signals2/signal_type.hpp>

#include "sstables/generation_type.hh"

namespace bs2 = boost::signals2;

namespace sstables {

enum class manager_event_type {
    add,
    unlink
};
using manager_signal_callback_type = std::function<future<>(sstables::generation_type, manager_event_type)>;
using manager_signal_connection_type = boost::signals2::scoped_connection;

} // namespace sstables
