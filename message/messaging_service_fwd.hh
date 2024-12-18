/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <boost/signals2/connection.hpp>
#include <boost/signals2/dummy_mutex.hpp>
#include <boost/signals2/signal_type.hpp>
#include "locator/host_id.hh"

namespace gms { class inet_address; }

namespace netw {

struct msg_addr;
enum class messaging_verb;
class messaging_service;

using connection_drop_signal_t = boost::signals2::signal_type<void (gms::inet_address, std::optional<locator::host_id>), boost::signals2::keywords::mutex_type<boost::signals2::dummy_mutex>>::type;
using connection_drop_slot_t = std::function<void(gms::inet_address, std::optional<locator::host_id>)>;
using connection_drop_registration_t = boost::signals2::scoped_connection;

}
