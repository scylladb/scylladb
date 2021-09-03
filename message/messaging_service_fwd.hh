/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <boost/signals2.hpp>
#include <boost/signals2/dummy_mutex.hpp>

namespace gms { class inet_address; }

namespace netw {

struct msg_addr;
enum class messaging_verb;
class messaging_service;

using connection_drop_signal_t = boost::signals2::signal_type<void (gms::inet_address), boost::signals2::keywords::mutex_type<boost::signals2::dummy_mutex>>::type;
using connection_drop_slot_t = std::function<void(gms::inet_address)>;
using connection_drop_registration_t = boost::signals2::scoped_connection;

}
