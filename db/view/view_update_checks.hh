/*
 * Copyright (C) 2019-present ScyllaDB
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

#include <seastar/core/future.hh>
#include "streaming/stream_reason.hh"
#include "seastarx.hh"

class table;

namespace db {

class system_distributed_keyspace;

}

namespace db::view {

future<bool> check_view_build_ongoing(db::system_distributed_keyspace& sys_dist_ks, const sstring& ks_name, const sstring& cf_name);
future<bool> check_needs_view_update_path(db::system_distributed_keyspace& sys_dist_ks, const table& t, streaming::stream_reason reason);

}
