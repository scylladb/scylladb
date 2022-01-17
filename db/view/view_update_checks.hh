/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/future.hh>
#include "streaming/stream_reason.hh"
#include "seastarx.hh"

namespace replica {
class table;
}

namespace db {

class system_distributed_keyspace;

}

namespace db::view {

future<bool> check_view_build_ongoing(db::system_distributed_keyspace& sys_dist_ks, const sstring& ks_name, const sstring& cf_name);
future<bool> check_needs_view_update_path(db::system_distributed_keyspace& sys_dist_ks, const replica::table& t, streaming::stream_reason reason);

}
