/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/future.hh>
#include "streaming/stream_reason.hh"
#include "locator/token_metadata_fwd.hh"
#include "seastarx.hh"

namespace replica {
class table;
}

namespace db {

class system_distributed_keyspace;

}

namespace db::view {

future<bool> check_needs_view_update_path(db::system_distributed_keyspace& sys_dist_ks, locator::token_metadata_ptr tmptr, const replica::table& t,
        streaming::stream_reason reason);

}
