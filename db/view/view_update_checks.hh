/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/future.hh>
#include "streaming/stream_reason.hh"
#include "locator/host_id.hh"
#include "seastarx.hh"

namespace replica {
class table;
}

namespace db {

class system_distributed_keyspace;

}

namespace locator {
template <typename NodeId>
class generic_token_metadata;
using token_metadata = generic_token_metadata<gms::inet_address>;
using token_metadata2 = generic_token_metadata<host_id>;
}

namespace db::view {

future<bool> check_needs_view_update_path(db::system_distributed_keyspace& sys_dist_ks, const locator::token_metadata& tm, const replica::table& t,
        streaming::stream_reason reason);

}
