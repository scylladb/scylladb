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

<<<<<<< HEAD
namespace db {

class system_distributed_keyspace;

}

namespace locator {
class token_metadata;
}

=======
>>>>>>> eaa3b774a6 (view: check_needs_view_update_path: get token_metadata_ptr)
namespace db::view {

<<<<<<< HEAD
future<bool> check_needs_view_update_path(db::system_distributed_keyspace& sys_dist_ks, const locator::token_metadata& tm, const replica::table& t,
=======
future<bool> check_needs_view_update_path(view_builder& vb, locator::token_metadata_ptr tmptr, const replica::table& t,
>>>>>>> eaa3b774a6 (view: check_needs_view_update_path: get token_metadata_ptr)
        streaming::stream_reason reason);

}
