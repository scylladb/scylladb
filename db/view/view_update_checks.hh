/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/future.hh>
#include "streaming/stream_reason.hh"
#include "locator/token_metadata_fwd.hh"
#include "seastarx.hh"

namespace replica {
class table;
}

namespace db::view {
class view_builder;

future<bool> check_needs_view_update_path(view_builder& vb, locator::token_metadata_ptr tmptr, const replica::table& t,
        streaming::stream_reason reason);

}
