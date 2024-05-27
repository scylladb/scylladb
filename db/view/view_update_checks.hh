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

namespace locator {
class token_metadata;
}

namespace db::view {
class view_builder;

future<bool> check_needs_view_update_path(view_builder& vb, const locator::token_metadata& tm, const replica::table& t,
        streaming::stream_reason reason);

}
