/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/sharded.hh>

namespace seastar::httpd {
class routes;
}

namespace replica { class database; }

namespace api {
struct http_context;
void set_commitlog(http_context& ctx, seastar::httpd::routes& r, seastar::sharded<replica::database>&);
void unset_commitlog(http_context& ctx, seastar::httpd::routes& r);

}
