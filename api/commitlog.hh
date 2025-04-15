/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
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
