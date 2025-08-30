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

namespace replica {
class database;
}

namespace api {

struct http_context;
void set_cache_service(http_context& ctx, seastar::sharded<replica::database>& db, seastar::httpd::routes& r);
void unset_cache_service(http_context& ctx, seastar::httpd::routes& r);

}
