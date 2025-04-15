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

class compaction_manager;

namespace api {
struct http_context;
void set_compaction_manager(http_context& ctx, seastar::httpd::routes& r, seastar::sharded<compaction_manager>& cm);
void unset_compaction_manager(http_context& ctx, seastar::httpd::routes& r);

}
