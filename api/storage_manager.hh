/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once
#include <seastar/core/sharded.hh>

namespace seastar::httpd {
class routes;
}

namespace sstables {
class storage_manager;
}

namespace api {
struct http_context;
void set_storage_manager(http_context& ctx, seastar::httpd::routes& r, seastar::sharded<sstables::storage_manager>& sstm);
void unset_storage_manager(http_context& ctx, seastar::httpd::routes& r);

}
