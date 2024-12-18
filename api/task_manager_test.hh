/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#ifndef SCYLLA_BUILD_MODE_RELEASE

#pragma once

#include <seastar/core/sharded.hh>

namespace tasks {
class task_manager;
}

namespace seastar::httpd {
class routes;
}

namespace api {
struct http_context;
void set_task_manager_test(http_context& ctx, seastar::httpd::routes& r, seastar::sharded<tasks::task_manager>& tm);
void unset_task_manager_test(http_context& ctx, seastar::httpd::routes& r);

}

#endif
