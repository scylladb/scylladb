/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#ifndef SCYLLA_BUILD_MODE_RELEASE

#pragma once

namespace cql_transport {
class controller;
}

namespace seastar::httpd {
class routes;
}

namespace api {
struct http_context;

void set_cql_server_test(http_context& ctx, seastar::httpd::routes& r, cql_transport::controller& ctl);
void unset_cql_server_test(http_context& ctx, seastar::httpd::routes& r);

}

#endif
