/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#ifndef SCYLLA_BUILD_MODE_RELEASE

#pragma once

#include <seastar/core/sharded.hh>

namespace service {
class storage_proxy;
}

namespace seastar::httpd {
class routes;
}

namespace api {
struct http_context;

void set_hinted_handoff_test(http_context& ctx, seastar::httpd::routes& r, seastar::sharded<service::storage_proxy>& proxy);
void unset_hinted_handoff_test(http_context& ctx, seastar::httpd::routes& r);

}

#endif
