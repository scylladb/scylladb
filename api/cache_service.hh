/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

namespace seastar::httpd {
class routes;
}

namespace api {

struct http_context;
void set_cache_service(http_context& ctx, seastar::httpd::routes& r);
void unset_cache_service(http_context& ctx, seastar::httpd::routes& r);

}
