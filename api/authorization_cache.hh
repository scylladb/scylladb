/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/sharded.hh>

namespace seastar::httpd {
class routes;
}

namespace auth {
class service;
}

namespace api {

struct http_context;
void set_authorization_cache(http_context& ctx, seastar::httpd::routes& r, seastar::sharded<auth::service> &auth_service);
void unset_authorization_cache(http_context& ctx, seastar::httpd::routes& r);

}
