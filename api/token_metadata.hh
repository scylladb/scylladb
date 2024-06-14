/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/sharded.hh>

namespace seastar::httpd {
class routes;
}

namespace locator { class shared_token_metadata; }

namespace api {
struct http_context;
void set_token_metadata(http_context& ctx, seastar::httpd::routes& r, seastar::sharded<locator::shared_token_metadata>& tm);
void unset_token_metadata(http_context& ctx, seastar::httpd::routes& r);

}
