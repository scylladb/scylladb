/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/sharded.hh>

namespace seastar::httpd {
class routes;
}

namespace locator { class shared_token_metadata; }
namespace gms { class gossiper; }

namespace api {
struct http_context;
void set_token_metadata(http_context& ctx, seastar::httpd::routes& r, seastar::sharded<locator::shared_token_metadata>& tm, seastar::sharded<gms::gossiper>& g);
void unset_token_metadata(http_context& ctx, seastar::httpd::routes& r);

}
