/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

namespace seastar::httpd {
class routes;
}

namespace api {
struct http_context;
void set_compaction_manager(http_context& ctx, seastar::httpd::routes& r);

}
