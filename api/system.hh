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

namespace db { class sstables_format_selector; }

namespace api {

struct http_context;
void set_system(http_context& ctx, seastar::httpd::routes& r);

void set_format_selector(http_context& ctx, seastar::httpd::routes& r, db::sstables_format_selector& sel);
void unset_format_selector(http_context& ctx, seastar::httpd::routes& r);

}
