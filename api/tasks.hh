/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/sharded.hh>
#include "db/snapshot-ctl.hh"

namespace seastar::httpd {
class routes;
}

namespace service {
class storage_service;
}

namespace api {

struct http_context;
void set_tasks_compaction_module(http_context& ctx, httpd::routes& r, sharded<service::storage_service>& ss, sharded<db::snapshot_ctl>& snap_ctl);
void unset_tasks_compaction_module(http_context& ctx, httpd::routes& r);

}
