/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <seastar/core/sharded.hh>
#include "db/snapshot-ctl.hh"

namespace seastar::httpd {
class routes;
}

namespace replica {
class database;
}

namespace service {
class storage_service;
}

namespace api {

struct http_context;
void set_tasks_compaction_module(http_context& ctx, httpd::routes& r, sharded<replica::database>& db, sharded<db::snapshot_ctl>& snap_ctl, sharded<service::storage_service>& ss);
void unset_tasks_compaction_module(http_context& ctx, httpd::routes& r);

}
