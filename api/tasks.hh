/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "api.hh"
#include "db/config.hh"

namespace api {

void set_tasks_compaction_module(http_context& ctx, httpd::routes& r, sharded<service::storage_service>& ss, sharded<db::snapshot_ctl>& snap_ctl);
void unset_tasks_compaction_module(http_context& ctx, httpd::routes& r);

}
