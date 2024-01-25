/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/sharded.hh>
#include "api/api_init.hh"
#include "db/config.hh"

namespace tasks {
    class task_manager;
}

namespace api {

void set_task_manager(http_context& ctx, httpd::routes& r, sharded<tasks::task_manager>& tm, db::config& cfg);
void unset_task_manager(http_context& ctx, httpd::routes& r);

}
