/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#ifndef SCYLLA_BUILD_MODE_RELEASE

#pragma once

#include <seastar/core/sharded.hh>
#include "api.hh"

namespace tasks {
class task_manager;
}

namespace api {

void set_task_manager_test(http_context& ctx, httpd::routes& r, sharded<tasks::task_manager>& tm);

}

#endif
