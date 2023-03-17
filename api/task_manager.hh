/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "api.hh"
#include "db/config.hh"

namespace api {

void set_task_manager(http_context& ctx, httpd::routes& r, db::config& cfg);

}
