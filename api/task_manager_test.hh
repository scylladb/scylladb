/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#ifndef SCYLLA_BUILD_MODE_RELEASE

#pragma once

#include "api.hh"
#include "db/config.hh"

namespace api {

void set_task_manager_test(http_context& ctx, routes& r, db::config& cfg);

}

#endif
