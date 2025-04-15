/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "api_init.hh"

namespace api {

void set_raft(http_context& ctx, httpd::routes& r, sharded<service::raft_group_registry>& raft_gr);
void unset_raft(http_context& ctx, httpd::routes& r);

}
