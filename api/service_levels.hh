/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#pragma once

#include "api.hh"

namespace api {

void set_service_levels(http_context& ctx, httpd::routes& r, cql_transport::controller& ctl, sharded<cql3::query_processor>& qp);

}