/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "api/api_init.hh"

namespace api {

void set_service_levels(http_context& ctx, httpd::routes& r, cql_transport::controller& ctl, sharded<cql3::query_processor>& qp);

}
