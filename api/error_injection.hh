/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "api/api_init.hh"

namespace api {

void set_error_injection(http_context& ctx, httpd::routes& r);

}
