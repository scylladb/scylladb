/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "api/api_init.hh"

namespace api {

void set_lsa(http_context& ctx, httpd::routes& r);

}
