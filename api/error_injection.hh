/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "api/api_init.hh"

namespace api {

void set_error_injection(http_context& ctx, httpd::routes& r);

}
