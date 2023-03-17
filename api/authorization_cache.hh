/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "api.hh"

namespace api {

void set_authorization_cache(http_context& ctx, httpd::routes& r, sharded<auth::service> &auth_service);
void unset_authorization_cache(http_context& ctx, httpd::routes& r);

}
