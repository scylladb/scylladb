/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/sharded.hh>
#include "api.hh"

namespace service { class storage_service; }

namespace api {

void set_token_metadata(http_context& ctx, httpd::routes& r, sharded<service::storage_service>& ss);
void unset_token_metadata(http_context& ctx, httpd::routes& r);

}
