/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/sharded.hh>
#include "api/api_init.hh"

namespace service { class storage_proxy; }

namespace api {

void set_storage_proxy(http_context& ctx, httpd::routes& r, sharded<service::storage_proxy>& proxy);
void unset_storage_proxy(http_context& ctx, httpd::routes& r);

}
