/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/sharded.hh>
#include "api.hh"

namespace api {

void set_storage_proxy(http_context& ctx, httpd::routes& r);
void unset_storage_proxy(http_context& ctx, httpd::routes& r);

}
