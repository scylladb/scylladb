/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "api/api_init.hh"
#include <seastar/http/api_docs.hh>

namespace api {

void set_config(std::shared_ptr<httpd::api_registry_builder20> rb, http_context& ctx, httpd::routes& r, const db::config& cfg, bool first = false);
void unset_config(http_context& ctx, httpd::routes& r);
}
