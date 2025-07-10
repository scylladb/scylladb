/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "api/api_init.hh"
#include <seastar/http/api_docs.hh>

namespace api {

void register_vector_search(std::shared_ptr<httpd::api_registry_builder20> rb, http_context& ctx, httpd::routes& r);
void register_vector_search_definitions(std::shared_ptr<httpd::api_registry_builder20> rb, http_context& ctx, httpd::routes& r);
}
