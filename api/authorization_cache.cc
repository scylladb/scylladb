/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "api/api-doc/authorization_cache.json.hh"

#include "api/authorization_cache.hh"
#include "auth/service.hh"

namespace api {
using namespace json;
using namespace seastar::httpd;

void set_authorization_cache(http_context& ctx, routes& r, sharded<auth::service> &auth_service) {
    httpd::authorization_cache_json::authorization_cache_reset.set(r, [&auth_service] (std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        co_await auth_service.invoke_on_all([] (auth::service& auth) -> future<>  {
            auth.reset_authorization_cache();
            return make_ready_future<>();
        });

        co_return json_void();
    });
}

void unset_authorization_cache(http_context& ctx, routes& r) {
    httpd::authorization_cache_json::authorization_cache_reset.unset(r);
}

}
