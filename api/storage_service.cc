/*
 * Copyright 2015 Cloudius Systems
 */

#include "storage_service.hh"
#include "api/api-doc/storage_service.json.hh"

namespace api {

future<> set_storage_service(http_context& ctx) {
    return ctx.http_server.set_routes([] (routes& r) {
        httpd::storage_service_json::local_hostid.set(r, [](const_req req) {
            return "";
        });
    });
}

}
