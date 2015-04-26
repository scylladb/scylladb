/*
 * Copyright 2015 Cloudius Systems
 */

#include "storage_service.hh"
#include "api/api-doc/storage_service.json.hh"

namespace api {

void set_storage_service(http_context& ctx, routes& r) {
    httpd::storage_service_json::local_hostid.set(r, [](const_req req) {
        return "";
    });
}

}
