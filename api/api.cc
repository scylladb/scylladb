/*
 * Copyright 2015 Cloudius Systems
 */

#include "api.hh"
#include "http/api_docs.hh"

namespace api {

future<> set_server(http_context& ctx) {
    auto rb = std::make_shared < api_registry_builder > ("api/api-doc/");

    return ctx.http_server.set_routes(rb->set_api_doc());
}

}

