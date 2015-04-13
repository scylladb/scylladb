/*
 * Copyright 2015 Cloudius Systems
 */

#include "api.hh"
#include "http/api_docs.hh"
#include "storage_service.hh"

namespace api {

future<> set_server(http_context& ctx) {
    auto rb = std::make_shared < api_registry_builder > ("api/api-doc/");

    return ctx.http_server.set_routes(rb->set_api_doc()).then([&ctx, rb] {
        ctx.http_server.set_routes(rb->register_function("storage_service",
                        "The storage service API"))
        .then([&ctx] {
            return set_storage_service(ctx);
        });
    });
}

}

